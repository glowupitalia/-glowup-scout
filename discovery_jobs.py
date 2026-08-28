"""Persistent process ownership and status for long-running Discovery jobs."""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_DATABASE = PROJECT_ROOT / "data" / "discovery_jobs.sqlite3"
DEFAULT_LOG_DIR = PROJECT_ROOT / "data" / "logs"
ACTIVE_STATUSES = {
    "launching", "running", "export_running", "notification_pending",
}
RESUMABLE_CHECKPOINT_STATUSES = {
    "running", "failed", "interrupted", "waiting_retry",
    "qogita_refresh_failed", "supplier_preparation_failed",
    "resource_paused",
    "computed", "export_pending", "export_running", "export_resource_paused",
    "export_complete", "notification_pending",
}


SCHEMA = """
CREATE TABLE IF NOT EXISTS discovery_job_runtime (
    job_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    phase TEXT NOT NULL,
    started_at TEXT,
    updated_at TEXT NOT NULL,
    completed_at TEXT,
    budget TEXT,
    progress_current INTEGER NOT NULL DEFAULT 0,
    progress_total INTEGER NOT NULL DEFAULT 0,
    resumable INTEGER NOT NULL DEFAULT 1,
    error TEXT,
    worker_pid INTEGER,
    lease_expires_at TEXT,
    selected_suppliers_json TEXT NOT NULL DEFAULT '[]',
    filters_json TEXT NOT NULL DEFAULT '{}',
    checkpoint_path TEXT,
    export_path TEXT
);
CREATE INDEX IF NOT EXISTS idx_discovery_job_runtime_updated
ON discovery_job_runtime(updated_at DESC);
"""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def process_alive(pid: int | None) -> bool:
    if not pid or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except (ProcessLookupError, ValueError):
        return False
    except PermissionError:
        return True
    return True


class DiscoveryJobRegistry:
    def __init__(self, path: str | Path | None = None):
        configured = path or os.environ.get("DISCOVERY_JOB_DATABASE") or DEFAULT_DATABASE
        self.path = Path(configured).expanduser().resolve()

    def _new_connection(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA busy_timeout=30000")
        return connection

    @contextmanager
    def _connect(self):
        connection = self._new_connection()
        try:
            yield connection
        finally:
            connection.close()

    def initialize(self):
        with self._connect() as connection:
            connection.executescript(SCHEMA)

    @staticmethod
    def _row(row):
        if row is None:
            return None
        result = dict(row)
        result["selected_suppliers"] = json.loads(
            result.pop("selected_suppliers_json") or "[]"
        )
        result["filters"] = json.loads(result.pop("filters_json") or "{}")
        result["resumable"] = bool(result.get("resumable"))
        return result

    def register_checkpoint(self, state: dict[str, Any]):
        self.initialize()
        now = utc_now()
        checkpoint_path = str(
            PROJECT_ROOT / "data" / "discovery_jobs" / f"{state['job_id']}.json"
        )
        with self._connect() as connection:
            connection.execute(
                """INSERT INTO discovery_job_runtime
                   (job_id,status,phase,started_at,updated_at,completed_at,budget,
                    progress_current,progress_total,resumable,error,worker_pid,
                    lease_expires_at,selected_suppliers_json,filters_json,checkpoint_path)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(job_id) DO UPDATE SET
                     phase=excluded.phase, started_at=COALESCE(discovery_job_runtime.started_at,excluded.started_at),
                     budget=excluded.budget, selected_suppliers_json=excluded.selected_suppliers_json,
                     filters_json=excluded.filters_json, checkpoint_path=excluded.checkpoint_path,
                     resumable=excluded.resumable, updated_at=excluded.updated_at""",
                (
                    state["job_id"], "resumable", state.get("phase") or "initialized",
                    state.get("started_at"), now, state.get("completed_at"),
                    str(state.get("run_budget") or "all"),
                    int(state.get("progress_current") or state.get("rotation_analyzed_this_run") or 0),
                    int(state.get("progress_total") or state.get("sampled_identifier_count") or 0),
                    1, None, None, None,
                    json.dumps(state.get("selected_suppliers") or []),
                    json.dumps(state.get("filters") or {}, sort_keys=True), checkpoint_path,
                ),
            )
            connection.commit()

    def get(self, job_id: str):
        self.initialize()
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM discovery_job_runtime WHERE job_id=?", (job_id,)
            ).fetchone()
        return self._row(row)

    def latest(self):
        self.initialize()
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM discovery_job_runtime ORDER BY updated_at DESC LIMIT 1"
            ).fetchone()
        return self._row(row)

    def latest_active(self):
        self.reconcile()
        self.initialize()
        placeholders = ",".join("?" for _ in ACTIVE_STATUSES)
        with self._connect() as connection:
            row = connection.execute(
                f"SELECT * FROM discovery_job_runtime WHERE status IN ({placeholders}) "
                "ORDER BY updated_at DESC LIMIT 1",
                tuple(sorted(ACTIVE_STATUSES)),
            ).fetchone()
        return self._row(row)

    def claim(self, job_id: str, *, pid: int, lease_seconds: int = 300) -> bool:
        self.initialize()
        now = datetime.now(timezone.utc)
        expires = (now + timedelta(seconds=lease_seconds)).isoformat().replace("+00:00", "Z")
        observed = now.isoformat().replace("+00:00", "Z")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM discovery_job_runtime WHERE job_id=?", (job_id,)
            ).fetchone()
            if row is None:
                connection.rollback()
                return False
            lease = _parse_time(row["lease_expires_at"])
            owner_alive = process_alive(row["worker_pid"])
            if row["status"] == "running" and owner_alive and lease and lease > now:
                connection.rollback()
                return False
            other = connection.execute(
                """SELECT job_id,status,worker_pid,lease_expires_at FROM discovery_job_runtime
                   WHERE job_id<>? AND status IN ('launching','running')""", (job_id,)
            ).fetchall()
            for active in other:
                active_lease = _parse_time(active["lease_expires_at"])
                if (
                    active["status"] == "launching" and active_lease and active_lease > now
                ) or (
                    process_alive(active["worker_pid"])
                    and (active_lease is None or active_lease > now)
                ):
                    connection.rollback()
                    return False
            connection.execute(
                """UPDATE discovery_job_runtime SET status='running',worker_pid=?,
                   lease_expires_at=?,updated_at=?,error=NULL WHERE job_id=?""",
                (pid, expires, observed, job_id),
            )
            connection.commit()
        return True

    def heartbeat(
        self, job_id: str, *, pid: int, phase: str | None = None,
        current: int | None = None, total: int | None = None,
        lease_seconds: int = 300,
    ):
        observed = datetime.now(timezone.utc)
        expires = (observed + timedelta(seconds=lease_seconds)).isoformat().replace("+00:00", "Z")
        updates = ["updated_at=?", "lease_expires_at=?"]
        values: list[Any] = [observed.isoformat().replace("+00:00", "Z"), expires]
        if phase:
            updates.append("phase=?")
            values.append(phase)
        if current is not None:
            updates.append("progress_current=?")
            values.append(int(current))
        if total is not None:
            updates.append("progress_total=?")
            values.append(int(total))
        values.extend([job_id, pid])
        with self._connect() as connection:
            connection.execute(
                f"UPDATE discovery_job_runtime SET {','.join(updates)} "
                "WHERE job_id=? AND worker_pid=? "
                "AND status IN ('running','export_running','notification_pending')", values,
            )
            connection.commit()

    def prepare_finalization(self, job_id: str, state: dict[str, Any]):
        """Persist the computation/export hand-off without retaining ownership."""
        with self._connect() as connection:
            connection.execute(
                """UPDATE discovery_job_runtime SET status='export_pending',
                   phase='export_pending',updated_at=?,completed_at=?,
                   progress_current=?,progress_total=?,resumable=1,error=NULL,
                   worker_pid=NULL,lease_expires_at=NULL WHERE job_id=?""",
                (
                    utc_now(), state.get("completed_at"),
                    int(state.get("progress_current") or state.get("selected_count") or 0),
                    int(state.get("progress_total") or state.get("selected_count") or 0),
                    job_id,
                ),
            )
            connection.commit()

    def claim_finalization(
        self, job_id: str, *, pid: int, lease_seconds: int = 300,
    ) -> bool:
        """Idempotently claim only the export/notification phase."""
        self.initialize()
        now = datetime.now(timezone.utc)
        observed = now.isoformat().replace("+00:00", "Z")
        expires = (now + timedelta(seconds=lease_seconds)).isoformat().replace("+00:00", "Z")
        allowed = {
            "computed", "export_pending", "export_running",
            "export_resource_paused", "export_complete", "notification_pending",
            # Recovery of a computation persisted before the state machine was added.
            "running", "resumable",
        }
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM discovery_job_runtime WHERE job_id=?", (job_id,)
            ).fetchone()
            if row is None or str(row["status"]) not in allowed:
                connection.rollback()
                return False
            owner = row["worker_pid"]
            lease = _parse_time(row["lease_expires_at"])
            if owner and int(owner) != int(pid) and process_alive(owner) and lease and lease > now:
                connection.rollback()
                return False
            next_status = (
                "notification_pending"
                if row["export_path"] and str(row["status"]) in {"export_complete", "notification_pending"}
                else "export_running"
            )
            next_phase = "notification_pending" if next_status == "notification_pending" else "export_running"
            connection.execute(
                """UPDATE discovery_job_runtime SET status=?,phase=?,worker_pid=?,
                   lease_expires_at=?,updated_at=?,resumable=1,error=NULL WHERE job_id=?""",
                (next_status, next_phase, int(pid), expires, observed, job_id),
            )
            connection.commit()
        return True

    def mark_export_complete(self, job_id: str, *, pid: int, export_path: str):
        with self._connect() as connection:
            connection.execute(
                """UPDATE discovery_job_runtime SET status='notification_pending',
                   phase='notification_pending',export_path=?,updated_at=?
                   WHERE job_id=? AND worker_pid=? AND status='export_running'""",
                (str(export_path), utc_now(), job_id, int(pid)),
            )
            connection.commit()

    def export_resource_pause(
        self, job_id: str, *, reason: str, metrics: dict[str, Any], phase: str,
    ):
        message = json.dumps(
            {"reason": reason, "metrics": metrics},
            ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        )[:2000]
        with self._connect() as connection:
            connection.execute(
                """UPDATE discovery_job_runtime SET status='export_resource_paused',
                   phase=?,resumable=1,error=?,updated_at=?,worker_pid=NULL,
                   lease_expires_at=NULL WHERE job_id=?""",
                (phase, message, utc_now(), job_id),
            )
            connection.commit()

    def launch_finalizer(self, job_id: str) -> int:
        """Start finalization in a clean interpreter after computation persists."""
        log_dir = DEFAULT_LOG_DIR
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / f"discovery-{job_id}.log"
        with log_path.open("ab", buffering=0) as log:
            process = subprocess.Popen(
                [sys.executable, str(PROJECT_ROOT / "discovery_finalize_worker.py"),
                 "--job-id", job_id],
                cwd=PROJECT_ROOT, stdin=subprocess.DEVNULL,
                stdout=log, stderr=log, start_new_session=True, close_fds=True,
            )
        return int(process.pid)

    def finish(self, job_id: str, state: dict[str, Any], *, export_path: str | None = None):
        status = str(state.get("status") or "failed")
        resumable = int(status in RESUMABLE_CHECKPOINT_STATUSES)
        with self._connect() as connection:
            connection.execute(
                """UPDATE discovery_job_runtime SET status=?,phase=?,updated_at=?,completed_at=?,
                   progress_current=?,progress_total=?,resumable=?,error=?,worker_pid=NULL,
                   lease_expires_at=NULL,export_path=COALESCE(?,export_path) WHERE job_id=?""",
                (
                    "resumable" if resumable else status,
                    state.get("phase") or status, utc_now(), state.get("completed_at"),
                    int(state.get("progress_current") or state.get("rotation_analyzed_this_run") or 0),
                    int(state.get("progress_total") or state.get("sampled_identifier_count") or 0),
                    resumable, (state.get("errors") or [{}])[-1].get("message"),
                    export_path, job_id,
                ),
            )
            connection.commit()

    def fail(self, job_id: str, message: str):
        with self._connect() as connection:
            connection.execute(
                """UPDATE discovery_job_runtime SET status='resumable',resumable=1,error=?,
                   updated_at=?,worker_pid=NULL,lease_expires_at=NULL WHERE job_id=?""",
                (str(message)[:500], utc_now(), job_id),
            )
            connection.commit()

    def resource_pause(
        self, job_id: str, *, reason: str, metrics: dict[str, Any], phase: str,
    ):
        """Release ownership without classifying a protective pause as failure."""
        message = json.dumps(
            {"reason": reason, "metrics": metrics},
            ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        )[:2000]
        with self._connect() as connection:
            connection.execute(
                """UPDATE discovery_job_runtime SET status='resource_paused',phase=?,
                   resumable=1,error=?,updated_at=?,worker_pid=NULL,lease_expires_at=NULL
                   WHERE job_id=?""",
                (phase, message, utc_now(), job_id),
            )
            connection.commit()

    def update_recovery_progress(
        self, job_id: str, *, phase: str, current: int, total: int,
    ):
        """Align registry counters after an offline, API-free recovery migration."""
        with self._connect() as connection:
            connection.execute(
                """UPDATE discovery_job_runtime SET status='resumable',phase=?,
                   progress_current=?,progress_total=?,resumable=1,updated_at=?,
                   worker_pid=NULL,lease_expires_at=NULL WHERE job_id=?""",
                (phase, int(current), int(total), utc_now(), job_id),
            )
            connection.commit()

    def reconcile(self):
        self.initialize()
        now = datetime.now(timezone.utc)
        placeholders = ",".join("?" for _ in ACTIVE_STATUSES)
        with self._connect() as connection:
            rows = connection.execute(
                f"SELECT * FROM discovery_job_runtime WHERE status IN ({placeholders})",
                tuple(sorted(ACTIVE_STATUSES)),
            ).fetchall()
            for row in rows:
                lease = _parse_time(row["lease_expires_at"])
                if row["status"] == "launching" and lease and lease > now:
                    continue
                if process_alive(row["worker_pid"]) and (lease is None or lease > now):
                    continue
                connection.execute(
                    """UPDATE discovery_job_runtime SET status='resumable',resumable=1,
                       worker_pid=NULL,lease_expires_at=NULL,updated_at=? WHERE job_id=?""",
                    (utc_now(), row["job_id"]),
                )
            connection.commit()

    def launch(self, job_id: str) -> int:
        self.reconcile()
        existing = self.get(job_id)
        if not existing:
            raise ValueError("Discovery job is not registered")
        active = self.latest_active()
        if active:
            if active["job_id"] == job_id:
                raise RuntimeError("Discovery job is already running")
            raise RuntimeError(f"Another Discovery job is running: {active['job_id']}")
        launch_deadline = (
            datetime.now(timezone.utc) + timedelta(seconds=60)
        ).isoformat().replace("+00:00", "Z")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            concurrent = connection.execute(
                "SELECT job_id FROM discovery_job_runtime "
                "WHERE status IN ('launching','running') LIMIT 1"
            ).fetchone()
            if concurrent:
                connection.rollback()
                raise RuntimeError(f"Discovery job is already active: {concurrent['job_id']}")
            connection.execute(
                """UPDATE discovery_job_runtime SET status='launching',worker_pid=NULL,
                   updated_at=?,lease_expires_at=? WHERE job_id=?""",
                (utc_now(), launch_deadline, job_id),
            )
            connection.commit()
        log_dir = DEFAULT_LOG_DIR
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / f"discovery-{job_id}.log"
        try:
            with log_path.open("ab", buffering=0) as log:
                process = subprocess.Popen(
                    [sys.executable, str(PROJECT_ROOT / "discovery_worker.py"), "--job-id", job_id],
                    cwd=PROJECT_ROOT, stdin=subprocess.DEVNULL, stdout=log, stderr=log,
                    start_new_session=True, close_fds=True,
                )
        except Exception:
            self.fail(job_id, "Unable to launch Discovery worker")
            raise
        with self._connect() as connection:
            connection.execute(
                """UPDATE discovery_job_runtime SET worker_pid=?,updated_at=?
                   WHERE job_id=? AND status IN ('launching','running')""",
                (process.pid, utc_now(), job_id),
            )
            connection.commit()
        return process.pid
