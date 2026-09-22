"""Automatic, crash-safe Discovery archive lifecycle and SQLite compaction."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import shutil
import sqlite3
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from discovery_archive import (
    DEFAULT_ARCHIVE_ROOT, DEFAULT_MOUNT_PATH, DEFAULT_VOLUME_UUID,
    ArchiveIntegrityError, ArchiveStorageUnavailable, archive_descriptor,
    create_job_archive, delete_internal_job_rows, register_archive_segment,
    validate_archive_volume, verify_archive_record,
)
from storage_gc import collect_storage_metrics, discovery_gc_plan
from storage_maintenance import StorageMaintenanceLock


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_DATABASE = PROJECT_ROOT / "data" / "discovery_incremental.sqlite3"
DEFAULT_RUNTIME_DATABASE = PROJECT_ROOT / "data" / "discovery_jobs.sqlite3"
DEFAULT_ROTATION_DATABASE = PROJECT_ROOT / "data" / "discovery_rotation.sqlite3"
DEFAULT_STATE = PROJECT_ROOT / "data" / "discovery-archive-lifecycle.json"
TERMINAL = {"completed"}
ACTIVE_RUNTIME = {
    "launching", "running", "computed", "export_pending", "export_running",
    "export_complete", "notification_pending", "resumable", "resource_paused",
}
LIFECYCLE_STATES = {
    "INTERNAL", "ARCHIVE_CANDIDATE", "COPYING", "COPIED", "VERIFIED",
    "REPOINTED", "INTERNAL_CLEANUP", "ARCHIVED", "RETRYABLE", "BLOCKED", "CORRUPT",
}
COMPACT_MIN_BYTES = 2 * 1024**3
COMPACT_MIN_PERCENT = 15.0
COMPACT_PRESSURE_FREE_BYTES = 55 * 1024**3


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{uuid.uuid4().hex}")
    with temporary.open("w", encoding="utf-8") as stream:
        json.dump(payload, stream, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _load_state(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {"version": 1, "jobs": {}, "metrics": {}, "updated_at": _now()}
    value = json.loads(path.read_text(encoding="utf-8"))
    if value.get("version") != 1 or not isinstance(value.get("jobs"), dict):
        raise RuntimeError("unsupported archive lifecycle state")
    return value


def _runtime_rows(path: Path) -> dict[str, dict[str, Any]]:
    if not path.is_file():
        return {}
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='discovery_job_runtime'"
        ).fetchone()
        if not exists:
            return {}
        return {str(row["job_id"]): dict(row) for row in connection.execute(
            "SELECT * FROM discovery_job_runtime"
        )}
    finally:
        connection.close()


def _reference_hash(job: dict[str, Any]) -> str:
    payload = {
        "references": job.get("references") or {},
        "dependencies": job.get("dependencies") or {},
        "components": [(x.get("name"), x.get("row_count")) for x in job.get("components") or []],
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


def plan_archive_candidates(
    *, database: Path = DEFAULT_DATABASE, runtime_database: Path = DEFAULT_RUNTIME_DATABASE,
    rotation_database: Path = DEFAULT_ROTATION_DATABASE,
) -> dict[str, Any]:
    """Reference-aware planner. Unknown roots always block; latest job is protected."""
    plan = discovery_gc_plan(database, runtime_database, rotation_database)
    runtime = _runtime_rows(runtime_database)
    jobs = plan.get("jobs") or []
    internal_jobs = [row for row in jobs if str(row.get("authority") or "internal") == "internal"]
    latest = max(internal_jobs, key=lambda row: (str(row.get("created_at") or ""), str(row.get("job_id"))), default=None)
    latest_id = str(latest["job_id"]) if latest else None
    candidates, blocked = [], []
    for job in jobs:
        job_id = str(job["job_id"])
        authority = str(job.get("authority") or "internal")
        reasons: list[str] = []
        runtime_row = runtime.get(job_id) or {}
        runtime_status = str(runtime_row.get("status") or job.get("status") or "").casefold()
        if authority == "archive":
            reasons.append("already_archive_authoritative")
        if str(job.get("status") or "").casefold() not in TERMINAL:
            reasons.append("not_terminal")
        if runtime_status in ACTIVE_RUNTIME or int(runtime_row.get("resumable") or 0):
            reasons.append("running_queued_or_recovery_required")
        if job_id == latest_id:
            reasons.append("current_latest_job")
        if any(component.get("decision") == "UNKNOWN_KEEP" for component in job.get("components") or []):
            reasons.append("reference_graph_unverified")
        if int(job.get("schema_version") or 1) != 1:
            reasons.append("unsupported_schema")
        export_path = str((job.get("dependencies") or {}).get("export_path") or "")
        if not export_path or not Path(export_path).is_file():
            reasons.append("required_export_artifact_missing")
        record = {**job, "reference_hash": _reference_hash(job), "blockers": sorted(set(reasons))}
        (blocked if reasons else candidates).append(record)
    candidates.sort(key=lambda row: (int(row.get("estimated_bytes") or 0), str(row["job_id"])))
    return {**plan, "candidates": candidates, "blocked": blocked, "current_job_id": latest_id}


class DiscoveryArchiveLifecycle:
    def __init__(
        self, *, database: Path = DEFAULT_DATABASE, runtime_database: Path = DEFAULT_RUNTIME_DATABASE,
        rotation_database: Path = DEFAULT_ROTATION_DATABASE, state_path: Path = DEFAULT_STATE,
        mount_path: Path = DEFAULT_MOUNT_PATH, archive_root: Path = DEFAULT_ARCHIVE_ROOT,
        expected_uuid: str = DEFAULT_VOLUME_UUID, uuid_probe=None, maintenance_lock=None,
    ):
        self.database = Path(database)
        self.runtime_database = Path(runtime_database)
        self.rotation_database = Path(rotation_database)
        self.state_path = Path(state_path)
        self.mount_path = Path(mount_path)
        self.archive_root = Path(archive_root)
        self.expected_uuid = expected_uuid
        self.uuid_probe = uuid_probe
        self.lock = maintenance_lock or StorageMaintenanceLock(self.database.parent / "discovery-maintenance.lock")

    def _save_job(self, job_id: str, state: str, **values: Any) -> dict[str, Any]:
        if state not in LIFECYCLE_STATES:
            raise ValueError(state)
        payload = _load_state(self.state_path)
        previous = payload["jobs"].get(job_id) or {}
        row = {**previous, **values, "job_id": job_id, "state": state, "updated_at": _now()}
        if state not in {"RETRYABLE", "BLOCKED", "CORRUPT"} and "error" not in values:
            row.pop("error", None)
        payload["jobs"][job_id] = row
        payload["updated_at"] = row["updated_at"]
        _atomic_json(self.state_path, payload)
        return row

    def _fresh_candidate(self, job_id: str, expected_hash: str) -> dict[str, Any]:
        plan = plan_archive_candidates(
            database=self.database, runtime_database=self.runtime_database,
            rotation_database=self.rotation_database,
        )
        candidate = next((row for row in plan["candidates"] if row["job_id"] == job_id), None)
        if not candidate or candidate["reference_hash"] != expected_hash:
            raise ArchiveIntegrityError("eligibility/reference graph changed before repoint")
        return candidate

    def process_job(self, candidate: dict[str, Any]) -> dict[str, Any]:
        job_id = str(candidate["job_id"])
        reference_hash = str(candidate["reference_hash"])
        persisted = (_load_state(self.state_path).get("jobs") or {}).get(job_id) or {}
        state = str(persisted.get("state") or "INTERNAL")
        archive = persisted.get("archive")
        if state in {"INTERNAL", "ARCHIVE_CANDIDATE", "RETRYABLE", "BLOCKED"}:
            self._save_job(job_id, "ARCHIVE_CANDIDATE", reference_hash=reference_hash)
        try:
            validate_archive_volume(
                mount_path=self.mount_path, expected_uuid=self.expected_uuid,
                uuid_probe=self.uuid_probe,
            )
            if state not in {"COPIED", "VERIFIED", "REPOINTED", "INTERNAL_CLEANUP"}:
                self._save_job(job_id, "COPYING", reference_hash=reference_hash)
                segment = self.archive_root / "v1" / f"discovery-{job_id}-v1" / "discovery-job.sqlite3"
                if segment.is_file():
                    archive = archive_descriptor(segment)
                else:
                    archive = create_job_archive(
                        source_path=self.database, job_id=job_id, archive_root=self.archive_root,
                        mount_path=self.mount_path, expected_uuid=self.expected_uuid,
                        uuid_probe=self.uuid_probe,
                    )
                self._save_job(job_id, "COPIED", archive=archive, reference_hash=reference_hash)
                state = "COPIED"
            if state == "COPIED":
                verify_archive_record(
                    archive, mount_path=self.mount_path, archive_root=self.archive_root,
                    uuid_probe=self.uuid_probe,
                )
                self._save_job(job_id, "VERIFIED", archive=archive, reference_hash=reference_hash)
                state = "VERIFIED"
            with self.lock.retention_apply_guard(timeout_seconds=0.0):
                if state == "VERIFIED":
                    self._fresh_candidate(job_id, reference_hash)
                    register_archive_segment(
                        self.database, archive, mount_path=self.mount_path,
                        archive_root=self.archive_root, uuid_probe=self.uuid_probe,
                    )
                    self._save_job(job_id, "REPOINTED", archive=archive, reference_hash=reference_hash)
                self._save_job(job_id, "INTERNAL_CLEANUP", archive=archive, reference_hash=reference_hash)
                with sqlite3.connect(self.database) as check:
                    internal_exists = bool(check.execute(
                        "SELECT 1 FROM discovery_incremental_jobs WHERE job_id=?", (job_id,),
                    ).fetchone())
                deleted = (
                    delete_internal_job_rows(
                        self.database, job_id, mount_path=self.mount_path,
                        archive_root=self.archive_root, uuid_probe=self.uuid_probe,
                    ) if internal_exists else dict(archive.get("row_counts") or {})
                )
            return self._save_job(
                job_id, "ARCHIVED", archive=archive, row_counts=deleted,
                rows_deleted=sum(deleted.values()), reference_hash=reference_hash,
            )
        except ArchiveStorageUnavailable as error:
            return self._save_job(job_id, "RETRYABLE", error=str(error), reference_hash=reference_hash)
        except ArchiveIntegrityError as error:
            return self._save_job(job_id, "BLOCKED", error=str(error), reference_hash=reference_hash)
        except Exception as error:
            return self._save_job(job_id, "RETRYABLE", error=f"{type(error).__name__}: {error}", reference_hash=reference_hash)

    def run(self, *, max_jobs: int | None = None, medium_first: bool = False) -> dict[str, Any]:
        plan = plan_archive_candidates(
            database=self.database, runtime_database=self.runtime_database,
            rotation_database=self.rotation_database,
        )
        candidates = list(plan["candidates"])
        persisted_jobs = (_load_state(self.state_path).get("jobs") or {})
        by_id = {str(row["job_id"]): row for row in plan.get("jobs") or []}
        selected_ids = {str(row["job_id"]) for row in candidates}
        for job_id, state in persisted_jobs.items():
            if state.get("state") in {
                "COPYING", "COPIED", "VERIFIED", "REPOINTED", "INTERNAL_CLEANUP", "RETRYABLE",
            } and job_id not in selected_ids and job_id in by_id:
                candidates.insert(0, {
                    **by_id[job_id], "job_id": job_id,
                    "reference_hash": state.get("reference_hash") or _reference_hash(by_id[job_id]),
                    "blockers": [],
                })
                selected_ids.add(job_id)
        if medium_first and candidates:
            candidates = [candidates[len(candidates) // 2]]
        if max_jobs is not None:
            candidates = candidates[:max(0, int(max_jobs))]
        results = [self.process_job(candidate) for candidate in candidates]
        metrics = self.metrics(plan=plan)
        payload = _load_state(self.state_path)
        payload["metrics"] = metrics
        payload["updated_at"] = _now()
        _atomic_json(self.state_path, payload)
        return {"planned": len(candidates), "results": results, "metrics": metrics, "blocked": plan["blocked"]}

    def metrics(self, *, plan: dict[str, Any] | None = None) -> dict[str, Any]:
        plan = plan or plan_archive_candidates(
            database=self.database, runtime_database=self.runtime_database,
            rotation_database=self.rotation_database,
        )
        connection = sqlite3.connect(self.database)
        try:
            page_size = int(connection.execute("PRAGMA page_size").fetchone()[0])
            pages = int(connection.execute("PRAGMA page_count").fetchone()[0])
            free_pages = int(connection.execute("PRAGMA freelist_count").fetchone()[0])
            archived = connection.execute(
                "SELECT COALESCE(SUM(json_extract(row_counts_json,'$.discovery_job_items')),0),COUNT(*) FROM discovery_archive_segments WHERE status='valid'"
            ).fetchone()
        finally:
            connection.close()
        internal = shutil.disk_usage(self.database.parent)
        x9 = shutil.disk_usage(self.mount_path)
        return {
            "observed_at": _now(), "internal_free_bytes": internal.free,
            "x9_free_bytes": x9.free, "archive_backlog_jobs": len(plan["candidates"]),
            "archive_backlog_estimated_bytes": sum(int(x.get("estimated_bytes") or 0) for x in plan["candidates"]),
            "archived_jobs": int(archived[1]), "archived_item_rows": int(archived[0]),
            "sqlite_bytes": pages * page_size, "reclaimable_bytes": free_pages * page_size,
            "reclaimable_percent": (free_pages * 100.0 / pages) if pages else 0.0,
        }

    def compaction_due(self, metrics: dict[str, Any] | None = None) -> bool:
        metrics = metrics or self.metrics()
        reclaimable = int(metrics["reclaimable_bytes"])
        percent = float(metrics["reclaimable_percent"])
        internal_free = int(metrics["internal_free_bytes"])
        return reclaimable >= COMPACT_MIN_BYTES and (
            percent >= COMPACT_MIN_PERCENT or internal_free < COMPACT_PRESSURE_FREE_BYTES
        )

    def compact(self) -> dict[str, Any]:
        """VACUUM INTO, verify, atomic swap, smoke test, then remove bounded rollback."""
        before = self.metrics()
        if not self.compaction_due(before):
            return {"status": "NOT_DUE", "before": before}
        runtime = _runtime_rows(self.runtime_database)
        active = [job for job, row in runtime.items() if str(row.get("status") or "").casefold() in ACTIVE_RUNTIME]
        if active:
            return {"status": "BLOCKED_ACTIVE_DISCOVERY", "active": active, "before": before}
        compact = self.database.with_name(f".{self.database.name}.compact-{uuid.uuid4().hex}")
        rollback = self.database.with_name(f".{self.database.name}.precompact-{uuid.uuid4().hex}")
        with self.lock.retention_apply_guard(timeout_seconds=0.0):
            connection = sqlite3.connect(self.database, timeout=30)
            try:
                connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                if connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                    raise ArchiveIntegrityError("source integrity_check failed")
                schema_count = int(connection.execute("SELECT COUNT(*) FROM sqlite_master").fetchone()[0])
                connection.execute("VACUUM INTO ?", (str(compact),))
            finally:
                connection.close()
            # A zero-length/stale WAL belongs to the old inode and must never
            # be replayed against the atomically promoted compact database.
            for sidecar in (Path(str(self.database) + "-wal"), Path(str(self.database) + "-shm")):
                if sidecar.exists():
                    sidecar.unlink()
            rebuilt = sqlite3.connect(compact)
            try:
                if rebuilt.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                    raise ArchiveIntegrityError("rebuilt integrity_check failed")
                if int(rebuilt.execute("SELECT COUNT(*) FROM sqlite_master").fetchone()[0]) != schema_count:
                    raise ArchiveIntegrityError("rebuilt schema mismatch")
            finally:
                rebuilt.close()
            with compact.open("rb") as stream:
                os.fsync(stream.fileno())
            os.replace(self.database, rollback)
            try:
                os.replace(compact, self.database)
                smoke = sqlite3.connect(self.database)
                try:
                    if smoke.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                        raise ArchiveIntegrityError("post-swap quick_check failed")
                finally:
                    smoke.close()
            except BaseException:
                if self.database.exists():
                    self.database.unlink()
                os.replace(rollback, self.database)
                raise
            rollback.unlink()
            directory = os.open(self.database.parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        after = self.metrics()
        payload = _load_state(self.state_path)
        payload["last_compaction"] = {"completed_at": _now(), "before": before, "after": after}
        _atomic_json(self.state_path, payload)
        return {"status": "COMPACTED", "before": before, "after": after}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-jobs", type=int)
    parser.add_argument("--medium-first", action="store_true")
    parser.add_argument("--compact-if-due", action="store_true")
    parser.add_argument("--summary", action="store_true")
    args = parser.parse_args(argv)
    lifecycle = DiscoveryArchiveLifecycle()
    cycle_lock = lifecycle.state_path.with_suffix(".lock")
    cycle_lock.parent.mkdir(parents=True, exist_ok=True)
    with cycle_lock.open("a+") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print(json.dumps({"status": "ARCHIVE_CYCLE_ALREADY_RUNNING"}))
            return 0
        result = lifecycle.run(max_jobs=args.max_jobs, medium_first=args.medium_first)
        if args.compact_if_due:
            result["compaction"] = lifecycle.compact()
    if args.summary:
        result = {
            "planned": result.get("planned"),
            "results": [{
                "job_id": row.get("job_id"), "state": row.get("state"),
                "rows_deleted": row.get("rows_deleted"), "error": row.get("error"),
            } for row in result.get("results") or []],
            "blocked_count": len(result.get("blocked") or []),
            "metrics": result.get("metrics"),
            "compaction": result.get("compaction"),
        }
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
