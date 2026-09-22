#!/usr/bin/env python3
"""Automatic, crash-safe Supplier/Qogita archive lifecycle."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import shutil
import sqlite3
import stat
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from discovery_archive import DEFAULT_MOUNT_PATH, DEFAULT_VOLUME_UUID, validate_archive_volume
from storage_gc import discovery_gc_plan
from supplier_archive import (
    QOGITA_ARCHIVE_ROOT, STAGING_ROOT, SUPPLIER_ARCHIVE_ROOT,
    ArchiveIntegrityError, ArchiveStorageUnavailable,
    archive_catalog_record, create_archive_segment,
    initialize_supplier_archive_catalog, object_row_counts, object_tables,
    register_archive_segment, verify_archive_segment,
)
from supplier_catalog_coordination import (
    SupplierCatalogCoordinationTimeout, supplier_catalog_writer_lock, weekly_intent_active,
)


ROOT = Path(__file__).resolve().parent
DEFAULT_DATABASE = ROOT / "data" / "supplier_catalog.sqlite3"
DEFAULT_STATE = ROOT / "data" / "supplier-archive-lifecycle.json"
DEFAULT_LOG = ROOT / "logs" / "supplier-archive-maintenance.log"
COMPACT_MIN_BYTES = 2 * 1024**3
COMPACT_MIN_PERCENT = 15.0
COMPACT_PRESSURE_FREE_BYTES = 55 * 1024**3
ACTIVE_RUN_STATUSES = {"running", "staging", "pending", "collecting"}
RECOVERY_BOOTSTRAP_STATUSES = {"running", "auto_stopped", "paused", "interrupted"}
LIFECYCLE_STATES = {
    "INTERNAL", "ARCHIVE_CANDIDATE", "COPYING", "COPIED", "VERIFIED",
    "REPOINTED", "INTERNAL_CLEANUP", "ARCHIVED", "RETRYABLE", "BLOCKED", "CORRUPT",
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        handle.flush(); os.fsync(handle.fileno())
    os.replace(temporary, path)
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _load_state(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {"version": 1, "jobs": {}, "created_at": _now()}
    value = json.loads(path.read_text(encoding="utf-8"))
    if int(value.get("version") or 0) != 1 or not isinstance(value.get("jobs"), dict):
        raise RuntimeError("unsupported supplier archive lifecycle state")
    return value


def _readonly(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def _tables(connection: sqlite3.Connection) -> set[str]:
    return {str(row[0]) for row in connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )}


def _reference_hash(row: dict[str, Any]) -> str:
    payload = {
        "segment_type": row["segment_type"], "object_id": row["object_id"],
        "classification": row.get("classification"), "roots": row.get("roots") or [],
        "row_counts": row.get("row_counts") or {}, "status": row.get("status"),
    }
    return hashlib.sha256(_canonical(payload).encode()).hexdigest()


def build_reference_graph(database: Path = DEFAULT_DATABASE) -> dict[str, Any]:
    discovery = discovery_gc_plan()
    discovery_snapshots = set(discovery.get("snapshot_roots") or [])
    discovery_generations = set(discovery.get("generation_roots") or [])
    connection = _readonly(database)
    try:
        tables = _tables(connection)
        active_generations = {
            str(row[0]): str(row[1]) for row in connection.execute(
                "SELECT supplier,run_id FROM supplier_catalog_active_generations"
            )
        }
        active_serving = {
            str(row[0]) for row in connection.execute(
                "SELECT serving_generation_id FROM qogita_serving_active"
            )
        }
        serving_sources = {
            str(row[0]) for row in connection.execute(
                "SELECT source_generation_id FROM qogita_serving_snapshots "
                "WHERE serving_generation_id IN (SELECT serving_generation_id FROM qogita_serving_active)"
            )
        }
        duty_snapshots = {
            str(row[0]) for row in connection.execute(
                "SELECT last_serving_generation_id FROM qogita_bootstrap_duty_cycles "
                "WHERE last_serving_generation_id IS NOT NULL AND state NOT IN ('completed')"
            )
        } if "qogita_bootstrap_duty_cycles" in tables else set()
        active_memberships = {
            str(row[0]) for row in connection.execute(
                "SELECT membership_version_id FROM qogita_membership_active"
            )
        } if "qogita_membership_active" in tables else set()
        membership_sources = {
            str(row[0]) for row in connection.execute(
                "SELECT source_generation_id FROM qogita_membership_versions "
                "WHERE membership_version_id IN (SELECT membership_version_id FROM qogita_membership_active)"
            )
        } if {"qogita_membership_versions", "qogita_membership_active"}.issubset(tables) else set()
        recovery_generations = {
            str(row[0]) for row in connection.execute(
                "SELECT staging_run_id FROM qogita_bootstrap_runs WHERE status IN (%s)"
                % ",".join("?" for _ in RECOVERY_BOOTSTRAP_STATUSES),
                tuple(sorted(RECOVERY_BOOTSTRAP_STATUSES)),
            )
        }
        unknown_queue_generations = {
            str(row[0]) for row in connection.execute(
                "SELECT DISTINCT run_id FROM qogita_enrichment_queue "
                "WHERE status NOT IN ('pending','completed','failed')"
            )
        }
        active_generation_ids = set(active_generations.values()) | serving_sources | membership_sources
        live_incremental_sources: set[str] = set()
        for table in ("supplier_generation_product_refs", "supplier_generation_scenario_refs"):
            if table not in tables or not active_generation_ids:
                continue
            placeholders = ",".join("?" for _ in active_generation_ids)
            live_incremental_sources.update(
                str(row[0]) for row in connection.execute(
                    f"SELECT DISTINCT source_run_id FROM {table} "
                    f"WHERE run_id IN ({placeholders}) AND source_run_id IS NOT NULL",
                    tuple(sorted(active_generation_ids)),
                )
            )
        return {
            "active_generations": active_generations,
            "active_serving": sorted(active_serving), "serving_sources": sorted(serving_sources),
            "duty_snapshots": sorted(duty_snapshots), "active_memberships": sorted(active_memberships),
            "membership_sources": sorted(membership_sources),
            "recovery_generations": sorted(recovery_generations),
            "unknown_queue_generations": sorted(unknown_queue_generations),
            "live_incremental_sources": sorted(live_incremental_sources),
            "discovery_snapshots": sorted(discovery_snapshots),
            "discovery_generations": sorted(discovery_generations),
            "discovery_unknowns": discovery.get("unknowns") or [],
        }
    finally:
        connection.close()


def plan_supplier_archive(database: Path = DEFAULT_DATABASE) -> dict[str, Any]:
    graph = build_reference_graph(database)
    connection = _readonly(database)
    connection.row_factory = sqlite3.Row
    candidates: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    all_rows: list[dict[str, Any]] = []
    try:
        tables = _tables(connection)
        archived = set()
        if "supplier_archive_segments" in tables:
            archived = {
                (str(row[0]), str(row[1])) for row in connection.execute(
                    "SELECT segment_type,object_id FROM supplier_archive_segments WHERE status='valid'"
                )
            }
        active_serving = set(graph["active_serving"])
        protected_snapshots = active_serving | set(graph["duty_snapshots"])
        for raw in connection.execute("SELECT * FROM qogita_serving_snapshots ORDER BY created_at"):
            snapshot = dict(raw); object_id = str(snapshot["serving_generation_id"])
            membership_rows = int(connection.execute(
                "SELECT COUNT(*) FROM qogita_serving_memberships WHERE serving_generation_id=?",
                (object_id,),
            ).fetchone()[0])
            roots = []
            if object_id in active_serving: roots.append("current_serving")
            if object_id in set(graph["duty_snapshots"]): roots.append("recovery_duty")
            if object_id in set(graph["discovery_snapshots"]): roots.append("discovery_history_dual_reader")
            classification = "CURRENT_SERVING" if object_id in protected_snapshots else (
                "HISTORICAL_REFERENCED" if "discovery_history_dual_reader" in roots
                else "HISTORICAL_UNREFERENCED"
            )
            row = {
                "segment_type": "qogita_serving_snapshot", "object_id": object_id,
                "classification": classification, "roots": roots, "status": snapshot.get("status"),
                "row_counts": {"qogita_serving_snapshots": 1,
                               "qogita_serving_memberships": membership_rows},
                "estimated_bytes": membership_rows * 192 + 4096,
            }
            row["reference_hash"] = _reference_hash(row); all_rows.append(row)
            if (row["segment_type"], object_id) in archived:
                row["blocker"] = "already_archive_authoritative"; blocked.append(row)
            elif object_id in protected_snapshots:
                row["blocker"] = "current_or_recovery_serving"; blocked.append(row)
            else:
                candidates.append(row)
        active_memberships = set(graph["active_memberships"])
        membership_rows = (
            connection.execute("SELECT * FROM qogita_membership_versions ORDER BY created_at")
            if "qogita_membership_versions" in tables else []
        )
        for raw in membership_rows:
            version = dict(raw); object_id = str(version["membership_version_id"])
            entries = int(connection.execute(
                "SELECT COUNT(*) FROM qogita_membership_entries WHERE membership_version_id=?",
                (object_id,),
            ).fetchone()[0])
            roots = ["active_membership"] if object_id in active_memberships else []
            row = {
                "segment_type": "qogita_membership_snapshot", "object_id": object_id,
                "classification": "ACTIVE_BASELINE" if roots else "HISTORICAL_UNREFERENCED",
                "roots": roots, "status": version.get("status"),
                "row_counts": {"qogita_membership_versions": 1, "qogita_membership_entries": entries},
                "estimated_bytes": entries * 128 + 4096,
            }
            row["reference_hash"] = _reference_hash(row); all_rows.append(row)
            if (row["segment_type"], object_id) in archived:
                row["blocker"] = "already_archive_authoritative"; blocked.append(row)
            elif roots:
                row["blocker"] = "active_membership"; blocked.append(row)
            else:
                candidates.append(row)
        protected_generations = (
            set(graph["active_generations"].values()) | set(graph["serving_sources"])
            | set(graph["membership_sources"]) | set(graph["recovery_generations"])
            | set(graph["unknown_queue_generations"]) | set(graph["live_incremental_sources"])
        )
        for raw in connection.execute("SELECT * FROM supplier_catalog_runs ORDER BY started_at"):
            run = dict(raw); object_id = str(run["run_id"])
            roots = []
            for supplier, run_id in graph["active_generations"].items():
                if object_id == run_id: roots.append(f"active_baseline:{supplier}")
            if object_id in set(graph["serving_sources"]): roots.append("current_serving_source")
            if object_id in set(graph["membership_sources"]): roots.append("active_membership_source")
            if object_id in set(graph["recovery_generations"]): roots.append("bootstrap_recovery")
            if object_id in set(graph["unknown_queue_generations"]): roots.append("queue_unknown")
            if object_id in set(graph["live_incremental_sources"]): roots.append("active_incremental_source")
            if object_id in set(graph["discovery_generations"]): roots.append("discovery_history_dual_reader")
            status = str(run.get("status") or "").casefold()
            row_counts = {
                "supplier_catalog_runs": 1,
                "supplier_catalog_products": int(connection.execute(
                    "SELECT COUNT(*) FROM supplier_catalog_products WHERE run_id=?", (object_id,),
                ).fetchone()[0]),
                "supplier_catalog_scenarios": int(connection.execute(
                    "SELECT COUNT(*) FROM supplier_catalog_scenarios WHERE run_id=?", (object_id,),
                ).fetchone()[0]),
                "qogita_enrichment_queue": int(connection.execute(
                    "SELECT COUNT(*) FROM qogita_enrichment_queue WHERE run_id=?", (object_id,),
                ).fetchone()[0]),
            }
            row = {
                "segment_type": "supplier_generation", "object_id": object_id,
                "classification": "ACTIVE_BASELINE" if object_id in protected_generations else (
                    "WEEKLY_STAGING" if status in ACTIVE_RUN_STATUSES else
                    ("HISTORICAL_REFERENCED" if "discovery_history_dual_reader" in roots
                     else "HISTORICAL_UNREFERENCED")),
                "roots": roots, "status": status, "row_counts": row_counts,
                "estimated_bytes": row_counts["supplier_catalog_products"] * 1280
                                   + row_counts["supplier_catalog_scenarios"] * 1024
                                   + row_counts["qogita_enrichment_queue"] * 256 + 4096,
            }
            row["reference_hash"] = _reference_hash(row); all_rows.append(row)
            if (row["segment_type"], object_id) in archived:
                row["blocker"] = "already_archive_authoritative"; blocked.append(row)
            elif graph["discovery_unknowns"]:
                row["blocker"] = "discovery_reference_graph_unknown"; blocked.append(row)
            elif object_id in protected_generations or status in ACTIVE_RUN_STATUSES:
                row["blocker"] = "active_baseline_source_staging_or_recovery"; blocked.append(row)
            else:
                candidates.append(row)
        candidates.sort(key=lambda row: (int(row["estimated_bytes"]), row["segment_type"], row["object_id"]))
        return {"graph": graph, "candidates": candidates, "blocked": blocked, "objects": all_rows}
    finally:
        connection.close()


def _delete_internal(connection: sqlite3.Connection, segment_type: str, object_id: str) -> dict[str, int]:
    before = object_row_counts(connection, segment_type, object_id)
    if segment_type == "qogita_serving_snapshot":
        connection.execute("DELETE FROM qogita_serving_memberships WHERE serving_generation_id=?", (object_id,))
        connection.execute("DELETE FROM qogita_serving_snapshots WHERE serving_generation_id=?", (object_id,))
    elif segment_type == "qogita_membership_snapshot":
        connection.execute("DELETE FROM qogita_membership_entries WHERE membership_version_id=?", (object_id,))
        connection.execute("DELETE FROM qogita_membership_versions WHERE membership_version_id=?", (object_id,))
    elif segment_type == "supplier_generation":
        tables = _tables(connection)
        bootstrap_ids = [str(row[0]) for row in connection.execute(
            "SELECT bootstrap_run_id FROM qogita_bootstrap_runs WHERE staging_run_id=?", (object_id,),
        )] if "qogita_bootstrap_runs" in tables else []
        for bootstrap_id in bootstrap_ids:
            connection.execute("DELETE FROM qogita_bootstrap_milestones WHERE bootstrap_run_id=?", (bootstrap_id,))
            connection.execute("DELETE FROM qogita_bootstrap_duty_cycles WHERE bootstrap_run_id=?", (bootstrap_id,))
            connection.execute("DELETE FROM qogita_bootstrap_products WHERE bootstrap_run_id=?", (bootstrap_id,))
        connection.execute("DELETE FROM qogita_bootstrap_runs WHERE staging_run_id=?", (object_id,))
        request_ids = [str(row[0]) for row in connection.execute(
            "SELECT catalog_request_id FROM qogita_catalog_requests WHERE staging_run_id=?", (object_id,),
        )] if "qogita_catalog_requests" in tables else []
        for request_id in request_ids:
            connection.execute("DELETE FROM qogita_webhook_events WHERE catalog_request_id=?", (request_id,))
        if "qogita_catalog_requests" in tables:
            connection.execute("DELETE FROM qogita_catalog_requests WHERE staging_run_id=?", (object_id,))
        if "qogita_enrichment_queue" in tables:
            connection.execute("DELETE FROM qogita_enrichment_queue WHERE run_id=?", (object_id,))
        if "supplier_generation_scenario_refs" in tables:
            connection.execute("DELETE FROM supplier_generation_scenario_refs WHERE run_id=?", (object_id,))
        if "supplier_generation_product_refs" in tables:
            connection.execute("DELETE FROM supplier_generation_product_refs WHERE run_id=?", (object_id,))
        connection.execute("DELETE FROM supplier_catalog_scenarios WHERE run_id=?", (object_id,))
        connection.execute("DELETE FROM supplier_catalog_products WHERE run_id=?", (object_id,))
        connection.execute("DELETE FROM supplier_catalog_runs WHERE run_id=?", (object_id,))
    else:
        raise ValueError(segment_type)
    return before


def _affected_foreign_key_children(
    connection: sqlite3.Connection, segment_type: str, object_id: str,
) -> list[str]:
    """Return every child table whose FK can be affected by this cleanup."""
    parents = object_tables(connection, segment_type, object_id)
    children: list[str] = []
    for (table,) in connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ):
        quoted = str(table).replace('"', '""')
        if any(str(row[2]) in parents for row in connection.execute(
            f'PRAGMA foreign_key_list("{quoted}")'
        )):
            children.append(str(table))
    return children


class SupplierArchiveLifecycle:
    def __init__(self, *, database: Path = DEFAULT_DATABASE, state_path: Path = DEFAULT_STATE,
                 mount_path: Path = DEFAULT_MOUNT_PATH, expected_volume_uuid: str = DEFAULT_VOLUME_UUID,
                 uuid_probe=None):
        self.database = Path(database); self.state_path = Path(state_path)
        self.mount_path = Path(mount_path); self.expected_volume_uuid = expected_volume_uuid
        self.uuid_probe = uuid_probe

    @staticmethod
    def _key(segment_type: str, object_id: str) -> str:
        return f"{segment_type}:{object_id}"

    def _save(self, row: dict[str, Any], state: str, **values) -> dict[str, Any]:
        if state not in LIFECYCLE_STATES:
            raise ValueError(state)
        payload = _load_state(self.state_path); key = self._key(row["segment_type"], row["object_id"])
        current = dict((payload.get("jobs") or {}).get(key) or {})
        if state not in {"RETRYABLE", "BLOCKED", "CORRUPT"} and "error" not in values:
            values["error"] = None
        current.update({
            "segment_type": row["segment_type"], "object_id": row["object_id"],
            "state": state, "updated_at": _now(), "reference_hash": row.get("reference_hash"),
            **values,
        })
        payload.setdefault("jobs", {})[key] = current; payload["updated_at"] = _now()
        _atomic_json(self.state_path, payload)
        return current

    def _current_candidate(self, row: dict[str, Any]) -> dict[str, Any] | None:
        plan = plan_supplier_archive(self.database)
        for candidate in plan["candidates"]:
            if (candidate["segment_type"], candidate["object_id"]) == (row["segment_type"], row["object_id"]):
                return candidate
        return None

    def process(self, row: dict[str, Any]) -> dict[str, Any]:
        state_row = (_load_state(self.state_path).get("jobs") or {}).get(
            self._key(row["segment_type"], row["object_id"]), {}
        )
        state = str(state_row.get("state") or "INTERNAL")
        archive = state_row.get("archive")
        if state == "RETRYABLE" and archive:
            state = str(state_row.get("resume_state") or "COPIED")
        try:
            if state in {"INTERNAL", "ARCHIVE_CANDIDATE", "COPYING", "RETRYABLE"}:
                self._save(row, "ARCHIVE_CANDIDATE")
                self._save(row, "COPYING")
                archive = create_archive_segment(
                    self.database, row["segment_type"], row["object_id"],
                    mount_path=self.mount_path, expected_volume_uuid=self.expected_volume_uuid,
                    uuid_probe=self.uuid_probe,
                )
                state_row = self._save(row, "COPIED", archive=archive)
                state = "COPIED"
            if state == "COPIED":
                verify_archive_segment(
                    archive, mount_path=self.mount_path,
                    expected_volume_uuid=self.expected_volume_uuid, uuid_probe=self.uuid_probe,
                )
                state_row = self._save(row, "VERIFIED", archive=archive)
                state = "VERIFIED"
            if state == "VERIFIED":
                with supplier_catalog_writer_lock(timeout_seconds=0):
                    fresh = self._current_candidate(row)
                    if not fresh or fresh["reference_hash"] != row["reference_hash"]:
                        return self._save(row, "BLOCKED", archive=archive,
                                          error="reference_graph_changed_before_repoint")
                    connection = sqlite3.connect(self.database, timeout=30)
                    try:
                        connection.execute("BEGIN IMMEDIATE")
                        register_archive_segment(connection, archive)
                        connection.commit()
                    except Exception:
                        connection.rollback(); raise
                    finally:
                        connection.close()
                state_row = self._save(row, "REPOINTED", archive=archive)
                state = "REPOINTED"
            if state in {"REPOINTED", "INTERNAL_CLEANUP"}:
                self._save(row, "INTERNAL_CLEANUP", archive=archive)
                with supplier_catalog_writer_lock(timeout_seconds=0):
                    connection = sqlite3.connect(self.database, timeout=30)
                    # Some production child FK columns have no matching index.
                    # Enforcing them row-by-row turns this bounded cleanup into
                    # repeated scans of multi-million-row tables. We delete the
                    # known children explicitly and run a complete FK check in
                    # the same transaction before it can commit.
                    connection.execute("PRAGMA foreign_keys=OFF")
                    try:
                        connection.execute("BEGIN IMMEDIATE")
                        fk_children = _affected_foreign_key_children(
                            connection, row["segment_type"], row["object_id"],
                        )
                        deleted = _delete_internal(
                            connection, row["segment_type"], row["object_id"],
                        )
                        for child in fk_children:
                            quoted = child.replace('"', '""')
                            violation = connection.execute(
                                f'PRAGMA foreign_key_check("{quoted}")'
                            ).fetchone()
                            if violation:
                                raise ArchiveIntegrityError(
                                    "foreign key violation after archive cleanup: "
                                    f"{tuple(violation)}"
                                )
                        connection.commit()
                    except Exception:
                        connection.rollback(); raise
                    finally:
                        connection.close()
                return self._save(
                    row, "ARCHIVED", archive=archive, row_counts=deleted,
                    rows_deleted=sum(deleted.values()), archived_bytes=int(archive["archive_file_size"]),
                )
            if state == "ARCHIVED":
                return state_row
            return state_row
        except ArchiveIntegrityError as error:
            return self._save(row, "CORRUPT", archive=archive, error=str(error))
        except (ArchiveStorageUnavailable, SupplierCatalogCoordinationTimeout) as error:
            return self._save(row, "RETRYABLE", archive=archive, resume_state=state, error=str(error))
        except Exception as error:
            return self._save(row, "RETRYABLE", archive=archive,
                              resume_state=state, error=f"{type(error).__name__}: {error}")

    def run(self, *, max_objects: int | None = None, medium_first: bool = False) -> dict[str, Any]:
        plan = plan_supplier_archive(self.database)
        candidates = list(plan["candidates"])
        persisted = (_load_state(self.state_path).get("jobs") or {})
        by_key = {self._key(x["segment_type"], x["object_id"]): x for x in plan["objects"]}
        selected = {self._key(x["segment_type"], x["object_id"]) for x in candidates}
        for key, state in persisted.items():
            if state.get("state") in {"COPYING", "COPIED", "VERIFIED", "REPOINTED", "INTERNAL_CLEANUP", "RETRYABLE"}:
                # A crash after the atomic repoint legitimately leaves an archive
                # catalog record while internal cleanup is still pending. Such a
                # boundary must resume even though the normal planner now sees the
                # archive as authoritative.
                if key not in selected and key in by_key:
                    candidates.insert(0, by_key[key]); selected.add(key)
        if medium_first and candidates:
            positive = [x for x in candidates if int(x["estimated_bytes"]) > 4096] or candidates
            candidates = [positive[len(positive) // 2]]
        if max_objects is not None:
            candidates = candidates[:max(0, int(max_objects))]
        results = [self.process(row) for row in candidates]
        metrics = self.metrics()
        state = _load_state(self.state_path); state["metrics"] = metrics; state["updated_at"] = _now()
        _atomic_json(self.state_path, state)
        return {"planned": len(candidates), "results": results, "metrics": metrics,
                "blocked": plan["blocked"]}

    def metrics(self) -> dict[str, Any]:
        plan = plan_supplier_archive(self.database)
        connection = sqlite3.connect(self.database)
        try:
            page_size = int(connection.execute("PRAGMA page_size").fetchone()[0])
            pages = int(connection.execute("PRAGMA page_count").fetchone()[0])
            free_pages = int(connection.execute("PRAGMA freelist_count").fetchone()[0])
            initialize_supplier_archive_catalog(connection); connection.commit()
            row = connection.execute(
                "SELECT COUNT(*),COALESCE(SUM(archive_file_size),0) FROM supplier_archive_segments WHERE status='valid'"
            ).fetchone()
        finally:
            connection.close()
        disk = shutil.disk_usage(self.database.parent); x9 = shutil.disk_usage(self.mount_path)
        return {
            "observed_at": _now(), "supplier_db_bytes": pages * page_size,
            "reclaimable_bytes": free_pages * page_size,
            "reclaimable_percent": free_pages * 100.0 / pages if pages else 0.0,
            "internal_free_bytes": disk.free, "x9_free_bytes": x9.free,
            "archive_backlog_objects": len(plan["candidates"]),
            "archive_backlog_bytes": sum(int(x["estimated_bytes"]) for x in plan["candidates"]),
            "archived_objects": int(row[0]), "archived_bytes": int(row[1]),
            "supplier_archive_bytes": sum(p.stat().st_size for p in SUPPLIER_ARCHIVE_ROOT.glob("*.sqlite3")),
            "qogita_archive_bytes": sum(p.stat().st_size for p in QOGITA_ARCHIVE_ROOT.glob("*.sqlite3")),
        }

    def compaction_due(self, metrics: dict[str, Any] | None = None) -> bool:
        metrics = metrics or self.metrics()
        return int(metrics["reclaimable_bytes"]) >= COMPACT_MIN_BYTES and (
            float(metrics["reclaimable_percent"]) >= COMPACT_MIN_PERCENT
            or int(metrics["internal_free_bytes"]) < COMPACT_PRESSURE_FREE_BYTES
        )

    def compact(self) -> dict[str, Any]:
        before = self.metrics()
        if not self.compaction_due(before):
            return {"status": "NOT_DUE", "before": before}
        if weekly_intent_active():
            return {"status": "BLOCKED_WEEKLY_INTENT", "before": before}
        compact = self.database.with_name(f".{self.database.name}.compact-{uuid.uuid4().hex}")
        rollback = self.database.with_name(f".{self.database.name}.precompact-{uuid.uuid4().hex}")
        source_mode = stat.S_IMODE(self.database.stat().st_mode)
        with supplier_catalog_writer_lock(timeout_seconds=0):
            source = sqlite3.connect(self.database, timeout=30)
            try:
                source.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                if source.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                    raise ArchiveIntegrityError("source integrity_check failed")
                active = tuple(source.execute(
                    "SELECT supplier,run_id FROM supplier_catalog_active_generations ORDER BY supplier"
                ).fetchall())
                serving = tuple(source.execute(
                    "SELECT supplier,serving_generation_id FROM qogita_serving_active ORDER BY supplier"
                ).fetchall())
                source.execute("VACUUM INTO ?", (str(compact),))
            finally:
                source.close()
            rebuilt = sqlite3.connect(compact)
            try:
                if rebuilt.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                    raise ArchiveIntegrityError("rebuilt integrity_check failed")
                if tuple(rebuilt.execute(
                    "SELECT supplier,run_id FROM supplier_catalog_active_generations ORDER BY supplier"
                ).fetchall()) != active:
                    raise ArchiveIntegrityError("active baseline changed during compaction")
                if tuple(rebuilt.execute(
                    "SELECT supplier,serving_generation_id FROM qogita_serving_active ORDER BY supplier"
                ).fetchall()) != serving:
                    raise ArchiveIntegrityError("active serving changed during compaction")
            finally:
                rebuilt.close()
            os.chmod(compact, source_mode)
            wal = Path(str(self.database) + "-wal")
            if wal.exists() and wal.stat().st_size:
                raise ArchiveIntegrityError("non-empty WAL blocks compact swap")
            for suffix in ("-wal", "-shm"):
                sidecar = Path(str(self.database) + suffix)
                if sidecar.exists():
                    sidecar.unlink()
            os.replace(self.database, rollback)
            try:
                os.replace(compact, self.database)
                smoke = sqlite3.connect(self.database)
                try:
                    if smoke.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                        raise ArchiveIntegrityError("post-swap quick_check failed")
                    if tuple(smoke.execute(
                        "SELECT supplier,run_id FROM supplier_catalog_active_generations ORDER BY supplier"
                    ).fetchall()) != active:
                        raise ArchiveIntegrityError("post-swap active baseline mismatch")
                finally:
                    smoke.close()
            except Exception:
                if self.database.exists():
                    self.database.unlink()
                os.replace(rollback, self.database)
                raise
            rollback.unlink()
        after = self.metrics()
        state = _load_state(self.state_path)
        state["last_compaction"] = {"status": "COMPACTED", "completed_at": _now(),
                                    "before": before, "after": after}
        _atomic_json(self.state_path, state)
        return {"status": "COMPACTED", "before": before, "after": after}


def launch_supplier_archive_maintenance() -> int:
    DEFAULT_LOG.parent.mkdir(parents=True, exist_ok=True)
    with DEFAULT_LOG.open("ab", buffering=0) as log:
        process = subprocess.Popen(
            [sys.executable, str(Path(__file__).resolve()), "--max-objects", "1",
             "--compact-if-due", "--summary"],
            cwd=ROOT, stdin=subprocess.DEVNULL, stdout=log, stderr=log,
            start_new_session=True, close_fds=True,
        )
    return int(process.pid)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-objects", type=int)
    parser.add_argument("--medium-first", action="store_true")
    parser.add_argument("--compact-if-due", action="store_true")
    parser.add_argument("--summary", action="store_true")
    args = parser.parse_args(argv)
    lifecycle = SupplierArchiveLifecycle()
    lock_path = lifecycle.state_path.with_suffix(".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print(_canonical({"status": "SUPPLIER_ARCHIVE_CYCLE_ALREADY_RUNNING"}))
            return 0
        result = lifecycle.run(max_objects=args.max_objects, medium_first=args.medium_first)
        if args.compact_if_due:
            result["compaction"] = lifecycle.compact()
    if args.summary:
        result = {
            "planned": result["planned"],
            "results": [{k: row.get(k) for k in ("segment_type", "object_id", "state", "rows_deleted", "error")}
                        for row in result["results"]],
            "blocked_count": len(result["blocked"]), "metrics": result["metrics"],
            "compaction": result.get("compaction"),
        }
    print(_canonical(result)); return 0


if __name__ == "__main__":
    raise SystemExit(main())
