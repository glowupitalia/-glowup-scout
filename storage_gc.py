"""Reference-aware storage audit and bounded host storage monitoring.

The module deliberately implements *analysis only*.  There is no delete,
archive, repoint, VACUUM, or checkpoint operation here.  Production databases
are opened through SQLite ``mode=ro`` with ``query_only`` enabled and every
potentially non-trivial query has a deadline.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import shutil
import sqlite3
import subprocess
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable
from urllib.parse import quote


PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data"
DISCOVERY_DATABASE = DATA_DIR / "discovery_incremental.sqlite3"
RUNTIME_DATABASE = DATA_DIR / "discovery_jobs.sqlite3"
ROTATION_DATABASE = DATA_DIR / "discovery_rotation.sqlite3"
SUPPLIER_DATABASE = DATA_DIR / "supplier_catalog.sqlite3"

KEEP = "KEEP"
ARCHIVE = "ARCHIVE_CANDIDATE"
DELETE = "DELETE_CANDIDATE"
REPOINT = "REPOINT_REQUIRED"
UNKNOWN = "UNKNOWN_KEEP"
SQL_DEADLINE_SECONDS = 10.0
MONITOR_MAX_RECORDS = 336
OUTBOX_TERMINAL_STATUSES = {"sent", "not_configured", "failed"}

GIB = 1024 ** 3
WATERMARK_VERSION = "storage_watermark_v1"
ESTIMATOR_VERSION = "storage_workload_estimator_v1"
RETENTION_POLICY_VERSION = "storage_retention_policy_v1"
STORAGE_AUDIT_MAX_RECORDS = 336
DEFAULT_STORAGE_AUDIT = DATA_DIR / "storage-workload-metrics.jsonl"
DB1E11_JOB_ID = "db1e11b8d6294342b811a343ca4a4142"

# Each state is entered only when both dimensions meet its lower boundary.
# Selecting the worse of the absolute and percentage classifications makes the
# policy fail-safe across volumes of different sizes.
WATERMARK_THRESHOLDS = (
    ("NORMAL", 50 * GIB, 20.0),
    ("PREVENTIVE", 40 * GIB, 16.0),
    ("PRESSURE", 25 * GIB, 10.0),
    ("CRITICAL", 15 * GIB, 6.0),
)
WATERMARK_SEVERITY = {
    "NORMAL": 0, "PREVENTIVE": 1, "PRESSURE": 2,
    "CRITICAL": 3, "EMERGENCY": 4, "UNKNOWN": 5,
}

# Versioned seed values.  They are deliberately internal defaults rather than
# user settings; completed-workload observations can supersede them later.
WORKLOAD_ESTIMATORS = {
    "discovery_full": {
        "db_growth_bytes": int(3.04 * GIB), "wal_headroom_bytes": int(0.75 * GIB),
        "export_headroom_bytes": int(0.25 * GIB), "other_headroom_bytes": int(0.46 * GIB),
        "post_run_floor_bytes": 40 * GIB, "maximum_post_state": "PREVENTIVE",
    },
    "discovery_korean_beauty": {
        "db_growth_bytes": int(0.21 * GIB), "wal_headroom_bytes": int(0.25 * GIB),
        "export_headroom_bytes": int(0.05 * GIB), "other_headroom_bytes": int(0.49 * GIB),
        "post_run_floor_bytes": 25 * GIB, "maximum_post_state": "PRESSURE",
    },
    "discovery_qudo": {
        "db_growth_bytes": int(0.03 * GIB), "wal_headroom_bytes": int(0.15 * GIB),
        "export_headroom_bytes": int(0.03 * GIB), "other_headroom_bytes": int(0.49 * GIB),
        "post_run_floor_bytes": 25 * GIB, "maximum_post_state": "PRESSURE",
    },
    "discovery_small": {
        "db_growth_bytes": int(0.23 * GIB), "wal_headroom_bytes": int(0.25 * GIB),
        "export_headroom_bytes": int(0.05 * GIB), "other_headroom_bytes": int(0.47 * GIB),
        "post_run_floor_bytes": 25 * GIB, "maximum_post_state": "PRESSURE",
    },
    "qogita_window": {
        "db_growth_bytes": int(0.26 * GIB), "wal_headroom_bytes": int(0.68 * GIB),
        "export_headroom_bytes": int(0.10 * GIB), "other_headroom_bytes": int(0.20 * GIB),
        "post_run_floor_bytes": 25 * GIB, "maximum_post_state": "PRESSURE",
    },
    "weekly_abw": {
        "db_growth_bytes": int(0.30 * GIB), "wal_headroom_bytes": int(0.25 * GIB),
        "export_headroom_bytes": 0, "other_headroom_bytes": int(0.20 * GIB),
        "post_run_floor_bytes": 25 * GIB, "maximum_post_state": "PRESSURE",
    },
    "weekly_umma": {
        "db_growth_bytes": int(0.20 * GIB), "wal_headroom_bytes": int(0.20 * GIB),
        "export_headroom_bytes": 0, "other_headroom_bytes": int(0.20 * GIB),
        "post_run_floor_bytes": 25 * GIB, "maximum_post_state": "PRESSURE",
    },
    "weekly_qudo": {
        "db_growth_bytes": int(0.10 * GIB), "wal_headroom_bytes": int(0.15 * GIB),
        "export_headroom_bytes": 0, "other_headroom_bytes": int(0.20 * GIB),
        "post_run_floor_bytes": 25 * GIB, "maximum_post_state": "PRESSURE",
    },
    "weekly_qogita_korean_beauty": {
        "db_growth_bytes": int(0.05 * GIB), "wal_headroom_bytes": int(0.05 * GIB),
        "export_headroom_bytes": 0, "other_headroom_bytes": int(0.10 * GIB),
        "post_run_floor_bytes": 25 * GIB, "maximum_post_state": "PRESSURE",
    },
}


class ReadBudgetExceeded(RuntimeError):
    """Raised when a read-only audit query exceeds its bounded deadline."""


def final_only_contract_eligibility(
    *, job_status: str, terminal_summary_valid: bool,
    cache_verification_state: str | None, outbox_statuses: Iterable[str] = (),
    resumable: bool = False,
) -> dict[str, Any]:
    """Pure future-GC gate; it never mutates retention state or deletes rows."""
    blockers: list[str] = []
    if str(job_status).casefold() != "completed" or resumable:
        blockers.append("job_not_terminal_non_resumable")
    if not terminal_summary_valid:
        blockers.append("terminal_summary_missing_or_invalid")
    if str(cache_verification_state or "").casefold() != "verified":
        blockers.append("cache_not_verified")
    non_terminal = sorted({
        str(value).casefold() for value in outbox_statuses
        if str(value).casefold() not in OUTBOX_TERMINAL_STATUSES
    })
    if non_terminal:
        blockers.append("outbox_non_terminal:" + ",".join(non_terminal))
    return {"eligible": not blockers, "blockers": blockers}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _file_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0


def _readonly_connection(path: Path):
    """Open an existing SQLite database without creating files or WAL state."""
    absolute = path.expanduser().resolve()
    if not absolute.is_file():
        raise FileNotFoundError(absolute)
    uri = f"file:{quote(str(absolute), safe='/')}?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=0.25)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    connection.execute("PRAGMA busy_timeout=250")
    return connection


def _sqlite_file_metrics(path: Path) -> dict[str, Any]:
    """Return bounded physical/free-page metrics without creating a database."""
    result = {
        "path": str(path), "readable": False, "file_size_bytes": _file_size(path),
        "wal_bytes": _file_size(Path(str(path) + "-wal")),
        "shm_bytes": _file_size(Path(str(path) + "-shm")),
        "page_size": None, "page_count": None, "freelist_count": None,
        "freelist_bytes": None, "freelist_percent": None,
    }
    if not path.is_file():
        return {**result, "error": "database_missing"}
    try:
        with _readonly_connection(path) as connection:
            page_size = int(connection.execute("PRAGMA page_size").fetchone()[0])
            page_count = int(connection.execute("PRAGMA page_count").fetchone()[0])
            freelist_count = int(connection.execute("PRAGMA freelist_count").fetchone()[0])
        freelist_bytes = page_size * freelist_count
        allocated = page_size * page_count
        return {
            **result, "readable": True, "page_size": page_size,
            "page_count": page_count, "freelist_count": freelist_count,
            "freelist_bytes": freelist_bytes,
            "freelist_percent": (
                (freelist_bytes * 100.0 / allocated) if allocated else 0.0
            ),
        }
    except (OSError, sqlite3.Error) as error:
        return {**result, "error": f"{type(error).__name__}: {error}"}


def classify_storage_watermark(
    *, filesystem_total_bytes: int | None, filesystem_free_bytes: int | None,
) -> dict[str, Any]:
    """Classify storage by the worse of approved absolute/percentage bands."""
    try:
        total = int(filesystem_total_bytes or 0)
        free = int(filesystem_free_bytes if filesystem_free_bytes is not None else -1)
    except (TypeError, ValueError):
        total, free = 0, -1
    if total <= 0 or free < 0 or free > total:
        return {
            "version": WATERMARK_VERSION, "state": "UNKNOWN", "reliable": False,
            "reason": "filesystem_metrics_unavailable", "threshold_crossed": "unknown",
            "filesystem_free_bytes": None if free < 0 else free,
            "filesystem_free_percent": None,
        }
    percent = free * 100.0 / total

    def dimension_state(value: float, *, percentage: bool) -> str:
        for state, absolute, ratio in WATERMARK_THRESHOLDS:
            threshold = ratio if percentage else absolute
            if value >= threshold:
                return state
        return "EMERGENCY"

    absolute_state = dimension_state(float(free), percentage=False)
    percentage_state = dimension_state(percent, percentage=True)
    state = max(
        (absolute_state, percentage_state),
        key=lambda value: WATERMARK_SEVERITY[value],
    )
    controlling = (
        "both" if absolute_state == percentage_state
        else ("absolute" if state == absolute_state else "percentage")
    )
    return {
        "version": WATERMARK_VERSION, "state": state, "reliable": True,
        "reason": (
            f"prudent_max:absolute={absolute_state},percentage={percentage_state}"
        ),
        "threshold_crossed": controlling,
        "absolute_state": absolute_state, "percentage_state": percentage_state,
        "filesystem_total_bytes": total, "filesystem_free_bytes": free,
        "filesystem_free_percent": percent,
    }


def archive_volume_status(
    archive_volume_uuid: str | None,
    *, verifier: Callable[[str], dict[str, Any] | None] | None = None,
) -> dict[str, Any]:
    """Describe optional future Tier 2 without ever guessing a filesystem path."""
    volume_uuid = str(archive_volume_uuid or "").strip() or None
    if volume_uuid is None:
        return {
            "archive_volume_uuid": None, "archive_available": False,
            "archive_reason": "tier2_not_configured",
        }
    if verifier is None:
        return {
            "archive_volume_uuid": volume_uuid, "archive_available": False,
            "archive_reason": "volume_verification_unavailable_fail_closed",
        }
    try:
        observed = verifier(volume_uuid) or {}
    except Exception as error:
        return {
            "archive_volume_uuid": volume_uuid, "archive_available": False,
            "archive_reason": f"volume_verification_failed:{type(error).__name__}",
        }
    verified = bool(
        observed.get("mounted")
        and str(observed.get("volume_uuid") or "") == volume_uuid
        and observed.get("verified")
    )
    return {
        "archive_volume_uuid": volume_uuid, "archive_available": verified,
        "archive_reason": "verified" if verified else "volume_not_mounted_or_verified",
    }


def collect_storage_metrics(
    project_root: str | Path = PROJECT_ROOT, *, disk_usage: Callable = shutil.disk_usage,
    archive_volume_uuid: str | None = None,
    archive_volume_verifier: Callable[[str], dict[str, Any] | None] | None = None,
) -> dict[str, Any]:
    """Collect admission metrics with constant-size filesystem/PRAGMA reads."""
    root = Path(project_root).resolve()
    data = root / "data"
    try:
        usage = disk_usage(root)
        total, free = int(usage.total), int(usage.free)
        filesystem_error = None
    except (OSError, ValueError) as error:
        total = free = None
        filesystem_error = f"{type(error).__name__}: {error}"
    databases = {
        "discovery": _sqlite_file_metrics(data / "discovery_incremental.sqlite3"),
        "supplier": _sqlite_file_metrics(data / "supplier_catalog.sqlite3"),
        "rotation": _sqlite_file_metrics(data / "discovery_rotation.sqlite3"),
    }
    watermark = classify_storage_watermark(
        filesystem_total_bytes=total, filesystem_free_bytes=free,
    )
    discovery_freelist = databases["discovery"].get("freelist_bytes")
    reliable = bool(
        watermark["reliable"]
        and all(value.get("readable") for value in databases.values())
    )
    archive = archive_volume_status(
        archive_volume_uuid, verifier=archive_volume_verifier,
    )
    return {
        "version": "storage_metrics_v1", "timestamp": _utc_now(),
        "reliable": reliable, "filesystem_error": filesystem_error,
        "filesystem_total_bytes": total, "filesystem_free_bytes": free,
        "filesystem_free_percent": watermark.get("filesystem_free_percent"),
        "filesystem_reusable_bytes": free,
        "discovery_sqlite_freelist_bytes": discovery_freelist,
        "discovery_effective_reusable_bytes": (
            free + discovery_freelist
            if free is not None and discovery_freelist is not None else None
        ),
        "effective_reusable_scope": "discovery_db_growth_only",
        "databases": databases, "watermark": watermark,
        **archive,
    }


def discovery_workload_type(
    selected_suppliers: Iterable[str], qogita_universe: str | None = "full",
) -> str:
    suppliers = tuple(sorted({str(value).casefold() for value in selected_suppliers if value}))
    universe = str(qogita_universe or "full").casefold()
    if "qogita" in suppliers and universe == "full":
        return "discovery_full"
    if len(suppliers) >= 3 and universe != "korean_beauty":
        return "discovery_full"
    if suppliers == ("qogita",) and universe == "korean_beauty":
        return "discovery_korean_beauty"
    if suppliers == ("qudo",):
        return "discovery_qudo"
    return "discovery_small"


def workload_estimate(workload_type: str) -> dict[str, Any]:
    seed = WORKLOAD_ESTIMATORS.get(str(workload_type))
    if not seed:
        return {
            "version": ESTIMATOR_VERSION, "workload_type": str(workload_type),
            "reliable": False, "reason": "unknown_workload_type",
        }
    return {
        "version": ESTIMATOR_VERSION, "workload_type": str(workload_type),
        "reliable": True, "source": "versioned_seed", **seed,
    }


def storage_admission_decision(
    metrics: dict[str, Any], workload_type: str,
    *, estimate: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return a pure fail-closed decision; it never reserves or changes storage."""
    estimate = dict(estimate or workload_estimate(workload_type))
    watermark = dict(metrics.get("watermark") or {})
    if not metrics.get("reliable") or not watermark.get("reliable") or not estimate.get("reliable"):
        return {
            "allowed": False, "status": "admission_blocked_storage",
            "reason": "storage_metrics_or_estimate_unreliable", "workload_type": workload_type,
            "watermark": watermark, "estimate": estimate, "retention_execution": False,
        }
    current_state = str(watermark.get("state") or "UNKNOWN")
    if current_state in {"CRITICAL", "EMERGENCY", "UNKNOWN"}:
        return {
            "allowed": False, "status": "admission_blocked_storage",
            "reason": f"new_heavy_workload_blocked_in_{current_state.casefold()}",
            "workload_type": workload_type, "watermark": watermark,
            "estimate": estimate, "retention_execution": False,
        }
    free = int(metrics["filesystem_free_bytes"])
    total = int(metrics["filesystem_total_bytes"])
    discovery_workload = str(workload_type).startswith("discovery_")
    reusable_freelist = int(metrics.get("discovery_sqlite_freelist_bytes") or 0) if discovery_workload else 0
    main_growth = max(0, int(estimate["db_growth_bytes"]) - reusable_freelist)
    required = sum((
        main_growth, int(estimate.get("wal_headroom_bytes") or 0),
        int(estimate.get("export_headroom_bytes") or 0),
        int(estimate.get("other_headroom_bytes") or 0),
    ))
    post_free = free - required
    post_watermark = classify_storage_watermark(
        filesystem_total_bytes=total, filesystem_free_bytes=post_free,
    )
    floor = int(estimate["post_run_floor_bytes"])
    maximum_state = str(estimate["maximum_post_state"])
    allowed = bool(
        post_free >= floor
        and post_watermark.get("reliable")
        and WATERMARK_SEVERITY.get(str(post_watermark.get("state")), 99)
        <= WATERMARK_SEVERITY[maximum_state]
    )
    reasons = []
    if post_free < floor:
        reasons.append("post_run_floor_not_met")
    if WATERMARK_SEVERITY.get(str(post_watermark.get("state")), 99) > WATERMARK_SEVERITY[maximum_state]:
        reasons.append("post_run_watermark_too_severe")
    return {
        "allowed": allowed,
        "status": "admitted" if allowed else "admission_blocked_storage",
        "reason": "admission_headroom_satisfied" if allowed else ",".join(reasons),
        "workload_type": workload_type, "watermark": watermark,
        "post_run_watermark": post_watermark, "estimate": estimate,
        "required_filesystem_headroom_bytes": required,
        "discovery_freelist_credit_bytes": reusable_freelist,
        "filesystem_free_after_estimate_bytes": post_free,
        "retention_execution": False,
    }


def evaluate_discovery_admission(
    selected_suppliers: Iterable[str], qogita_universe: str | None = "full",
    *, metrics: dict[str, Any] | None = None, project_root: str | Path = PROJECT_ROOT,
) -> dict[str, Any]:
    workload_type = discovery_workload_type(selected_suppliers, qogita_universe)
    return storage_admission_decision(
        metrics or collect_storage_metrics(project_root), workload_type,
    )


def evaluate_qogita_window_admission(
    *, metrics: dict[str, Any] | None = None, project_root: str | Path = PROJECT_ROOT,
) -> dict[str, Any]:
    return storage_admission_decision(
        metrics or collect_storage_metrics(project_root), "qogita_window",
    )


def evaluate_weekly_admission(
    supplier: str, *, metrics: dict[str, Any] | None = None,
    project_root: str | Path = PROJECT_ROOT,
) -> dict[str, Any]:
    return storage_admission_decision(
        metrics or collect_storage_metrics(project_root),
        f"weekly_{str(supplier).casefold()}",
    )


def compaction_metrics(metrics: dict[str, Any]) -> dict[str, Any]:
    discovery = ((metrics.get("databases") or {}).get("discovery") or {})
    freelist = discovery.get("freelist_bytes")
    ratio = discovery.get("freelist_percent")
    known = freelist is not None and ratio is not None
    return {
        "version": "compaction_advisory_v1",
        "freelist_bytes": freelist, "freelist_percent": ratio,
        "compaction_recommended": bool(
            known and (int(freelist) >= 5 * GIB or float(ratio) >= 30.0)
        ),
        "execution_supported": False,
    }


def append_storage_audit_event(
    event: dict[str, Any], path: str | Path = DEFAULT_STORAGE_AUDIT,
    *, max_records: int = STORAGE_AUDIT_MAX_RECORDS,
) -> bool:
    """Persist a bounded operational audit; callers decide when writes are allowed."""
    record = {"timestamp": _utc_now(), "schema_version": 1, **event}
    before = event.get("storage_before") or {}
    after = event.get("storage_after") or {}
    if before or after:
        workload_type = str(event.get("workload_type") or "")
        database_name = "discovery" if workload_type.startswith("discovery") else (
            "supplier" if workload_type.startswith(("qogita", "weekly")) else None
        )
        before_db = ((before.get("databases") or {}).get(database_name) or {})
        after_db = ((after.get("databases") or {}).get(database_name) or {})
        record["workload_metrics"] = {
            "estimator_version": ESTIMATOR_VERSION,
            "filesystem_free_pre_bytes": before.get("filesystem_free_bytes"),
            "filesystem_free_post_bytes": after.get("filesystem_free_bytes"),
            "relevant_database": database_name,
            "relevant_db_pre_bytes": before_db.get("file_size_bytes"),
            "relevant_db_post_bytes": after_db.get("file_size_bytes"),
            "freelist_pre_bytes": before_db.get("freelist_bytes"),
            "freelist_post_bytes": after_db.get("freelist_bytes"),
            "wal_peak_observed_bytes": max(
                int(before_db.get("wal_bytes") or 0),
                int(after_db.get("wal_bytes") or 0),
            ),
            "export_bytes": int(event.get("export_bytes") or 0),
            "universe_size": int(event.get("universe_size") or 0),
            "elapsed_seconds": event.get("elapsed_seconds"),
            "success": event.get("success"),
        }
    try:
        destination = Path(path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        with destination.with_name(destination.name + ".lock").open("a+") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            append_monitor_snapshot(destination, record, max_records=max_records)
        return True
    except (OSError, TypeError, ValueError):
        # A telemetry write must never replace the primary workload outcome.
        return False


def _retention_gate(job: dict[str, Any]) -> list[str]:
    blockers: list[str] = []
    if str(job.get("job_id")) == DB1E11_JOB_ID:
        blockers.append("explicit_db1e11_exclusion")
    if str(job.get("retention_mode") or "full").casefold() != "full":
        blockers.append("storage_mode_not_full")
    if str(job.get("status") or "").casefold() != "completed" or job.get("resumable"):
        blockers.append("job_not_terminal_non_resumable")
    if not job.get("terminal_summary_valid"):
        blockers.append("terminal_summary_missing_or_invalid")
    if str(job.get("cache_verification_state") or "").casefold() != "verified":
        blockers.append("cache_not_verified")
    if any(
        str(value).casefold() not in OUTBOX_TERMINAL_STATUSES
        for value in (job.get("outbox_statuses") or [])
    ):
        blockers.append("outbox_non_terminal")
    if not job.get("rotation_committed"):
        blockers.append("rotation_not_committed")
    if not job.get("operational_export_preserved"):
        blockers.append("operational_export_missing")
    if not job.get("references_known"):
        blockers.append("reference_state_unknown")
    if not job.get("scope"):
        blockers.append("scope_unknown")
    return sorted(set(blockers))


def plan_discovery_retention(
    jobs: Iterable[dict[str, Any]], watermark_state: str,
) -> dict[str, Any]:
    """Apply approved E/D policy to an inventory without mutating any job."""
    state = str(watermark_state or "UNKNOWN").upper()
    global_keep = 10 if state == "PREVENTIVE" else (
        5 if state in {"PRESSURE", "CRITICAL", "EMERGENCY"} else None
    )
    inventory = [dict(job) for job in jobs]

    def order_key(job: dict[str, Any]):
        return (
            str(job.get("completed_at") or job.get("created_at") or ""),
            str(job.get("job_id") or ""),
        )

    full_completed = sorted(
        [job for job in inventory if str(job.get("status") or "").casefold() == "completed"
         and str(job.get("retention_mode") or "full").casefold() == "full"],
        key=order_key, reverse=True,
    )
    globally_protected = {
        str(job.get("job_id")) for job in (
            full_completed if global_keep is None else full_completed[:global_keep]
        )
    }
    scope_representatives: set[str] = set()
    represented_scopes: set[str] = set()
    for job in full_completed:
        scope = str(job.get("scope") or "")
        if scope and scope not in represented_scopes:
            scope_representatives.add(str(job.get("job_id")))
            represented_scopes.add(scope)

    rows = []
    for job in sorted(inventory, key=order_key, reverse=True):
        job_id = str(job.get("job_id") or "")
        blockers = _retention_gate(job)
        reasons: list[str] = []
        if not job.get("references_known"):
            classification = "UNKNOWN_KEEP"
            reasons.extend(blockers or ["reference_state_unknown"])
        elif blockers:
            classification = "KEEP_BLOCKED"
            reasons.extend(blockers)
        elif job_id in scope_representatives:
            classification = "KEEP_SCOPE_REPRESENTATIVE"
            reasons.append("latest_full_for_scope")
        elif job_id in globally_protected or global_keep is None:
            classification = "KEEP_FULL_RECENT"
            reasons.append(
                "normal_no_retention" if global_keep is None
                else f"latest_{global_keep}_global_full"
            )
        else:
            classification = "FINAL_ONLY_ELIGIBLE"
            reasons.extend((
                "all_reference_aware_gates_passed",
                "newer_full_exists_in_scope",
                f"outside_latest_{global_keep}_global_full",
            ))
        sqlite_reclaim = (
            int(job.get("estimated_sqlite_reclaim_bytes") or 0)
            if classification == "FINAL_ONLY_ELIGIBLE" else 0
        )
        file_reclaim = (
            int(job.get("estimated_filesystem_reclaim_bytes") or 0)
            if classification == "FINAL_ONLY_ELIGIBLE" else 0
        )
        rows.append({
            **job, "classification": classification, "reasons": reasons,
            "estimated_logical_reclaim_bytes": sqlite_reclaim + file_reclaim,
            "estimated_sqlite_freelist_gain_bytes": sqlite_reclaim,
            "estimated_filesystem_file_reclaim_bytes": file_reclaim,
        })
    eligible = [row for row in rows if row["classification"] == "FINAL_ONLY_ELIGIBLE"]
    return {
        "version": RETENTION_POLICY_VERSION, "watermark_state": state,
        "policy": (
            "NONE" if global_keep is None
            else ("E_KEEP_GLOBAL_10_SCOPE_1" if global_keep == 10 else "D_KEEP_GLOBAL_5_SCOPE_1")
        ),
        "execution_supported": False, "writes_performed": 0,
        "jobs": rows,
        "summary": {
            "job_count": len(rows), "candidate_count": len(eligible),
            "logical_reclaim_bytes": sum(row["estimated_logical_reclaim_bytes"] for row in eligible),
            "sqlite_freelist_gain_bytes": sum(row["estimated_sqlite_freelist_gain_bytes"] for row in eligible),
            "filesystem_file_reclaim_bytes": sum(row["estimated_filesystem_file_reclaim_bytes"] for row in eligible),
            "final_only_changes": 0, "delete_operations": 0,
        },
    }


def _terminal_summary_valid(metadata: dict[str, Any]) -> bool:
    payload = metadata.get("terminal_summary")
    if not isinstance(payload, dict) or int(metadata.get("terminal_summary_version") or 0) != 1:
        return False
    serialized = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str,
    )
    return metadata.get("terminal_summary_sha256") == hashlib.sha256(serialized.encode()).hexdigest()


def _retention_file_estimate(job_id: str, export_path: str | None, jobs_dir: Path) -> int:
    preserved = Path(export_path).expanduser().resolve() if export_path else None
    total = 0
    if not jobs_dir.is_dir():
        return 0
    for path in jobs_dir.glob(f"{job_id}.*"):
        if not path.is_file() or path.name.endswith(".state.json"):
            continue
        if preserved and path.resolve() == preserved:
            continue
        if path.name.endswith(".operational.xlsx"):
            continue
        total += _file_size(path)
    return total


def discovery_retention_inventory(
    project_root: str | Path = PROJECT_ROOT,
) -> dict[str, Any]:
    """Build the approved retention inputs; uncertainty is explicit and sticky."""
    root = Path(project_root).resolve()
    incremental_path = root / "data/discovery_incremental.sqlite3"
    runtime_path = root / "data/discovery_jobs.sqlite3"
    rotation_path = root / "data/discovery_rotation.sqlite3"
    jobs_dir = root / "data/discovery_jobs"
    errors: list[str] = []
    incremental: dict[str, dict[str, Any]] = {}
    markers: dict[str, str] = {}
    runtime: dict[str, dict[str, Any]] = {}
    outbox: dict[str, list[str]] = defaultdict(list)
    rotations: dict[str, dict[str, Any]] = {}
    estimates: dict[str, int] = {}
    try:
        gc_plan = discovery_gc_plan(incremental_path, runtime_path, rotation_path)
        estimates = {str(row["job_id"]): int(row.get("estimated_bytes") or 0) for row in gc_plan["jobs"]}
    except (OSError, sqlite3.Error, ReadBudgetExceeded) as error:
        errors.append(f"component_estimates:{type(error).__name__}")
    try:
        with _readonly_connection(incremental_path) as connection:
            for row in _query(connection, "SELECT * FROM discovery_incremental_jobs"):
                incremental[str(row["job_id"])] = dict(row)
            if "discovery_amazon_cache_indexed_jobs" in _table_names(connection):
                for row in _query(
                    connection,
                    "SELECT source_job_id,verification_state FROM discovery_amazon_cache_indexed_jobs",
                ):
                    markers[str(row[0])] = str(row[1])
    except (OSError, sqlite3.Error, ReadBudgetExceeded) as error:
        errors.append(f"incremental:{type(error).__name__}")
    try:
        with _readonly_connection(runtime_path) as connection:
            tables = _table_names(connection)
            for row in _query(connection, "SELECT * FROM discovery_job_runtime"):
                runtime[str(row["job_id"])] = dict(row)
            if "notification_outbox" in tables:
                for row in _query(connection, "SELECT entity_id,status FROM notification_outbox"):
                    outbox[str(row[0])].append(str(row[1]))
    except (OSError, sqlite3.Error, ReadBudgetExceeded) as error:
        errors.append(f"runtime:{type(error).__name__}")
    try:
        with _readonly_connection(rotation_path) as connection:
            for row in _query(
                connection,
                """SELECT job_id,scope_key,COUNT(*) AS total,
                          SUM(status='analyzed') AS analyzed
                     FROM discovery_rotation_selections GROUP BY job_id,scope_key""",
            ):
                rotations[str(row["job_id"])] = dict(row)
    except (OSError, sqlite3.Error, ReadBudgetExceeded) as error:
        errors.append(f"rotation:{type(error).__name__}")
    all_ids = set(incremental) | set(runtime) | set(rotations)
    jobs = []
    for job_id in sorted(all_ids):
        inc = incremental.get(job_id) or {}
        run = runtime.get(job_id) or {}
        rotation = rotations.get(job_id) or {}
        metadata = _json_dict(inc.get("metadata_json"))
        export_path = run.get("export_path")
        references_known = bool(
            not errors and inc and run and rotation and job_id in markers
        )
        jobs.append({
            "job_id": job_id,
            "status": run.get("status") or inc.get("status") or "unknown",
            "created_at": inc.get("created_at") or run.get("started_at"),
            "completed_at": run.get("completed_at") or metadata.get("completed_at"),
            "scope": rotation.get("scope_key") or metadata.get("rotation_scope"),
            "retention_mode": metadata.get("retention_mode") or "full",
            "resumable": bool(run.get("resumable")),
            "terminal_summary_valid": _terminal_summary_valid(metadata),
            "cache_verification_state": markers.get(job_id),
            "outbox_statuses": outbox.get(job_id, []),
            "rotation_committed": bool(
                rotation and int(rotation.get("total") or 0) == int(rotation.get("analyzed") or 0)
            ),
            "operational_export_preserved": bool(export_path and Path(str(export_path)).is_file()),
            "references_known": references_known,
            "estimated_sqlite_reclaim_bytes": int(estimates.get(job_id) or 0),
            "estimated_filesystem_reclaim_bytes": _retention_file_estimate(
                job_id, str(export_path) if export_path else None, jobs_dir,
            ),
        })
    return {"jobs": jobs, "errors": errors, "reliable": not errors}


def production_retention_plan(
    watermark_state: str, project_root: str | Path = PROJECT_ROOT,
) -> dict[str, Any]:
    inventory = discovery_retention_inventory(project_root)
    plan = plan_discovery_retention(inventory["jobs"], watermark_state)
    plan["inventory_reliable"] = inventory["reliable"]
    plan["inventory_errors"] = inventory["errors"]
    return plan


def _query(connection, sql: str, parameters: Iterable[Any] = ()) -> list[sqlite3.Row]:
    started = time.monotonic()
    # sqlite3.Connection objects do not expose an attribute dictionary on all
    # supported Python builds; callers use the shared conservative deadline.
    connection.set_progress_handler(
        lambda: 1 if time.monotonic() - started > SQL_DEADLINE_SECONDS else 0,
        5_000,
    )
    try:
        return connection.execute(sql, tuple(parameters)).fetchall()
    except sqlite3.OperationalError as error:
        if "interrupted" in str(error).casefold():
            raise ReadBudgetExceeded("read-only query exceeded deadline") from error
        raise
    finally:
        connection.set_progress_handler(None, 0)


def _table_names(connection) -> set[str]:
    return {
        str(row[0]) for row in _query(
            connection,
            "SELECT name FROM sqlite_master WHERE type='table'",
        )
    }


def _columns(connection, table: str) -> set[str]:
    return {str(row[1]) for row in _query(connection, f"PRAGMA table_info({table})")}


def _json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(str(value or "{}"))
        return parsed if isinstance(parsed, dict) else {}
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}


def _supplier_snapshot_roots(metadata: dict[str, Any]) -> tuple[set[str], set[str]]:
    """Return (serving snapshot ids, supplier generation ids) frozen by a job."""
    snapshots: set[str] = set()
    generations: set[str] = set()
    raw = metadata.get("supplier_snapshot_set") or metadata.get("supplier_snapshots") or {}
    values = raw.values() if isinstance(raw, dict) else raw if isinstance(raw, list) else []
    for value in values:
        if not isinstance(value, dict):
            continue
        supplier = str(value.get("supplier") or "").casefold()
        for key in ("serving_generation_id", "serving_snapshot_id"):
            if value.get(key):
                snapshots.add(str(value[key]))
        # Qogita calls its immutable serving membership a snapshot_id. Other
        # suppliers use snapshot_id for their materialized catalog generation.
        if value.get("snapshot_id"):
            target = snapshots if supplier == "qogita" else generations
            target.add(str(value["snapshot_id"]))
        for key in ("run_id", "source_generation_id"):
            if value.get(key):
                generations.add(str(value[key]))
    return snapshots, generations


def _component_bytes(connection, table: str, job_id: str, count: int) -> int:
    """Estimate payload bytes from at most 128 indexed rows, never a full payload scan."""
    if count <= 0:
        return 0
    payload_columns = {
        "discovery_incremental_jobs": "metadata_json",
        "discovery_job_items": "product_json",
        "discovery_purchase_scenarios": "scenario_json",
        "discovery_listing_classifications": "COALESCE(display_name,'')",
        "discovery_catalog_results": "diagnostics_json",
        "discovery_listings": "listing_json",
        "discovery_observations": "observation_json",
        "discovery_combinations": "combination_json",
        "discovery_resource_events": "metrics_json",
    }
    column = payload_columns.get(table)
    if not column:
        return 0
    rows = _query(
        connection,
        f"SELECT length({column}) AS n FROM {table} WHERE job_id=? LIMIT 128",
        (job_id,),
    )
    measured = [int(row[0] or 0) for row in rows]
    average = (sum(measured) / len(measured)) if measured else 0
    # Row/header/index overhead is deliberately conservative and documented.
    return int(count * (average + 96))


def _count_by_job(connection, table: str, job_id: str) -> int:
    return int(_query(connection, f"SELECT COUNT(*) FROM {table} WHERE job_id=?", (job_id,))[0][0])


def discovery_gc_plan(
    incremental_path: str | Path = DISCOVERY_DATABASE,
    runtime_path: str | Path = RUNTIME_DATABASE,
    rotation_path: str | Path = ROTATION_DATABASE,
) -> dict[str, Any]:
    """Build component-level Discovery retention decisions without writes."""
    incremental_path = Path(incremental_path)
    runtime_path = Path(runtime_path)
    rotation_path = Path(rotation_path)
    result: dict[str, Any] = {
        "jobs": [], "cache_reference_map": {}, "snapshot_roots": [],
        "generation_roots": [], "unknowns": [],
    }
    runtime: dict[str, dict[str, Any]] = {}
    outbox: dict[str, int] = defaultdict(int)
    rotations: dict[str, int] = defaultdict(int)
    global_history: dict[str, int] = defaultdict(int)
    runtime_verified = outbox_verified = rotation_verified = False

    if runtime_path.is_file():
        with _readonly_connection(runtime_path) as connection:
            tables = _table_names(connection)
            if "discovery_job_runtime" in tables:
                runtime_verified = True
                for row in _query(connection, "SELECT * FROM discovery_job_runtime"):
                    runtime[str(row["job_id"])] = dict(row)
            if "notification_outbox" in tables:
                outbox_verified = True
                for row in _query(
                    connection,
                    "SELECT entity_id,COUNT(*) AS n FROM notification_outbox GROUP BY entity_id",
                ):
                    outbox[str(row[0])] = int(row[1])
    if rotation_path.is_file():
        with _readonly_connection(rotation_path) as connection:
            tables = _table_names(connection)
            if "discovery_rotation_selections" in tables:
                rotation_verified = True
                for row in _query(
                    connection,
                    "SELECT job_id,COUNT(*) FROM discovery_rotation_selections GROUP BY job_id",
                ):
                    rotations[str(row[0])] = int(row[1])
            if "discovery_rotation_global_history" in tables:
                for row in _query(
                    connection,
                    "SELECT last_job_id,COUNT(*) FROM discovery_rotation_global_history "
                    "WHERE last_job_id IS NOT NULL GROUP BY last_job_id",
                ):
                    global_history[str(row[0])] = int(row[1])

    with _readonly_connection(incremental_path) as connection:
        tables = _table_names(connection)
        cache_map: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        if "discovery_amazon_cache" in tables:
            columns = _columns(connection, "discovery_amazon_cache")
            if "freshness_json" in columns:
                rows = _query(
                    connection,
                    """SELECT source_job_id,COUNT(*) AS catalog,
                              SUM(json_extract(freshness_json,'$.bsr_observed_at') IS NOT NULL) AS bsr,
                              SUM(json_extract(freshness_json,'$.pricing_observed_at') IS NOT NULL) AS pricing,
                              SUM(json_extract(freshness_json,'$.competition_observed_at') IS NOT NULL) AS competition
                       FROM discovery_amazon_cache GROUP BY source_job_id""",
                )
                for row in rows:
                    cache_map[str(row[0])].update({
                        "Catalog": int(row[1] or 0), "BSR": int(row[2] or 0),
                        "Pricing": int(row[3] or 0), "Competition": int(row[4] or 0),
                    })
            else:
                for row in _query(
                    connection,
                    "SELECT source_job_id,COUNT(*) FROM discovery_amazon_cache GROUP BY source_job_id",
                ):
                    cache_map[str(row[0])]["Catalog"] = int(row[1])
        if "discovery_amazon_fee_cache" in tables:
            for row in _query(
                connection,
                "SELECT source_job_id,COUNT(*) FROM discovery_amazon_fee_cache GROUP BY source_job_id",
            ):
                cache_map[str(row[0])]["Fee"] = int(row[1])
        result["cache_reference_map"] = {
            key: dict(value) for key, value in sorted(cache_map.items())
        }

        component_tables = [
            "discovery_job_items", "discovery_purchase_scenarios",
            "discovery_listing_classifications", "discovery_catalog_results",
            "discovery_listings", "discovery_observations",
            "discovery_combinations", "discovery_resource_events",
        ]
        jobs = _query(connection, "SELECT * FROM discovery_incremental_jobs ORDER BY created_at")
        snapshot_roots: set[str] = set()
        generation_roots: set[str] = set()
        for row in jobs:
            job = dict(row)
            job_id = str(job["job_id"])
            metadata = _json_dict(job.get("metadata_json"))
            snapshots, generations = _supplier_snapshot_roots(metadata)
            snapshot_roots.update(snapshots)
            generation_roots.update(generations)
            refs = dict(cache_map.get(job_id, {}))
            catalog_refs = refs.get("Catalog", 0)
            fee_refs = refs.get("Fee", 0)
            active = str(runtime.get(job_id, {}).get("status") or job.get("status") or "").casefold() not in {
                "completed", "cancelled", "canceled", "failed", "legacy_incompatible",
            }
            export_path = str(runtime.get(job_id, {}).get("export_path") or "")
            base_dependencies = {
                "active_job": active,
                "outbox_events": outbox.get(job_id, 0),
                "rotation_selections": rotations.get(job_id, 0),
                "global_history_last_job": global_history.get(job_id, 0),
                "export_path": export_path or None,
                "frozen_snapshots": sorted(snapshots),
                "frozen_generations": sorted(generations),
            }
            counts: dict[str, int] = {}
            components: list[dict[str, Any]] = []
            for table in component_tables:
                if table not in tables:
                    continue
                count = _count_by_job(connection, table, job_id)
                counts[table] = count
                estimated = _component_bytes(connection, table, job_id, count)
                catalog_component = table in {
                    "discovery_job_items", "discovery_listing_classifications",
                    "discovery_catalog_results", "discovery_listings",
                }
                fee_component = table == "discovery_observations"
                if active:
                    decision, reason = KEEP, "job is active/non-terminal"
                elif catalog_component and catalog_refs:
                    decision, reason = KEEP, "Amazon cache source references require source rows"
                elif fee_component and fee_refs:
                    decision, reason = KEEP, "Fee cache points to source observations"
                elif refs:
                    decision, reason = REPOINT, "job is partially protected; row-level compaction/repointing required"
                elif any((outbox.get(job_id), rotations.get(job_id), global_history.get(job_id), export_path, snapshots, generations)):
                    decision, reason = ARCHIVE, "historical/export/rotation/snapshot dependency remains"
                elif not (runtime_verified and outbox_verified and rotation_verified):
                    decision, reason = UNKNOWN, "one or more safety roots could not be verified"
                else:
                    decision, reason = DELETE, "zero known cache, serving, bootstrap, outbox, export, or rotation dependency"
                components.append({
                    "name": table, "decision": decision, "reason": reason,
                    "row_count": count, "estimated_bytes": estimated,
                })
            if active or catalog_refs:
                classification = "PROTECTED"
            elif fee_refs:
                classification = "PARTIALLY_PROTECTED"
            elif refs:
                classification = "PARTIALLY_PROTECTED"
            else:
                classification = "UNREFERENCED"
            result["jobs"].append({
                "job_id": job_id, "status": job.get("status"),
                "classification": classification, "references": refs,
                "dependencies": base_dependencies, "components": components,
                "estimated_bytes": sum(value["estimated_bytes"] for value in components),
            })
        result["snapshot_roots"] = sorted(snapshot_roots)
        result["generation_roots"] = sorted(generation_roots)
    return result


def qogita_snapshot_plan(
    supplier_path: str | Path = SUPPLIER_DATABASE,
    *, historical_snapshot_roots: Iterable[str] = (),
) -> dict[str, Any]:
    """Classify immutable Qogita serving snapshots using persisted roots."""
    roots = {str(value) for value in historical_snapshot_roots if value}
    result = {"snapshots": [], "active": [], "logical_reclaim_bytes": 0}
    with _readonly_connection(Path(supplier_path)) as connection:
        tables = _table_names(connection)
        required = {"qogita_serving_snapshots", "qogita_serving_active"}
        if not required.issubset(tables):
            return {**result, "unknown": "Qogita serving schema unavailable"}
        active = {
            str(row[0]) for row in _query(
                connection, "SELECT serving_generation_id FROM qogita_serving_active"
            )
        }
        result["active"] = sorted(active)
        duty_last: set[str] = set()
        if "qogita_bootstrap_duty_cycles" in tables:
            duty_last = {
                str(row[0]) for row in _query(
                    connection,
                    "SELECT last_serving_generation_id FROM qogita_bootstrap_duty_cycles "
                    "WHERE last_serving_generation_id IS NOT NULL",
                )
            }
        for row in _query(connection, "SELECT * FROM qogita_serving_snapshots ORDER BY created_at"):
            snapshot = dict(row)
            snapshot_id = str(snapshot["serving_generation_id"])
            memberships = int(snapshot.get("enriched_product_count") or 0)
            # Conservative logical estimate: row values + both membership indexes.
            estimated = memberships * 192 + 768
            if snapshot_id in active or snapshot_id in duty_last:
                classification, decision = "PROTECTED", KEEP
                reason = "active serving pointer or duty-cycle root"
            elif snapshot_id in roots:
                classification, decision = "HISTORICAL_REFERENCED", ARCHIVE
                reason = "frozen by a persisted Discovery job"
            else:
                classification, decision = "UNREFERENCED_HISTORICAL", DELETE
                reason = "no active, duty-cycle, or Discovery snapshot reference"
                result["logical_reclaim_bytes"] += estimated
            result["snapshots"].append({
                "snapshot_id": snapshot_id,
                "window": snapshot.get("bootstrap_window_number"),
                "source_generation_id": snapshot.get("source_generation_id"),
                "classification": classification, "decision": decision,
                "reason": reason, "membership_rows": memberships,
                "estimated_bytes": estimated,
            })
    return result


def supplier_generation_plan(
    supplier_path: str | Path = SUPPLIER_DATABASE,
    *, discovery_generation_roots: Iterable[str] = (),
) -> dict[str, Any]:
    """Classify supplier generations; uncertainty always resolves to KEEP."""
    discovery_roots = {str(value) for value in discovery_generation_roots if value}
    result = {"generations": [], "unknowns": []}
    with _readonly_connection(Path(supplier_path)) as connection:
        tables = _table_names(connection)
        if "supplier_catalog_runs" not in tables:
            return {**result, "unknown": "supplier catalog schema unavailable"}
        active = set()
        if "supplier_catalog_active_generations" in tables:
            active = {str(row[0]) for row in _query(connection, "SELECT run_id FROM supplier_catalog_active_generations")}
        serving_sources = set()
        if "qogita_serving_snapshots" in tables:
            serving_sources = {str(row[0]) for row in _query(connection, "SELECT DISTINCT source_generation_id FROM qogita_serving_snapshots")}
        bootstrap_sources = set()
        if "qogita_bootstrap_runs" in tables:
            bootstrap_sources = {str(row[0]) for row in _query(connection, "SELECT DISTINCT staging_run_id FROM qogita_bootstrap_runs")}
        incremental_sources: set[str] = set()
        source_reference_unknown = False
        for table in ("supplier_generation_product_refs", "supplier_generation_scenario_refs"):
            if table not in tables or "source_run_id" not in _columns(connection, table):
                continue
            try:
                incremental_sources.update(
                    str(row[0]) for row in _query(
                        connection,
                        f"SELECT DISTINCT source_run_id FROM {table} WHERE source_run_id IS NOT NULL",
                    )
                )
            except ReadBudgetExceeded:
                # One bounded pass is attempted. Repeated per-generation scans
                # would be an unacceptable production audit pattern.
                source_reference_unknown = True
        runs = _query(connection, "SELECT * FROM supplier_catalog_runs ORDER BY started_at")
        for row in runs:
            run = dict(row)
            run_id = str(run["run_id"])
            product_count = int(run.get("product_count") or 0)
            scenario_count = int(run.get("scenario_count") or 0)
            source_ref = run_id in incremental_sources
            unknown_reason = "bounded source-reference scan timed out" if source_reference_unknown else None
            roots = []
            if run_id in active:
                roots.append("active_generation")
            if run_id in serving_sources:
                roots.append("serving_snapshot_source")
            if run_id in bootstrap_sources:
                roots.append("bootstrap_run")
            if run_id in discovery_roots:
                roots.append("discovery_job")
            if source_ref:
                roots.append("incremental_generation_source")
            # Bounded approximation; physical bytes need dbstat/VACUUM maintenance.
            estimated = product_count * 1_280 + scenario_count * 1_024
            if roots:
                classification, decision = "PROTECTED", KEEP
                reason = "referenced by " + ", ".join(roots)
            elif unknown_reason:
                classification, decision, reason = "UNKNOWN", UNKNOWN, unknown_reason
                result["unknowns"].append({"run_id": run_id, "reason": reason})
            elif str(run.get("status") or "").casefold() in {"running", "staging", "pending"}:
                classification, decision, reason = "PROTECTED", KEEP, "generation is non-terminal"
            elif product_count == 0 and scenario_count == 0:
                classification, decision, reason = "UNREFERENCED", DELETE, "empty terminal generation with zero roots"
            else:
                classification, decision, reason = "UNREFERENCED", ARCHIVE, "payload-bearing generation has zero roots; archive review required"
            result["generations"].append({
                "run_id": run_id, "supplier": run.get("supplier"),
                "status": run.get("status"), "classification": classification,
                "decision": decision, "reason": reason, "roots": roots,
                "products": product_count, "scenarios": scenario_count,
                "estimated_bytes": estimated,
            })
    return result


def _registered_artifact_paths(runtime_path: Path, incremental_path: Path, jobs_dir: Path) -> tuple[set[Path], set[Path]]:
    exports: set[Path] = set()
    checkpoints: set[Path] = set()
    if not runtime_path.is_file():
        pass
    else:
        with _readonly_connection(runtime_path) as connection:
            if "discovery_job_runtime" in _table_names(connection):
                columns = _columns(connection, "discovery_job_runtime")
                selected = [name for name in ("export_path", "checkpoint_path") if name in columns]
                if selected:
                    for row in _query(connection, f"SELECT {','.join(selected)} FROM discovery_job_runtime"):
                        values = dict(zip(selected, row))
                        if values.get("export_path"):
                            exports.add(Path(str(values["export_path"])).expanduser().resolve())
                        if values.get("checkpoint_path"):
                            checkpoints.add(Path(str(values["checkpoint_path"])).expanduser().resolve())
    if incremental_path.is_file():
        with _readonly_connection(incremental_path) as connection:
            if "discovery_incremental_jobs" in _table_names(connection):
                columns = _columns(connection, "discovery_incremental_jobs")
                if "legacy_checkpoint_path" in columns:
                    for row in _query(
                        connection,
                        "SELECT legacy_checkpoint_path FROM discovery_incremental_jobs WHERE legacy_checkpoint_path IS NOT NULL",
                    ):
                        checkpoints.add(Path(str(row[0])).expanduser().resolve())
    # Compact state is authoritative export metadata for modern jobs. Files are
    # small; parsing is bounded and avoids treating the current technical export
    # as an unregistered backup.
    for state_path in jobs_dir.glob("*.state.json"):
        try:
            state = json.loads(state_path.read_text(encoding="utf-8"))
        except (OSError, ValueError, json.JSONDecodeError):
            continue
        for key in ("export_state", "operational_export", "technical_export"):
            value = state.get(key)
            if isinstance(value, dict) and value.get("path"):
                exports.add(Path(str(value["path"])).expanduser().resolve())
    return exports, checkpoints


def file_retention_plan(
    data_dir: str | Path = DATA_DIR,
    runtime_path: str | Path = RUNTIME_DATABASE,
    incremental_path: str | Path = DISCOVERY_DATABASE,
) -> dict[str, Any]:
    """Classify checkpoint/export files without reading workbook payloads."""
    data_dir = Path(data_dir)
    files: list[dict[str, Any]] = []
    jobs_dir = data_dir / "discovery_jobs"
    if not jobs_dir.is_dir():
        return {"files": [], "estimated_bytes": 0}
    registered, checkpoint_roots = _registered_artifact_paths(
        Path(runtime_path), Path(incremental_path), jobs_dir,
    )
    compact_jobs = {path.name.removesuffix(".state.json") for path in jobs_dir.glob("*.state.json")}
    unreadable_compact_jobs: set[str] = set()
    for state_path in jobs_dir.glob("*.state.json"):
        try:
            json.loads(state_path.read_text(encoding="utf-8"))
        except (OSError, ValueError, json.JSONDecodeError):
            unreadable_compact_jobs.add(state_path.name.removesuffix(".state.json"))
    for path in sorted(jobs_dir.iterdir()):
        if not path.is_file():
            continue
        resolved = path.resolve()
        size = _file_size(path)
        name = path.name
        job_id = name.split(".", 1)[0]
        if job_id in unreadable_compact_jobs:
            decision, reason, category = UNKNOWN, "authoritative compact metadata is unreadable", "unknown_artifact"
        elif name.endswith(".operational.xlsx"):
            decision, reason, category = KEEP, "operational export is the default current artifact", "operational_export"
        elif resolved in registered:
            decision, reason, category = KEEP, "export is registered by authoritative runtime metadata", "registered_export"
        elif name.endswith(".xlsx") and any(token in name for token in ("pre-", "legacy-", ".glowup_")):
            decision, reason, category = ARCHIVE, "diagnostic/compatibility backup; no current export registration", "backup_export"
        elif name.endswith(".xlsx"):
            decision, reason, category = ARCHIVE, "unregistered technical or legacy export requires review", "technical_or_legacy_export"
        elif name.endswith(".json") and not name.endswith(".state.json") and job_id in compact_jobs:
            decision, category = REPOINT, "legacy_checkpoint"
            reason = (
                "legacy checkpoint is still registered; metadata migration is required before removal"
                if resolved in checkpoint_roots
                else "compact state exists; explicit reader audit is required before removal"
            )
        elif name.endswith(".state.json"):
            decision, reason, category = KEEP, "compact authoritative state", "compact_state"
        else:
            decision, reason, category = ARCHIVE, "unregistered runtime artifact requires explicit review", "runtime_artifact"
        files.append({
            "path": str(path.relative_to(data_dir)), "size": size,
            "category": category, "decision": decision, "reason": reason,
        })
    return {"files": files, "estimated_bytes": sum(row["size"] for row in files)}


def build_storage_gc_dry_run(
    project_root: str | Path = PROJECT_ROOT,
    *, paths: dict[str, str | Path] | None = None,
) -> dict[str, Any]:
    """Return the complete dry-run graph.  This function performs zero writes."""
    project_root = Path(project_root).resolve()
    configured = {
        "incremental": project_root / "data/discovery_incremental.sqlite3",
        "runtime": project_root / "data/discovery_jobs.sqlite3",
        "rotation": project_root / "data/discovery_rotation.sqlite3",
        "supplier": project_root / "data/supplier_catalog.sqlite3",
        "data": project_root / "data",
    }
    configured.update({key: Path(value) for key, value in (paths or {}).items()})
    discovery = discovery_gc_plan(configured["incremental"], configured["runtime"], configured["rotation"])
    snapshots = qogita_snapshot_plan(configured["supplier"], historical_snapshot_roots=discovery["snapshot_roots"])
    generations = supplier_generation_plan(configured["supplier"], discovery_generation_roots=discovery["generation_roots"])
    files = file_retention_plan(configured["data"], configured["runtime"], configured["incremental"])
    buckets = defaultdict(int)
    for job in discovery["jobs"]:
        for component in job["components"]:
            buckets[component["decision"]] += component["estimated_bytes"]
    for snapshot in snapshots["snapshots"]:
        buckets[snapshot["decision"]] += snapshot["estimated_bytes"]
    for generation in generations["generations"]:
        buckets[generation["decision"]] += generation["estimated_bytes"]
    for artifact in files["files"]:
        buckets[artifact["decision"]] += artifact["size"]
    file_delete = sum(row["size"] for row in files["files"] if row["decision"] == DELETE)
    database_delete = buckets[DELETE] - file_delete
    logical = buckets[DELETE] + buckets[ARCHIVE]
    return {
        "kind": "REFERENCE-AWARE GC DRY RUN", "generated_at": _utc_now(),
        "mode": "dry-run", "writes_performed": 0,
        "discovery": discovery, "qogita_snapshots": snapshots,
        "supplier_generations": generations, "files": files,
        "summary": {
            "protected_bytes": buckets[KEEP],
            "archive_candidate_bytes": buckets[ARCHIVE],
            "delete_candidate_bytes": buckets[DELETE],
            "repoint_required_bytes": buckets[REPOINT],
            "unknown_keep_bytes": buckets[UNKNOWN],
            "logical_delete_candidate_bytes": buckets[DELETE],
            "logical_reclaim_including_archive_candidates_bytes": logical,
            "potential_reclaim_after_repoint_bytes": buckets[REPOINT],
            # Deleting DB rows only adds freelist space. File artifacts are the
            # only immediate physical reclaim. No deletion is executed here.
            "physical_reclaim_immediate_if_approved_bytes": file_delete,
            "physical_reclaim_after_future_maintenance_estimate": database_delete + file_delete,
            "actual_physical_reclaim_bytes": 0,
        },
    }


def _bounded_tree_size(root: Path, *, max_files: int = 50_000, seconds: float = 2.0) -> tuple[int | None, int, bool]:
    started = time.monotonic()
    total = 0
    count = 0
    stack = [root]
    while stack:
        if count >= max_files or time.monotonic() - started > seconds:
            return None, count, True
        current = stack.pop()
        try:
            for entry in os.scandir(current):
                if entry.is_symlink():
                    continue
                if entry.is_dir(follow_symlinks=False):
                    stack.append(Path(entry.path))
                elif entry.is_file(follow_symlinks=False):
                    count += 1
                    total += entry.stat(follow_symlinks=False).st_size
        except OSError:
            continue
    return total, count, False


def _run_text(command: list[str], runner: Callable[..., Any] = subprocess.run) -> str:
    try:
        result = runner(command, capture_output=True, text=True, timeout=3, check=False)
        return str(result.stdout or result.stderr or "")
    except (OSError, subprocess.SubprocessError):
        return ""


def _parse_swap(text: str) -> tuple[int | None, int | None]:
    match = re.search(r"total\s*=\s*([\d.]+)([MG])\s+used\s*=\s*([\d.]+)([MG])", text)
    if not match:
        return None, None
    def value(number: str, unit: str) -> int:
        return int(float(number) * (1024 ** (3 if unit == "G" else 2)))
    return value(match.group(3), match.group(4)), value(match.group(1), match.group(2))


def _vm_metrics(text: str) -> dict[str, int | None]:
    page_match = re.search(r"page size of (\d+) bytes", text)
    page_size = int(page_match.group(1)) if page_match else 4096
    values = {}
    for label, raw in re.findall(r"^([^:]+):\s+([\d.]+)\.?$", text, re.MULTILINE):
        values[label.strip()] = int(float(raw))
    compressor = values.get("Pages occupied by compressor")
    free_pages = sum(values.get(key, 0) for key in ("Pages free", "Pages speculative"))
    return {
        "compressor_bytes": compressor * page_size if compressor is not None else None,
        "free_memory_bytes": free_pages * page_size if values else None,
    }


def _top_processes(text: str, limit: int = 10) -> list[dict[str, Any]]:
    rows = []
    for line in text.splitlines():
        parts = line.strip().split(None, 2)
        if len(parts) < 3 or not parts[1].isdigit():
            continue
        rows.append({"pid": int(parts[0]), "rss_bytes": int(parts[1]) * 1024, "command": parts[2]})
    return sorted(rows, key=lambda row: row["rss_bytes"], reverse=True)[:limit]


def classify_disk(free_bytes: int) -> str:
    gib = free_bytes / (1024 ** 3)
    if gib < 15:
        return "CRITICAL"
    if gib < 25:
        return "RED"
    if gib <= 40:
        return "YELLOW"
    return "GREEN"


def classify_swap(used: int | None, total: int | None, delta: int | None = None) -> str:
    if used is None or total in (None, 0):
        return "UNKNOWN"
    ratio = used / total
    # A full swap file with unknown trend can be historical macOS allocation;
    # only classify RED when the monitor proves it is still growing.
    if ratio >= 0.90 and delta is not None and delta > 0:
        return "RED"
    if ratio >= 0.70:
        return "YELLOW"
    return "GREEN"


def storage_monitor_snapshot(
    project_root: str | Path = PROJECT_ROOT,
    *, previous: dict[str, Any] | None = None,
    runner: Callable[..., Any] = subprocess.run,
) -> dict[str, Any]:
    """Collect a bounded snapshot using file metadata and tiny aggregate rows."""
    root = Path(project_root).resolve()
    data = root / "data"
    usage = shutil.disk_usage(root)
    tree_size, files_seen, truncated = _bounded_tree_size(data)
    db_paths = {
        "supplier": data / "supplier_catalog.sqlite3",
        "discovery": data / "discovery_incremental.sqlite3",
        "rotation": data / "discovery_rotation.sqlite3",
    }
    db = {
        key: {"size": _file_size(path), "wal": _file_size(Path(str(path) + "-wal"))}
        for key, path in db_paths.items()
    }
    qogita = {"snapshot_count": None, "snapshot_membership_total": None, "enriched": None, "scenarios": None, "pending": None}
    if db_paths["supplier"].is_file():
        try:
            with _readonly_connection(db_paths["supplier"]) as connection:
                tables = _table_names(connection)
                if "qogita_serving_snapshots" in tables:
                    row = _query(connection, "SELECT COUNT(*),COALESCE(SUM(enriched_product_count),0) FROM qogita_serving_snapshots")[0]
                    qogita.update(snapshot_count=int(row[0]), snapshot_membership_total=int(row[1]))
                if "qogita_bootstrap_runs" in tables:
                    row = _query(connection, "SELECT products_attempted,offers_success,scenarios_written,target_count FROM qogita_bootstrap_runs ORDER BY updated_at DESC LIMIT 1")[0]
                    qogita.update(enriched=int(row[1]), scenarios=int(row[2]), pending=max(0, int(row[3]) - int(row[0])))
        except (sqlite3.Error, ReadBudgetExceeded, IndexError):
            qogita["read_status"] = "unavailable_keep"
    swap_text = _run_text(["/usr/sbin/sysctl", "vm.swapusage"], runner)
    swap_used, swap_total = _parse_swap(swap_text)
    previous_used = ((previous or {}).get("swap") or {}).get("used_bytes")
    swap_delta = swap_used - previous_used if swap_used is not None and previous_used is not None else None
    vm = _vm_metrics(_run_text(["vm_stat"], runner))
    pressure_text = _run_text(["memory_pressure", "-Q"], runner).strip()
    processes = _top_processes(_run_text(["ps", "-axo", "pid=,rss=,command="], runner))
    return {
        "timestamp": _utc_now(),
        "disk": {
            "total_bytes": usage.total, "used_bytes": usage.used,
            "free_bytes": usage.free, "status": classify_disk(usage.free),
        },
        "data": {"size_bytes": tree_size, "files_seen": files_seen, "scan_truncated": truncated},
        "databases": db, "qogita": qogita,
        "swap": {
            "used_bytes": swap_used, "total_bytes": swap_total,
            "delta_bytes": swap_delta,
            "status": classify_swap(swap_used, swap_total, swap_delta),
        },
        "memory": {**vm, "pressure": pressure_text or None},
        "top_process_rss": processes,
    }


def append_monitor_snapshot(path: str | Path, snapshot: dict[str, Any], *, max_records: int = MONITOR_MAX_RECORDS) -> None:
    """Persist a capped JSONL history.  Not called by production dry-run."""
    if max_records <= 0:
        raise ValueError("max_records must be positive")
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    records: list[str] = []
    if destination.is_file():
        records = [line for line in destination.read_text(encoding="utf-8").splitlines() if line.strip()]
    records.append(json.dumps(snapshot, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    records = records[-max_records:]
    temporary = destination.with_name(destination.name + ".tmp")
    temporary.write_text("\n".join(records) + "\n", encoding="utf-8")
    os.replace(temporary, destination)


def _main() -> int:
    parser = argparse.ArgumentParser(description="GlowUp-Scout read-only storage tooling")
    parser.add_argument("command", choices=("dry-run", "monitor"))
    parser.add_argument("--project-root", default=str(PROJECT_ROOT))
    arguments = parser.parse_args()
    payload = (
        build_storage_gc_dry_run(arguments.project_root)
        if arguments.command == "dry-run"
        else storage_monitor_snapshot(arguments.project_root)
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
