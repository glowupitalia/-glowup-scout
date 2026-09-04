"""Reference-aware storage audit and bounded host storage monitoring.

The module deliberately implements *analysis only*.  There is no delete,
archive, repoint, VACUUM, or checkpoint operation here.  Production databases
are opened through SQLite ``mode=ro`` with ``query_only`` enabled and every
potentially non-trivial query has a deadline.
"""

from __future__ import annotations

import argparse
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
