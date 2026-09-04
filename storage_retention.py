"""Manual, reference-aware Discovery retention executor.

Planning and preview are read-only.  Applying a plan requires both ``apply=True``
and an explicit confirmation phrase.  The executor never removes external
files, never runs VACUUM, and commits at most one FINAL_ONLY conversion per
SQLite transaction.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from discovery_incremental import (
    DiscoveryIncrementalStore,
    RETENTION_MODE_FINAL_ONLY,
    RETENTION_MODE_FULL,
    TERMINAL_SUMMARY_VERSION,
)
from storage_gc import (
    DB1E11_JOB_ID,
    PROJECT_ROOT,
    RETENTION_POLICY_VERSION,
    append_storage_audit_event,
    collect_storage_metrics,
    final_only_contract_eligibility,
    production_retention_plan,
)


EXECUTOR_VERSION = "storage_retention_executor_v1"
EXECUTION_PLAN_VERSION = "storage_retention_execution_plan_v1"
APPLY_CONFIRMATION = "APPLY_FINAL_ONLY"
DEFAULT_AUDIT_NAME = "retention-executor-audit.jsonl"
ACTIVE_OR_RESUMABLE_STATUSES = {
    "launching", "queued", "running", "computed", "export_pending",
    "export_running", "export_complete", "notification_pending",
    "waiting_retry", "resource_paused", "export_resource_paused", "resumable",
}

TABLE_SPECS = {
    "discovery_job_items": {
        "columns": (
            "sequence_no", "canonical_identifier", "identifier_type", "product_json",
            "catalog_status", "pricing_status", "fees_status", "terminal_status", "updated_at",
        ),
        "order": "sequence_no,canonical_identifier",
        "key": "identifier",
    },
    "discovery_purchase_scenarios": {
        "columns": ("canonical_identifier", "scenario_id", "scenario_json", "supplier"),
        "order": "canonical_identifier,scenario_id",
        "key": "identifier",
    },
    "discovery_catalog_results": {
        "columns": (
            "canonical_identifier", "catalog_status", "diagnostics_json", "updated_at",
        ),
        "order": "canonical_identifier",
        "key": "identifier",
    },
    "discovery_listings": {
        "columns": ("canonical_identifier", "asin", "listing_json", "updated_at"),
        "order": "canonical_identifier,asin",
        "key": "identifier",
    },
    "discovery_listing_classifications": {
        "columns": (
            "canonical_identifier", "asin", "marketplace_id", "path_hash",
            "classification_id", "parent_id", "depth", "display_name", "is_leaf",
        ),
        "order": "canonical_identifier,asin,marketplace_id,path_hash,classification_id",
        "key": "identifier",
    },
    "discovery_combinations": {
        "columns": (
            "combination_id", "canonical_identifier", "combination_json", "updated_at",
        ),
        "order": "canonical_identifier,combination_id",
        "key": "identifier",
    },
    "discovery_observations": {
        "columns": ("observation_id", "observation_json", "updated_at"),
        "order": "observation_id",
        "key": "observation",
    },
    "discovery_resource_events": {
        "columns": ("event_id", "level", "reason", "metrics_json", "observed_at"),
        "order": "event_id",
        "key": "none",
    },
}


class RetentionAbort(RuntimeError):
    """A fail-closed plan or verification failure."""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _canonical(value: Any) -> str:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str,
    )


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(str(value or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _open_readonly(path: Path) -> sqlite3.Connection:
    if not path.is_file():
        raise RetentionAbort(f"required_database_missing:{path.name}")
    connection = sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True, timeout=0.25)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    connection.execute("PRAGMA busy_timeout=250")
    return connection


def _value_bytes(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, bytes):
        return len(value)
    return len(str(value).encode("utf-8"))


def _row_payload(row: sqlite3.Row, columns: Iterable[str]) -> list[Any]:
    return [row[column] for column in columns]


def _collect_observation_ids(value: Any, result: set[str]) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            if key == "amazon_observation_id" and child:
                result.add(str(child))
            else:
                _collect_observation_ids(child, result)
    elif isinstance(value, list):
        for child in value:
            _collect_observation_ids(child, result)


def _table_manifest(
    connection: sqlite3.Connection, job_id: str, table: str,
    retained_identifiers: set[str], retained_observations: set[str],
) -> dict[str, Any]:
    spec = TABLE_SPECS[table]
    columns = tuple(spec["columns"])
    cursor = connection.execute(
        f"SELECT {','.join(columns)} FROM {table} WHERE job_id=? ORDER BY {spec['order']}",
        (job_id,),
    )
    all_digest = hashlib.sha256()
    retained_digest = hashlib.sha256()
    total_count = retained_count = total_bytes = retained_bytes = 0
    for row in cursor:
        payload = _row_payload(row, columns)
        encoded = (_canonical(payload) + "\n").encode("utf-8")
        row_bytes = sum(_value_bytes(value) for value in payload)
        all_digest.update(encoded)
        total_count += 1
        total_bytes += row_bytes
        if spec["key"] == "identifier":
            keep = str(row["canonical_identifier"]) in retained_identifiers
        elif spec["key"] == "observation":
            keep = str(row["observation_id"]) in retained_observations
        else:
            keep = False
        if keep:
            retained_digest.update(encoded)
            retained_count += 1
            retained_bytes += row_bytes
    return {
        "rows_full": total_count,
        "rows_retained": retained_count,
        "rows_remove": total_count - retained_count,
        "logical_bytes_full": total_bytes,
        "logical_bytes_retained": retained_bytes,
        "logical_bytes_remove": total_bytes - retained_bytes,
        "full_sha256": all_digest.hexdigest(),
        "retained_sha256": retained_digest.hexdigest(),
    }


def _incremental_manifest(connection: sqlite3.Connection, job_id: str) -> dict[str, Any]:
    job = connection.execute(
        "SELECT * FROM discovery_incremental_jobs WHERE job_id=?", (job_id,),
    ).fetchone()
    if not job:
        raise RetentionAbort(f"job_missing:{job_id}")
    metadata = _json_dict(job["metadata_json"])
    terminal = metadata.get("terminal_summary")
    terminal_hash = metadata.get("terminal_summary_sha256")
    valid_terminal = bool(
        isinstance(terminal, dict)
        and int(metadata.get("terminal_summary_version") or 0) == TERMINAL_SUMMARY_VERSION
        and terminal_hash == _digest(terminal)
    )
    full_summary_matches_terminal = False
    terminal_summary_mismatches: list[str] = []
    if valid_terminal:
        store = DiscoveryIncrementalStore(Path(connection.execute(
            "PRAGMA database_list"
        ).fetchone()[2]))
        # Historical terminal summaries may legitimately contain finalizer/UI
        # fields that the FULL database summary does not expose directly.  The
        # authoritative comparison is therefore the subset recomputable from
        # current intermediate rows, not a requirement that both dictionaries
        # have identical key sets.
        current_summary = store._terminal_summary_payload(
            job_id, connection=connection, state=None,
        )
        terminal_summary_mismatches = sorted(
            key for key, value in terminal.items()
            if key in current_summary
            and key not in {"retention_mode", "exact_replay_capable"}
            and current_summary.get(key) != value
        )
        full_summary_matches_terminal = not terminal_summary_mismatches
    final_rows = connection.execute(
        """SELECT canonical_identifier,product_json FROM discovery_job_items
           WHERE job_id=? AND json_extract(product_json,'$.is_final_result')=1
           ORDER BY sequence_no,canonical_identifier""",
        (job_id,),
    ).fetchall()
    retained_identifiers = {str(row["canonical_identifier"]) for row in final_rows}
    observation_ids: set[str] = set()
    required_scenarios: set[tuple[str, str]] = set()
    required_combinations: set[tuple[str, str]] = set()
    required_listings: set[tuple[str, str]] = set()
    for row in final_rows:
        identifier = str(row["canonical_identifier"])
        product = _json_dict(row["product_json"])
        _collect_observation_ids(product, observation_ids)
        recommended = product.get("recommended_combination")
        if isinstance(recommended, dict):
            scenario_id = recommended.get("scenario_id")
            combination_id = recommended.get("combination_id")
            asin = recommended.get("asin")
            if scenario_id:
                required_scenarios.add((identifier, str(scenario_id)))
            if combination_id:
                required_combinations.add((identifier, str(combination_id)))
            if asin:
                required_listings.add((identifier, str(asin)))
        scenario_id = product.get("best_purchase_scenario")
        if scenario_id and not isinstance(scenario_id, (dict, list)):
            required_scenarios.add((identifier, str(scenario_id)))

    for table, column in (
        ("discovery_listings", "listing_json"),
        ("discovery_combinations", "combination_json"),
    ):
        for row in connection.execute(
            f"SELECT {column} FROM {table} WHERE job_id=? AND canonical_identifier IN "
            "(SELECT canonical_identifier FROM discovery_job_items "
            "WHERE job_id=? AND json_extract(product_json,'$.is_final_result')=1)",
            (job_id, job_id),
        ):
            _collect_observation_ids(_json_dict(row[0]), observation_ids)

    observed_ids: set[str] = set()
    for row in connection.execute(
        "SELECT observation_id,observation_json FROM discovery_observations WHERE job_id=?",
        (job_id,),
    ):
        observation_id = str(row["observation_id"])
        observed_ids.add(observation_id)
        payload = _json_dict(row["observation_json"])
        canonical_identifier = str(payload.get("canonical_ean") or "")
        product_keys = {
            str(value) for value in (_json_dict(payload.get("diagnostics")).get("product_keys") or [])
        }
        if canonical_identifier in retained_identifiers or product_keys.intersection(retained_identifiers):
            observation_ids.add(observation_id)

    unresolved: list[str] = []
    missing_observations = sorted(observation_ids - observed_ids)
    if missing_observations:
        unresolved.append(f"missing_observations:{len(missing_observations)}")
    if valid_terminal and int(terminal.get("final_opportunity_count") or 0) != len(final_rows):
        unresolved.append("terminal_final_opportunity_count_mismatch")
    for identifier, scenario_id in sorted(required_scenarios):
        if not connection.execute(
            """SELECT 1 FROM discovery_purchase_scenarios
               WHERE job_id=? AND canonical_identifier=? AND scenario_id=?""",
            (job_id, identifier, scenario_id),
        ).fetchone():
            unresolved.append(f"missing_scenario:{identifier}:{scenario_id}")
    for identifier, combination_id in sorted(required_combinations):
        if not connection.execute(
            """SELECT 1 FROM discovery_combinations
               WHERE job_id=? AND canonical_identifier=? AND combination_id=?""",
            (job_id, identifier, combination_id),
        ).fetchone():
            unresolved.append(f"missing_combination:{identifier}:{combination_id}")
    for identifier, asin in sorted(required_listings):
        if not connection.execute(
            """SELECT 1 FROM discovery_listings
               WHERE job_id=? AND canonical_identifier=? AND asin=?""",
            (job_id, identifier, asin),
        ).fetchone():
            unresolved.append(f"missing_listing:{identifier}:{asin}")

    tables = {
        table: _table_manifest(
            connection, job_id, table, retained_identifiers, observation_ids,
        )
        for table in TABLE_SPECS
    }
    full_bytes = sum(value["logical_bytes_full"] for value in tables.values())
    retained_bytes = sum(value["logical_bytes_retained"] for value in tables.values())
    final_hashes = {
        "final_opportunities_sha256": tables["discovery_job_items"]["retained_sha256"],
        "economics_sha256": _digest({
            name: tables[name]["retained_sha256"]
            for name in (
                "discovery_job_items", "discovery_purchase_scenarios",
                "discovery_combinations",
            )
        }),
        "closure_sha256": _digest({
            name: value["retained_sha256"] for name, value in tables.items()
        }),
    }
    return {
        "job_id": job_id,
        "job_status": str(job["status"]),
        "retention_mode": str(metadata.get("retention_mode") or RETENTION_MODE_FULL).casefold(),
        "exact_replay_capable": bool(metadata.get("exact_replay_capable", True)),
        "terminal_summary_version": metadata.get("terminal_summary_version"),
        "terminal_summary_sha256": terminal_hash,
        "terminal_summary_valid": valid_terminal,
        "full_summary_matches_terminal": full_summary_matches_terminal,
        "terminal_summary_mismatches": terminal_summary_mismatches,
        "terminal_summary": terminal if isinstance(terminal, dict) else None,
        "final_identifier_count": len(retained_identifiers),
        "retained_identifiers": sorted(retained_identifiers),
        "retained_observation_ids": sorted(observation_ids),
        "unresolved_references": unresolved,
        "tables": tables,
        "logical_bytes_full": full_bytes,
        "logical_bytes_final_only": retained_bytes,
        "logical_bytes_remove": full_bytes - retained_bytes,
        **final_hashes,
    }


def _hash_query(connection: sqlite3.Connection, sql: str, parameters=()) -> dict[str, Any]:
    digest = hashlib.sha256()
    count = 0
    for row in connection.execute(sql, tuple(parameters)):
        digest.update((_canonical(list(row)) + "\n").encode("utf-8"))
        count += 1
    return {"count": count, "sha256": digest.hexdigest()}


def _file_manifest(path: str | None) -> dict[str, Any] | None:
    if not path:
        return None
    source = Path(path).expanduser().resolve()
    try:
        details = source.stat()
        digest = hashlib.sha256()
        with source.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return {
            "path": str(source), "present": True, "size_bytes": details.st_size,
            "mtime_ns": details.st_mtime_ns, "sha256": digest.hexdigest(),
        }
    except OSError:
        return {"path": str(source), "present": False}


class RetentionExecutor:
    def __init__(self, project_root: str | Path = PROJECT_ROOT):
        self.root = Path(project_root).resolve()
        data = self.root / "data"
        self.incremental_path = data / "discovery_incremental.sqlite3"
        self.runtime_path = data / "discovery_jobs.sqlite3"
        self.rotation_path = data / "discovery_rotation.sqlite3"
        self.audit_path = data / DEFAULT_AUDIT_NAME

    def _active_jobs(self) -> list[dict[str, Any]]:
        with _open_readonly(self.runtime_path) as connection:
            rows = connection.execute(
                "SELECT job_id,status,resumable,worker_pid FROM discovery_job_runtime"
            ).fetchall()
        return [
            dict(row) for row in rows
            if str(row["status"] or "").casefold() in ACTIVE_OR_RESUMABLE_STATUSES
            or bool(row["resumable"])
        ]

    def _reference_snapshot(
        self, job_id: str, *, incremental_connection: sqlite3.Connection | None = None,
    ) -> dict[str, Any]:
        owns_incremental = incremental_connection is None
        incremental = incremental_connection or _open_readonly(self.incremental_path)
        try:
            marker_row = incremental.execute(
                """SELECT * FROM discovery_amazon_cache_indexed_jobs
                   WHERE source_job_id=?""", (job_id,),
            ).fetchone()
            marker = dict(marker_row) if marker_row else None
            catalog_cache = _hash_query(
                incremental,
                """SELECT canonical_identifier,catalog_status,catalog_observed_at,
                          payload_schema_version,payload_sha256,materialized_at
                     FROM discovery_amazon_cache WHERE source_job_id=?
                     ORDER BY canonical_identifier""",
                (job_id,),
            )
            fee_cache = _hash_query(
                incremental,
                """SELECT fee_cache_key,observation_id,fee_status,fee_observed_at,
                          payload_schema_version,payload_sha256,materialized_at
                     FROM discovery_amazon_fee_cache WHERE source_job_id=?
                     ORDER BY fee_cache_key""",
                (job_id,),
            )
        finally:
            if owns_incremental:
                incremental.close()
        with _open_readonly(self.runtime_path) as runtime_connection:
            runtime_row = runtime_connection.execute(
                "SELECT * FROM discovery_job_runtime WHERE job_id=?", (job_id,),
            ).fetchone()
            outbox = _hash_query(
                runtime_connection,
                """SELECT event_type,channel,status,created_at,updated_at,attempted_at,
                          sent_at,attempt_count,provider_message_id,failure_reason,
                          attachment_name,attachment_size,attachment_status,attachment_error
                     FROM notification_outbox WHERE entity_id=?
                     ORDER BY event_type,channel,created_at""",
                (job_id,),
            )
            outbox_statuses = [
                str(row[0]) for row in runtime_connection.execute(
                    "SELECT status FROM notification_outbox WHERE entity_id=? ORDER BY status",
                    (job_id,),
                )
            ]
        with _open_readonly(self.rotation_path) as rotation_connection:
            rotation = _hash_query(
                rotation_connection,
                """SELECT scope_key,cycle_id,canonical_identifier,selected_at,status,
                          catalog_status,analyzed_at
                     FROM discovery_rotation_selections WHERE job_id=?
                     ORDER BY scope_key,cycle_id,canonical_identifier""",
                (job_id,),
            )
            rotation_summary = rotation_connection.execute(
                """SELECT scope_key,COUNT(*) total,
                          SUM(CASE WHEN status='analyzed' THEN 1 ELSE 0 END) analyzed
                     FROM discovery_rotation_selections WHERE job_id=? GROUP BY scope_key""",
                (job_id,),
            ).fetchone()
        runtime = dict(runtime_row) if runtime_row else None
        export_path = str(runtime.get("export_path") or "") if runtime else ""
        export_manifest = _file_manifest(export_path)
        known = bool(marker and runtime and rotation_summary)
        snapshot = {
            "known": known,
            "runtime": runtime,
            "outbox": outbox,
            "outbox_statuses": outbox_statuses,
            "rotation": rotation,
            "rotation_scope": str(rotation_summary["scope_key"]) if rotation_summary else None,
            "rotation_total": int(rotation_summary["total"] or 0) if rotation_summary else 0,
            "rotation_analyzed": int(rotation_summary["analyzed"] or 0) if rotation_summary else 0,
            "cache_marker": marker,
            "catalog_cache": catalog_cache,
            "fee_cache": fee_cache,
            "operational_export_path": export_path or None,
            "operational_export_present": bool(
                export_manifest and export_manifest.get("present")
            ),
            "operational_export_manifest": export_manifest,
        }
        snapshot["sha256"] = _digest(snapshot)
        return snapshot

    def _candidate_snapshot(self, planner_row: dict[str, Any]) -> dict[str, Any]:
        job_id = str(planner_row["job_id"])
        with _open_readonly(self.incremental_path) as connection:
            manifest = _incremental_manifest(connection, job_id)
        references = self._reference_snapshot(job_id)
        blockers = []
        if planner_row.get("classification") != "FINAL_ONLY_ELIGIBLE":
            blockers.append("planner_did_not_select_job")
        if job_id == DB1E11_JOB_ID:
            blockers.append("explicit_db1e11_exclusion")
        runtime = references.get("runtime") or {}
        marker = references.get("cache_marker") or {}
        contract = final_only_contract_eligibility(
            job_status=str(runtime.get("status") or manifest["job_status"]),
            terminal_summary_valid=manifest["terminal_summary_valid"],
            cache_verification_state=marker.get("verification_state"),
            outbox_statuses=references.get("outbox_statuses") or [],
            resumable=bool(runtime.get("resumable")),
        )
        blockers.extend(contract["blockers"])
        if manifest["retention_mode"] != RETENTION_MODE_FULL:
            blockers.append("storage_mode_not_full")
        if not manifest["full_summary_matches_terminal"]:
            blockers.append("full_summary_differs_from_terminal_summary")
        blockers.extend(manifest["unresolved_references"])
        if runtime.get("worker_pid") is not None:
            blockers.append("worker_pid_present")
        if not marker.get("materialization_version"):
            blockers.append("cache_materialization_version_unknown")
        if not references.get("rotation_total") or (
            references.get("rotation_total") != references.get("rotation_analyzed")
        ):
            blockers.append("rotation_not_committed")
        if not references.get("operational_export_present"):
            blockers.append("operational_export_missing")
        if not references.get("known"):
            blockers.append("reference_state_unknown")
        checkpoint = runtime.get("checkpoint_path")
        terminal = manifest.get("terminal_summary") or {}
        technical = terminal.get("technical_export")
        files = {
            "checkpoint": {
                "path": checkpoint, "classification": "REMOVABLE_CANDIDATE",
                "execution": False,
            } if checkpoint else None,
            "technical_export": {
                "value": technical, "classification": "ARCHIVE_CANDIDATE",
                "execution": False,
            } if technical else None,
            "operational_export": {
                "path": references.get("operational_export_path"),
                "classification": "KEEP", "execution": False,
            },
        }
        snapshot = {
            "job_id": job_id,
            "scope": planner_row.get("scope"),
            "completed_at": planner_row.get("completed_at"),
            "classification": planner_row.get("classification"),
            "expected_retention_mode": RETENTION_MODE_FULL,
            "expected_terminal_summary_sha256": manifest["terminal_summary_sha256"],
            "expected_terminal_summary_version": manifest["terminal_summary_version"],
            "expected_cache_marker": references.get("cache_marker"),
            "expected_reference_state_sha256": references["sha256"],
            "expected_estimated_reclaim_bytes": int(
                planner_row.get("estimated_logical_reclaim_bytes") or 0
            ),
            "manifest": manifest,
            "references": references,
            "external_files": files,
            "blockers": sorted(set(blockers)),
        }
        snapshot["eligible"] = not snapshot["blockers"]
        fingerprint_payload = {
            key: snapshot[key] for key in (
                "job_id", "scope", "completed_at", "classification",
                "expected_retention_mode", "expected_terminal_summary_sha256",
                "expected_terminal_summary_version", "expected_cache_marker",
                "expected_reference_state_sha256", "expected_estimated_reclaim_bytes",
                "manifest", "blockers",
            )
        }
        snapshot["state_fingerprint"] = _digest(fingerprint_payload)
        return snapshot

    def build_plan(
        self, watermark_state: str, *, job_ids: Iterable[str] | None = None,
    ) -> dict[str, Any]:
        planner = production_retention_plan(watermark_state, self.root)
        requested = {str(value) for value in (job_ids or []) if value}
        candidates = [
            row for row in planner["jobs"]
            if row.get("classification") == "FINAL_ONLY_ELIGIBLE"
            and (not requested or str(row.get("job_id")) in requested)
        ]
        missing = requested - {str(row.get("job_id")) for row in candidates}
        if missing:
            raise RetentionAbort("jobs_not_selected_by_planner:" + ",".join(sorted(missing)))
        plan = {
            "version": EXECUTION_PLAN_VERSION,
            "executor_version": EXECUTOR_VERSION,
            "planner_version": planner.get("version") or RETENTION_POLICY_VERSION,
            "generated_at": _now(),
            "watermark_state": str(watermark_state).upper(),
            "policy": planner.get("policy"),
            "inventory_reliable": bool(planner.get("inventory_reliable")),
            "inventory_errors": planner.get("inventory_errors") or [],
            "candidates": [self._candidate_snapshot(row) for row in candidates],
            "execution": False,
        }
        plan["plan_sha256"] = _digest(plan)
        return plan

    @staticmethod
    def validate_plan(plan: dict[str, Any]) -> None:
        if plan.get("version") != EXECUTION_PLAN_VERSION:
            raise RetentionAbort("unsupported_plan_version")
        expected = plan.get("plan_sha256")
        unsigned = {key: value for key, value in plan.items() if key != "plan_sha256"}
        if not expected or expected != _digest(unsigned):
            raise RetentionAbort("invalid_plan_sha256")
        if not plan.get("inventory_reliable") or plan.get("inventory_errors"):
            raise RetentionAbort("plan_inventory_unreliable")

    def preview(self, plan: dict[str, Any]) -> dict[str, Any]:
        self.validate_plan(plan)
        results = []
        for expected in plan.get("candidates") or []:
            current_planner = production_retention_plan(plan["watermark_state"], self.root)
            row = next(
                (value for value in current_planner["jobs"]
                 if value.get("job_id") == expected.get("job_id")), None,
            )
            if not row or row.get("classification") != "FINAL_ONLY_ELIGIBLE":
                results.append({
                    "job_id": expected.get("job_id"), "status": "STALE_PLAN",
                    "reason": "no_longer_selected_by_planner",
                })
                continue
            current = self._candidate_snapshot(row)
            stale = current["state_fingerprint"] != expected.get("state_fingerprint")
            results.append({
                **current,
                "status": "STALE_PLAN" if stale else (
                    "ELIGIBLE" if current["eligible"] else "KEEP_BLOCKED"
                ),
                "writes_performed": 0,
            })
        return {
            "mode": "dry-run", "plan_sha256": plan["plan_sha256"],
            "results": results, "writes_performed": 0,
            "delete_operations": 0, "vacuum_operations": 0,
            "external_file_operations": 0,
        }

    def _assert_no_active_jobs(self) -> None:
        active = self._active_jobs()
        if active:
            raise RetentionAbort(
                "active_or_resumable_discovery_present:"
                + ",".join(str(row["job_id"]) for row in active)
            )

    def apply_plan(
        self, plan: dict[str, Any], *, apply: bool = False,
        confirmed_job_ids: Iterable[str] = (), confirmation: str | None = None,
    ) -> dict[str, Any]:
        if not apply:
            return self.preview(plan)
        self.validate_plan(plan)
        candidates = plan.get("candidates") or []
        candidate_ids = {str(row.get("job_id")) for row in candidates}
        if confirmation != APPLY_CONFIRMATION or set(confirmed_job_ids) != candidate_ids:
            raise RetentionAbort("explicit_apply_confirmation_required_for_every_job")
        self._assert_no_active_jobs()
        results = []
        for expected in candidates:
            self._assert_no_active_jobs()
            results.append(self._apply_one(plan, expected))
        return {
            "mode": "apply", "plan_sha256": plan["plan_sha256"],
            "results": results,
            "writes_performed": sum(row["status"] == "APPLIED" for row in results),
            "vacuum_operations": 0, "external_file_operations": 0,
        }

    def _apply_one(self, plan: dict[str, Any], expected: dict[str, Any]) -> dict[str, Any]:
        job_id = str(expected["job_id"])
        started = time.monotonic()
        # Idempotent replay is recognized before planner eligibility is re-evaluated.
        with _open_readonly(self.incremental_path) as read_connection:
            row = read_connection.execute(
                "SELECT metadata_json FROM discovery_incremental_jobs WHERE job_id=?", (job_id,),
            ).fetchone()
            metadata = _json_dict(row[0]) if row else {}
        if str(metadata.get("retention_mode") or "full").casefold() == RETENTION_MODE_FINAL_ONLY:
            audit = metadata.get("retention_execution_audit") or {}
            if audit.get("plan_sha256") == plan["plan_sha256"]:
                return {"job_id": job_id, "status": "NOOP_ALREADY_APPLIED", "rows_removed": {}}
            raise RetentionAbort(f"job_already_final_only_with_different_plan:{job_id}")

        current_plan = production_retention_plan(plan["watermark_state"], self.root)
        planner_row = next(
            (value for value in current_plan["jobs"] if value.get("job_id") == job_id), None,
        )
        if not planner_row or planner_row.get("classification") != "FINAL_ONLY_ELIGIBLE":
            raise RetentionAbort(f"stale_plan_not_current_candidate:{job_id}")
        current = self._candidate_snapshot(planner_row)
        if not current["eligible"] or current["state_fingerprint"] != expected.get("state_fingerprint"):
            raise RetentionAbort(f"stale_or_ineligible_plan:{job_id}")

        connection = sqlite3.connect(self.incremental_path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=30000")
        pre_sizes = collect_storage_metrics(self.root)
        try:
            connection.execute("BEGIN IMMEDIATE")
            pre = _incremental_manifest(connection, job_id)
            if pre["terminal_summary_sha256"] != expected["expected_terminal_summary_sha256"]:
                raise RetentionAbort(f"terminal_summary_changed:{job_id}")
            if pre["closure_sha256"] != expected["manifest"]["closure_sha256"]:
                raise RetentionAbort(f"retained_closure_changed:{job_id}")
            transaction_references = self._reference_snapshot(
                job_id, incremental_connection=connection,
            )
            if transaction_references["sha256"] != expected["expected_reference_state_sha256"]:
                raise RetentionAbort(f"reference_state_changed:{job_id}")

            connection.execute(
                "CREATE TEMP TABLE retention_keep_identifiers(value TEXT PRIMARY KEY)"
            )
            connection.executemany(
                "INSERT INTO retention_keep_identifiers(value) VALUES(?)",
                ((value,) for value in pre["retained_identifiers"]),
            )
            connection.execute(
                "CREATE TEMP TABLE retention_keep_observations(value TEXT PRIMARY KEY)"
            )
            connection.executemany(
                "INSERT INTO retention_keep_observations(value) VALUES(?)",
                ((value,) for value in pre["retained_observation_ids"]),
            )
            removed: dict[str, int] = {}
            for table, spec in TABLE_SPECS.items():
                if spec["key"] == "identifier":
                    cursor = connection.execute(
                        f"""DELETE FROM {table} WHERE job_id=? AND canonical_identifier
                            NOT IN (SELECT value FROM retention_keep_identifiers)""",
                        (job_id,),
                    )
                elif spec["key"] == "observation":
                    cursor = connection.execute(
                        f"""DELETE FROM {table} WHERE job_id=? AND observation_id
                            NOT IN (SELECT value FROM retention_keep_observations)""",
                        (job_id,),
                    )
                else:
                    cursor = connection.execute(f"DELETE FROM {table} WHERE job_id=?", (job_id,))
                removed[table] = int(cursor.rowcount)
                if removed[table] != int(pre["tables"][table]["rows_remove"]):
                    raise RetentionAbort(f"removed_count_mismatch:{job_id}:{table}")

            job_row = connection.execute(
                "SELECT metadata_json FROM discovery_incremental_jobs WHERE job_id=?", (job_id,),
            ).fetchone()
            job_metadata = _json_dict(job_row[0])
            audit = {
                "applied_at": _now(), "executor_version": EXECUTOR_VERSION,
                "plan_version": plan["version"], "plan_sha256": plan["plan_sha256"],
                "actor": "manual", "rows_removed": removed,
                "logical_reclaim_estimate": pre["logical_bytes_remove"],
                "planner_reclaim_estimate": expected["expected_estimated_reclaim_bytes"],
                "pre_hashes": {
                    "closure": pre["closure_sha256"],
                    "final_opportunities": pre["final_opportunities_sha256"],
                    "economics": pre["economics_sha256"],
                    "references": transaction_references["sha256"],
                    "tables_full": {
                        name: value["full_sha256"]
                        for name, value in pre["tables"].items()
                    },
                    "tables_retained": {
                        name: value["retained_sha256"]
                        for name, value in pre["tables"].items()
                    },
                },
            }
            job_metadata.update({
                "retention_mode": RETENTION_MODE_FINAL_ONLY,
                "exact_replay_capable": False,
                "retention_execution_audit": audit,
            })
            connection.execute(
                "UPDATE discovery_incremental_jobs SET metadata_json=?,updated_at=? WHERE job_id=?",
                (_canonical(job_metadata), audit["applied_at"], job_id),
            )

            post = _incremental_manifest(connection, job_id)
            for table in TABLE_SPECS:
                if post["tables"][table]["full_sha256"] != pre["tables"][table]["retained_sha256"]:
                    raise RetentionAbort(f"post_closure_mismatch:{job_id}:{table}")
            if post["final_opportunities_sha256"] != pre["final_opportunities_sha256"]:
                raise RetentionAbort(f"final_opportunities_changed:{job_id}")
            if post["economics_sha256"] != pre["economics_sha256"]:
                raise RetentionAbort(f"economics_changed:{job_id}")
            if post["terminal_summary_sha256"] != pre["terminal_summary_sha256"]:
                raise RetentionAbort(f"terminal_summary_changed_post_pruning:{job_id}")

            store = DiscoveryIncrementalStore(self.incremental_path)
            final_summary = store.summary(job_id, connection=connection)
            terminal = pre["terminal_summary"] or {}
            for key, value in terminal.items():
                if key in {"retention_mode", "exact_replay_capable"}:
                    continue
                if final_summary.get(key) != value:
                    raise RetentionAbort(f"final_only_summary_mismatch:{job_id}:{key}")
            if final_summary.get("retention_mode") != RETENTION_MODE_FINAL_ONLY:
                raise RetentionAbort(f"final_only_mode_not_visible:{job_id}")
            if final_summary.get("exact_replay_capable") is not False:
                raise RetentionAbort(f"exact_replay_still_enabled:{job_id}")

            post_references = self._reference_snapshot(
                job_id, incremental_connection=connection,
            )
            if post_references["sha256"] != transaction_references["sha256"]:
                raise RetentionAbort(f"cache_rotation_or_outbox_changed:{job_id}")
            audit["post_hashes"] = {
                "closure": post["closure_sha256"],
                "final_opportunities": post["final_opportunities_sha256"],
                "economics": post["economics_sha256"],
                "references": post_references["sha256"],
                "tables": {
                    name: value["full_sha256"]
                    for name, value in post["tables"].items()
                },
            }
            job_metadata["retention_execution_audit"] = audit
            connection.execute(
                "UPDATE discovery_incremental_jobs SET metadata_json=? WHERE job_id=?",
                (_canonical(job_metadata), job_id),
            )
            connection.commit()
        except BaseException as error:
            connection.rollback()
            append_storage_audit_event({
                "event": "retention_execution", "actor": "manual", "job_id": job_id,
                "plan_sha256": plan.get("plan_sha256"), "result": "rollback",
                "error": f"{type(error).__name__}:{error}",
                "elapsed_seconds": time.monotonic() - started,
                "retention_execution": True,
            }, path=self.audit_path)
            raise
        finally:
            connection.close()
        post_sizes = collect_storage_metrics(self.root)
        result = {
            "job_id": job_id, "status": "APPLIED", "rows_removed": removed,
            "logical_rows_removed": sum(removed.values()),
            "logical_bytes_removed": pre["logical_bytes_remove"],
            "database_bytes_before": (
                pre_sizes.get("databases", {}).get("discovery", {}).get("file_size_bytes")
            ),
            "database_bytes_after": (
                post_sizes.get("databases", {}).get("discovery", {}).get("file_size_bytes")
            ),
            "freelist_bytes_before": pre_sizes.get("discovery_sqlite_freelist_bytes"),
            "freelist_bytes_after": post_sizes.get("discovery_sqlite_freelist_bytes"),
            "filesystem_free_before": pre_sizes.get("filesystem_free_bytes"),
            "filesystem_free_after": post_sizes.get("filesystem_free_bytes"),
            "wal_bytes_after": (
                post_sizes.get("databases", {}).get("discovery", {}).get("wal_bytes")
            ),
            "wal_peak_observed_bytes": max(
                int(pre_sizes.get("databases", {}).get("discovery", {}).get("wal_bytes") or 0),
                int(post_sizes.get("databases", {}).get("discovery", {}).get("wal_bytes") or 0),
            ),
            "vacuum_operations": 0, "external_file_operations": 0,
        }
        append_storage_audit_event({
            "event": "retention_execution", "actor": "manual", "job_id": job_id,
            "plan_sha256": plan["plan_sha256"], "result": "applied",
            "rows_removed": removed, "logical_bytes_removed": pre["logical_bytes_remove"],
            "elapsed_seconds": time.monotonic() - started,
            "storage_before": pre_sizes, "storage_after": post_sizes,
            "retention_execution": True,
        }, path=self.audit_path)
        return result


def _load_plan(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise RetentionAbort("plan_must_be_a_json_object")
    return value


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Manual reference-aware Discovery retention")
    parser.add_argument("--project-root", default=str(PROJECT_ROOT))
    subparsers = parser.add_subparsers(dest="command", required=True)
    plan_parser = subparsers.add_parser("plan", help="Generate a read-only execution plan")
    plan_parser.add_argument("--watermark", required=True, choices=(
        "NORMAL", "PREVENTIVE", "PRESSURE", "CRITICAL", "EMERGENCY",
    ))
    plan_parser.add_argument("--job", action="append", default=[])
    plan_parser.add_argument("--output", type=Path)
    execute_parser = subparsers.add_parser("execute", help="Preview a saved plan by default")
    execute_parser.add_argument("--plan", type=Path, required=True)
    execute_parser.add_argument("--apply", action="store_true")
    execute_parser.add_argument("--confirm", choices=(APPLY_CONFIRMATION,))
    execute_parser.add_argument("--confirm-job", action="append", default=[])
    args = parser.parse_args(argv)
    executor = RetentionExecutor(args.project_root)
    if args.command == "plan":
        result = executor.build_plan(args.watermark, job_ids=args.job)
        serialized = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True)
        if args.output:
            args.output.write_text(serialized + "\n", encoding="utf-8")
        print(serialized)
        return 0
    plan = _load_plan(args.plan)
    result = executor.apply_plan(
        plan, apply=args.apply, confirmed_job_ids=args.confirm_job,
        confirmation=args.confirm,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
