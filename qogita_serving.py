"""Restart-safe Qogita duty cycles and immutable partial serving snapshots."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from supplier_catalog import DEFAULT_DATABASE_PATH, canonical_gtin14, json_dumps, utc_now


RUN_WINDOW_SECONDS = 4 * 60 * 60
REST_WINDOW_SECONDS = 3 * 60 * 60


SCHEMA = """
CREATE TABLE IF NOT EXISTS qogita_bootstrap_duty_cycles (
    bootstrap_run_id TEXT PRIMARY KEY,
    state TEXT NOT NULL,
    run_window_seconds INTEGER NOT NULL,
    rest_window_seconds INTEGER NOT NULL,
    current_window_started_at TEXT,
    current_window_deadline TEXT,
    last_window_completed_at TEXT,
    rest_until TEXT,
    window_number INTEGER NOT NULL DEFAULT 0,
    last_serving_generation_id TEXT,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (bootstrap_run_id) REFERENCES qogita_bootstrap_runs(bootstrap_run_id)
);

CREATE TABLE IF NOT EXISTS qogita_serving_snapshots (
    serving_generation_id TEXT PRIMARY KEY,
    supplier TEXT NOT NULL CHECK (supplier='qogita'),
    source_generation_id TEXT NOT NULL,
    bootstrap_run_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    bootstrap_window_number INTEGER NOT NULL,
    status TEXT NOT NULL,
    product_catalog_count INTEGER NOT NULL,
    enriched_product_count INTEGER NOT NULL,
    usable_identifier_count INTEGER NOT NULL,
    scenario_count INTEGER NOT NULL,
    pending_count INTEGER NOT NULL,
    failed_count INTEGER NOT NULL,
    coverage_percent REAL NOT NULL,
    last_enrichment_at TEXT,
    product_catalog_coverage_type TEXT NOT NULL,
    product_catalog_coverage_complete INTEGER NOT NULL,
    scenario_enrichment_status TEXT NOT NULL,
    bootstrap_state TEXT NOT NULL,
    diagnostics_json TEXT NOT NULL DEFAULT '{}',
    FOREIGN KEY (source_generation_id) REFERENCES supplier_catalog_runs(run_id),
    FOREIGN KEY (bootstrap_run_id) REFERENCES qogita_bootstrap_runs(bootstrap_run_id)
);

CREATE TABLE IF NOT EXISTS qogita_serving_memberships (
    serving_generation_id TEXT NOT NULL,
    canonical_product_key TEXT NOT NULL,
    scenario_count INTEGER NOT NULL,
    offer_tier_observed_at TEXT,
    PRIMARY KEY (serving_generation_id, canonical_product_key),
    FOREIGN KEY (serving_generation_id)
        REFERENCES qogita_serving_snapshots(serving_generation_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_qogita_serving_membership_product
ON qogita_serving_memberships(canonical_product_key, serving_generation_id);

CREATE TABLE IF NOT EXISTS qogita_serving_active (
    supplier TEXT PRIMARY KEY CHECK (supplier='qogita'),
    serving_generation_id TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (serving_generation_id)
        REFERENCES qogita_serving_snapshots(serving_generation_id)
);
"""


def _connect(path: str | Path) -> sqlite3.Connection:
    absolute = Path(path).expanduser().resolve()
    absolute.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(absolute, timeout=60)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.execute("PRAGMA busy_timeout=60000")
    return connection


def _timestamp(value: str | datetime | None = None) -> str:
    if value is None:
        return utc_now()
    if isinstance(value, datetime):
        value = value.astimezone(timezone.utc).isoformat()
    return str(value).replace("+00:00", "Z")


def _datetime(value: str) -> datetime:
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)


class QogitaServingStore:
    """Owns the serving pointer without changing Qogita latest_success."""

    def __init__(self, path: str | Path = DEFAULT_DATABASE_PATH):
        self.path = Path(path).expanduser().resolve()

    def initialize(self) -> None:
        with _connect(self.path) as connection:
            connection.executescript(SCHEMA)

    def duty_state(self, bootstrap_run_id: str) -> dict[str, Any] | None:
        self.initialize()
        with _connect(self.path) as connection:
            row = connection.execute(
                "SELECT * FROM qogita_bootstrap_duty_cycles WHERE bootstrap_run_id=?",
                (bootstrap_run_id,),
            ).fetchone()
            return dict(row) if row else None

    def ensure_running_window(
        self, bootstrap_run_id: str, *, now: str | datetime | None = None,
        run_window_seconds: int = RUN_WINDOW_SECONDS,
        rest_window_seconds: int = REST_WINDOW_SECONDS,
    ) -> dict[str, Any]:
        """Create/resume a window; a persisted REST is never shortened."""
        self.initialize()
        now_text = _timestamp(now)
        now_dt = _datetime(now_text)
        with _connect(self.path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            bootstrap = connection.execute(
                "SELECT status FROM qogita_bootstrap_runs WHERE bootstrap_run_id=?",
                (bootstrap_run_id,),
            ).fetchone()
            if not bootstrap:
                raise ValueError("Bootstrap run not found")
            row = connection.execute(
                "SELECT * FROM qogita_bootstrap_duty_cycles WHERE bootstrap_run_id=?",
                (bootstrap_run_id,),
            ).fetchone()
            if row and row["state"] == "resting" and row["rest_until"]:
                if now_dt < _datetime(row["rest_until"]):
                    connection.commit()
                    return dict(row)
            if row and row["state"] in {"completed", "auto_stopped", "checkpointing"}:
                connection.commit()
                return dict(row)
            if row and row["state"] == "running" and row["current_window_deadline"]:
                connection.commit()
                return dict(row)
            window_number = int(row["window_number"] if row else 0) + 1
            deadline = _timestamp(now_dt + timedelta(seconds=int(run_window_seconds)))
            connection.execute(
                """INSERT INTO qogita_bootstrap_duty_cycles (
                       bootstrap_run_id,state,run_window_seconds,rest_window_seconds,
                       current_window_started_at,current_window_deadline,window_number,updated_at
                   ) VALUES (?,'running',?,?,?,?,?,?)
                   ON CONFLICT(bootstrap_run_id) DO UPDATE SET
                       state='running',run_window_seconds=excluded.run_window_seconds,
                       rest_window_seconds=excluded.rest_window_seconds,
                       current_window_started_at=excluded.current_window_started_at,
                       current_window_deadline=excluded.current_window_deadline,
                       rest_until=NULL,window_number=excluded.window_number,
                       updated_at=excluded.updated_at""",
                (bootstrap_run_id, int(run_window_seconds), int(rest_window_seconds),
                 now_text, deadline, window_number, now_text),
            )
            connection.commit()
        return self.duty_state(bootstrap_run_id)

    def mark_checkpointing(self, bootstrap_run_id: str, *, now=None) -> dict[str, Any]:
        with _connect(self.path) as connection:
            connection.execute(
                """UPDATE qogita_bootstrap_duty_cycles SET state='checkpointing',updated_at=?
                   WHERE bootstrap_run_id=?""",
                (_timestamp(now), bootstrap_run_id),
            )
            connection.commit()
        return self.duty_state(bootstrap_run_id)

    def begin_rest(
        self, bootstrap_run_id: str, *, serving_generation_id: str,
        now: str | datetime | None = None,
    ) -> dict[str, Any]:
        now_text = _timestamp(now)
        with _connect(self.path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT rest_window_seconds FROM qogita_bootstrap_duty_cycles WHERE bootstrap_run_id=?",
                (bootstrap_run_id,),
            ).fetchone()
            if not row:
                raise ValueError("Duty cycle not found")
            rest_until = _timestamp(
                _datetime(now_text) + timedelta(seconds=int(row["rest_window_seconds"]))
            )
            connection.execute(
                """UPDATE qogita_bootstrap_duty_cycles SET state='resting',
                       last_window_completed_at=?,rest_until=?,last_serving_generation_id=?,
                       updated_at=? WHERE bootstrap_run_id=?""",
                (now_text, rest_until, serving_generation_id, now_text, bootstrap_run_id),
            )
            connection.commit()
        return self.duty_state(bootstrap_run_id)

    def mark_auto_stopped(self, bootstrap_run_id: str, *, now=None) -> dict[str, Any]:
        with _connect(self.path) as connection:
            connection.execute(
                """UPDATE qogita_bootstrap_duty_cycles SET state='auto_stopped',updated_at=?
                   WHERE bootstrap_run_id=?""",
                (_timestamp(now), bootstrap_run_id),
            )
            connection.commit()
        return self.duty_state(bootstrap_run_id)

    def recover_auto_stopped_window(
        self, bootstrap_run_id: str, *, expected_serving_generation_id: str,
        now: str | datetime | None = None,
    ) -> dict[str, Any]:
        """Close an interrupted window without publishing a serving snapshot.

        Authoritative product/scenario reconciliation is performed by the
        bootstrap store first. This transition only records the normal REST
        boundary so the next invocation opens a fresh numbered window.
        """
        self.initialize()
        now_text = _timestamp(now)
        connection = _connect(self.path)
        try:
            connection.execute("BEGIN IMMEDIATE")
            run = connection.execute(
                """SELECT status,stop_reason,health_json FROM qogita_bootstrap_runs
                   WHERE bootstrap_run_id=? AND run_mode='production'""",
                (bootstrap_run_id,),
            ).fetchone()
            duty = connection.execute(
                """SELECT * FROM qogita_bootstrap_duty_cycles
                   WHERE bootstrap_run_id=?""",
                (bootstrap_run_id,),
            ).fetchone()
            active = connection.execute(
                "SELECT serving_generation_id FROM qogita_serving_active WHERE supplier='qogita'"
            ).fetchone()
            claims = connection.execute(
                """SELECT COUNT(*) FROM qogita_bootstrap_products
                   WHERE bootstrap_run_id=? AND worker_id IS NOT NULL""",
                (bootstrap_run_id,),
            ).fetchone()[0]
            if not run or not duty:
                raise ValueError("Qogita production recovery state is missing")
            if run["status"] != "auto_stopped" or duty["state"] != "auto_stopped":
                raise ValueError("Qogita production bootstrap is not auto-stopped")
            if not active or active["serving_generation_id"] != expected_serving_generation_id:
                raise RuntimeError("Qogita active serving snapshot changed during recovery")
            if int(claims or 0):
                raise RuntimeError("Qogita recovery requires zero active claims")
            deadline = _datetime(duty["current_window_deadline"])
            rest_until = _timestamp(
                deadline + timedelta(seconds=int(duty["rest_window_seconds"]))
            )
            health = json.loads(run["health_json"] or "{}")
            health["interrupted_window_recovery"] = {
                "window_number": int(duty["window_number"]),
                "previous_stop_reason": run["stop_reason"],
                "reconciled_at": now_text,
                "serving_generation_id_preserved": expected_serving_generation_id,
                "next_action": "open_next_window_after_rest",
            }
            connection.execute(
                """UPDATE qogita_bootstrap_runs SET status='running',stop_reason=NULL,
                          health_json=?,updated_at=? WHERE bootstrap_run_id=?""",
                (json_dumps(health), now_text, bootstrap_run_id),
            )
            connection.execute(
                """UPDATE qogita_bootstrap_duty_cycles SET state='resting',
                          last_window_completed_at=?,rest_until=?,updated_at=?
                   WHERE bootstrap_run_id=?""",
                (duty["current_window_deadline"], rest_until, now_text, bootstrap_run_id),
            )
            connection.commit()
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()
        result = self.duty_state(bootstrap_run_id)
        result["active_serving_generation_id"] = expected_serving_generation_id
        return result

    def mark_completed(
        self, bootstrap_run_id: str, *, serving_generation_id: str, now=None,
    ) -> dict[str, Any]:
        now_text = _timestamp(now)
        with _connect(self.path) as connection:
            connection.execute(
                """UPDATE qogita_bootstrap_duty_cycles SET state='completed',
                       last_window_completed_at=?,rest_until=NULL,
                       last_serving_generation_id=?,updated_at=? WHERE bootstrap_run_id=?""",
                (now_text, serving_generation_id, now_text, bootstrap_run_id),
            )
            connection.commit()
        return self.duty_state(bootstrap_run_id)

    def build_snapshot(
        self, bootstrap_run_id: str, *, window_number: int,
        bootstrap_state: str, now: str | datetime | None = None,
        serving_generation_id: str | None = None,
    ) -> dict[str, Any]:
        """Validate eligible rows and atomically switch only the serving pointer."""
        self.initialize()
        serving_generation_id = serving_generation_id or uuid4().hex
        created_at = _timestamp(now)
        connection = _connect(self.path)
        try:
            connection.execute("BEGIN IMMEDIATE")
            run = connection.execute(
                """SELECT bootstrap.*,source.product_count,source.product_catalog_coverage_type,
                          source.product_catalog_coverage_complete
                     FROM qogita_bootstrap_runs bootstrap
                     JOIN supplier_catalog_runs source ON source.run_id=bootstrap.staging_run_id
                    WHERE bootstrap.bootstrap_run_id=? AND bootstrap.run_mode='production'""",
                (bootstrap_run_id,),
            ).fetchone()
            if not run:
                raise ValueError("Production bootstrap not found")
            source_generation_id = run["staging_run_id"]
            eligible = connection.execute(
                """SELECT selected.canonical_product_key,selected.scenario_count,
                          product.offer_tier_observed_at,product.canonical_gtin
                     FROM qogita_bootstrap_products selected
                     JOIN supplier_catalog_products product
                       ON product.run_id=selected.staging_run_id
                      AND product.canonical_product_key=selected.canonical_product_key
                    WHERE selected.bootstrap_run_id=? AND selected.status='enriched'
                      AND selected.variant_fid IS NOT NULL AND selected.variant_fid<>''
                      AND product.enrichment_status IN ('enriched','carried_forward')""",
                (bootstrap_run_id,),
            ).fetchall()
            valid = [row for row in eligible if canonical_gtin14(row["canonical_gtin"])]
            if len(valid) != len(eligible):
                raise RuntimeError("Serving snapshot contains invalid canonical GTIN")
            expected_scenarios = sum(int(row["scenario_count"] or 0) for row in valid)
            actual_scenarios = connection.execute(
                """SELECT COUNT(*) FROM supplier_catalog_scenarios scenario
                     JOIN qogita_bootstrap_products selected
                       ON selected.staging_run_id=scenario.run_id
                      AND selected.canonical_product_key=scenario.canonical_product_key
                    WHERE selected.bootstrap_run_id=? AND selected.status='enriched'""",
                (bootstrap_run_id,),
            ).fetchone()[0]
            if int(actual_scenarios) != expected_scenarios:
                raise RuntimeError("Serving snapshot scenario membership is inconsistent")
            counts = dict(connection.execute(
                """SELECT COUNT(*) total,
                          SUM(status IN ('pending','fid_resolved','resolver_retryable','offers_retryable')) pending,
                          SUM(status IN ('resolver_permanent','offers_permanent','parsing_failure')) failed
                     FROM qogita_bootstrap_products WHERE bootstrap_run_id=?""",
                (bootstrap_run_id,),
            ).fetchone())
            usable = connection.execute(
                """SELECT COUNT(DISTINCT scenario.canonical_ean)
                     FROM supplier_catalog_scenarios scenario
                     JOIN qogita_bootstrap_products selected
                       ON selected.staging_run_id=scenario.run_id
                      AND selected.canonical_product_key=scenario.canonical_product_key
                    WHERE selected.bootstrap_run_id=? AND selected.status='enriched'
                      AND scenario.canonical_ean IS NOT NULL""",
                (bootstrap_run_id,),
            ).fetchone()[0]
            last_enrichment = max(
                (row["offer_tier_observed_at"] for row in valid
                 if row["offer_tier_observed_at"]), default=None,
            )
            total = int(counts["total"] or 0)
            enriched = len(valid)
            if total <= 0 or enriched <= 0:
                raise RuntimeError("Serving snapshot has no verified enriched products")
            coverage = (enriched / total * 100.0) if total else 0.0
            scenario_status = (
                "full" if total and enriched == total and not int(counts["failed"] or 0)
                else "partial"
            )
            connection.execute(
                """INSERT INTO qogita_serving_snapshots (
                       serving_generation_id,supplier,source_generation_id,bootstrap_run_id,
                       created_at,bootstrap_window_number,status,product_catalog_count,
                       enriched_product_count,usable_identifier_count,scenario_count,
                       pending_count,failed_count,coverage_percent,last_enrichment_at,
                       product_catalog_coverage_type,product_catalog_coverage_complete,
                       scenario_enrichment_status,bootstrap_state,diagnostics_json
                   ) VALUES (?,'qogita',?,?,?,?,'valid',?,?,?,?,?,?,?,?,?,1,?,?,?)""",
                (serving_generation_id, source_generation_id, bootstrap_run_id,
                 created_at, int(window_number), total, enriched, int(usable or 0),
                 int(actual_scenarios), int(counts["pending"] or 0),
                 int(counts["failed"] or 0), coverage, last_enrichment,
                 run["product_catalog_coverage_type"], scenario_status, bootstrap_state,
                 json_dumps({"membership_model": "immutable_source_reference_v1"})),
            )
            connection.executemany(
                """INSERT INTO qogita_serving_memberships (
                       serving_generation_id,canonical_product_key,scenario_count,
                       offer_tier_observed_at) VALUES (?,?,?,?)""",
                ((serving_generation_id, row["canonical_product_key"],
                  int(row["scenario_count"] or 0), row["offer_tier_observed_at"])
                 for row in valid),
            )
            connection.execute(
                """INSERT INTO qogita_serving_active(supplier,serving_generation_id,updated_at)
                   VALUES ('qogita',?,?) ON CONFLICT(supplier) DO UPDATE SET
                   serving_generation_id=excluded.serving_generation_id,
                   updated_at=excluded.updated_at""",
                (serving_generation_id, created_at),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return self.active_snapshot()

    def active_snapshot(self) -> dict[str, Any] | None:
        self.initialize()
        with _connect(self.path) as connection:
            row = connection.execute(
                """SELECT snapshot.* FROM qogita_serving_active active
                     JOIN qogita_serving_snapshots snapshot
                       ON snapshot.serving_generation_id=active.serving_generation_id
                    WHERE active.supplier='qogita' AND snapshot.status='valid'"""
            ).fetchone()
            if not row:
                return None
            result = dict(row)
            result["product_catalog_coverage_complete"] = bool(
                result["product_catalog_coverage_complete"]
            )
            result["diagnostics"] = json.loads(result.pop("diagnostics_json") or "{}")
            return result

    def checkpoint_sqlite(self) -> dict[str, int]:
        """Checkpoint committed WAL pages without blocking active readers."""
        self.initialize()
        with _connect(self.path) as connection:
            row = connection.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
            return {"busy": int(row[0]), "log_pages": int(row[1]),
                    "checkpointed_pages": int(row[2])}
