"""Persistent weekly supplier orchestration and resumable enrichment work.

This module owns scheduling/audit/queue mechanics only. Supplier acquisition
and enrichment are injected so the Scout cache never falls back to Manager's
tracked-product universe.
"""

from __future__ import annotations

import fcntl
import json
import os
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4
from zoneinfo import ZoneInfo

from supplier_catalog import DEFAULT_DATABASE_PATH, json_dumps, utc_now
from supplier_incremental import SupplierIncrementalStore


WEEKLY_SUPPLIERS = ("abw", "umma", "qudo")
QOGITA_KOREAN_BEAUTY_STEP = "qogita_korean_beauty"
WEEKLY_STEPS = (*WEEKLY_SUPPLIERS, QOGITA_KOREAN_BEAUTY_STEP)
WEEKLY_TIMEZONE = "Europe/Rome"
WEEKLY_WEEKDAY = 6  # datetime.weekday(): Sunday
WEEKLY_HOUR = 2
DEFAULT_WEEKLY_DATABASE_PATH = DEFAULT_DATABASE_PATH.with_name("supplier_weekly.sqlite3")
DEFAULT_WEEKLY_LOCK_PATH = Path("/tmp/glowup-scout-weekly-suppliers.lock")


@dataclass(frozen=True)
class SupplierRatePolicy:
    min_pacing_seconds: float = 0.5
    max_requests_per_run: int = 20_000
    max_retries: int = 2
    rate_limit_threshold: int = 5
    server_error_threshold: int = 20
    consecutive_error_threshold: int = 10
    backoff_seconds: float = 2.0
    reconciliation_days: int = 60
    reconciliation_budget: int = 250

    def validate(self) -> None:
        if self.min_pacing_seconds < 0 or self.max_requests_per_run <= 0:
            raise ValueError("Invalid request pacing policy")
        if min(
            self.max_retries, self.rate_limit_threshold,
            self.server_error_threshold, self.consecutive_error_threshold,
            self.reconciliation_days, self.reconciliation_budget,
        ) < 0:
            raise ValueError("Invalid supplier safety policy")


DEFAULT_RATE_POLICIES = {
    "abw": SupplierRatePolicy(min_pacing_seconds=0.75, max_requests_per_run=5_000),
    "umma": SupplierRatePolicy(min_pacing_seconds=0.5, max_requests_per_run=10_000),
    "qudo": SupplierRatePolicy(min_pacing_seconds=0.6, max_requests_per_run=15_000),
    # This policy is used only by the curated-membership collector.  It does
    # not opt Qogita into the normal supplier enrichment pipeline.
    QOGITA_KOREAN_BEAUTY_STEP: SupplierRatePolicy(
        min_pacing_seconds=0.35, max_requests_per_run=1_000, max_retries=4,
    ),
}


SCHEMA = """
CREATE TABLE IF NOT EXISTS supplier_weekly_runs (
    run_id TEXT PRIMARY KEY,
    scheduled_at TEXT,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    status TEXT NOT NULL,
    trigger_type TEXT NOT NULL,
    timezone TEXT NOT NULL,
    diagnostics_json TEXT NOT NULL DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS supplier_weekly_states (
    run_id TEXT NOT NULL,
    supplier TEXT NOT NULL,
    sequence_number INTEGER NOT NULL,
    status TEXT NOT NULL,
    baseline_before TEXT,
    baseline_after TEXT,
    started_at TEXT,
    completed_at TEXT,
    new_count INTEGER NOT NULL DEFAULT 0,
    changed_count INTEGER NOT NULL DEFAULT 0,
    unchanged_count INTEGER NOT NULL DEFAULT 0,
    removed_count INTEGER NOT NULL DEFAULT 0,
    enriched_count INTEGER NOT NULL DEFAULT 0,
    carried_forward_count INTEGER NOT NULL DEFAULT 0,
    reconciliation_count INTEGER NOT NULL DEFAULT 0,
    failure_count INTEGER NOT NULL DEFAULT 0,
    request_count INTEGER NOT NULL DEFAULT 0,
    retry_count INTEGER NOT NULL DEFAULT 0,
    rate_limit_count INTEGER NOT NULL DEFAULT 0,
    server_error_count INTEGER NOT NULL DEFAULT 0,
    duration_seconds REAL,
    promotion_result TEXT,
    error_code TEXT,
    error_message TEXT,
    rate_policy_json TEXT NOT NULL,
    diagnostics_json TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY (run_id,supplier)
);
CREATE TABLE IF NOT EXISTS supplier_sync_work_items (
    run_id TEXT NOT NULL,
    supplier TEXT NOT NULL,
    canonical_product_key TEXT NOT NULL,
    product_state TEXT NOT NULL,
    work_status TEXT NOT NULL,
    priority INTEGER NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    claimed_by TEXT,
    lease_expires_at TEXT,
    last_progress TEXT,
    error_class TEXT,
    error_message TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (run_id,supplier,canonical_product_key)
);
CREATE INDEX IF NOT EXISTS idx_supplier_sync_work_queue
ON supplier_sync_work_items(run_id,supplier,work_status,priority,canonical_product_key);
"""


def _connect(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=60)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.execute("PRAGMA busy_timeout=60000")
    return connection


def _parse(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def _utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _valid_local_candidate(day, hour: int, zone: ZoneInfo) -> datetime:
    """Return calendar 02:00, or 03:00 when DST makes 02:00 nonexistent."""
    naive = datetime(day.year, day.month, day.day, hour)
    candidate = naive.replace(tzinfo=zone, fold=0)
    round_trip = candidate.astimezone(timezone.utc).astimezone(zone)
    if (round_trip.hour, round_trip.minute) != (hour, 0):
        candidate = datetime(day.year, day.month, day.day, hour + 1, tzinfo=zone)
    return candidate


def next_weekly_refresh(now: datetime | None = None) -> datetime:
    zone = ZoneInfo(WEEKLY_TIMEZONE)
    local_now = (now or datetime.now(timezone.utc)).astimezone(zone)
    days = (WEEKLY_WEEKDAY - local_now.weekday()) % 7
    day = (local_now + timedelta(days=days)).date()
    candidate = _valid_local_candidate(day, WEEKLY_HOUR, zone)
    if candidate <= local_now:
        candidate = _valid_local_candidate(day + timedelta(days=7), WEEKLY_HOUR, zone)
    return candidate


def schedule_key(value: datetime) -> str:
    local = value.astimezone(ZoneInfo(WEEKLY_TIMEZONE))
    return f"{local:%Y-%m-%d}-weekly-supplier-sync"


class WeeklySupplierStore:
    def __init__(self, path: str | Path = DEFAULT_WEEKLY_DATABASE_PATH):
        self.path = Path(path).expanduser().resolve()

    def initialize(self) -> None:
        with _connect(self.path) as connection:
            connection.executescript(SCHEMA)

    def start_run(self, *, trigger_type: str, scheduled_at: str | None = None,
                  run_id: str | None = None) -> str:
        self.initialize()
        run_id = run_id or uuid4().hex
        diagnostics = {}
        if scheduled_at:
            parsed = _parse(scheduled_at)
            local = parsed.astimezone(ZoneInfo(WEEKLY_TIMEZONE))
            diagnostics = {
                "scheduled_local_date": local.date().isoformat(),
                "schedule_key": schedule_key(parsed),
            }
        with _connect(self.path) as connection:
            connection.execute(
                """INSERT INTO supplier_weekly_runs
                   (run_id,scheduled_at,started_at,completed_at,status,trigger_type,
                    timezone,diagnostics_json)
                   VALUES (?,?,?,NULL,'running',?,?,?)""",
                (run_id, scheduled_at, utc_now(), trigger_type, WEEKLY_TIMEZONE,
                 json_dumps(diagnostics)),
            )
        return run_id

    def has_completed_schedule(self, scheduled_at: datetime) -> bool:
        self.initialize()
        key = schedule_key(scheduled_at)
        with _connect(self.path) as connection:
            rows = connection.execute(
                "SELECT scheduled_at,status FROM supplier_weekly_runs WHERE status IN ('success','partial_success')"
            ).fetchall()
        return any(value["scheduled_at"] and schedule_key(_parse(value["scheduled_at"])) == key for value in rows)

    def start_supplier(self, run_id: str, supplier: str, sequence: int,
                       baseline_before: str | None, policy: SupplierRatePolicy) -> None:
        policy.validate()
        with _connect(self.path) as connection:
            connection.execute(
                """INSERT OR REPLACE INTO supplier_weekly_states
                   (run_id,supplier,sequence_number,status,baseline_before,started_at,rate_policy_json)
                   VALUES (?,?,?,'running',?,?,?)""",
                (run_id, supplier, sequence, baseline_before, utc_now(), json_dumps(asdict(policy))),
            )

    def finish_supplier(self, run_id: str, supplier: str, result: dict[str, Any]) -> None:
        fields = {
            "status": result.get("status", "failed"),
            "baseline_after": result.get("baseline_after"),
            "new_count": result.get("new", 0), "changed_count": result.get("changed", 0),
            "unchanged_count": result.get("unchanged", 0), "removed_count": result.get("removed", 0),
            "enriched_count": result.get("enriched", 0),
            "carried_forward_count": result.get("carried_forward", 0),
            "reconciliation_count": result.get("reconciliation", 0),
            "failure_count": result.get("failures", 0), "request_count": result.get("requests", 0),
            "retry_count": result.get("retry", 0), "rate_limit_count": result.get("rate_limits", 0),
            "server_error_count": result.get("server_errors", 0),
            "duration_seconds": result.get("duration_seconds"),
            "promotion_result": result.get("promotion_result"),
            "error_code": result.get("error_code"), "error_message": result.get("error_message"),
            "diagnostics_json": json_dumps(result.get("diagnostics") or {}),
        }
        assignments = ",".join(f"{name}=?" for name in fields)
        with _connect(self.path) as connection:
            connection.execute(
                f"UPDATE supplier_weekly_states SET completed_at=?,{assignments} WHERE run_id=? AND supplier=?",
                (utc_now(), *fields.values(), run_id, supplier),
            )

    def finish_run(self, run_id: str) -> dict[str, Any]:
        with _connect(self.path) as connection:
            states = [dict(row) for row in connection.execute(
                "SELECT * FROM supplier_weekly_states WHERE run_id=? ORDER BY sequence_number", (run_id,),
            )]
            usable = {
                "success", "waiting_for_source", "skipped",
                "admission_blocked_storage",
            }
            status = "success" if states and all(row["status"] == "success" for row in states) else (
                "partial_success" if any(row["status"] in usable for row in states) else "failed"
            )
            connection.execute(
                "UPDATE supplier_weekly_runs SET status=?,completed_at=? WHERE run_id=?",
                (status, utc_now(), run_id),
            )
        return {"run_id": run_id, "status": status, "suppliers": states}

    def latest_supplier_state(self, supplier: str) -> dict[str, Any] | None:
        self.initialize()
        with _connect(self.path) as connection:
            row = connection.execute(
                """SELECT state.* FROM supplier_weekly_states state
                   JOIN supplier_weekly_runs run ON run.run_id=state.run_id
                   WHERE state.supplier=? ORDER BY run.started_at DESC LIMIT 1""", (supplier,),
            ).fetchone()
        return dict(row) if row else None

    def enqueue(self, run_id: str, supplier: str, rows: list[dict[str, Any]]) -> int:
        self.initialize()
        priority = {"new": 400, "changed": 300, "reconciliation_due": 200, "retryable": 100}
        now = utc_now()
        with _connect(self.path) as connection:
            before = connection.total_changes
            for row in rows:
                state = str(row.get("product_state") or "retryable")
                connection.execute(
                    """INSERT OR IGNORE INTO supplier_sync_work_items
                       (run_id,supplier,canonical_product_key,product_state,work_status,priority,updated_at)
                       VALUES (?,?,?,?,'pending',?,?)""",
                    (run_id, supplier, str(row["canonical_product_key"]), state,
                     priority.get(state, 0), now),
                )
            return connection.total_changes - before

    def release_expired(self, run_id: str, supplier: str, *, now: datetime | None = None) -> int:
        self.initialize()
        cutoff = _utc(now or datetime.now(timezone.utc))
        with _connect(self.path) as connection:
            cursor = connection.execute(
                """UPDATE supplier_sync_work_items SET work_status='pending',claimed_by=NULL,
                   lease_expires_at=NULL,updated_at=? WHERE run_id=? AND supplier=?
                   AND work_status='claimed' AND lease_expires_at<?""",
                (cutoff, run_id, supplier, cutoff),
            )
            return cursor.rowcount

    def claim(self, run_id: str, supplier: str, worker_id: str, *, batch_size: int = 100,
              lease_seconds: int = 900, now: datetime | None = None) -> list[dict[str, Any]]:
        self.initialize()
        observed = now or datetime.now(timezone.utc)
        expires = observed + timedelta(seconds=lease_seconds)
        connection = _connect(self.path)
        try:
            connection.execute("BEGIN IMMEDIATE")
            cutoff = _utc(observed)
            connection.execute(
                """UPDATE supplier_sync_work_items SET work_status='pending',claimed_by=NULL,
                   lease_expires_at=NULL,updated_at=? WHERE run_id=? AND supplier=?
                   AND work_status='claimed' AND lease_expires_at<?""",
                (cutoff, run_id, supplier, cutoff),
            )
            keys = [row[0] for row in connection.execute(
                """SELECT canonical_product_key FROM supplier_sync_work_items
                   WHERE run_id=? AND supplier=? AND work_status='pending'
                   ORDER BY priority DESC,canonical_product_key LIMIT ?""",
                (run_id, supplier, batch_size),
            )]
            for key in keys:
                connection.execute(
                    """UPDATE supplier_sync_work_items SET work_status='claimed',claimed_by=?,
                       lease_expires_at=?,attempts=attempts+1,updated_at=?
                       WHERE run_id=? AND supplier=? AND canonical_product_key=? AND work_status='pending'""",
                    (worker_id, _utc(expires), cutoff, run_id, supplier, key),
                )
            rows = [dict(row) for row in connection.execute(
                """SELECT * FROM supplier_sync_work_items WHERE run_id=? AND supplier=?
                   AND claimed_by=? AND work_status='claimed' ORDER BY priority DESC,canonical_product_key""",
                (run_id, supplier, worker_id),
            )]
            connection.commit()
            return rows
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def complete_item(self, run_id: str, supplier: str, key: str, worker_id: str,
                      *, progress: str = "scenario_enriched") -> bool:
        self.initialize()
        with _connect(self.path) as connection:
            cursor = connection.execute(
                """UPDATE supplier_sync_work_items SET work_status='complete',last_progress=?,
                   claimed_by=NULL,lease_expires_at=NULL,updated_at=? WHERE run_id=? AND supplier=?
                   AND canonical_product_key=? AND work_status='claimed' AND claimed_by=?""",
                (progress, utc_now(), run_id, supplier, key, worker_id),
            )
            return cursor.rowcount == 1

    def fail_item(self, run_id: str, supplier: str, key: str, worker_id: str, *,
                  retryable: bool, error_class: str, error_message: str = "") -> bool:
        self.initialize()
        status = "pending" if retryable else "permanent_failure"
        with _connect(self.path) as connection:
            cursor = connection.execute(
                """UPDATE supplier_sync_work_items SET work_status=?,error_class=?,error_message=?,
                   claimed_by=NULL,lease_expires_at=NULL,updated_at=? WHERE run_id=? AND supplier=?
                   AND canonical_product_key=? AND work_status='claimed' AND claimed_by=?""",
                (status, error_class, error_message[:500], utc_now(), run_id, supplier, key, worker_id),
            )
            return cursor.rowcount == 1

    def queue_summary(self, run_id: str, supplier: str) -> dict[str, int]:
        self.initialize()
        with _connect(self.path) as connection:
            return dict(connection.execute(
                """SELECT work_status,COUNT(*) FROM supplier_sync_work_items
                   WHERE run_id=? AND supplier=? GROUP BY work_status""", (run_id, supplier),
            ).fetchall())


@contextmanager
def weekly_lock(path: str | Path = DEFAULT_WEEKLY_LOCK_PATH):
    descriptor = os.open(Path(path), os.O_CREAT | os.O_RDWR, 0o600)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("weekly_supplier_sync_already_running") from exc
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


class WeeklySupplierOrchestrator:
    """Run suppliers sequentially while isolating supplier-specific failures."""

    def __init__(self, handlers: dict[str, Callable[..., dict[str, Any]]], *,
                 store: WeeklySupplierStore | None = None,
                 policies: dict[str, SupplierRatePolicy] | None = None,
                 baseline_provider: Callable[[str], str | None] | None = None,
                 admission_check: Callable[[str, dict[str, Any]], dict[str, Any]] | None = None,
                 storage_metrics: Callable[[], dict[str, Any]] | None = None):
        self.handlers = handlers
        self.store = store or WeeklySupplierStore()
        self.policies = {**DEFAULT_RATE_POLICIES, **(policies or {})}
        self.baseline_provider = baseline_provider or (lambda supplier: None)
        self.admission_check = admission_check
        self.storage_metrics = storage_metrics

    def run(self, *, trigger_type: str = "manual", scheduled_at: datetime | None = None,
            sources: dict[str, Any] | None = None) -> dict[str, Any]:
        sources = sources or {}
        run_id = self.store.start_run(
            trigger_type=trigger_type,
            scheduled_at=_utc(scheduled_at) if scheduled_at else None,
        )
        for sequence, supplier in enumerate(WEEKLY_STEPS, start=1):
            baseline = self.baseline_provider(supplier)
            policy = self.policies[supplier]
            self.store.start_supplier(run_id, supplier, sequence, baseline, policy)
            started = time.monotonic()
            storage_before = self.storage_metrics() if self.storage_metrics else None
            admission = (
                self.admission_check(supplier, storage_before or {})
                if self.admission_check else {"allowed": True, "status": "admitted"}
            )
            if not admission.get("allowed"):
                result = {
                    "status": "admission_blocked_storage",
                    "baseline_after": baseline,
                    "promotion_result": "baseline_preserved",
                    "error_code": "admission_blocked_storage",
                    "error_message": str(admission.get("reason") or "storage headroom unavailable"),
                    "diagnostics": {
                        "storage_admission": admission, "retention_execution": False,
                    },
                }
            elif supplier == "abw" and not sources.get("abw"):
                result = {
                    "status": "waiting_for_source", "baseline_after": baseline,
                    "promotion_result": "baseline_preserved",
                    "error_code": "source_refresh_required",
                    "error_message": "Import manuale XLSX ABW richiesto",
                }
            elif supplier not in self.handlers:
                result = {
                    "status": "failed", "baseline_after": baseline,
                    "promotion_result": "baseline_preserved",
                    "error_code": "handler_unavailable",
                }
            else:
                try:
                    result = dict(self.handlers[supplier](
                        run_id=run_id, source=sources.get(supplier), policy=policy,
                        work_store=self.store,
                    ))
                    result.setdefault("status", "success")
                    result.setdefault("promotion_result", "promoted")
                    result.setdefault("baseline_after", baseline)
                except Exception as exc:
                    result = {
                        "status": "failed", "baseline_after": baseline,
                        "promotion_result": "baseline_preserved",
                        "failures": 1, "error_code": type(exc).__name__,
                        "error_message": str(exc),
                    }
            diagnostics = dict(result.get("diagnostics") or {})
            diagnostics["storage"] = {
                "workload_type": f"weekly_{supplier}",
                "filesystem_pre": storage_before,
                "filesystem_post": self.storage_metrics() if self.storage_metrics else None,
                "admission": admission,
                "elapsed_seconds": time.monotonic() - started,
                "success": result.get("status") == "success",
            }
            result["diagnostics"] = diagnostics
            result["duration_seconds"] = time.monotonic() - started
            self.store.finish_supplier(run_id, supplier, result)
        return self.store.finish_run(run_id)


class IncrementalWeeklyHandler:
    """Resumable NEW/CHANGED/reconciliation handler shared by supplier adapters.

    ``enumerate_catalog`` performs only the supplier's lightweight full index or
    official export. ``enrich_product`` performs the expensive detail work for
    queue items. ``publish_generation`` owns the supplier-specific atomic gate.
    """

    def __init__(self, supplier: str, *, enumerate_catalog: Callable[..., dict[str, Any]],
                 enrich_product: Callable[..., list[dict[str, Any]]],
                 publish_generation: Callable[..., dict[str, Any]],
                 previous_run_id: Callable[[], str | None],
                 incremental_store: SupplierIncrementalStore | None = None,
                 batch_size: int = 100):
        if supplier not in WEEKLY_SUPPLIERS:
            raise ValueError("Weekly live scope excludes this supplier")
        self.supplier = supplier
        self.enumerate_catalog = enumerate_catalog
        self.enrich_product = enrich_product
        self.publish_generation = publish_generation
        self.previous_run_id = previous_run_id
        self.incremental = incremental_store or SupplierIncrementalStore()
        self.batch_size = batch_size

    def __call__(self, *, run_id: str, source: Any, policy: SupplierRatePolicy,
                 work_store: WeeklySupplierStore) -> dict[str, Any]:
        enumeration = self.enumerate_catalog(source=source, policy=policy)
        products = list(enumeration.get("products") or [])
        previous = self.previous_run_id()
        generation_run_id = f"{run_id}-{self.supplier}"
        # The first incremental run imports the already-active, validated
        # baseline into the immutable reference store.  This prevents a safe
        # rollout from classifying the entire supplier universe as NEW.
        if previous and not self.incremental.has_generation(previous):
            self.incremental.compose_generation(
                previous, self.supplier,
                enumeration.get("previous_products") or (),
                scenarios_by_product=enumeration.get("previous_scenarios_by_product") or {},
                reconciliation_days=policy.reconciliation_days,
            )
        counts = self.incremental.compose_generation(
            generation_run_id, self.supplier, products, previous_run_id=previous,
            reconciliation_days=policy.reconciliation_days,
        )
        queue = self.incremental.enrichment_queue(generation_run_id)
        selected = [
            {**row, "product_state": row["queue_reason"]} for row in queue
            if row["queue_reason"] in {"new", "changed", "enrichment_failed", "identifier_unresolved"}
        ]
        reconciliation = [row for row in queue if row["queue_reason"] == "reconciliation_due"]
        selected.extend(
            {**row, "product_state": "reconciliation_due"}
            for row in reconciliation[:policy.reconciliation_budget]
        )
        work_store.enqueue(run_id, self.supplier, selected)
        worker = f"weekly-{self.supplier}-{os.getpid()}"
        budget_exhausted = False
        metrics = {
            "enriched": 0, "failures": 0, "requests": int(enumeration.get("requests") or 0),
            "retry": int(enumeration.get("retry") or 0),
            "rate_limits": int(enumeration.get("rate_limits") or 0),
            "server_errors": int(enumeration.get("server_errors") or 0),
            "reconciliation": 0,
        }
        consecutive_errors = 0
        while True:
            batch = work_store.claim(
                run_id, self.supplier, worker, batch_size=self.batch_size,
            )
            if not batch:
                break
            for item in batch:
                key = item["canonical_product_key"]
                if metrics["requests"] >= policy.max_requests_per_run:
                    work_store.fail_item(
                        run_id, self.supplier, key, worker, retryable=True,
                        error_class="request_budget_exhausted",
                    )
                    budget_exhausted = True
                    continue
                try:
                    outcome = self.enrich_product(
                        canonical_product_key=key,
                        product=self.incremental.product_payload(generation_run_id, key),
                        policy=policy,
                    )
                    scenarios = outcome.get("scenarios") if isinstance(outcome, dict) else outcome
                    if isinstance(outcome, dict) and outcome.get("product_updates"):
                        self.incremental.persist_product_update(
                            generation_run_id, self.supplier, key,
                            outcome["product_updates"],
                        )
                    self.incremental.persist_enrichment(
                        generation_run_id, self.supplier, key, [
                            {**scenario, "canonical_product_key": key}
                            for scenario in (scenarios or [])
                        ],
                    )
                    work_store.complete_item(run_id, self.supplier, key, worker)
                    metrics["enriched"] += 1
                    metrics["requests"] += int(
                        outcome.get("requests", 1) if isinstance(outcome, dict) else 1
                    )
                    metrics["retry"] += int(outcome.get("retry", 0) if isinstance(outcome, dict) else 0)
                    metrics["rate_limits"] += int(
                        outcome.get("rate_limits", 0) if isinstance(outcome, dict) else 0
                    )
                    metrics["server_errors"] += int(
                        outcome.get("server_errors", 0) if isinstance(outcome, dict) else 0
                    )
                    if item["product_state"] == "reconciliation_due":
                        metrics["reconciliation"] += 1
                    consecutive_errors = 0
                except Exception as exc:
                    attempts = int(item.get("attempts") or 1)
                    retryable = (
                        bool(getattr(exc, "retryable", False))
                        and attempts <= policy.max_retries
                    )
                    work_store.fail_item(
                        run_id, self.supplier, key, worker, retryable=retryable,
                        error_class=type(exc).__name__, error_message=str(exc),
                    )
                    metrics["failures"] += 1
                    consecutive_errors += 1
                    remote_status = getattr(exc, "remote_status", None)
                    if remote_status == 429 or getattr(exc, "code", None) == "rate_limited":
                        metrics["rate_limits"] += 1
                    if isinstance(remote_status, int) and remote_status >= 500:
                        metrics["server_errors"] += 1
            if budget_exhausted or (
                metrics["rate_limits"] >= policy.rate_limit_threshold
                or metrics["server_errors"] >= policy.server_error_threshold
                or consecutive_errors >= policy.consecutive_error_threshold
            ):
                break
        queue_summary = work_store.queue_summary(run_id, self.supplier)
        if (queue_summary.get("pending") or queue_summary.get("claimed")
                or queue_summary.get("permanent_failure")):
            return {
                "status": "failed", **metrics, **counts,
                "baseline_after": previous, "promotion_result": "baseline_preserved",
                "error_code": "incremental_queue_incomplete",
                "diagnostics": {"queue": queue_summary},
            }
        promotion = self.publish_generation(
            run_id=generation_run_id, supplier=self.supplier, previous_run_id=previous,
            enumeration=enumeration, incremental_store=self.incremental,
        )
        return {
            "status": "success", **metrics,
            "new": counts.get("new", 0), "changed": counts.get("changed", 0),
            "unchanged": counts.get("unchanged", 0), "removed": counts.get("removed", 0),
            "carried_forward": self.incremental.generation_summary(generation_run_id)["enrichment_states"].get(
                "carried_forward", 0
            ),
            "baseline_after": promotion.get("run_id", run_id),
            "promotion_result": promotion.get("promotion_result", "promoted"),
            "diagnostics": {"queue": queue_summary, **(enumeration.get("diagnostics") or {})},
        }
