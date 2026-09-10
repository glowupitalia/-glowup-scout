#!/usr/bin/env python3
"""Restart-safe operator for one explicitly configured Qogita bootstrap run."""

from __future__ import annotations

import argparse
import fcntl
import json
import logging
import os
import signal
import shutil
import sys
import time
from collections import Counter, deque
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path
from typing import Any

from qogita_bootstrap import (
    QogitaBootstrapClient,
    QogitaBootstrapStore,
    run_qogita_bootstrap_concurrent,
)
from supplier_catalog import DEFAULT_DATABASE_PATH, utc_now
from qogita_serving import (
    QogitaServingStore, REST_WINDOW_SECONDS, RUN_WINDOW_SECONDS,
)
from notifications import NotificationContent, send_notification
from storage_gc import (
    append_storage_audit_event,
    collect_storage_metrics,
    evaluate_qogita_window_admission,
    production_retention_plan,
)
from supplier_catalog_coordination import (
    SupplierCatalogCoordinationTimeout,
    supplier_catalog_writer_lock,
    weekly_intent_active,
)


ROOT = Path(__file__).resolve().parent
DEFAULT_POINTER = ROOT / "data" / "qogita_bootstrap_current.json"
DEFAULT_LOCK = ROOT / "data" / "qogita-bootstrap.lock"
MINIMUM_OFFERS_PACING = 1.15
DEFAULT_MINIMUM_FREE_BYTES = 15 * 1024**3
MILESTONES = (25000, 50000, 100000, 200000)
WRITER_RETRY_SECONDS = 5.0
DEFAULT_RECOVERY_COOLDOWN_SECONDS = 300
DEFAULT_MAX_AUTO_RESUMES = 3


class QogitaFailureCategory(StrEnum):
    PRODUCT_NOT_FOUND = "PRODUCT_NOT_FOUND"
    PRODUCT_INVALID = "PRODUCT_INVALID"
    PRODUCT_NO_OFFERS = "PRODUCT_NO_OFFERS"
    PRODUCT_IDENTIFIER_UNRESOLVED = "PRODUCT_IDENTIFIER_UNRESOLVED"
    NETWORK_TRANSIENT = "NETWORK_TRANSIENT"
    TIMEOUT_TRANSIENT = "TIMEOUT_TRANSIENT"
    RATE_LIMIT = "RATE_LIMIT"
    UPSTREAM_5XX_TRANSIENT = "UPSTREAM_5XX_TRANSIENT"
    AUTH_REFRESHABLE = "AUTH_REFRESHABLE"
    AUTH_FATAL = "AUTH_FATAL"
    SQLITE_BUSY_TRANSIENT = "SQLITE_BUSY_TRANSIENT"
    STORAGE_FATAL = "STORAGE_FATAL"
    SOURCE_GLOBAL_FATAL = "SOURCE_GLOBAL_FATAL"
    UNKNOWN_FATAL = "UNKNOWN_FATAL"


PRODUCT_CATEGORIES = {
    QogitaFailureCategory.PRODUCT_NOT_FOUND,
    QogitaFailureCategory.PRODUCT_INVALID,
    QogitaFailureCategory.PRODUCT_NO_OFFERS,
    QogitaFailureCategory.PRODUCT_IDENTIFIER_UNRESOLVED,
}
RECOVERABLE_CATEGORIES = {
    QogitaFailureCategory.NETWORK_TRANSIENT,
    QogitaFailureCategory.TIMEOUT_TRANSIENT,
    QogitaFailureCategory.RATE_LIMIT,
    QogitaFailureCategory.UPSTREAM_5XX_TRANSIENT,
    QogitaFailureCategory.AUTH_REFRESHABLE,
    QogitaFailureCategory.SQLITE_BUSY_TRANSIENT,
}


def classify_qogita_outcome(outcome: dict[str, Any]) -> QogitaFailureCategory | None:
    """Map one terminal attempt to an explicit local/transient/structural category."""
    if outcome.get("status") == "success":
        if int(outcome.get("scenario_count") or 0) == 0:
            return QogitaFailureCategory.PRODUCT_NO_OFFERS
        return None
    code = str(outcome.get("error_code") or "").casefold()
    status = int(outcome.get("http_status") or 0)
    if status == 404:
        return QogitaFailureCategory.PRODUCT_NOT_FOUND
    if "authentication" in code:
        return (QogitaFailureCategory.AUTH_REFRESHABLE if outcome.get("retryable")
                else QogitaFailureCategory.AUTH_FATAL)
    if status == 429:
        return QogitaFailureCategory.RATE_LIMIT
    if status >= 500:
        return QogitaFailureCategory.UPSTREAM_5XX_TRANSIENT
    if "timeout" in code:
        return QogitaFailureCategory.TIMEOUT_TRANSIENT
    if "network" in code:
        return QogitaFailureCategory.NETWORK_TRANSIENT
    if code.startswith("resolver_"):
        if any(part in code for part in ("missing", "bad_", "mismatch")):
            return QogitaFailureCategory.PRODUCT_IDENTIFIER_UNRESOLVED
        return QogitaFailureCategory.PRODUCT_INVALID
    if code.startswith(("offers_parsing", "offers_duplicate", "variant_fid_conflict")):
        return QogitaFailureCategory.PRODUCT_INVALID
    if code.startswith("offers_") and not outcome.get("retryable"):
        return QogitaFailureCategory.PRODUCT_INVALID
    return QogitaFailureCategory.UNKNOWN_FATAL


def _stop_reason(category: QogitaFailureCategory, detail: str, *, recoverable: bool) -> str:
    mode = "recoverable" if recoverable else "fatal"
    return f"{mode}:{category.value}:{detail}"


def parse_stop_reason(reason: str | None, run: dict[str, Any] | None = None):
    value = str(reason or "")
    if value == "ten_consecutive_product_errors":
        # Legacy stop from the former raw gate. It is safe to retry because all
        # claims were released and terminal product rows remain terminal.
        return True, QogitaFailureCategory.NETWORK_TRANSIENT
    parts = value.split(":", 2)
    if len(parts) >= 2 and parts[0] in {"recoverable", "fatal"}:
        try:
            return parts[0] == "recoverable", QogitaFailureCategory(parts[1])
        except ValueError:
            pass
    return False, QogitaFailureCategory.UNKNOWN_FATAL


def classify_structural_exception(error: BaseException):
    if getattr(error, "qogita_sqlite_context", None):
        return True, QogitaFailureCategory.SQLITE_BUSY_TRANSIENT
    message = str(error).casefold()
    code = str(getattr(error, "code", "")).casefold()
    if "disk_free" in message:
        return False, QogitaFailureCategory.STORAGE_FATAL
    if "source_generation" in code or "production source" in message:
        return False, QogitaFailureCategory.SOURCE_GLOBAL_FATAL
    if "authentication" in code:
        return bool(getattr(error, "retryable", False)), (
            QogitaFailureCategory.AUTH_REFRESHABLE
            if getattr(error, "retryable", False)
            else QogitaFailureCategory.AUTH_FATAL
        )
    return False, QogitaFailureCategory.UNKNOWN_FATAL


class ShutdownController:
    """Turn SIGTERM/SIGINT into a boundary-safe stop request."""

    def __init__(self):
        self.requested = False
        self.signal_number: int | None = None

    def request(self, signal_number, _frame=None):
        self.requested = True
        self.signal_number = int(signal_number)

    def install(self):
        signal.signal(signal.SIGTERM, self.request)
        signal.signal(signal.SIGINT, self.request)


def _send_structural_alert(
    database: Path, bootstrap_run_id: str, category: QogitaFailureCategory,
    reason: str,
):
    """Use the shared, idempotent notification outbox for operator-only failures."""
    content = NotificationContent(
        event_type="qogita_bootstrap_auto_stopped",
        subject=f"Glow Up Scout: Qogita fermo ({category.value})",
        text=(f"La run Qogita {bootstrap_run_id} si è fermata in sicurezza.\n"
              f"Categoria: {category.value}\nMotivo: {reason[:300]}\n"),
        html=("<html><body><p>La run Qogita si è fermata in sicurezza.</p>"
              f"<p>Categoria: {category.value}</p></body></html>"),
    )
    try:
        send_notification(
            content, entity_id=f"{bootstrap_run_id}:{category.value}",
            database_path=database,
        )
    except Exception:
        logging.exception("Qogita structural alert delivery failed")


def _load_env(path: Path):
    """Load missing process variables without exposing values."""
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key or key in os.environ:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        os.environ[key] = value


def _read_pointer(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    required = {"bootstrap_run_id", "source_generation_id"}
    if not isinstance(payload, dict) or not required.issubset(payload):
        raise RuntimeError("Qogita bootstrap pointer is malformed")
    return payload


def _size(path: Path) -> int:
    return path.stat().st_size if path.exists() else 0


def storage_snapshot(database: Path) -> dict[str, Any]:
    usage = shutil.disk_usage(database.parent)
    return {
        "database_bytes": _size(database),
        "wal_bytes": _size(Path(str(database) + "-wal")),
        "shm_bytes": _size(Path(str(database) + "-shm")),
        "disk_free_bytes": int(usage.free),
    }


class ProductionHealthGuard:
    def __init__(self, *, store: QogitaBootstrapStore, bootstrap_run_id: str,
                 database: Path, minimum_free_bytes: int, window_number: int = 0):
        self.store = store
        self.bootstrap_run_id = bootstrap_run_id
        self.database = database
        self.minimum_free_bytes = int(minimum_free_bytes)
        self.started = time.monotonic()
        self.window_number = int(window_number)
        run = store.bootstrap(bootstrap_run_id) or {}
        self.initial_completed = int((run.get("last_progress") or {}).get("offers_success") or 0)
        self.metric_baseline = {
            "http_429": int(run.get("rate_limit_count") or 0),
            "http_5xx": int(run.get("server_error_count") or 0),
        }
        self.last_metrics: dict[str, int] = dict(self.metric_baseline)
        self.recent_429: deque[float] = deque()
        self.consecutive_global_errors = 0
        self.category_counts: Counter[str] = Counter()
        self.retry_category_counts: Counter[str] = Counter()
        self.product_quarantine_count = 0
        self.last_progress_at = utc_now()

    def initial_check(self):
        self.store.validate_production_source(self.bootstrap_run_id)
        storage = storage_snapshot(self.database)
        if storage["disk_free_bytes"] < self.minimum_free_bytes:
            raise RuntimeError("disk_free_below_guardrail")
        integrity = self.store.database_integrity()
        if integrity["quick_check"] != "ok":
            raise RuntimeError("sqlite_integrity_failure")
        if integrity["duplicate_scenario_identities"]:
            raise RuntimeError("duplicate_scenario_identity")
        return {**storage, **integrity}

    def product(self, payload: dict[str, Any]) -> str | None:
        outcome = payload.get("outcome") or {}
        metrics = payload.get("metrics") or {}
        now = time.monotonic()
        previous_429 = int(self.last_metrics.get("http_429", 0))
        for _ in range(max(0, int(metrics.get("http_429", 0)) - previous_429)):
            self.recent_429.append(now)
        while self.recent_429 and now - self.recent_429[0] > 600:
            self.recent_429.popleft()
        self.last_metrics = {key: int(value or 0) for key, value in metrics.items()
                             if isinstance(value, (int, float))}
        category = classify_qogita_outcome(outcome)
        for retry in outcome.get("recovered_retries") or []:
            retry_category = classify_qogita_outcome(retry)
            if retry_category:
                self.retry_category_counts[retry_category.value] += 1
        if category:
            self.category_counts[category.value] += 1
        if category in PRODUCT_CATEGORIES or category is None:
            self.consecutive_global_errors = 0
            self.last_progress_at = utc_now()
            if category in PRODUCT_CATEGORIES:
                self.product_quarantine_count += 1
        else:
            self.consecutive_global_errors += 1
        if category == QogitaFailureCategory.AUTH_FATAL:
            return _stop_reason(category, "global_authentication_failure", recoverable=False)
        if category == QogitaFailureCategory.UNKNOWN_FATAL:
            return _stop_reason(category, "unclassified_terminal_failure", recoverable=False)
        if category in RECOVERABLE_CATEGORIES and self.consecutive_global_errors >= 10:
            return _stop_reason(category, "persistent_global_failure", recoverable=True)
        if len(self.recent_429) >= 10:
            return _stop_reason(
                QogitaFailureCategory.RATE_LIMIT,
                "ten_http_429_within_10_minutes", recoverable=True,
            )
        processed = int(payload.get("processed") or 0)
        if processed % 100 == 0:
            storage = storage_snapshot(self.database)
            if storage["disk_free_bytes"] < self.minimum_free_bytes:
                return _stop_reason(
                    QogitaFailureCategory.STORAGE_FATAL,
                    "disk_free_below_guardrail", recoverable=False,
                )
        return None

    def checkpoint(self, run: dict[str, Any]):
        self.store.validate_production_source(self.bootstrap_run_id)
        progress = dict(run.get("last_progress") or {})
        storage = storage_snapshot(self.database)
        if storage["disk_free_bytes"] < self.minimum_free_bytes:
            raise RuntimeError("disk_free_below_guardrail")
        elapsed = max(0.001, time.monotonic() - self.started)
        completed = int(progress.get("offers_success") or 0)
        newly_completed = max(0, completed - self.initial_completed)
        rate = newly_completed / elapsed
        remaining = int(progress.get("remaining") or 0)
        health = {
            **storage,
            "completed_products": completed,
            "newly_completed_products": newly_completed,
            "remaining_products": remaining,
            "throughput_products_per_hour": rate * 3600,
            "eta_seconds": (remaining / rate if rate > 0 else None),
            "observed_at": time.time(),
            "last_progress_at": self.last_progress_at,
            "current_window": self.window_number,
            "current_throughput": rate * 3600,
            "failure_count_by_category": dict(self.category_counts),
            "retry_count_by_category": dict(self.retry_category_counts),
            "product_quarantine_count": self.product_quarantine_count,
            "auto_resume_count": int((run.get("health") or {}).get("auto_resume_count") or 0),
            "auto_resume_streak": (
                0 if newly_completed else
                int((run.get("health") or {}).get("auto_resume_streak") or 0)
            ),
            "structural_stop_count": int((run.get("health") or {}).get("structural_stop_count") or 0),
            "last_fatal_category": (run.get("health") or {}).get("last_fatal_category"),
        }
        self.store.update_health(self.bootstrap_run_id, health)
        reached = self.store.record_milestones(
            self.bootstrap_run_id, metrics={**progress, **health}, milestones=MILESTONES,
        )
        if reached:
            logging.info("Qogita bootstrap milestones reached: %s", reached)


def _parser():
    parser = argparse.ArgumentParser(description="Resume one persisted Qogita production bootstrap")
    parser.add_argument("--pointer", default=str(DEFAULT_POINTER))
    parser.add_argument("--database", default=str(DEFAULT_DATABASE_PATH))
    parser.add_argument("--lock", default=str(DEFAULT_LOCK))
    parser.add_argument("--workers", type=int, default=2, choices=(2,))
    parser.add_argument("--offers-pacing", type=float, default=MINIMUM_OFFERS_PACING)
    parser.add_argument("--product-link-pacing", type=float, default=0.6)
    parser.add_argument("--checkpoint-every", type=int, default=100)
    parser.add_argument("--minimum-free-bytes", type=int, default=DEFAULT_MINIMUM_FREE_BYTES)
    parser.add_argument("--max-products", type=int)
    parser.add_argument("--run-window-seconds", type=int, default=RUN_WINDOW_SECONDS)
    parser.add_argument("--rest-window-seconds", type=int, default=REST_WINDOW_SECONDS)
    parser.add_argument("--recovery-cooldown-seconds", type=int,
                        default=DEFAULT_RECOVERY_COOLDOWN_SECONDS)
    parser.add_argument("--max-auto-resumes", type=int, default=DEFAULT_MAX_AUTO_RESUMES)
    parser.add_argument("--execute", action="store_true")
    return parser


def main(argv=None):
    args = _parser().parse_args(argv)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s qogita_production_bootstrap %(message)s",
    )
    if not args.execute:
        raise SystemExit("--execute is required; this runner never creates a bootstrap")
    if args.offers_pacing < MINIMUM_OFFERS_PACING:
        raise SystemExit("offers pacing below the production minimum")
    pointer = _read_pointer(Path(args.pointer).expanduser().resolve())
    environment_file = Path(
        pointer.get("environment_file") or (ROOT / ".env")
    ).expanduser().resolve()
    _load_env(environment_file)
    email = os.environ.get("QOGITA_EMAIL")
    password = os.environ.get("QOGITA_PASSWORD")
    if not email or not password:
        raise SystemExit("Qogita credentials are unavailable")
    database = Path(args.database).expanduser().resolve()
    shutdown = ShutdownController()
    shutdown.install()
    lock_path = Path(args.lock).expanduser().resolve()
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_handle = lock_path.open("a+", encoding="utf-8")
    try:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        logging.info("Qogita production bootstrap is already running")
        return 0
    store = QogitaBootstrapStore(database)
    serving_store = QogitaServingStore(database)
    run = store.bootstrap(pointer["bootstrap_run_id"])
    if not run or run.get("staging_run_id") != pointer["source_generation_id"]:
        raise SystemExit("Configured bootstrap/source pair does not exist")
    if run.get("run_mode") != "production":
        raise SystemExit("Configured bootstrap is not a production bootstrap")
    if run.get("status") == "awaiting_promotion_review":
        logging.info("Qogita bootstrap already awaits promotion review")
        return 0
    try:
        base_url = os.environ.get("QOGITA_BASE_URL", "https://api.qogita.com")
        credentials = {"base_url": base_url, "email": email, "password": password}

        def client_factory(auth_manager, rate_limiter):
            return QogitaBootstrapClient(
                **credentials, auth_manager=auth_manager, rate_limiter=rate_limiter,
            )

        last_storage_block_key = None
        window_storage_before = None
        while True:
            persisted_duty = serving_store.duty_state(pointer["bootstrap_run_id"])
            now_utc = datetime.now(timezone.utc)
            needs_new_window = not persisted_duty
            if persisted_duty and persisted_duty.get("state") == "resting":
                rest_value = persisted_duty.get("rest_until")
                needs_new_window = bool(
                    not rest_value
                    or now_utc >= datetime.fromisoformat(
                        str(rest_value).replace("Z", "+00:00")
                    ).astimezone(timezone.utc)
                )
            if weekly_intent_active():
                logging.info(
                    "Qogita supplier-catalog handoff active; no new duty work will start"
                )
                time.sleep(WRITER_RETRY_SECONDS)
                continue
            if needs_new_window:
                window_storage_before = collect_storage_metrics(ROOT)
                admission = evaluate_qogita_window_admission(
                    metrics=window_storage_before,
                )
                if not admission["allowed"]:
                    state = str((admission.get("watermark") or {}).get("state") or "UNKNOWN")
                    plan = production_retention_plan(state)
                    admission["retention_plan"] = plan.get("summary")
                    block_key = (state, admission.get("reason"))
                    if block_key != last_storage_block_key:
                        append_storage_audit_event({
                            "event": "admission", "workload_type": "qogita_window",
                            "workload_id": pointer["bootstrap_run_id"],
                            "decision": admission, "retention_execution": False,
                        }, path=database.parent / "storage-workload-metrics.jsonl")
                        logging.warning(
                            "QOGITA WINDOW ADMISSION BLOCKED | state=%s reason=%s",
                            state, admission.get("reason"),
                        )
                        last_storage_block_key = block_key
                    time.sleep(60.0)
                    continue
                append_storage_audit_event({
                    "event": "admission", "workload_type": "qogita_window",
                    "workload_id": pointer["bootstrap_run_id"],
                    "decision": admission, "storage_before": window_storage_before,
                    "retention_execution": False,
                }, path=database.parent / "storage-workload-metrics.jsonl")
                last_storage_block_key = None
            try:
                with supplier_catalog_writer_lock(timeout_seconds=0):
                    duty = serving_store.ensure_running_window(
                        pointer["bootstrap_run_id"], run_window_seconds=args.run_window_seconds,
                        rest_window_seconds=args.rest_window_seconds,
                    )
            except SupplierCatalogCoordinationTimeout:
                logging.info("Qogita waiting for supplier-catalog writer handoff")
                time.sleep(WRITER_RETRY_SECONDS)
                continue
            if duty["state"] == "completed":
                logging.info("Qogita duty cycle is complete and awaits promotion review")
                return 0
            if duty["state"] == "auto_stopped":
                stopped = store.bootstrap(pointer["bootstrap_run_id"]) or {}
                recoverable, category = parse_stop_reason(stopped.get("stop_reason"), stopped)
                resumes = int((stopped.get("health") or {}).get("auto_resume_streak") or 0)
                if recoverable and resumes < max(0, args.max_auto_resumes):
                    logging.warning(
                        "Qogita recoverable stop; cooldown=%ss category=%s resume=%s/%s",
                        args.recovery_cooldown_seconds, category.value,
                        resumes + 1, args.max_auto_resumes,
                    )
                    if shutdown.requested:
                        return 0
                    time.sleep(max(0, args.recovery_cooldown_seconds))
                    store.reconcile_interrupted_window(pointer["bootstrap_run_id"])
                    active = serving_store.active_snapshot() or {}
                    serving_store.auto_resume_window(
                        pointer["bootstrap_run_id"],
                        expected_serving_generation_id=active["serving_generation_id"],
                        failure_category=category.value,
                    )
                    continue
                logging.error(
                    "Qogita structural stop requires operator review: category=%s", category.value,
                )
                _send_structural_alert(
                    database, pointer["bootstrap_run_id"], category,
                    str(stopped.get("stop_reason") or "structural stop"),
                )
                return 0
            if duty["state"] == "checkpointing":
                try:
                    with supplier_catalog_writer_lock(timeout_seconds=0):
                        current = store.bootstrap(pointer["bootstrap_run_id"]) or {}
                        progress = current.get("last_progress") or {}
                        complete = current.get("status") == "awaiting_promotion_review" or not int(
                            progress.get("remaining") or 0
                        )
                        wal = serving_store.checkpoint_sqlite()
                        snapshot = serving_store.build_snapshot(
                            pointer["bootstrap_run_id"], window_number=int(duty["window_number"]),
                            bootstrap_state=("completed" if complete else "resting"),
                        )
                        if complete:
                            serving_store.mark_completed(
                                pointer["bootstrap_run_id"],
                                serving_generation_id=snapshot["serving_generation_id"],
                            )
                            return 0
                        rest = serving_store.begin_rest(
                            pointer["bootstrap_run_id"],
                            serving_generation_id=snapshot["serving_generation_id"],
                        )
                except SupplierCatalogCoordinationTimeout:
                    time.sleep(WRITER_RETRY_SECONDS)
                    continue
                logging.info(
                    "Recovered checkpointing window; snapshot=%s WAL=%s REST until %s",
                    snapshot["serving_generation_id"], wal, rest["rest_until"],
                )
                continue
            if duty["state"] == "resting":
                rest_until = datetime.fromisoformat(
                    duty["rest_until"].replace("Z", "+00:00")
                ).astimezone(timezone.utc)
                remaining = max(0.0, (rest_until - datetime.now(timezone.utc)).total_seconds())
                if remaining:
                    logging.info("Qogita REST window; no supplier traffic until %s", duty["rest_until"])
                    time.sleep(min(remaining, 60.0))
                    continue
                continue

            guard = ProductionHealthGuard(
                store=store, bootstrap_run_id=pointer["bootstrap_run_id"],
                database=database, minimum_free_bytes=args.minimum_free_bytes,
                window_number=int(duty["window_number"]),
            )
            initial = guard.initial_check()
            logging.info(
                "Qogita duty window %s preflight: %s", duty["window_number"], initial,
            )
            if weekly_intent_active():
                logging.info("Qogita yielding before active duty to weekly supplier sync")
                time.sleep(WRITER_RETRY_SECONDS)
                continue
            deadline = datetime.fromisoformat(
                duty["current_window_deadline"].replace("Z", "+00:00")
            ).astimezone(timezone.utc)
            try:
                with supplier_catalog_writer_lock(timeout_seconds=0):
                    store.resume_production(pointer["bootstrap_run_id"])
                    result = run_qogita_bootstrap_concurrent(
                        pointer["bootstrap_run_id"], store=store, client_factory=client_factory,
                        workers=2, max_products=args.max_products,
                        checkpoint_every=max(1, args.checkpoint_every),
                        product_link_pacing=args.product_link_pacing,
                        offers_pacing=args.offers_pacing,
                        health_callback=guard.product, checkpoint_callback=guard.checkpoint,
                        graceful_stop_callback=lambda: (
                            shutdown.requested or datetime.now(timezone.utc) >= deadline
                            or weekly_intent_active()
                        ),
                        **credentials,
                    )
                    logging.info(
                        "Qogita duty invocation ended: status=%s attempted=%s graceful=%s",
                        result.get("status"), result.get("invocation_products_attempted"),
                        result.get("graceful_stop"),
                    )
                    append_storage_audit_event({
                        "event": "workload_completed", "workload_type": "qogita_window",
                        "workload_id": pointer["bootstrap_run_id"],
                        "window_number": duty.get("window_number"),
                        "universe_size": int(result.get("invocation_products_attempted") or 0),
                        "elapsed_seconds": result.get("wall_elapsed_seconds"),
                        "success": not bool(result.get("auto_stop_reason")),
                        "storage_before": window_storage_before,
                        "storage_after": collect_storage_metrics(ROOT),
                        "retention_execution": False,
                    }, path=database.parent / "storage-workload-metrics.jsonl")
                    if result.get("auto_stop_reason"):
                        serving_store.mark_auto_stopped(pointer["bootstrap_run_id"])
                        continue
                    progress = result.get("last_progress") or {}
                    complete = result.get("status") == "awaiting_promotion_review" or not int(
                        progress.get("remaining") or 0
                    )
                    if shutdown.requested:
                        store.mark_stopped(
                            pointer["bootstrap_run_id"],
                            f"signal_interrupt:{shutdown.signal_number}",
                            health={**(store.bootstrap(pointer["bootstrap_run_id"]) or {}).get("health", {}),
                                    "interrupted_at": utc_now(), "clean_shutdown": True},
                        )
                        serving_store.mark_auto_stopped(pointer["bootstrap_run_id"])
                        logging.info("Qogita clean shutdown checkpoint completed")
                        return 0
                    if result.get("graceful_stop") or complete:
                        serving_store.mark_checkpointing(pointer["bootstrap_run_id"])
                        wal = serving_store.checkpoint_sqlite()
                        snapshot = serving_store.build_snapshot(
                            pointer["bootstrap_run_id"], window_number=int(duty["window_number"]),
                            bootstrap_state=("completed" if complete else "resting"),
                        )
                        if complete:
                            serving_store.mark_completed(
                                pointer["bootstrap_run_id"],
                                serving_generation_id=snapshot["serving_generation_id"],
                            )
                            logging.info(
                                "Qogita final serving snapshot %s created; promotion remains pending",
                                snapshot["serving_generation_id"],
                            )
                            return 0
                        rest = serving_store.begin_rest(
                            pointer["bootstrap_run_id"],
                            serving_generation_id=snapshot["serving_generation_id"],
                        )
                        logging.info(
                            "Qogita serving snapshot %s created; WAL=%s; REST until %s",
                            snapshot["serving_generation_id"], wal, rest["rest_until"],
                        )
                        continue
                    # A bounded diagnostic invocation must not spin or create a snapshot.
                    return 0
            except SupplierCatalogCoordinationTimeout:
                logging.info("Qogita writer lock unavailable; yielding without writes")
                time.sleep(WRITER_RETRY_SECONDS)
                continue
    except Exception as exc:
        logging.exception("Qogita production bootstrap stopped safely: %s", type(exc).__name__)
        sqlite_context = getattr(exc, "qogita_sqlite_context", None)
        recoverable, category = classify_structural_exception(exc)
        reason = _stop_reason(category, type(exc).__name__, recoverable=recoverable)
        health = {
            "stopped_at": utc_now(), "failure_category": category.value,
            "recoverable": recoverable,
        }
        if sqlite_context:
            health.update({
                "primary_error": type(exc).__name__,
                "sqlite_lock_exhausted": True,
                "sqlite_context": sqlite_context,
            })
        try:
            store.mark_stopped(pointer["bootstrap_run_id"], reason, health=health)
        except Exception:
            logging.exception(
                "Qogita stop-state persistence failed; primary error remains %s",
                type(exc).__name__,
            )
        try:
            serving_store.mark_auto_stopped(pointer["bootstrap_run_id"])
        except Exception:
            logging.exception(
                "Qogita duty auto-stop persistence failed; primary error remains %s",
                type(exc).__name__,
            )
        if not recoverable:
            _send_structural_alert(
                database, pointer["bootstrap_run_id"], category, reason,
            )
        return 75 if recoverable else 0
    finally:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
        lock_handle.close()


if __name__ == "__main__":
    raise SystemExit(main())
