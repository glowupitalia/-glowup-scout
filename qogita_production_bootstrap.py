#!/usr/bin/env python3
"""Restart-safe operator for one explicitly configured Qogita bootstrap run."""

from __future__ import annotations

import argparse
import fcntl
import json
import logging
import os
import shutil
import sys
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from qogita_bootstrap import (
    QogitaBootstrapClient,
    QogitaBootstrapStore,
    run_qogita_bootstrap_concurrent,
)
from supplier_catalog import DEFAULT_DATABASE_PATH
from qogita_serving import (
    QogitaServingStore, REST_WINDOW_SECONDS, RUN_WINDOW_SECONDS,
)


ROOT = Path(__file__).resolve().parent
DEFAULT_POINTER = ROOT / "data" / "qogita_bootstrap_current.json"
DEFAULT_LOCK = ROOT / "data" / "qogita-bootstrap.lock"
MINIMUM_OFFERS_PACING = 1.15
DEFAULT_MINIMUM_FREE_BYTES = 15 * 1024**3
MILESTONES = (25000, 50000, 100000, 200000)


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
                 database: Path, minimum_free_bytes: int):
        self.store = store
        self.bootstrap_run_id = bootstrap_run_id
        self.database = database
        self.minimum_free_bytes = int(minimum_free_bytes)
        self.started = time.monotonic()
        run = store.bootstrap(bootstrap_run_id) or {}
        self.metric_baseline = {
            "http_429": int(run.get("rate_limit_count") or 0),
            "http_5xx": int(run.get("server_error_count") or 0),
        }
        self.last_metrics: dict[str, int] = dict(self.metric_baseline)
        self.recent_429: deque[float] = deque()
        self.consecutive_errors = 0
        self.errors = 0

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
        if outcome.get("status") == "success":
            self.consecutive_errors = 0
        else:
            self.consecutive_errors += 1
            self.errors += 1
        error_code = str(outcome.get("error_code") or "")
        if error_code in {"variant_fid_conflict", "offers_duplicate_scenario_identity"}:
            return error_code
        if "authentication" in error_code:
            return "persistent_authentication_failure"
        if len(self.recent_429) >= 5:
            return "five_http_429_within_10_minutes"
        if self.consecutive_errors >= 10:
            return "ten_consecutive_product_errors"
        if (int(metrics.get("http_5xx", 0))
                - self.metric_baseline["http_5xx"] >= 20):
            return "twenty_http_5xx"
        processed = int(payload.get("processed") or 0)
        if processed >= 100 and self.errors >= 10 and self.errors / processed > 0.10:
            return "unexpected_product_error_rate"
        if processed % 100 == 0:
            storage = storage_snapshot(self.database)
            if storage["disk_free_bytes"] < self.minimum_free_bytes:
                return "disk_free_below_guardrail"
        return None

    def checkpoint(self, run: dict[str, Any]):
        self.store.validate_production_source(self.bootstrap_run_id)
        progress = dict(run.get("last_progress") or {})
        storage = storage_snapshot(self.database)
        if storage["disk_free_bytes"] < self.minimum_free_bytes:
            raise RuntimeError("disk_free_below_guardrail")
        elapsed = max(0.001, float(run.get("wall_elapsed_seconds") or 0))
        completed = int(progress.get("offers_success") or 0)
        reusable = int(run.get("reusable_products") or 0)
        newly_completed = max(0, completed - reusable)
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

        while True:
            duty = serving_store.ensure_running_window(
                pointer["bootstrap_run_id"], run_window_seconds=args.run_window_seconds,
                rest_window_seconds=args.rest_window_seconds,
            )
            if duty["state"] == "completed":
                logging.info("Qogita duty cycle is complete and awaits promotion review")
                return 0
            if duty["state"] == "auto_stopped":
                logging.error("Qogita duty cycle is auto-stopped; operator review required")
                return 0
            if duty["state"] == "checkpointing":
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

            store.resume_production(pointer["bootstrap_run_id"])
            guard = ProductionHealthGuard(
                store=store, bootstrap_run_id=pointer["bootstrap_run_id"],
                database=database, minimum_free_bytes=args.minimum_free_bytes,
            )
            initial = guard.initial_check()
            logging.info(
                "Qogita duty window %s preflight: %s", duty["window_number"], initial,
            )
            deadline = datetime.fromisoformat(
                duty["current_window_deadline"].replace("Z", "+00:00")
            ).astimezone(timezone.utc)
            result = run_qogita_bootstrap_concurrent(
                pointer["bootstrap_run_id"], store=store, client_factory=client_factory,
                workers=2, max_products=args.max_products,
                checkpoint_every=max(1, args.checkpoint_every),
                product_link_pacing=args.product_link_pacing,
                offers_pacing=args.offers_pacing,
                health_callback=guard.product, checkpoint_callback=guard.checkpoint,
                graceful_stop_callback=lambda: datetime.now(timezone.utc) >= deadline,
                **credentials,
            )
            logging.info(
                "Qogita duty invocation ended: status=%s attempted=%s graceful=%s",
                result.get("status"), result.get("invocation_products_attempted"),
                result.get("graceful_stop"),
            )
            if result.get("auto_stop_reason"):
                serving_store.mark_auto_stopped(pointer["bootstrap_run_id"])
                return 0
            progress = result.get("last_progress") or {}
            complete = result.get("status") == "awaiting_promotion_review" or not int(
                progress.get("remaining") or 0
            )
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
    except Exception as exc:
        logging.exception("Qogita production bootstrap stopped safely: %s", type(exc).__name__)
        store.mark_stopped(pointer["bootstrap_run_id"], str(exc))
        serving_store.mark_auto_stopped(pointer["bootstrap_run_id"])
        return 0
    finally:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
        lock_handle.close()


if __name__ == "__main__":
    raise SystemExit(main())
