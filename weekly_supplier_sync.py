#!/usr/bin/env python3
"""CLI boundary for the Scout weekly supplier pipeline.

Live supplier handlers are loaded from ``supplier_weekly_adapters``. The CLI
never includes Qogita and refuses to run when an adapter is unavailable.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone

from supplier_catalog import SupplierCatalogStore
from supplier_weekly import (
    DEFAULT_WEEKLY_DATABASE_PATH, WEEKLY_SUPPLIERS, WeeklySupplierOrchestrator,
    WeeklySupplierStore, next_weekly_refresh, weekly_lock,
)


def _baseline(supplier: str) -> str | None:
    metadata = SupplierCatalogStore().active_generation_metadata(supplier)
    return metadata.get("run_id") if metadata else None


def _handlers():
    try:
        from supplier_weekly_adapters import build_weekly_handlers
        return build_weekly_handlers()
    except ImportError:
        return {}


def status_payload(store: WeeklySupplierStore) -> dict:
    return {
        "timezone": "Europe/Rome",
        "next_refresh": next_weekly_refresh().isoformat(),
        "suppliers": {
            supplier: store.latest_supplier_state(supplier)
            for supplier in WEEKLY_SUPPLIERS
        },
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("status", "run", "dry-run", "import-abw"))
    parser.add_argument("--trigger", choices=("manual", "scheduled"), default="manual")
    parser.add_argument("--abw-export")
    parser.add_argument("--database", default=str(DEFAULT_WEEKLY_DATABASE_PATH))
    args = parser.parse_args(argv)
    store = WeeklySupplierStore(args.database)
    if args.action == "status":
        print(json.dumps(status_payload(store), ensure_ascii=False, default=str))
        return 0
    if args.action == "dry-run":
        handlers = _handlers()
        print(json.dumps({
            "status": "dry_run", "live_calls": 0,
            "order": list(WEEKLY_SUPPLIERS),
            "suppliers": {
                supplier: {
                    "baseline": _baseline(supplier),
                    "handler": "waiting_for_source" if supplier == "abw" and not args.abw_export
                    else ("incremental" if supplier in handlers else "unavailable"),
                    "source": "manual_official_xlsx" if supplier == "abw" and args.abw_export else None,
                } for supplier in WEEKLY_SUPPLIERS
            },
        }, ensure_ascii=False))
        return 0
    if args.action == "import-abw" and not args.abw_export:
        parser.error("import-abw requires --abw-export")
    scheduled_at = datetime.now(timezone.utc) if args.trigger == "scheduled" else None
    if scheduled_at and store.has_completed_schedule(scheduled_at):
        print(json.dumps({
            "status": "skipped", "reason": "weekly_calendar_run_already_completed",
            "scheduled_at": scheduled_at.isoformat(),
        }))
        return 0
    try:
        with weekly_lock():
            handlers = _handlers()
            if args.action == "import-abw":
                handlers = {
                    "abw": handlers["abw"],
                    "umma": lambda **_: {"status": "skipped", "baseline_after": _baseline("umma"),
                                          "promotion_result": "baseline_preserved"},
                    "qudo": lambda **_: {"status": "skipped", "baseline_after": _baseline("qudo"),
                                          "promotion_result": "baseline_preserved"},
                }
            orchestrator = WeeklySupplierOrchestrator(
                handlers, store=store, baseline_provider=_baseline,
            )
            result = orchestrator.run(
                trigger_type=args.trigger,
                scheduled_at=scheduled_at,
                sources={"abw": args.abw_export} if args.abw_export else {},
            )
    except RuntimeError as exc:
        if str(exc) != "weekly_supplier_sync_already_running":
            raise
        result = {"status": "skipped", "reason": str(exc)}
    print(json.dumps(result, ensure_ascii=False, default=str))
    return 0 if result["status"] in {"success", "partial_success", "skipped"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
