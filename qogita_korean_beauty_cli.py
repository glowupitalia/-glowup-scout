#!/usr/bin/env python3
"""Operator-safe dry-run CLI for Qogita Korean Beauty membership acquisition."""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

from qogita_korean_beauty import (
    QogitaKoreanBeautyCollector,
    QogitaMembershipReconciler,
)
from supplier_catalog import DEFAULT_DATABASE_PATH


def _production_context(database: Path) -> dict[str, str]:
    connection = sqlite3.connect(f"file:{database.resolve()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        row = connection.execute(
            """SELECT snapshot.source_generation_id,snapshot.bootstrap_run_id,
                      snapshot.serving_generation_id
                 FROM qogita_serving_active active
                 JOIN qogita_serving_snapshots snapshot
                   ON snapshot.serving_generation_id=active.serving_generation_id
                WHERE active.supplier='qogita' AND snapshot.status='valid'"""
        ).fetchone()
    finally:
        connection.close()
    if not row:
        raise SystemExit("No active valid Qogita serving snapshot")
    return dict(row)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Acquire and reconcile Qogita Korean Beauty without production writes",
    )
    result.add_argument("--database", default=str(DEFAULT_DATABASE_PATH))
    result.add_argument("--pacing", type=float, default=0.35)
    result.add_argument("--max-attempts", type=int, default=5)
    result.add_argument("--max-pages", type=int)
    result.add_argument(
        "--execute", action="store_true",
        help="Perform public curated-search GETs; never writes or activates membership",
    )
    return result


def main(argv=None) -> int:
    args = parser().parse_args(argv)
    database = Path(args.database).expanduser().resolve()
    context = _production_context(database)
    if not args.execute:
        print(json.dumps({
            "status": "authorization_required",
            "production_writes": False,
            "membership_activation": False,
            **context,
        }, sort_keys=True))
        return 0
    collector = QogitaKoreanBeautyCollector(
        pacing_seconds=args.pacing, max_attempts=args.max_attempts,
    )
    try:
        acquisition = collector.collect(max_pages=args.max_pages)
    finally:
        collector.close()
    reconciliation = QogitaMembershipReconciler(database).reconcile(
        acquisition["entries"],
        source_generation_id=context["source_generation_id"],
        bootstrap_run_id=context["bootstrap_run_id"],
        serving_generation_id=context["serving_generation_id"],
    )
    report = {
        "status": "dry_run_complete",
        "production_writes": False,
        "membership_activation": False,
        **context,
        "acquisition_status": acquisition["acquisition_status"],
        "curated": acquisition["metrics"],
        "catalog_bootstrap_serving": reconciliation["metrics"],
        "catalog_absent_gtins": reconciliation["catalog_absent_gtins"],
        "gtin_fid_conflicts": acquisition["gtin_fid_conflicts"],
        "fid_gtin_conflicts": acquisition["fid_gtin_conflicts"],
    }
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
