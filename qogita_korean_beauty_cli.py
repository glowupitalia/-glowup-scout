#!/usr/bin/env python3
"""Operator-safe dry-run CLI for Qogita Korean Beauty membership acquisition."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from qogita_korean_beauty import (
    QogitaKoreanBeautyCollector, active_qogita_context,
    refresh_korean_beauty_membership,
)
from supplier_catalog import DEFAULT_DATABASE_PATH


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Acquire and reconcile Qogita Korean Beauty without production writes",
    )
    result.add_argument("--database", default=str(DEFAULT_DATABASE_PATH))
    result.add_argument("--pacing", type=float, default=0.35)
    result.add_argument("--max-attempts", type=int, default=5)
    result.add_argument("--max-pages", type=int)
    result.add_argument(
        "--persist", action="store_true",
        help="Persist a validated version in the supplier DB after acquisition",
    )
    result.add_argument(
        "--activate", action="store_true",
        help="Atomically activate the newly persisted valid membership",
    )
    result.add_argument(
        "--execute", action="store_true",
        help="Perform public curated-search GETs (persistence requires --persist)",
    )
    return result


def main(argv=None) -> int:
    args = parser().parse_args(argv)
    if args.activate and not args.persist:
        raise SystemExit("--activate requires --persist")
    database = Path(args.database).expanduser().resolve()
    context = active_qogita_context(database)
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
    # Reuse the same orchestration contract as the weekly scheduler.  The
    # collector has already run, so expose its result through a one-shot shim.
    class Collected:
        def collect(self, **_kwargs):
            return acquisition

    report = refresh_korean_beauty_membership(
        path=database, collector=Collected(), persist=args.persist,
        activate=args.activate, max_pages=args.max_pages,
    )
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
