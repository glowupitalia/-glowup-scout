#!/usr/bin/env python3
"""Manual entry point for Scout's supplier-first catalog generations."""

from __future__ import annotations

import argparse
import json
import os
import time

from supplier_catalog import SupplierCatalogStore, run_supplier_sync, supplier_catalog_lock
from supplier_catalog_collectors import (
    QOGITA_EXPORT_COVERAGE,
    QogitaCatalogExportReader,
    build_collector,
)


def _aliases(values):
    configured = list(values or [])
    configured.extend(
        part.strip()
        for part in os.environ.get("SCOUT_QOGITA_SELLER_ALIASES", "").split(",")
        if part.strip()
    )
    return sorted(set(configured))


def parser():
    value = argparse.ArgumentParser(
        description="Synchronize one supplier-first catalog into Scout's local cache."
    )
    value.add_argument("--supplier", required=True, choices=("qogita", "umma", "abw", "qudo"))
    value.add_argument("--database", help="Scout supplier catalog SQLite path")
    value.add_argument("--limit", type=int, help="Sample products; sample generations are never promoted")
    value.add_argument("--dry-run", action="store_true", help="Collect without writing the cache")
    value.add_argument(
        "--no-promote", action="store_true",
        help="Persist a successful generation without activating it",
    )
    value.add_argument("--seller-alias", action="append", default=[], help="Qogita seller alias (repeatable)")
    value.add_argument(
        "--source-file", help="Official supplier export to parse locally (Qogita CSV or ABW XLSX)",
    )
    return value


def _sync_qogita_export(args, store):
    reader = QogitaCatalogExportReader(args.source_file)
    metadata = reader.metadata()
    started = time.monotonic()
    with supplier_catalog_lock("qogita") as acquired:
        if not acquired:
            return {"status": "skipped", "reason": "already_running", "supplier": "qogita"}
        if args.dry_run:
            count = sum(1 for _ in reader.products(limit=args.limit))
            return {
                "status": "dry_run", "supplier": "qogita", "products": count,
                "scenarios": 0, "coverage_type": QOGITA_EXPORT_COVERAGE["type"],
                "coverage_complete": False, "metadata": metadata,
            }
        sampled = args.limit is not None
        run_id = store.start_run(
            "qogita", coverage_type=QOGITA_EXPORT_COVERAGE["type"],
            coverage_description=QOGITA_EXPORT_COVERAGE["description"],
            coverage_complete=False, sampled=sampled,
        )
        try:
            source_count = metadata.get("Original data rows")
            result = store.publish_product_catalog_stream(
                run_id, supplier="qogita", products=reader.products(limit=args.limit),
                elapsed_seconds=time.monotonic() - started,
                product_catalog_coverage_type="filtered_catalog",
                product_catalog_coverage_complete=False,
                scenario_enrichment_status="none",
                source_type="official_qogita_async_catalog_export",
                source_count=int(source_count) if source_count else None,
                export_generated_at=str(metadata.get("Catalog as of") or "").replace("Catalog As Of ", "") or None,
                upstream_catalog_version=metadata.get("Original file") or args.source_file,
                diagnostics={
                    "coverage_description": QOGITA_EXPORT_COVERAGE["description"],
                    "completeness_reason": (
                        "The supplied export is named Filtered; request filters have not been proven empty"
                    ),
                    "export_metadata": metadata,
                    "scenario_enrichment_limitation": (
                        "Lowest price is an aggregate signal; seller offer and tier enrichment is not included"
                    ),
                },
                promote=not sampled,
            )
            return {
                "run_id": run_id, "status": "sample_success" if sampled else "success",
                "supplier": "qogita", "products": result["product_count"],
                "scenarios": result["scenario_count"], "promoted": not sampled,
                "coverage_type": "filtered_catalog", "coverage_complete": False,
                "diagnostics": result,
            }
        except Exception as exc:
            store.fail(
                run_id, error_code=getattr(exc, "code", "sync_failed"),
                error_message=str(exc), elapsed_seconds=time.monotonic() - started,
            )
            return {"run_id": run_id, "status": "failed", "supplier": "qogita",
                    "error_code": getattr(exc, "code", "sync_failed")}


def main(argv=None):
    args = parser().parse_args(argv)
    aliases = _aliases(args.seller_alias)
    if args.supplier == "qogita" and not aliases and not args.source_file:
        parser().error("Qogita requires --seller-alias or SCOUT_QOGITA_SELLER_ALIASES")
    if args.source_file and args.supplier not in {"qogita", "abw"}:
        parser().error("--source-file is supported only for Qogita CSV or ABW XLSX")
    store = SupplierCatalogStore(args.database) if args.database else SupplierCatalogStore()
    if args.supplier == "qogita" and args.source_file:
        result = _sync_qogita_export(args, store)
        print(json.dumps(result, ensure_ascii=False, sort_keys=True, default=str))
        return 0 if result.get("status") in {"success", "sample_success", "dry_run", "skipped"} else 1
    collector = build_collector(
        args.supplier, qogita_seller_aliases=aliases, source_file=args.source_file,
    )
    result = run_supplier_sync(
        args.supplier, collector, store=store, limit=args.limit, dry_run=args.dry_run,
        promote=not args.no_promote,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, default=str))
    return 0 if result.get("status") in {"success", "sample_success", "dry_run", "skipped"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
