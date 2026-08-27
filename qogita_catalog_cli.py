#!/usr/bin/env python3
"""Operator CLI for the gated Qogita supplier-first catalog pipeline."""

from __future__ import annotations

import argparse
import json
import os

from qogita_catalog_pipeline import (
    QogitaCatalogDownloader,
    QogitaCatalogPipelineStore,
    QogitaCatalogRequestClient,
    download_pending_catalog,
    prepare_staging_generation,
    validate_qogita_catalog,
)
from supplier_catalog import SupplierCatalogStore
from qogita_bootstrap import (
    QogitaBootstrapClient,
    QogitaBootstrapStore,
    run_qogita_bootstrap,
    run_qogita_bootstrap_concurrent,
)


def _client():
    required = {
        "base_url": os.environ.get("QOGITA_BASE_URL", "https://api.qogita.com"),
        "email": os.environ.get("QOGITA_EMAIL"),
        "password": os.environ.get("QOGITA_PASSWORD"),
    }
    if not required["email"] or not required["password"]:
        raise SystemExit("QOGITA_EMAIL and QOGITA_PASSWORD are required")
    return QogitaCatalogRequestClient(**required)


def parser():
    root = argparse.ArgumentParser(description="Qogita supplier-first catalog operations")
    root.add_argument("--database", help="Scout supplier catalog SQLite path")
    commands = root.add_subparsers(dest="command", required=True)
    request = commands.add_parser("request-full", help="Request a body-empty full catalog export")
    request.add_argument("--execute", action="store_true", help="Actually call Qogita")
    commands.add_parser("show-pending")
    download = commands.add_parser("download")
    download.add_argument("catalog_request_id")
    download.add_argument("--destination", required=True)
    download.add_argument("--allowed-host", action="append", default=[])
    validate = commands.add_parser("validate")
    validate.add_argument("catalog_request_id")
    stage = commands.add_parser("prepare-staging")
    stage.add_argument("catalog_request_id")
    stage.add_argument("--allow-quarantine", action="store_true")
    stage.add_argument("--previous-run-id")
    stage.add_argument("--reuse-unchanged-scenarios-after")
    queue = commands.add_parser("show-enrichment-queue")
    queue.add_argument("run_id")
    queue.add_argument("--limit", type=int, default=100)
    probe = commands.add_parser("rate-limit-probe")
    probe.add_argument("variant_fid", nargs="+", help="1-50 variant FIDs")
    probe.add_argument("--pacing", type=float, default=0.5)
    probe.add_argument("--execute", action="store_true", help="Actually call /offers/")
    create_bootstrap = commands.add_parser(
        "create-bootstrap", help="Create a deterministic staged-catalog enrichment checkpoint",
    )
    create_bootstrap.add_argument("staging_run_id")
    create_bootstrap.add_argument("--target-count", type=int, required=True)
    create_bootstrap.add_argument("--batch-size", type=int, default=100)
    create_bootstrap.add_argument("--bootstrap-run-id")
    create_bootstrap.add_argument("--exclude-bootstrap-run-id", action="append", default=[])
    show_bootstrap = commands.add_parser("show-bootstrap")
    show_bootstrap.add_argument("bootstrap_run_id")
    run_bootstrap = commands.add_parser("run-bootstrap")
    run_bootstrap.add_argument("bootstrap_run_id")
    run_bootstrap.add_argument("--max-products", type=int)
    run_bootstrap.add_argument("--batch-size", type=int)
    run_bootstrap.add_argument("--product-link-pacing", type=float, default=0.6)
    run_bootstrap.add_argument("--offers-pacing", type=float, default=1.0)
    run_bootstrap.add_argument("--workers", type=int, choices=(1, 2), default=1)
    run_bootstrap.add_argument("--execute", action="store_true")
    return root


def main(argv=None):
    args = parser().parse_args(argv)
    pipeline = QogitaCatalogPipelineStore(args.database) if args.database else QogitaCatalogPipelineStore()
    catalog = SupplierCatalogStore(args.database) if args.database else SupplierCatalogStore()
    if args.command == "request-full":
        if not args.execute:
            result = {
                "status": "authorization_required", "request_mode": "full",
                "filters": {}, "request_body": {}, "remote_call_performed": False,
            }
        else:
            client = _client()
            try:
                request_id = client.request_full_catalog()
            finally:
                client.close()
            result = pipeline.create_request(
                request_id, request_mode="full", filters={}, request_body={},
            )
    elif args.command == "show-pending":
        result = pipeline.pending_requests()
    elif args.command == "download":
        hosts = set(args.allowed_host) | {
            value.strip() for value in os.environ.get(
                "QOGITA_CATALOG_DOWNLOAD_HOSTS", ""
            ).split(",") if value.strip()
        }
        downloader = QogitaCatalogDownloader(args.destination, allowed_hosts=hosts)
        result = download_pending_catalog(
            args.catalog_request_id, store=pipeline, downloader=downloader,
        )
    elif args.command == "validate":
        request = pipeline.request(args.catalog_request_id)
        if not request or not request.get("local_file_path"):
            raise SystemExit("Request has no downloaded catalog")
        previous = catalog.active_generation_metadata("qogita")
        result = validate_qogita_catalog(
            request["local_file_path"], request_mode=request["request_mode"],
            filters=request["filters"], previous_row_count=(previous or {}).get("product_count"),
        )
        pipeline.save_validation(args.catalog_request_id, result)
    elif args.command == "prepare-staging":
        result = prepare_staging_generation(
            args.catalog_request_id, pipeline_store=pipeline, catalog_store=catalog,
            allow_quarantine=args.allow_quarantine,
            previous_run_id=args.previous_run_id,
            reuse_unchanged_scenarios_after=args.reuse_unchanged_scenarios_after,
        )
    elif args.command == "show-enrichment-queue":
        result = pipeline.enrichment_queue(args.run_id, limit=args.limit)
    elif args.command == "rate-limit-probe":
        if not 1 <= len(args.variant_fid) <= 50:
            raise SystemExit("Provide between 1 and 50 variant FIDs")
        if not args.execute:
            result = {
                "status": "authorization_required", "variant_count": len(args.variant_fid),
                "pacing_seconds": args.pacing, "remote_call_performed": False,
            }
        else:
            client = _client()
            try:
                result = client.rate_limit_probe(args.variant_fid, pacing_seconds=args.pacing)
            finally:
                client.close()
    elif args.command == "create-bootstrap":
        bootstrap = QogitaBootstrapStore(args.database) if args.database else QogitaBootstrapStore()
        result = bootstrap.create_bootstrap(
            args.staging_run_id, target_count=args.target_count,
            batch_size=args.batch_size, bootstrap_run_id=args.bootstrap_run_id,
            exclude_bootstrap_run_ids=tuple(args.exclude_bootstrap_run_id),
        )
    elif args.command == "show-bootstrap":
        bootstrap = QogitaBootstrapStore(args.database) if args.database else QogitaBootstrapStore()
        result = bootstrap.bootstrap(args.bootstrap_run_id)
        if result is None:
            raise SystemExit("Bootstrap run not found")
    elif args.command == "run-bootstrap":
        bootstrap = QogitaBootstrapStore(args.database) if args.database else QogitaBootstrapStore()
        if not args.execute:
            result = {
                "status": "authorization_required",
                "bootstrap_run_id": args.bootstrap_run_id,
                "max_products": args.max_products,
                "remote_call_performed": False,
            }
        else:
            base = _client()
            credentials = {
                "base_url": base.base_url, "email": base.email, "password": base.password,
            }
            base.close()
            def client_factory(auth_manager, rate_limiter):
                return QogitaBootstrapClient(
                    **credentials, auth_manager=auth_manager, rate_limiter=rate_limiter,
                )
            result = run_qogita_bootstrap_concurrent(
                args.bootstrap_run_id, store=bootstrap, client_factory=client_factory,
                workers=args.workers, max_products=args.max_products,
                product_link_pacing=args.product_link_pacing,
                offers_pacing=args.offers_pacing, **credentials,
            )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
