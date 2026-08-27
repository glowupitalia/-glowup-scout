"""Detached worker entry point for a persisted Discovery checkpoint."""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

import requests

from discovery import DiscoveryCheckpointStore, run_discovery
from discovery_amazon import (
    RefreshingTokenProvider,
    correlate_catalog_items,
    get_item_offers_batch,
    parse_item_offers_batch,
    search_catalog_by_gtins_batch,
)
from discovery_excel import write_discovery_excel
from discovery_jobs import DiscoveryJobRegistry, PROJECT_ROOT
from notifications import send_discovery_terminal_notification
from product_fees import search_product_fees_batch


logger = logging.getLogger(__name__)


def load_env(path: Path = PROJECT_ROOT / ".env"):
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return
    for line in lines:
        if "=" in line and not line.lstrip().startswith("#"):
            key, value = line.strip().split("=", 1)
            os.environ.setdefault(key, value)


def get_access_token():
    response = requests.post(
        "https://api.amazon.com/auth/o2/token",
        data={
            "grant_type": "refresh_token",
            "refresh_token": os.environ["LWA_REFRESH_TOKEN"],
            "client_id": os.environ["LWA_CLIENT_ID"],
            "client_secret": os.environ["LWA_CLIENT_SECRET"],
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.json()["access_token"]


def execute(job_id: str, *, registry=None, checkpoint_store=None):
    load_env()
    registry = registry or DiscoveryJobRegistry()
    checkpoint_store = checkpoint_store or DiscoveryCheckpointStore()
    pid = os.getpid()
    if not registry.claim(job_id, pid=pid):
        raise RuntimeError(f"Discovery job {job_id} is already owned by another worker")
    state = checkpoint_store.load(job_id)
    token_provider = RefreshingTokenProvider(get_access_token)

    def catalog_batch(gtins, current_job_id, products=None):
        items = search_catalog_by_gtins_batch(
            gtins, token_provider,
            marketplace_id=os.environ["MARKETPLACE_ID"], job_id=current_job_id,
        )
        return correlate_catalog_items(gtins, items, products)

    def pricing_batch(asins, current_job_id):
        entries = get_item_offers_batch(
            asins, token_provider,
            marketplace_id=os.environ["MARKETPLACE_ID"], job_id=current_job_id,
        )
        return parse_item_offers_batch(entries)

    def progress(phase, progress_state):
        registry.heartbeat(
            job_id, pid=pid,
            phase=progress_state.get("progress_phase") or phase,
            current=progress_state.get("progress_current"),
            total=progress_state.get("progress_total") or progress_state.get("sampled_identifier_count"),
        )

    try:
        result = run_discovery(
            state["filters"], checkpoint_store=checkpoint_store,
            catalog_batch=catalog_batch, pricing_batch=pricing_batch,
            fees_batch=search_product_fees_batch, token_provider=token_provider,
            job_id=job_id, selected_suppliers=state.get("selected_suppliers"),
            run_budget=state.get("run_budget"), progress=progress,
        )
        output_path = None
        if result.get("status") == "completed":
            output_path = PROJECT_ROOT / "data" / "discovery_jobs" / f"{job_id}.xlsx"
            write_discovery_excel(result, str(output_path))
            result["export_state"] = {
                "status": "generated", "file_name": output_path.name,
                "generated_at": result.get("completed_at") or result.get("updated_at"),
                "result_products": len(result.get("results") or []),
            }
        else:
            result["export_state"] = {
                "status": "pending", "generated_at": None,
                "result_products": len(result.get("results") or []),
            }
        checkpoint_store.save(result)
        registry.finish(job_id, result, export_path=str(output_path) if output_path else None)
        try:
            send_discovery_terminal_notification(
                result, database_path=registry.path, runtime=registry.get(job_id),
            )
        except Exception:
            logger.error(
                "DISCOVERY NOTIFICATION FAILED | job_id=%s", job_id,
            )
        return result
    except Exception as exc:
        logger.exception("DISCOVERY WORKER FAILED | job_id=%s", job_id)
        registry.fail(job_id, str(exc))
        raise


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--job-id", required=True)
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    execute(args.job_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
