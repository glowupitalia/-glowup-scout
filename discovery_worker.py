"""Detached worker entry point for a persisted Discovery checkpoint."""

from __future__ import annotations

import argparse
import hashlib
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
from discovery_incremental import (
    DiscoveryIncrementalStore,
    LightweightCheckpointStore,
    prepare_incremental_job,
)
from discovery_incremental_runner import ResourcePause, run_incremental_discovery
from discovery_resources import DiscoveryResourceGovernor
from discovery_jobs import DiscoveryJobRegistry, PROJECT_ROOT
from notifications import send_discovery_terminal_notification
from product_fees import search_product_fees_batch
from supplier_catalog import SupplierCatalogStore
from discovery_rotation import DiscoveryRotationStore
from discovery_freshness import DiscoveryAmazonCache
from storage_gc import (
    append_storage_audit_event,
    collect_storage_metrics,
    evaluate_discovery_admission,
    production_retention_plan,
)


logger = logging.getLogger(__name__)


def _export_metadata(path: Path, result: dict) -> dict:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    details = path.stat()
    return {
        "status": "completed",
        "valid": True,
        "job_id": result["job_id"],
        "file_name": path.name,
        "file_size": details.st_size,
        "sha256": digest.hexdigest(),
        "generated_at": result.get("completed_at") or result.get("updated_at"),
        "result_products": len(result.get("results") or []),
    }


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
    runtime = registry.get(job_id) or {}
    selected_suppliers = runtime.get("selected_suppliers") or []
    qogita_universe = "full"
    for hint_path in (checkpoint_store.state_path(job_id), checkpoint_store.path(job_id)):
        try:
            if hint_path.is_file() and hint_path.stat().st_size <= 1024 * 1024:
                import json
                with hint_path.open("r", encoding="utf-8") as source:
                    qogita_universe = str(json.load(source).get("qogita_universe") or "full")
                break
        except (OSError, ValueError, TypeError):
            # FULL is the conservative budget when a legacy hint is unavailable.
            qogita_universe = "full"
    storage_before = collect_storage_metrics()
    admission = evaluate_discovery_admission(
        selected_suppliers, qogita_universe, metrics=storage_before,
    )
    if not admission["allowed"]:
        plan = production_retention_plan(
            str((admission.get("watermark") or {}).get("state") or "UNKNOWN")
        )
        admission["retention_plan"] = plan.get("summary")
        registry.admission_blocked(job_id, decision=admission)
        append_storage_audit_event({
            "event": "admission", "workload_id": job_id,
            "decision": admission, "storage_before": storage_before,
            "retention_execution": False,
        }, path=Path(registry.path).parent / "storage-workload-metrics.jsonl")
        logger.warning(
            "DISCOVERY ADMISSION BLOCKED | job_id=%s reason=%s",
            job_id, admission.get("reason"),
        )
        return {
            "job_id": job_id, "status": "admission_blocked_storage",
            "phase": "admission_blocked_storage", "storage_admission": admission,
        }
    append_storage_audit_event({
        "event": "admission", "workload_id": job_id,
        "decision": admission, "storage_before": storage_before,
        "retention_execution": False,
    }, path=Path(registry.path).parent / "storage-workload-metrics.jsonl")
    if not registry.claim(job_id, pid=pid):
        raise RuntimeError(f"Discovery job {job_id} is already owned by another worker")
    incremental_store = DiscoveryIncrementalStore()
    amazon_cache = DiscoveryAmazonCache(incremental_store)
    resource_governor = DiscoveryResourceGovernor(database_path=incremental_store.path)

    def preparation_progress(phase, current, total):
        registry.heartbeat(
            job_id, pid=pid, phase=phase, current=current, total=total,
        )

    try:
        incremental = incremental_store.has_job(job_id)
        incremental_state = incremental_store.summary(job_id) if incremental else None
        preparing = bool(
            incremental_state
            and (
                incremental_state.get("phase") == "preparing"
                or (
                    incremental_state.get("prepared_total")
                    and not incremental_state.get("preparation_complete")
                )
            )
        )
        state = (
            checkpoint_store.load(job_id)
            if not incremental or preparing
            else LightweightCheckpointStore().load(job_id)
        )
        if (
            (not incremental or preparing) and state.get("phase") == "initialized"
            and state.get("selected_suppliers")
            and Path(checkpoint_store.root) == Path("data/discovery_jobs")
        ):
            prepared_current = int(
                (incremental_state or {}).get("prepared_current")
                or (incremental_state or {}).get("selected_count") or 0
            )
            preparation_progress("preparing", prepared_current, int(
                state.get("progress_total") or state.get("sampled_identifier_count") or 0
            ))
            prepared = prepare_incremental_job(
                state, supplier_store=SupplierCatalogStore(),
                rotation_store=DiscoveryRotationStore(),
                amazon_cache=amazon_cache, start_sequence=prepared_current,
                progress=preparation_progress, resource_governor=resource_governor,
            )
            incremental_store.create_job(
                prepared["metadata"], prepared["candidates"],
                progress=preparation_progress, resource_governor=resource_governor,
            )
            state = incremental_store.summary(job_id)
            LightweightCheckpointStore().save(state)
            incremental = True
    except ResourcePause as exc:
        metrics = exc.snapshot.as_dict()
        if incremental_store.has_job(job_id):
            incremental_store.record_resource_event(
                job_id, "hard", exc.reason, {**metrics, "threshold": exc.threshold},
            )
            incremental_store.set_phase(job_id, "resource_paused", status="resource_paused")
        registry.resource_pause(
            job_id, reason=exc.reason, metrics=metrics, phase="preparing",
        )
        logger.warning(
            "DISCOVERY PREPARATION RESOURCE PAUSED | job_id=%s reason=%s",
            job_id, exc.reason,
        )
        return {
            "job_id": job_id, "status": "resource_paused", "phase": "preparing",
            "resource_pause": {
                "reason": exc.reason, "observed": metrics,
                "threshold": exc.threshold, "resumable": True,
            },
        }
    except Exception as exc:
        logger.exception("DISCOVERY PREPARATION FAILED | job_id=%s", job_id)
        registry.fail(job_id, str(exc))
        raise

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
        if incremental:
            result = run_incremental_discovery(
                job_id, store=incremental_store,
                metadata_store=LightweightCheckpointStore(),
                catalog_batch=catalog_batch, pricing_batch=pricing_batch,
                fees_batch=search_product_fees_batch, token_provider=token_provider,
                rotation_store=DiscoveryRotationStore(),
                amazon_cache=amazon_cache,
                progress=progress,
                resource_governor=resource_governor,
            )
        else:
            result = run_discovery(
                state["filters"], checkpoint_store=checkpoint_store,
                catalog_batch=catalog_batch, pricing_batch=pricing_batch,
                fees_batch=search_product_fees_batch, token_provider=token_provider,
                job_id=job_id, selected_suppliers=state.get("selected_suppliers"),
                run_budget=state.get("run_budget"), progress=progress,
            )
        if incremental and result.get("status") == "completed":
            amazon_cache.index_completed_jobs(
                progress=lambda _phase, _current, _total: registry.heartbeat(
                    job_id, pid=pid,
                )
            )
            # Persist a small hand-off and let a clean interpreter perform the
            # export.  The computation process can then exit and the OS releases
            # every Catalog/economics object before workbook generation starts.
            result.update({"status": "computed", "phase": "export_pending"})
            incremental_store.set_phase(job_id, "export_pending", status="computed")
            LightweightCheckpointStore().save(result)
            registry.prepare_finalization(job_id, result)
            finalizer_pid = registry.launch_finalizer(job_id)
            logger.info(
                "DISCOVERY COMPUTATION HANDED OFF | job_id=%s finalizer_pid=%s",
                job_id, finalizer_pid,
            )
            append_storage_audit_event({
                "event": "workload_computed", "workload_id": job_id,
                "workload_type": admission.get("workload_type"),
                "universe_size": int(result.get("selected_count") or 0),
                "success": True, "storage_before": storage_before,
                "storage_after": collect_storage_metrics(),
                "retention_execution": False,
            }, path=Path(registry.path).parent / "storage-workload-metrics.jsonl")
            return result

        output_path = None
        if result.get("status") == "completed":
            output_path = PROJECT_ROOT / "data" / "discovery_jobs" / f"{job_id}.xlsx"
            export_result = result
            write_discovery_excel(export_result, str(output_path))
            result["export_state"] = _export_metadata(output_path, result)
        else:
            result["export_state"] = {
                "status": "pending", "generated_at": None,
                "result_products": len(result.get("results") or []),
            }
        if incremental:
            LightweightCheckpointStore().save(result)
        else:
            checkpoint_store.save(result)
        registry.finish(job_id, result, export_path=str(output_path) if output_path else None)
        append_storage_audit_event({
            "event": "workload_completed", "workload_id": job_id,
            "workload_type": admission.get("workload_type"),
            "universe_size": int(result.get("sampled_identifier_count") or 0),
            "export_bytes": output_path.stat().st_size if output_path else 0,
            "success": result.get("status") == "completed",
            "storage_before": storage_before,
            "storage_after": collect_storage_metrics(),
            "retention_execution": False,
        }, path=Path(registry.path).parent / "storage-workload-metrics.jsonl")
        try:
            send_discovery_terminal_notification(
                result, database_path=registry.path, runtime=registry.get(job_id),
            )
        except Exception:
            logger.error(
                "DISCOVERY NOTIFICATION FAILED | job_id=%s", job_id,
            )
        return result
    except ResourcePause as exc:
        metrics = exc.snapshot.as_dict()
        incremental_store.record_resource_event(
            job_id, "hard", exc.reason, {**metrics, "threshold": exc.threshold},
        )
        incremental_store.set_phase(job_id, "resource_paused", status="resource_paused")
        paused = {
            **incremental_store.summary(job_id),
            "status": "resource_paused", "phase": "resource_paused",
            "resource_pause": {
                "reason": exc.reason, "metric": exc.reason,
                "observed": metrics, "threshold": exc.threshold,
                "resumable": True,
            },
        }
        LightweightCheckpointStore().save(paused)
        registry.resource_pause(
            job_id, reason=exc.reason, metrics=metrics,
            phase=paused.get("progress_phase") or "resource_paused",
        )
        logger.warning("DISCOVERY RESOURCE PAUSED | job_id=%s reason=%s", job_id, exc.reason)
        return paused
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
