"""Bounded-memory execution of a persisted Discovery job."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from discovery import (
    _build_amazon_observations,
    _call_catalog_batch,
    _ensure_product_listings,
    _evaluate_product_combinations,
    _process_fee_batch,
    _sync_observation_fee_fields,
)
from discovery_incremental import (
    DiscoveryIncrementalStore,
    LightweightCheckpointStore,
)
from discovery_resources import DiscoveryResourceGovernor, ResourcePause


def _valid_price(value: Any) -> bool:
    try:
        return Decimal(str(value)) > 0
    except Exception:
        return False


def _checkpoint(
    store: DiscoveryIncrementalStore, metadata_store: LightweightCheckpointStore,
    job_id: str, *, progress_phase: str, progress_current: int, progress_total: int,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    state = store.summary(job_id)
    state.update({
        "progress_phase": progress_phase,
        "progress_current": int(progress_current),
        "progress_total": int(progress_total),
        **(extra or {}),
    })
    written = metadata_store.save(state)
    store.add_checkpoint_bytes(job_id, written)
    return state


def run_incremental_discovery(
    job_id: str, *, store: DiscoveryIncrementalStore,
    metadata_store: LightweightCheckpointStore,
    catalog_batch, pricing_batch, fees_batch, token_provider,
    rotation_store=None, resource_governor: DiscoveryResourceGovernor | None = None,
    progress=None, sleep_func=time.sleep, catalog_batch_interval: float = 0.5,
    pricing_batch_interval: float = 10.0, fee_batch_interval: float = 2.0,
) -> dict[str, Any]:
    """Resume the same immutable selection using only bounded record batches."""
    state = store.summary(job_id)
    filters = state["filters"]
    governor = resource_governor or DiscoveryResourceGovernor(database_path=store.path)
    selected = int(state["selected_count"])
    batch_number = int(state.get("last_completed_batch") or 0)

    # Catalog and rotation use separate SQLite stores. Reconcile committed
    # definitive results before claiming more work so a process death between
    # those two idempotent commits cannot leave the cycle behind.
    if rotation_store is not None and state.get("rotation_scope"):
        for statuses in store.iter_definitive_catalog_status_batches(job_id):
            rotation_store.commit_catalog_results(job_id, statuses)
    store.requeue_catalog_incomplete(job_id)

    # Catalog commit is the rotation commit point. A crash before the SQLite
    # transaction may repeat only that in-flight batch; after it, it is skipped.
    while True:
        batch = store.pending_catalog_batch(job_id, 20)
        if not batch:
            break
        governor.before_next_batch()
        identifiers = list(dict.fromkeys(row["gtin"] for row in batch))
        mapping = _call_catalog_batch(catalog_batch, identifiers, job_id, batch)
        for row in batch:
            _ensure_product_listings(
                row, mapping.get(row["gtin"], {"status": "not_found"})
            )
        batch_number += 1
        summary = store.commit_catalog_batch(job_id, batch, batch_number=batch_number)
        if batch_number % 50 == 0:
            store.passive_wal_checkpoint()
        if rotation_store is not None and state.get("rotation_scope"):
            rotation_store.commit_catalog_results(
                job_id,
                {
                    str(row.get("canonical_ean") or row.get("gtin")): row.get("catalog_status")
                    for row in batch
                },
            )
        state = _checkpoint(
            store, metadata_store, job_id, progress_phase="catalog",
            progress_current=summary["catalog_completed_count"], progress_total=selected,
        )
        if progress:
            progress("catalog", state)
        if any(row.get("catalog_status") == "catalog_incomplete" for row in batch):
            store.set_phase(job_id, "suppliers_loaded", status="waiting_retry")
            return _checkpoint(
                store, metadata_store, job_id, progress_phase="catalog",
                progress_current=summary["catalog_completed_count"],
                progress_total=selected,
                extra={"status": "waiting_retry", "catalog_retry_pending": True},
            )
        if catalog_batch_interval:
            sleep_func(catalog_batch_interval)

    store.set_phase(job_id, "catalog_complete")
    _checkpoint(
        store, metadata_store, job_id, progress_phase="catalog",
        progress_current=selected, progress_total=selected,
    )

    # Compatibility is already part of the Catalog result. Apply Beauty/BSR
    # record-by-record so the phase remains restart-safe and memory-bounded.
    if store.summary(job_id)["phase"] == "catalog_complete":
        processed = 0
        transformed = []
        for candidate in store.iter_candidates(job_id):
            for listing in candidate.get("amazon_listings") or []:
                if listing.get("compatibility_status") != "compatible":
                    listing["evaluation_status"] = "catalog_incompatible"
                    continue
                bsr = listing.get("bsr_beauty")
                if listing.get("beauty_status") != "display_group_beauty":
                    listing["evaluation_status"] = "beauty_filtered"
                    listing["exclusion_reason"] = "not_beauty_display_group"
                elif not isinstance(bsr, int) or isinstance(bsr, bool) or bsr <= 0:
                    listing["evaluation_status"] = "bsr_filtered"
                    listing["exclusion_reason"] = "invalid_bsr"
                elif not filters["bsr_min"] <= bsr <= filters["bsr_max"]:
                    listing["evaluation_status"] = "bsr_filtered"
                    listing["exclusion_reason"] = "bsr_out_of_range"
                else:
                    listing["evaluation_status"] = "bsr_passed"
            transformed.append(candidate)
            processed += 1
            if len(transformed) == 250:
                store.update_candidates(job_id, transformed)
                transformed = []
                governor.before_next_batch()
        if transformed:
            store.update_candidates(job_id, transformed)
        store.set_phase(job_id, "bsr_filtered")
        _checkpoint(
            store, metadata_store, job_id, progress_phase="bsr_filtered",
            progress_current=selected, progress_total=selected,
        )

    # Collect at most twenty unique pending ASINs for each Product Pricing call.
    while store.summary(job_id)["phase"] == "bsr_filtered":
        candidates: list[dict[str, Any]] = []
        asins: list[str] = []
        seen: set[str] = set()
        for candidate in store.iter_candidates(job_id):
            pending = [
                listing for listing in candidate.get("amazon_listings") or []
                if listing.get("evaluation_status") == "bsr_passed"
                and not listing.get("pricing_status")
            ]
            if pending:
                candidates.append(candidate)
            for listing in pending:
                asin = listing["asin"]
                if asin not in seen:
                    seen.add(asin)
                    asins.append(asin)
                if len(asins) == 20:
                    break
            if len(asins) == 20:
                break
        if not asins:
            store.set_phase(job_id, "pricing_complete")
            _checkpoint(
                store, metadata_store, job_id, progress_phase="pricing_complete",
                progress_current=selected, progress_total=selected,
            )
            break
        governor.before_next_batch()
        mapping = pricing_batch(asins, job_id)
        for candidate in candidates:
            for listing in candidate.get("amazon_listings") or []:
                if listing.get("asin") not in seen or listing.get("pricing_status"):
                    continue
                pricing = mapping.get(listing["asin"], {"status": "missing"})
                listing.update({
                    "pricing_status": pricing.get("status"),
                    "fba_sellers": pricing.get("Venditori FBA"),
                    "total_sellers": pricing.get("Venditori totali"),
                    "seller_count_source": pricing.get("Seller count source"),
                    "reference_price": pricing.get("reference_price"),
                    "price_source": pricing.get("price_source"),
                    "min_fba_price": pricing.get("Prezzo minimo FBA Amount"),
                    "min_fbm_price": pricing.get("Prezzo minimo FBM Amount"),
                })
            store.update_candidates(job_id, [candidate])
        if pricing_batch_interval:
            sleep_func(pricing_batch_interval)

    if store.summary(job_id)["phase"] == "pricing_complete":
        transformed = []
        observations_batch = []
        for candidate in store.iter_candidates(job_id):
            for listing in candidate.get("amazon_listings") or []:
                if listing.get("evaluation_status") != "bsr_passed":
                    continue
                reasons = []
                fba, total = listing.get("fba_sellers"), listing.get("total_sellers")
                if not isinstance(fba, int) or not isinstance(total, int):
                    reasons.append("seller_counts_unavailable")
                else:
                    if fba > filters["max_fba_sellers"]:
                        reasons.append("fba_sellers_above_threshold")
                    if total > filters["max_total_sellers"]:
                        reasons.append("total_sellers_above_threshold")
                if not _valid_price(listing.get("reference_price")):
                    reasons.append("missing_reference_price")
                listing["evaluation_status"] = (
                    "competition_filtered" if reasons else "competition_passed"
                )
                listing["competition_status"] = "filtered" if reasons else "passed"
                if reasons:
                    listing["exclusion_reasons"] = reasons
                    listing["exclusion_reason"] = ",".join(reasons)
                else:
                    listing.pop("exclusion_reason", None)
                    listing.pop("exclusion_reasons", None)
            observations_batch.extend(_build_amazon_observations([candidate]))
            transformed.append(candidate)
            if len(transformed) == 250:
                store.upsert_observations(job_id, observations_batch)
                store.update_candidates(job_id, transformed)
                transformed, observations_batch = [], []
                governor.before_next_batch()
        if transformed:
            store.upsert_observations(job_id, observations_batch)
            store.update_candidates(job_id, transformed)
        store.set_phase(job_id, "competition_filtered")
        _checkpoint(
            store, metadata_store, job_id, progress_phase="competition_filtered",
            progress_current=selected, progress_total=selected,
        )

    while store.summary(job_id)["phase"] == "competition_filtered":
        pending = store.pending_observations(job_id, 20)
        if not pending:
            store.set_phase(job_id, "fees_complete")
            _checkpoint(
                store, metadata_store, job_id, progress_phase="fees_complete",
                progress_current=selected, progress_total=selected,
            )
            break
        governor.before_next_batch()
        requests_ = [{
            "asin": row["asin"], "price": float(row["reference_price"]),
            "identifier": f"discovery|{job_id}|{row['observation_id']}|{row['asin']}",
        } for row in pending]
        _process_fee_batch(
            pending, requests_, fees_batch=fees_batch, token_provider=token_provider,
            job_id=job_id, sleep_func=sleep_func,
            save_progress=lambda: store.upsert_observations(job_id, pending),
        )
        for observation in pending:
            if observation.get("fee_status") == "valid":
                _sync_observation_fee_fields(observation)
        store.upsert_observations(job_id, pending)
        if any(row.get("fee_status") == "fee_pending" for row in pending):
            store.set_phase(job_id, "fees_pending", status="waiting_retry")
            _checkpoint(
                store, metadata_store, job_id, progress_phase="fees_pending",
                progress_current=selected - len(pending), progress_total=selected,
            )
            break
        if fee_batch_interval:
            sleep_func(fee_batch_interval)

    if store.summary(job_id)["phase"] == "fees_complete":
        final_products = 0
        transformed = []
        for candidate in store.iter_candidates(job_id):
            observations = store.observations_for_candidate(job_id, candidate)
            passed = _evaluate_product_combinations(
                candidate, observations, filters["minimum_margin"]
            )
            candidate["is_final_result"] = bool(passed)
            final_products += int(bool(passed))
            transformed.append(candidate)
            if len(transformed) == 250:
                store.update_candidates(job_id, transformed, replace_scenarios=True)
                transformed = []
                governor.before_next_batch()
        if transformed:
            store.update_candidates(job_id, transformed, replace_scenarios=True)
        store.set_phase(job_id, "completed", status="completed")
        state = _checkpoint(
            store, metadata_store, job_id, progress_phase="completed",
            progress_current=selected, progress_total=selected,
            extra={"completed_at": state.get("completed_at") or
                   datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                   "final_products": final_products},
        )
        return state

    summary = store.summary(job_id)
    return _checkpoint(
        store, metadata_store, job_id,
        progress_phase=summary["phase"],
        progress_current=summary["catalog_completed_count"],
        progress_total=summary["selected_count"],
    )


__all__ = ["ResourcePause", "run_incremental_discovery"]
