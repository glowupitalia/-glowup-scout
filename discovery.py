"""Supplier-first Discovery V1 orchestration with atomic checkpoints."""

from __future__ import annotations

import json
import inspect
import logging
import os
import random
import tempfile
import time
from datetime import datetime, timezone
from decimal import Decimal
from email.utils import parsedate_to_datetime
from pathlib import Path
from uuid import uuid4

import requests

from batch_analysis import (
    ProductFeeParseError,
    bsr_points,
    calculate_economics,
    fba_seller_points,
    margin_points,
    opportunity_score,
    parse_product_fee_result,
    total_seller_points,
)
from purchase_scenarios import (
    AmazonObservation,
    OpportunityCombination,
    amazon_observation_key,
    assign_scenario_roles,
    canonicalize_target_prices,
    normalize_amazon_listing,
    normalize_amazon_observation,
    normalize_economics,
    normalize_opportunity_combination,
    normalize_purchase_scenario,
    opportunity_combination_validation,
    opportunity_combination_key,
    purchase_scenario_validation,
    recommended_combination,
    recommended_scenario,
)
from supplier_preparation import prepare_suppliers, normalize_run_budget, normalize_selected_suppliers
from discovery_rotation import DiscoveryRotationStore
from qogita_discovery import load_qogita_cache_rows, normalize_qogita_candidates
from qogita_refresh import (
    inspect_qogita_cache,
    refresh_qogita_seller_catalogs,
    snapshots_advanced,
)


logger = logging.getLogger(__name__)
MAX_BATCH_SIZE = 20
FEE_ELEMENT_MAX_RETRIES = 2
FEE_ELEMENT_BACKOFF_SECONDS = 2
FEE_SYSTEMIC_MIN_FAILED_ITEMS = 10
FEE_SYSTEMIC_FAILURE_RATIO = Decimal("0.80")
TRANSIENT_FEE_STATUSES = {"servererror"}
TRANSIENT_FEE_CODES = {
    "internalerror", "serviceunavailable", "requestthrottled",
    "throttling", "toomanyrequests",
}
TRANSIENT_FEE_TYPES = {"receiver"}
DISCOVERY_SCHEMA_VERSION = "supplier_multi_listing_v1"
DISCOVERY_CHECKPOINT_SCHEMA_VERSION = 2
LEGACY_CHECKPOINT_MESSAGE = (
    "Questo risultato è stato creato con una versione precedente della "
    "Discovery. Avvia una nuova ricerca per utilizzare l'analisi multi-scenario."
)


class FeeSystemicOutage(RuntimeError):
    """A request-level Product Fees outage that must pause the whole job."""

    def __init__(self, reason, *, status_code=None):
        super().__init__(_sanitized_fee_text(reason))
        self.reason = _sanitized_fee_text(reason)
        self.status_code = status_code


def _chunks(values, size=MAX_BATCH_SIZE):
    for start in range(0, len(values), size):
        yield values[start:start + size]


def _json_default(value):
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"Unsupported checkpoint value: {type(value).__name__}")


class DiscoveryCheckpointStore:
    def __init__(self, root="data/discovery_jobs"):
        self.root = Path(root)

    def path(self, job_id):
        return self.root / f"{job_id}.json"

    def state_path(self, job_id):
        return self.root / f"{job_id}.state.json"

    def create(self, filters):
        job_id = uuid4().hex
        state = {
            "job_id": job_id,
            "schema_version": DISCOVERY_CHECKPOINT_SCHEMA_VERSION,
            "discovery_schema_version": DISCOVERY_SCHEMA_VERSION,
            "checkpoint_compatibility": "compatible",
            "status": "running",
            "phase": "initialized",
            "filters": dict(filters),
            "created_at": _now(),
            "started_at": _now(),
            "completed_at": None,
            "updated_at": _now(),
            "candidates": [],
            "amazon_listings": [],
            "amazon_observations": [],
            "opportunity_combinations": [],
            "results": [],
            "funnel": {},
            "errors": [],
        }
        self.save(state)
        return state

    def save(self, state):
        self.root.mkdir(parents=True, exist_ok=True)
        state["updated_at"] = _now()
        descriptor, temporary = tempfile.mkstemp(
            prefix=f".{state['job_id']}.", suffix=".json", dir=self.root
        )
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as output:
                json.dump(state, output, ensure_ascii=False, sort_keys=True, default=_json_default)
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary, self.path(state["job_id"]))
        except Exception:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass
            raise

    def load(self, job_id):
        compact_path = self.state_path(job_id)
        if compact_path.exists():
            with compact_path.open("r", encoding="utf-8") as source:
                state = json.load(source)
            if state.get("status") == "completed":
                from discovery_incremental import (
                    DiscoveryIncrementalStore, IncrementalCandidateCollection,
                )
                incremental = DiscoveryIncrementalStore()
                if incremental.has_job(job_id):
                    state["results"] = list(
                        IncrementalCandidateCollection(incremental, job_id, final_only=True)
                    )
            return normalize_discovery_state(state)
        with self.path(job_id).open("r", encoding="utf-8") as source:
            return normalize_discovery_state(json.load(source))

    def latest_incomplete(self):
        if not self.root.exists():
            return None
        states = []
        compact_jobs = {
            path.name.removesuffix(".state.json")
            for path in self.root.glob("*.state.json")
        }
        paths = list(self.root.glob("*.state.json")) + [
            path for path in self.root.glob("*.json")
            if path.stem not in compact_jobs
        ]
        for path in paths:
            try:
                state = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                continue
            if state.get("status") in {
                "running", "failed", "interrupted", "waiting_retry",
                "qogita_refresh_failed", "supplier_preparation_failed",
                "resource_paused",
            }:
                states.append(state)
        latest = max(states, key=lambda row: row.get("updated_at", ""), default=None)
        return normalize_discovery_state(latest) if latest else None


def _now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def default_filters():
    return {
        "bsr_min": 0,
        "bsr_max": 20000,
        "max_fba_sellers": 4,
        "max_total_sellers": 8,
        "minimum_margin": 15,
        "minimum_qogita_stock": 1,
    }


def validate_filters(filters):
    normalized = {key: int(filters[key]) for key in default_filters()}
    if normalized["bsr_min"] < 0 or normalized["bsr_max"] <= normalized["bsr_min"]:
        raise ValueError("Intervallo BSR non valido")
    for key in ("max_fba_sellers", "max_total_sellers", "minimum_margin", "minimum_qogita_stock"):
        if normalized[key] < 0:
            raise ValueError(f"Filtro non valido: {key}")
    return normalized


def _checkpoint(store, state, phase, *, progress=None):
    state["phase"] = phase
    state["progress_phase"] = phase
    store.save(state)
    logger.info(
        "DISCOVERY CHECKPOINT | job_id=%s phase=%s candidates=%s results=%s",
        state["job_id"], phase, len(state.get("candidates") or []), len(state.get("results") or []),
    )
    if progress:
        progress(phase, state)


def _fees_batch_with_retry(
    fees_batch,
    requests_,
    token_provider,
    *,
    job_id,
    sleep_func,
    attempts=4,
):
    last_error = None
    for attempt in range(1, attempts + 1):
        try:
            result = fees_batch(requests_, token_provider.get())
            return result if result is not None else []
        except requests.HTTPError as exc:
            last_error = exc
            status = exc.response.status_code if exc.response is not None else None
            if status == 401:
                token_provider.invalidate()
            if status not in {401, 429, 500, 502, 503, 504}:
                raise
        except requests.RequestException as exc:
            last_error = exc
            status = None
        if attempt < attempts:
            delay = 2 ** attempt
            logger.warning(
                "DISCOVERY FEES RETRY | job_id=%s attempt=%s status=%s delay=%s",
                job_id, attempt, status, delay,
            )
            sleep_func(delay)
    status_code = None
    if isinstance(last_error, requests.HTTPError) and last_error.response is not None:
        status_code = last_error.response.status_code
    cause = type(last_error).__name__ if last_error else "ProductFeesBatchFailure"
    raise FeeSystemicOutage(cause, status_code=status_code) from last_error


def _sanitized_fee_text(value):
    return " ".join(str(value or "").replace("\r", " ").replace("\n", " ").split())[:300]


def _fee_unavailable_reason(diagnostics):
    code = str(diagnostics.get("error_code") or "").casefold()
    if code == "internalerror":
        return "amazon_internal_error"
    if code in {"requestthrottled", "throttling", "toomanyrequests"}:
        return "amazon_throttled"
    if code == "serviceunavailable":
        return "amazon_service_unavailable"
    return "amazon_transient_error"


def classify_product_fee_entry(entry):
    """Classify an element response without retrying client-side failures."""
    if not entry:
        return {
            "classification": "permanent", "status": "MissingResult",
            "error_code": "MissingResult", "error_type": "",
            "error_message": "Product Fees result missing",
        }
    result = (entry or {}).get("FeesEstimateResult") or entry or {}
    status = _sanitized_fee_text(result.get("Status"))
    error = result.get("Error") or {}
    error_code = _sanitized_fee_text(error.get("Code"))
    error_type = _sanitized_fee_text(error.get("Type"))
    error_message = _sanitized_fee_text(error.get("Message"))
    if status.casefold() == "success":
        classification = "success"
    elif (
        status.casefold() in TRANSIENT_FEE_STATUSES
        or error_code.casefold() in TRANSIENT_FEE_CODES
        or error_type.casefold() in TRANSIENT_FEE_TYPES
    ):
        classification = "transient"
    else:
        classification = "permanent"
    return {
        "classification": classification,
        "status": status,
        "error_code": error_code,
        "error_type": error_type,
        "error_message": error_message,
    }


def _fee_entries_by_request(entries):
    by_identifier = {}
    by_asin = {}
    for entry in entries or []:
        result = (entry or {}).get("FeesEstimateResult") or entry or {}
        identifier = result.get("FeesEstimateIdentifier") or {}
        seller_identifier = identifier.get("SellerInputIdentifier")
        asin = identifier.get("IdValue")
        if seller_identifier:
            by_identifier[str(seller_identifier)] = entry
        if asin:
            by_asin.setdefault(str(asin), entry)
    return by_identifier, by_asin


def _header_seconds(value, *, now=None):
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        try:
            target = parsedate_to_datetime(str(value))
        except (TypeError, ValueError, OverflowError):
            return 0.0
        current = now or datetime.now(timezone.utc)
        if target.tzinfo is None:
            target = target.replace(tzinfo=timezone.utc)
        parsed = (target.astimezone(timezone.utc) - current).total_seconds()
    return max(0.0, parsed)


def _fee_element_retry_delay(entries, retry_number):
    delay = FEE_ELEMENT_BACKOFF_SECONDS * (2 ** (retry_number - 1))
    delay = max(delay, _header_seconds(getattr(entries, "retry_after", None)))
    try:
        rate_limit = float(getattr(entries, "rate_limit", None))
    except (TypeError, ValueError):
        rate_limit = 0
    if rate_limit > 0:
        delay = max(delay, 1.0 / rate_limit)
    return delay + random.uniform(0, max(0.1, delay * 0.10))


def _log_fee_element(job_id, row, diagnostics, *, attempt, retry, backoff, outcome):
    logger.info(
        "DISCOVERY FEES ELEMENT | job_id=%s asin=%s attempt=%s status=%s "
        "error_code=%s error_type=%s retry=%s backoff=%s outcome=%s",
        job_id, row.get("asin"), attempt,
        diagnostics.get("status") or "<missing>",
        diagnostics.get("error_code") or "<none>",
        diagnostics.get("error_type") or "<none>",
        "yes" if retry else "no", backoff, outcome,
    )


def _fee_request_wave(
    pending,
    *,
    fees_batch,
    token_provider,
    job_id,
    sleep_func,
    isolate,
):
    """Execute one retry wave, optionally isolating each transient entry.

    Amazon occasionally returns an HTTP 200 where every element in an otherwise
    valid Product Fees batch is a transient Receiver/InternalError. Replaying the
    exact same batch keeps every request in the same failure domain. After that
    condition has been observed, retry waves use sequential one-entry batches so
    one problematic entry cannot prevent unrelated entries from succeeding.
    """
    groups = [[pair] for pair in pending] if isolate and len(pending) > 1 else [pending]
    combined = []
    single_result = None
    for group_number, group in enumerate(groups, start=1):
        active_requests = [request for _, request in group]
        entries = _fees_batch_with_retry(
            fees_batch, active_requests, token_provider,
            job_id=job_id, sleep_func=sleep_func,
        )
        single_result = entries
        combined.extend(entries or [])
        if group_number >= len(groups):
            continue
        pacing = max(
            _header_seconds(getattr(entries, "retry_after", None)),
            _fee_rate_limit_delay(entries),
        )
        if pacing:
            logger.info(
                "DISCOVERY FEES ISOLATION PACE | job_id=%s group=%s/%s delay=%s",
                job_id, group_number, len(groups), pacing,
            )
            sleep_func(pacing)
    return single_result if len(groups) == 1 else combined


def _fee_rate_limit_delay(entries):
    try:
        rate_limit = float(getattr(entries, "rate_limit", None))
    except (TypeError, ValueError):
        return 0.0
    return 1.0 / rate_limit if rate_limit > 0 else 0.0


def _process_fee_batch(
    batch,
    requests_,
    *,
    fees_batch,
    token_provider,
    job_id,
    sleep_func,
    save_progress,
    max_retries=FEE_ELEMENT_MAX_RETRIES,
):
    """Retry only transient element failures, preserving successful rows."""
    pending = list(zip(batch, requests_))
    isolate_pending = False
    for cycle_attempt in range(1, max_retries + 2):
        entries = _fee_request_wave(
            pending,
            fees_batch=fees_batch,
            token_provider=token_provider,
            job_id=job_id,
            sleep_func=sleep_func,
            isolate=isolate_pending,
        )
        by_identifier, by_asin = _fee_entries_by_request(entries)
        retry_rows = []
        transient_diagnostics = []
        for row, request in pending:
            entry = (
                by_identifier.get(request["identifier"])
                or by_asin.get(row["asin"])
            )
            diagnostics = classify_product_fee_entry(entry)
            total_attempt = int(row.get("fee_attempts") or 0) + 1
            row["fee_attempts"] = total_attempt
            row["fee_last_attempt_at"] = _now()
            row["fee_phase"] = "product_fees"
            if diagnostics["classification"] == "success":
                for key in (
                    "fee_error", "fee_error_status", "fee_error_code",
                    "fee_error_type", "fee_pending_at", "fee_unavailable_reason",
                    "fee_retry_count", "fee_retryable_reason",
                ):
                    row.pop(key, None)
                try:
                    estimate = parse_product_fee_result(
                        entry, reference_price=row["reference_price"]
                    )
                except ProductFeeParseError as exc:
                    row["fee_status"] = "invalid"
                    row["fee_error"] = _sanitized_fee_text(exc)
                    outcome = "invalid"
                else:
                    row["fee_status"] = "valid"
                    row["fee_estimate"] = estimate
                    outcome = "valid"
                _log_fee_element(
                    job_id, row, diagnostics, attempt=total_attempt,
                    retry=False, backoff=0, outcome=outcome,
                )
                continue

            row.update({
                "fee_error": diagnostics["error_message"] or diagnostics["status"],
                "fee_error_status": diagnostics["status"],
                "fee_error_code": diagnostics["error_code"],
                "fee_error_type": diagnostics["error_type"],
            })
            if diagnostics["classification"] == "transient":
                row["fee_status"] = "retryable_error"
                row["fee_pending_at"] = _now()
                if cycle_attempt <= max_retries:
                    retry_rows.append((row, request))
                    transient_diagnostics.append((row, diagnostics, total_attempt))
                    continue
                row["fee_status"] = "unavailable"
                row["fee_unavailable_reason"] = _fee_unavailable_reason(diagnostics)
                row["fee_retry_count"] = max(0, total_attempt - 1)
                outcome = "unavailable"
            else:
                row["fee_status"] = "invalid"
                outcome = "invalid"
            _log_fee_element(
                job_id, row, diagnostics, attempt=total_attempt,
                retry=False, backoff=0, outcome=outcome,
            )

        unavailable_rows = [
            row for row, _ in pending if row.get("fee_status") == "unavailable"
        ]
        if (
            len(unavailable_rows) >= FEE_SYSTEMIC_MIN_FAILED_ITEMS
            and Decimal(len(unavailable_rows)) / Decimal(len(pending))
            >= FEE_SYSTEMIC_FAILURE_RATIO
        ):
            for row in unavailable_rows:
                row["fee_status"] = "retryable_error"
                row["fee_retryable_reason"] = row.pop(
                    "fee_unavailable_reason", "amazon_transient_error"
                )
            save_progress()
            raise FeeSystemicOutage("ElementFailureRate")
        save_progress()
        if not retry_rows:
            return
        if len(retry_rows) == len(pending) and len(pending) > 1:
            isolate_pending = True
        delay = _fee_element_retry_delay(entries, cycle_attempt)
        for row, diagnostics, total_attempt in transient_diagnostics:
            _log_fee_element(
                job_id, row, diagnostics, attempt=total_attempt,
                retry=True, backoff=delay, outcome="retrying",
            )
        sleep_func(delay)
        pending = retry_rows


def _legacy_transient_fee_row(row):
    if row.get("fee_status") in {"fee_pending", "retryable_error"}:
        return True
    if (
        row.get("fee_status") == "unavailable"
        and row.get("fee_unavailable_reason") == "amazon_internal_error"
    ):
        return True
    if row.get("fee_status") != "invalid":
        return False
    diagnostics = {
        "FeesEstimateResult": {
            "Status": row.get("fee_error_status"),
            "Error": {
                "Code": row.get("fee_error_code"),
                "Type": row.get("fee_error_type"),
                "Message": row.get("fee_error"),
            },
        }
    }
    if classify_product_fee_entry(diagnostics)["classification"] == "transient":
        return True
    return _sanitized_fee_text(row.get("fee_error")).casefold() == (
        "there is an internal service failure."
    )


def _prepare_fee_resume(state):
    fee_rows = state.get("amazon_observations") or state.get("candidates") or []
    if state.get("phase") == "fees_pending":
        rows = [
            row for row in fee_rows
            if row.get("fee_status") in {None, "", "fee_pending", "retryable_error"}
        ]
    elif state.get("phase") == "completed":
        rows = [row for row in fee_rows if _legacy_transient_fee_row(row)]
        legacy_rows = [
            row for row in state.get("candidates") or []
            if _legacy_transient_fee_row(row)
        ]
        if legacy_rows and state.get("amazon_observations"):
            by_id = _observation_map(state)
            for legacy in legacy_rows:
                observation = by_id.get(legacy.get("amazon_observation_id"))
                if observation:
                    observation["fee_status"] = "fee_pending"
                    observation["fee_error"] = legacy.get("fee_error")
                    rows.append(observation)
            rows = _unique(rows, lambda row: row.get("observation_id") or id(row))
    else:
        return False
    if not rows:
        return False
    for row in rows:
        row["fee_status"] = "fee_pending"
        row["fee_pending_at"] = row.get("fee_pending_at") or _now()
    state["status"] = "running"
    state["phase"] = "competition_filtered"
    state["results"] = []
    return True


def fee_coverage(rows):
    """Return mutually exclusive Product Fees coverage counters."""
    values = list(rows or [])
    counts = {
        "fee_target_count": len(values),
        "fee_valid_count": 0,
        "fee_unavailable_count": 0,
        "fee_invalid_count": 0,
        "fee_pending_count": 0,
    }
    for row in values:
        status = row.get("fee_status")
        if status == "valid":
            counts["fee_valid_count"] += 1
        elif status == "unavailable":
            counts["fee_unavailable_count"] += 1
        elif status in {"invalid", "fee_invalid"}:
            counts["fee_invalid_count"] += 1
        else:
            counts["fee_pending_count"] += 1
    counts["fee_coverage_partial"] = bool(
        counts["fee_unavailable_count"] or counts["fee_invalid_count"]
    )
    return counts


def _unique(values, key):
    selected = {}
    for value in values:
        selected.setdefault(key(value), value)
    return list(selected.values())


def discovery_checkpoint_compatibility(state):
    """Classify checkpoints without inventing tiers that were never persisted."""
    if not isinstance(state, dict):
        return {"status": "legacy_incompatible", "reason": "state_not_object"}
    products = []
    seen = set()
    for collection_name in ("candidates", "results"):
        for product in state.get(collection_name) or []:
            marker = product.get("product_key") or product.get("gtin") or id(product)
            if marker in seen:
                continue
            seen.add(marker)
            products.append(product)

    schema_current = (
        state.get("discovery_schema_version") == DISCOVERY_SCHEMA_VERSION
    )
    has_multiscenario_funnel = all(
        key in (state.get("funnel") or {})
        for key in ("qogita_products", "qogita_scenarios")
    )
    if not products:
        if schema_current or has_multiscenario_funnel:
            return {"status": "compatible", "reason": None}
        return {
            "status": "legacy_incompatible",
            "reason": "missing_multiscenario_schema",
        }

    for product in products:
        scenarios = product.get("scenarios")
        if not isinstance(scenarios, list) or not scenarios:
            return {
                "status": "legacy_incompatible",
                "reason": "missing_purchase_scenarios",
            }
        for scenario in scenarios:
            valid, reason = purchase_scenario_validation(scenario)
            if not valid:
                return {
                    "status": "legacy_incompatible",
                    "reason": reason,
                }
    for product in state.get("results") or []:
        if recommended_scenario(product) is None:
            return {
                "status": "legacy_incompatible",
                "reason": "missing_valid_recommended_scenario",
            }
        if (
            product.get("opportunity_combinations")
            and recommended_combination(product) is None
        ):
            return {
                "status": "legacy_incompatible",
                "reason": "missing_valid_recommended_combination",
            }
    return {
        "status": "compatible" if schema_current else "compatible_inferred",
        "reason": None,
    }


def discovery_funnel_view(state):
    """Expose only the canonical multi-scenario funnel counters used by the UI."""
    funnel = state.get("funnel") or {}
    product_keys = (
        "supplier_products_total", "qogita_products", "amazon_found",
        "beauty_valid", "bsr_passed", "competition_passed", "fee_valid",
        "fee_unavailable", "final_opportunities", "final_products",
    )
    scenario_keys = (
        "supplier_scenarios_total", "qogita_scenarios", "scenarios_evaluated",
        "scenarios_margin_passed", "scenarios_margin_below_threshold",
    )
    listing_keys = (
        "amazon_listings_found", "compatible_listings", "beauty_listings",
        "bsr_passed_listings", "competition_passed_listings",
        "fee_valid_listings", "fee_unavailable_count", "excluded_listings",
    )
    combination_keys = (
        "combinations_evaluated", "combinations_margin_passed",
        "combinations_margin_below_threshold",
    )
    return {
        "products": {key: int(funnel.get(key) or 0) for key in product_keys},
        "scenarios": {key: int(funnel.get(key) or 0) for key in scenario_keys},
        "suppliers": {
            key: int(funnel.get(key) or 0) for key in (
                "qogita_products", "qogita_scenarios", "umma_products",
                "umma_scenarios", "abw_products", "abw_scenarios",
                "abw_standard_scenarios", "abw_bulk_box_scenarios",
                "abw_stale_scenarios", "qudo_products", "qudo_scenarios",
                "qudo_stale_scenarios", "supplier_products_total",
                "supplier_scenarios_total",
            )
        },
        "listings": {key: int(funnel.get(key) or 0) for key in listing_keys},
        "purchase_scenarios": {
            "supplier_scenarios_total": int(
                funnel.get("supplier_scenarios_total")
                or funnel.get("qogita_scenarios") or 0
            ),
        },
        "combinations": {
            key: int(funnel.get(key) or 0) for key in combination_keys
        },
    }


def recalculate_diagnostic_funnel(state):
    """Rebuild product/listing funnel counters from persisted pipeline state.

    Product counters aggregate by ProductCandidate; listing counters aggregate
    AmazonListing rows.  In particular, a multi-listing Catalog result counts
    as one found product, and a product is competition-filtered only when at
    least one of its listings reached that filter and none passed it.
    """
    funnel = state.setdefault("funnel", {})
    products = list(state.get("candidates") or [])
    product_listings = [
        (product, listing)
        for product in products
        for listing in product.get("amazon_listings") or []
    ]
    listings = [listing for _, listing in product_listings]
    found_products = [
        product for product in products if product.get("amazon_listings")
    ]
    compatible = [
        listing for listing in listings
        if listing.get("compatibility_status") == "compatible"
    ]
    beauty = [
        listing for listing in compatible
        if listing.get("beauty_status") == "display_group_beauty"
    ]
    beauty_ids = {id(listing) for listing in beauty}
    bsr_passed_statuses = {
        "bsr_passed", "competition_passed", "competition_filtered",
    }
    bsr_passed = [
        listing for listing in listings
        if listing.get("evaluation_status") in bsr_passed_statuses
    ]
    bsr_passed_ids = {id(listing) for listing in bsr_passed}
    competition_passed = [
        listing for listing in listings
        if listing.get("evaluation_status") == "competition_passed"
    ]
    competition_passed_ids = {id(listing) for listing in competition_passed}

    def product_marker(product):
        return product.get("product_key") or id(product)

    def exclusion_reasons(listing):
        reasons = listing.get("exclusion_reasons")
        if isinstance(reasons, (list, tuple, set)):
            return {str(reason) for reason in reasons}
        reason = listing.get("exclusion_reason")
        return {
            item.strip() for item in str(reason or "").split(",")
            if item.strip()
        }

    competition_filtered_products = 0
    for product in products:
        rows = list(product.get("amazon_listings") or [])
        if any(
            row.get("evaluation_status") == "competition_passed"
            for row in rows
        ):
            continue
        if any(
            row.get("evaluation_status") == "competition_filtered"
            for row in rows
        ):
            competition_filtered_products += 1

    funnel.update({
        "amazon_found": len(found_products),
        "amazon_products_found": len(found_products),
        "amazon_listings_found": len(listings),
        "compatible_listings": len(compatible),
        "beauty_listings": len(beauty),
        "beauty_valid": len({
            product_marker(product)
            for product, listing in product_listings
            if id(listing) in beauty_ids
        }),
        "beauty_valid_bsr": sum(
            isinstance(row.get("bsr_beauty"), int)
            and not isinstance(row.get("bsr_beauty"), bool)
            and row.get("bsr_beauty") > 0
            for row in beauty
        ),
        "bsr_in_range": len({
            product_marker(product)
            for product, listing in product_listings
            if id(listing) in bsr_passed_ids
        }),
        "bsr_passed": len({
            product_marker(product)
            for product, listing in product_listings
            if id(listing) in bsr_passed_ids
        }),
        "bsr_passed_listings": len(bsr_passed),
        "competition_passed": len({
            product_marker(product)
            for product, listing in product_listings
            if id(listing) in competition_passed_ids
        }),
        "competition_passed_listings": len(competition_passed),
        "competition_filtered_products": competition_filtered_products,
        "fba_threshold_excluded": sum(
            "fba_sellers_above_threshold" in exclusion_reasons(listing)
            for listing in listings
        ),
        "total_sellers_threshold_excluded": sum(
            "total_sellers_above_threshold" in exclusion_reasons(listing)
            for listing in listings
        ),
    })
    return funnel


def _migrate_observation_sources(observation):
    estimate = observation.get("fee_estimate") or {}
    observation["fba_source"] = (
        observation.get("fba_source") or estimate.get("source")
    )
    referral_fee = estimate.get("referral_fee")
    try:
        amazon_referral = Decimal(str(referral_fee)) > 0
    except Exception:
        amazon_referral = False
    if observation.get("referral_source") not in {
        "amazon_referral_fee", "fallback_19_percent",
    }:
        observation["referral_source"] = (
            "amazon_referral_fee" if amazon_referral
            else "fallback_19_percent"
        )
    if observation["referral_source"] == "fallback_19_percent":
        observation["referral_rate"] = Decimal("0.19")
        try:
            observation["referral_fee"] = (
                Decimal(str(observation.get("reference_price")))
                * Decimal("0.19")
            )
        except Exception:
            observation["referral_fee"] = None
    else:
        observation["referral_fee"] = referral_fee
        observation["referral_rate"] = estimate.get("referral_rate")
    return observation


def normalize_discovery_state(state):
    """Normalize compatible checkpoints and explicitly reject legacy results."""
    for listing in state.get("amazon_listings") or []:
        listing.setdefault("min_fba_price", None)
        listing.setdefault("min_fbm_price", None)
        normalize_amazon_listing(listing)
    seen = set()
    normalized_products = []
    for collection in (state.get("candidates") or [], state.get("results") or []):
        for product in collection:
            marker = id(product)
            if marker in seen:
                continue
            seen.add(marker)
            normalized_products.append(product)
            normalize_economics(product.get("economics"), model="Product.economics")
            for field_name in ("margin_percent",):
                if field_name in product:
                    normalize_economics(product, model="Product")
                    break
            for field_name in (
                "score", "score_bsr", "score_fba", "score_total_sellers",
                "score_margin",
            ):
                if field_name in product and product[field_name] not in (None, ""):
                    try:
                        product[field_name] = int(Decimal(str(product[field_name])))
                    except (ArithmeticError, TypeError, ValueError):
                        product[field_name] = None
                        product.setdefault("numeric_normalization_errors", []).append(
                            f"Product.{field_name}:invalid_numeric_value"
                        )
            for scenario in product.get("scenarios") or []:
                normalize_purchase_scenario(scenario)
            embedded_observation = product.get("amazon_observation")
            if isinstance(embedded_observation, dict):
                _migrate_observation_sources(embedded_observation)
                normalize_amazon_observation(embedded_observation)
            for embedded_observation in product.get("amazon_observations") or []:
                _migrate_observation_sources(embedded_observation)
                normalize_amazon_observation(embedded_observation)
            for listing in product.get("amazon_listings") or []:
                listing.setdefault("min_fba_price", None)
                listing.setdefault("min_fbm_price", None)
                normalize_amazon_listing(listing)
            for combination in product.get("opportunity_combinations") or []:
                normalize_opportunity_combination(combination)
    for observation in state.get("amazon_observations") or []:
        observation.setdefault("min_fba_price", None)
        observation.setdefault("min_fbm_price", None)
        _migrate_observation_sources(observation)
        normalize_amazon_observation(observation)
    for product in normalized_products:
        _refresh_recommended_combination_from_checkpoint(product)
    if state.get("persistence") != "incremental_sqlite_v1" or state.get("candidates"):
        recalculate_diagnostic_funnel(state)
    compatibility = discovery_checkpoint_compatibility(state)
    state["checkpoint_compatibility"] = compatibility["status"]
    state["checkpoint_compatibility_reason"] = compatibility["reason"]
    if compatibility["status"] in {"compatible", "compatible_inferred"}:
        state["discovery_schema_version"] = DISCOVERY_SCHEMA_VERSION
    return state


def _observation_map(state):
    return {
        row["observation_id"]: row
        for row in state.get("amazon_observations") or []
        if row.get("observation_id")
    }


def _listing_map(state):
    return {
        row["listing_id"]: row
        for product in state.get("candidates") or []
        for row in product.get("amazon_listings") or []
        if row.get("listing_id")
    }


def _legacy_listing(product):
    asin = str(product.get("asin") or "")
    if not asin:
        return None
    return {
        "listing_id": f"legacy|{asin}", "marketplace": "IT",
        "canonical_ean": product.get("gtin") or product.get("canonical_ean"),
        "asin": asin, "title": product.get("amazon_title") or "",
        "brand": product.get("amazon_brand") or "", "manufacturer": "",
        "product_type": product.get("product_type") or "",
        "display_group": "beauty_display_on_website",
        "browse_classification": {}, "bsr_beauty": product.get("bsr_beauty"),
        "beauty_status": product.get("beauty_status"), "identifiers": [],
        "package_quantity": None, "number_of_items": None,
        "package_level": None, "volume_value": None, "volume_unit": None,
        "model_number": None, "part_number": None, "relationships": [],
        "variation_theme": None, "main_image": None,
        "compatibility_status": "compatible",
        "compatibility_reason": ["legacy_resolved_catalog"],
        "min_fba_price": product.get("min_fba_price"),
        "min_fbm_price": product.get("min_fbm_price"), "diagnostics": {},
    }


def _ensure_product_listings(product, catalog):
    listings = list(catalog.get("listings") or [])
    if not listings and catalog.get("status") == "resolved":
        temporary = dict(product)
        temporary.update(catalog)
        legacy = _legacy_listing(temporary)
        if legacy:
            listings = [legacy]
    for listing in listings:
        listing.setdefault("compatibility_status", "compatible")
        listing.setdefault("compatibility_reason", [])
        listing.setdefault("catalog_status", "resolved")
    product["amazon_listings"] = listings
    compatible = [
        row for row in listings
        if row.get("compatibility_status") == "compatible"
    ]
    product["catalog_status"] = (
        "resolved" if compatible else catalog.get("status") or "not_found"
    )
    product["catalog_identifier_type"] = catalog.get("identifier_type")
    product["catalog_diagnostics"] = dict(catalog.get("diagnostics") or {})
    if compatible:
        primary = compatible[0]
        product.update({
            "asin": primary.get("asin"),
            "amazon_title": primary.get("title"),
            "amazon_brand": primary.get("brand"),
            "bsr_beauty": primary.get("bsr_beauty"),
            "beauty_status": primary.get("beauty_status"),
            "product_type": primary.get("product_type"),
        })


def _call_catalog_batch(catalog_batch, identifiers, job_id, products):
    """Pass supplier identity when supported without breaking old adapters."""
    try:
        signature = inspect.signature(catalog_batch)
        supports_products = (
            len(signature.parameters) >= 3
            or any(
                parameter.kind == inspect.Parameter.VAR_POSITIONAL
                for parameter in signature.parameters.values()
            )
        )
    except (TypeError, ValueError):
        supports_products = False
    return (
        catalog_batch(identifiers, job_id, products)
        if supports_products else catalog_batch(identifiers, job_id)
    )


def _build_amazon_observations(products, marketplace="IT"):
    observations = {}
    for product in products:
        listings = product.get("amazon_listings") or []
        if not listings:
            legacy = _legacy_listing(product)
            if legacy:
                for key in (
                    "reference_price", "price_source", "fba_sellers",
                    "total_sellers", "seller_count_source", "pricing_status",
                    "competition_status",
                ):
                    legacy[key] = product.get(key)
                listings = [legacy]
                product["amazon_listings"] = listings
        for listing in listings:
            asin = str(listing.get("asin") or "")
            reference_price = listing.get("reference_price")
            if (
                not asin or reference_price is None
                or listing.get("competition_status") not in {None, "passed"}
            ):
                continue
            observation_id = amazon_observation_key(asin, reference_price)
            if observation_id not in observations:
                observation = AmazonObservation(
                    observation_id=observation_id,
                    marketplace=str(listing.get("marketplace") or marketplace),
                    canonical_ean=str(
                        product.get("gtin") or product.get("canonical_ean") or ""
                    ),
                    asin=asin,
                    amazon_brand=str(listing.get("brand") or ""),
                    amazon_title=str(listing.get("title") or ""),
                    bsr_beauty=int(listing["bsr_beauty"]),
                    reference_price=Decimal(str(reference_price)),
                    price_source=str(listing.get("price_source") or ""),
                    fba_sellers=int(listing["fba_sellers"]),
                    total_sellers=int(listing["total_sellers"]),
                    seller_count_source=str(listing.get("seller_count_source") or ""),
                    min_fba_price=(
                        Decimal(str(listing["min_fba_price"]))
                        if listing.get("min_fba_price") is not None else None
                    ),
                    min_fbm_price=(
                        Decimal(str(listing["min_fbm_price"]))
                        if listing.get("min_fbm_price") is not None else None
                    ),
                    observed_at=_now(),
                    diagnostics={"product_keys": [], "listing_ids": []},
                ).to_dict()
                observations[observation_id] = observation
            diagnostics = observations[observation_id]["diagnostics"]
            if product["product_key"] not in diagnostics["product_keys"]:
                diagnostics["product_keys"].append(product["product_key"])
            if listing.get("listing_id") not in diagnostics["listing_ids"]:
                diagnostics["listing_ids"].append(listing.get("listing_id"))
            listing["amazon_observation_id"] = observation_id
            if len(listings) == 1:
                product["amazon_observation_id"] = observation_id
    return sorted(observations.values(), key=lambda row: row["observation_id"])


def _sync_observation_fee_fields(observation):
    estimate = observation.get("fee_estimate") or {}
    observation.update({
        "fba_fee_net": estimate.get("fba_fee_net"),
        "fba_fee_gross": estimate.get("fba_fee_gross"),
        "fba_source": estimate.get("source"),
    })
    _migrate_observation_sources(observation)


def _copy_recommended_legacy_fields(product, recommended, observation):
    """Expose the recommended scenario to existing UI/tests without losing N scenarios."""
    economics = recommended.get("economics") or {}
    product.update({
        "seller_alias": recommended.get("supplier_alias"),
        "offer_qid": recommended.get("supplier_offer_id"),
        "stock": recommended.get("stock"),
        "selling_unit": recommended.get("selling_unit"),
        "mov": recommended.get("account_mov"),
        "tier_is_active": recommended.get("tier_is_active"),
        "currency": recommended.get("account_mov_currency"),
        "cost_net": recommended.get("cost_net_unit_eur"),
        "cost_vat": recommended.get("vat_amount_unit"),
        "cost_gross": recommended.get("cost_gross_unit_eur"),
        "snapshot_at": recommended.get("snapshot_at"),
        "economics": economics,
        "margin_percent": recommended.get("margin_percent"),
        "score": recommended.get("score"),
        "opportunity": recommended.get("opportunity"),
        "fee_estimate": observation.get("fee_estimate"),
        "amazon_observation": observation,
    })


def _evaluate_product_scenarios(product, observation, minimum_margin):
    shared_bsr = bsr_points(observation.get("bsr_beauty"))
    shared_fba = fba_seller_points(
        observation.get("fba_sellers"), observation.get("total_sellers")
    )
    shared_total = total_seller_points(observation.get("total_sellers"))
    for scenario in product.get("scenarios") or []:
        economics = calculate_economics(
            observation.get("reference_price"),
            scenario.get("cost_gross_unit_eur"),
            observation.get("fee_estimate"),
        )
        canonicalize_target_prices(economics)
        scenario["economics"] = economics
        scenario["economics_status"] = economics.get("status")
        if economics.get("status") != "ready":
            scenario.update({
                "margin_percent": None, "score_bsr": shared_bsr,
                "score_fba": shared_fba, "score_total_sellers": shared_total,
                "score_margin": 0, "score": shared_bsr + shared_fba + shared_total,
                "opportunity": opportunity_score(
                    observation.get("bsr_beauty"), observation.get("fba_sellers"),
                    observation.get("total_sellers"), None,
                )[1],
            })
            continue
        margin = float(economics["margin_percent"])
        score, opportunity = opportunity_score(
            observation.get("bsr_beauty"), observation.get("fba_sellers"),
            observation.get("total_sellers"), margin,
        )
        scenario.update({
            "margin_percent": margin, "score_bsr": shared_bsr,
            "score_fba": shared_fba, "score_total_sellers": shared_total,
            "score_margin": margin_points(margin), "score": score,
            "opportunity": opportunity,
            "evaluation_status": (
                "margin_passed" if margin >= minimum_margin
                else "margin_below_threshold"
            ),
        })
    roles = assign_scenario_roles(product.get("scenarios") or [], minimum_margin)
    product["scenario_roles"] = roles
    recommended = next(
        (row for row in product.get("scenarios") or []
         if row.get("scenario_id") == roles["scenario_raccomandato"]),
        None,
    )
    if recommended:
        _copy_recommended_legacy_fields(product, recommended, observation)
    passed = any(
        row.get("evaluation_status") == "margin_passed"
        for row in product.get("scenarios") or []
    )
    product["evaluation_status"] = (
        "margin_passed" if passed else "margin_below_threshold"
    )
    if passed:
        product.pop("exclusion_reason", None)
    else:
        product["exclusion_reason"] = "margin_below_threshold"
    return passed


def _combination_sort_key(row):
    return (
        -int(row.get("score") or 0),
        -Decimal(str(row.get("margin_percent") or "-Infinity")),
        -Decimal(str(row.get("profit") or "-Infinity")),
        Decimal(str(row.get("cost_gross_unit_eur") or "Infinity")),
        str(row.get("asin") or ""),
        str(row.get("scenario_id") or ""),
    )


def _positive_integer_or_infinity(value):
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return Decimal("Infinity")
    return Decimal(parsed) if parsed > 0 else Decimal("Infinity")


def _select_recommended_combination(combinations, scenario_by_id):
    """Apply operational tie-breaks only to comparable supplier scenarios."""
    if not combinations:
        return None
    economic_key = lambda row: _combination_sort_key(row)[:4]
    best_economics = min(economic_key(row) for row in combinations)
    tied = [row for row in combinations if economic_key(row) == best_economics]
    suppliers = {
        str(scenario_by_id.get(row.get("scenario_id"), {}).get("supplier") or "").casefold()
        for row in tied
    }
    if len(suppliers) == 1 and suppliers == {"umma"}:
        def umma_operational_key(row):
            scenario = scenario_by_id.get(row.get("scenario_id"), {})
            stock = scenario.get("stock")
            try:
                known_positive_stock = int(stock) > 0
            except (TypeError, ValueError):
                known_positive_stock = False
            return (
                _positive_integer_or_infinity(
                    scenario.get("minimum_product_quantity")
                ),
                _positive_integer_or_infinity(scenario.get("selling_unit")),
                0 if known_positive_stock else 1,
                str(row.get("asin") or ""),
                str(row.get("scenario_id") or ""),
            )
        return min(tied, key=umma_operational_key)
    if len(suppliers) == 1 and suppliers == {"abw"}:
        scenario_types = {
            str(
                scenario_by_id.get(row.get("scenario_id"), {}).get(
                    "scenario_type"
                ) or ""
            ).casefold()
            for row in tied
        }
        # ABW quantity requirements are comparable only within the same
        # commercial family. Standard tiers and bulk boxes deliberately never
        # influence one another's operational tie-break.
        if scenario_types == {"abw_standard"}:
            def abw_standard_key(row):
                scenario = scenario_by_id.get(row.get("scenario_id"), {})
                maximum = scenario.get("maximum_product_quantity")
                try:
                    maximum_key = (
                        (0, Decimal("0")) if maximum is None
                        else (1, -Decimal(int(maximum)))
                    )
                except (TypeError, ValueError):
                    maximum_key = (1, Decimal("Infinity"))
                return (
                    _positive_integer_or_infinity(
                        scenario.get("minimum_product_quantity")
                    ),
                    maximum_key,
                    str(row.get("asin") or ""),
                    str(row.get("scenario_id") or ""),
                )
            return min(tied, key=abw_standard_key)
        if scenario_types == {"abw_bulk_box"}:
            return min(tied, key=lambda row: (
                _positive_integer_or_infinity(
                    scenario_by_id.get(row.get("scenario_id"), {}).get(
                        "bundle_quantity"
                    )
                ),
                str(row.get("asin") or ""),
                str(row.get("scenario_id") or ""),
            ))
    # Qogita and cross-supplier ties retain deterministic identity ordering;
    # incomparable MOV/MOQ/bundle requirements never influence the result.
    return min(tied, key=lambda row: (
        str(row.get("asin") or ""),
        str(row.get("supplier") or "").casefold(),
        str(row.get("scenario_id") or ""),
    ))


def _refresh_recommended_combination_from_checkpoint(product):
    """Reapply current deterministic roles to already-evaluated local data."""
    combinations = [
        row for row in product.get("opportunity_combinations") or []
        if opportunity_combination_validation(row)[0]
    ]
    scenario_by_id = {
        row.get("scenario_id"): row for row in product.get("scenarios") or []
    }
    recommended = _select_recommended_combination(combinations, scenario_by_id)
    if not recommended:
        return
    roles = product.setdefault("combination_roles", {})
    roles.update({
        "recommended_combination": recommended.get("combination_id"),
        "best_listing": recommended.get("asin"),
        "best_purchase_scenario": recommended.get("scenario_id"),
    })
    product["recommended_combination"] = recommended
    product["best_listing"] = recommended.get("asin")
    product["best_purchase_scenario"] = recommended.get("scenario_id")
    scenario_roles = product.setdefault("scenario_roles", {})
    scenario_roles["scenario_raccomandato"] = recommended.get("scenario_id")
    scenario = scenario_by_id.get(recommended.get("scenario_id"))
    observations = {
        row.get("observation_id"): row
        for row in product.get("amazon_observations") or []
    }
    embedded = product.get("amazon_observation")
    if isinstance(embedded, dict) and embedded.get("observation_id"):
        observations.setdefault(embedded["observation_id"], embedded)
    observation = observations.get(recommended.get("amazon_observation_id"))
    if scenario and observation:
        _copy_recommended_legacy_fields(product, scenario, observation)
        product.update({
            "asin": recommended.get("asin"),
            "amazon_observation": observation,
            "amazon_title": observation.get("amazon_title"),
            "amazon_brand": observation.get("amazon_brand"),
            "bsr_beauty": observation.get("bsr_beauty"),
            "reference_price": observation.get("reference_price"),
            "fba_sellers": observation.get("fba_sellers"),
            "total_sellers": observation.get("total_sellers"),
            "amazon_offers_url": (
                f"https://www.amazon.it/gp/offer-listing/{recommended.get('asin')}"
            ),
        })


def _evaluate_product_combinations(product, observation_by_id, minimum_margin):
    """Fan out local economics across scenarios × fee-valid Amazon listings."""
    combinations = []
    product["amazon_observations"] = [
        observation_by_id[listing["amazon_observation_id"]]
        for listing in product.get("amazon_listings") or []
        if listing.get("amazon_observation_id") in observation_by_id
    ]
    scenario_by_id = {
        row.get("scenario_id"): row for row in product.get("scenarios") or []
    }
    listing_by_asin = {
        row.get("asin"): row for row in product.get("amazon_listings") or []
    }
    for listing in product.get("amazon_listings") or []:
        observation = observation_by_id.get(listing.get("amazon_observation_id"))
        if not observation:
            continue
        if observation.get("fee_status") == "unavailable":
            listing["evaluation_status"] = "economics_unavailable"
            listing["exclusion_reason"] = "amazon_fee_unavailable"
            listing["fee_unavailable_reason"] = observation.get(
                "fee_unavailable_reason"
            ) or "amazon_internal_error"
            continue
        if observation.get("fee_status") != "valid":
            continue
        shared_bsr = bsr_points(observation.get("bsr_beauty"))
        shared_fba = fba_seller_points(
            observation.get("fba_sellers"), observation.get("total_sellers")
        )
        shared_total = total_seller_points(observation.get("total_sellers"))
        for scenario in product.get("scenarios") or []:
            economics = calculate_economics(
                observation.get("reference_price"),
                scenario.get("cost_gross_unit_eur"),
                observation.get("fee_estimate"),
            )
            canonicalize_target_prices(economics)
            margin = economics.get("margin_percent")
            if economics.get("status") == "ready":
                score, opportunity = opportunity_score(
                    observation.get("bsr_beauty"), observation.get("fba_sellers"),
                    observation.get("total_sellers"), float(margin),
                )
                status = (
                    "margin_passed"
                    if Decimal(str(margin)) >= Decimal(str(minimum_margin))
                    else "margin_below_threshold"
                )
            else:
                score = shared_bsr + shared_fba + shared_total
                opportunity = opportunity_score(
                    observation.get("bsr_beauty"), observation.get("fba_sellers"),
                    observation.get("total_sellers"), None,
                )[1]
                status = "economics_unavailable"
            combination = OpportunityCombination(
                combination_id=opportunity_combination_key(
                    scenario["scenario_id"], observation["observation_id"]
                ),
                product_key=product["product_key"],
                scenario_id=scenario["scenario_id"], asin=observation["asin"],
                amazon_observation_id=observation["observation_id"],
                supplier=str(scenario.get("supplier") or ""),
                scenario_label=str(scenario.get("scenario_label") or ""),
                cost_gross_unit_eur=Decimal(str(scenario["cost_gross_unit_eur"])),
                price_reference=Decimal(str(observation["reference_price"])),
                profit=(
                    Decimal(str(economics["profit"]))
                    if economics.get("profit") is not None else None
                ),
                margin_percent=(
                    Decimal(str(margin)) if margin is not None else None
                ),
                target_prices=dict(economics.get("target_prices") or {}),
                score=score, opportunity=opportunity,
                evaluation_status=status,
                score_bsr=shared_bsr, score_fba=shared_fba,
                score_total_sellers=shared_total,
                score_margin=margin_points(float(margin)) if margin is not None else 0,
                diagnostics={"economics_status": economics.get("status")},
            ).to_dict()
            combination["economics"] = economics
            combinations.append(combination)

    combinations.sort(key=_combination_sort_key)
    product["opportunity_combinations"] = combinations
    if not combinations:
        filtered_listings = [
            row for row in product.get("amazon_listings") or []
            if row.get("evaluation_status") == "competition_filtered"
        ]
        if filtered_listings:
            product["evaluation_status"] = "competition_filtered"
            reasons = sorted({
                reason for row in filtered_listings
                for reason in row.get("exclusion_reasons") or []
            })
            product["exclusion_reasons"] = reasons
            product["exclusion_reason"] = ",".join(reasons)
        else:
            product["evaluation_status"] = "economics_unavailable"
            if any(
                row.get("fee_status") == "unavailable"
                for row in product.get("amazon_listings") or []
            ):
                product["exclusion_reason"] = "amazon_fee_unavailable"
                product["exclusion_reasons"] = ["amazon_fee_unavailable"]
        product["combination_roles"] = {
            "recommended_combination": None, "best_listing": None,
            "best_purchase_scenario": None,
            "minimum_profitable_combination": None,
            "minimum_profitable_combination_by_supplier": {},
        }
        return False
    profitable = [
        row for row in combinations if row["evaluation_status"] == "margin_passed"
    ]
    recommended = _select_recommended_combination(combinations, scenario_by_id)
    combinations_by_scenario = {}
    for combination in combinations:
        combinations_by_scenario.setdefault(
            combination["scenario_id"], []
        ).append(combination)
    for scenario_id, scenario_combinations in combinations_by_scenario.items():
        scenario = scenario_by_id[scenario_id]
        scenario_best = min(scenario_combinations, key=_combination_sort_key)
        scenario["economics"] = scenario_best["economics"]
        scenario["economics_status"] = scenario_best["diagnostics"]["economics_status"]
        scenario["best_asin"] = scenario_best["asin"]
        for key in (
            "margin_percent", "score", "opportunity", "score_bsr", "score_fba",
            "score_total_sellers", "score_margin", "evaluation_status",
        ):
            scenario[key] = scenario_best.get(key)
    profitable_by_supplier = {}
    for row in profitable:
        profitable_by_supplier.setdefault(row.get("supplier") or "unknown", []).append(row)

    def requirement(row):
        scenario_row = scenario_by_id[row["scenario_id"]]
        if str(scenario_row.get("supplier") or "").casefold() == "umma":
            return Decimal(str(scenario_row.get("minimum_product_quantity") or "Infinity"))
        return Decimal(str(scenario_row.get("account_mov") or "Infinity"))

    minimum_by_supplier = {
        supplier: min(rows, key=lambda row: (
            requirement(row), -Decimal(str(row.get("margin_percent") or 0)),
            str(row.get("asin") or ""), str(row.get("scenario_id") or ""),
        ))
        for supplier, rows in profitable_by_supplier.items()
    }
    minimum_profitable = (
        next(iter(minimum_by_supplier.values()))
        if len(minimum_by_supplier) == 1 else None
    )
    product["combination_roles"] = {
        "recommended_combination": recommended["combination_id"],
        "best_listing": recommended["asin"],
        "best_purchase_scenario": recommended["scenario_id"],
        "minimum_profitable_combination": (
            minimum_profitable.get("combination_id")
            if minimum_profitable else None
        ),
        "minimum_profitable_combination_by_supplier": {
            supplier: row["combination_id"]
            for supplier, row in minimum_by_supplier.items()
        },
    }
    product["recommended_combination"] = recommended
    product["best_listing"] = recommended["asin"]
    product["best_purchase_scenario"] = recommended["scenario_id"]
    scenario = scenario_by_id[recommended["scenario_id"]]
    observation = observation_by_id[recommended["amazon_observation_id"]]
    listing = listing_by_asin.get(recommended["asin"], {})
    # Keep legacy single-result fields as a projection of the recommended pair.
    roles = assign_scenario_roles(product.get("scenarios") or [], minimum_margin)
    roles["scenario_raccomandato"] = scenario["scenario_id"]
    product["scenario_roles"] = roles
    _copy_recommended_legacy_fields(product, scenario, observation)
    product.update({
        "asin": recommended["asin"], "amazon_observation": observation,
        "amazon_title": observation.get("amazon_title") or listing.get("title"),
        "amazon_brand": observation.get("amazon_brand") or listing.get("brand"),
        "bsr_beauty": observation.get("bsr_beauty"),
        "reference_price": observation.get("reference_price"),
        "fba_sellers": observation.get("fba_sellers"),
        "total_sellers": observation.get("total_sellers"),
        "amazon_offers_url": (
            f"https://www.amazon.it/gp/offer-listing/{recommended['asin']}"
        ),
    })
    passed = bool(profitable)
    product["evaluation_status"] = (
        "margin_passed" if passed else "margin_below_threshold"
    )
    product["exclusion_reason"] = None if passed else "margin_below_threshold"
    return passed


def _evaluate_available_combinations(state, filters):
    """Evaluate every fee-valid listing even while sibling listings are pending."""
    results = []
    observations = _observation_map(state)
    for product in state.get("candidates") or []:
        passed = _evaluate_product_combinations(
            product, observations, filters["minimum_margin"]
        )
        if passed:
            results.append(product)
    results.sort(key=lambda row: (
        -row["score"], row["bsr_beauty"], -row["margin_percent"],
        row["gtin"], row["asin"],
    ))
    state["results"] = results
    combinations = [
        combination for product in state.get("candidates") or []
        for combination in product.get("opportunity_combinations") or []
        if combination.get("diagnostics", {}).get("economics_status") == "ready"
    ]
    state["opportunity_combinations"] = [
        combination for product in state.get("candidates") or []
        for combination in product.get("opportunity_combinations") or []
    ]
    state["funnel"].update({
        "margin_passed": len(results),
        "margin_below_threshold": sum(
            product.get("exclusion_reason") == "margin_below_threshold"
            for product in state.get("candidates") or []
        ),
        "final_opportunities": len(results),
        "final_products": len(results),
        "combinations_evaluated": len(combinations),
        "combinations_margin_passed": sum(
            row.get("evaluation_status") == "margin_passed" for row in combinations
        ),
        "combinations_margin_below_threshold": sum(
            row.get("evaluation_status") == "margin_below_threshold" for row in combinations
        ),
    })
    return results


def run_discovery(
    filters,
    *,
    checkpoint_store,
    catalog_batch,
    pricing_batch,
    fees_batch,
    token_provider,
    qogita_loader=load_qogita_cache_rows,
    qogita_normalizer=normalize_qogita_candidates,
    qogita_refresher=refresh_qogita_seller_catalogs,
    selected_suppliers=None,
    run_budget=None,
    rotation_store=None,
    supplier_preparer=prepare_suppliers,
    job_id=None,
    progress=None,
    sleep_func=time.sleep,
    now_provider=lambda: datetime.now(timezone.utc),
    catalog_batch_interval=0.5,
    pricing_batch_interval=10.0,
    fee_batch_interval=2.0,
):
    filters = validate_filters(filters)
    state = checkpoint_store.load(job_id) if job_id else checkpoint_store.create(filters)
    job_id = state["job_id"]
    active_rotation_store = rotation_store
    if active_rotation_store is None and supplier_preparer is prepare_suppliers:
        active_rotation_store = DiscoveryRotationStore()
    if selected_suppliers is not None:
        selected_suppliers = normalize_selected_suppliers(selected_suppliers)
        if not selected_suppliers:
            raise ValueError("Seleziona almeno un fornitore")
        if job_id and state.get("selected_suppliers") not in (None, selected_suppliers):
            raise ValueError("Il checkpoint appartiene a fornitori differenti")
        if job_id and state.get("filters") != filters:
            raise ValueError("Il checkpoint appartiene a filtri differenti")
        normalized_budget = normalize_run_budget(run_budget)
        checkpoint_budget = state.get("run_budget")
        if job_id and checkpoint_budget is not None and checkpoint_budget != (
            "all" if normalized_budget is None else normalized_budget
        ):
            raise ValueError("Il checkpoint appartiene a un budget Discovery differente")
        state["selected_suppliers"] = selected_suppliers
        state["run_budget"] = "all" if normalized_budget is None else normalized_budget
    state.setdefault("amazon_observations", [])
    normalize_discovery_state(state)
    if state.get("checkpoint_compatibility") == "legacy_incompatible":
        state["status"] = "legacy_incompatible"
        state["phase"] = "legacy_incompatible"
        state["compatibility_message"] = LEGACY_CHECKPOINT_MESSAGE
        checkpoint_store.save(state)
        return state
    if job_id and _prepare_fee_resume(state):
        if not state["amazon_observations"]:
            state["amazon_observations"] = _build_amazon_observations(
                state.get("candidates") or []
            )
            by_asin = {row.get("asin"): row for row in state["amazon_observations"]}
            for product in state.get("candidates") or []:
                observation = by_asin.get(product.get("asin"))
                if not observation:
                    continue
                for key in (
                    "fee_status", "fee_estimate", "fee_attempts", "fee_error",
                    "fee_error_status", "fee_error_code", "fee_error_type",
                    "fee_pending_at", "fee_phase",
                ):
                    if key in product:
                        observation[key] = product[key]
        checkpoint_store.save(state)
        logger.info(
            "DISCOVERY FEES RESUME | job_id=%s phase=competition_filtered pending=%s",
            job_id,
            sum(
                row.get("fee_status") == "fee_pending"
                for row in state.get("amazon_observations") or []
            ),
        )
    started = time.monotonic()
    logger.info("DISCOVERY START | job_id=%s phase=%s", job_id, state["phase"])
    try:
        if selected_suppliers is not None and state["phase"] in {
            "initialized", "supplier_preparing", "supplier_preparation_failed",
        }:
            state["status"] = "running"
            _checkpoint(
                checkpoint_store, state, "supplier_preparing", progress=progress,
            )
            preparation_kwargs = {
                "now": now_provider(),
                "run_budget": run_budget,
                "progress": (
                    (lambda phase, supplier: progress(
                        phase, {**state, "current_supplier": supplier}
                    )) if progress else None
                ),
            }
            signature = inspect.signature(supplier_preparer)
            accepts_kwargs = any(
                parameter.kind == inspect.Parameter.VAR_KEYWORD
                for parameter in signature.parameters.values()
            )
            if accepts_kwargs or "rotation_store" in signature.parameters:
                preparation_kwargs.update({
                    "rotation_store": active_rotation_store,
                    "rotation_job_id": job_id,
                })
            prepared = supplier_preparer(
                selected_suppliers, filters, **preparation_kwargs,
            )
            state.update({
                "selected_suppliers": prepared["selected_suppliers"],
                "supplier_snapshot_set": prepared["supplier_snapshot_set"],
                "supplier_errors": {
                    supplier: row.get("error")
                    for supplier, row in prepared["supplier_snapshot_set"].items()
                    if row.get("error")
                },
                "supplier_warnings": prepared.get("supplier_warnings") or [],
                "supplier_coverage": prepared.get("coverage") or {},
                "supplier_diagnostics": prepared.get("supplier_diagnostics") or {},
                "usable_suppliers": prepared.get("usable_suppliers") or [],
                "candidates": prepared.get("candidates") or [],
                "amazon_observations": [],
                "total_supplier_ean_universe": prepared.get("total_supplier_ean_universe", 0),
                "eligible_identifier_count": prepared.get("eligible_identifier_count", 0),
                "run_budget": prepared.get("run_budget", state.get("run_budget", "all")),
                "sampled_identifier_count": prepared.get("sampled_identifier_count", 0),
                "sampling_strategy": prepared.get("sampling_strategy"),
                "rotation_scope": prepared.get("rotation_scope"),
                "rotation_cycle_id": prepared.get("rotation_cycle_id"),
                "rotation_universe_count": prepared.get("rotation_universe_count"),
                "rotation_analyzed_before_run": prepared.get(
                    "rotation_analyzed_before_run", 0
                ),
                "rotation_selected_identifiers": prepared.get(
                    "rotation_selected_identifiers", []
                ),
                "rotation_analyzed_this_run": prepared.get(
                    "rotation_analyzed_this_run", 0
                ),
                "rotation_remaining_after_run": prepared.get(
                    "rotation_remaining_after_run"
                ),
            })
            if not state["usable_suppliers"]:
                state["status"] = "supplier_preparation_failed"
                state["funnel"] = {
                    "supplier_products_total": 0,
                    "supplier_scenarios_total": 0,
                    "final_products": 0,
                    "final_opportunities": 0,
                }
                _checkpoint(
                    checkpoint_store, state, "supplier_preparation_failed",
                    progress=progress,
                )
                return state
            supplier_products = prepared["coverage"]["products_by_supplier"]
            supplier_scenarios = prepared["coverage"]["scenarios_by_supplier"]
            total_scenarios = sum(
                len(row.get("scenarios") or []) for row in state["candidates"]
            )
            state["funnel"] = {
                **{
                    f"{supplier}_products": int(supplier_products.get(supplier, 0))
                    for supplier in selected_suppliers
                },
                **{
                    f"{supplier}_scenarios": int(supplier_scenarios.get(supplier, 0))
                    for supplier in selected_suppliers
                },
                "supplier_products_total": len(state["candidates"]),
                "supplier_scenarios_total": total_scenarios,
                "unique_supplier_eans": prepared["coverage"].get("unique_eans", 0),
                "shared_supplier_eans": prepared["coverage"].get("shared_eans", 0),
                "catalog_invalid_identifier": 0,
                "amazon_found": 0, "amazon_products_found": 0,
                "amazon_listings_found": 0, "compatible_listings": 0,
                "beauty_valid_bsr": 0, "beauty_valid": 0,
                "beauty_listings": 0, "bsr_in_range": 0,
                "bsr_passed": 0, "bsr_passed_listings": 0,
                "competition_passed": 0, "competition_passed_listings": 0,
                "competition_filtered_products": 0,
                "fba_threshold_excluded": 0,
                "total_sellers_threshold_excluded": 0,
                "fee_valid": 0, "fee_valid_listings": 0,
                "fee_pending": 0, "fee_invalid": 0,
                "excluded_listings": 0,
                "margin_passed": 0, "margin_below_threshold": 0,
                "final_opportunities": 0, "final_products": 0,
                "scenarios_evaluated": 0,
                "scenarios_margin_passed": 0,
                "scenarios_margin_below_threshold": 0,
                "combinations_evaluated": 0,
                "combinations_margin_passed": 0,
                "combinations_margin_below_threshold": 0,
            }
            for supplier_stats in prepared.get("supplier_diagnostics", {}).values():
                if not isinstance(supplier_stats, dict):
                    continue
                for key, value in supplier_stats.items():
                    if (
                        isinstance(value, int) and not isinstance(value, bool)
                        and key not in {"supplier_products_total", "supplier_scenarios_total"}
                    ):
                        state["funnel"][key] = value
            _checkpoint(
                checkpoint_store, state, "suppliers_loaded", progress=progress,
            )

        if selected_suppliers is None and state["phase"] in {
            "initialized", "qogita_checking", "qogita_refresh_required",
            "qogita_refreshing", "qogita_refresh_failed",
        }:
            resume_phase = state["phase"]
            if resume_phase == "initialized":
                _checkpoint(
                    checkpoint_store, state, "qogita_checking",
                    progress=progress,
                )
            state["status"] = "running"
            rows = qogita_loader()
            cache_before = inspect_qogita_cache(
                rows, now=now_provider(),
            )
            aliases = cache_before["seller_aliases"]
            state["qogita_snapshot_before"] = (
                state.get("qogita_snapshot_before")
                or cache_before["snapshots"]
            )
            state["qogita_seller_aliases"] = aliases
            state["qogita_refresh_error"] = None
            state.setdefault("qogita_refresh_started_at", None)
            state.setdefault("qogita_refresh_completed_at", None)

            interrupted_refresh_completed = (
                resume_phase == "qogita_refreshing"
                and cache_before["fresh"]
                and snapshots_advanced(
                    state.get("qogita_snapshot_before"),
                    cache_before["snapshots"], aliases,
                )
            )
            force_refresh = resume_phase in {
                "qogita_refresh_required", "qogita_refresh_failed",
            }
            if cache_before["fresh"] and not force_refresh:
                state["qogita_snapshot_after"] = cache_before["snapshots"]
                state["qogita_refresh_status"] = (
                    "refreshed" if interrupted_refresh_completed
                    else "cache_fresh"
                )
                state["qogita_refresh_completed_at"] = (
                    state.get("qogita_refresh_completed_at")
                    if interrupted_refresh_completed else None
                )
                state["qogita_refresh_duration_seconds"] = float(
                    state.get("qogita_refresh_duration_seconds") or 0
                )
                state["qogita_seller_aliases_updated"] = (
                    aliases if interrupted_refresh_completed else []
                )
            else:
                state["qogita_refresh_status"] = "refresh_required"
                state["qogita_refresh_started_at"] = _now()
                _checkpoint(
                    checkpoint_store, state, "qogita_refresh_required",
                    progress=progress,
                )
                if not aliases:
                    refresh_result = {
                        "status": "failed",
                        "updated_aliases": [],
                        "error_code": "qogita_cache_missing",
                        "duration_seconds": 0,
                    }
                else:
                    state["qogita_refresh_status"] = "refreshing"
                    _checkpoint(
                        checkpoint_store, state, "qogita_refreshing",
                        progress=progress,
                    )
                    refresh_result = qogita_refresher(aliases)
                state["qogita_refresh_completed_at"] = _now()
                state["qogita_refresh_duration_seconds"] = float(
                    refresh_result.get("duration_seconds") or 0
                )
                state["qogita_seller_aliases_updated"] = list(
                    refresh_result.get("updated_aliases") or []
                )
                if refresh_result.get("status") != "success":
                    state["status"] = "qogita_refresh_failed"
                    state["qogita_refresh_status"] = "refresh_failed"
                    state["qogita_refresh_error"] = str(
                        refresh_result.get("error_code")
                        or "refresh_failed"
                    )[:100]
                    state["qogita_snapshot_after"] = None
                    _checkpoint(
                        checkpoint_store, state, "qogita_refresh_failed",
                        progress=progress,
                    )
                    return state

                rows = qogita_loader()
                cache_after = inspect_qogita_cache(
                    rows, now=now_provider(),
                )
                refreshed_aliases = cache_after["seller_aliases"]
                refresh_valid = (
                    cache_after["fresh"]
                    and set(refreshed_aliases) == set(aliases)
                    and snapshots_advanced(
                        state["qogita_snapshot_before"],
                        cache_after["snapshots"], aliases,
                    )
                )
                if not refresh_valid:
                    state["status"] = "qogita_refresh_failed"
                    state["qogita_refresh_status"] = "refresh_failed"
                    state["qogita_refresh_error"] = (
                        "refreshed_cache_not_fresh"
                    )
                    state["qogita_snapshot_after"] = cache_after["snapshots"]
                    _checkpoint(
                        checkpoint_store, state, "qogita_refresh_failed",
                        progress=progress,
                    )
                    return state
                state["qogita_snapshot_after"] = cache_after["snapshots"]
                state["qogita_refresh_status"] = "refreshed"

            normalizer_kwargs = {
                "minimum_stock": filters["minimum_qogita_stock"]
            }
            if qogita_normalizer is normalize_qogita_candidates:
                normalizer_kwargs["now"] = now_provider()
            candidates, source_stats = qogita_normalizer(rows, **normalizer_kwargs)
            state["candidates"] = candidates
            state["amazon_observations"] = []
            state["funnel"] = {
                "qogita_products": source_stats.get("qogita_products", len(candidates)),
                "qogita_scenarios": source_stats.get(
                    "qogita_scenarios",
                    sum(len(row.get("scenarios") or [row]) for row in candidates),
                ),
                "qogita_initial": source_stats["initial"],
                "valid_gtin": source_stats["valid_gtin"],
                "catalog_invalid_identifier": 0,
                "amazon_found": 0,
                "beauty_valid_bsr": 0,
                "bsr_in_range": 0,
                "competition_passed": 0,
                "competition_filtered_products": 0,
                "fba_threshold_excluded": 0,
                "total_sellers_threshold_excluded": 0,
                "fee_valid": 0,
                "fee_pending": 0,
                "fee_invalid": 0,
                "margin_passed": 0,
                "margin_below_threshold": 0,
                "final_opportunities": 0,
                "scenarios_evaluated": 0,
                "scenarios_margin_passed": 0,
                "scenarios_margin_below_threshold": 0,
                "supplier_products_total": len(candidates),
                "supplier_scenarios_total": sum(
                    len(row.get("scenarios") or []) for row in candidates
                ),
                "amazon_products_found": 0,
                "amazon_listings_found": 0,
                "compatible_listings": 0,
                "beauty_listings": 0,
                "bsr_passed_listings": 0,
                "competition_passed_listings": 0,
                "fee_valid_listings": 0,
                "excluded_listings": 0,
                "combinations_evaluated": 0,
                "combinations_margin_passed": 0,
                "combinations_margin_below_threshold": 0,
                "final_products": 0,
                "qudo_products": int(source_stats.get("qudo_products") or 0),
                "qudo_scenarios": int(source_stats.get("qudo_scenarios") or 0),
                "qudo_stale_scenarios": int(
                    source_stats.get("qudo_stale_scenarios") or 0
                ),
            }
            state["source_diagnostics"] = source_stats
            _checkpoint(checkpoint_store, state, "qogita_loaded", progress=progress)

        if state["phase"] in {"qogita_loaded", "suppliers_loaded"}:
            if active_rotation_store is not None and state.get("rotation_scope"):
                state.update(active_rotation_store.commit_catalog_results(
                    job_id,
                    {
                        str(row.get("canonical_ean") or row.get("gtin") or ""):
                        row.get("catalog_status")
                        for row in state.get("candidates") or []
                    },
                ))
            pending = [row for row in state["candidates"] if not row.get("catalog_status")]
            catalog_total = len(state["candidates"])
            catalog_completed = catalog_total - len(pending)
            batches = list(_chunks(pending))
            for batch_number, batch in enumerate(batches, start=1):
                identifiers = list(dict.fromkeys(row["gtin"] for row in batch))
                mapping = _call_catalog_batch(
                    catalog_batch, identifiers, job_id, batch
                )
                for row in batch:
                    catalog = mapping.get(row["gtin"], {"status": "not_found"})
                    _ensure_product_listings(row, catalog)
                catalog_completed += len(batch)
                state.update({
                    "progress_phase": "catalog",
                    "progress_current": catalog_completed,
                    "progress_total": catalog_total,
                })
                checkpoint_store.save(state)
                if active_rotation_store is not None and state.get("rotation_scope"):
                    state.update(active_rotation_store.commit_catalog_results(
                        job_id,
                        {
                            str(row.get("canonical_ean") or row.get("gtin") or ""):
                            row.get("catalog_status")
                            for row in batch
                        },
                    ))
                    checkpoint_store.save(state)
                if progress:
                    progress("catalog", state)
                logger.info("DISCOVERY CATALOG BATCH | job_id=%s batch=%s size=%s", job_id, batch_number, len(batch))
                if batch_number < len(batches) and catalog_batch_interval:
                    sleep_func(catalog_batch_interval)
            state["funnel"]["catalog_invalid_identifier"] = sum(
                row.get("catalog_status") == "invalid_identifier"
                for row in state["candidates"]
            )
            all_listings = [
                listing for row in state["candidates"]
                for listing in row.get("amazon_listings") or []
            ]
            state["amazon_listings"] = all_listings
            recalculate_diagnostic_funnel(state)
            if active_rotation_store is not None and state.get("rotation_scope"):
                state.update(active_rotation_store.commit_catalog_results(
                    job_id,
                    {
                        str(row.get("canonical_ean") or row.get("gtin") or ""):
                        row.get("catalog_status")
                        for row in state.get("candidates") or []
                    },
                ))
            _checkpoint(checkpoint_store, state, "catalog_complete", progress=progress)

        if state["phase"] == "catalog_complete":
            for row in state["candidates"]:
                for listing in row.get("amazon_listings") or []:
                    if listing.get("compatibility_status") != "compatible":
                        listing["evaluation_status"] = "catalog_incompatible"
                        continue
                    bsr = listing.get("bsr_beauty")
                    if listing.get("beauty_status") != "display_group_beauty":
                        listing["evaluation_status"] = "beauty_filtered"
                        listing["exclusion_reason"] = "not_beauty_display_group"
                        continue
                    if not isinstance(bsr, int) or isinstance(bsr, bool) or bsr <= 0:
                        listing["evaluation_status"] = "bsr_filtered"
                        listing["exclusion_reason"] = "invalid_bsr"
                        continue
                    if not filters["bsr_min"] <= bsr <= filters["bsr_max"]:
                        listing["evaluation_status"] = "bsr_filtered"
                        listing["exclusion_reason"] = "bsr_out_of_range"
                        continue
                    listing["evaluation_status"] = "bsr_passed"
            recalculate_diagnostic_funnel(state)
            _checkpoint(checkpoint_store, state, "bsr_filtered", progress=progress)

        if state["phase"] == "bsr_filtered":
            pending = [
                listing for row in state["candidates"]
                for listing in row.get("amazon_listings") or []
                if listing.get("evaluation_status") == "bsr_passed"
                and not listing.get("pricing_status")
            ]
            pending_asins = list(dict.fromkeys(row["asin"] for row in pending))
            pricing_total = len(pending_asins)
            pricing_completed = 0
            batches = list(_chunks(pending_asins))
            for batch_number, asins in enumerate(batches, start=1):
                mapping = pricing_batch(asins, job_id)
                for listing in pending:
                    if listing["asin"] not in asins:
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
                pricing_completed += len(asins)
                state.update({
                    "progress_phase": "pricing",
                    "progress_current": pricing_completed,
                    "progress_total": pricing_total,
                })
                checkpoint_store.save(state)
                if progress:
                    progress("pricing", state)
                logger.info("DISCOVERY PRICING BATCH | job_id=%s batch=%s size=%s", job_id, batch_number, len(asins))
                if batch_number < len(batches) and pricing_batch_interval:
                    sleep_func(pricing_batch_interval)
            _checkpoint(checkpoint_store, state, "pricing_complete", progress=progress)

        if state["phase"] == "pricing_complete":
            for product in state["candidates"]:
              for row in product.get("amazon_listings") or []:
                if row.get("evaluation_status") != "bsr_passed":
                    continue
                fba = row.get("fba_sellers")
                total = row.get("total_sellers")
                price = row.get("reference_price")
                reasons = []
                if not isinstance(fba, int) or not isinstance(total, int):
                    reasons.append("seller_counts_unavailable")
                else:
                    if fba > filters["max_fba_sellers"]:
                        reasons.append("fba_sellers_above_threshold")
                    if total > filters["max_total_sellers"]:
                        reasons.append("total_sellers_above_threshold")
                try:
                    valid_price = Decimal(str(price)) > 0
                except Exception:
                    valid_price = False
                if not valid_price:
                    reasons.append("missing_reference_price")
                if not reasons:
                    row["evaluation_status"] = "competition_passed"
                    row["competition_status"] = "passed"
                    row.pop("exclusion_reason", None)
                    row.pop("exclusion_reasons", None)
                else:
                    row["evaluation_status"] = "competition_filtered"
                    row["competition_status"] = "filtered"
                    row["exclusion_reasons"] = reasons
                    row["exclusion_reason"] = ",".join(reasons)
            recalculate_diagnostic_funnel(state)
            state["amazon_observations"] = _build_amazon_observations(
                state["candidates"]
            )
            _checkpoint(checkpoint_store, state, "competition_filtered", progress=progress)

        if state["phase"] == "competition_filtered":
            pending = [
                row for row in state.get("amazon_observations") or []
                if row.get("fee_status") in {None, "", "fee_pending", "retryable_error"}
            ]
            fees_total = len(pending)
            fees_completed = 0
            batches = list(_chunks(pending))
            fee_systemic_outage = None
            for batch_number, batch in enumerate(batches, start=1):
                requests_ = [{
                    "asin": row["asin"],
                    "price": float(row["reference_price"]),
                    "identifier": f"discovery|{job_id}|{row['observation_id']}|{row['asin']}",
                } for row in batch]
                try:
                    _process_fee_batch(
                        batch, requests_, fees_batch=fees_batch,
                        token_provider=token_provider, job_id=job_id,
                        sleep_func=sleep_func,
                        save_progress=lambda: checkpoint_store.save(state),
                    )
                except FeeSystemicOutage as exc:
                    fee_systemic_outage = exc
                    break
                fees_completed += len(batch)
                state.update({
                    "progress_phase": "fees",
                    "progress_current": fees_completed,
                    "progress_total": fees_total,
                })
                checkpoint_store.save(state)
                if progress:
                    progress("fees", state)
                logger.info("DISCOVERY FEES BATCH | job_id=%s batch=%s size=%s", job_id, batch_number, len(batch))
                if batch_number < len(batches) and fee_batch_interval:
                    sleep_func(fee_batch_interval)
            if fee_systemic_outage is not None:
                state["status"] = "waiting_retry"
                state["resumable"] = True
                state["fee_outage_reason"] = fee_systemic_outage.reason
                state["duration_seconds"] = time.monotonic() - started
                _checkpoint(checkpoint_store, state, "fees_pending", progress=progress)
                return state
            for observation in state.get("amazon_observations") or []:
                if observation.get("fee_status") == "valid":
                    _sync_observation_fee_fields(observation)
            observation_by_id = _observation_map(state)
            for product in state.get("candidates") or []:
                for listing in product.get("amazon_listings") or []:
                    observation = observation_by_id.get(
                        listing.get("amazon_observation_id")
                    )
                    if not observation:
                        continue
                    for key in (
                        "fee_status", "fee_estimate", "fee_attempts", "fee_error",
                        "fee_error_status", "fee_error_code", "fee_error_type",
                        "fee_pending_at", "fee_phase", "fee_unavailable_reason",
                        "fee_retry_count", "fee_last_attempt_at",
                    ):
                        if key in observation:
                            listing[key] = observation[key]
                    if len(product.get("amazon_listings") or []) == 1:
                        for key in (
                            "fee_status", "fee_estimate", "fee_attempts", "fee_error",
                            "fee_error_status", "fee_error_code", "fee_error_type",
                            "fee_pending_at", "fee_phase", "fee_unavailable_reason",
                            "fee_retry_count", "fee_last_attempt_at",
                        ):
                            if key in observation:
                                product[key] = observation[key]
            def product_count_for_fee_status(status):
                return sum(
                    len((row.get("diagnostics") or {}).get("product_keys") or [row])
                    for row in state.get("amazon_observations") or []
                    if row.get("fee_status") == status
                )

            state["funnel"]["fee_valid"] = len({
                product_key
                for observation in state.get("amazon_observations") or []
                if observation.get("fee_status") == "valid"
                for product_key in (observation.get("diagnostics") or {}).get("product_keys") or []
            })
            state["funnel"]["fee_valid_listings"] = sum(
                row.get("fee_status") == "valid"
                for row in state.get("amazon_observations") or []
            )
            coverage = fee_coverage(state.get("amazon_observations") or [])
            state.update(coverage)
            state["funnel"].update(coverage)
            state["funnel"]["fee_pending"] = (
                product_count_for_fee_status("fee_pending")
                + product_count_for_fee_status("retryable_error")
            )
            state["funnel"]["fee_unavailable"] = product_count_for_fee_status("unavailable")
            state["funnel"]["fee_invalid"] = product_count_for_fee_status("invalid")
            if state["funnel"]["fee_pending"]:
                _evaluate_available_combinations(state, filters)
                state["status"] = "waiting_retry"
                state["duration_seconds"] = time.monotonic() - started
                _checkpoint(
                    checkpoint_store, state, "fees_pending", progress=progress
                )
            else:
                _checkpoint(
                    checkpoint_store, state, "fees_complete", progress=progress
                )

        if state["phase"] == "fees_complete":
            results = []
            observations = _observation_map(state)
            for row in state["candidates"]:
                passed = _evaluate_product_combinations(
                    row, observations, filters["minimum_margin"]
                )
                if passed:
                    results.append(row)
            results.sort(key=lambda row: (
                -row["score"], row["bsr_beauty"],
                -row["margin_percent"], row["gtin"], row["asin"],
            ))
            state["results"] = results
            state["funnel"]["margin_passed"] = len(results)
            state["funnel"]["margin_below_threshold"] = sum(
                row.get("exclusion_reason") == "margin_below_threshold"
                for row in state["candidates"]
            )
            state["funnel"]["final_opportunities"] = len(results)
            evaluated_combinations = [
                combination for product in state["candidates"]
                for combination in product.get("opportunity_combinations") or []
                if combination.get("diagnostics", {}).get("economics_status") == "ready"
            ]
            # Legacy scenario counters remain product-scenario counts for one-listing jobs.
            evaluated_scenarios = [
                scenario for product in state["candidates"]
                for scenario in product.get("scenarios") or []
                if scenario.get("economics_status") == "ready"
            ]
            state["funnel"]["scenarios_evaluated"] = len(evaluated_scenarios)
            state["funnel"]["scenarios_margin_passed"] = sum(
                row.get("evaluation_status") == "margin_passed"
                for row in evaluated_scenarios
            )
            state["funnel"]["scenarios_margin_below_threshold"] = sum(
                row.get("evaluation_status") == "margin_below_threshold"
                for row in evaluated_scenarios
            )
            state["funnel"]["combinations_evaluated"] = len(evaluated_combinations)
            state["funnel"]["combinations_margin_passed"] = sum(
                row.get("evaluation_status") == "margin_passed"
                for row in evaluated_combinations
            )
            state["funnel"]["combinations_margin_below_threshold"] = sum(
                row.get("evaluation_status") == "margin_below_threshold"
                for row in evaluated_combinations
            )
            state["funnel"]["final_products"] = len(results)
            state["funnel"]["excluded_listings"] = sum(
                row.get("evaluation_status") in {
                    "catalog_incompatible", "beauty_filtered", "bsr_filtered",
                    "competition_filtered",
                }
                or row.get("fee_status") == "invalid"
                or row.get("fee_status") == "unavailable"
                for product in state["candidates"]
                for row in product.get("amazon_listings") or []
            )
            state["opportunity_combinations"] = [
                combination for product in state["candidates"]
                for combination in product.get("opportunity_combinations") or []
            ]
            state["status"] = "completed"
            state["completed_at"] = _now()
            state["duration_seconds"] = time.monotonic() - started
            _checkpoint(checkpoint_store, state, "completed", progress=progress)
        return state
    except Exception as exc:
        state["status"] = "failed"
        state.setdefault("errors", []).append({"phase": state.get("phase"), "message": str(exc)[:500], "at": _now()})
        checkpoint_store.save(state)
        logger.exception("DISCOVERY FAILED | job_id=%s phase=%s", job_id, state.get("phase"))
        raise
