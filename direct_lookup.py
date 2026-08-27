"""Supplier-first direct EAN lookup built on the Discovery economic core."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any

from discovery import DiscoveryCheckpointStore, run_discovery
from purchase_scenarios import merge_product_candidates, scenario_requirement_label, target_price
from supplier_catalog import SupplierCatalogStore, canonical_gtin14


DIRECT_LOOKUP_SCHEMA_VERSION = 1
DIRECT_SUPPLIERS = ("abw", "umma", "qudo", "qogita")


def format_eur(value: Any, fallback: str = "—") -> str:
    if value in (None, "", "None"):
        return fallback
    try:
        amount = Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except (InvalidOperation, TypeError, ValueError):
        return fallback
    rendered = f"{amount:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"€{rendered}"


def format_percent(value: Any, fallback: str = "—") -> str:
    if value in (None, "", "None"):
        return fallback
    try:
        amount = Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except (InvalidOperation, TypeError, ValueError):
        return fallback
    return f"{str(amount).replace('.', ',')}%"


def scenario_stock_availability(scenario: dict[str, Any]) -> str:
    stock = scenario.get("stock_quantity")
    if stock is None:
        stock = scenario.get("stock")
    availability_values = []
    for value in (scenario.get("availability_status"), scenario.get("availability_text")):
        if value not in (None, "") and str(value) not in availability_values:
            availability_values.append(str(value))
    availability = " · ".join(availability_values)
    if stock is not None:
        return f"{stock} pz" + (f" · {availability}" if availability else "")
    return str(availability or "—")


def load_direct_supplier_context(
    identifier: str, *, store: SupplierCatalogStore | None = None,
    suppliers=DIRECT_SUPPLIERS, now: datetime | None = None,
) -> dict[str, Any]:
    """Read one EAN from active Scout generations without Manager fallback."""
    store = store or SupplierCatalogStore()
    requested = str(identifier or "").strip()
    comparison = canonical_gtin14(requested)
    if comparison is None:
        raise ValueError("EAN/GTIN non valido")
    now = now or datetime.now(timezone.utc)
    candidates = []
    statuses = {}
    for supplier in suppliers:
        generation = (
            store.serving_generation_metadata(supplier) if supplier == "qogita"
            else store.active_generation_metadata(supplier)
        )
        if not generation:
            statuses[supplier] = {
                "availability_status": "unavailable", "snapshot_id": None,
                "snapshot_at": None, "freshness": "unavailable",
                "reason": (
                    "scenario_bootstrap_in_progress" if supplier == "qogita"
                    else "baseline_missing"
                ),
            }
            continue
        matches = store.active_candidates_for_identifier(supplier, requested)
        completed = generation.get("completed_at")
        completed_at = None
        try:
            completed_at = datetime.fromisoformat(str(completed).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            pass
        age_hours = (
            (now - completed_at.astimezone(timezone.utc)).total_seconds() / 3600
            if completed_at else None
        )
        catalog_present_pending = bool(
            supplier == "qogita" and not matches
            and store.serving_catalog_contains_identifier(supplier, requested)
        )
        statuses[supplier] = {
            "availability_status": (
                "available" if matches else
                "catalog_present_scenarios_pending" if catalog_present_pending else
                "ean_absent"
            ),
            "snapshot_id": generation.get("run_id"), "snapshot_at": completed,
            "freshness": "fresh" if age_hours is not None and age_hours <= 168 else "stale",
            "age_hours": age_hours,
            "scenario_count": sum(len(row.get("scenarios") or []) for row in matches),
            "coverage_type": generation.get("product_catalog_coverage_type")
            or generation.get("coverage_type"),
            "coverage_percent": generation.get("coverage_percent"),
            "bootstrap_state": generation.get("bootstrap_state"),
        }
        candidates.extend(matches)
    merged = merge_product_candidates(*([row] for row in candidates)) if candidates else []
    if merged:
        candidate = merged[0]
    else:
        canonical_ean = requested[-13:] if len(requested) == 14 and requested.startswith("0") else requested
        candidate = {
            "product_key": f"direct:{comparison}", "canonical_ean": canonical_ean,
            "gtin": canonical_ean, "identifier_type": "EAN" if len(canonical_ean) == 13 else "GTIN",
            "brand": None, "title": None, "scenarios": [], "suppliers": [],
        }
    candidate["gtin"] = candidate.get("canonical_ean") or requested
    candidate["direct_lookup"] = True
    for scenario in candidate.get("scenarios") or []:
        scenario.setdefault("supplier_snapshot_at", statuses.get(
            str(scenario.get("supplier") or "").casefold(), {}
        ).get("snapshot_at"))
    return {
        "requested_identifier": requested,
        "canonical_identifier": comparison,
        "candidate": candidate,
        "supplier_snapshot_set": statuses,
        "supplier_memberships": sorted({
            str(row.get("supplier") or "").casefold()
            for row in candidate.get("scenarios") or []
        }),
    }


def direct_supplier_preparer(context):
    def prepare(selected_suppliers, filters, **kwargs):
        candidate = context["candidate"]
        statuses = context["supplier_snapshot_set"]
        selected = list(selected_suppliers)
        scenario_counts = {
            supplier: sum(
                str(row.get("supplier") or "").casefold() == supplier
                for row in candidate.get("scenarios") or []
            ) for supplier in selected
        }
        product_counts = {supplier: int(count > 0) for supplier, count in scenario_counts.items()}
        return {
            "selected_suppliers": selected,
            "supplier_snapshot_set": {supplier: statuses[supplier] for supplier in selected},
            "supplier_diagnostics": {}, "supplier_warnings": [],
            "candidates": [candidate], "usable_suppliers": selected,
            "coverage": {
                "unique_eans": 1, "shared_eans": int(sum(product_counts.values()) > 1),
                "products_by_supplier": product_counts,
                "scenarios_by_supplier": scenario_counts,
            },
            "total_supplier_ean_universe": 1, "eligible_identifier_count": 1,
            "run_budget": "direct", "sampled_identifier_count": 1,
            "sampling_strategy": "explicit_direct_identifier_v1",
        }
    return prepare


def run_direct_lookup(
    identifier: str, *, catalog_batch, pricing_batch, fees_batch, token_provider,
    store: SupplierCatalogStore | None = None,
    checkpoint_store: DiscoveryCheckpointStore | None = None,
    job_id: str | None = None, sleep_func=lambda _: None,
) -> dict[str, Any]:
    context = load_direct_supplier_context(identifier, store=store)
    checkpoint_store = checkpoint_store or DiscoveryCheckpointStore("data/direct_lookup_jobs")
    filters = {
        "bsr_min": 0, "bsr_max": 2_147_483_647,
        "max_fba_sellers": 2_147_483_647,
        "max_total_sellers": 2_147_483_647,
        "minimum_margin": 0, "minimum_qogita_stock": 0,
    }
    state = run_discovery(
        filters, checkpoint_store=checkpoint_store,
        catalog_batch=catalog_batch, pricing_batch=pricing_batch,
        fees_batch=fees_batch, token_provider=token_provider,
        selected_suppliers=list(DIRECT_SUPPLIERS), run_budget=None,
        supplier_preparer=direct_supplier_preparer(context), rotation_store=None,
        job_id=job_id, sleep_func=sleep_func,
        catalog_batch_interval=0, pricing_batch_interval=0, fee_batch_interval=0,
    )
    state.update({
        "schema_version": max(int(state.get("schema_version") or 0), 2),
        "direct_lookup_schema_version": DIRECT_LOOKUP_SCHEMA_VERSION,
        "lookup_type": "direct_ean", "ean_requested": context["requested_identifier"],
        "canonical_identifier": context["canonical_identifier"],
        "supplier_memberships": context["supplier_memberships"],
        "supplier_snapshot_set": context["supplier_snapshot_set"],
        "rotation_scope": None, "sampling_strategy": "explicit_direct_identifier_v1",
    })
    checkpoint_store.save(state)
    return state


def direct_scenario_rows(state: dict[str, Any]) -> list[dict[str, Any]]:
    candidates = state.get("candidates") or []
    if not candidates:
        return []
    product = candidates[0]
    recommended = (product.get("recommended_combination") or {}).get("scenario_id")
    rows = []
    for scenario in product.get("scenarios") or []:
        economics = scenario.get("economics") or {}
        best_combination = next((
            row for row in product.get("opportunity_combinations") or []
            if row.get("scenario_id") == scenario.get("scenario_id")
            and row.get("asin") == scenario.get("best_asin")
        ), {})
        margin = best_combination.get("margin_percent", scenario.get("margin_percent"))
        rows.append({
            "Raccomandato": "✓" if scenario.get("scenario_id") == recommended else "",
            "Fornitore": str(scenario.get("supplier") or "").upper(),
            "Scenario": scenario.get("scenario_label") or scenario.get("scenario_type"),
            "Requisito": scenario_requirement_label(scenario),
            "Costo": format_eur(scenario.get("cost_gross_unit_eur")),
            "Stock / disponibilità": scenario_stock_availability(scenario),
            "Freshness": " · ".join(filter(None, (
                str(scenario.get("freshness_status") or ""),
                str(scenario.get("snapshot_at") or scenario.get("supplier_snapshot_at") or ""),
            ))) or "—",
            "Prezzo Amazon": format_eur(best_combination.get("price_reference")),
            "Utile": format_eur(economics.get("profit")),
            "Margine": format_percent(economics.get("margin_percent")),
            "P15": format_eur(target_price(economics, 15)),
            "P20": format_eur(target_price(economics, 20)),
            "P25": format_eur(target_price(economics, 25)),
            "Score": scenario.get("score") if scenario.get("score") is not None else "—",
            "Stato": scenario.get("evaluation_status") or "economics_unavailable",
            "_margin": Decimal(str(margin)) if margin is not None else Decimal("-Infinity"),
            "_cost": Decimal(str(scenario.get("cost_gross_unit_eur") or "Infinity")),
        })
    rows.sort(key=lambda row: (
        row["Raccomandato"] != "✓", -row["_margin"], row["_cost"],
        row["Fornitore"], str(row["Scenario"]),
    ))
    for row in rows:
        row.pop("_margin", None)
        row.pop("_cost", None)
    return rows
