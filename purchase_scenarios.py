"""Supplier-neutral models and ranking rules for Discovery purchase scenarios."""

from __future__ import annotations

import hashlib
from dataclasses import asdict, dataclass, field
from decimal import Decimal
from typing import Any


def _stable_id(prefix: str, *parts: object) -> str:
    material = "|".join(str(part or "").strip().casefold() for part in parts)
    digest = hashlib.sha256(material.encode("utf-8")).hexdigest()[:24]
    return f"{prefix}_{digest}"


def product_key(canonical_ean: str) -> str:
    return _stable_id("product", canonical_ean)


def scenario_key(
    *, supplier: str, supplier_alias: str, supplier_product_id: str,
    supplier_offer_id: str, variant_id: str, canonical_ean: str,
    scenario_type: str, account_mov: object,
) -> str:
    """Build an ID from scenario identity, deliberately excluding price/time."""
    try:
        normalized_mov = Decimal(str(account_mov)).normalize()
    except Exception:
        normalized_mov = str(account_mov or "")
    return _stable_id(
        "scenario", supplier, supplier_alias, supplier_product_id,
        supplier_offer_id, variant_id, canonical_ean, scenario_type,
        normalized_mov,
    )


@dataclass(frozen=True)
class PurchaseScenario:
    scenario_id: str
    product_key: str
    canonical_ean: str
    identifier_type: str
    supplier: str
    supplier_alias: str
    supplier_product_id: str
    supplier_offer_id: str
    variant_id: str
    brand: str
    title: str
    scenario_type: str
    scenario_label: str
    scenario_order: int
    account_mov: Decimal
    account_mov_currency: str
    account_mov_eur: Decimal
    selling_unit: int
    cost_net_unit_eur: Decimal
    vat_rate: Decimal
    vat_amount_unit: Decimal
    cost_gross_unit_eur: Decimal
    stock: int | None
    snapshot_id: str
    snapshot_at: str
    freshness_status: str
    tier_is_active: bool
    source_metadata: dict[str, Any] = field(default_factory=dict)
    supplier_barcode_raw: str | None = None
    minimum_product_quantity: int | None = None
    maximum_product_quantity: int | None = None
    warehouse: str | None = None
    shipping_mode: str | None = None
    lead_time: str | None = None
    availability_status: str | None = None
    availability_text: str | None = None
    condition_key: str | None = None
    bundle_quantity: int | None = None
    source_pack_total_price: Decimal | None = None
    source_net_unit_price: Decimal | None = None
    source_currency: str | None = None
    fx_rate: Decimal | None = None
    fx_date: str | None = None
    fx_source: str | None = None
    fx_status: str | None = None
    supplier_sku: str | None = None
    product_url: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ProductCandidate:
    product_key: str
    canonical_ean: str
    identifier_type: str
    brand: str
    title: str
    category: str
    image_url: str
    scenarios: tuple[PurchaseScenario, ...]

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["scenarios"] = [scenario.to_dict() for scenario in self.scenarios]
        return value


@dataclass(frozen=True)
class AmazonObservation:
    observation_id: str
    marketplace: str
    canonical_ean: str
    asin: str
    amazon_brand: str
    amazon_title: str
    bsr_beauty: int
    reference_price: Decimal
    price_source: str
    fba_sellers: int
    total_sellers: int
    seller_count_source: str
    min_fba_price: Decimal | None = None
    min_fbm_price: Decimal | None = None
    fba_fee_net: Decimal | None = None
    fba_fee_gross: Decimal | None = None
    fba_source: str | None = None
    referral_fee: Decimal | None = None
    referral_rate: Decimal | None = None
    referral_source: str | None = None
    observed_at: str | None = None
    diagnostics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class AmazonListing:
    """Catalog identity for one Amazon page associated with a physical product."""

    listing_id: str
    marketplace: str
    canonical_ean: str
    asin: str
    title: str
    brand: str
    manufacturer: str
    product_type: str
    display_group: str
    browse_classification: dict[str, Any]
    bsr_beauty: int | None
    beauty_status: str
    identifiers: tuple[dict[str, Any], ...]
    package_quantity: int | None
    number_of_items: int | None
    package_level: str | None
    volume_value: Decimal | None
    volume_unit: str | None
    model_number: str | None
    part_number: str | None
    relationships: tuple[dict[str, Any], ...]
    variation_theme: str | None
    main_image: str | None
    compatibility_status: str
    compatibility_reason: tuple[str, ...]
    min_fba_price: Decimal | None = None
    min_fbm_price: Decimal | None = None
    diagnostics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class OpportunityCombination:
    """Local economics for one supplier scenario sold through one Amazon ASIN."""

    combination_id: str
    product_key: str
    scenario_id: str
    asin: str
    amazon_observation_id: str
    supplier: str
    scenario_label: str
    cost_gross_unit_eur: Decimal
    price_reference: Decimal
    profit: Decimal | None
    margin_percent: Decimal | None
    target_prices: dict[str, Decimal | None]
    score: int
    opportunity: str
    evaluation_status: str
    score_bsr: int
    score_fba: int
    score_total_sellers: int
    score_margin: int
    diagnostics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def amazon_listing_key(marketplace: str, asin: str) -> str:
    return _stable_id("listing", marketplace, asin)


def opportunity_combination_key(scenario_id: str, observation_id: str) -> str:
    return _stable_id("combination", scenario_id, observation_id)


def amazon_observation_key(asin: str, reference_price: object) -> str:
    """Product Fees cache key: ASIN plus the exact reference price."""
    return _stable_id("amazon", asin, Decimal(str(reference_price)).normalize())


def _number(value: object, default: Decimal = Decimal("0")) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return default


TARGET_MARGIN_KEYS = ("15", "20", "25")
PURCHASE_SCENARIO_REQUIRED_FIELDS = (
    "scenario_id", "product_key", "canonical_ean", "supplier",
    "scenario_type", "account_mov", "cost_net_unit_eur",
    "cost_gross_unit_eur",
)


def purchase_scenario_validation(scenario: object) -> tuple[bool, str | None]:
    """Validate the supplier-neutral identity and economics inputs of a scenario."""
    if not isinstance(scenario, dict):
        return False, "scenario_not_object"
    missing = [
        field for field in PURCHASE_SCENARIO_REQUIRED_FIELDS
        if scenario.get(field) in (None, "")
    ]
    if missing:
        return False, "missing_" + "_".join(missing)
    for field in ("account_mov", "cost_net_unit_eur", "cost_gross_unit_eur"):
        value = _number(scenario.get(field), Decimal("NaN"))
        if not value.is_finite() or value <= 0:
            return False, f"invalid_{field}"
    return True, None


def recommended_scenario(product: object) -> dict | None:
    """Return a valid recommended scenario, never a synthetic legacy fallback."""
    if not isinstance(product, dict):
        return None
    recommended_id = (product.get("scenario_roles") or {}).get(
        "scenario_raccomandato"
    )
    for scenario in product.get("scenarios") or []:
        valid, _ = purchase_scenario_validation(scenario)
        if valid and scenario.get("scenario_id") == recommended_id:
            return scenario
    return None


def recommended_combination(product: object) -> dict | None:
    if not isinstance(product, dict):
        return None
    combination_id = (product.get("combination_roles") or {}).get(
        "recommended_combination"
    )
    combination = next(
        (
            row for row in product.get("opportunity_combinations") or []
            if row.get("combination_id") == combination_id
        ),
        None,
    )
    valid, _ = opportunity_combination_validation(combination)
    return combination if valid else None


def opportunity_combination_validation(value: object) -> tuple[bool, str | None]:
    if not isinstance(value, dict):
        return False, "combination_not_object"
    for field_name in (
        "combination_id", "scenario_id", "asin", "cost_gross_unit_eur",
        "price_reference", "profit", "margin_percent", "score",
    ):
        if value.get(field_name) in (None, ""):
            return False, f"missing_{field_name}"
    for field_name in (
        "cost_gross_unit_eur", "price_reference", "profit", "margin_percent",
    ):
        parsed = _number(value.get(field_name), Decimal("NaN"))
        if not parsed.is_finite():
            return False, f"invalid_{field_name}"
    return True, None


def scenario_requirement_label(scenario: object) -> str:
    if not isinstance(scenario, dict):
        return "—"
    supplier = str(scenario.get("supplier") or "").casefold()
    if supplier == "qogita":
        return f"MOV € {_number(scenario.get('account_mov')):,.0f}"
    if supplier == "umma":
        minimum = scenario.get("minimum_product_quantity")
        mov = _number(scenario.get("account_mov"))
        currency = str(scenario.get("account_mov_currency") or "USD")
        quantity = f"Min. {minimum} pz" if minimum is not None else "Min. —"
        return f"{quantity} · MOV {currency} {mov:,.0f}"
    if supplier == "abw":
        scenario_type = str(scenario.get("scenario_type") or "").casefold()
        if scenario_type == "abw_bulk_box":
            quantity = scenario.get("bundle_quantity")
            return f"Box {quantity}" if quantity is not None else "Bulk Box"
        minimum = scenario.get("minimum_product_quantity")
        maximum = scenario.get("maximum_product_quantity")
        if minimum is None:
            return "Fascia quantità"
        return (
            f"{minimum}–{maximum} pz" if maximum is not None
            else f"Da {minimum} pz"
        )
    if supplier == "qudo":
        minimum = scenario.get("minimum_product_quantity")
        unit = scenario.get("selling_unit")
        mov = _number(scenario.get("account_mov"))
        currency = str(scenario.get("account_mov_currency") or "EUR")
        quantity = f"Min. {minimum} pz" if minimum is not None else "Min. —"
        if unit is not None and unit != minimum:
            quantity += f" · multipli di {unit}"
        return f"{quantity} · MOV {currency} {mov:,.0f}"
    return str(scenario.get("scenario_label") or "—")


def merge_product_candidates(*collections: list[dict]) -> list[dict]:
    """Merge supplier candidates by canonical EAN without touching Amazon data."""
    merged: dict[str, dict] = {}
    scenario_ids: dict[str, set[str]] = {}
    for collection in collections:
        for candidate in collection or []:
            ean = str(
                candidate.get("canonical_ean") or candidate.get("gtin") or ""
            ).strip()
            if not ean:
                continue
            if ean not in merged:
                merged[ean] = {
                    "product_key": candidate.get("product_key") or product_key(ean),
                    "canonical_ean": ean,
                    "gtin": ean,
                    "identifier_type": candidate.get("identifier_type") or "",
                    "brand": candidate.get("brand") or "",
                    "title": candidate.get("title") or "",
                    "category": candidate.get("category") or "",
                    "image_url": candidate.get("image_url") or "",
                    "scenarios": [],
                }
                scenario_ids[ean] = set()
            target = merged[ean]
            for field in ("brand", "title", "category", "image_url"):
                if not target.get(field) and candidate.get(field):
                    target[field] = candidate[field]
            for scenario in candidate.get("scenarios") or []:
                scenario_id = str(scenario.get("scenario_id") or "")
                if not scenario_id or scenario_id in scenario_ids[ean]:
                    continue
                scenario_ids[ean].add(scenario_id)
                target["scenarios"].append(scenario)
    for candidate in merged.values():
        candidate["scenarios"].sort(key=lambda row: (
            str(row.get("supplier") or ""),
            int(row.get("scenario_order") or 0),
            str(row.get("scenario_id") or ""),
        ))
        suppliers = sorted({
            str(row.get("supplier") or "").casefold()
            for row in candidate["scenarios"] if row.get("supplier")
        })
        candidate["supplier"] = suppliers[0] if len(suppliers) == 1 else "multi"
        candidate["suppliers"] = suppliers
    return sorted(merged.values(), key=lambda row: row["canonical_ean"])


def canonicalize_target_prices(economics: dict | None) -> dict:
    """Keep target-price keys and numeric values stable across JSON round-trips."""
    if not isinstance(economics, dict):
        return economics or {}
    raw = economics.get("target_prices")
    if not isinstance(raw, dict):
        return economics

    def canonical_value(value: object) -> Decimal | None:
        if value is None:
            return None
        try:
            parsed = Decimal(str(value))
        except Exception:
            return None
        return parsed if parsed.is_finite() else None

    economics["target_prices"] = {
        key: canonical_value(raw.get(key, raw.get(int(key))))
        for key in TARGET_MARGIN_KEYS
    }
    return economics


def _record_numeric_error(value: dict, model: str, field_name: str) -> None:
    errors = value.setdefault("numeric_normalization_errors", [])
    marker = f"{model}.{field_name}:invalid_numeric_value"
    if marker not in errors:
        errors.append(marker)


def _normalize_decimal_field(value: dict, field_name: str, model: str) -> None:
    raw = value.get(field_name)
    if raw in (None, ""):
        value[field_name] = None
        return
    try:
        parsed = Decimal(str(raw))
    except (ArithmeticError, TypeError, ValueError):
        parsed = Decimal("NaN")
    if not parsed.is_finite():
        value[field_name] = None
        _record_numeric_error(value, model, field_name)
        return
    value[field_name] = parsed


def _normalize_integer_field(value: dict, field_name: str, model: str) -> None:
    raw = value.get(field_name)
    if raw in (None, ""):
        value[field_name] = None
        return
    try:
        parsed = Decimal(str(raw))
        if not parsed.is_finite() or parsed != parsed.to_integral_value():
            raise ValueError
        value[field_name] = int(parsed)
    except (ArithmeticError, TypeError, ValueError):
        value[field_name] = None
        _record_numeric_error(value, model, field_name)


def normalize_purchase_scenario(value: dict) -> dict:
    """Restore the numeric contract of a PurchaseScenario after JSON load."""
    if not isinstance(value, dict):
        return value
    for field_name in (
        "account_mov", "account_mov_eur", "cost_net_unit_eur", "vat_rate",
        "vat_amount_unit", "cost_gross_unit_eur", "source_net_unit_price",
        "source_pack_total_price", "fx_rate", "margin_percent",
    ):
        _normalize_decimal_field(value, field_name, "PurchaseScenario")
    for field_name in (
        "scenario_order", "selling_unit", "stock", "minimum_product_quantity",
        "maximum_product_quantity", "score", "score_bsr", "score_fba",
        "score_total_sellers", "score_margin", "bundle_quantity",
    ):
        _normalize_integer_field(value, field_name, "PurchaseScenario")
    normalize_economics(value.get("economics"), model="PurchaseScenario.economics")
    return value


def normalize_amazon_listing(value: dict) -> dict:
    """Restore persisted Catalog/Pricing numeric values without inventing them."""
    if not isinstance(value, dict):
        return value
    for field_name in (
        "volume_value", "reference_price", "min_fba_price", "min_fbm_price",
    ):
        _normalize_decimal_field(value, field_name, "AmazonListing")
    for field_name in (
        "bsr_beauty", "package_quantity", "number_of_items", "fba_sellers",
        "total_sellers",
    ):
        _normalize_integer_field(value, field_name, "AmazonListing")
    return value


def normalize_amazon_observation(value: dict) -> dict:
    """Restore the numeric contract of an AmazonObservation after JSON load."""
    if not isinstance(value, dict):
        return value
    for field_name in (
        "reference_price", "min_fba_price", "min_fbm_price", "fba_fee_net",
        "fba_fee_gross", "referral_fee", "referral_rate",
    ):
        _normalize_decimal_field(value, field_name, "AmazonObservation")
    for field_name in ("bsr_beauty", "fba_sellers", "total_sellers", "fee_attempts"):
        _normalize_integer_field(value, field_name, "AmazonObservation")
    estimate = value.get("fee_estimate")
    if isinstance(estimate, dict):
        for field_name in (
            "fba_fee_net", "fba_tax", "fba_fee_gross", "referral_fee",
            "referral_rate",
        ):
            _normalize_decimal_field(
                estimate, field_name, "AmazonObservation.fee_estimate"
            )
    return value


def normalize_economics(value: dict | None, *, model: str = "Economics") -> dict | None:
    if not isinstance(value, dict):
        return value
    for field_name in (
        "reference_price", "cost", "referral_fee", "referral_rate",
        "fba_fee_net", "fba_fee_gross", "profit", "margin_percent",
    ):
        _normalize_decimal_field(value, field_name, model)
    canonicalize_target_prices(value)
    return value


def normalize_opportunity_combination(value: dict) -> dict:
    """Restore the numeric contract of a scenario/listing economics result."""
    if not isinstance(value, dict):
        return value
    for field_name in (
        "cost_gross_unit_eur", "price_reference", "profit", "margin_percent",
    ):
        _normalize_decimal_field(value, field_name, "OpportunityCombination")
    for field_name in (
        "score", "score_bsr", "score_fba", "score_total_sellers",
        "score_margin",
    ):
        _normalize_integer_field(value, field_name, "OpportunityCombination")
    canonicalize_target_prices(value)
    normalize_economics(
        value.get("economics"), model="OpportunityCombination.economics"
    )
    return value


def target_price(economics: dict | None, margin: int | str):
    canonical = canonicalize_target_prices(economics)
    return (canonical.get("target_prices") or {}).get(str(margin))


def assign_scenario_roles(scenarios: list[dict], minimum_margin: object) -> dict:
    """Assign deterministic, supplier-neutral scenario roles."""
    if not scenarios:
        return {
            "scenario_base": None,
            "scenario_base_by_supplier": {},
            "scenario_minimo_redditizio": None,
            "scenario_minimo_redditizio_by_supplier": {},
            "scenario_migliore": None,
            "scenario_raccomandato": None,
        }
    threshold = _number(minimum_margin)
    for scenario in scenarios:
        scenario["roles"] = []

    def operational_requirement(row):
        if str(row.get("supplier") or "").casefold() == "umma":
            return _number(row.get("minimum_product_quantity"))
        return _number(row.get("account_mov"))

    by_supplier: dict[str, list[dict]] = {}
    for scenario in scenarios:
        by_supplier.setdefault(
            str(scenario.get("supplier") or "unknown").casefold(), []
        ).append(scenario)
    base_by_supplier = {
        supplier: min(rows, key=lambda row: (
            operational_requirement(row),
            _number(row.get("cost_gross_unit_eur")),
            row.get("scenario_id") or "",
        ))
        for supplier, rows in by_supplier.items()
    }
    base = (
        next(iter(base_by_supplier.values()))
        if len(base_by_supplier) == 1 else None
    )
    profitable = [
        row for row in scenarios
        if row.get("economics_status") == "ready"
        and _number(row.get("margin_percent"), Decimal("-Infinity")) >= threshold
    ]
    profitable_by_supplier = {
        supplier: [row for row in rows if row in profitable]
        for supplier, rows in by_supplier.items()
    }
    minimum_profitable_by_supplier = {
        supplier: min(rows, key=lambda row: (
            operational_requirement(row),
            -_number(row.get("margin_percent")),
            row.get("scenario_id") or "",
        ))
        for supplier, rows in profitable_by_supplier.items() if rows
    }
    minimum_profitable = (
        next(iter(minimum_profitable_by_supplier.values()))
        if len(by_supplier) == 1 and minimum_profitable_by_supplier else None
    )
    evaluated = [row for row in scenarios if row.get("economics_status") == "ready"]
    best = min(evaluated, key=lambda row: (
        -_number(row.get("margin_percent")),
        _number(row.get("cost_gross_unit_eur")),
        operational_requirement(row) if len(by_supplier) == 1 else Decimal("0"),
        row.get("scenario_id") or "",
    ), default=next(iter(base_by_supplier.values())))
    recommended = min(evaluated, key=lambda row: (
        -int(row.get("score") or 0),
        -_number(row.get("margin_percent")),
        operational_requirement(row) if len(by_supplier) == 1 else Decimal("0"),
        row.get("scenario_id") or "",
    ), default=next(iter(base_by_supplier.values())))

    roles = {
        "scenario_base": base.get("scenario_id") if base else None,
        "scenario_base_by_supplier": {
            supplier: row.get("scenario_id")
            for supplier, row in base_by_supplier.items()
        },
        "scenario_minimo_redditizio": (
            minimum_profitable.get("scenario_id") if minimum_profitable else None
        ),
        "scenario_minimo_redditizio_by_supplier": {
            supplier: row.get("scenario_id")
            for supplier, row in minimum_profitable_by_supplier.items()
        },
        "scenario_migliore": best.get("scenario_id"),
        "scenario_raccomandato": recommended.get("scenario_id"),
    }
    labels = {
        "scenario_base": "Base",
        "scenario_minimo_redditizio": "Minimo redditizio",
        "scenario_migliore": "Migliore",
        "scenario_raccomandato": "Raccomandato",
    }
    by_id = {row.get("scenario_id"): row for row in scenarios}
    for role, scenario_id in roles.items():
        if isinstance(scenario_id, dict):
            label = labels.get(role.replace("_by_supplier", ""))
            for supplier_scenario_id in scenario_id.values():
                if supplier_scenario_id in by_id and label not in by_id[supplier_scenario_id]["roles"]:
                    by_id[supplier_scenario_id]["roles"].append(label)
            continue
        if scenario_id in by_id:
            by_id[scenario_id]["roles"].append(labels[role])
    return roles
