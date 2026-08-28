"""Batch Amazon adapters used only by Discovery V1."""

from __future__ import annotations

import logging
import random
import re
import time
import urllib.parse
from decimal import Decimal

import requests

from batch_analysis import select_reference_price
from purchase_scenarios import AmazonListing, amazon_listing_key


logger = logging.getLogger(__name__)
EU_ENDPOINT = "https://sellingpartnerapi-eu.amazon.com"
MAX_BATCH_SIZE = 20
CATALOG_BATCH_INTERVAL_SECONDS = 0.5
TRANSIENT_STATUSES = {429, 500, 502, 503, 504}


class AmazonBatchError(RuntimeError):
    pass


class CatalogItems(list):
    """Catalog items plus non-persistent pagination diagnostics."""

    def __init__(
        self, values=(), *, invalid_identifiers=(), incomplete_identifiers=(),
        batch_diagnostics=(),
    ):
        super().__init__(values)
        self.invalid_identifiers = tuple(invalid_identifiers)
        self.incomplete_identifiers = tuple(incomplete_identifiers)
        self.batch_diagnostics = tuple(batch_diagnostics)


class RefreshingTokenProvider:
    def __init__(self, fetcher, *, lifetime_seconds=3000, clock=time.monotonic):
        self.fetcher = fetcher
        self.lifetime_seconds = lifetime_seconds
        self.clock = clock
        self._token = None
        self._expires_at = 0

    def invalidate(self):
        self._token = None
        self._expires_at = 0

    def get(self):
        now = self.clock()
        if not self._token or now >= self._expires_at:
            self._token = self.fetcher()
            self._expires_at = now + self.lifetime_seconds
        return self._token


def _rate_limit(response):
    raw = response.headers.get("x-amzn-RateLimit-Limit")
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _request_with_retry(
    method,
    url,
    *,
    token_provider,
    request_func=requests.request,
    max_attempts=4,
    backoff_seconds=2,
    sleep_func=time.sleep,
    job_id="",
    phase="",
    random_func=random.random,
    **kwargs,
):
    last_error = None
    base_headers = dict(kwargs.pop("headers", {}) or {})
    for attempt in range(1, max_attempts + 1):
        try:
            headers = dict(base_headers)
            # Token acquisition is part of the logical Amazon request. Keeping
            # it inside the retry boundary prevents a transient LWA outage from
            # being misclassified as an immediate Catalog result.
            headers["x-amz-access-token"] = token_provider.get()
            response = request_func(method, url, headers=headers, **kwargs)
        except requests.RequestException as exc:
            last_error = exc
            status = None
        else:
            status = response.status_code
            limit = _rate_limit(response)
            logger.info(
                "DISCOVERY AMAZON | job_id=%s phase=%s attempt=%s status=%s rate_limit=%s",
                job_id, phase, attempt, status, limit if limit is not None else "unknown",
            )
            if 200 <= status < 300:
                return response
            if status == 401 and attempt < max_attempts:
                token_provider.invalidate()
                last_error = AmazonBatchError("Amazon token rejected")
                continue
            if status not in TRANSIENT_STATUSES:
                response.raise_for_status()
            last_error = AmazonBatchError(f"Amazon temporary status {status}")
        if attempt < max_attempts:
            delay = backoff_seconds * (2 ** (attempt - 1))
            delay += min(1.0, delay * 0.1) * random_func()
            logger.warning(
                "DISCOVERY AMAZON RETRY | job_id=%s phase=%s attempt=%s "
                "status=%s error=%s cause=%s delay=%.3f",
                job_id, phase, attempt, status,
                type(last_error).__name__ if last_error else "none",
                _request_exception_cause(last_error), delay,
            )
            sleep_func(delay)
    raise last_error or AmazonBatchError("Amazon request failed")


def _request_exception_cause(error):
    """Return only exception class names, never URLs, payloads or credentials."""
    if error is None:
        return "none"
    names = []
    seen = set()
    current = error
    while current is not None and id(current) not in seen and len(names) < 5:
        seen.add(id(current))
        names.append(type(current).__name__)
        current = current.__cause__ or current.__context__
    return ">".join(names)


def classify_catalog_identifier(value):
    """Map supported numeric identifiers to the exact SP-API identifier type."""
    identifier = str(value or "").strip()
    if not identifier.isdigit():
        return None
    return {12: "UPC", 13: "EAN", 14: "GTIN"}.get(len(identifier))


def _valid_gs1_check_digit(identifier):
    """Validate UPC-12, EAN-13 and GTIN-14 using the GS1 check digit."""
    value = str(identifier or "").strip()
    if not value.isdigit() or len(value) not in {12, 13, 14}:
        return False
    body = value[:-1]
    weighted = sum(
        int(digit) * (3 if offset % 2 == 0 else 1)
        for offset, digit in enumerate(reversed(body))
    )
    expected = (10 - weighted % 10) % 10
    return expected == int(value[-1])


def normalize_commercial_identifier(value, identifier_type=None):
    """Return raw GS1 identity and a GTIN-14 comparison form when valid."""
    raw_identifier = str(value or "").strip()
    raw_type = str(identifier_type or "").strip().upper() or (
        classify_catalog_identifier(raw_identifier)
    )
    expected_type = classify_catalog_identifier(raw_identifier)
    canonical = None
    if (
        raw_type in {"EAN", "UPC", "GTIN"}
        and expected_type == raw_type
        and _valid_gs1_check_digit(raw_identifier)
    ):
        canonical = raw_identifier.zfill(14)
    return {
        "raw_identifier": raw_identifier,
        "raw_type": raw_type or None,
        "canonical_gtin14": canonical,
    }


def catalog_identifier_batches(identifiers):
    """Return homogeneous identifier batches while preserving input order."""
    if not identifiers or len(identifiers) > MAX_BATCH_SIZE:
        raise ValueError("Catalog batch must contain 1-20 identifiers")
    grouped = {}
    invalid = []
    for raw_identifier in identifiers:
        identifier = str(raw_identifier or "").strip()
        identifier_type = classify_catalog_identifier(identifier)
        if identifier_type is None:
            invalid.append(identifier)
            continue
        values = grouped.setdefault(identifier_type, [])
        if identifier not in values:
            values.append(identifier)
    batches = [
        (identifier_type, values[start:start + MAX_BATCH_SIZE])
        for identifier_type, values in grouped.items()
        for start in range(0, len(values), MAX_BATCH_SIZE)
    ]
    return batches, invalid


def search_catalog_by_gtins_batch(
    gtins,
    token_provider,
    *,
    marketplace_id,
    request_func=requests.request,
    sleep_func=time.sleep,
    job_id="",
):
    batches, invalid = catalog_identifier_batches(gtins)
    items = []
    incomplete_identifiers = []
    batch_diagnostics = []
    for batch_number, (identifier_type, identifiers) in enumerate(batches, start=1):
        search_params = {
            "marketplaceIds": marketplace_id,
            "identifiers": ",".join(identifiers),
            "identifiersType": identifier_type,
            "pageSize": MAX_BATCH_SIZE,
            "includedData": (
                "summaries,identifiers,salesRanks,productTypes,images,"
                "relationships,attributes,classifications,dimensions"
            ),
        }
        page_token = None
        page_count = 0
        items_received = 0
        number_of_results = None
        had_next_token = False
        complete = True
        error = None
        while True:
            params = dict(search_params)
            if page_token:
                params["pageToken"] = page_token
            try:
                response = _request_with_retry(
                    "GET",
                    f"{EU_ENDPOINT}/catalog/2022-04-01/items",
                    token_provider=token_provider,
                    request_func=request_func,
                    sleep_func=sleep_func,
                    params=params,
                    headers={"Accept": "application/json"},
                    timeout=30,
                    job_id=job_id,
                    phase="catalog",
                )
            except Exception as exc:
                complete = False
                error = type(exc).__name__
                incomplete_identifiers.extend(identifiers)
                logger.error(
                    "DISCOVERY CATALOG INCOMPLETE | job_id=%s batch=%s "
                    "page=%s identifier_type=%s error=%s cause=%s",
                    job_id, batch_number, page_count + 1, identifier_type, error,
                    _request_exception_cause(exc),
                )
                break
            payload = response.json()
            page_count += 1
            page_items = payload.get("items") or []
            items_received += len(page_items)
            items.extend(page_items)
            if payload.get("numberOfResults") is not None:
                number_of_results = payload.get("numberOfResults")
            next_token = (payload.get("pagination") or {}).get("nextToken")
            if not next_token:
                break
            had_next_token = True
            page_token = next_token
            sleep_func(CATALOG_BATCH_INTERVAL_SECONDS)
        batch_diagnostics.append({
            "identifier_type": identifier_type,
            "page_count": page_count,
            "number_of_results": number_of_results,
            "had_next_token": had_next_token,
            "items_received": items_received,
            "input_identifier_count": len(identifiers),
            "complete": complete,
            "error": error,
        })
        if not complete:
            # This logical lookup has demonstrated an outage. Do not continue
            # with another identifier-type request; keep every unattempted
            # identifier explicitly retryable instead.
            for skipped_type, skipped_identifiers in batches[batch_number:]:
                incomplete_identifiers.extend(skipped_identifiers)
                batch_diagnostics.append({
                    "identifier_type": skipped_type,
                    "page_count": 0,
                    "number_of_results": None,
                    "had_next_token": False,
                    "items_received": 0,
                    "input_identifier_count": len(skipped_identifiers),
                    "complete": False,
                    "error": "circuit_open",
                })
            break
        if batch_number < len(batches):
            sleep_func(CATALOG_BATCH_INTERVAL_SECONDS)
    unique_items = {}
    without_asin = []
    for item in items:
        asin = str(item.get("asin") or "").strip()
        if asin:
            unique_items.setdefault(asin, item)
        else:
            without_asin.append(item)
    return CatalogItems(
        [*unique_items.values(), *without_asin],
        invalid_identifiers=invalid,
        incomplete_identifiers=dict.fromkeys(incomplete_identifiers),
        batch_diagnostics=batch_diagnostics,
    )


def _item_gtins(item):
    values = set()
    for marketplace in item.get("identifiers") or []:
        for identifier in marketplace.get("identifiers") or []:
            if str(identifier.get("identifierType") or "").upper() in {"EAN", "GTIN", "UPC"}:
                value = str(identifier.get("identifier") or "").strip()
                if value:
                    values.add(value)
    return values


def _item_trade_identifiers(item):
    values = []
    for marketplace in item.get("identifiers") or []:
        for identifier in marketplace.get("identifiers") or []:
            identifier_type = str(identifier.get("identifierType") or "").upper()
            if identifier_type not in {"EAN", "GTIN", "UPC"}:
                continue
            normalized = normalize_commercial_identifier(
                identifier.get("identifier"), identifier_type
            )
            if normalized["raw_identifier"]:
                values.append(normalized)
    return values


def beauty_rank(item):
    """Return a rank only when Amazon explicitly labels its display group Beauty."""
    for marketplace in item.get("salesRanks") or []:
        for rank in marketplace.get("displayGroupRanks") or []:
            group = str(rank.get("websiteDisplayGroup") or "").casefold()
            title = str(rank.get("title") or "").casefold()
            if group != "beauty_display_on_website" and title not in {
                "beauty", "bellezza",
            }:
                continue
            try:
                value = int(rank.get("rank"))
            except (TypeError, ValueError):
                continue
            if value > 0:
                return value, "display_group_beauty"
    return None, "beauty_rank_unverified"


def _attribute_value(attributes, name):
    rows = (attributes or {}).get(name) or []
    if not rows or not isinstance(rows[0], dict):
        return None
    return rows[0].get("value")


def _attribute_measure(attributes, *names):
    for name in names:
        rows = (attributes or {}).get(name) or []
        if not rows or not isinstance(rows[0], dict):
            continue
        try:
            value = Decimal(str(rows[0].get("value")))
        except Exception:
            continue
        unit = str(rows[0].get("unit") or "").casefold()
        if value > 0:
            return value, unit
    return None, None


def _normalized_text(value):
    return re.sub(r"[^a-z0-9]+", "", str(value or "").casefold())


def _title_volume(value):
    match = re.search(
        r"(?<!\d)(\d+(?:[.,]\d+)?)\s*"
        r"(fl\s*oz|ml|cl|l|kg|g)(?!\w)",
        str(value or ""), re.I,
    )
    if not match:
        return None, None
    unit = re.sub(r"\s+", " ", match.group(2).casefold()).strip()
    return Decimal(match.group(1).replace(",", ".")), unit


MEASURE_UNITS = {
    "ml": ("volume", Decimal("1"), "ml"),
    "milliliter": ("volume", Decimal("1"), "ml"),
    "milliliters": ("volume", Decimal("1"), "ml"),
    "millilitre": ("volume", Decimal("1"), "ml"),
    "millilitres": ("volume", Decimal("1"), "ml"),
    "cl": ("volume", Decimal("10"), "ml"),
    "centiliter": ("volume", Decimal("10"), "ml"),
    "centiliters": ("volume", Decimal("10"), "ml"),
    "centilitre": ("volume", Decimal("10"), "ml"),
    "centilitres": ("volume", Decimal("10"), "ml"),
    "l": ("volume", Decimal("1000"), "ml"),
    "liter": ("volume", Decimal("1000"), "ml"),
    "liters": ("volume", Decimal("1000"), "ml"),
    "litre": ("volume", Decimal("1000"), "ml"),
    "litres": ("volume", Decimal("1000"), "ml"),
    # NIST exact conversion used by Amazon-style fluid-ounce attributes.
    "fl oz": ("volume", Decimal("29.5735295625"), "ml"),
    "fl_oz": ("volume", Decimal("29.5735295625"), "ml"),
    "fluid ounce": ("volume", Decimal("29.5735295625"), "ml"),
    "fluid ounces": ("volume", Decimal("29.5735295625"), "ml"),
    "fluid_ounce": ("volume", Decimal("29.5735295625"), "ml"),
    "fluid_ounces": ("volume", Decimal("29.5735295625"), "ml"),
    "g": ("weight", Decimal("1"), "g"),
    "gram": ("weight", Decimal("1"), "g"),
    "grams": ("weight", Decimal("1"), "g"),
    "kg": ("weight", Decimal("1000"), "g"),
    "kilogram": ("weight", Decimal("1000"), "g"),
    "kilograms": ("weight", Decimal("1000"), "g"),
}
MEASURE_ABSOLUTE_TOLERANCE = Decimal("1")
MEASURE_RELATIVE_TOLERANCE = Decimal("0.01")


def _normalize_measure(value, unit):
    try:
        amount = Decimal(str(value))
    except Exception:
        return None
    normalized_unit = re.sub(
        r"\s+", " ", str(unit or "").strip().casefold()
    )
    definition = MEASURE_UNITS.get(normalized_unit)
    if not definition or not amount.is_finite() or amount <= 0:
        return None
    dimension, multiplier, base_unit = definition
    return {
        "dimension": dimension,
        "value": amount * multiplier,
        "unit": base_unit,
        "source_value": amount,
        "source_unit": normalized_unit,
    }


def _measure_comparison(expected_value, expected_unit, actual_value, actual_unit):
    expected = _normalize_measure(expected_value, expected_unit)
    actual = _normalize_measure(actual_value, actual_unit)
    diagnostic = {
        "supplier_normalized_value": expected.get("value") if expected else None,
        "supplier_normalized_unit": expected.get("unit") if expected else None,
        "amazon_normalized_value": actual.get("value") if actual else None,
        "amazon_normalized_unit": actual.get("unit") if actual else None,
        "supplier_dimension": expected.get("dimension") if expected else None,
        "amazon_dimension": actual.get("dimension") if actual else None,
        "absolute_delta": None,
        "relative_delta": None,
        "absolute_tolerance": MEASURE_ABSOLUTE_TOLERANCE,
        "relative_tolerance": MEASURE_RELATIVE_TOLERANCE,
        "matched_by": None,
    }
    if not expected or not actual:
        diagnostic["status"] = "not_comparable"
        return None, diagnostic
    if expected["dimension"] != actual["dimension"]:
        diagnostic["status"] = "different_dimensions"
        return None, diagnostic
    delta = abs(expected["value"] - actual["value"])
    relative = delta / expected["value"]
    absolute_match = delta <= MEASURE_ABSOLUTE_TOLERANCE
    relative_match = relative <= MEASURE_RELATIVE_TOLERANCE
    diagnostic.update({
        "absolute_delta": delta, "relative_delta": relative,
        "matched_by": (
            "absolute_and_relative" if absolute_match and relative_match
            else "absolute" if absolute_match
            else "relative" if relative_match else None
        ),
        "status": "match" if absolute_match or relative_match else "mismatch",
    })
    return absolute_match or relative_match, diagnostic


def _product_identity(product):
    product = product or {}
    scenarios = product.get("scenarios") or []
    brand = product.get("brand") or next(
        (row.get("brand") for row in scenarios if row.get("brand")), ""
    )
    title = product.get("title") or next(
        (row.get("title") for row in scenarios if row.get("title")), ""
    )
    volume_value = product.get("volume_value")
    volume_unit = product.get("volume_unit")
    if volume_value is None:
        volume_value, volume_unit = _title_volume(title)
    package_quantity = product.get("package_quantity")
    if package_quantity is None and not re.search(
        r"(?:pack|confezione|set)\s*(?:da|x)?\s*[2-9]", str(title), re.I
    ):
        package_quantity = 1
    return {
        "brand": str(brand or ""), "title": str(title or ""),
        "volume_value": volume_value, "volume_unit": volume_unit,
        "package_quantity": package_quantity,
        "product_type": product.get("product_type"),
    }


def _listing_compatibility(item, product):
    identity = _product_identity(product)
    summary = next(
        (row for row in item.get("summaries") or [] if isinstance(row, dict)), {}
    )
    attributes = item.get("attributes") or {}
    hard_conflicts = []
    evidence = []
    diagnostics = {}
    amazon_brand = str(summary.get("brand") or _attribute_value(attributes, "brand") or "")
    if identity["brand"] and amazon_brand:
        supplier_brand = _normalized_text(identity["brand"])
        listing_brand = _normalized_text(amazon_brand)
        if supplier_brand not in listing_brand and listing_brand not in supplier_brand:
            hard_conflicts.append("brand_mismatch")
        else:
            evidence.append("brand_match")

    package_quantity = summary.get("packageQuantity")
    if package_quantity is None:
        package_quantity = _attribute_value(attributes, "item_package_quantity")
    number_of_items = _attribute_value(attributes, "number_of_items")
    expected_pack = identity["package_quantity"]
    for actual, reason in (
        (package_quantity, "package_quantity_mismatch"),
        (number_of_items, "number_of_items_mismatch"),
    ):
        if expected_pack is not None and actual is not None:
            try:
                mismatch = int(actual) != int(expected_pack)
            except (TypeError, ValueError):
                mismatch = False
            if mismatch:
                hard_conflicts.append(reason)
            else:
                evidence.append(reason.replace("mismatch", "match"))

    listing_volume, listing_unit = _attribute_measure(
        attributes, "item_volume", "liquid_volume"
    )
    if listing_volume is None:
        listing_volume, listing_unit = _title_volume(
            summary.get("size") or summary.get("itemName")
        )
    expected_volume = identity["volume_value"]
    expected_unit = identity["volume_unit"]
    if expected_volume is not None and listing_volume is not None:
        volume_match, measurement = _measure_comparison(
            expected_volume, expected_unit, listing_volume, listing_unit
        )
        diagnostics["measurement_comparison"] = measurement
        if volume_match is False:
            hard_conflicts.append("volume_mismatch")
        elif volume_match is True:
            evidence.append("volume_match")

    listing_type = next(
        (row.get("productType") for row in item.get("productTypes") or [] if row.get("productType")),
        None,
    )
    if identity["product_type"] and listing_type:
        if _normalized_text(identity["product_type"]) != _normalized_text(listing_type):
            hard_conflicts.append("product_type_mismatch")
        else:
            evidence.append("product_type_match")
    if hard_conflicts:
        return (
            "incompatible", tuple(sorted(set(hard_conflicts))), evidence,
            diagnostics,
        )
    return (
        "compatible", tuple(evidence or ["ean_match_no_hard_conflict"]),
        evidence, diagnostics,
    )


def _catalog_listing(item, canonical_ean, product=None, marketplace="IT"):
    summary = next(
        (row for row in item.get("summaries") or [] if isinstance(row, dict)), {}
    )
    attributes = item.get("attributes") or {}
    marketplace = str(summary.get("marketplaceId") or marketplace)
    rank, rank_status = beauty_rank(item)
    compatibility, reasons, evidence, compatibility_diagnostics = (
        _listing_compatibility(item, product)
    )
    volume_value, volume_unit = _attribute_measure(
        attributes, "item_volume", "liquid_volume"
    )
    package_quantity = summary.get("packageQuantity")
    if package_quantity is None:
        package_quantity = _attribute_value(attributes, "item_package_quantity")
    images = [
        image for group in item.get("images") or []
        for image in group.get("images") or []
        if image.get("variant") == "MAIN"
    ]
    main_image = max(
        images, key=lambda row: int(row.get("height") or 0) * int(row.get("width") or 0),
        default={},
    ).get("link")
    display_group = str(summary.get("websiteDisplayGroup") or "")
    relationships = tuple(
        relationship
        for group in item.get("relationships") or []
        for relationship in group.get("relationships") or []
    )
    identifiers = tuple(
        identifier
        for group in item.get("identifiers") or []
        for identifier in group.get("identifiers") or []
    )
    variation_rows = attributes.get("variation_theme") or []
    variation = (
        variation_rows[0].get("name") or variation_rows[0].get("value")
        if variation_rows and isinstance(variation_rows[0], dict) else None
    )
    listing = AmazonListing(
        listing_id=amazon_listing_key(marketplace, str(item.get("asin") or "")),
        marketplace=marketplace, canonical_ean=canonical_ean,
        asin=str(item.get("asin") or ""),
        title=str(summary.get("itemName") or ""),
        brand=str(summary.get("brand") or _attribute_value(attributes, "brand") or ""),
        manufacturer=str(summary.get("manufacturer") or _attribute_value(attributes, "manufacturer") or ""),
        product_type=str(next((row.get("productType") for row in item.get("productTypes") or [] if row.get("productType")), "")),
        display_group=display_group,
        browse_classification=dict(summary.get("browseClassification") or {}),
        bsr_beauty=rank, beauty_status=rank_status,
        identifiers=identifiers,
        package_quantity=int(package_quantity) if package_quantity is not None else None,
        number_of_items=(
            int(_attribute_value(attributes, "number_of_items"))
            if _attribute_value(attributes, "number_of_items") is not None else None
        ),
        package_level=_attribute_value(attributes, "package_level"),
        volume_value=volume_value, volume_unit=volume_unit,
        model_number=str(summary.get("modelNumber") or _attribute_value(attributes, "model_number") or "") or None,
        part_number=str(summary.get("partNumber") or _attribute_value(attributes, "part_number") or "") or None,
        relationships=relationships, variation_theme=str(variation or "") or None,
        main_image=main_image,
        compatibility_status=compatibility,
        compatibility_reason=reasons,
        diagnostics={
            "compatibility_evidence": evidence,
            "commercial_identifiers": _item_trade_identifiers(item),
            **compatibility_diagnostics,
            "classification_records": item.get("classifications") or [],
            "dimensions": item.get("dimensions") or [],
        },
    ).to_dict()
    return listing


def correlate_catalog_items(gtins, items, products=None):
    input_identifiers = [str(gtin or "").strip() for gtin in gtins]
    by_gtin = {gtin: [] for gtin in input_identifiers}
    raw_inputs = set(input_identifiers)
    canonical_inputs = {}
    normalized_inputs = {}
    for gtin in input_identifiers:
        normalized = normalize_commercial_identifier(gtin)
        normalized_inputs[gtin] = normalized
        canonical = normalized.get("canonical_gtin14")
        if canonical:
            canonical_inputs.setdefault(canonical, set()).add(gtin)
    invalid_identifiers = set(getattr(items, "invalid_identifiers", ()))
    incomplete_identifiers = set(getattr(items, "incomplete_identifiers", ()))
    diagnostic_by_identifier = {}
    for diagnostic in getattr(items, "batch_diagnostics", ()):
        identifier_type = diagnostic.get("identifier_type")
        for gtin in input_identifiers:
            if classify_catalog_identifier(gtin) == identifier_type:
                diagnostic_by_identifier[gtin] = dict(diagnostic)
    for item in items or []:
        matched_inputs = _item_gtins(item) & raw_inputs
        for normalized in _item_trade_identifiers(item):
            canonical = normalized.get("canonical_gtin14")
            if canonical:
                matched_inputs.update(canonical_inputs.get(canonical, ()))
        for gtin in matched_inputs:
            by_gtin[gtin].append(item)
    if isinstance(products, list):
        products = {
            str(row.get("canonical_ean") or row.get("gtin") or ""): row
            for row in products
        }
    products = products or {}
    result = {}
    for gtin, matches in by_gtin.items():
        identifier_type = classify_catalog_identifier(gtin)
        correlation_diagnostics = {
            **diagnostic_by_identifier.get(gtin, {}),
            "input_identifier": normalized_inputs[gtin],
        }
        if gtin in invalid_identifiers or identifier_type is None:
            result[gtin] = {
                "status": "invalid_identifier",
                "identifier_type": None,
            }
            continue
        unique = {str(item.get("asin") or ""): item for item in matches if item.get("asin")}
        if not unique:
            if gtin in incomplete_identifiers:
                result[gtin] = {
                    "status": "catalog_incomplete",
                    "identifier_type": identifier_type,
                    "listings": [],
                    "diagnostics": correlation_diagnostics,
                }
                continue
            result[gtin] = {
                "status": "not_found",
                "identifier_type": identifier_type,
                "listings": [],
                "diagnostics": correlation_diagnostics,
            }
            continue
        listings = [
            _catalog_listing(item, gtin, products.get(gtin))
            for _, item in sorted(unique.items())
        ]
        if gtin not in products and len(listings) > 1:
            result[gtin] = {
                "status": "ambiguous", "identifier_type": identifier_type,
                "listings": listings,
                "ambiguity_reason": "supplier_identity_unavailable",
                "diagnostics": correlation_diagnostics,
            }
            continue
        compatible = [
            row for row in listings
            if row.get("compatibility_status") == "compatible"
        ]
        if not compatible:
            result[gtin] = {
                "status": "ambiguous",
                "identifier_type": identifier_type,
                "listings": listings,
                "ambiguity_reason": "no_compatible_listing",
                "diagnostics": correlation_diagnostics,
            }
            continue
        primary = compatible[0]
        result[gtin] = {
            "status": "resolved",
            "identifier_type": identifier_type,
            "asin": primary.get("asin"),
            "amazon_title": primary.get("title"),
            "amazon_brand": primary.get("brand"),
            "bsr_beauty": primary.get("bsr_beauty"),
            "beauty_status": primary.get("beauty_status"),
            "product_type": primary.get("product_type"),
            "listings": listings,
            "compatible_listing_count": len(compatible),
            "diagnostics": correlation_diagnostics,
        }
    return result


def build_item_offers_batch_requests(asins, marketplace_id):
    if not asins or len(asins) > MAX_BATCH_SIZE:
        raise ValueError("Pricing batch must contain 1-20 ASINs")
    return {"requests": [{
        "uri": f"/products/pricing/v0/items/{asin}/offers",
        "method": "GET",
        "MarketplaceId": marketplace_id,
        "ItemCondition": "New",
        "CustomerType": "Consumer",
    } for asin in asins]}


def get_item_offers_batch(
    asins,
    token_provider,
    *,
    marketplace_id,
    request_func=requests.request,
    sleep_func=time.sleep,
    job_id="",
):
    response = _request_with_retry(
        "POST",
        f"{EU_ENDPOINT}/batches/products/pricing/v0/itemOffers",
        token_provider=token_provider,
        request_func=request_func,
        sleep_func=sleep_func,
        json=build_item_offers_batch_requests(asins, marketplace_id),
        headers={"Accept": "application/json", "Content-Type": "application/json"},
        timeout=40,
        job_id=job_id,
        phase="pricing",
    )
    payload = response.json()
    return payload.get("responses", payload if isinstance(payload, list) else [])


def _money_amount(value):
    try:
        amount = float((value or {}).get("Amount"))
    except (TypeError, ValueError):
        return None
    return amount if amount > 0 else None


def _pricing_asin(entry, payload):
    asin = str(payload.get("ASIN") or "").strip()
    if asin:
        return asin
    request = entry.get("request") or {}
    uri = str(request.get("uri") or entry.get("uri") or "")
    match = re.search(r"/items/([A-Z0-9]{10})/offers", uri, re.I)
    return match.group(1).upper() if match else ""


def _offer_counts(summary, offers):
    fba = fbm = 0
    found_summary = False
    for row in summary.get("NumberOfOffers") or []:
        condition = str(row.get("condition") or row.get("Condition") or "").casefold()
        if condition and condition != "new":
            continue
        channel = str(row.get("fulfillmentChannel") or row.get("FulfillmentChannel") or "").casefold()
        try:
            count = int(row.get("OfferCount", row.get("offerCount")))
        except (TypeError, ValueError):
            continue
        found_summary = True
        if channel == "amazon":
            fba += count
        elif channel == "merchant":
            fbm += count
    if found_summary:
        return fba, fba + fbm, "summary_number_of_offers"
    fba = sum(bool(row.get("IsFulfilledByAmazon")) for row in offers)
    return fba, len(offers), "offers_fallback"


def parse_item_offers_batch(entries):
    parsed = {}
    for entry in entries or []:
        body = entry.get("body") or entry
        payload = body.get("payload") or body.get("Payload") or {}
        asin = _pricing_asin(entry, payload)
        if not asin:
            continue
        status = (entry.get("status") or {}).get("statusCode", entry.get("statusCode", 200))
        if int(status or 0) >= 400:
            parsed[asin] = {"status": "error", "status_code": int(status)}
            continue
        summary = payload.get("Summary") or {}
        offers = payload.get("Offers") or []
        fba_count, total_count, count_source = _offer_counts(summary, offers)
        buy_box = None
        for row in summary.get("BuyBoxPrices") or []:
            buy_box = _money_amount(row.get("LandedPrice"))
            if buy_box is not None:
                break
        fba_prices = []
        fbm_prices = []
        for row in summary.get("LowestPrices") or []:
            amount = _money_amount(row.get("LandedPrice"))
            channel = str(row.get("fulfillmentChannel") or "").casefold()
            if amount is not None:
                (fba_prices if channel == "amazon" else fbm_prices).append(amount)
        for offer in offers:
            listing = _money_amount(offer.get("ListingPrice")) or 0
            shipping = _money_amount(offer.get("Shipping")) or 0
            landed = listing + shipping
            if landed > 0:
                (fba_prices if offer.get("IsFulfilledByAmazon") else fbm_prices).append(landed)
        pricing = {
            "Buy Box Amount": buy_box,
            "Prezzo minimo FBA Amount": min(fba_prices, default=None),
            "Prezzo minimo FBM Amount": min(fbm_prices, default=None),
            "Venditori FBA": fba_count,
            "Venditori totali": total_count,
            "Seller count source": count_source,
        }
        reference, source = select_reference_price(pricing)
        parsed[asin] = {
            "status": "success",
            **pricing,
            "reference_price": reference,
            "price_source": source,
        }
    return parsed
