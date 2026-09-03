"""Fast, Amazon-only lookup for one commercial identifier.

The direct lookup intentionally stops after Catalog and Pricing. Discovery,
supplier scenarios, Fees, rotation and job persistence belong to the separate
full-catalog workflow and must never be pulled into this module.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Callable

from discovery_amazon import normalize_commercial_identifier
from discovery_freshness import AmazonFreshnessPolicy


DIRECT_LOOKUP_SCHEMA_VERSION = 2
NEGATIVE_CATALOG_STATUSES = {"not_found"}


def format_eur(value: Any, fallback: str = "—") -> str:
    if value in (None, "", "None"):
        return fallback
    try:
        amount = Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except (InvalidOperation, TypeError, ValueError):
        return fallback
    rendered = f"{amount:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"€{rendered}"


def _parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _fresh(value: Any, ttl, now: datetime) -> bool:
    observed = _parse_time(value)
    return bool(observed and now - observed <= ttl)


def _canonical_identifier(identifier: str) -> tuple[str, str]:
    requested = str(identifier or "").strip()
    normalized = normalize_commercial_identifier(requested)
    if not normalized.get("canonical_gtin14"):
        raise ValueError("EAN/GTIN non valido")
    canonical = requested[1:] if len(requested) == 14 and requested.startswith("0") else requested
    return requested, canonical


def _pricing_fields(value: dict[str, Any]) -> dict[str, Any]:
    """Normalize the robust Pricing parser contract onto an Amazon listing."""
    total = value.get("Venditori totali", value.get("total_sellers"))
    fba = value.get("Venditori FBA", value.get("fba_sellers"))
    try:
        fbm = int(total) - int(fba) if total is not None and fba is not None else None
    except (TypeError, ValueError):
        fbm = None
    buy_box = value.get("Buy Box Amount", value.get("buy_box_price"))
    reference = value.get("reference_price")
    source = value.get("price_source")
    if buy_box is None and source == "buy_box":
        buy_box = reference
    return {
        "pricing_status": value.get("status", value.get("pricing_status")),
        "buy_box_price": buy_box,
        "reference_price": reference,
        "price_source": source,
        "min_fba_price": value.get(
            "Prezzo minimo FBA Amount", value.get("min_fba_price")
        ),
        "min_fbm_price": value.get(
            "Prezzo minimo FBM Amount", value.get("min_fbm_price")
        ),
        "fba_sellers": fba,
        "fbm_sellers": fbm,
        "total_sellers": total,
        "seller_count_source": value.get(
            "Seller count source", value.get("seller_count_source")
        ),
    }


def _manager_listing(value: dict[str, Any]) -> dict[str, Any]:
    identity = value.get("identity") or {}
    bsr = value.get("bsr") or {}
    lowest_new = value.get("lowest_new") or {}
    return {
        "asin": identity.get("asin"),
        "title": identity.get("title"),
        "brand": identity.get("brand"),
        "main_image": identity.get("image_url"),
        "browse_classification": {
            "classificationId": bsr.get("category_id"),
            "displayName": bsr.get("category_name"),
        },
        "compatibility_status": "compatible",
        "bsr_rank": bsr.get("rank"),
        "bsr_category": bsr.get("category_id"),
        "bsr_category_label": bsr.get("category_name"),
        "bsr_status": bsr.get("status"),
        "bsr_observed_at": bsr.get("observed_at"),
        "bsr_observed_days": bsr.get("observed_days"),
        "lowest_new_price": lowest_new.get("current_price"),
        "lowest_new_status": lowest_new.get("status"),
        "lowest_new_observed_at": lowest_new.get("observed_at"),
        "lowest_new_observed_date": lowest_new.get("business_date") or lowest_new.get("observed_date"),
        "lowest_new_source": lowest_new.get("source"),
        "manager_identity": identity,
    }


def _compatible_listings(listings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    compatible = [
        row for row in listings
        if row.get("compatibility_status") in (None, "", "compatible")
    ]
    return compatible or listings


def _display_result(
    *, requested: str, canonical: str, catalog_status: str,
    listings: list[dict[str, Any]], catalog_observed_at: Any,
    cache_status: str, pricing_observed_at: Any = None,
) -> dict[str, Any]:
    compatible = _compatible_listings(listings)
    effective_status = catalog_status
    selected: dict[str, Any] = {}
    if catalog_status == "resolved":
        if len(compatible) == 1:
            selected = compatible[0]
        elif len(compatible) > 1:
            effective_status = "ambiguous"
    browse = selected.get("browse_classification") or {}
    asin = selected.get("asin")
    observed_at = (
        selected.get("pricing_observed_at") or pricing_observed_at
        or selected.get("catalog_observed_at") or catalog_observed_at
    )
    return {
        "schema_version": DIRECT_LOOKUP_SCHEMA_VERSION,
        "lookup_type": "direct_amazon_ean",
        "requested_ean": requested,
        "canonical_ean": canonical,
        "catalog_status": effective_status,
        "amazon_present": effective_status in {"resolved", "ambiguous"},
        "asin": asin,
        "title": selected.get("title"),
        "brand": selected.get("brand"),
        "image_url": selected.get("main_image"),
        "product_type": selected.get("product_type"),
        "browse_classification": browse,
        "category": browse.get("displayName") or selected.get("product_type"),
        "category_id": browse.get("classificationId"),
        "bsr_beauty": selected.get("bsr_beauty"),
        "bsr_rank": selected.get("bsr_rank") or selected.get("bsr_beauty"),
        "bsr_category": selected.get("bsr_category"),
        "bsr_category_label": selected.get("bsr_category_label"),
        "bsr_status": selected.get("bsr_status"),
        "bsr_observed_at": selected.get("bsr_observed_at"),
        "bsr_observed_days": selected.get("bsr_observed_days"),
        "lowest_new_price": selected.get("lowest_new_price"),
        "lowest_new_status": selected.get("lowest_new_status"),
        "lowest_new_observed_at": selected.get("lowest_new_observed_at"),
        "lowest_new_observed_date": selected.get("lowest_new_observed_date"),
        "lowest_new_source": selected.get("lowest_new_source"),
        "buy_box_price": selected.get("buy_box_price") or (
            selected.get("reference_price")
            if selected.get("price_source") == "buy_box" else None
        ),
        "reference_price": selected.get("reference_price"),
        "min_fba_price": selected.get("min_fba_price"),
        "min_fbm_price": selected.get("min_fbm_price"),
        "total_sellers": selected.get("total_sellers"),
        "fba_sellers": selected.get("fba_sellers"),
        "fbm_sellers": selected.get("fbm_sellers"),
        "amazon_product_url": f"https://www.amazon.it/dp/{asin}" if asin else None,
        "amazon_offers_url": (
            f"https://www.amazon.it/gp/offer-listing/{asin}" if asin else None
        ),
        "observed_at": observed_at,
        "catalog_observed_at": catalog_observed_at,
        "pricing_observed_at": selected.get("pricing_observed_at") or pricing_observed_at,
        "cache_status": cache_status,
        "identity_status": selected.get("identity_status") or catalog_status,
        "pricing_status": selected.get("pricing_status"),
        "listings": listings,
    }


class DirectAmazonLookup:
    """Resolve one identifier through point cache reads and Catalog/Pricing only."""

    def __init__(
        self, *, cache: Any,
        catalog_lookup: Callable[[list[str], str], dict[str, Any]],
        pricing_lookup: Callable[[list[str], str], dict[str, Any]],
        manager_lookup: Callable[[str], dict[str, Any] | None] | None = None,
        freshness_policy: AmazonFreshnessPolicy | None = None,
        now: Callable[[], datetime] | None = None,
    ):
        self.cache = cache
        self.catalog_lookup = catalog_lookup
        self.pricing_lookup = pricing_lookup
        self.manager_lookup = manager_lookup
        self.policy = freshness_policy or AmazonFreshnessPolicy.from_environment()
        self.now = now or (lambda: datetime.now(timezone.utc))

    def lookup(self, identifier: str) -> dict[str, Any]:
        requested, canonical = _canonical_identifier(identifier)
        now = self.now()
        if self.manager_lookup is not None:
            try:
                manager = self.manager_lookup(canonical)
            except Exception:
                manager = None
            if manager:
                return self._lookup_manager(requested, canonical, manager, now)
        cached = self.cache.get(canonical) if self.cache is not None else None
        status = str((cached or {}).get("catalog_status") or "")
        catalog_ttl = (
            self.policy.catalog_negative
            if status in NEGATIVE_CATALOG_STATUSES else self.policy.catalog_resolved
        )
        catalog_fresh = bool(
            cached and status != "catalog_incomplete"
            and _fresh(cached.get("catalog_observed_at"), catalog_ttl, now)
        )
        context_id = f"direct:{canonical}"

        if catalog_fresh:
            listings = [dict(row) for row in cached.get("amazon_listings") or []]
            for listing in listings:
                listing.update(_pricing_fields(listing))
            if status in NEGATIVE_CATALOG_STATUSES:
                return _display_result(
                    requested=requested, canonical=canonical, catalog_status=status,
                    listings=[], catalog_observed_at=cached.get("catalog_observed_at"),
                    cache_status="negative_cache_hit",
                )
            if status == "ambiguous" or len(_compatible_listings(listings)) != 1:
                return _display_result(
                    requested=requested, canonical=canonical,
                    catalog_status="ambiguous", listings=listings,
                    catalog_observed_at=cached.get("catalog_observed_at"),
                    pricing_observed_at=cached.get("pricing_observed_at"),
                    cache_status="catalog_cache_hit",
                )
            listing = _compatible_listings(listings)[0]
            pricing_stamp = listing.get("pricing_observed_at") or cached.get(
                "pricing_observed_at"
            )
            if _fresh(pricing_stamp, self.policy.pricing, now):
                return _display_result(
                    requested=requested, canonical=canonical,
                    catalog_status="resolved", listings=listings,
                    catalog_observed_at=cached.get("catalog_observed_at"),
                    pricing_observed_at=pricing_stamp, cache_status="full_cache_hit",
                )
            self._refresh_pricing(listing, context_id, now)
            return _display_result(
                requested=requested, canonical=canonical,
                catalog_status="resolved", listings=listings,
                catalog_observed_at=cached.get("catalog_observed_at"),
                pricing_observed_at=listing.get("pricing_observed_at"),
                cache_status="catalog_cache_hit_pricing_refreshed",
            )

        catalog_mapping = self.catalog_lookup([canonical], context_id)
        catalog = dict(catalog_mapping.get(canonical) or {
            "status": "not_found", "listings": [],
        })
        status = str(catalog.get("status") or "not_found")
        listings = [dict(row) for row in catalog.get("listings") or []]
        catalog_stamp = now.isoformat().replace("+00:00", "Z")
        for listing in listings:
            listing["catalog_observed_at"] = catalog_stamp
        if status == "resolved" and len(_compatible_listings(listings)) == 1:
            self._refresh_pricing(_compatible_listings(listings)[0], context_id, now)
        elif status == "resolved" and len(_compatible_listings(listings)) != 1:
            status = "ambiguous"
        return _display_result(
            requested=requested, canonical=canonical, catalog_status=status,
            listings=listings, catalog_observed_at=catalog_stamp,
            pricing_observed_at=max(
                (row.get("pricing_observed_at") for row in listings
                 if row.get("pricing_observed_at")), default=None,
            ),
            cache_status="cache_miss" if cached is None else "catalog_refreshed",
        )

    def _lookup_manager(self, requested, canonical, manager, now):
        listing = _manager_listing(manager)
        listing["identity_status"] = "canonical_manager"
        missing_catalog = any(
            not listing.get(field) for field in ("asin", "title", "brand", "main_image")
        )
        if missing_catalog:
            try:
                catalog = self.catalog_lookup([canonical], f"direct:{canonical}").get(canonical) or {}
                candidates = _compatible_listings([dict(row) for row in catalog.get("listings") or []])
                match = next(
                    (row for row in candidates if row.get("asin") == listing.get("asin")),
                    None,
                )
                if match:
                    for target, source in (
                        ("title", "title"), ("brand", "brand"),
                        ("main_image", "main_image"), ("product_type", "product_type"),
                    ):
                        listing[target] = listing.get(target) or match.get(source)
            except Exception:
                pass
        asin = str(listing.get("asin") or "").strip()
        if asin:
            try:
                pricing = dict(self.pricing_lookup([asin], f"direct:{canonical}").get(asin) or {"status": "missing"})
            except Exception:
                pricing = {"status": "error"}
            listing.update(_pricing_fields(pricing))
            if listing.get("pricing_status") == "success":
                listing["pricing_observed_at"] = now.isoformat().replace("+00:00", "Z")
        return _display_result(
            requested=requested, canonical=canonical, catalog_status="resolved",
            listings=[listing],
            catalog_observed_at=(manager.get("identity") or {}).get("updated_at"),
            pricing_observed_at=listing.get("pricing_observed_at"),
            cache_status="manager_canonical",
        )

    def _refresh_pricing(
        self, listing: dict[str, Any], context_id: str, now: datetime,
    ) -> None:
        asin = str(listing.get("asin") or "").strip()
        if not asin:
            return
        mapping = self.pricing_lookup([asin], context_id)
        pricing = dict(mapping.get(asin) or {"status": "missing"})
        listing.update(_pricing_fields(pricing))
        listing["pricing_observed_at"] = now.isoformat().replace("+00:00", "Z")


def run_direct_lookup(
    identifier: str, *, catalog_batch, pricing_batch, cache=None,
    manager_lookup=None,
    freshness_policy: AmazonFreshnessPolicy | None = None,
    now: Callable[[], datetime] | None = None,
) -> dict[str, Any]:
    """Compatibility entry point for the Streamlit page; no jobs are created."""
    if cache is None:
        from discovery_freshness import DiscoveryAmazonCache
        from discovery_incremental import DiscoveryIncrementalStore

        cache = DiscoveryAmazonCache(DiscoveryIncrementalStore())
    return DirectAmazonLookup(
        cache=cache, catalog_lookup=catalog_batch, pricing_lookup=pricing_batch,
        manager_lookup=manager_lookup,
        freshness_policy=freshness_policy, now=now,
    ).lookup(identifier)


__all__ = [
    "DIRECT_LOOKUP_SCHEMA_VERSION", "DirectAmazonLookup", "format_eur",
    "run_direct_lookup",
]
