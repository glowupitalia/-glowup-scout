"""Production incremental weekly adapters for Scout-owned supplier caches.

Only lightweight supplier-wide indexes are enumerated up front.  Expensive
detail calls are made by :class:`IncrementalWeeklyHandler` for NEW/CHANGED or
bounded reconciliation work.  Global Qogita enrichment remains outside this
module; the only Qogita step is the identity-only Korean Beauty membership.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from purchase_scenarios import normalize_purchase_scenario
from qogita_korean_beauty import (
    QogitaKoreanBeautyCollector, refresh_korean_beauty_membership,
)
from supplier_catalog import (
    SupplierCatalogGeneration, SupplierCatalogStore, canonical_gtin14,
    supplier_product_cache_key, supplier_promotion_gate,
)
from supplier_catalog_collectors import (
    ABW_COVERAGE, MANAGER_ROOT, QUDO_COVERAGE, UMMA_COVERAGE,
    AbwCatalogCollector, _load_manager_environment, _qudo_page_identifier_evidence,
)
from supplier_incremental import SupplierIncrementalStore
from supplier_weekly import IncrementalWeeklyHandler
from umma_discovery import normalize_umma_barcode, normalize_umma_candidates
from qudo_discovery import normalize_qudo_candidates


logger = logging.getLogger(__name__)


def _iso_now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class WeeklyRemoteError(RuntimeError):
    def __init__(self, cause):
        super().__init__(str(cause))
        self.code = getattr(cause, "code", type(cause).__name__)
        self.remote_status = getattr(cause, "remote_status", None)
        self.retryable = self.code in {"timeout", "network_error", "rate_limited", "remote_error"} or (
            isinstance(self.remote_status, int) and self.remote_status >= 500
        )


class _ResponseTelemetry:
    def __init__(self):
        self.rate_limits = 0
        self.server_errors = 0
        self.retry_signals = 0

    async def observe(self, response):
        status = int(response.status_code)
        if status == 429:
            self.rate_limits += 1
            self.retry_signals += 1
        elif status >= 500:
            self.server_errors += 1
            self.retry_signals += 1

    def snapshot(self):
        return self.retry_signals, self.rate_limits, self.server_errors

    def delta(self, before):
        current = self.snapshot()
        return tuple(current[index] - before[index] for index in range(3))


class _StableAsyncAdapter:
    """Keep an async client's complete lifecycle on one owned event loop."""

    def __init__(self):
        self._runner: asyncio.Runner | None = None

    def _run_async(self, coroutine):
        if self._runner is None:
            self._runner = asyncio.Runner()
        return self._runner.run(coroutine)

    def _close_async_client(self) -> None:
        runner = self._runner
        if runner is None:
            return
        close_error = None
        try:
            client = getattr(self, "_client", None)
            if client is not None:
                runner.run(client.close())
        except BaseException as exc:
            close_error = exc
        finally:
            self._client = None
            try:
                runner.close()
            except BaseException as exc:
                if close_error is None:
                    close_error = exc
                else:
                    logger.exception("weekly adapter runner cleanup also failed")
            self._runner = None
        if close_error is not None:
            raise close_error


def validate_umma_gap(current_gap: int, previous_gap: int) -> None:
    """Quarantine a materially worse enumeration while tolerating the known small gap."""
    allowed = max(50, int(previous_gap or 0) * 2)
    if int(current_gap or 0) > allowed:
        raise RuntimeError("UMMA enumeration gap increased anomalously")


def _brand(payload):
    for attribute in payload.get("attributes") or []:
        if str(attribute.get("name") or "").casefold() == "brand":
            return str(((attribute.get("terms") or [{}])[0]).get("name") or "")
    return ""


def _baseline_seed(store: SupplierCatalogStore, supplier: str,
                   current_products: list[dict[str, Any]]):
    """Bridge a pre-incremental active generation without triggering full detail."""
    active = store.latest_success(supplier)
    if not active:
        return [], {}
    active_products = {
        row["canonical_product_key"]: row for row in active["products"]
    }
    current_by_key = {row["canonical_product_key"]: row for row in current_products}
    previous_products = []
    for key, old in active_products.items():
        # Matching identities deliberately use the new light-index payload for
        # the migration baseline. Subsequent weekly runs compare real signals.
        row = dict(current_by_key.get(key) or old)
        if key in current_by_key:
            for field in ("canonical_ean", "canonical_gtin", "identifier_type",
                          "raw_identifiers"):
                if not row.get(field) and old.get(field):
                    row[field] = old[field]
            row["identifier_valid"] = bool(
                row.get("canonical_gtin") or canonical_gtin14(row.get("canonical_ean"))
            )
        previous_products.append(row)
    scenarios = {}
    for scenario in active["scenarios"]:
        key = scenario.get("supplier_catalog_product_key")
        if key:
            payload = dict(scenario)
            payload["canonical_product_key"] = key
            payload.setdefault(
                "enriched_at",
                payload.get("snapshot_at")
                or active.get("scenario_enrichment_observed_at")
                or active.get("completed_at"),
            )
            scenarios.setdefault(key, []).append(payload)
    return previous_products, scenarios


def _catalog_generation(supplier, run_id, enumeration, incremental_store,
                        catalog_store=None):
    products, scenarios = incremental_store.generation_records(run_id)
    coverage = {"abw": ABW_COVERAGE, "umma": UMMA_COVERAGE,
                "qudo": QUDO_COVERAGE}[supplier]
    diagnostics = dict(enumeration.get("diagnostics") or {})
    for scenario in scenarios:
        scenario.pop("supplier_catalog_product_key", None)
        normalize_purchase_scenario(scenario)
    generation = SupplierCatalogGeneration(
        supplier=supplier,
        coverage_type=coverage["type"], coverage_description=coverage["description"],
        coverage_complete=coverage["complete"], products=tuple(products),
        scenarios=tuple({
            "scenario_id": row["scenario_id"],
            "canonical_product_key": row["canonical_product_key"],
            "canonical_ean": row.get("canonical_ean"),
            "raw_identifier": row.get("supplier_barcode_raw") or row.get("canonical_ean"),
            "raw_identifier_type": row.get("identifier_type"),
            "supplier_product_id": row.get("supplier_product_id"),
            "supplier_offer_id": row.get("supplier_offer_id"),
            "supplier_sku": row.get("supplier_sku"),
            "scenario_type": row.get("scenario_type"), "scenario_label": row.get("scenario_label"),
            "price": row.get("cost_net_unit_eur"), "currency": "EUR",
            "stock": row.get("stock_quantity"),
            "minimum_quantity": row.get("minimum_product_quantity"),
            "maximum_quantity": row.get("maximum_product_quantity"),
            "selling_unit": row.get("selling_unit"), "account_mov": row.get("account_mov"),
            "account_mov_currency": row.get("account_mov_currency"),
            "warehouse": row.get("warehouse"), "shipping_mode": row.get("shipping_mode"),
            "availability_status": row.get("availability_status"),
            "lead_time": row.get("lead_time"), "payload": row,
        } for row in scenarios),
        page_count=int(enumeration.get("pages") or 0),
        request_count=int(enumeration.get("requests") or 0),
        retry_count=int(enumeration.get("retry") or 0),
        rate_limit_count=int(enumeration.get("rate_limits") or 0),
        server_error_count=int(enumeration.get("server_errors") or 0),
        source_type=diagnostics.get("source_type"),
        source_count=diagnostics.get("source_count"),
        enumerated_count=len(products), unique_count=len(products),
        completeness_status=coverage["type"],
        completeness_reason=coverage["description"],
        product_catalog_coverage_type=coverage["type"],
        product_catalog_coverage_complete=coverage["complete"],
        scenario_enrichment_status="partial", scenario_enrichment_count=len(scenarios),
        diagnostics=diagnostics,
    )
    store = catalog_store or SupplierCatalogStore()
    store.start_run(
        supplier, run_id=run_id, coverage_type=coverage["type"],
        coverage_description=coverage["description"],
        coverage_complete=coverage["complete"], sampled=False,
    )
    # Supplier-specific structural proofs remain enforced by the canonical
    # cache gate embodied in the generation metadata and atomic store publish.
    gate = supplier_promotion_gate(supplier, generation)
    store.publish(run_id, generation, elapsed_seconds=0, promote=gate["passed"])
    if not gate["passed"]:
        raise RuntimeError("Supplier promotion gate failed: " + ",".join(gate["reasons"]))
    return {"run_id": run_id, "promotion_result": "promoted"}


class UmmaIncrementalAdapter(_StableAsyncAdapter):
    supplier = "umma"

    def __init__(self, *, catalog_store=None, manager_root=MANAGER_ROOT):
        super().__init__()
        self.store = catalog_store or SupplierCatalogStore()
        self.manager_root = Path(manager_root)
        self._client = None
        self._fx = None
        self._telemetry = _ResponseTelemetry()

    def previous_run_id(self):
        value = self.store.active_generation_metadata(self.supplier)
        return value.get("run_id") if value else None

    def enumerate_catalog(self, *, source=None, policy=None):
        return self._run_async(self._enumerate(policy))

    async def _enumerate(self, policy):
        _load_manager_environment(self.manager_root)
        from purchase_prices.fx import EcbFxClient, FxError
        from purchase_prices.umma import UmmaClient
        self._client = UmmaClient()
        self._client.client.event_hooks.setdefault("response", []).append(self._telemetry.observe)
        fx_client = EcbFxClient()
        products, seen, rows_received = [], set(), 0
        total = None; skip = 0; pages = 0
        try:
            while total is None or skip < total:
                payload = await self._client._get("/search/product", params={
                    "skip": str(skip), "take": "100", "orderById": "DESC",
                })
                pages += 1
                current_total = int(payload.get("totalCount") or 0)
                if total is None: total = current_total
                elif total != current_total: raise RuntimeError("UMMA totalCount changed during pagination")
                items = payload.get("items") or []
                rows_received += len(items)
                for item in items:
                    product_id = str(item.get("id") or "")
                    options = []
                    for mapper in item.get("mapperSaleProducts") or []:
                        option = mapper.get("productOption") if isinstance(mapper, dict) else None
                        if isinstance(option, dict): options.append((mapper, option))
                    if not options:
                        options = [({}, {})]
                    for mapper, option in options:
                        option_id = str(option.get("id") or "") or None
                        sku = str(option.get("sku") or option.get("erpSku") or "") or None
                        barcode = str(option.get("barcode") or "").strip()
                        try:
                            canonical_ean, identifier_type, _, _ = (
                                normalize_umma_barcode(barcode, "standard")
                                if barcode else (None, None, "", None)
                            )
                        except (TypeError, ValueError):
                            canonical_ean, identifier_type = None, None
                        key = supplier_product_cache_key(
                            "umma", product_id, supplier_option_id=option_id,
                            supplier_sku=sku, fallback_identifier=canonical_ean,
                        )
                        if key in seen: continue
                        seen.add(key)
                        products.append({
                            "canonical_product_key": key,
                            "canonical_ean": canonical_ean,
                            "canonical_gtin": canonical_gtin14(canonical_ean),
                            "identifier_type": identifier_type,
                            "raw_identifiers": ([{"value": barcode, "type": "UMMA_BARCODE"}] if barcode else []),
                            "identifier_valid": bool(canonical_ean),
                            "supplier_product_id": product_id, "supplier_option_id": option_id,
                            "supplier_sku": sku, "brand": item.get("brandName") or item.get("brand"),
                            "title": option.get("englishName") or item.get("englishName"),
                            "product_id": product_id, "option_id": option_id,
                            "raw_barcode": barcode, "is_display": item.get("isDisplay"),
                            "metadata": {"mapper_sale_product_id": mapper.get("id")},
                        })
                skip += 100
                if skip < total: await asyncio.sleep(policy.min_pacing_seconds)
            try: self._fx = await fx_client.latest_usd_to_eur()
            except FxError: self._fx = None
        finally:
            await fx_client.close()
        previous_products, previous_scenarios = _baseline_seed(self.store, "umma", products)
        gap = max(0, int(total or 0) - len({row["supplier_product_id"] for row in products}))
        previous_meta = self.store.active_generation_metadata("umma") or {}
        old_gap = int((previous_meta.get("diagnostics") or {}).get("enumeration_gap") or 0)
        validate_umma_gap(gap, old_gap)
        retry, rate_limits, server_errors = self._telemetry.snapshot()
        return {"products": products, "previous_products": previous_products,
                "previous_scenarios_by_product": previous_scenarios,
                "pages": pages, "requests": self._client.request_count,
                "retry": retry, "rate_limits": rate_limits, "server_errors": server_errors,
                "diagnostics": {"source_type": "global_search_index", "source_count": total,
                    "search_total_count": total, "enumerated_count": rows_received,
                    "unique_product_ids": len({row['supplier_product_id'] for row in products}),
                    "enumeration_gap": gap}}

    def enrich_product(self, *, canonical_product_key, product, policy):
        return self._run_async(self._enrich(product, policy))

    async def _enrich(self, product, policy):
        from purchase_prices.umma import UmmaError, normalize_offer, parse_product_modes
        before = self._client.request_count
        before_telemetry = self._telemetry.snapshot()
        try:
            await asyncio.sleep(policy.min_pacing_seconds)
            detail = await self._client._get(f"/product/{product['supplier_product_id']}")
            barcode = str(product.get("raw_barcode") or "")
            offers = parse_product_modes(detail, expected_gtin=barcode)
        except UmmaError as exc:
            if exc.code in {"not_found", "no_offers"}:
                retry, rate_limits, server_errors = self._telemetry.delta(before_telemetry)
                return {"scenarios": [], "requests": self._client.request_count - before,
                        "retry": retry, "rate_limits": rate_limits,
                        "server_errors": server_errors}
            raise WeeklyRemoteError(exc) from exc
        rows = []
        observed = _iso_now()
        for normalized in (normalize_offer(offer, self._fx) for offer in offers):
            offer = normalized.source
            rows.append({"run_id": "weekly", "gtin": offer.gtin,
                "supplier_product_id": offer.supplier_product_id,
                "mapper_sale_product_id": offer.mapper_sale_product_id,
                "product_option_id": offer.product_option_id, "supplier_sku": offer.supplier_sku,
                "product_name": offer.product_name, "sales_mode": offer.sales_mode,
                "observed_at": observed, "original_unit_price": str(offer.original_unit_price),
                "original_currency": offer.original_currency,
                "net_unit_price_eur": str(normalized.net_unit_price_eur) if normalized.net_unit_price_eur is not None else None,
                "vat_rate_percent": "22", "vat_amount_eur": str(normalized.vat_amount_eur) if normalized.vat_amount_eur is not None else None,
                "gross_unit_price_eur": str(normalized.gross_unit_price_eur) if normalized.gross_unit_price_eur is not None else None,
                "available_quantity": offer.available_quantity, "availability_status": offer.availability_status,
                "minimum_product_quantity": offer.minimum_product_quantity, "selling_unit": offer.selling_unit,
                "maximum_quantity": offer.maximum_quantity, "lead_time": offer.lead_time,
                "minimum_order_value": "700", "minimum_order_currency": "USD"})
        candidates, _ = normalize_umma_candidates(rows, now=datetime.now(timezone.utc))
        scenarios = [scenario for candidate in candidates for scenario in candidate["scenarios"]]
        retry, rate_limits, server_errors = self._telemetry.delta(before_telemetry)
        return {"scenarios": scenarios, "requests": self._client.request_count - before,
                "retry": retry, "rate_limits": rate_limits,
                "server_errors": server_errors}

    def close(self):
        self._close_async_client()


class AbwIncrementalAdapter:
    supplier = "abw"

    def __init__(self, *, catalog_store=None):
        self.store = catalog_store or SupplierCatalogStore()
        self._scenarios = {}

    def previous_run_id(self):
        value = self.store.active_generation_metadata(self.supplier)
        return value.get("run_id") if value else None

    def enumerate_catalog(self, *, source=None, policy=None):
        path = Path(source).expanduser().resolve()
        if not path.is_file():
            raise FileNotFoundError(path)
        generation = AbwCatalogCollector(source_file=path)(
            run_id="weekly-abw-source", limit=None, dry_run=False,
        )
        grouped = {}
        for row in generation.scenarios:
            payload = dict(row.get("payload") or row)
            key = row.get("canonical_product_key")
            payload["canonical_product_key"] = key
            grouped.setdefault(key, []).append(payload)
        products = []
        for source_product in generation.products:
            row = dict(source_product)
            key = row["canonical_product_key"]
            signals = [{field: scenario.get(field) for field in (
                "scenario_id", "scenario_type", "cost_net_unit_eur",
                "minimum_product_quantity", "maximum_product_quantity",
                "selling_unit", "availability_status", "warehouse",
            )} for scenario in sorted(grouped.get(key, []), key=lambda item: item["scenario_id"])]
            row.update({
                "identifier_valid": bool(row.get("canonical_gtin") or canonical_gtin14(row.get("canonical_ean"))),
                "product_id": row.get("supplier_product_id"),
                "option_id": row.get("supplier_option_id"),
                "catalog_number": (row.get("metadata") or {}).get("catalog_no"),
                "upc": row.get("canonical_ean"),
                "warehouse": "unspecified",
                "availability": (row.get("metadata") or {}).get("availability_text"),
                "commercial_signal": hashlib.sha256(json.dumps(
                    signals, sort_keys=True, default=str,
                ).encode()).hexdigest(),
            })
            products.append(row)
        self._scenarios = grouped
        previous_products, previous_scenarios = _baseline_seed(self.store, "abw", products)
        diagnostics = dict(generation.diagnostics or {})
        return {"products": products, "previous_products": previous_products,
                "previous_scenarios_by_product": previous_scenarios,
                "pages": 1, "requests": 0, "diagnostics": diagnostics}

    def enrich_product(self, *, canonical_product_key, product, policy):
        # Official XLSX rows are the authoritative, zero-request enrichment for
        # NEW/CHANGED ABW products. No website detail sweep is hidden here.
        return {"scenarios": self._scenarios.get(canonical_product_key, []), "requests": 0}

    def close(self):
        return None


class QudoIncrementalAdapter(_StableAsyncAdapter):
    supplier = "qudo"

    def __init__(self, *, catalog_store=None, manager_root=MANAGER_ROOT):
        super().__init__()
        self.store = catalog_store or SupplierCatalogStore(); self.manager_root = Path(manager_root)
        self._client = None
        self._telemetry = _ResponseTelemetry()

    def previous_run_id(self):
        value = self.store.active_generation_metadata(self.supplier)
        return value.get("run_id") if value else None

    def enumerate_catalog(self, *, source=None, policy=None):
        return self._run_async(self._enumerate(policy))

    async def _enumerate(self, policy):
        _load_manager_environment(self.manager_root)
        from purchase_prices.qudo import QudoClient, select_qudo_variation
        self._client = QudoClient()
        self._client.client.event_hooks.setdefault("response", []).append(self._telemetry.observe)
        products=[]; seen=set(); page=1; pages=None; total=None
        while pages is None or page <= pages:
            response = await self._client._get("/wp-json/wc/store/v1/products", params={"per_page":"100","page":str(page)})
            payload=response.json(); current_total=int(response.headers.get("X-WP-Total") or 0); current_pages=int(response.headers.get("X-WP-TotalPages") or 0)
            if pages is None: pages,total=current_pages,current_total
            elif (pages,total)!=(current_pages,current_total): raise RuntimeError("Qudo catalog count changed during pagination")
            for parent in payload:
                try: variation_id=select_qudo_variation(parent)
                except Exception: continue
                key=supplier_product_cache_key("qudo", parent.get("id"), supplier_option_id=variation_id, supplier_sku=parent.get("sku"))
                if key in seen: continue
                seen.add(key)
                variation = next((row for row in parent.get("variations") or [] if str(row.get("id"))==str(variation_id)), {})
                products.append({"canonical_product_key":key,"canonical_ean":None,"canonical_gtin":None,
                    "identifier_type":None,"raw_identifiers":[],"identifier_valid":False,
                    "supplier_product_id":str(parent.get("id") or ""),"supplier_option_id":str(variation_id),
                    "supplier_sku":str(parent.get("sku") or "") or None,"brand":_brand(parent),"title":parent.get("name"),
                    "product_id":str(parent.get("id") or ""),"variation_id":str(variation_id),
                    "index_name":parent.get("name"),"index_permalink":parent.get("permalink"),
                    "index_purchasable":parent.get("is_purchasable"),"index_stock_signal":parent.get("is_in_stock"),
                    "product_url":parent.get("permalink"),
                    "metadata":{"product_url":parent.get("permalink"),"parent_contract":{"id":parent.get("id"),"variations":[variation]}}})
            page += 1
            if page <= pages: await asyncio.sleep(policy.min_pacing_seconds)
        previous_products, previous_scenarios = _baseline_seed(self.store,"qudo",products)
        # Carry proven identifiers from the active baseline into the new light index.
        old={row["canonical_product_key"]:row for row in previous_products}
        for row in products:
            prior=old.get(row["canonical_product_key"])
            if prior:
                for field in ("canonical_ean","canonical_gtin","identifier_type","raw_identifiers"):
                    if prior.get(field): row[field]=prior[field]
                row["identifier_valid"]=bool(row.get("canonical_gtin") or canonical_gtin14(row.get("canonical_ean")))
        retry, rate_limits, server_errors = self._telemetry.snapshot()
        return {"products":products,"previous_products":previous_products,
                "previous_scenarios_by_product":previous_scenarios,"pages":pages,
                "requests":self._client.request_count,"retry":retry,
                "rate_limits":rate_limits,"server_errors":server_errors,
                "diagnostics":{"source_type":"woocommerce_store_api_light_index","source_count":total,
                    "global_catalog_total":total,"qudo_offer_products":len(products),"enumerated_count":len(products)}}

    def enrich_product(self, *, canonical_product_key, product, policy):
        return self._run_async(self._enrich(product, policy))

    async def _enrich(self, product, policy):
        from purchase_prices.qudo import parse_product_page, parse_store_offer
        url=product.get("product_url") or product.get("metadata",{}).get("product_url")
        before = self._client.request_count
        before_telemetry = self._telemetry.snapshot()
        try:
            await asyncio.sleep(policy.min_pacing_seconds); page_response=await self._client._get(url)
            evidence=_qudo_page_identifier_evidence(page_response.text)
            if len(evidence["selected_gtins"]) != 1: raise ValueError("Qudo identifier unresolved")
            gtin=evidence["selected_gtins"][0]
            page=parse_product_page(page_response.text,product_url=url,expected_gtin=gtin)
            await asyncio.sleep(policy.min_pacing_seconds)
            variation_response=await self._client._get(f"/wp-json/wc/store/v1/products/{product['variation_id']}")
            parent=product.get("metadata",{}).get("parent_contract") or {"id":product["product_id"],"variations":[]}
            offer=parse_store_offer(page,parent,variation_response.json())
        except Exception as exc:
            if getattr(exc, "code", None):
                raise WeeklyRemoteError(exc) from exc
            raise
        observed=_iso_now()
        row={"run_id":"weekly","seller_sku":offer.supplier_sku,"gtin":offer.gtin,"supplier":"QUDO",
            "supplier_product_id":offer.supplier_product_id,"supplier_offer_id":offer.supplier_offer_id,
            "supplier_sku":offer.supplier_sku,"product_name":offer.product_name,"brand":product.get("brand"),
            "observed_at":observed,"currency":offer.currency,"unit_price":str(offer.net_unit_price),
            "available_quantity":offer.available_quantity,"availability_status":offer.availability_status,
            "minimum_product_quantity":offer.minimum_product_quantity,"selling_unit":offer.selling_unit,
            "minimum_order_value":str(offer.minimum_order_value),"minimum_order_currency":offer.minimum_order_currency,
            "product_url":offer.product_url,"identifier_source":evidence["identifier_source"]}
        candidates,_=normalize_qudo_candidates([row],now=datetime.now(timezone.utc))
        scenarios=[scenario for candidate in candidates for scenario in candidate["scenarios"]]
        retry, rate_limits, server_errors = self._telemetry.delta(before_telemetry)
        return {"scenarios":scenarios,"requests":self._client.request_count-before,
            "retry":retry,"rate_limits":rate_limits,"server_errors":server_errors,"product_updates":{
            "canonical_ean":gtin,"canonical_gtin":canonical_gtin14(gtin),
            "identifier_type":"EAN" if len(gtin)==13 else "GTIN",
            "raw_identifiers":[{"value":v,"type":"HTML_MARKER"} for v in evidence["marker_gtins"]]+[
                {"value":v,"type":"JSON_LD"} for v in evidence["json_ld_gtins"] if v not in evidence["marker_gtins"]],
            "identifier_valid":True}}

    def close(self):
        self._close_async_client()


def _make_handler(adapter):
    handler = IncrementalWeeklyHandler(
        adapter.supplier, enumerate_catalog=adapter.enumerate_catalog,
        enrich_product=adapter.enrich_product,
        publish_generation=lambda **kwargs: _catalog_generation(
            **kwargs, catalog_store=adapter.store,
        ),
        previous_run_id=adapter.previous_run_id,
    )
    def run(**kwargs):
        try:
            result = handler(**kwargs)
        except BaseException:
            try:
                adapter.close()
            except BaseException:
                logger.exception(
                    "weekly supplier adapter cleanup failed; preserving primary error",
                    extra={"supplier": adapter.supplier},
                )
            raise
        else:
            adapter.close()
            return result
    run.incremental_handler = handler
    run.adapter = adapter
    return run


def build_weekly_handlers():
    abw=AbwIncrementalAdapter(); umma=UmmaIncrementalAdapter(); qudo=QudoIncrementalAdapter()
    def korean_beauty(*, policy, **_kwargs):
        collector = QogitaKoreanBeautyCollector(
            pacing_seconds=policy.min_pacing_seconds,
            max_attempts=policy.max_retries + 1,
        )
        try:
            report = refresh_korean_beauty_membership(
                collector=collector, persist=True, activate=True,
            )
        finally:
            collector.close()
        diff = report["membership_diff"]
        curated = report["curated"]
        if not report["membership_activation"]:
            return {
                "status": "failed",
                "baseline_after": report["previous_membership_version_id"],
                "promotion_result": "baseline_preserved",
                "failures": 1,
                "requests": curated.get("pages_requested", 0),
                "retry": curated.get("http_retry_count", 0),
                "rate_limits": curated.get("http_status_counts", {}).get("429", 0),
                "server_errors": sum(
                    count for status, count in curated.get("http_status_counts", {}).items()
                    if int(status) >= 500
                ),
                "error_code": "membership_validation_failed",
                "error_message": ", ".join(report["validation_errors"]),
                "diagnostics": report,
            }
        return {
            "status": "success",
            "baseline_after": report["active_membership"]["membership_version_id"],
            "promotion_result": "membership_activated",
            "new": diff["gtin_added_count"],
            "changed": diff["fid_changed_count"],
            "unchanged": diff["gtin_unchanged_count"],
            "removed": diff["gtin_removed_count"],
            "requests": curated.get("pages_requested", 0),
            "retry": curated.get("http_retry_count", 0),
            "rate_limits": curated.get("http_status_counts", {}).get("429", 0),
            "server_errors": sum(
                count for status, count in curated.get("http_status_counts", {}).items()
                if int(status) >= 500
            ),
            "diagnostics": report,
        }
    return {
        "abw": _make_handler(abw), "umma": _make_handler(umma),
        "qudo": _make_handler(qudo),
        "qogita_korean_beauty": korean_beauty,
    }
