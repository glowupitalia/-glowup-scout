"""Generation-based supplier-first catalog cache for Scout.

The cache is owned by Scout and never derives its universe from Manager's
tracked products.  A generation becomes visible only after a complete atomic
publish.  Sample runs are intentionally never promoted.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Iterable
from uuid import uuid4

from purchase_scenarios import normalize_purchase_scenario


SUPPORTED_SUPPLIERS = ("qogita", "umma", "abw", "qudo")
DEFAULT_DATABASE_PATH = Path(__file__).resolve().parent / "data" / "supplier_catalog.sqlite3"
DEFAULT_LOCK_DIRECTORY = Path(os.environ.get("SCOUT_SUPPLIER_LOCK_DIR", "/tmp"))
DEFAULT_RETENTION_GENERATIONS = 14
FULL_COMPLETENESS_STATUSES = {"full_account_catalog", "full_relevant_catalog"}


class SupplierCatalogError(RuntimeError):
    pass


class SupplierCatalogBusy(SupplierCatalogError):
    pass


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _json_default(value):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (set, tuple)):
        return list(value)
    raise TypeError(f"Unsupported JSON value: {type(value).__name__}")


def json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=_json_default)


def canonical_gtin14(value: Any) -> str | None:
    """Return the GS1 comparison form while preserving raw identifiers elsewhere."""
    text = str(value or "").strip()
    if not re.fullmatch(r"(?:\d{8}|\d{12}|\d{13}|\d{14})", text):
        return None
    digits = [int(character) for character in text]
    expected = (10 - sum(
        digit * (3 if (len(digits) - index) % 2 == 0 else 1)
        for index, digit in enumerate(digits[:-1])
    ) % 10) % 10
    return text.zfill(14) if digits[-1] == expected else None


_VOLATILE_COMPARISON_FIELDS = {
    "run_id", "snapshot_id", "snapshot_at", "observed_at", "freshness_status",
    "supplier_catalog_run_id", "supplier_catalog_completed_at",
    "supplier_catalog_product_key",
}


def _stable_record_payload(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _stable_record_payload(item)
            for key, item in value.items()
            if key not in _VOLATILE_COMPARISON_FIELDS and item is not None
        }
    if isinstance(value, (list, tuple)):
        return [_stable_record_payload(item) for item in value]
    return value


def supplier_generation_delta(previous: dict[str, Any] | None,
                              current: SupplierCatalogGeneration) -> dict[str, Any]:
    """Compare immutable identities without treating snapshot timestamps as changes."""
    previous = previous or {}

    product_fields = (
        "canonical_product_key", "canonical_ean", "canonical_gtin", "identifier_type", "raw_identifiers",
        "supplier_product_id", "supplier_option_id", "supplier_sku", "brand", "title",
        "size_value", "size_unit", "pack_count", "metadata",
    )

    def indexed_products(rows):
        return {
            str(row.get("canonical_product_key") or ""): json_dumps(_stable_record_payload({
                field: row.get(field) for field in product_fields
            }))
            for row in rows or () if str(row.get("canonical_product_key") or "")
        }

    def indexed_scenarios(rows):
        indexed = {}
        for row in rows or ():
            payload = row.get("payload") or row
            identity = str(payload.get("scenario_id") or row.get("scenario_id") or "")
            if identity:
                indexed[identity] = json_dumps(_stable_record_payload(payload))
        return indexed

    before_products = indexed_products(previous.get("products"))
    after_products = indexed_products(current.products)
    before_scenarios = indexed_scenarios(previous.get("scenarios"))
    after_scenarios = indexed_scenarios(current.scenarios)

    def diff(before, after):
        new = sorted(after.keys() - before.keys())
        removed = sorted(before.keys() - after.keys())
        changed = sorted(key for key in before.keys() & after.keys() if before[key] != after[key])
        return {"new": new, "changed": changed, "removed": removed,
                "unchanged_count": len(before.keys() & after.keys()) - len(changed)}

    product_delta = diff(before_products, after_products)
    scenario_delta = diff(before_scenarios, after_scenarios)
    return {
        "products": product_delta,
        "scenarios": scenario_delta,
        "counts": {
            "products_new": len(product_delta["new"]),
            "products_changed": len(product_delta["changed"]),
            "products_removed": len(product_delta["removed"]),
            "scenarios_new": len(scenario_delta["new"]),
            "scenarios_changed": len(scenario_delta["changed"]),
            "scenarios_removed": len(scenario_delta["removed"]),
        },
    }


def supplier_product_cache_key(
    supplier: str, supplier_product_id: Any, *, supplier_option_id: Any = None,
    supplier_sku: Any = None, fallback_identifier: Any = None,
) -> str:
    """Identify a supplier catalog entity without collapsing it by EAN."""
    supplier = _validate_supplier(supplier)
    sku = str(supplier_sku or "").strip()
    if supplier == "qudo" and supplier_product_id and supplier_option_id:
        # Qudo can expose a page SKU and a different offer/display SKU for the
        # same authoritative product + variation pair. Preserve both raw SKU
        # values in their records, but do not let that mutable presentation
        # field split one supplier product into multiple cache identities.
        sku = ""
    parts = (
        supplier, str(supplier_product_id or "").strip(),
        str(supplier_option_id or "").strip(), sku,
        str(fallback_identifier or "").strip(),
    )
    if not any(parts[1:]):
        raise SupplierCatalogError("Supplier product identity is missing")
    digest = hashlib.sha256("|".join(parts).casefold().encode("utf-8")).hexdigest()[:24]
    return f"supplier_product_{digest}"


def _scenario_supplier_option_id(scenario: dict[str, Any]) -> Any:
    supplier = str(scenario.get("supplier") or "").casefold()
    if supplier == "umma":
        return scenario.get("variant_id") or scenario.get("supplier_offer_id")
    return scenario.get("supplier_offer_id") or scenario.get("variant_id")


@dataclass(frozen=True)
class SupplierCatalogGeneration:
    supplier: str
    coverage_type: str
    coverage_description: str
    coverage_complete: bool
    products: tuple[dict[str, Any], ...]
    scenarios: tuple[dict[str, Any], ...]
    page_count: int = 0
    request_count: int = 0
    retry_count: int = 0
    rate_limit_count: int = 0
    server_error_count: int = 0
    source_type: str | None = None
    source_count: int | None = None
    enumerated_count: int | None = None
    unique_count: int | None = None
    completeness_status: str = "partial_catalog"
    completeness_reason: str = "Complete enumeration has not been proven"
    export_generated_at: str | None = None
    upstream_catalog_version: str | None = None
    product_catalog_coverage_type: str | None = None
    product_catalog_coverage_complete: bool | None = None
    scenario_enrichment_status: str | None = None
    scenario_enrichment_count: int | None = None
    scenario_enrichment_observed_at: str | None = None
    diagnostics: dict[str, Any] = field(default_factory=dict)


PRODUCT_CATALOG_COVERAGE_TYPES = {
    "full_account_catalog", "full_relevant_catalog", "filtered_catalog",
    "partial_catalog", "search_discovered_catalog", "seller_catalog",
}
SCENARIO_ENRICHMENT_STATUSES = {"none", "partial", "full", "stale"}


def _validate_supplier(supplier: str) -> str:
    clean = str(supplier or "").strip().casefold()
    if clean not in SUPPORTED_SUPPLIERS:
        raise ValueError(f"Unsupported supplier: {supplier}")
    return clean


def _connect(path: str | Path) -> sqlite3.Connection:
    absolute = Path(path).expanduser().resolve()
    absolute.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(absolute)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    # Weekly staging/promotion and interactive Discovery/direct lookup may run
    # concurrently. WAL keeps readers on the active snapshot while a new
    # generation is assembled and atomically activated.
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.execute("PRAGMA busy_timeout=60000")
    return connection


SCHEMA = """
CREATE TABLE IF NOT EXISTS supplier_catalog_runs (
    run_id TEXT PRIMARY KEY,
    supplier TEXT NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    status TEXT NOT NULL,
    product_count INTEGER NOT NULL DEFAULT 0,
    scenario_count INTEGER NOT NULL DEFAULT 0,
    page_count INTEGER NOT NULL DEFAULT 0,
    request_count INTEGER NOT NULL DEFAULT 0,
    retry_count INTEGER NOT NULL DEFAULT 0,
    rate_limit_count INTEGER NOT NULL DEFAULT 0,
    server_error_count INTEGER NOT NULL DEFAULT 0,
    elapsed_seconds REAL,
    coverage_type TEXT NOT NULL,
    coverage_description TEXT NOT NULL,
    coverage_complete INTEGER NOT NULL DEFAULT 0,
    source_type TEXT,
    source_count INTEGER,
    enumerated_count INTEGER,
    unique_count INTEGER,
    completeness_status TEXT NOT NULL DEFAULT 'partial_catalog',
    completeness_reason TEXT,
    export_generated_at TEXT,
    upstream_catalog_version TEXT,
    product_catalog_coverage_type TEXT NOT NULL DEFAULT 'partial_catalog',
    product_catalog_coverage_complete INTEGER NOT NULL DEFAULT 0,
    scenario_enrichment_status TEXT NOT NULL DEFAULT 'none',
    scenario_enrichment_count INTEGER NOT NULL DEFAULT 0,
    scenario_enrichment_observed_at TEXT,
    sampled INTEGER NOT NULL DEFAULT 0,
    error_code TEXT,
    error_message TEXT,
    diagnostics_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_supplier_catalog_runs_supplier_started
ON supplier_catalog_runs (supplier, started_at DESC);

CREATE TABLE IF NOT EXISTS supplier_catalog_products (
    run_id TEXT NOT NULL,
    supplier TEXT NOT NULL,
    canonical_product_key TEXT NOT NULL,
    canonical_ean TEXT,
    canonical_gtin TEXT,
    identifier_type TEXT,
    raw_identifiers_json TEXT NOT NULL,
    supplier_product_id TEXT,
    supplier_option_id TEXT,
    supplier_sku TEXT,
    brand TEXT,
    title TEXT,
    size_value TEXT,
    size_unit TEXT,
    pack_count INTEGER,
    catalog_fingerprint TEXT,
    variant_fid TEXT,
    variant_fid_source TEXT,
    product_url TEXT,
    enrichment_status TEXT NOT NULL DEFAULT 'none',
    enrichment_error_code TEXT,
    enrichment_error_message TEXT,
    offer_tier_observed_at TEXT,
    catalog_delta_status TEXT,
    metadata_json TEXT NOT NULL,
    PRIMARY KEY (run_id, canonical_product_key),
    FOREIGN KEY (run_id) REFERENCES supplier_catalog_runs(run_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_supplier_catalog_products_ean
ON supplier_catalog_products (supplier, canonical_ean);
CREATE INDEX IF NOT EXISTS idx_supplier_catalog_products_gtin
ON supplier_catalog_products (supplier, canonical_gtin);
CREATE INDEX IF NOT EXISTS idx_supplier_catalog_products_run_ean
ON supplier_catalog_products (run_id, canonical_ean);
CREATE INDEX IF NOT EXISTS idx_supplier_catalog_products_run_gtin
ON supplier_catalog_products (run_id, canonical_gtin);
CREATE INDEX IF NOT EXISTS idx_supplier_catalog_products_fingerprint
ON supplier_catalog_products (run_id, catalog_fingerprint);

CREATE TABLE IF NOT EXISTS supplier_catalog_scenarios (
    run_id TEXT NOT NULL,
    supplier TEXT NOT NULL,
    scenario_id TEXT NOT NULL,
    canonical_product_key TEXT NOT NULL,
    canonical_ean TEXT,
    raw_identifier TEXT,
    raw_identifier_type TEXT,
    supplier_product_id TEXT,
    supplier_offer_id TEXT,
    supplier_sku TEXT,
    scenario_type TEXT NOT NULL,
    scenario_label TEXT,
    price TEXT,
    currency TEXT,
    stock INTEGER,
    minimum_quantity INTEGER,
    maximum_quantity INTEGER,
    selling_unit INTEGER,
    account_mov TEXT,
    account_mov_currency TEXT,
    warehouse TEXT,
    shipping_mode TEXT,
    availability_status TEXT,
    lead_time TEXT,
    payload_json TEXT NOT NULL,
    PRIMARY KEY (run_id, scenario_id),
    FOREIGN KEY (run_id) REFERENCES supplier_catalog_runs(run_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_supplier_catalog_scenarios_ean
ON supplier_catalog_scenarios (supplier, canonical_ean);
CREATE INDEX IF NOT EXISTS idx_supplier_catalog_scenarios_product
ON supplier_catalog_scenarios (run_id, canonical_product_key);
CREATE INDEX IF NOT EXISTS idx_supplier_catalog_scenarios_product_order
ON supplier_catalog_scenarios (run_id, canonical_product_key, scenario_id);

CREATE TABLE IF NOT EXISTS supplier_catalog_active_generations (
    supplier TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES supplier_catalog_runs(run_id)
);
"""


class SupplierCatalogStore:
    def __init__(self, path: str | Path = DEFAULT_DATABASE_PATH):
        self.path = Path(path).expanduser().resolve()

    def initialize(self) -> None:
        with _connect(self.path) as connection:
            connection.executescript(SCHEMA)
            columns = {
                row[1] for row in connection.execute("PRAGMA table_info(supplier_catalog_runs)")
            }
            migrations = {
                "source_type": "TEXT",
                "source_count": "INTEGER",
                "enumerated_count": "INTEGER",
                "unique_count": "INTEGER",
                "completeness_status": "TEXT NOT NULL DEFAULT 'partial_catalog'",
                "completeness_reason": "TEXT",
                "export_generated_at": "TEXT",
                "upstream_catalog_version": "TEXT",
                "product_catalog_coverage_type": "TEXT NOT NULL DEFAULT 'partial_catalog'",
                "product_catalog_coverage_complete": "INTEGER NOT NULL DEFAULT 0",
                "scenario_enrichment_status": "TEXT NOT NULL DEFAULT 'none'",
                "scenario_enrichment_count": "INTEGER NOT NULL DEFAULT 0",
                "scenario_enrichment_observed_at": "TEXT",
            }
            for name, declaration in migrations.items():
                if name not in columns:
                    connection.execute(
                        f"ALTER TABLE supplier_catalog_runs ADD COLUMN {name} {declaration}"
                    )
            product_columns = {
                row[1] for row in connection.execute("PRAGMA table_info(supplier_catalog_products)")
            }
            product_migrations = {
                "canonical_gtin": "TEXT",
                "catalog_fingerprint": "TEXT",
                "variant_fid": "TEXT",
                "variant_fid_source": "TEXT",
                "product_url": "TEXT",
                "enrichment_status": "TEXT NOT NULL DEFAULT 'none'",
                "enrichment_error_code": "TEXT",
                "enrichment_error_message": "TEXT",
                "offer_tier_observed_at": "TEXT",
                "catalog_delta_status": "TEXT",
            }
            for name, declaration in product_migrations.items():
                if name not in product_columns:
                    connection.execute(
                        f"ALTER TABLE supplier_catalog_products ADD COLUMN {name} {declaration}"
                    )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_supplier_catalog_products_gtin "
                "ON supplier_catalog_products (supplier, canonical_gtin)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_supplier_catalog_products_fingerprint "
                "ON supplier_catalog_products (run_id, catalog_fingerprint)"
            )

    def start_run(
        self, supplier: str, *, coverage_type: str,
        coverage_description: str, coverage_complete: bool, sampled: bool,
        run_id: str | None = None, started_at: str | None = None,
    ) -> str:
        supplier = _validate_supplier(supplier)
        self.initialize()
        run_id = run_id or uuid4().hex
        with _connect(self.path) as connection:
            connection.execute(
                """INSERT INTO supplier_catalog_runs (
                    run_id,supplier,started_at,status,coverage_type,
                    coverage_description,coverage_complete,sampled
                ) VALUES (?,?,?,'running',?,?,?,?)""",
                (run_id, supplier, started_at or utc_now(), coverage_type,
                 coverage_description, int(coverage_complete), int(sampled)),
            )
        return run_id

    def publish(
        self, run_id: str, generation: SupplierCatalogGeneration, *,
        elapsed_seconds: float, promote: bool = True,
    ) -> None:
        supplier = _validate_supplier(generation.supplier)
        if generation.coverage_complete and generation.completeness_status not in FULL_COMPLETENESS_STATUSES:
            raise SupplierCatalogError(
                "A complete catalog requires persisted FULL completeness proof"
            )
        product_rows = list(generation.products)
        scenario_rows = list(generation.scenarios)
        product_keys = {
            str(row.get("canonical_product_key") or row.get("product_key") or "")
            for row in product_rows
        }
        if "" in product_keys or len(product_keys) != len(product_rows):
            raise SupplierCatalogError("Product identities are missing or duplicated")
        scenario_ids = {str(row.get("scenario_id") or "") for row in scenario_rows}
        if "" in scenario_ids or len(scenario_ids) != len(scenario_rows):
            raise SupplierCatalogError("Scenario identities are missing or duplicated")
        if any(
            str(row.get("canonical_product_key") or row.get("product_key") or "") not in product_keys
            for row in scenario_rows
        ):
            raise SupplierCatalogError("Scenario references an unknown product")
        scenario_product_keys = {
            str(row.get("canonical_product_key") or row.get("product_key") or "")
            for row in scenario_rows
        }

        completed_at = utc_now()
        connection = _connect(self.path)
        try:
            connection.execute("BEGIN IMMEDIATE")
            run = connection.execute(
                "SELECT supplier,status,sampled FROM supplier_catalog_runs WHERE run_id=?",
                (run_id,),
            ).fetchone()
            if not run or run["supplier"] != supplier or run["status"] != "running":
                raise SupplierCatalogError("Generation is not publishable")
            for row in product_rows:
                key = str(row.get("canonical_product_key") or row.get("product_key"))
                enrichment_status = (
                    "enriched" if key in scenario_product_keys else
                    row.get("enrichment_status") or (
                        "identifier_unresolved" if not row.get("canonical_gtin")
                        and not canonical_gtin14(row.get("canonical_ean")) else "none"
                    )
                )
                connection.execute(
                    """INSERT INTO supplier_catalog_products (
                        run_id,supplier,canonical_product_key,canonical_ean,
                        canonical_gtin,identifier_type,raw_identifiers_json,supplier_product_id,
                        supplier_option_id,supplier_sku,brand,title,size_value,
                        size_unit,pack_count,catalog_fingerprint,variant_fid,
                        variant_fid_source,product_url,
                        enrichment_status,offer_tier_observed_at,catalog_delta_status,
                        metadata_json
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (run_id, supplier, key, row.get("canonical_ean"),
                     row.get("canonical_gtin") or canonical_gtin14(row.get("canonical_ean")),
                     row.get("identifier_type"), json_dumps(row.get("raw_identifiers") or []),
                     row.get("supplier_product_id"), row.get("supplier_option_id"),
                     row.get("supplier_sku"), row.get("brand"), row.get("title"),
                     row.get("size_value"), row.get("size_unit"), row.get("pack_count"),
                     row.get("catalog_fingerprint"), row.get("variant_fid"),
                     row.get("variant_fid_source"),
                     row.get("product_url"), enrichment_status,
                     row.get("offer_tier_observed_at"),
                     row.get("catalog_delta_status"),
                     json_dumps(row.get("metadata") or {})),
                )
            for row in scenario_rows:
                payload = dict(row.get("payload") or row)
                key = str(row.get("canonical_product_key") or row.get("product_key"))
                connection.execute(
                    """INSERT INTO supplier_catalog_scenarios (
                        run_id,supplier,scenario_id,canonical_product_key,
                        canonical_ean,raw_identifier,raw_identifier_type,
                        supplier_product_id,supplier_offer_id,supplier_sku,
                        scenario_type,scenario_label,price,currency,stock,
                        minimum_quantity,maximum_quantity,selling_unit,account_mov,
                        account_mov_currency,warehouse,shipping_mode,
                        availability_status,lead_time,payload_json
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (run_id, supplier, row["scenario_id"], key,
                     row.get("canonical_ean"), row.get("raw_identifier"),
                     row.get("raw_identifier_type"), row.get("supplier_product_id"),
                     row.get("supplier_offer_id"), row.get("supplier_sku"),
                     row.get("scenario_type"), row.get("scenario_label"),
                     str(row.get("price")) if row.get("price") is not None else None,
                     row.get("currency"), row.get("stock"),
                     row.get("minimum_quantity"), row.get("maximum_quantity"),
                     row.get("selling_unit"),
                     str(row.get("account_mov")) if row.get("account_mov") is not None else None,
                     row.get("account_mov_currency"), row.get("warehouse"),
                     row.get("shipping_mode"), row.get("availability_status"),
                     row.get("lead_time"), json_dumps(payload)),
                )
            status = "sample_success" if bool(run["sampled"]) else "success"
            connection.execute(
                """UPDATE supplier_catalog_runs SET status=?,completed_at=?,
                    product_count=?,scenario_count=?,page_count=?,request_count=?,
                    retry_count=?,rate_limit_count=?,server_error_count=?,
                    elapsed_seconds=?,coverage_type=?,coverage_description=?,
                    coverage_complete=?,source_type=?,source_count=?,
                    enumerated_count=?,unique_count=?,completeness_status=?,
                    completeness_reason=?,export_generated_at=?,
                    upstream_catalog_version=?,product_catalog_coverage_type=?,
                    product_catalog_coverage_complete=?,scenario_enrichment_status=?,
                    scenario_enrichment_count=?,scenario_enrichment_observed_at=?,
                    diagnostics_json=? WHERE run_id=?""",
                (status, completed_at, len(product_rows), len(scenario_rows),
                 generation.page_count, generation.request_count,
                 generation.retry_count, generation.rate_limit_count,
                 generation.server_error_count, elapsed_seconds,
                 generation.coverage_type, generation.coverage_description,
                 int(generation.coverage_complete), generation.source_type,
                 generation.source_count, generation.enumerated_count,
                 generation.unique_count, generation.completeness_status,
                 generation.completeness_reason, generation.export_generated_at,
                 generation.upstream_catalog_version,
                 generation.product_catalog_coverage_type or generation.completeness_status,
                 int(
                     generation.coverage_complete
                     if generation.product_catalog_coverage_complete is None
                     else generation.product_catalog_coverage_complete
                 ),
                 generation.scenario_enrichment_status or (
                     "full" if scenario_rows and generation.coverage_complete else (
                         "partial" if scenario_rows else "none"
                     )
                 ),
                 len(scenario_rows) if generation.scenario_enrichment_count is None
                 else generation.scenario_enrichment_count,
                 generation.scenario_enrichment_observed_at or generation.export_generated_at,
                 json_dumps(generation.diagnostics),
                 run_id),
            )
            if promote and status == "success":
                connection.execute(
                    """INSERT INTO supplier_catalog_active_generations
                       (supplier,run_id,updated_at) VALUES (?,?,?)
                       ON CONFLICT(supplier) DO UPDATE SET
                         run_id=excluded.run_id,updated_at=excluded.updated_at""",
                    (supplier, run_id, completed_at),
                )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def publish_product_catalog_stream(
        self, run_id: str, *, supplier: str,
        products: Iterable[dict[str, Any]], elapsed_seconds: float,
        product_catalog_coverage_type: str,
        product_catalog_coverage_complete: bool,
        scenario_enrichment_status: str = "none",
        source_type: str | None = None, source_count: int | None = None,
        export_generated_at: str | None = None,
        upstream_catalog_version: str | None = None,
        diagnostics: dict[str, Any] | None = None,
        promote: bool = True,
        previous_run_id: str | None = None,
        reuse_unchanged_scenarios_after: str | None = None,
    ) -> dict[str, Any]:
        """Atomically publish a large product catalog without materializing it.

        Offer/tier scenarios may be carried forward only for unchanged products
        whose previous enrichment timestamp satisfies the caller-provided cutoff.
        Carried scenarios retain their original payload provenance; they are not
        silently reclassified as freshly enriched.
        """
        supplier = _validate_supplier(supplier)
        if product_catalog_coverage_type not in PRODUCT_CATALOG_COVERAGE_TYPES:
            raise SupplierCatalogError("Unsupported product catalog coverage type")
        if scenario_enrichment_status not in SCENARIO_ENRICHMENT_STATUSES:
            raise SupplierCatalogError("Unsupported scenario enrichment status")
        self.initialize()
        completed_at = utc_now()
        connection = _connect(self.path)
        try:
            connection.execute("BEGIN IMMEDIATE")
            run = connection.execute(
                "SELECT supplier,status,sampled FROM supplier_catalog_runs WHERE run_id=?",
                (run_id,),
            ).fetchone()
            if not run or run["supplier"] != supplier or run["status"] != "running":
                raise SupplierCatalogError("Generation is not publishable")
            if previous_run_id:
                previous = connection.execute(
                    """SELECT run_id FROM supplier_catalog_runs
                       WHERE run_id=? AND supplier=? AND status IN ('success','sample_success')""",
                    (previous_run_id, supplier),
                ).fetchone()
                if not previous:
                    raise SupplierCatalogError(
                        "Explicit comparison generation is missing or not complete"
                    )
            else:
                previous = connection.execute(
                    "SELECT run_id FROM supplier_catalog_active_generations WHERE supplier=?",
                    (supplier,),
                ).fetchone()
                previous_run_id = previous["run_id"] if previous else None

            # Only the compact baseline key/fingerprint map is materialized. The
            # new export remains streaming, so both full generations are never
            # held in memory and no post-insert random-update sweep is needed.
            previous_products = {}
            if previous_run_id:
                previous_products = {
                    row["canonical_product_key"]: dict(row)
                    for row in connection.execute(
                    """SELECT canonical_product_key,catalog_fingerprint,variant_fid,
                              variant_fid_source,enrichment_status,offer_tier_observed_at
                       FROM supplier_catalog_products WHERE run_id=?""",
                    (previous_run_id,),
                )}
            delta = {"new": 0, "changed": 0, "removed": 0, "unchanged": 0}
            reused_enriched_products = 0
            product_count = 0
            for row in products:
                key = str(row.get("canonical_product_key") or row.get("product_key") or "")
                if not key:
                    raise SupplierCatalogError("Product identity is missing")
                metadata = dict(row.get("metadata") or {})
                fingerprint = row.get("catalog_fingerprint") or metadata.get("catalog_fingerprint")
                previous_product = previous_products.pop(key, None)
                if previous_product is None:
                    product_state = "new"
                elif (previous_product["catalog_fingerprint"] or "") == (fingerprint or ""):
                    product_state = "unchanged"
                else:
                    product_state = "changed"
                delta[product_state] += 1
                variant_fid = row.get("variant_fid") or metadata.get("variant_fid")
                variant_fid_source = row.get("variant_fid_source")
                if not variant_fid and previous_product and previous_product.get("variant_fid"):
                    variant_fid = previous_product["variant_fid"]
                    variant_fid_source = previous_product.get("variant_fid_source")
                enrichment_status = row.get("enrichment_status") or "none"
                observed_at = row.get("offer_tier_observed_at")
                previous_observed = previous_product.get("offer_tier_observed_at") if previous_product else None
                can_carry_observation = bool(
                    product_state == "unchanged" and previous_observed
                    and previous_product.get("enrichment_status") in {
                        "enriched", "full", "carried_forward",
                    }
                    and (
                        not reuse_unchanged_scenarios_after
                        or previous_observed >= reuse_unchanged_scenarios_after
                    )
                )
                if can_carry_observation:
                    enrichment_status = "carried_forward"
                    observed_at = previous_observed
                    reused_enriched_products += 1
                elif variant_fid:
                    enrichment_status = "enrichment_pending"
                connection.execute(
                    """INSERT INTO supplier_catalog_products (
                        run_id,supplier,canonical_product_key,canonical_ean,
                        canonical_gtin,identifier_type,raw_identifiers_json,
                        supplier_product_id,supplier_option_id,supplier_sku,brand,title,
                        size_value,size_unit,pack_count,catalog_fingerprint,variant_fid,
                        variant_fid_source,
                        product_url,enrichment_status,offer_tier_observed_at,
                        catalog_delta_status,metadata_json
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (run_id, supplier, key, row.get("canonical_ean"),
                     row.get("canonical_gtin") or canonical_gtin14(row.get("canonical_ean")),
                     row.get("identifier_type"), json_dumps(row.get("raw_identifiers") or []),
                     row.get("supplier_product_id"), row.get("supplier_option_id"),
                     row.get("supplier_sku"), row.get("brand"), row.get("title"),
                     row.get("size_value"), row.get("size_unit"), row.get("pack_count"),
                     fingerprint, variant_fid, variant_fid_source,
                     row.get("product_url") or metadata.get("product_url"),
                     enrichment_status, observed_at, product_state, json_dumps(metadata)),
                )
                product_count += 1

            delta["removed"] = len(previous_products)
            reused_scenarios = 0
            if previous_run_id:
                if reuse_unchanged_scenarios_after:
                    scenario_columns = (
                        "supplier,scenario_id,canonical_product_key,canonical_ean,raw_identifier,"
                        "raw_identifier_type,supplier_product_id,supplier_offer_id,supplier_sku,"
                        "scenario_type,scenario_label,price,currency,stock,minimum_quantity,"
                        "maximum_quantity,selling_unit,account_mov,account_mov_currency,warehouse,"
                        "shipping_mode,availability_status,lead_time,payload_json"
                    )
                    connection.execute(
                        f"""INSERT INTO supplier_catalog_scenarios (run_id,{scenario_columns})
                            SELECT ?,{','.join('scenario.' + name for name in scenario_columns.split(','))}
                            FROM supplier_catalog_scenarios scenario
                            JOIN supplier_catalog_products previous
                              ON previous.run_id=scenario.run_id
                             AND previous.canonical_product_key=scenario.canonical_product_key
                            JOIN supplier_catalog_products current
                              ON current.run_id=?
                             AND current.canonical_product_key=previous.canonical_product_key
                             AND COALESCE(current.catalog_fingerprint,'')=COALESCE(previous.catalog_fingerprint,'')
                            WHERE scenario.run_id=?
                              AND previous.offer_tier_observed_at>=?""",
                        (run_id, run_id, previous_run_id, reuse_unchanged_scenarios_after),
                    )
                    reused_scenarios = connection.execute(
                        "SELECT COUNT(*) FROM supplier_catalog_scenarios WHERE run_id=?",
                        (run_id,),
                    ).fetchone()[0]

            combined_diagnostics = {
                **(diagnostics or {}),
                "generation_delta": delta,
                "reused_scenarios": reused_scenarios,
                "streaming_publish": True,
            }
            status = "sample_success" if bool(run["sampled"]) else "success"
            coverage_type = product_catalog_coverage_type
            effective_enrichment_status = (
                "partial" if reused_enriched_products and scenario_enrichment_status == "none"
                else scenario_enrichment_status
            )
            connection.execute(
                """UPDATE supplier_catalog_runs SET status=?,completed_at=?,product_count=?,
                    scenario_count=?,elapsed_seconds=?,coverage_type=?,coverage_description=?,
                    coverage_complete=?,source_type=?,source_count=?,enumerated_count=?,unique_count=?,
                    completeness_status=?,completeness_reason=?,export_generated_at=?,
                    upstream_catalog_version=?,product_catalog_coverage_type=?,
                    product_catalog_coverage_complete=?,scenario_enrichment_status=?,
                    scenario_enrichment_count=?,scenario_enrichment_observed_at=?,diagnostics_json=?
                    WHERE run_id=?""",
                (status, completed_at, product_count, reused_scenarios, elapsed_seconds,
                 coverage_type, str((diagnostics or {}).get("coverage_description") or coverage_type),
                 int(product_catalog_coverage_complete), source_type, source_count,
                 product_count, product_count, coverage_type,
                 str((diagnostics or {}).get("completeness_reason") or coverage_type),
                 export_generated_at, upstream_catalog_version, product_catalog_coverage_type,
                 int(product_catalog_coverage_complete), effective_enrichment_status,
                 reused_enriched_products, None, json_dumps(combined_diagnostics), run_id),
            )
            if promote and status == "success":
                connection.execute(
                    """INSERT INTO supplier_catalog_active_generations
                       (supplier,run_id,updated_at) VALUES (?,?,?)
                       ON CONFLICT(supplier) DO UPDATE SET
                         run_id=excluded.run_id,updated_at=excluded.updated_at""",
                    (supplier, run_id, completed_at),
                )
            connection.commit()
            return {"product_count": product_count, "scenario_count": reused_scenarios,
                    "generation_delta": delta, "reused_scenarios": reused_scenarios}
        except sqlite3.IntegrityError as exc:
            connection.rollback()
            raise SupplierCatalogError("Product identities are missing or duplicated") from exc
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def fail(self, run_id: str, *, error_code: str, error_message: str,
             elapsed_seconds: float, diagnostics: dict | None = None) -> None:
        self.initialize()
        with _connect(self.path) as connection:
            connection.execute(
                """UPDATE supplier_catalog_runs SET status='failed',completed_at=?,
                   elapsed_seconds=?,error_code=?,error_message=?,diagnostics_json=?
                   WHERE run_id=? AND status='running'""",
                (utc_now(), elapsed_seconds, error_code, str(error_message)[:500],
                 json_dumps(diagnostics or {}), run_id),
            )

    def promote_run(self, run_id: str) -> None:
        """Atomically activate an already validated complete generation."""
        self.initialize()
        with _connect(self.path) as connection:
            run = connection.execute(
                """SELECT supplier,status,sampled,product_catalog_coverage_complete,
                          product_count,scenario_count
                   FROM supplier_catalog_runs WHERE run_id=?""",
                (run_id,),
            ).fetchone()
            if not run or run["status"] != "success" or bool(run["sampled"]):
                raise SupplierCatalogError("Only a complete successful generation can be promoted")
            if not bool(run["product_catalog_coverage_complete"]):
                raise SupplierCatalogError("Incomplete product catalog cannot be promoted")
            if int(run["product_count"] or 0) <= 0 or int(run["scenario_count"] or 0) <= 0:
                raise SupplierCatalogError("Empty product/scenario generation cannot be promoted")
            connection.execute(
                """INSERT INTO supplier_catalog_active_generations
                   (supplier,run_id,updated_at) VALUES (?,?,?)
                   ON CONFLICT(supplier) DO UPDATE SET
                     run_id=excluded.run_id,updated_at=excluded.updated_at""",
                (run["supplier"], run_id, utc_now()),
            )

    def latest_success(self, supplier: str) -> dict[str, Any] | None:
        supplier = _validate_supplier(supplier)
        self.initialize()
        connection = _connect(self.path)
        try:
            run = connection.execute(
                """SELECT r.* FROM supplier_catalog_active_generations a
                   JOIN supplier_catalog_runs r ON r.run_id=a.run_id
                   WHERE a.supplier=? AND r.status='success'""",
                (supplier,),
            ).fetchone()
            if not run:
                return None
            products = []
            for row in connection.execute(
                "SELECT * FROM supplier_catalog_products WHERE run_id=? ORDER BY canonical_product_key",
                (run["run_id"],),
            ):
                product = dict(row)
                product["raw_identifiers"] = json.loads(product.pop("raw_identifiers_json") or "[]")
                product["metadata"] = json.loads(product.pop("metadata_json") or "{}")
                products.append(product)
            scenarios = []
            for row in connection.execute(
                """SELECT canonical_product_key,payload_json
                   FROM supplier_catalog_scenarios
                   WHERE run_id=? ORDER BY scenario_id""",
                (run["run_id"],),
            ):
                scenario = normalize_purchase_scenario(json.loads(row["payload_json"]))
                scenario["supplier_catalog_product_key"] = row["canonical_product_key"]
                scenarios.append(scenario)
            result = dict(run)
            result["coverage_complete"] = bool(result["coverage_complete"])
            result["product_catalog_coverage_complete"] = bool(
                result.get("product_catalog_coverage_complete")
            )
            result["sampled"] = bool(result["sampled"])
            result["diagnostics"] = json.loads(result.pop("diagnostics_json") or "{}")
            result["products"] = products
            result["scenarios"] = scenarios
            return result
        finally:
            connection.close()

    def run_status(self, run_id: str) -> dict[str, Any] | None:
        self.initialize()
        with _connect(self.path) as connection:
            row = connection.execute(
                "SELECT * FROM supplier_catalog_runs WHERE run_id=?", (run_id,)
            ).fetchone()
            if not row:
                return None
            result = dict(row)
            result["diagnostics"] = json.loads(result.pop("diagnostics_json") or "{}")
            result["coverage_complete"] = bool(result["coverage_complete"])
            result["product_catalog_coverage_complete"] = bool(
                result.get("product_catalog_coverage_complete")
            )
            result["sampled"] = bool(result["sampled"])
            return result

    def active_generation_metadata(self, supplier: str) -> dict[str, Any] | None:
        supplier = _validate_supplier(supplier)
        self.initialize()
        with _connect(self.path) as connection:
            row = connection.execute(
                """SELECT run.* FROM supplier_catalog_active_generations active
                   JOIN supplier_catalog_runs run ON run.run_id=active.run_id
                   WHERE active.supplier=? AND run.status='success'""",
                (supplier,),
            ).fetchone()
            if not row:
                return None
            result = dict(row)
            result["coverage_complete"] = bool(result["coverage_complete"])
            result["product_catalog_coverage_complete"] = bool(
                result.get("product_catalog_coverage_complete")
            )
            result["diagnostics"] = json.loads(result.pop("diagnostics_json") or "{}")
            return result

    def serving_generation_metadata(self, supplier: str) -> dict[str, Any] | None:
        """Return a validated partial serving snapshot without implying promotion."""
        supplier = _validate_supplier(supplier)
        if supplier != "qogita":
            return self.active_generation_metadata(supplier)
        from qogita_serving import QogitaServingStore

        snapshot = QogitaServingStore(self.path).active_snapshot()
        if not snapshot:
            return None
        duty = QogitaServingStore(self.path).duty_state(snapshot["bootstrap_run_id"]) or {}
        return {
            **snapshot,
            "run_id": snapshot["serving_generation_id"],
            "completed_at": snapshot["created_at"],
            "coverage_type": snapshot["product_catalog_coverage_type"],
            "coverage_description": (
                "Full account product catalog; partial verified offer/tier serving snapshot"
            ),
            "coverage_complete": snapshot["product_catalog_coverage_complete"],
            "product_count": snapshot["product_catalog_count"],
            "scenario_enrichment_count": snapshot["enriched_product_count"],
            "scenario_count": snapshot["scenario_count"],
            "sampled": False,
            "serving_snapshot": True,
            "duty_state": duty.get("state"),
            "current_window_started_at": duty.get("current_window_started_at"),
            "current_window_deadline": duty.get("current_window_deadline"),
            "rest_until": duty.get("rest_until"),
        }

    def latest_serving(self, supplier: str) -> dict[str, Any] | None:
        """Load immutable data referenced by the active serving snapshot."""
        supplier = _validate_supplier(supplier)
        if supplier != "qogita":
            return self.latest_success(supplier)
        metadata = self.serving_generation_metadata(supplier)
        if not metadata:
            return None
        self.initialize()
        with _connect(self.path) as connection:
            products = []
            for row in connection.execute(
                """SELECT product.* FROM qogita_serving_memberships membership
                     JOIN supplier_catalog_products product
                       ON product.run_id=?
                      AND product.canonical_product_key=membership.canonical_product_key
                    WHERE membership.serving_generation_id=?
                    ORDER BY product.canonical_product_key""",
                (metadata["source_generation_id"], metadata["serving_generation_id"]),
            ):
                product = dict(row)
                product["raw_identifiers"] = json.loads(
                    product.pop("raw_identifiers_json") or "[]"
                )
                product["metadata"] = json.loads(product.pop("metadata_json") or "{}")
                products.append(product)
            scenarios = []
            for row in connection.execute(
                """SELECT scenario.canonical_product_key,scenario.payload_json
                     FROM qogita_serving_memberships membership
                     JOIN supplier_catalog_scenarios scenario
                       ON scenario.run_id=?
                      AND scenario.canonical_product_key=membership.canonical_product_key
                    WHERE membership.serving_generation_id=?
                    ORDER BY scenario.scenario_id""",
                (metadata["source_generation_id"], metadata["serving_generation_id"]),
            ):
                scenario = normalize_purchase_scenario(json.loads(row["payload_json"]))
                scenario["supplier_catalog_product_key"] = row["canonical_product_key"]
                scenario["supplier_serving_generation_id"] = metadata["serving_generation_id"]
                scenarios.append(scenario)
        return {**metadata, "products": products, "scenarios": scenarios}

    def active_identifiers(self, suppliers) -> set[str]:
        """Return the active supplier-first identifier union without payloads."""
        selected = [_validate_supplier(value) for value in suppliers]
        if not selected:
            return set()
        self.initialize()
        promoted = [supplier for supplier in selected if supplier != "qogita"]
        with _connect(self.path) as connection:
            identifiers = set()
            if promoted:
                placeholders = ",".join("?" for _ in promoted)
                identifiers.update(
                    row["canonical_ean"]
                    for row in connection.execute(
                        f"""SELECT DISTINCT scenario.canonical_ean
                             FROM supplier_catalog_active_generations active
                             JOIN supplier_catalog_scenarios scenario
                               ON scenario.run_id=active.run_id
                             WHERE active.supplier IN ({placeholders})
                               AND scenario.canonical_ean IS NOT NULL""",
                        promoted,
                    )
                )
            if "qogita" in selected:
                from qogita_serving import QogitaServingStore
                QogitaServingStore(self.path).initialize()
                identifiers.update(
                    row["canonical_ean"]
                    for row in connection.execute(
                        """SELECT DISTINCT scenario.canonical_ean
                             FROM qogita_serving_active active
                             JOIN qogita_serving_snapshots snapshot
                               ON snapshot.serving_generation_id=active.serving_generation_id
                             JOIN qogita_serving_memberships membership
                               ON membership.serving_generation_id=snapshot.serving_generation_id
                             JOIN supplier_catalog_scenarios scenario
                               ON scenario.run_id=snapshot.source_generation_id
                              AND scenario.canonical_product_key=membership.canonical_product_key
                            WHERE active.supplier='qogita'
                              AND snapshot.status='valid'
                              AND scenario.canonical_ean IS NOT NULL"""
                    )
                )
            return identifiers

    def active_identifier_universe(self, suppliers) -> dict[str, int]:
        """Count the union eligible for Discovery without loading scenario payloads."""
        selected = [_validate_supplier(value) for value in suppliers]
        if not selected:
            return {"total": 0, "eligible": 0}
        self.initialize()
        promoted = [supplier for supplier in selected if supplier != "qogita"]
        queries = []
        parameters: list[str] = []
        if promoted:
            placeholders = ",".join("?" for _ in promoted)
            queries.append(
                f"""SELECT scenario.canonical_ean AS identifier
                       FROM supplier_catalog_active_generations active
                       JOIN supplier_catalog_scenarios scenario
                         ON scenario.run_id=active.run_id
                      WHERE active.supplier IN ({placeholders})
                        AND scenario.canonical_ean IS NOT NULL
                      GROUP BY scenario.canonical_ean"""
            )
            parameters.extend(promoted)
        if "qogita" in selected:
            from qogita_serving import QogitaServingStore
            QogitaServingStore(self.path).initialize()
            queries.append(
                """SELECT selected.gtin AS identifier
                     FROM qogita_serving_active active
                     JOIN qogita_serving_snapshots snapshot
                       ON snapshot.serving_generation_id=active.serving_generation_id
                     JOIN qogita_serving_memberships membership
                       ON membership.serving_generation_id=snapshot.serving_generation_id
                     JOIN qogita_bootstrap_products selected
                       ON selected.bootstrap_run_id=snapshot.bootstrap_run_id
                      AND selected.canonical_product_key=membership.canonical_product_key
                    WHERE active.supplier='qogita' AND snapshot.status='valid'
                      AND membership.scenario_count>0
                    GROUP BY selected.gtin"""
            )
        with _connect(self.path) as connection:
            identifiers = [
                row["identifier"]
                for row in connection.execute(" UNION ".join(queries), parameters)
            ]
        return {
            "total": len(identifiers),
            "eligible": sum(canonical_gtin14(value) is not None for value in identifiers),
        }

    def active_identifier_memberships(self, suppliers) -> dict[str, tuple[str, ...]]:
        """Return identifier membership without materializing product payloads."""
        selected = [_validate_supplier(value) for value in suppliers]
        memberships: dict[str, set[str]] = {}
        self.initialize()
        promoted = [supplier for supplier in selected if supplier != "qogita"]
        with _connect(self.path) as connection:
            if promoted:
                placeholders = ",".join("?" for _ in promoted)
                for row in connection.execute(
                    f"""SELECT active.supplier,scenario.canonical_ean
                          FROM supplier_catalog_active_generations active
                          JOIN supplier_catalog_scenarios scenario
                            ON scenario.run_id=active.run_id
                         WHERE active.supplier IN ({placeholders})
                           AND scenario.canonical_ean IS NOT NULL
                         GROUP BY active.supplier,scenario.canonical_ean""",
                    promoted,
                ):
                    memberships.setdefault(row["canonical_ean"], set()).add(row["supplier"])
            if "qogita" in selected:
                from qogita_serving import QogitaServingStore
                QogitaServingStore(self.path).initialize()
                for row in connection.execute(
                    """SELECT scenario.canonical_ean
                         FROM qogita_serving_active active
                         JOIN qogita_serving_snapshots snapshot
                           ON snapshot.serving_generation_id=active.serving_generation_id
                         JOIN qogita_serving_memberships membership
                           ON membership.serving_generation_id=snapshot.serving_generation_id
                         JOIN supplier_catalog_scenarios scenario
                           ON scenario.run_id=snapshot.source_generation_id
                          AND scenario.canonical_product_key=membership.canonical_product_key
                        WHERE active.supplier='qogita' AND snapshot.status='valid'
                          AND scenario.canonical_ean IS NOT NULL
                        GROUP BY scenario.canonical_ean"""
                ):
                    memberships.setdefault(row["canonical_ean"], set()).add("qogita")
        return {
            identifier: tuple(sorted(values))
            for identifier, values in memberships.items()
            if canonical_gtin14(identifier) is not None
        }

    def active_candidates_for_identifier(
        self, supplier: str, identifier: str,
    ) -> list[dict[str, Any]]:
        """Load one identifier from the active generation without scanning it.

        This is intentionally narrow for explicit user lookups.  It preserves
        the same normalized candidate/scenario contract as ``latest_candidates``
        while avoiding a full catalog materialization.
        """
        supplier = _validate_supplier(supplier)
        comparison = canonical_gtin14(identifier)
        if comparison is None:
            return []
        if supplier == "qogita":
            return self._serving_candidates_for_identifier(identifier, comparison)
        self.initialize()
        with _connect(self.path) as connection:
            active = connection.execute(
                """SELECT run.run_id,run.completed_at,run.coverage_type,
                          run.coverage_complete
                   FROM supplier_catalog_active_generations active
                   JOIN supplier_catalog_runs run ON run.run_id=active.run_id
                   WHERE active.supplier=? AND run.status='success'""",
                (supplier,),
            ).fetchone()
            if not active:
                return []
            product_rows = connection.execute(
                """SELECT * FROM supplier_catalog_products
                   WHERE run_id=? AND canonical_ean IN (?,?,?)
                   ORDER BY canonical_product_key""",
                (
                    active["run_id"], identifier, comparison,
                    comparison[1:] if comparison.startswith("0") else comparison,
                ),
            ).fetchall()
            matched_products = [
                row for row in product_rows
                if canonical_gtin14(row["canonical_ean"]) == comparison
            ]
            candidates = []
            for product_row in matched_products:
                scenario_rows = connection.execute(
                    """SELECT payload_json FROM supplier_catalog_scenarios
                       WHERE run_id=? AND canonical_product_key=?
                       ORDER BY scenario_id""",
                    (active["run_id"], product_row["canonical_product_key"]),
                ).fetchall()
                scenarios = [
                    normalize_purchase_scenario(json.loads(row["payload_json"]))
                    for row in scenario_rows
                ]
                if not scenarios:
                    continue
                scenarios.sort(key=lambda row: (
                    int(row.get("scenario_order") or 0),
                    str(row.get("scenario_id") or ""),
                ))
                candidates.append({
                    "product_key": scenarios[0].get("product_key"),
                    "canonical_ean": product_row["canonical_ean"],
                    "identifier_type": product_row["identifier_type"],
                    "brand": product_row["brand"],
                    "title": product_row["title"],
                    "volume_value": product_row["size_value"],
                    "volume_unit": product_row["size_unit"],
                    "pack_count": product_row["pack_count"],
                    "scenarios": scenarios,
                    "supplier_catalog_run_id": active["run_id"],
                    "supplier_catalog_completed_at": active["completed_at"],
                    "coverage_type": active["coverage_type"],
                    "coverage_complete": bool(active["coverage_complete"]),
                })
            return candidates

    def active_candidate_generation_metadata(
        self, supplier: str,
    ) -> dict[str, Any] | None:
        """Load immutable candidate-source metadata once for a preparation run."""
        supplier = _validate_supplier(supplier)
        if supplier == "qogita":
            return self.serving_generation_metadata("qogita")
        self.initialize()
        with _connect(self.path) as connection:
            row = connection.execute(
                """SELECT run.run_id,run.completed_at,run.coverage_type,
                          run.coverage_complete
                     FROM supplier_catalog_active_generations active
                     JOIN supplier_catalog_runs run ON run.run_id=active.run_id
                    WHERE active.supplier=? AND run.status='success'""",
                (supplier,),
            ).fetchone()
        return dict(row) if row else None

    def iter_active_candidates_for_identifiers(
        self, supplier: str, identifiers: Iterable[str], *, batch_size: int = 500,
        generation_metadata: dict[str, Any] | None = None,
    ):
        """Yield active candidates using bounded, indexed supplier batches.

        The direct-EAN method above intentionally remains point-oriented.  Full
        Discovery preparation uses this set-oriented path so generation and
        serving metadata are not reloaded for every identifier.
        """
        supplier = _validate_supplier(supplier)
        metadata = generation_metadata or self.active_candidate_generation_metadata(supplier)
        if not metadata:
            return
        values = iter(identifiers)
        with _connect(self.path) as connection:
            while True:
                requested = []
                try:
                    for _ in range(max(1, int(batch_size))):
                        requested.append(str(next(values)))
                except StopIteration:
                    pass
                if not requested:
                    return
                requested_by_gtin = {
                    canonical_gtin14(value): value for value in requested
                    if canonical_gtin14(value) is not None
                }
                comparisons = sorted(requested_by_gtin)
                if not comparisons:
                    continue
                placeholders = ",".join("?" for _ in comparisons)
                if supplier == "qogita":
                    run_id = str(metadata["source_generation_id"])
                    serving_id = str(metadata["serving_generation_id"])
                    product_rows = connection.execute(
                        f"""SELECT product.*
                              FROM supplier_catalog_products AS product
                                   INDEXED BY idx_supplier_catalog_products_run_gtin
                              JOIN qogita_serving_memberships membership
                                ON membership.serving_generation_id=?
                               AND membership.canonical_product_key=product.canonical_product_key
                             WHERE product.run_id=?
                               AND product.canonical_gtin IN ({placeholders})
                             ORDER BY product.canonical_product_key""",
                        (serving_id, run_id, *comparisons),
                    ).fetchall()
                else:
                    run_id = str(metadata["run_id"])
                    product_rows = connection.execute(
                        f"""SELECT * FROM supplier_catalog_products
                             WHERE run_id=? AND canonical_gtin IN ({placeholders})
                             ORDER BY canonical_product_key""",
                        (run_id, *comparisons),
                    ).fetchall()
                if not product_rows:
                    continue
                product_keys = [str(row["canonical_product_key"]) for row in product_rows]
                key_placeholders = ",".join("?" for _ in product_keys)
                scenario_rows = connection.execute(
                    f"""SELECT canonical_product_key,payload_json
                           FROM supplier_catalog_scenarios
                          WHERE run_id=?
                            AND canonical_product_key IN ({key_placeholders})
                          ORDER BY canonical_product_key,scenario_id""",
                    (run_id, *product_keys),
                ).fetchall()
                scenarios_by_key: dict[str, list[dict[str, Any]]] = {}
                for row in scenario_rows:
                    scenarios_by_key.setdefault(
                        str(row["canonical_product_key"]), [],
                    ).append(normalize_purchase_scenario(json.loads(row["payload_json"])))
                for product_row in product_rows:
                    comparison = canonical_gtin14(product_row["canonical_gtin"])
                    identifier = requested_by_gtin.get(comparison)
                    scenarios = scenarios_by_key.get(
                        str(product_row["canonical_product_key"]), [],
                    )
                    if not identifier or not scenarios:
                        continue
                    scenarios.sort(key=lambda row: (
                        int(row.get("scenario_order") or 0),
                        str(row.get("scenario_id") or ""),
                    ))
                    candidate = {
                        "product_key": scenarios[0].get("product_key"),
                        "canonical_ean": identifier,
                        "identifier_type": product_row["identifier_type"],
                        "brand": product_row["brand"],
                        "title": product_row["title"],
                        "volume_value": product_row["size_value"],
                        "volume_unit": product_row["size_unit"],
                        "pack_count": product_row["pack_count"],
                        "scenarios": scenarios,
                        "supplier_catalog_run_id": run_id,
                        "supplier_catalog_completed_at": (
                            metadata.get("created_at") if supplier == "qogita"
                            else metadata.get("completed_at")
                        ),
                        "coverage_type": (
                            metadata.get("product_catalog_coverage_type")
                            if supplier == "qogita" else metadata.get("coverage_type")
                        ),
                        "coverage_complete": True if supplier == "qogita" else bool(
                            metadata.get("coverage_complete")
                        ),
                    }
                    if supplier == "qogita":
                        candidate["supplier_serving_generation_id"] = serving_id
                    yield candidate

    def _serving_candidates_for_identifier(
        self, identifier: str, comparison: str,
    ) -> list[dict[str, Any]]:
        metadata = self.serving_generation_metadata("qogita")
        if not metadata:
            return []
        with _connect(self.path) as connection:
            product_rows = connection.execute(
                """SELECT product.* FROM qogita_serving_memberships membership
                     JOIN supplier_catalog_products product
                       ON product.run_id=?
                      AND product.canonical_product_key=membership.canonical_product_key
                    WHERE membership.serving_generation_id=?
                      AND product.canonical_gtin=?
                    ORDER BY product.canonical_product_key""",
                (metadata["source_generation_id"], metadata["serving_generation_id"], comparison),
            ).fetchall()
            candidates = []
            for product_row in product_rows:
                scenario_rows = connection.execute(
                    """SELECT payload_json FROM supplier_catalog_scenarios
                       WHERE run_id=? AND canonical_product_key=? ORDER BY scenario_id""",
                    (metadata["source_generation_id"], product_row["canonical_product_key"]),
                ).fetchall()
                scenarios = [
                    normalize_purchase_scenario(json.loads(row["payload_json"]))
                    for row in scenario_rows
                ]
                if not scenarios:
                    continue
                candidates.append({
                    "product_key": scenarios[0].get("product_key"),
                    "canonical_ean": product_row["canonical_ean"],
                    "identifier_type": product_row["identifier_type"],
                    "brand": product_row["brand"], "title": product_row["title"],
                    "volume_value": product_row["size_value"],
                    "volume_unit": product_row["size_unit"],
                    "pack_count": product_row["pack_count"], "scenarios": scenarios,
                    "supplier_catalog_run_id": metadata["source_generation_id"],
                    "supplier_serving_generation_id": metadata["serving_generation_id"],
                    "supplier_catalog_completed_at": metadata["created_at"],
                    "coverage_type": metadata["product_catalog_coverage_type"],
                    "coverage_complete": True,
                })
            return candidates

    def serving_catalog_contains_identifier(self, supplier: str, identifier: str) -> bool:
        """Check immutable source catalog membership, never mutable bootstrap state."""
        supplier = _validate_supplier(supplier)
        comparison = canonical_gtin14(identifier)
        metadata = self.serving_generation_metadata(supplier)
        if supplier != "qogita" or comparison is None or not metadata:
            return False
        with _connect(self.path) as connection:
            return connection.execute(
                """SELECT 1 FROM supplier_catalog_products
                   WHERE run_id=? AND canonical_gtin=? LIMIT 1""",
                (metadata["source_generation_id"], comparison),
            ).fetchone() is not None

    def iter_products(self, run_id: str, *, fetch_size: int = 1000):
        """Yield product rows in bounded batches for catalogs with hundreds of thousands of GTINs."""
        self.initialize()
        connection = _connect(self.path)
        try:
            cursor = connection.execute(
                "SELECT * FROM supplier_catalog_products WHERE run_id=? ORDER BY canonical_product_key",
                (run_id,),
            )
            while True:
                rows = cursor.fetchmany(max(1, int(fetch_size)))
                if not rows:
                    break
                for row in rows:
                    product = dict(row)
                    product["raw_identifiers"] = json.loads(
                        product.pop("raw_identifiers_json") or "[]"
                    )
                    product["metadata"] = json.loads(product.pop("metadata_json") or "{}")
                    yield product
        finally:
            connection.close()

    def iter_scenarios(self, run_id: str, *, fetch_size: int = 1000):
        self.initialize()
        connection = _connect(self.path)
        try:
            cursor = connection.execute(
                """SELECT canonical_product_key,payload_json
                   FROM supplier_catalog_scenarios WHERE run_id=? ORDER BY scenario_id""",
                (run_id,),
            )
            while True:
                rows = cursor.fetchmany(max(1, int(fetch_size)))
                if not rows:
                    break
                for row in rows:
                    scenario = normalize_purchase_scenario(json.loads(row["payload_json"]))
                    scenario["supplier_catalog_product_key"] = row["canonical_product_key"]
                    yield scenario
        finally:
            connection.close()

    def latest_candidates(self, supplier: str) -> list[dict[str, Any]]:
        """Build Discovery candidates exclusively from Scout's active generation.

        Products without a currently valid purchase scenario stay available in
        ``latest_success()['products']`` for catalog diagnostics, but cannot
        participate in supplier economics and are omitted from this view.
        """
        generation = self.latest_serving(supplier) if supplier == "qogita" else self.latest_success(supplier)
        if not generation:
            return []
        product_by_key = {
            str(product["canonical_product_key"]): product
            for product in generation["products"]
        }
        scenarios_by_key: dict[str, list[dict[str, Any]]] = {}
        for scenario in generation["scenarios"]:
            key = str(scenario.get("product_key") or "")
            if key:
                scenarios_by_key.setdefault(key, []).append(scenario)

        candidates = []
        for key, scenarios in sorted(scenarios_by_key.items()):
            product = product_by_key.get(
                str(scenarios[0].get("supplier_catalog_product_key") or "")
            )
            if not product:
                raise SupplierCatalogError(
                    f"Active generation contains an orphan scenario: {key}"
                )
            scenarios.sort(key=lambda row: (
                int(row.get("scenario_order") or 0), str(row.get("scenario_id") or "")
            ))
            candidates.append({
                "product_key": key,
                "canonical_ean": product.get("canonical_ean"),
                "identifier_type": product.get("identifier_type"),
                "brand": product.get("brand"),
                "title": product.get("title"),
                "volume_value": product.get("size_value"),
                "volume_unit": product.get("size_unit"),
                "pack_count": product.get("pack_count"),
                "scenarios": scenarios,
                "supplier_catalog_run_id": generation["run_id"],
                "supplier_catalog_completed_at": generation["completed_at"],
                "coverage_type": generation["coverage_type"],
                "coverage_complete": generation["coverage_complete"],
            })
        return candidates


@contextmanager
def supplier_catalog_lock(supplier: str, *, lock_directory: str | Path = DEFAULT_LOCK_DIRECTORY):
    supplier = _validate_supplier(supplier)
    path = Path(lock_directory).expanduser().resolve() / f"glowup-scout-supplier-{supplier}.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
            return
        try:
            yield True
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def run_supplier_sync(
    supplier: str,
    collector: Callable[..., SupplierCatalogGeneration],
    *, store: SupplierCatalogStore | None = None, limit: int | None = None,
    dry_run: bool = False, lock_directory: str | Path = DEFAULT_LOCK_DIRECTORY,
    promote: bool = True,
) -> dict[str, Any]:
    supplier = _validate_supplier(supplier)
    if limit is not None and (isinstance(limit, bool) or int(limit) <= 0):
        raise ValueError("limit must be a positive integer")
    store = store or SupplierCatalogStore()
    started = time.monotonic()
    with supplier_catalog_lock(supplier, lock_directory=lock_directory) as acquired:
        if not acquired:
            return {"status": "skipped", "reason": "already_running", "supplier": supplier}
        if dry_run:
            generation = collector(run_id=None, limit=limit, dry_run=True)
            return {
                "status": "dry_run", "supplier": supplier,
                "products": len(generation.products),
                "scenarios": len(generation.scenarios),
                "coverage_type": generation.coverage_type,
                "coverage_complete": generation.coverage_complete,
                "elapsed_seconds": time.monotonic() - started,
                "diagnostics": generation.diagnostics,
            }
        sampled = limit is not None
        coverage = getattr(collector, "coverage", {})
        run_id = store.start_run(
            supplier,
            coverage_type=str(coverage.get("type") or "unknown"),
            coverage_description=str(coverage.get("description") or "Coverage pending collection"),
            coverage_complete=bool(coverage.get("complete")), sampled=sampled,
        )
        try:
            generation = collector(run_id=run_id, limit=limit, dry_run=False)
            previous = store.latest_success(supplier)
            gate = supplier_promotion_gate(supplier, generation, sampled=sampled)
            generation = replace(
                generation,
                diagnostics={
                    **generation.diagnostics,
                    "generation_delta": supplier_generation_delta(previous, generation),
                    "promotion_gate": gate,
                },
            )
            store.publish(
                run_id, generation, elapsed_seconds=time.monotonic() - started,
                promote=bool(gate["passed"] and promote),
            )
            return {
                "run_id": run_id,
                "status": "sample_success" if sampled else "success",
                "supplier": supplier, "products": len(generation.products),
                "scenarios": len(generation.scenarios),
                "coverage_type": generation.coverage_type,
                "coverage_complete": generation.coverage_complete,
                "promoted": bool(gate["passed"] and promote),
                "promotion_authorized": bool(gate["passed"]),
                "elapsed_seconds": time.monotonic() - started,
                "diagnostics": generation.diagnostics,
            }
        except Exception as exc:
            failure_diagnostics = dict(getattr(exc, "diagnostics", {}) or {})
            store.fail(
                run_id, error_code=getattr(exc, "code", "sync_failed"),
                error_message=str(exc), elapsed_seconds=time.monotonic() - started,
                diagnostics=failure_diagnostics,
            )
            return {
                "run_id": run_id, "status": "failed", "supplier": supplier,
                "error_code": getattr(exc, "code", "sync_failed"),
                "elapsed_seconds": time.monotonic() - started,
                "diagnostics": failure_diagnostics,
            }


def supplier_promotion_gate(
    supplier: str, generation: SupplierCatalogGeneration, *, sampled: bool = False,
) -> dict[str, Any]:
    """Return an explicit, supplier-aware quality gate for active baselines."""
    supplier = _validate_supplier(supplier)
    reasons = []
    if sampled:
        reasons.append("sample_generation")
    if not generation.products:
        reasons.append("empty_product_catalog")
    if not generation.scenarios:
        reasons.append("no_purchase_scenarios")
    diagnostics = generation.diagnostics or {}
    if supplier == "qogita":
        reasons.append("qogita_scenario_baseline_not_authorized")
    elif supplier == "abw":
        if generation.completeness_status != "full_relevant_catalog":
            reasons.append("abw_beauty_enumeration_not_complete")
        if generation.source_count != generation.enumerated_count:
            reasons.append("abw_source_enumeration_mismatch")
    elif supplier == "umma":
        source = int(diagnostics.get("search_total_count") or generation.source_count or 0)
        unique = int(diagnostics.get("unique_product_ids") or generation.unique_count or 0)
        gap = int(diagnostics.get("enumeration_gap") or max(0, source - unique))
        if source <= 0 or unique <= 0 or source - unique != gap:
            reasons.append("umma_enumeration_proof_incoherent")
        if generation.completeness_status != "partial_catalog":
            reasons.append("umma_coverage_must_remain_partial")
    elif supplier == "qudo":
        if generation.completeness_status not in FULL_COMPLETENESS_STATUSES:
            reasons.append("qudo_relevant_enumeration_not_complete")
        if int(diagnostics.get("global_catalog_total") or generation.source_count or 0) <= 0:
            reasons.append("qudo_global_index_count_missing")
        if int(diagnostics.get("qudo_offer_products") or generation.enumerated_count or 0) <= 0:
            reasons.append("qudo_offer_universe_empty")
        global_total = int(diagnostics.get("global_catalog_total") or generation.source_count or 0)
        qudo_products = int(
            diagnostics.get("qudo_offer_products") or generation.enumerated_count or 0
        )
        valid_gtin_products = int(
            diagnostics.get("canonical_gtin_products")
            if "canonical_gtin_products" in diagnostics else sum(
                1 for row in generation.products
                if row.get("canonical_gtin") or canonical_gtin14(row.get("canonical_ean"))
            )
        )
        normalizer = diagnostics.get("normalizer") or {}
        normalized_scenarios = int(
            normalizer.get("qudo_scenarios")
            if "qudo_scenarios" in normalizer else len(generation.scenarios)
        )
        # Historical full audits established roughly 6.878 QUDO offers among
        # 7.023 global products and 6.805 usable identifiers. These loose
        # ratio guards detect truncation without pinning a permanent row count.
        if global_total and qudo_products / global_total < 0.90:
            reasons.append("qudo_offer_coverage_anomalous")
        if qudo_products and valid_gtin_products / qudo_products < 0.90:
            reasons.append("qudo_identifier_coverage_anomalous")
        if len(generation.products) != qudo_products:
            reasons.append("qudo_persisted_product_identity_mismatch")
        if normalized_scenarios != len(generation.scenarios):
            reasons.append("qudo_persisted_scenario_count_mismatch")
    return {"passed": not reasons, "reasons": reasons}


def candidates_to_cache_records(candidates: list[dict]) -> tuple[tuple[dict, ...], tuple[dict, ...]]:
    products: dict[str, dict] = {}
    scenarios: dict[str, dict] = {}
    for candidate in candidates or []:
        for scenario in candidate.get("scenarios") or []:
            raw_identifiers = []
            raw = scenario.get("supplier_barcode_raw") or scenario.get("canonical_ean")
            if raw and raw not in [row.get("value") for row in raw_identifiers]:
                raw_identifiers.append({
                    "value": str(raw),
                    "type": scenario.get("identifier_type"),
                })
            payload = dict(scenario)
            key = supplier_product_cache_key(
                payload.get("supplier"), payload.get("supplier_product_id"),
                supplier_option_id=_scenario_supplier_option_id(payload),
                supplier_sku=payload.get("supplier_sku"),
                fallback_identifier=payload.get("canonical_ean"),
            )
            scenarios[payload["scenario_id"]] = {
                "scenario_id": payload["scenario_id"],
                "canonical_product_key": key,
                "canonical_ean": payload.get("canonical_ean"),
                "raw_identifier": payload.get("supplier_barcode_raw") or payload.get("canonical_ean"),
                "raw_identifier_type": payload.get("identifier_type"),
                "supplier_product_id": payload.get("supplier_product_id"),
                "supplier_offer_id": payload.get("supplier_offer_id"),
                "supplier_sku": payload.get("supplier_sku"),
                "scenario_type": payload.get("scenario_type"),
                "scenario_label": payload.get("scenario_label"),
                "price": payload.get("cost_net_unit_eur"),
                "currency": "EUR",
                "stock": payload.get("stock"),
                "minimum_quantity": payload.get("minimum_product_quantity"),
                "maximum_quantity": payload.get("maximum_product_quantity"),
                "selling_unit": payload.get("selling_unit"),
                "account_mov": payload.get("account_mov"),
                "account_mov_currency": payload.get("account_mov_currency"),
                "warehouse": payload.get("warehouse"),
                "shipping_mode": payload.get("shipping_mode"),
                "availability_status": payload.get("availability_status"),
                "lead_time": payload.get("lead_time"),
                "payload": payload,
            }
            products[key] = {
                "canonical_product_key": key,
                "canonical_ean": candidate.get("canonical_ean"),
                "canonical_gtin": canonical_gtin14(candidate.get("canonical_ean")),
                "identifier_type": candidate.get("identifier_type"),
                "raw_identifiers": raw_identifiers,
                "supplier_product_id": payload.get("supplier_product_id"),
                "supplier_option_id": _scenario_supplier_option_id(payload),
                "supplier_sku": payload.get("supplier_sku"),
                "brand": candidate.get("brand"),
                "title": candidate.get("title"),
                "size_value": candidate.get("volume_value"),
                "size_unit": candidate.get("volume_unit"),
                "pack_count": candidate.get("pack_count"),
                "metadata": {
                    "category": candidate.get("category"),
                    "image_url": candidate.get("image_url"),
                },
            }
    return tuple(products.values()), tuple(scenarios.values())


def generation_is_fresh(generation: dict | None, ttl_hours: int, *, now=None) -> bool:
    """Evaluate freshness from the immutable generation completion time."""
    if not generation or generation.get("status") != "success":
        return False
    raw = generation.get("completed_at")
    try:
        completed = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return False
    if completed.tzinfo is None:
        completed = completed.replace(tzinfo=timezone.utc)
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return (current.astimezone(timezone.utc) - completed.astimezone(timezone.utc)).total_seconds() <= ttl_hours * 3600
