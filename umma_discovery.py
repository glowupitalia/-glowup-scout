"""Read-only UMMA cache adapter producing supplier-neutral purchase scenarios."""

from __future__ import annotations

import csv
import io
import os
import re
import shutil
import sqlite3
import subprocess
import urllib.parse
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, localcontext
from pathlib import Path

from purchase_scenarios import (
    ProductCandidate,
    PurchaseScenario,
    product_key,
    scenario_key,
)


SUPPLIER = "umma"
SUPPLIER_ALIAS = "umma_authenticated_buyer"
VAT_RATE = Decimal("0.22")
UMMA_TTL_HOURS = 48
MODE_LABELS = {
    "u_quick": "U-Quick",
    "europe_direct": "Europe Direct",
    "standard": "Standard",
}
MODE_ORDER = {mode: index for index, mode in enumerate(MODE_LABELS, start=1)}
MODE_WAREHOUSE = {
    "u_quick": "rocket",
    "europe_direct": "europe",
    "standard": "standard",
}
GTIN_PATTERN = re.compile(r"^(?:\d{8}|\d{12}|\d{13}|\d{14})$")
EUROPE_DIRECT_BARCODE_PATTERN = re.compile(r"^(\d{13})ED$")


class UmmaCacheError(RuntimeError):
    pass


UMMA_CACHE_SQL = """
WITH latest_success AS (
    SELECT run_id, gtin, seller_sku, observed_at,
           ROW_NUMBER() OVER (
               PARTITION BY gtin ORDER BY observed_at DESC, started_at DESC
           ) AS position
    FROM umma_purchase_price_runs
    WHERE status = 'success' AND observed_at IS NOT NULL
), latest_attempt AS (
    SELECT gtin, status AS latest_attempt_status,
           started_at AS latest_attempt_at,
           ROW_NUMBER() OVER (
               PARTITION BY gtin ORDER BY started_at DESC
           ) AS position
    FROM umma_purchase_price_runs
)
SELECT s.run_id, s.seller_sku, s.gtin, s.supplier_product_id,
       s.mapper_sale_product_id, s.product_option_id, s.supplier_sku,
       s.product_name, s.sales_mode, s.observed_at, s.original_unit_price,
       s.original_currency, s.price_basis, s.price_basis_source,
       s.fx_usd_to_eur_rate, s.fx_reference_rate, s.fx_rate_date,
       s.fx_source, s.fx_stale, s.net_unit_price_eur,
       s.vat_rate_percent, s.vat_amount_eur, s.gross_unit_price_eur,
       s.available_quantity, s.availability_status,
       s.minimum_product_quantity, s.selling_unit, s.maximum_quantity,
       s.lead_time, s.minimum_order_value, s.minimum_order_currency,
       s.pricing_scope, s.source, a.latest_attempt_status,
       a.latest_attempt_at
FROM latest_success l
JOIN umma_purchase_price_snapshots s ON s.run_id = l.run_id
LEFT JOIN latest_attempt a ON a.gtin = l.gtin AND a.position = 1
WHERE l.position = 1
ORDER BY s.gtin, s.sales_mode
""".strip()


def _decimal(value, *, positive=False):
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if not parsed.is_finite() or (positive and parsed <= 0):
        return None
    return parsed


def _integer(value, *, positive=False):
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    if positive and parsed <= 0:
        return None
    return parsed


def _truthy(value):
    if isinstance(value, bool):
        return value
    return str(value or "").strip().casefold() in {"1", "true", "t", "yes"}


def _identifier_type(value):
    return {8: "GTIN-8", 12: "UPC", 13: "EAN", 14: "GTIN"}.get(len(value), "")


def valid_ean13(value):
    """Validate an EAN-13 check digit without coercing other identifiers."""
    text = str(value or "")
    if not re.fullmatch(r"\d{13}", text):
        return False
    weighted = sum(
        int(digit) * (1 if index % 2 == 0 else 3)
        for index, digit in enumerate(text[:12])
    )
    expected = (10 - weighted % 10) % 10
    return int(text[-1]) == expected


def normalize_umma_barcode(value, sales_mode):
    """Return canonical identifier data for one persisted UMMA mode.

    UMMA Europe Direct warehouse variants use the strictly documented
    ``EAN13ED`` supplier barcode. No other suffix is removed.
    """
    raw = str(value or "").strip()
    mode = str(sales_mode or "").strip().casefold()
    if mode == "europe_direct":
        match = EUROPE_DIRECT_BARCODE_PATTERN.fullmatch(raw)
        if match:
            canonical = match.group(1)
            if not valid_ean13(canonical):
                return None, None, raw, "invalid_ean13_check_digit"
            return canonical, "EAN", raw, None
    if GTIN_PATTERN.fullmatch(raw):
        return raw, _identifier_type(raw), raw, None
    return None, None, raw, "invalid_supplier_barcode_format"


def _scenario_type(mode):
    # Europe Direct is the canonical warehouse scenario name requested by the
    # confirmed UMMA barcode contract. Existing Standard/U-Quick identifiers
    # remain unchanged for checkpoint compatibility.
    return "europe_direct" if mode == "europe_direct" else f"umma_{mode}"


def _parse_timestamp(value):
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def snapshot_is_stale(value, *, now=None, hours=UMMA_TTL_HOURS):
    observed = _parse_timestamp(value)
    if observed is None:
        return True
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    # Manager uses `age > STALE_AFTER`: exactly 48h is still valid.
    return (current.astimezone(timezone.utc) - observed).total_seconds() > hours * 3600


def _parse_env_file(path):
    values = {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return values
    for line in lines:
        clean = line.strip()
        if not clean or clean.startswith("#") or "=" not in clean:
            continue
        key, value = clean.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def _manager_settings(manager_root):
    settings = dict(os.environ)
    manager_values = _parse_env_file(manager_root / ".env")
    for key in ("DATABASE_URL", "GLOWUP_DB_PATH"):
        if manager_values.get(key):
            settings[key] = manager_values[key]
    return settings


def _postgres_rows(database_url):
    executable = shutil.which("psql")
    if not executable:
        raise UmmaCacheError("psql non disponibile per la cache Manager")
    parsed = urllib.parse.urlparse(database_url)
    if parsed.scheme not in {"postgres", "postgresql"} or not parsed.hostname:
        raise UmmaCacheError("DATABASE_URL Manager non valida")
    environment = dict(os.environ)
    environment.update({
        "PGHOST": parsed.hostname,
        "PGPORT": str(parsed.port or 5432),
        "PGUSER": urllib.parse.unquote(parsed.username or ""),
        "PGPASSWORD": urllib.parse.unquote(parsed.password or ""),
        "PGDATABASE": urllib.parse.unquote(parsed.path.lstrip("/")),
    })
    environment["PGOPTIONS"] = (
        f"{environment.get('PGOPTIONS', '').strip()} "
        "-c default_transaction_read_only=on"
    ).strip()
    completed = subprocess.run(
        [executable, "-X", "--csv", "-v", "ON_ERROR_STOP=1", "-c", UMMA_CACHE_SQL],
        check=False, capture_output=True, text=True, env=environment, timeout=30,
    )
    if completed.returncode:
        raise UmmaCacheError("lettura cache UMMA Manager non riuscita")
    return list(csv.DictReader(io.StringIO(completed.stdout)))


def _sqlite_rows(database_path):
    absolute = Path(database_path).expanduser().resolve()
    connection = sqlite3.connect(f"file:{absolute}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        return [dict(row) for row in connection.execute(UMMA_CACHE_SQL).fetchall()]
    finally:
        connection.close()


def load_umma_cache_rows(manager_root=None):
    """Load the latest successful UMMA modes without refreshing or writing."""
    root = Path(
        manager_root or Path(__file__).resolve().parent.parent / "Glow-Up-Manager"
    )
    settings = _manager_settings(root)
    database_url = settings.get("DATABASE_URL")
    if database_url:
        return _postgres_rows(database_url)
    database_path = settings.get("GLOWUP_DB_PATH") or root / "data/glow_up_manager.db"
    return _sqlite_rows(database_path)


def _mode_is_available(row, mode):
    stock = _integer(row.get("available_quantity"))
    status = str(row.get("availability_status") or "").casefold()
    if mode in {"u_quick", "europe_direct"}:
        return stock is not None and stock > 0 and status == "in_stock"
    if mode == "standard":
        return status == "available_to_order" and stock is None
    return False


def _fx_status(row):
    raw_rate = row.get("fx_usd_to_eur_rate")
    if raw_rate in (None, "") or not row.get("fx_rate_date") or not row.get("fx_source"):
        return "missing"
    rate = _decimal(raw_rate, positive=True)
    if rate is None:
        return "invalid"
    if _truthy(row.get("fx_stale")):
        return "stale"
    return "valid"


def normalize_umma_candidates(rows, *, now=None):
    """Create one product per EAN and one scenario per usable persisted mode."""
    source_rows = list(rows or [])
    diagnostics = {
        "umma_products_source": len({
            str(row.get("gtin") or "").strip() for row in source_rows
        }),
        "umma_products": 0,
        "umma_scenarios": 0,
        "umma_stale_scenarios": 0,
        "umma_invalid_fx_scenarios": 0,
        "umma_unavailable_scenarios": 0,
        "invalid_identifier": 0,
        "supplier_products_total": 0,
        "supplier_scenarios_total": 0,
        "rejected_scenarios": [],
    }
    grouped = {}
    for source_row in source_rows:
        row = dict(source_row)
        mode = str(row.get("sales_mode") or "").casefold()
        canonical, identifier_type, raw, error = normalize_umma_barcode(
            row.get("gtin"), mode
        )
        if error:
            diagnostics["invalid_identifier"] += 1
            diagnostics["rejected_scenarios"].append({
                "canonical_ean": canonical,
                "supplier_barcode_raw": raw,
                "sales_mode": mode,
                "reason": error,
            })
            continue
        row["_canonical_ean"] = canonical
        row["_identifier_type"] = identifier_type
        row["_supplier_barcode_raw"] = raw
        grouped.setdefault(canonical, []).append(row)
    candidates = []
    for ean, product_rows in grouped.items():
        latest_by_mode = {}
        for row in product_rows:
            mode = str(row.get("sales_mode") or "").casefold()
            current = latest_by_mode.get(mode)
            if current is None or str(row.get("observed_at") or "") > str(
                current.get("observed_at") or ""
            ):
                latest_by_mode[mode] = row
        scenarios = []
        for mode in sorted(latest_by_mode, key=lambda value: MODE_ORDER.get(value, 99)):
            row = latest_by_mode[mode]
            rejection = None
            if mode not in MODE_LABELS or not _mode_is_available(row, mode):
                rejection = "mode_unavailable"
                diagnostics["umma_unavailable_scenarios"] += 1
            minimum = _integer(row.get("minimum_product_quantity"), positive=True)
            selling_unit = _integer(row.get("selling_unit"), positive=True)
            source_price = _decimal(row.get("original_unit_price"), positive=True)
            account_mov = _decimal(row.get("minimum_order_value"), positive=True)
            source_currency = str(row.get("original_currency") or "").upper()
            mov_currency = str(row.get("minimum_order_currency") or "").upper()
            if (
                minimum is None or selling_unit is None or source_price is None
                or account_mov is None or source_currency != "USD"
                or mov_currency != "USD"
            ):
                rejection = rejection or "invalid_quantity_price_or_currency"
            observed_at = _parse_timestamp(row.get("observed_at"))
            latest_attempt_at = _parse_timestamp(row.get("latest_attempt_at"))
            later_failure = (
                str(row.get("latest_attempt_status") or "success") != "success"
                and latest_attempt_at is not None
                and observed_at is not None
                and latest_attempt_at > observed_at
            )
            freshness = (
                "stale"
                if snapshot_is_stale(row.get("observed_at"), now=now) or later_failure
                else "fresh"
            )
            if freshness == "stale":
                rejection = rejection or "stale_snapshot"
                diagnostics["umma_stale_scenarios"] += 1
            fx_status = _fx_status(row)
            if fx_status != "valid":
                rejection = rejection or f"fx_{fx_status}"
                diagnostics["umma_invalid_fx_scenarios"] += 1
            if rejection:
                diagnostics["rejected_scenarios"].append({
                    "canonical_ean": ean, "sales_mode": mode,
                    "snapshot_id": str(row.get("run_id") or ""),
                    "snapshot_at": str(row.get("observed_at") or ""),
                    "freshness_status": freshness, "fx_status": fx_status,
                    "reason": rejection,
                })
                continue

            fx_rate = _decimal(row.get("fx_usd_to_eur_rate"), positive=True)
            with localcontext() as context:
                context.prec = 40
                net = source_price * fx_rate
                vat = net * VAT_RATE
                gross = net + vat
                account_mov_eur = account_mov * fx_rate
            product_id = str(row.get("supplier_product_id") or "")
            mapper_id = str(row.get("mapper_sale_product_id") or "")
            option_id = str(row.get("product_option_id") or "")
            if not product_id or not option_id:
                diagnostics["rejected_scenarios"].append({
                    "canonical_ean": ean, "sales_mode": mode,
                    "reason": "missing_supplier_identity",
                })
                continue
            scenario_type = _scenario_type(mode)
            scenario = PurchaseScenario(
                scenario_id=scenario_key(
                    supplier=SUPPLIER, supplier_alias=SUPPLIER_ALIAS,
                    supplier_product_id=product_id,
                    supplier_offer_id=mapper_id, variant_id=option_id,
                    canonical_ean=ean, scenario_type=scenario_type,
                    account_mov=account_mov,
                ),
                product_key=product_key(ean), canonical_ean=ean,
                identifier_type=_identifier_type(ean), supplier=SUPPLIER,
                supplier_alias=SUPPLIER_ALIAS,
                supplier_product_id=product_id,
                supplier_offer_id=mapper_id, variant_id=option_id,
                brand="", title=str(row.get("product_name") or "").strip(),
                scenario_type=scenario_type,
                scenario_label=MODE_LABELS[mode], scenario_order=MODE_ORDER[mode],
                account_mov=account_mov, account_mov_currency="USD",
                account_mov_eur=account_mov_eur, selling_unit=selling_unit,
                cost_net_unit_eur=net, vat_rate=VAT_RATE,
                vat_amount_unit=vat, cost_gross_unit_eur=gross,
                stock=_integer(row.get("available_quantity")),
                snapshot_id=str(row.get("run_id") or ""),
                snapshot_at=str(row.get("observed_at") or ""),
                freshness_status=freshness, tier_is_active=True,
                supplier_barcode_raw=row["_supplier_barcode_raw"],
                source_metadata={
                    "supplier_barcode_raw": row["_supplier_barcode_raw"],
                    "mapper_sale_product_id": mapper_id,
                    "supplier_sku": str(row.get("supplier_sku") or ""),
                    "price_basis": str(row.get("price_basis") or ""),
                    "price_basis_source": str(row.get("price_basis_source") or ""),
                    "pricing_scope": str(row.get("pricing_scope") or ""),
                    "source": str(row.get("source") or ""),
                },
                minimum_product_quantity=minimum,
                maximum_product_quantity=_integer(row.get("maximum_quantity")),
                warehouse=MODE_WAREHOUSE[mode], shipping_mode=mode,
                lead_time=str(row.get("lead_time") or "") or None,
                availability_status=str(row.get("availability_status") or ""),
                source_net_unit_price=source_price, source_currency="USD",
                fx_rate=fx_rate, fx_date=str(row.get("fx_rate_date") or ""),
                fx_source=str(row.get("fx_source") or ""), fx_status=fx_status,
            ).to_dict()
            scenarios.append(scenario)
        if not scenarios:
            continue
        first = min(scenarios, key=lambda row: (
            row["scenario_order"], row["scenario_id"]
        ))
        product = ProductCandidate(
            product_key=product_key(ean), canonical_ean=ean,
            identifier_type=first["identifier_type"], brand="",
            title=first["title"], category="", image_url="",
            scenarios=tuple(),
        ).to_dict()
        product["scenarios"] = scenarios
        product.update({"gtin": ean, "supplier": "UMMA"})
        candidates.append(product)
    diagnostics["umma_products"] = len(candidates)
    diagnostics["umma_scenarios"] = sum(
        len(candidate["scenarios"]) for candidate in candidates
    )
    diagnostics["supplier_products_total"] = diagnostics["umma_products"]
    diagnostics["supplier_scenarios_total"] = diagnostics["umma_scenarios"]
    return sorted(candidates, key=lambda row: row["canonical_ean"]), diagnostics
