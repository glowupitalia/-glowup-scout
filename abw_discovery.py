"""Read-only ABW cache adapter producing supplier-neutral purchase scenarios."""

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

from purchase_scenarios import ProductCandidate, PurchaseScenario, product_key, scenario_key


SUPPLIER = "abw"
SUPPLIER_ALIAS = "abw_authenticated_buyer"
VAT_RATE = Decimal("0.22")
ABW_TTL_HOURS = 48
ACCOUNT_MOV = Decimal("250")
ACCOUNT_MOV_CURRENCY = "USD"
GTIN_PATTERN = re.compile(r"^(?:\d{8}|\d{12}|\d{13}|\d{14})$")


class AbwCacheError(RuntimeError):
    pass


ABW_CACHE_SQL = """
WITH latest_generation AS (
    SELECT run_id, gtin, MAX(observed_at) AS observed_at,
           ROW_NUMBER() OVER (
               PARTITION BY gtin ORDER BY MAX(observed_at) DESC, run_id DESC
           ) AS position
    FROM abw_purchase_price_snapshots
    WHERE is_valid = 1
    GROUP BY run_id, gtin
), latest_attempt AS (
    SELECT p.gtin, p.status AS latest_attempt_status,
           p.observed_at AS latest_attempt_at, r.status AS latest_run_status,
           ROW_NUMBER() OVER (
               PARTITION BY p.gtin ORDER BY p.observed_at DESC, p.run_id DESC
           ) AS position
    FROM abw_purchase_price_results p
    JOIN abw_purchase_price_runs r ON r.run_id = p.run_id
)
SELECT s.run_id, s.seller_sku, s.gtin, s.supplier_product_id,
       s.option_product_id, s.product_name, s.brand, s.mode,
       s.condition_key, s.condition_label, s.tier_min_quantity,
       s.tier_max_quantity, s.pack_size, s.pack_price,
       s.net_unit_price_eur, s.currency, s.price_source, s.price_basis,
       s.vat_rate, s.vat_amount, s.gross_unit_price,
       s.available_quantity, s.availability_status, s.stock_text,
       s.lead_time, s.warehouse, s.discount_label, s.product_url,
       s.minimum_order_value, s.minimum_order_currency, s.observed_at,
       s.source, a.latest_attempt_status, a.latest_attempt_at,
       a.latest_run_status
FROM latest_generation l
JOIN abw_purchase_price_snapshots s
  ON s.run_id = l.run_id AND s.gtin = l.gtin AND s.is_valid = 1
LEFT JOIN latest_attempt a ON a.gtin = l.gtin AND a.position = 1
WHERE l.position = 1
ORDER BY s.gtin, s.mode, s.tier_min_quantity, s.condition_key, s.warehouse
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
    return parsed if not positive or parsed > 0 else None


def _truthy(value):
    if isinstance(value, bool):
        return value
    return str(value or "").strip().casefold() in {"1", "true", "t", "yes"}


def _parse_timestamp(value):
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def snapshot_is_stale(value, *, now=None, hours=ABW_TTL_HOURS):
    observed = _parse_timestamp(value)
    if observed is None:
        return True
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    # Manager uses age > STALE_AFTER: exactly 48 hours is still fresh.
    return (current.astimezone(timezone.utc) - observed).total_seconds() > hours * 3600


def _identifier_type(value):
    return {8: "GTIN-8", 12: "UPC", 13: "EAN", 14: "GTIN"}.get(len(value), "")


def valid_abw_identifier(value):
    """Validate supported GS1 identifiers, including their check digit."""
    text = str(value or "").strip()
    if not GTIN_PATTERN.fullmatch(text):
        return False
    digits = [int(character) for character in text]
    expected = (10 - sum(
        digit * (3 if (len(digits) - index) % 2 == 0 else 1)
        for index, digit in enumerate(digits[:-1])
    ) % 10) % 10
    return digits[-1] == expected


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
        raise AbwCacheError("psql non disponibile per la cache Manager")
    parsed = urllib.parse.urlparse(database_url)
    if parsed.scheme not in {"postgres", "postgresql"} or not parsed.hostname:
        raise AbwCacheError("DATABASE_URL Manager non valida")
    environment = dict(os.environ)
    environment.update({
        "PGHOST": parsed.hostname,
        "PGPORT": str(parsed.port or 5432),
        "PGUSER": urllib.parse.unquote(parsed.username or ""),
        "PGPASSWORD": urllib.parse.unquote(parsed.password or ""),
        "PGDATABASE": urllib.parse.unquote(parsed.path.lstrip("/")),
    })
    existing = environment.get("PGOPTIONS", "").strip()
    environment["PGOPTIONS"] = (
        f"{existing} -c default_transaction_read_only=on"
    ).strip()
    completed = subprocess.run(
        [executable, "-X", "--csv", "-v", "ON_ERROR_STOP=1", "-c", ABW_CACHE_SQL],
        check=False, capture_output=True, text=True, env=environment, timeout=30,
    )
    if completed.returncode:
        raise AbwCacheError("lettura cache ABW Manager non riuscita")
    return list(csv.DictReader(io.StringIO(completed.stdout)))


def _sqlite_rows(database_path):
    absolute = Path(database_path).expanduser().resolve()
    connection = sqlite3.connect(f"file:{absolute}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        return [dict(row) for row in connection.execute(ABW_CACHE_SQL).fetchall()]
    finally:
        connection.close()


def load_abw_cache_rows(manager_root=None):
    """Read the latest valid ABW generations without refresh or DB writes."""
    root = Path(manager_root or Path(__file__).resolve().parent.parent / "Glow-Up-Manager")
    settings = _manager_settings(root)
    database_url = settings.get("DATABASE_URL")
    if database_url:
        return _postgres_rows(database_url)
    database_path = settings.get("GLOWUP_DB_PATH") or root / "data/glow_up_manager.db"
    return _sqlite_rows(database_path)


def _availability_is_usable(row):
    if "is_sellable" in row and not _truthy(row.get("is_sellable")):
        return False
    if _truthy(row.get("out_of_stock")):
        return False
    numeric_stock = _integer(row.get("available_quantity"))
    if numeric_stock is not None and numeric_stock <= 0:
        return False
    status = str(row.get("availability_status") or "").strip().casefold()
    return status in {"available", "in_stock", "available_to_order"}


def _scenario_identity(row, *, ean, scenario_type, account_mov):
    condition = str(row.get("condition_key") or "").strip()
    warehouse = str(row.get("warehouse") or "core").strip().casefold()
    return scenario_key(
        supplier=SUPPLIER, supplier_alias=SUPPLIER_ALIAS,
        supplier_product_id=str(row.get("supplier_product_id") or ""),
        supplier_offer_id=str(row.get("option_product_id") or ""),
        variant_id=f"{condition}|{warehouse}", canonical_ean=ean,
        scenario_type=scenario_type, account_mov=account_mov,
    )


def normalize_abw_candidates(rows, *, now=None):
    """Create one candidate per GTIN and one scenario per valid ABW condition."""
    source_rows = [dict(row) for row in (rows or [])]
    diagnostics = {
        "abw_products_source": len({str(row.get("gtin") or "").strip() for row in source_rows}),
        "abw_products": 0, "abw_scenarios": 0,
        "abw_standard_scenarios": 0, "abw_bulk_box_scenarios": 0,
        "abw_stale_scenarios": 0, "abw_unavailable_scenarios": 0,
        "invalid_identifier": 0, "supplier_products_total": 0,
        "supplier_scenarios_total": 0, "rejected_scenarios": [],
    }
    grouped = {}
    for row in source_rows:
        ean = str(row.get("gtin") or "").strip()
        if not valid_abw_identifier(ean):
            diagnostics["invalid_identifier"] += 1
            diagnostics["rejected_scenarios"].append({
                "canonical_ean": ean, "condition_key": row.get("condition_key"),
                "reason": "invalid_identifier",
            })
            continue
        grouped.setdefault(ean, []).append(row)

    candidates = []
    for ean, product_rows in grouped.items():
        # The loader already selects one generation; fixtures may include history.
        latest_snapshot = max(str(row.get("observed_at") or "") for row in product_rows)
        generation_rows = [
            row for row in product_rows
            if str(row.get("observed_at") or "") == latest_snapshot
        ]
        scenarios = []
        for row in generation_rows:
            mode = str(row.get("mode") or "").strip().casefold()
            scenario_type = {
                "standard": "abw_standard", "bulk_box": "abw_bulk_box",
            }.get(mode)
            rejection = None
            if not scenario_type:
                rejection = "unsupported_mode"
            if not _availability_is_usable(row):
                rejection = rejection or "unavailable"
                diagnostics["abw_unavailable_scenarios"] += 1
            minimum = _integer(row.get("tier_min_quantity"), positive=True)
            maximum = _integer(row.get("tier_max_quantity"), positive=True)
            bundle = _integer(row.get("pack_size"), positive=True)
            pack_total = _decimal(row.get("pack_price"), positive=True)
            persisted_net = _decimal(row.get("net_unit_price_eur"), positive=True)
            if mode == "bulk_box":
                if bundle is None or pack_total is None:
                    rejection = rejection or "invalid_bulk_box"
                    net = None
                else:
                    with localcontext() as context:
                        context.prec = 40
                        net = pack_total / Decimal(bundle)
                minimum = bundle
            else:
                net = persisted_net
                if minimum is None:
                    rejection = rejection or "invalid_standard_tier"
            currency = str(row.get("currency") or "").upper()
            account_mov = _decimal(row.get("minimum_order_value"), positive=True)
            account_currency = str(row.get("minimum_order_currency") or "").upper()
            if net is None or currency != "EUR" or account_mov is None or account_currency != "USD":
                rejection = rejection or "invalid_price_or_currency"
            observed = _parse_timestamp(row.get("observed_at"))
            latest_attempt = _parse_timestamp(row.get("latest_attempt_at"))
            later_failure = (
                str(row.get("latest_attempt_status") or "found") != "found"
                and latest_attempt is not None and observed is not None
                and latest_attempt > observed
            ) or (
                str(row.get("latest_run_status") or "success") == "failed"
                and latest_attempt is not None and observed is not None
                and latest_attempt > observed
            )
            freshness = (
                "stale" if snapshot_is_stale(row.get("observed_at"), now=now) or later_failure
                else "fresh"
            )
            if freshness == "stale":
                rejection = rejection or "stale_snapshot"
                diagnostics["abw_stale_scenarios"] += 1
            product_id = str(row.get("supplier_product_id") or "").strip()
            option_id = str(row.get("option_product_id") or "").strip()
            condition_key = str(row.get("condition_key") or "").strip()
            if not product_id or not option_id or not condition_key:
                rejection = rejection or "missing_supplier_identity"
            if rejection:
                diagnostics["rejected_scenarios"].append({
                    "canonical_ean": ean, "mode": mode,
                    "condition_key": condition_key,
                    "snapshot_id": str(row.get("run_id") or ""),
                    "snapshot_at": str(row.get("observed_at") or ""),
                    "freshness_status": freshness, "reason": rejection,
                })
                continue

            with localcontext() as context:
                context.prec = 40
                vat = net * VAT_RATE
                gross = net + vat
            scenario = PurchaseScenario(
                scenario_id=_scenario_identity(
                    row, ean=ean, scenario_type=scenario_type,
                    account_mov=account_mov,
                ),
                product_key=product_key(ean), canonical_ean=ean,
                identifier_type=_identifier_type(ean), supplier=SUPPLIER,
                supplier_alias=SUPPLIER_ALIAS,
                supplier_product_id=product_id,
                supplier_offer_id=option_id, variant_id=condition_key,
                brand=str(row.get("brand") or "").strip(),
                title=str(row.get("product_name") or "").strip(),
                scenario_type=scenario_type,
                scenario_label=str(row.get("condition_label") or "").strip()
                or (f"Box {bundle}" if mode == "bulk_box" else condition_key),
                scenario_order=(1 if mode == "standard" else 2) * 1_000 + (minimum or 0),
                account_mov=account_mov, account_mov_currency="USD",
                account_mov_eur=None,
                selling_unit=bundle if mode == "bulk_box" else None,
                cost_net_unit_eur=net, vat_rate=VAT_RATE,
                vat_amount_unit=vat, cost_gross_unit_eur=gross,
                stock=_integer(row.get("available_quantity")),
                snapshot_id=str(row.get("run_id") or ""),
                snapshot_at=str(row.get("observed_at") or ""),
                freshness_status=freshness, tier_is_active=True,
                minimum_product_quantity=minimum,
                maximum_product_quantity=maximum,
                warehouse=str(row.get("warehouse") or "core"),
                shipping_mode=mode,
                lead_time=str(row.get("lead_time") or "") or None,
                availability_status=str(row.get("availability_status") or ""),
                availability_text=str(row.get("stock_text") or "") or None,
                condition_key=condition_key, bundle_quantity=bundle,
                source_pack_total_price=pack_total,
                source_net_unit_price=net, source_currency="EUR",
                source_metadata={
                    "seller_sku": str(row.get("seller_sku") or ""),
                    "condition_key": condition_key,
                    "discount_label": str(row.get("discount_label") or ""),
                    "price_source": str(row.get("price_source") or ""),
                    "price_basis": str(row.get("price_basis") or ""),
                    "product_url": str(row.get("product_url") or ""),
                    "source": str(row.get("source") or ""),
                    "cost_scope": "merchandise_gross_excluding_landed_costs",
                },
            ).to_dict()
            scenarios.append(scenario)
            diagnostics[
                "abw_standard_scenarios" if mode == "standard"
                else "abw_bulk_box_scenarios"
            ] += 1
        if not scenarios:
            continue
        scenarios.sort(key=lambda row: (row["scenario_order"], row["scenario_id"]))
        first = scenarios[0]
        product = ProductCandidate(
            product_key=product_key(ean), canonical_ean=ean,
            identifier_type=first["identifier_type"], brand=first["brand"],
            title=first["title"], category="", image_url="", scenarios=tuple(),
        ).to_dict()
        product["scenarios"] = scenarios
        product.update({"gtin": ean, "supplier": "ABW"})
        candidates.append(product)
    diagnostics["abw_products"] = len(candidates)
    diagnostics["abw_scenarios"] = sum(len(row["scenarios"]) for row in candidates)
    diagnostics["supplier_products_total"] = diagnostics["abw_products"]
    diagnostics["supplier_scenarios_total"] = diagnostics["abw_scenarios"]
    return sorted(candidates, key=lambda row: row["canonical_ean"]), diagnostics
