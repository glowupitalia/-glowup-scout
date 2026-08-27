"""Read-only Qudo cache adapter producing supplier-neutral scenarios."""

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


SUPPLIER = "qudo"
SUPPLIER_ALIAS = "qudo_public_catalog"
SCENARIO_TYPE = "qudo_standard"
VAT_RATE = Decimal("0.22")
QUDO_TTL_HOURS = 48
GTIN_PATTERN = re.compile(r"^(?:\d{8}|\d{12}|\d{13}|\d{14})$")


class QudoCacheError(RuntimeError):
    pass


QUDO_CACHE_SQL = """
WITH latest_success AS (
    SELECT run_id, gtin, seller_sku, observed_at,
           ROW_NUMBER() OVER (
               PARTITION BY gtin ORDER BY observed_at DESC, started_at DESC
           ) AS position
    FROM qudo_purchase_price_runs
    WHERE status = 'success' AND observed_at IS NOT NULL
), latest_attempt AS (
    SELECT gtin, status AS latest_attempt_status,
           started_at AS latest_attempt_at,
           ROW_NUMBER() OVER (
               PARTITION BY gtin ORDER BY started_at DESC
           ) AS position
    FROM qudo_purchase_price_runs
)
SELECT s.run_id, s.seller_sku, s.gtin, s.supplier,
       s.supplier_product_id, s.supplier_offer_id, s.supplier_sku,
       s.product_name, s.observed_at, s.currency, s.unit_price,
       s.price_basis, s.pricing_scope, s.available_quantity,
       s.availability_status, s.minimum_product_quantity, s.selling_unit,
       s.minimum_order_value, s.minimum_order_currency, s.product_url,
       s.source, a.latest_attempt_status, a.latest_attempt_at
FROM latest_success l
JOIN qudo_purchase_price_snapshots s ON s.run_id = l.run_id
LEFT JOIN latest_attempt a ON a.gtin = l.gtin AND a.position = 1
WHERE l.position = 1
ORDER BY s.gtin
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


def _parse_timestamp(value):
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def snapshot_is_stale(value, *, now=None, hours=QUDO_TTL_HOURS):
    observed = _parse_timestamp(value)
    if observed is None:
        return True
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    # Manager uses age > STALE_AFTER; exactly 48 hours remains fresh.
    return (current.astimezone(timezone.utc) - observed).total_seconds() > hours * 3600


def _identifier_type(value):
    return {8: "GTIN-8", 12: "UPC", 13: "EAN", 14: "GTIN"}.get(len(value), "")


def valid_qudo_identifier(value):
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
        raise QudoCacheError("psql non disponibile per la cache Manager")
    parsed = urllib.parse.urlparse(database_url)
    if parsed.scheme not in {"postgres", "postgresql"} or not parsed.hostname:
        raise QudoCacheError("DATABASE_URL Manager non valida")
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
        [executable, "-X", "--csv", "-v", "ON_ERROR_STOP=1", "-c", QUDO_CACHE_SQL],
        check=False, capture_output=True, text=True, env=environment, timeout=30,
    )
    if completed.returncode:
        raise QudoCacheError("lettura cache Qudo Manager non riuscita")
    return list(csv.DictReader(io.StringIO(completed.stdout)))


def _sqlite_rows(database_path):
    absolute = Path(database_path).expanduser().resolve()
    connection = sqlite3.connect(f"file:{absolute}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        return [dict(row) for row in connection.execute(QUDO_CACHE_SQL).fetchall()]
    finally:
        connection.close()


def load_qudo_cache_rows(manager_root=None):
    """Read latest successful Qudo snapshots without refresh or DB writes."""
    root = Path(manager_root or Path(__file__).resolve().parent.parent / "Glow-Up-Manager")
    settings = _manager_settings(root)
    database_url = settings.get("DATABASE_URL")
    if database_url:
        return _postgres_rows(database_url)
    database_path = settings.get("GLOWUP_DB_PATH") or root / "data/glow_up_manager.db"
    return _sqlite_rows(database_path)


def _scenario_identity(row, *, ean, account_mov):
    supplier_sku = str(row.get("supplier_sku") or "").strip()
    return scenario_key(
        supplier=SUPPLIER, supplier_alias=SUPPLIER_ALIAS,
        supplier_product_id=str(row.get("supplier_product_id") or ""),
        supplier_offer_id=str(row.get("supplier_offer_id") or ""),
        variant_id=supplier_sku, canonical_ean=ean,
        scenario_type=SCENARIO_TYPE, account_mov=account_mov,
    )


def normalize_qudo_candidates(rows, *, now=None):
    """Create one Qudo scenario per valid, fresh, purchasable GTIN."""
    source_rows = [dict(row) for row in (rows or [])]
    diagnostics = {
        "qudo_products_source": len({str(row.get("gtin") or "").strip() for row in source_rows}),
        "qudo_products": 0, "qudo_scenarios": 0,
        "qudo_stale_scenarios": 0, "qudo_unavailable_scenarios": 0,
        "invalid_identifier": 0, "supplier_products_total": 0,
        "supplier_scenarios_total": 0, "rejected_scenarios": [],
    }
    # The SQL loader already returns one latest success per GTIN. Fixtures may
    # include history, so retain only the latest snapshot deterministically.
    grouped = {}
    for row in source_rows:
        ean = str(row.get("gtin") or "").strip()
        if not valid_qudo_identifier(ean):
            diagnostics["invalid_identifier"] += 1
            diagnostics["rejected_scenarios"].append({
                "canonical_ean": ean, "reason": "invalid_identifier",
            })
            continue
        identity_sku = str(row.get("supplier_sku") or "").strip()
        if identity_sku.casefold().startswith("qudo-"):
            identity_sku = identity_sku[5:]
        identity = (
            ean,
            str(row.get("supplier_product_id") or "").strip(),
            str(row.get("supplier_offer_id") or "").strip(),
            identity_sku,
        )
        current = grouped.get(identity)
        if current is None or (
            str(row.get("observed_at") or ""), str(row.get("run_id") or "")
        ) > (
            str(current.get("observed_at") or ""), str(current.get("run_id") or "")
        ):
            grouped[identity] = row

    candidates = []
    for identity, row in grouped.items():
        ean = identity[0]
        rejection = None
        net = _decimal(row.get("unit_price"), positive=True)
        currency = str(row.get("currency") or "").strip().upper()
        price_basis = str(row.get("price_basis") or "").strip().casefold()
        account_mov = _decimal(row.get("minimum_order_value"), positive=True)
        account_currency = str(row.get("minimum_order_currency") or "").strip().upper()
        stock = _integer(row.get("available_quantity"), positive=True)
        minimum = _integer(row.get("minimum_product_quantity"), positive=True)
        selling_unit = _integer(row.get("selling_unit"), positive=True)
        availability = str(row.get("availability_status") or "").strip().casefold()
        if net is None or currency != "EUR" or price_basis != "net_ex_vat":
            rejection = "invalid_price_or_currency"
        elif account_mov is None or account_currency != "EUR":
            rejection = "invalid_account_mov"
        elif availability != "in_stock" or stock is None:
            rejection = "unavailable"
            diagnostics["qudo_unavailable_scenarios"] += 1
        elif minimum is None or selling_unit is None:
            rejection = "invalid_quantity_requirement"
        elif stock < minimum or stock < selling_unit:
            rejection = "insufficient_stock"
            diagnostics["qudo_unavailable_scenarios"] += 1

        observed = _parse_timestamp(row.get("observed_at"))
        latest_attempt = _parse_timestamp(row.get("latest_attempt_at"))
        later_failure = (
            str(row.get("latest_attempt_status") or "success") != "success"
            and latest_attempt is not None and observed is not None
            and latest_attempt > observed
        )
        freshness = (
            "stale" if snapshot_is_stale(row.get("observed_at"), now=now) or later_failure
            else "fresh"
        )
        if freshness == "stale":
            rejection = rejection or "stale_snapshot"
            diagnostics["qudo_stale_scenarios"] += 1

        product_id = str(row.get("supplier_product_id") or "").strip()
        offer_id = str(row.get("supplier_offer_id") or "").strip()
        supplier_sku = str(row.get("supplier_sku") or "").strip()
        title = str(row.get("product_name") or "").strip()
        if not product_id or not offer_id or not supplier_sku or not title:
            rejection = rejection or "missing_supplier_identity"
        if rejection:
            diagnostics["rejected_scenarios"].append({
                "canonical_ean": ean, "supplier_product_id": product_id,
                "supplier_offer_id": offer_id,
                "snapshot_id": str(row.get("run_id") or ""),
                "snapshot_at": str(row.get("observed_at") or ""),
                "freshness_status": freshness, "reason": rejection,
            })
            continue

        with localcontext() as context:
            context.prec = 40
            vat = net * VAT_RATE
            gross = net + vat
        product_url = str(row.get("product_url") or "").strip() or None
        scenario = PurchaseScenario(
            scenario_id=_scenario_identity(row, ean=ean, account_mov=account_mov),
            product_key=product_key(ean), canonical_ean=ean,
            identifier_type=_identifier_type(ean), supplier=SUPPLIER,
            supplier_alias=SUPPLIER_ALIAS,
            supplier_product_id=product_id, supplier_offer_id=offer_id,
            variant_id=supplier_sku,
            brand=str(row.get("brand") or "").strip(), title=title,
            scenario_type=SCENARIO_TYPE, scenario_label="Qudo",
            scenario_order=1, account_mov=account_mov,
            account_mov_currency="EUR", account_mov_eur=account_mov,
            selling_unit=selling_unit, cost_net_unit_eur=net,
            vat_rate=VAT_RATE, vat_amount_unit=vat,
            cost_gross_unit_eur=gross, stock=stock,
            snapshot_id=str(row.get("run_id") or ""),
            snapshot_at=str(row.get("observed_at") or ""),
            freshness_status=freshness, tier_is_active=True,
            supplier_barcode_raw=ean,
            minimum_product_quantity=minimum,
            maximum_product_quantity=stock,
            shipping_mode="standard", availability_status=availability,
            source_net_unit_price=net, source_currency="EUR",
            supplier_sku=supplier_sku, product_url=product_url,
            source_metadata={
                "manager_seller_sku": str(row.get("seller_sku") or ""),
                "supplier_sku": supplier_sku,
                "product_url": product_url,
                "price_basis": price_basis,
                "pricing_scope": str(row.get("pricing_scope") or ""),
                "source": str(row.get("source") or ""),
                "identifier_source": str(row.get("identifier_source") or ""),
                "stock_source": "add_to_cart.maximum",
                "cost_scope": "merchandise_gross_excluding_landed_costs",
            },
        ).to_dict()
        product = ProductCandidate(
            product_key=product_key(ean), canonical_ean=ean,
            identifier_type=scenario["identifier_type"], brand=scenario["brand"],
            title=title, category="", image_url="", scenarios=tuple(),
        ).to_dict()
        product["scenarios"] = [scenario]
        product.update({"gtin": ean, "supplier": "Qudo"})
        candidates.append(product)

    diagnostics["qudo_products"] = len(candidates)
    diagnostics["qudo_scenarios"] = len(candidates)
    diagnostics["supplier_products_total"] = len(candidates)
    diagnostics["supplier_scenarios_total"] = len(candidates)
    return sorted(candidates, key=lambda row: row["canonical_ean"]), diagnostics
