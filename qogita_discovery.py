"""Read-only access to Glow-Up-Manager's persisted Qogita seller catalog."""

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
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path

from purchase_scenarios import (
    ProductCandidate,
    PurchaseScenario,
    product_key,
    scenario_key,
)


VAT_RATE = Decimal("0.22")
CENT = Decimal("0.01")
GTIN_PATTERN = re.compile(r"^(?:\d{8}|\d{12}|\d{13}|\d{14})$")


class QogitaCacheError(RuntimeError):
    pass


def _decimal(value, *, positive=False):
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if not parsed.is_finite() or (positive and parsed <= 0):
        return None
    return parsed


def valid_gtin(value):
    return bool(GTIN_PATTERN.fullmatch(str(value or "").strip()))


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


CATALOG_SQL = """
WITH latest AS (
    SELECT run_id, seller_alias, observed_at,
           ROW_NUMBER() OVER (
               PARTITION BY seller_alias ORDER BY observed_at DESC
           ) AS position
    FROM qogita_seller_catalog_runs
    WHERE status = 'success'
)
SELECT p.run_id, p.gtin, p.variant_fid, p.offer_qid, p.product_name, p.brand,
       p.category_name, p.image_url, p.inventory, p.selling_unit,
       p.product_url, p.observed_at, l.seller_alias,
       t.tier_mov, t.currency, t.tier_price, t.is_active
FROM latest l
JOIN qogita_seller_product_snapshots p ON p.run_id = l.run_id
LEFT JOIN qogita_seller_tier_snapshots t
       ON t.run_id = p.run_id AND t.offer_qid = p.offer_qid
WHERE l.position = 1
ORDER BY l.seller_alias, p.gtin, t.tier_mov
""".strip()


def _postgres_rows(database_url):
    executable = shutil.which("psql")
    if not executable:
        raise QogitaCacheError("psql non disponibile per la cache Manager")
    environment = dict(os.environ)
    parsed = urllib.parse.urlparse(database_url)
    if parsed.scheme not in {"postgres", "postgresql"} or not parsed.hostname:
        raise QogitaCacheError("DATABASE_URL Manager non valida")
    environment.update({
        "PGHOST": parsed.hostname,
        "PGPORT": str(parsed.port or 5432),
        "PGUSER": urllib.parse.unquote(parsed.username or ""),
        "PGPASSWORD": urllib.parse.unquote(parsed.password or ""),
        "PGDATABASE": urllib.parse.unquote(parsed.path.lstrip("/")),
    })
    existing_options = environment.get("PGOPTIONS", "").strip()
    environment["PGOPTIONS"] = (
        f"{existing_options} -c default_transaction_read_only=on"
    ).strip()
    completed = subprocess.run(
        [executable, "-X", "--csv", "-v", "ON_ERROR_STOP=1", "-c", CATALOG_SQL],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
        timeout=30,
    )
    if completed.returncode:
        raise QogitaCacheError("lettura cache Qogita Manager non riuscita")
    return list(csv.DictReader(io.StringIO(completed.stdout)))


def _sqlite_rows(database_path):
    absolute = Path(database_path).expanduser().resolve()
    connection = sqlite3.connect(f"file:{absolute}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        return [dict(row) for row in connection.execute(CATALOG_SQL).fetchall()]
    finally:
        connection.close()


def load_qogita_cache_rows(manager_root=None):
    """Load rows without invoking Manager jobs or opening a writable DB."""
    root = Path(manager_root or Path(__file__).resolve().parent.parent / "Glow-Up-Manager")
    settings = _manager_settings(root)
    database_url = settings.get("DATABASE_URL")
    if database_url:
        return _postgres_rows(database_url)
    database_path = settings.get("GLOWUP_DB_PATH") or root / "data/glow_up_manager.db"
    return _sqlite_rows(database_path)


def _is_active(value):
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "t", "yes"}


def _identifier_type(value):
    return {8: "GTIN-8", 12: "UPC", 13: "EAN", 14: "GTIN"}.get(len(value), "")


def _legacy_base_fields(product):
    """Keep the old read shape while the core consumes scenarios[]."""
    scenario = min(product["scenarios"], key=lambda row: (
        row["account_mov"], row["cost_gross_unit_eur"], row["scenario_id"]
    ))
    return {
        "seller_alias": scenario["supplier_alias"],
        "offer_qid": scenario["supplier_offer_id"],
        "stock": scenario["stock"],
        "selling_unit": scenario["selling_unit"],
        "mov": scenario["account_mov"],
        "tier_is_active": scenario["tier_is_active"],
        "currency": scenario["account_mov_currency"],
        "cost_net": scenario["cost_net_unit_eur"],
        "cost_vat": scenario["vat_amount_unit"],
        "cost_gross": scenario["cost_gross_unit_eur"],
        "snapshot_at": scenario["snapshot_at"],
        "snapshot_stale": scenario["freshness_status"] != "fresh",
    }


def normalize_qogita_candidates(rows, *, minimum_stock=1, now=None):
    """Produce one product with every valid Qogita MOV tier as a scenario."""
    grouped = {}
    for row in rows or []:
        gtin = str(row.get("gtin") or "").strip()
        grouped.setdefault(gtin, []).append(row)

    candidates = []
    diagnostics = {
        "initial": len({
            (str(row.get("seller_alias") or ""), str(row.get("offer_qid") or ""))
            for row in rows or []
        }),
        "valid_gtin": 0,
        "invalid_gtin": 0,
        "missing_price": 0,
        "below_stock": 0,
        "below_selling_unit": 0,
        "duplicates": 0,
        "stale_snapshot": 0,
        "qogita_products": 0,
        "qogita_scenarios": 0,
    }
    for gtin, product_rows in grouped.items():
        if not valid_gtin(gtin):
            diagnostics["invalid_gtin"] += 1
            continue
        diagnostics["valid_gtin"] += 1
        eligible_rows = []
        for row in product_rows:
            stock = int(row.get("inventory") or 0)
            selling_unit = int(row.get("selling_unit") or 1)
            price = _decimal(row.get("tier_price"), positive=True)
            mov = _decimal(row.get("tier_mov"), positive=True)
            currency = str(row.get("currency") or "").strip().upper()
            if stock < minimum_stock:
                continue
            if selling_unit < 1 or stock < selling_unit:
                continue
            snapshot_at = str(row.get("observed_at") or "")
            if snapshot_is_stale(snapshot_at, now=now):
                continue
            if price is None or mov is None or currency != "EUR":
                continue
            eligible_rows.append((mov, price, str(row.get("seller_alias") or ""), row))
        if not eligible_rows:
            if max((int(row.get("inventory") or 0) for row in product_rows), default=0) < minimum_stock:
                diagnostics["below_stock"] += 1
            elif all(
                int(row.get("selling_unit") or 1) < 1
                or int(row.get("inventory") or 0)
                < int(row.get("selling_unit") or 1)
                for row in product_rows
            ):
                diagnostics["below_selling_unit"] += 1
            elif all(snapshot_is_stale(str(row.get("observed_at") or ""), now=now) for row in product_rows):
                diagnostics["stale_snapshot"] += 1
            else:
                diagnostics["missing_price"] += 1
            continue
        eligible_rows.sort(key=lambda item: (item[0], item[1], item[2], str(item[3].get("offer_qid") or "")))
        key = product_key(gtin)
        scenarios = []
        for order, (mov, net, _, selected) in enumerate(eligible_rows, start=1):
            gross = (net * (Decimal("1") + VAT_RATE)).quantize(CENT, rounding=ROUND_HALF_UP)
            vat = (gross - net).quantize(CENT, rounding=ROUND_HALF_UP)
            alias = str(selected.get("seller_alias") or "").strip()
            offer_id = str(selected.get("offer_qid") or "").strip()
            variant_id = str(selected.get("variant_fid") or "").strip()
            snapshot_at = str(selected.get("observed_at") or "")
            scenario = PurchaseScenario(
                scenario_id=scenario_key(
                    supplier="qogita", supplier_alias=alias,
                    supplier_product_id=variant_id,
                    supplier_offer_id=offer_id, variant_id=variant_id,
                    canonical_ean=gtin, scenario_type="qogita_mov",
                    account_mov=mov,
                ),
                product_key=key, canonical_ean=gtin,
                identifier_type=_identifier_type(gtin), supplier="qogita",
                supplier_alias=alias, supplier_product_id=variant_id,
                supplier_offer_id=offer_id, variant_id=variant_id,
                brand=str(selected.get("brand") or "").strip(),
                title=str(selected.get("product_name") or "").strip(),
                scenario_type="qogita_mov",
                scenario_label=f"MOV € {mov:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
                scenario_order=order, account_mov=mov,
                account_mov_currency="EUR", account_mov_eur=mov,
                selling_unit=int(selected.get("selling_unit") or 1),
                cost_net_unit_eur=net.quantize(CENT, rounding=ROUND_HALF_UP),
                vat_rate=VAT_RATE, vat_amount_unit=vat,
                cost_gross_unit_eur=gross,
                stock=int(selected.get("inventory") or 0),
                snapshot_id=str(selected.get("run_id") or ""),
                snapshot_at=snapshot_at, freshness_status="fresh",
                tier_is_active=_is_active(selected.get("is_active")),
                source_metadata={
                    "category": str(selected.get("category_name") or "").strip(),
                    "product_url": str(selected.get("product_url") or "").strip(),
                },
            )
            scenarios.append(scenario)
        selected = eligible_rows[0][3]
        product = ProductCandidate(
            product_key=key, canonical_ean=gtin,
            identifier_type=_identifier_type(gtin),
            brand=str(selected.get("brand") or "").strip(),
            title=str(selected.get("product_name") or "").strip(),
            category=str(selected.get("category_name") or "").strip(),
            image_url=str(selected.get("image_url") or "").strip(),
            scenarios=tuple(scenarios),
        ).to_dict()
        product.update({
            "gtin": gtin, "supplier": "Qogita",
            **_legacy_base_fields(product),
        })
        candidates.append(product)
        offer_identities = {
            (str(row.get("seller_alias") or ""), str(row.get("offer_qid") or ""))
            for row in product_rows
        }
        diagnostics["duplicates"] += max(0, len(offer_identities) - 1)
    diagnostics["qogita_products"] = len(candidates)
    diagnostics["qogita_scenarios"] = sum(len(row["scenarios"]) for row in candidates)
    return candidates, diagnostics


def snapshot_is_stale(value, *, hours=24, now=None):
    try:
        observed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return True
    if observed.tzinfo is None:
        observed = observed.replace(tzinfo=timezone.utc)
    current = now or datetime.now(timezone.utc)
    return (current - observed.astimezone(timezone.utc)).total_seconds() >= hours * 3600
