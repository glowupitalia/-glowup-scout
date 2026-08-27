"""Supplier-neutral incremental catalog/enrichment composition.

The reference store is intentionally additive: immutable product/scenario
versions are shared by generation memberships, so unchanged commercial data is
not copied for every nightly generation. Existing supplier_catalog tables stay
untouched until a separately authorised migration.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

from supplier_catalog import DEFAULT_DATABASE_PATH, SUPPORTED_SUPPLIERS, json_dumps, utc_now


PRODUCT_STATES = {
    "new", "changed", "unchanged", "removed", "unavailable", "identifier_unresolved",
}
ENRICHMENT_STATES = {
    "enrichment_pending", "enriched", "carried_forward", "enrichment_failed",
    "reconciliation_due", "unavailable",
}

FINGERPRINT_FIELDS = {
    "qogita": (
        "gtin", "name", "category", "brand", "lowest_price", "unit",
        "lowest_offer_inventory", "preorder", "delivery", "number_of_offers",
        "total_inventory", "product_url",
    ),
    "umma": (
        "product_id", "option_id", "raw_barcode", "mode", "price", "currency",
        "stock", "minimum_quantity", "selling_unit", "availability", "is_display",
    ),
    "abw": (
        "product_id", "option_id", "catalog_number", "upc", "warehouse",
        "availability", "commercial_signal",
    ),
    "qudo": (
        "product_id", "variation_id", "supplier_sku", "index_name",
        "index_permalink", "index_purchasable", "index_stock_signal",
    ),
}


def _hash(value: Any) -> str:
    return hashlib.sha256(json_dumps(value).encode("utf-8")).hexdigest()


def supplier_fingerprint(supplier: str, record: dict[str, Any], *,
                         custom: Callable[[dict[str, Any]], Any] | None = None) -> str:
    clean = str(supplier or "").casefold()
    if clean not in SUPPORTED_SUPPLIERS:
        raise ValueError(f"Unsupported supplier: {supplier}")
    payload = custom(record) if custom else {
        field: record.get(field) for field in FINGERPRINT_FIELDS[clean]
    }
    return _hash({"supplier": clean, "payload": payload})


SCHEMA = """
CREATE TABLE IF NOT EXISTS supplier_product_versions (
    version_hash TEXT PRIMARY KEY,
    supplier TEXT NOT NULL,
    canonical_product_key TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS supplier_scenario_versions (
    version_hash TEXT PRIMARY KEY,
    supplier TEXT NOT NULL,
    scenario_id TEXT NOT NULL,
    canonical_product_key TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    enriched_at TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS supplier_generation_product_refs (
    run_id TEXT NOT NULL,
    supplier TEXT NOT NULL,
    canonical_product_key TEXT NOT NULL,
    product_version_hash TEXT NOT NULL,
    product_state TEXT NOT NULL,
    enrichment_state TEXT NOT NULL,
    enrichment_observed_at TEXT,
    reconciliation_due_at TEXT,
    source_run_id TEXT,
    PRIMARY KEY (run_id,canonical_product_key),
    FOREIGN KEY (product_version_hash) REFERENCES supplier_product_versions(version_hash)
);
CREATE TABLE IF NOT EXISTS supplier_generation_scenario_refs (
    run_id TEXT NOT NULL,
    supplier TEXT NOT NULL,
    scenario_id TEXT NOT NULL,
    canonical_product_key TEXT NOT NULL,
    scenario_version_hash TEXT NOT NULL,
    enrichment_state TEXT NOT NULL,
    carried_forward INTEGER NOT NULL DEFAULT 0,
    source_enriched_at TEXT NOT NULL,
    source_run_id TEXT,
    PRIMARY KEY (run_id,scenario_id),
    FOREIGN KEY (scenario_version_hash) REFERENCES supplier_scenario_versions(version_hash)
);
CREATE INDEX IF NOT EXISTS idx_supplier_generation_product_state
ON supplier_generation_product_refs(run_id,product_state,enrichment_state,reconciliation_due_at);
"""


def _connect(path: str | Path):
    connection = sqlite3.connect(Path(path).expanduser().resolve(), timeout=60)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA busy_timeout=60000")
    return connection


def _as_utc(value: str) -> datetime:
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)


class SupplierIncrementalStore:
    def __init__(self, path: str | Path = DEFAULT_DATABASE_PATH):
        self.path = Path(path).expanduser().resolve()

    def initialize(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with _connect(self.path) as connection:
            connection.executescript(SCHEMA)

    def has_generation(self, run_id: str) -> bool:
        self.initialize()
        with _connect(self.path) as connection:
            return connection.execute(
                "SELECT 1 FROM supplier_generation_product_refs WHERE run_id=? LIMIT 1",
                (run_id,),
            ).fetchone() is not None

    def product_payload(self, run_id: str, canonical_product_key: str) -> dict[str, Any]:
        """Return one immutable enumeration payload for bounded detail work."""
        self.initialize()
        with _connect(self.path) as connection:
            row = connection.execute(
                """SELECT version.payload_json FROM supplier_generation_product_refs ref
                   JOIN supplier_product_versions version
                     ON version.version_hash=ref.product_version_hash
                   WHERE ref.run_id=? AND ref.canonical_product_key=?""",
                (run_id, canonical_product_key),
            ).fetchone()
        if not row:
            raise ValueError("Unknown generation product")
        return json.loads(row["payload_json"])

    def generation_records(self, run_id: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Materialize a small/medium completed generation for atomic publication."""
        self.initialize()
        with _connect(self.path) as connection:
            products = [json.loads(row[0]) for row in connection.execute(
                """SELECT version.payload_json FROM supplier_generation_product_refs ref
                   JOIN supplier_product_versions version
                     ON version.version_hash=ref.product_version_hash
                   WHERE ref.run_id=? AND ref.product_state!='removed'
                   ORDER BY ref.canonical_product_key""", (run_id,),
            )]
            scenarios = [json.loads(row[0]) for row in connection.execute(
                """SELECT version.payload_json FROM supplier_generation_scenario_refs ref
                   JOIN supplier_scenario_versions version
                     ON version.version_hash=ref.scenario_version_hash
                   WHERE ref.run_id=? ORDER BY ref.scenario_id""", (run_id,),
            )]
        return products, scenarios

    def compose_generation(
        self, run_id: str, supplier: str, products: Iterable[dict[str, Any]], *,
        previous_run_id: str | None = None,
        scenarios_by_product: dict[str, list[dict[str, Any]]] | None = None,
        reconciliation_days: int = 60, now: str | None = None,
        fingerprint: Callable[[dict[str, Any]], Any] | None = None,
    ) -> dict[str, int]:
        """Compose auditable refs without copying unchanged scenario payloads."""
        clean = str(supplier or "").casefold()
        if clean not in SUPPORTED_SUPPLIERS:
            raise ValueError(f"Unsupported supplier: {supplier}")
        if reconciliation_days <= 0:
            raise ValueError("reconciliation_days must be positive")
        self.initialize()
        observed = now or utc_now()
        scenarios_by_product = scenarios_by_product or {}
        current_keys: set[str] = set()
        counts = {state: 0 for state in PRODUCT_STATES}
        counts.update({"scenario_versions_created": 0, "scenario_refs": 0})
        with _connect(self.path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            for record in products:
                key = str(record.get("canonical_product_key") or "")
                if not key or key in current_keys:
                    raise ValueError("Product identity is missing or duplicated")
                current_keys.add(key)
                fp = supplier_fingerprint(clean, record, custom=fingerprint)
                version_payload = {**record, "catalog_fingerprint": fp}
                version_hash = _hash({"supplier": clean, "product": version_payload})
                connection.execute(
                    """INSERT OR IGNORE INTO supplier_product_versions
                       (version_hash,supplier,canonical_product_key,fingerprint,payload_json,created_at)
                       VALUES (?,?,?,?,?,?)""",
                    (version_hash, clean, key, fp, json_dumps(version_payload), observed),
                )
                previous = None
                if previous_run_id:
                    previous = connection.execute(
                        """SELECT ref.*,version.fingerprint FROM supplier_generation_product_refs ref
                           JOIN supplier_product_versions version
                             ON version.version_hash=ref.product_version_hash
                           WHERE ref.run_id=? AND ref.canonical_product_key=?""",
                        (previous_run_id, key),
                    ).fetchone()
                identifier_valid = record.get("identifier_valid", True)
                state = "identifier_unresolved" if not identifier_valid else (
                    "new" if previous is None else
                    "unchanged" if previous["fingerprint"] == fp else "changed"
                )
                enrichment_state = "enrichment_pending"
                enrichment_at = None
                due_at = None
                source_run = None
                provided = scenarios_by_product.get(key) or []
                if provided:
                    enrichment_state = "enriched"
                    for scenario in provided:
                        scenario_id = str(scenario.get("scenario_id") or "")
                        enriched_at = str(scenario.get("enriched_at") or observed)
                        if not scenario_id:
                            raise ValueError("Scenario identity is missing")
                        scenario_hash = _hash({"supplier": clean, "scenario": scenario})
                        created = connection.execute(
                            """INSERT OR IGNORE INTO supplier_scenario_versions
                               (version_hash,supplier,scenario_id,canonical_product_key,payload_json,
                                enriched_at,created_at) VALUES (?,?,?,?,?,?,?)""",
                            (scenario_hash, clean, scenario_id, key, json_dumps(scenario),
                             enriched_at, observed),
                        ).rowcount
                        counts["scenario_versions_created"] += int(created)
                        connection.execute(
                            """INSERT INTO supplier_generation_scenario_refs
                               (run_id,supplier,scenario_id,canonical_product_key,
                                scenario_version_hash,enrichment_state,carried_forward,
                                source_enriched_at,source_run_id) VALUES (?,?,?,?,?,'enriched',0,?,?)""",
                            (run_id, clean, scenario_id, key, scenario_hash, enriched_at, run_id),
                        )
                        counts["scenario_refs"] += 1
                        enrichment_at = enriched_at if enrichment_at is None else max(enrichment_at, enriched_at)
                elif state == "unchanged" and previous_run_id:
                    prior_scenarios = connection.execute(
                        """SELECT * FROM supplier_generation_scenario_refs
                           WHERE run_id=? AND canonical_product_key=?""",
                        (previous_run_id, key),
                    ).fetchall()
                    if prior_scenarios:
                        oldest = min(row["source_enriched_at"] for row in prior_scenarios)
                        due = _as_utc(oldest) + timedelta(days=reconciliation_days)
                        due_at = due.isoformat().replace("+00:00", "Z")
                        enrichment_state = (
                            "reconciliation_due" if due <= _as_utc(observed) else "carried_forward"
                        )
                        enrichment_at = oldest
                        source_run = previous_run_id
                        for scenario in prior_scenarios:
                            connection.execute(
                                """INSERT INTO supplier_generation_scenario_refs
                                   (run_id,supplier,scenario_id,canonical_product_key,
                                    scenario_version_hash,enrichment_state,carried_forward,
                                    source_enriched_at,source_run_id) VALUES (?,?,?,?,?,?,1,?,?)""",
                                (run_id, clean, scenario["scenario_id"], key,
                                 scenario["scenario_version_hash"], enrichment_state,
                                 scenario["source_enriched_at"], previous_run_id),
                            )
                            counts["scenario_refs"] += 1
                connection.execute(
                    """INSERT INTO supplier_generation_product_refs
                       (run_id,supplier,canonical_product_key,product_version_hash,product_state,
                        enrichment_state,enrichment_observed_at,reconciliation_due_at,source_run_id)
                       VALUES (?,?,?,?,?,?,?,?,?)""",
                    (run_id, clean, key, version_hash, state, enrichment_state,
                     enrichment_at, due_at, source_run),
                )
                counts[state] += 1
            if previous_run_id:
                previous_rows = connection.execute(
                    "SELECT * FROM supplier_generation_product_refs WHERE run_id=?",
                    (previous_run_id,),
                ).fetchall()
                for previous in previous_rows:
                    key = previous["canonical_product_key"]
                    if key in current_keys:
                        continue
                    connection.execute(
                        """INSERT INTO supplier_generation_product_refs
                           (run_id,supplier,canonical_product_key,product_version_hash,
                            product_state,enrichment_state,enrichment_observed_at,
                            reconciliation_due_at,source_run_id)
                           VALUES (?,?,?,?,'removed','unavailable',?,?,?)""",
                        (run_id, clean, key, previous["product_version_hash"],
                         previous["enrichment_observed_at"],
                         previous["reconciliation_due_at"], previous_run_id),
                    )
                    counts["removed"] += 1
            connection.commit()
        return counts

    def enrichment_queue(self, run_id: str, *, now: str | None = None):
        self.initialize()
        observed = now or utc_now()
        priority = {
            "new": 700, "changed": 600, "enrichment_failed": 500,
            "identifier_unresolved": 400, "reconciliation_due": 300,
            "carried_forward": 200, "unchanged": 100,
        }
        with _connect(self.path) as connection:
            rows = connection.execute(
                """SELECT * FROM supplier_generation_product_refs
                   WHERE run_id=? AND product_state NOT IN ('removed','unavailable')""",
                (run_id,),
            ).fetchall()
        result = []
        for row in rows:
            value = dict(row)
            signal = value["enrichment_state"]
            if value["product_state"] in {"new", "changed", "identifier_unresolved"}:
                signal = value["product_state"]
            if value.get("reconciliation_due_at") and value["reconciliation_due_at"] <= observed:
                signal = "reconciliation_due"
            value["queue_reason"] = signal
            value["priority"] = priority.get(signal, priority.get(value["product_state"], 0))
            result.append(value)
        return sorted(result, key=lambda row: (-row["priority"], row["canonical_product_key"]))

    def persist_enrichment(self, run_id: str, supplier: str, canonical_product_key: str,
                           scenarios: Iterable[dict[str, Any]], *,
                           enriched_at: str | None = None) -> dict[str, int]:
        """Idempotently replace one product's scenario refs after enrichment.

        Scenario versions are content addressed while generation membership is
        keyed by stable scenario_id, so process restarts cannot duplicate tiers.
        """
        clean = str(supplier or "").casefold()
        if clean not in SUPPORTED_SUPPLIERS:
            raise ValueError(f"Unsupported supplier: {supplier}")
        self.initialize()
        observed = enriched_at or utc_now()
        rows = list(scenarios)
        scenario_ids = [str(row.get("scenario_id") or "") for row in rows]
        if "" in scenario_ids or len(set(scenario_ids)) != len(scenario_ids):
            raise ValueError("Scenario identities are missing or duplicated")
        connection = _connect(self.path)
        try:
            connection.execute("BEGIN IMMEDIATE")
            product = connection.execute(
                """SELECT 1 FROM supplier_generation_product_refs
                   WHERE run_id=? AND supplier=? AND canonical_product_key=?""",
                (run_id, clean, canonical_product_key),
            ).fetchone()
            if not product:
                raise ValueError("Unknown generation product")
            connection.execute(
                "DELETE FROM supplier_generation_scenario_refs WHERE run_id=? AND canonical_product_key=?",
                (run_id, canonical_product_key),
            )
            versions_created = 0
            for scenario in rows:
                payload = dict(scenario)
                payload.setdefault("enriched_at", observed)
                scenario_id = str(payload["scenario_id"])
                scenario_hash = _hash({"supplier": clean, "scenario": payload})
                versions_created += int(connection.execute(
                    """INSERT OR IGNORE INTO supplier_scenario_versions
                       (version_hash,supplier,scenario_id,canonical_product_key,payload_json,
                        enriched_at,created_at) VALUES (?,?,?,?,?,?,?)""",
                    (scenario_hash, clean, scenario_id, canonical_product_key,
                     json_dumps(payload), observed, observed),
                ).rowcount)
                connection.execute(
                    """INSERT INTO supplier_generation_scenario_refs
                       (run_id,supplier,scenario_id,canonical_product_key,scenario_version_hash,
                        enrichment_state,carried_forward,source_enriched_at,source_run_id)
                       VALUES (?,?,?,?,?,'enriched',0,?,?)""",
                    (run_id, clean, scenario_id, canonical_product_key, scenario_hash,
                     observed, run_id),
                )
            connection.execute(
                """UPDATE supplier_generation_product_refs SET enrichment_state='enriched',
                   enrichment_observed_at=?,reconciliation_due_at=NULL,source_run_id=?
                   WHERE run_id=? AND supplier=? AND canonical_product_key=?""",
                (observed, run_id, run_id, clean, canonical_product_key),
            )
            connection.commit()
            return {"scenario_refs": len(rows), "scenario_versions_created": versions_created}
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def persist_product_update(self, run_id: str, supplier: str,
                               canonical_product_key: str,
                               updates: dict[str, Any]) -> None:
        """Persist identifiers discovered during bounded detail enrichment."""
        clean = str(supplier or "").casefold()
        self.initialize()
        connection = _connect(self.path)
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """SELECT version.payload_json FROM supplier_generation_product_refs ref
                   JOIN supplier_product_versions version
                     ON version.version_hash=ref.product_version_hash
                   WHERE ref.run_id=? AND ref.supplier=? AND ref.canonical_product_key=?""",
                (run_id, clean, canonical_product_key),
            ).fetchone()
            if not row:
                raise ValueError("Unknown generation product")
            payload = {**json.loads(row["payload_json"]), **dict(updates or {})}
            fingerprint = supplier_fingerprint(clean, payload)
            payload["catalog_fingerprint"] = fingerprint
            version_hash = _hash({"supplier": clean, "product": payload})
            connection.execute(
                """INSERT OR IGNORE INTO supplier_product_versions
                   (version_hash,supplier,canonical_product_key,fingerprint,payload_json,created_at)
                   VALUES (?,?,?,?,?,?)""",
                (version_hash, clean, canonical_product_key, fingerprint,
                 json_dumps(payload), utc_now()),
            )
            connection.execute(
                """UPDATE supplier_generation_product_refs SET product_version_hash=?
                   WHERE run_id=? AND supplier=? AND canonical_product_key=?""",
                (version_hash, run_id, clean, canonical_product_key),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def generation_summary(self, run_id: str) -> dict[str, Any]:
        self.initialize()
        with _connect(self.path) as connection:
            product_states = dict(connection.execute(
                "SELECT product_state,COUNT(*) FROM supplier_generation_product_refs WHERE run_id=? GROUP BY 1",
                (run_id,),
            ).fetchall())
            enrichment_states = dict(connection.execute(
                "SELECT enrichment_state,COUNT(*) FROM supplier_generation_product_refs WHERE run_id=? GROUP BY 1",
                (run_id,),
            ).fetchall())
            scenario_refs = connection.execute(
                "SELECT COUNT(*) FROM supplier_generation_scenario_refs WHERE run_id=?", (run_id,),
            ).fetchone()[0]
            scenario_versions = connection.execute(
                "SELECT COUNT(*) FROM supplier_scenario_versions",
            ).fetchone()[0]
        return {
            "product_states": product_states, "enrichment_states": enrichment_states,
            "scenario_refs": scenario_refs, "scenario_versions": scenario_versions,
        }
