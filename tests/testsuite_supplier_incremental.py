import sqlite3
import tempfile
import unittest
from pathlib import Path

from supplier_incremental import SupplierIncrementalStore, supplier_fingerprint


def product(key, *, price="1.00", valid=True):
    return {
        "canonical_product_key": key, "gtin": key, "name": f"Product {key}",
        "category": "Beauty", "brand": "Brand", "lowest_price": price,
        "unit": 1, "lowest_offer_inventory": 10, "preorder": False,
        "delivery": 1, "number_of_offers": 1, "total_inventory": 10,
        "product_url": f"https://example.test/{key}", "identifier_valid": valid,
    }


def scenario(key, *, price="1.00", enriched_at="2026-01-01T00:00:00Z"):
    return {
        "scenario_id": f"scenario-{key}", "price": price,
        "enriched_at": enriched_at,
    }


class SupplierIncrementalTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "incremental.sqlite3"
        self.store = SupplierIncrementalStore(self.path)

    def tearDown(self):
        self.temporary.cleanup()

    def test_supplier_specific_fingerprints_do_not_force_same_fields(self):
        qogita = product("1")
        changed = {**qogita, "number_of_offers": 2}
        self.assertNotEqual(
            supplier_fingerprint("qogita", qogita),
            supplier_fingerprint("qogita", changed),
        )
        umma = {"product_id": "1", "option_id": "2", "mode": "standard", "price": "1"}
        self.assertEqual(
            supplier_fingerprint("umma", umma),
            supplier_fingerprint("umma", {**umma, "number_of_offers": 999}),
        )
        qudo = {"product_id": "1", "variation_id": "2", "index_name": "A"}
        self.assertEqual(
            supplier_fingerprint("qudo", qudo),
            supplier_fingerprint("qudo", {**qudo, "detail_price": "99", "detail_stock": 1}),
        )

    def test_new_changed_unchanged_removed_and_unresolved(self):
        self.store.compose_generation("g1", "qogita", [product("a"), product("b"), product("c")])
        result = self.store.compose_generation(
            "g2", "qogita",
            [product("a"), product("b", price="2.00"), product("d", valid=False)],
            previous_run_id="g1",
        )
        self.assertEqual(result["unchanged"], 1)
        self.assertEqual(result["changed"], 1)
        self.assertEqual(result["identifier_unresolved"], 1)
        self.assertEqual(result["removed"], 1)

    def test_carry_forward_reuses_version_and_preserves_timestamp(self):
        source_time = "2026-08-01T00:00:00Z"
        self.store.compose_generation(
            "g1", "qogita", [product("a")],
            scenarios_by_product={"a": [scenario("a", enriched_at=source_time)]},
            now=source_time,
        )
        result = self.store.compose_generation(
            "g2", "qogita", [product("a")], previous_run_id="g1",
            reconciliation_days=30, now="2026-08-05T00:00:00Z",
        )
        self.assertEqual(result["scenario_versions_created"], 0)
        with sqlite3.connect(self.path) as connection:
            previous = connection.execute(
                "SELECT scenario_version_hash FROM supplier_generation_scenario_refs WHERE run_id='g1'"
            ).fetchone()[0]
            current = connection.execute(
                """SELECT scenario_version_hash,carried_forward,source_enriched_at
                   FROM supplier_generation_scenario_refs WHERE run_id='g2'"""
            ).fetchone()
        self.assertEqual(current, (previous, 1, source_time))
        self.assertEqual(self.store.generation_summary("g2")["scenario_versions"], 1)

    def test_reconciliation_due_is_configurable(self):
        source_time = "2026-08-01T00:00:00Z"
        self.store.compose_generation(
            "g1", "qogita", [product("a")],
            scenarios_by_product={"a": [scenario("a", enriched_at=source_time)]},
            now=source_time,
        )
        self.store.compose_generation(
            "g2", "qogita", [product("a")], previous_run_id="g1",
            reconciliation_days=5, now="2026-08-07T00:00:00Z",
        )
        summary = self.store.generation_summary("g2")
        self.assertEqual(summary["enrichment_states"], {"reconciliation_due": 1})
        queue = self.store.enrichment_queue("g2", now="2026-08-07T00:00:00Z")
        self.assertEqual(queue[0]["queue_reason"], "reconciliation_due")

    def test_changed_product_does_not_carry_scenario(self):
        self.store.compose_generation(
            "g1", "qogita", [product("a")],
            scenarios_by_product={"a": [scenario("a")]},
        )
        self.store.compose_generation(
            "g2", "qogita", [product("a", price="2")], previous_run_id="g1",
        )
        self.assertEqual(self.store.generation_summary("g2")["scenario_refs"], 0)


if __name__ == "__main__":
    unittest.main()
