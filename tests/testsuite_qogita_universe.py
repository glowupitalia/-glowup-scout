import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from discovery import normalize_discovery_state
from discovery_rotation import DiscoveryRotationStore, rotation_scope_key
from qogita_korean_beauty import QogitaMembershipStore
from supplier_catalog import SupplierCatalogStore


GTIN_A = "08809447256221"
GTIN_B = "08809525249565"
GTIN_ABSENT = "08809971482240"
EAN_A = GTIN_A[1:]
EAN_B = GTIN_B[1:]


def scenario_payload(gtin, suffix):
    return json.dumps({
        "scenario_id": f"qogita-{suffix}", "product_key": f"product-{suffix}",
        "canonical_ean": gtin[1:], "identifier_type": "EAN", "supplier": "qogita",
        "supplier_alias": "qogita", "supplier_product_id": suffix,
        "scenario_type": "qogita_tier", "scenario_order": 1,
    })


class QogitaUniverseTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.database = Path(self.temporary.name) / "supplier.sqlite3"
        connection = sqlite3.connect(self.database)
        connection.executescript("""
            CREATE TABLE supplier_catalog_products(
                run_id TEXT,canonical_product_key TEXT,canonical_gtin TEXT,
                identifier_type TEXT,brand TEXT,title TEXT,size_value TEXT,
                size_unit TEXT,pack_count INTEGER
            );
            CREATE INDEX idx_supplier_catalog_products_run_gtin
                ON supplier_catalog_products(run_id,canonical_gtin);
            CREATE TABLE supplier_catalog_scenarios(
                run_id TEXT,canonical_product_key TEXT,canonical_ean TEXT,
                scenario_id TEXT,payload_json TEXT
            );
            CREATE INDEX idx_supplier_catalog_scenarios_product
                ON supplier_catalog_scenarios(run_id,canonical_product_key);
            CREATE TABLE qogita_serving_active(
                supplier TEXT,serving_generation_id TEXT,updated_at TEXT
            );
            CREATE TABLE qogita_serving_snapshots(
                serving_generation_id TEXT,source_generation_id TEXT,
                bootstrap_run_id TEXT,status TEXT
            );
            CREATE TABLE qogita_serving_memberships(
                serving_generation_id TEXT,canonical_product_key TEXT,
                scenario_count INTEGER
            );
            CREATE INDEX idx_qogita_serving_membership_product
                ON qogita_serving_memberships(canonical_product_key,serving_generation_id);
        """)
        for gtin, suffix in ((GTIN_A, "a"), (GTIN_B, "b")):
            connection.execute(
                "INSERT INTO supplier_catalog_products VALUES(?,?,?,?,?,?,?,?,?)",
                ("source-1", f"key-{suffix}", gtin, "EAN", "Brand", suffix,
                 None, None, 1),
            )
            connection.execute(
                "INSERT INTO supplier_catalog_scenarios VALUES(?,?,?,?,?)",
                ("source-1", f"key-{suffix}", gtin[1:], f"scenario-{suffix}",
                 scenario_payload(gtin, suffix)),
            )
            connection.execute(
                "INSERT INTO qogita_serving_memberships VALUES(?,?,1)",
                ("serving-1", f"key-{suffix}"),
            )
        connection.execute(
            "INSERT INTO qogita_serving_active VALUES('qogita','serving-1','now')"
        )
        connection.execute(
            "INSERT INTO qogita_serving_snapshots VALUES(?,?,?,'valid')",
            ("serving-1", "source-1", "bootstrap-1"),
        )
        connection.commit()
        connection.close()
        membership = QogitaMembershipStore(self.database)
        membership.create_version(
            source_generation_id="source-1", membership_version_id="membership-1",
        )
        membership.finalize_version(
            "membership-1", entries=[
                {"canonical_gtin": GTIN_A, "canonical_product_key": "key-a",
                 "variant_fid": "fid-a"},
                {"canonical_gtin": GTIN_ABSENT, "canonical_product_key": None,
                 "variant_fid": "fid-absent"},
            ], acquisition_status="complete", metrics={},
        )
        membership.activate("membership-1")
        self.store = SupplierCatalogStore(self.database)
        self.initialize_patch = patch(
            "supplier_catalog.SupplierCatalogStore.initialize", return_value=None,
        )
        self.serving_patch = patch(
            "qogita_serving.QogitaServingStore.initialize", return_value=None,
        )
        self.initialize_patch.start()
        self.serving_patch.start()

    def tearDown(self):
        self.serving_patch.stop()
        self.initialize_patch.stop()
        self.temporary.cleanup()

    def test_legacy_default_and_full_universe_are_unchanged(self):
        self.assertEqual(normalize_discovery_state({})["qogita_universe"], "full")
        self.assertEqual(
            self.store.active_identifiers(["qogita"]), {EAN_A, EAN_B},
        )
        self.assertEqual(
            self.store.active_identifiers(["qogita"], qogita_universe="full"),
            {EAN_A, EAN_B},
        )

    def test_korean_beauty_is_membership_intersection_serving(self):
        identifiers = self.store.active_identifiers(
            ["qogita"], qogita_universe="korean_beauty",
        )
        self.assertEqual(identifiers, {EAN_A})
        memberships = self.store.active_identifier_memberships(
            ["qogita"], qogita_universe="korean_beauty",
            qogita_membership_version_id="membership-1",
            qogita_serving_generation_id="serving-1",
        )
        self.assertEqual(memberships, {EAN_A: ("qogita",)})
        self.assertNotIn(GTIN_ABSENT, memberships)
        self.assertNotIn(EAN_B, memberships)

    def test_batched_candidates_use_frozen_membership_without_scenario_copy(self):
        metadata = {
            "source_generation_id": "source-1",
            "serving_generation_id": "serving-1",
            "qogita_universe": "korean_beauty",
            "qogita_membership_version_id": "membership-1",
            "created_at": "now", "product_catalog_coverage_type": "full",
        }
        rows = list(self.store.iter_active_candidates_for_identifiers(
            "qogita", [EAN_A, EAN_B], generation_metadata=metadata,
        ))
        self.assertEqual([row["canonical_ean"] for row in rows], [EAN_A])
        self.assertEqual(rows[0]["scenarios"][0]["scenario_id"], "qogita-a")
        with sqlite3.connect(self.database) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM supplier_catalog_scenarios"
                ).fetchone()[0],
                2,
            )

    def test_membership_version_is_frozen_independently_from_active_pointer(self):
        membership = QogitaMembershipStore(self.database)
        membership.create_version(
            source_generation_id="source-1", membership_version_id="membership-2",
        )
        membership.finalize_version(
            "membership-2", entries=[{
                "canonical_gtin": GTIN_B, "canonical_product_key": "key-b",
                "variant_fid": "fid-b",
            }], acquisition_status="complete", metrics={},
        )
        membership.activate("membership-2")
        frozen = self.store.active_identifier_memberships(
            ["qogita"], qogita_universe="korean_beauty",
            qogita_membership_version_id="membership-1",
            qogita_serving_generation_id="serving-1",
        )
        active = self.store.active_identifier_memberships(
            ["qogita"], qogita_universe="korean_beauty",
            qogita_serving_generation_id="serving-1",
        )
        self.assertEqual(set(frozen), {EAN_A})
        self.assertEqual(set(active), {EAN_B})

    def test_rotation_scope_distinguishes_semantics_not_membership_version(self):
        full = rotation_scope_key(["qogita"])
        korean = rotation_scope_key(
            ["qogita"], qogita_universe="korean_beauty",
        )
        self.assertNotEqual(full, korean)
        self.assertEqual(
            korean,
            rotation_scope_key(["qogita"], qogita_universe="korean_beauty"),
        )
        self.assertEqual(
            rotation_scope_key(["abw"]),
            rotation_scope_key(["abw"], qogita_universe="korean_beauty"),
        )

    def test_rotation_stores_full_and_korean_as_separate_scopes(self):
        rotation = DiscoveryRotationStore(
            Path(self.temporary.name) / "rotation.sqlite3"
        )
        candidate = {
            "canonical_ean": GTIN_A, "gtin": GTIN_A,
            "scenarios": [{"supplier": "qogita"}],
        }
        _, full = rotation.select_current_universe(
            "full-job", [candidate], ["qogita"], None,
        )
        _, korean = rotation.select_current_universe(
            "korean-job", [candidate], ["qogita"], None,
            qogita_universe="korean_beauty",
        )
        self.assertNotEqual(full["rotation_scope"], korean["rotation_scope"])


if __name__ == "__main__":
    unittest.main()
