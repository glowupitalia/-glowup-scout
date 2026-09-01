import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from discovery_incremental import DiscoveryIncrementalStore, LightweightCheckpointStore
from discovery_incremental_runner import run_incremental_discovery
from discovery_resources import DiscoveryResourceGovernor, ResourcePolicy
from discovery import _evaluate_product_combinations
from discovery_taxonomy import (
    BEAUTY_DEPARTMENT_ID,
    MARKETPLACE_IT,
    MODE_ALL,
    MODE_MANUAL,
    MODE_ONLY_BEAUTY,
    classification_paths_allowed,
    apply_qogita_listing_filter,
    extract_listing_classification_paths,
    filter_qogita_scenarios,
    normalize_qogita_category_filter,
)


FRAGRANCE = "6306898031"
MAKEUP = "6306900031"
LIPSTICK = "6307022031"
SKINCARE = "6306897031"
BATH_BODY = "4327880031"
HEALTH = "1571289031"
HAIR = "4327902031"
NAIL = "6306899031"


def taxonomy_listing(asin="ASIN1", leaf=LIPSTICK, parent=MAKEUP):
    return {
        "asin": asin,
        "compatibility_status": "compatible",
        "beauty_status": "display_group_beauty",
        "bsr_beauty": 100,
        "browse_classification": {
            "classificationId": leaf, "displayName": "Leaf",
        },
        "diagnostics": {"classification_records": [{
            "marketplaceId": MARKETPLACE_IT,
            "classifications": [{
                "classificationId": leaf, "displayName": "Leaf",
                "parent": {
                    "classificationId": parent, "displayName": "Parent",
                    "parent": {
                        "classificationId": BEAUTY_DEPARTMENT_ID,
                        "displayName": "Bellezza",
                    },
                },
            }],
        }]},
    }


def config(
    *, parents=(MAKEUP,), excluded=(), only_beauty=False, unknown=True,
    beauty_customized=False,
):
    return {
        "qogita_category_filter_enabled": True,
        "qogita_category_selected_parent_ids": list(parents),
        "qogita_category_child_overrides": {
            MAKEUP: {"excluded_ids": list(excluded)},
        } if excluded else {},
        "qogita_category_include_unknown": unknown,
        "qogita_category_only_beauty": only_beauty,
        "qogita_category_beauty_selection_customized": beauty_customized,
    }


class QogitaTaxonomyTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.store = DiscoveryIncrementalStore(
            Path(self.temporary.name) / "incremental.sqlite3"
        )
        self.checkpoints = LightweightCheckpointStore(
            Path(self.temporary.name) / "checkpoints"
        )

    def tearDown(self):
        self.temporary.cleanup()

    def test_structured_ancestry_is_projected_without_title_keywords(self):
        paths = extract_listing_classification_paths(taxonomy_listing())
        self.assertEqual(len(paths), 1)
        self.assertEqual(
            [row["classification_id"] for row in paths[0]["nodes"]],
            [BEAUTY_DEPARTMENT_ID, MAKEUP, LIPSTICK],
        )

    def test_parent_child_and_lipstick_exclusion(self):
        path = [[BEAUTY_DEPARTMENT_ID, MAKEUP, LIPSTICK]]
        self.assertTrue(classification_paths_allowed(path, config()))
        self.assertFalse(classification_paths_allowed(
            path, config(excluded=(LIPSTICK,)),
        ))
        other_makeup = [[BEAUTY_DEPARTMENT_ID, MAKEUP, "6307028031"]]
        self.assertTrue(classification_paths_allowed(
            other_makeup, config(excluded=(LIPSTICK,)),
        ))

    def test_fragrance_parent_can_be_excluded(self):
        fragrance_path = [[BEAUTY_DEPARTMENT_ID, FRAGRANCE, "6306974031"]]
        self.assertFalse(classification_paths_allowed(
            fragrance_path, config(parents=(MAKEUP,)),
        ))

    def test_unknown_and_future_categories_follow_explicit_modes(self):
        self.assertTrue(classification_paths_allowed([], config(unknown=True)))
        self.assertFalse(classification_paths_allowed([], config(unknown=False)))
        self.assertFalse(classification_paths_allowed(
            [["future-department", "future-category"]], config(),
        ))
        self.assertTrue(classification_paths_allowed(
            [["future-department", "future-category"]], {},
        ))

    def test_filter_mode_is_explicit_and_backward_compatible(self):
        self.assertEqual(
            normalize_qogita_category_filter({})["qogita_category_filter_mode"],
            MODE_ALL,
        )
        self.assertEqual(
            normalize_qogita_category_filter(config(only_beauty=True))[
                "qogita_category_filter_mode"
            ],
            MODE_ONLY_BEAUTY,
        )
        self.assertEqual(
            normalize_qogita_category_filter(config())["qogita_category_filter_mode"],
            MODE_MANUAL,
        )
        explicit = config(only_beauty=True)
        explicit["qogita_category_filter_mode"] = MODE_MANUAL
        self.assertEqual(
            normalize_qogita_category_filter(explicit)[
                "qogita_category_filter_mode"
            ],
            MODE_MANUAL,
        )

    def test_only_beauty_uses_structured_department(self):
        self.assertTrue(classification_paths_allowed(
            [[BEAUTY_DEPARTMENT_ID, MAKEUP]],
            config(parents=(MAKEUP,), only_beauty=True),
        ))
        self.assertTrue(classification_paths_allowed(
            [[BEAUTY_DEPARTMENT_ID, FRAGRANCE]],
            config(parents=(MAKEUP,), only_beauty=True),
        ))
        self.assertFalse(classification_paths_allowed(
            [["1571289031"]],
            config(parents=(MAKEUP,), only_beauty=True),
        ))
        self.assertTrue(classification_paths_allowed(
            [], config(parents=(MAKEUP,), only_beauty=True, unknown=True),
        ))
        self.assertFalse(classification_paths_allowed(
            [], config(parents=(MAKEUP,), only_beauty=True, unknown=False),
        ))

    def test_only_beauty_customized_selection_is_an_exact_beauty_whitelist(self):
        selected = (SKINCARE, MAKEUP, BATH_BODY)
        subset = config(
            parents=selected, only_beauty=True, beauty_customized=True,
        )
        for classification_id in selected:
            self.assertTrue(classification_paths_allowed(
                [[BEAUTY_DEPARTMENT_ID, classification_id]], subset,
            ))
        for classification_id in (FRAGRANCE, HAIR, NAIL):
            self.assertFalse(classification_paths_allowed(
                [[BEAUTY_DEPARTMENT_ID, classification_id]], subset,
            ))
        self.assertFalse(classification_paths_allowed(
            [[HEALTH]], subset,
        ))

    def test_only_beauty_customized_selection_honors_child_overrides(self):
        subset = config(
            parents=(MAKEUP,), excluded=(LIPSTICK,), only_beauty=True,
            beauty_customized=True,
        )
        self.assertFalse(classification_paths_allowed(
            [[BEAUTY_DEPARTMENT_ID, MAKEUP, LIPSTICK]], subset,
        ))
        self.assertTrue(classification_paths_allowed(
            [[BEAUTY_DEPARTMENT_ID, MAKEUP, "6307028031"]], subset,
        ))

    def test_only_beauty_legacy_empty_selection_defaults_to_all_beauty(self):
        legacy = config(parents=(), only_beauty=True)
        for classification_id in (SKINCARE, MAKEUP, FRAGRANCE, HAIR, NAIL):
            self.assertTrue(classification_paths_allowed(
                [[BEAUTY_DEPARTMENT_ID, classification_id]], legacy,
            ))

    def test_nicola_manual_selection_is_an_exact_structured_whitelist(self):
        selected = (SKINCARE, MAKEUP, BATH_BODY, HEALTH)
        manual = config(parents=selected, unknown=False)
        for classification_id in selected:
            self.assertTrue(classification_paths_allowed(
                [[classification_id, f"{classification_id}-leaf"]], manual,
            ))
        for classification_id in (FRAGRANCE, HAIR, NAIL, "future-category"):
            self.assertFalse(classification_paths_allowed(
                [[classification_id, f"{classification_id}-leaf"]], manual,
            ))
        self.assertFalse(classification_paths_allowed([], manual))

    def test_ambiguous_product_is_included_when_any_path_is_allowed(self):
        paths = [
            [BEAUTY_DEPARTMENT_ID, FRAGRANCE],
            [BEAUTY_DEPARTMENT_ID, MAKEUP, LIPSTICK],
        ]
        self.assertTrue(classification_paths_allowed(paths, config()))

    def test_supplier_isolation_removes_only_qogita(self):
        candidate = {
            "scenarios": [
                {"scenario_id": "q", "supplier": "qogita"},
                {"scenario_id": "a", "supplier": "abw"},
            ],
        }
        removed = filter_qogita_scenarios(
            candidate, [[BEAUTY_DEPARTMENT_ID, FRAGRANCE]], config(),
        )
        self.assertTrue(removed)
        self.assertEqual(candidate["scenarios"], [{"scenario_id": "a", "supplier": "abw"}])

    def test_mixed_ambiguous_listings_exclude_qogita_only_on_disallowed_listing(self):
        candidate = {
            "product_key": "p", "scenarios": [
                {"scenario_id": "q", "supplier": "qogita", "scenario_label": "Q",
                 "cost_gross_unit_eur": Decimal("10")},
                {"scenario_id": "a", "supplier": "abw", "scenario_label": "A",
                 "cost_gross_unit_eur": Decimal("10")},
            ],
            "amazon_listings": [
                {"asin": "FRAGRANCE", "amazon_observation_id": "o1"},
                {"asin": "MAKEUP", "amazon_observation_id": "o2"},
            ],
        }
        removed = apply_qogita_listing_filter(candidate, {
            "FRAGRANCE": [(BEAUTY_DEPARTMENT_ID, FRAGRANCE)],
            "MAKEUP": [(BEAUTY_DEPARTMENT_ID, MAKEUP)],
        }, config())
        self.assertFalse(removed)
        self.assertEqual(candidate["amazon_listings"][0]["excluded_suppliers"], ["qogita"])
        self.assertNotIn("excluded_suppliers", candidate["amazon_listings"][1])
        observations = {
            key: {
                "observation_id": key, "asin": asin, "fee_status": "valid",
                "reference_price": Decimal("30"), "bsr_beauty": 100,
                "fba_sellers": 1, "total_sellers": 2,
                "fee_estimate": {
                    "fba_fee_net": Decimal("4"), "fba_fee_gross": Decimal("4.88"),
                    "referral_fee": Decimal("4.5"), "referral_rate": Decimal("0.15"),
                },
            }
            for key, asin in (("o1", "FRAGRANCE"), ("o2", "MAKEUP"))
        }
        _evaluate_product_combinations(candidate, observations, 0)
        pairs = {
            (row["asin"], row["supplier"])
            for row in candidate["opportunity_combinations"]
        }
        self.assertEqual(pairs, {
            ("FRAGRANCE", "abw"), ("MAKEUP", "abw"), ("MAKEUP", "qogita"),
        })

    def test_projection_and_scenario_removal_are_indexed_and_idempotent(self):
        candidate = {
            "canonical_ean": "0000000000001", "gtin": "0000000000001",
            "catalog_status": "resolved", "amazon_listings": [taxonomy_listing()],
            "scenarios": [
                {"scenario_id": "q", "supplier": "qogita"},
                {"scenario_id": "a", "supplier": "abw"},
            ],
        }
        self.store.create_job({
            "job_id": "job", "phase": "catalog_complete", "filters": {},
        }, [candidate])
        paths = self.store.classification_paths_for_identifiers(
            "job", ["0000000000001"],
        )
        self.assertEqual(paths["0000000000001"], [
            (BEAUTY_DEPARTMENT_ID, MAKEUP, LIPSTICK),
        ])
        candidate["scenarios"] = [candidate["scenarios"][1]]
        self.store.update_candidates(
            "job", [candidate], remove_qogita_scenarios=["0000000000001"],
        )
        hydrated = next(self.store.iter_candidates("job"))
        self.assertEqual([row["supplier"] for row in hydrated["scenarios"]], ["abw"])
        with self.store._connect() as connection:
            indexes = {
                row[1] for row in connection.execute(
                    "PRAGMA index_list(discovery_listing_classifications)"
                )
            }
            taxonomy_plan = " ".join(
                str(row[3]) for row in connection.execute(
                    """EXPLAIN QUERY PLAN SELECT canonical_identifier
                       FROM discovery_listing_classifications
                       WHERE marketplace_id=? AND classification_id=?""",
                    (MARKETPLACE_IT, LIPSTICK),
                )
            )
            scenario_plan = " ".join(
                str(row[3]) for row in connection.execute(
                    """EXPLAIN QUERY PLAN SELECT canonical_identifier
                       FROM discovery_purchase_scenarios
                       WHERE job_id=? AND supplier=?""", ("job", "qogita"),
                )
            )
        self.assertIn("idx_discovery_taxonomy_node_identifier", indexes)
        self.assertIn("idx_discovery_taxonomy_identifier", indexes)
        self.assertIn("idx_discovery_taxonomy_node_identifier", taxonomy_plan)
        self.assertIn("idx_discovery_scenarios_supplier", scenario_plan)
        with self.store._connect() as connection:
            connection.execute("DELETE FROM discovery_listing_classifications")
            connection.commit()
        self.assertEqual(self.store.backfill_classification_projection("job"), 3)
        self.assertEqual(self.store.backfill_classification_projection("job"), 0)

    def test_candidate_iteration_uses_bounded_set_queries_not_n_plus_one(self):
        candidates = []
        for index in range(600):
            candidates.append({
                "canonical_ean": f"{index:013d}", "gtin": f"{index:013d}",
                "scenarios": [{"scenario_id": str(index), "supplier": "qogita"}],
                "amazon_listings": [],
            })
        self.store.create_job({"job_id": "batch", "phase": "suppliers_loaded"}, candidates)
        statements = []
        original = self.store._new_connection

        def traced_connection():
            connection = original()
            connection.set_trace_callback(statements.append)
            return connection

        self.store._new_connection = traced_connection
        self.assertEqual(sum(1 for _ in self.store.iter_candidates("batch")), 600)
        scenario_reads = [value for value in statements if "FROM discovery_purchase_scenarios" in value]
        self.assertEqual(len(scenario_reads), 3)

    def test_fresh_catalog_filter_skips_pricing_and_fees_for_excluded_qogita(self):
        row = {
            "product_key": "p", "canonical_ean": "0000000000001",
            "gtin": "0000000000001", "catalog_status": "resolved",
            "scenarios": [{
                "scenario_id": "q", "supplier": "qogita",
                "scenario_label": "Qogita", "cost_gross_unit_eur": "10",
            }],
            "amazon_listings": [taxonomy_listing(parent=FRAGRANCE, leaf="6306974031")],
        }
        metadata = {
            "job_id": "filtered", "phase": "catalog_complete",
            "filters": {
                "bsr_min": 1, "bsr_max": 20_000, "max_fba_sellers": 10,
                "max_total_sellers": 10, "minimum_margin": 25,
            },
            **config(parents=(MAKEUP,)),
        }
        self.store.create_job(metadata, [row])
        result = run_incremental_discovery(
            "filtered", store=self.store, metadata_store=self.checkpoints,
            catalog_batch=lambda *_: self.fail("Fresh Catalog must not be called"),
            pricing_batch=lambda *_: self.fail("Excluded Qogita must skip Pricing"),
            fees_batch=lambda *_: self.fail("Excluded Qogita must skip Fees"),
            token_provider=object(), sleep_func=lambda *_: None,
            catalog_batch_interval=0, pricing_batch_interval=0, fee_batch_interval=0,
            resource_governor=DiscoveryResourceGovernor(
                policy=ResourcePolicy(
                    rss_soft_bytes=10**15, rss_hard_bytes=10**16,
                    available_soft_bytes=0, available_hard_bytes=0,
                    disk_free_hard_bytes=0, wal_soft_bytes=10**15,
                    wal_hard_bytes=10**16,
                    write_rate_soft_bytes_per_second=10**15,
                    write_rate_hard_bytes_per_second=10**16,
                ), database_path=self.store.path, sleep_func=lambda *_: None,
            ),
        )
        self.assertEqual(result["status"], "completed")
        hydrated = next(self.store.iter_candidates("filtered"))
        self.assertEqual(hydrated["scenarios"], [])
        self.assertEqual(
            hydrated["amazon_listings"][0]["evaluation_status"],
            "qogita_category_filtered",
        )

    def test_mixed_listing_filter_avoids_pricing_for_excluded_qogita_listing(self):
        fragrance = taxonomy_listing("FRAGRANCE", parent=FRAGRANCE, leaf="6306974031")
        makeup = taxonomy_listing("MAKEUP", parent=MAKEUP, leaf=LIPSTICK)
        row = {
            "product_key": "p", "canonical_ean": "0000000000001",
            "gtin": "0000000000001", "catalog_status": "ambiguous",
            "scenarios": [{
                "scenario_id": "q", "supplier": "qogita",
                "scenario_label": "Qogita", "cost_gross_unit_eur": "10",
            }], "amazon_listings": [fragrance, makeup],
        }
        self.store.create_job({
            "job_id": "mixed", "phase": "catalog_complete",
            "filters": {
                "bsr_min": 1, "bsr_max": 20_000, "max_fba_sellers": 10,
                "max_total_sellers": 10, "minimum_margin": 25,
            }, **config(parents=(MAKEUP,)),
        }, [row])
        pricing_calls = []

        def pricing(asins, *_):
            pricing_calls.extend(asins)
            return {asin: {"status": "missing"} for asin in asins}

        run_incremental_discovery(
            "mixed", store=self.store, metadata_store=self.checkpoints,
            catalog_batch=lambda *_: self.fail("Fresh Catalog must not be called"),
            pricing_batch=pricing,
            fees_batch=lambda *_: self.fail("Missing Pricing must skip Fees"),
            token_provider=object(), sleep_func=lambda *_: None,
            catalog_batch_interval=0, pricing_batch_interval=0, fee_batch_interval=0,
        )
        self.assertEqual(pricing_calls, ["MAKEUP"])
        hydrated = next(self.store.iter_candidates("mixed"))
        statuses = {
            row["asin"]: row["evaluation_status"]
            for row in hydrated["amazon_listings"]
        }
        self.assertEqual(statuses["FRAGRANCE"], "qogita_category_filtered")

    def test_manual_health_bypasses_only_the_legacy_beauty_gate(self):
        health = taxonomy_listing("HEALTH", leaf="health-leaf", parent=HEALTH)
        health["beauty_status"] = "other_display_group"
        health["diagnostics"]["classification_records"][0]["classifications"][0][
            "parent"
        ].pop("parent")
        row = {
            "product_key": "p", "canonical_ean": "0000000000001",
            "gtin": "0000000000001", "catalog_status": "resolved",
            "scenarios": [{
                "scenario_id": "q", "supplier": "qogita",
                "scenario_label": "Qogita", "cost_gross_unit_eur": "10",
            }], "amazon_listings": [health],
        }
        self.store.create_job({
            "job_id": "health", "phase": "catalog_complete",
            "filters": {
                "bsr_min": 1, "bsr_max": 20_000, "max_fba_sellers": 10,
                "max_total_sellers": 10, "minimum_margin": 25,
            }, **config(parents=(HEALTH,), unknown=False),
        }, [row])
        pricing_calls = []

        def pricing(asins, *_):
            pricing_calls.extend(asins)
            return {asin: {"status": "missing"} for asin in asins}

        run_incremental_discovery(
            "health", store=self.store, metadata_store=self.checkpoints,
            catalog_batch=lambda *_: self.fail("Fresh Catalog must not be called"),
            pricing_batch=pricing,
            fees_batch=lambda *_: self.fail("Missing Pricing must skip Fees"),
            token_provider=object(), sleep_func=lambda *_: None,
            catalog_batch_interval=0, pricing_batch_interval=0, fee_batch_interval=0,
        )
        self.assertEqual(pricing_calls, ["HEALTH"])
        listing = next(self.store.iter_candidates("health"))["amazon_listings"][0]
        self.assertNotEqual(listing.get("evaluation_status"), "beauty_filtered")

    def test_manual_health_still_requires_existing_bsr_beauty_policy(self):
        health = taxonomy_listing("HEALTH", leaf="health-leaf", parent=HEALTH)
        health["beauty_status"] = "other_display_group"
        health["bsr_beauty"] = None
        health["diagnostics"]["classification_records"][0]["classifications"][0][
            "parent"
        ].pop("parent")
        row = {
            "product_key": "p", "canonical_ean": "0000000000001",
            "gtin": "0000000000001", "catalog_status": "resolved",
            "scenarios": [{"scenario_id": "q", "supplier": "qogita"}],
            "amazon_listings": [health],
        }
        self.store.create_job({
            "job_id": "health-no-bsr", "phase": "catalog_complete",
            "filters": {
                "bsr_min": 1, "bsr_max": 20_000, "max_fba_sellers": 10,
                "max_total_sellers": 10, "minimum_margin": 25,
            },
            **config(parents=(HEALTH,), unknown=False),
        }, [row])
        run_incremental_discovery(
            "health-no-bsr", store=self.store, metadata_store=self.checkpoints,
            catalog_batch=lambda *_: self.fail("Fresh Catalog must not be called"),
            pricing_batch=lambda *_: self.fail("Missing BSR must skip Pricing"),
            fees_batch=lambda *_: self.fail("Missing BSR must skip Fees"),
            token_provider=object(), sleep_func=lambda *_: None,
            catalog_batch_interval=0, pricing_batch_interval=0, fee_batch_interval=0,
        )
        listing = next(self.store.iter_candidates("health-no-bsr"))[
            "amazon_listings"
        ][0]
        self.assertEqual(listing["evaluation_status"], "bsr_filtered")

    def test_manual_health_preserves_other_supplier_legacy_beauty_gate(self):
        health = taxonomy_listing("HEALTH", leaf="health-leaf", parent=HEALTH)
        health["beauty_status"] = "other_display_group"
        health["diagnostics"]["classification_records"][0]["classifications"][0][
            "parent"
        ].pop("parent")
        row = {
            "product_key": "p", "canonical_ean": "0000000000001",
            "gtin": "0000000000001", "catalog_status": "resolved",
            "scenarios": [
                {"scenario_id": "q", "supplier": "qogita"},
                {"scenario_id": "a", "supplier": "abw"},
            ], "amazon_listings": [health],
        }
        self.store.create_job({
            "job_id": "shared-health", "phase": "catalog_complete",
            "filters": {
                "bsr_min": 1, "bsr_max": 20_000, "max_fba_sellers": 10,
                "max_total_sellers": 10, "minimum_margin": 25,
            },
            **config(parents=(HEALTH,), unknown=False),
        }, [row])
        run_incremental_discovery(
            "shared-health", store=self.store, metadata_store=self.checkpoints,
            catalog_batch=lambda *_: self.fail("Fresh Catalog must not be called"),
            pricing_batch=lambda asins, *_: {
                asin: {"status": "missing"} for asin in asins
            },
            fees_batch=lambda *_: self.fail("Missing Pricing must skip Fees"),
            token_provider=object(), sleep_func=lambda *_: None,
            catalog_batch_interval=0, pricing_batch_interval=0, fee_batch_interval=0,
        )
        listing = next(self.store.iter_candidates("shared-health"))[
            "amazon_listings"
        ][0]
        self.assertEqual(listing["excluded_suppliers"], ["abw"])

    def test_completed_job_fragrance_acceptance_fixture(self):
        # Read-only production snapshot distilled from b4b88690...: the test
        # carries counts/classification IDs, not production payloads.
        opportunities = (
            [("qogita", FRAGRANCE)] * 23
            + [("qogita", MAKEUP)] * 283
            + [("abw", FRAGRANCE)] * 8
            + [("abw", MAKEUP)] * 40
            + [("umma", MAKEUP)] * 16
        )
        filtered = [
            row for row in opportunities
            if not (row[0] == "qogita" and row[1] == FRAGRANCE)
        ]
        self.assertEqual(len(opportunities), 370)
        self.assertEqual(len(filtered), 347)
        self.assertEqual(sum(row[0] == "qogita" for row in filtered), 283)
        self.assertEqual(sum(row[0] == "abw" for row in filtered), 48)
        self.assertEqual(sum(row[0] == "umma" for row in filtered), 16)


if __name__ == "__main__":
    unittest.main()
