import copy
import hashlib
import json
import tempfile
import tracemalloc
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from direct_lookup import DirectAmazonLookup
from discovery_freshness import (
    AmazonFreshnessPolicy,
    CACHE_PAYLOAD_SCHEMA_VERSION,
    DiscoveryAmazonCache,
    PlanAction,
    fee_cache_key,
    plan_cached_product,
    planning_counts,
    reusable_fee,
)
from discovery_incremental import DiscoveryIncrementalStore, prepare_incremental_job
from discovery_incremental import LightweightCheckpointStore
from discovery_incremental_runner import run_incremental_discovery
from discovery_rotation import DiscoveryRotationStore


NOW = datetime.now(timezone.utc)
FRESH = (NOW - timedelta(hours=1)).isoformat().replace("+00:00", "Z")
STALE = (NOW - timedelta(days=40)).isoformat().replace("+00:00", "Z")


def valid_gtin(value):
    body = f"88000{value:08d}"
    check = (10 - sum(
        int(digit) * (3 if index % 2 == 0 else 1)
        for index, digit in enumerate(reversed(body))
    ) % 10) % 10
    return body + str(check)


def resolved_cache(**changes):
    value = {
        "catalog_status": "resolved",
        "catalog_observed_at": FRESH,
        "bsr_observed_at": FRESH,
        "pricing_observed_at": FRESH,
        "competition_observed_at": FRESH,
        "fee_cache_key": "A|EUR|10.00",
        "fee_status": "valid",
        "fee_observed_at": FRESH,
    }
    value.update(changes)
    return value


class FakeSupplierStore:
    def __init__(self, identifiers):
        self.identifiers = list(identifiers)

    def serving_generation_metadata(self, supplier):
        return {
            "run_id": "snapshot-2", "completed_at": FRESH,
            "product_count": len(self.identifiers), "scenario_count": len(self.identifiers),
            "product_catalog_coverage_complete": True,
        }

    def active_identifier_memberships(self, suppliers):
        return {identifier: ("qogita",) for identifier in self.identifiers}

    def active_candidates_for_identifier(self, supplier, identifier):
        return [{
            "canonical_ean": identifier, "gtin": identifier,
            "scenarios": [{
                "supplier": supplier, "scenario_id": f"{supplier}-{identifier}",
                "cost_gross_unit_eur": 8.0,
            }],
        }]


class FakeCache:
    def __init__(self, values):
        self.values = values
        self.index_calls = 0

    def index_completed_jobs(self, *, progress=None):
        self.index_calls += 1
        if progress is not None:
            progress("preparing_cache", 0, len(self.values))
            progress("preparing_cache", len(self.values), len(self.values))

    def get_many(self, identifiers):
        for identifier in identifiers:
            yield identifier, copy.deepcopy(self.values.get(identifier))


class FreshnessPolicyTests(unittest.TestCase):
    def setUp(self):
        self.policy = AmazonFreshnessPolicy()

    def test_fresh_dimensions_reuse_without_amazon_work(self):
        plan = plan_cached_product(resolved_cache(), policy=self.policy, now=NOW)
        self.assertEqual(plan["actions"], [PlanAction.CACHE_REUSE.value])

    def test_filter_changes_do_not_affect_freshness_plan(self):
        first = plan_cached_product(resolved_cache(), policy=self.policy, now=NOW)
        second = plan_cached_product(resolved_cache(), policy=self.policy, now=NOW)
        self.assertEqual(first, second)

    def test_stale_pricing_does_not_refresh_catalog(self):
        plan = plan_cached_product(
            resolved_cache(pricing_observed_at=STALE, competition_observed_at=STALE),
            policy=self.policy, now=NOW,
        )
        self.assertIn(PlanAction.REFRESH_PRICING.value, plan["actions"])
        self.assertNotIn(PlanAction.REFRESH_CATALOG.value, plan["actions"])

    def test_negative_catalog_has_shorter_ttl(self):
        cached = {
            "catalog_status": "not_found",
            "catalog_observed_at": (NOW - timedelta(days=8)).isoformat(),
        }
        self.assertEqual(
            plan_cached_product(cached, policy=self.policy, now=NOW)["primary_action"],
            PlanAction.REFRESH_CATALOG.value,
        )

    def test_fee_key_is_price_sensitive(self):
        self.assertNotEqual(fee_cache_key("B000", 10), fee_cache_key("B000", 10.01))
        self.assertTrue(reusable_fee({"fee_status": "valid", "fee_observed_at": FRESH}, self.policy, now=NOW))

    def test_qogita_snapshot_growth_separates_new_and_economics_reuse(self):
        rows = [
            plan_cached_product(resolved_cache(), policy=self.policy, now=NOW)
            for _ in range(500)
        ] + [
            plan_cached_product(None, policy=self.policy, now=NOW)
            for _ in range(5_000)
        ]
        counts = planning_counts(rows)
        self.assertEqual(counts["cache_reuse_count"], 500)
        self.assertEqual(counts["new_lookup_count"], 5_000)

    def test_one_hundred_twenty_thousand_item_counting_is_streaming_and_bounded(self):
        tracemalloc.start()
        counts = planning_counts(
            {"primary_action": PlanAction.NEW_LOOKUP.value}
            for _ in range(120_000)
        )
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        self.assertEqual(counts["requested_universe_count"], 120_000)
        self.assertLess(peak, 5 * 1024 * 1024)


class PlannerIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        root = Path(self.temporary.name)
        self.rotation = DiscoveryRotationStore(root / "rotation.sqlite3")
        self.identifiers = [valid_gtin(value) for value in range(6)]

    def tearDown(self):
        self.temporary.cleanup()

    def state(self, job_id, budget="all", margin=20):
        return {
            "job_id": job_id,
            "status": "initialized", "phase": "initialized",
            "selected_suppliers": ["qogita"], "run_budget": budget,
            "discovery_planner_version": "automatic_amazon_freshness_v1",
            "filters": {
                "bsr_min": 0, "bsr_max": 20000, "max_fba_sellers": 5,
                "max_total_sellers": 10, "minimum_margin": margin,
                "minimum_qogita_stock": 100,
            },
        }

    def test_all_catalog_means_full_universe_and_cycle_does_not_reset(self):
        cache = FakeCache({identifier: resolved_cache(
            amazon_listings=[{
                "asin": f"A{index}", "pricing_status": "success",
                "reference_price": 20, "bsr_beauty": 100,
                "pricing_observed_at": FRESH, "competition_observed_at": FRESH,
                "catalog_observed_at": FRESH,
            }]
        ) for index, identifier in enumerate(self.identifiers[:3])})
        supplier = FakeSupplierStore(self.identifiers)
        first = prepare_incremental_job(
            self.state("job-1"), supplier_store=supplier,
            rotation_store=self.rotation, amazon_cache=cache,
            freshness_policy=AmazonFreshnessPolicy(),
        )
        self.assertEqual(first["metadata"]["requested_universe_count"], 6)
        self.assertEqual(first["metadata"]["sampled_identifier_count"], 6)
        self.rotation.commit_catalog_results(
            "job-1", {identifier: "resolved" for identifier in self.identifiers},
        )
        second = prepare_incremental_job(
            self.state("job-2", margin=25), supplier_store=supplier,
            rotation_store=self.rotation, amazon_cache=cache,
            freshness_policy=AmazonFreshnessPolicy(),
        )
        self.assertEqual(second["metadata"]["sampled_identifier_count"], 6)
        self.assertEqual(second["metadata"]["rotation_cycle_id"], 1)
        self.assertEqual(second["metadata"]["cache_reuse_count"], 3)
        self.assertEqual(second["metadata"]["new_lookup_count"], 3)

    def test_bounded_priority_new_then_stale_then_fresh(self):
        values = {
            self.identifiers[0]: resolved_cache(),
            self.identifiers[1]: resolved_cache(pricing_observed_at=STALE),
        }
        prepared = prepare_incremental_job(
            self.state("job", budget=2), supplier_store=FakeSupplierStore(self.identifiers),
            rotation_store=self.rotation, amazon_cache=FakeCache(values),
            freshness_policy=AmazonFreshnessPolicy(),
        )
        candidates = list(prepared["candidates"])
        self.assertEqual(prepared["metadata"]["new_lookup_count"], 2)
        self.assertTrue(all(row["amazon_plan"]["primary_action"] == "NEW_LOOKUP" for row in candidates))

    def test_full_universe_mixed_plan_evaluates_all_products(self):
        identifiers = [valid_gtin(value) for value in range(1_000)]
        values = {
            identifier: resolved_cache()
            for identifier in identifiers[:400]
        }
        values.update({
            identifier: resolved_cache(
                pricing_observed_at=STALE, competition_observed_at=STALE,
            )
            for identifier in identifiers[400:700]
        })
        prepared = prepare_incremental_job(
            self.state("mixed-full"),
            supplier_store=FakeSupplierStore(identifiers),
            rotation_store=self.rotation,
            amazon_cache=FakeCache(values),
            freshness_policy=AmazonFreshnessPolicy(),
        )
        metadata = prepared["metadata"]
        self.assertEqual(metadata["requested_universe_count"], 1_000)
        self.assertEqual(metadata["sampled_identifier_count"], 1_000)
        self.assertEqual(metadata["cache_reuse_count"], 400)
        self.assertEqual(metadata["refresh_count"], 300)
        self.assertEqual(metadata["new_lookup_count"], 300)

    def test_qogita_growth_enters_same_cycle_without_reset(self):
        known = [valid_gtin(value) for value in range(100)]
        cache = FakeCache({identifier: resolved_cache() for identifier in known})
        first = prepare_incremental_job(
            self.state("growth-before"), supplier_store=FakeSupplierStore(known),
            rotation_store=self.rotation, amazon_cache=cache,
            freshness_policy=AmazonFreshnessPolicy(),
        )
        self.assertEqual(first["metadata"]["rotation_cycle_id"], 1)
        grown = known + [valid_gtin(value) for value in range(100, 5_100)]
        second = prepare_incremental_job(
            self.state("growth-after"), supplier_store=FakeSupplierStore(grown),
            rotation_store=self.rotation, amazon_cache=cache,
            freshness_policy=AmazonFreshnessPolicy(),
        )
        self.assertEqual(second["metadata"]["rotation_cycle_id"], 1)
        self.assertEqual(second["metadata"]["requested_universe_count"], 5_100)
        self.assertEqual(second["metadata"]["new_lookup_count"], 5_000)

    def test_stale_pricing_seeds_catalog_but_removes_pricing(self):
        identifier = self.identifiers[0]
        cache = FakeCache({identifier: resolved_cache(
            pricing_observed_at=STALE, competition_observed_at=STALE,
            amazon_listings=[{
                "asin": "A1", "pricing_status": "success", "reference_price": 20,
                "bsr_beauty": 100, "catalog_observed_at": FRESH,
                "pricing_observed_at": STALE, "competition_observed_at": STALE,
            }],
        )})
        prepared = prepare_incremental_job(
            self.state("job", budget=1), supplier_store=FakeSupplierStore([identifier]),
            rotation_store=self.rotation, amazon_cache=cache,
            freshness_policy=AmazonFreshnessPolicy(),
        )
        candidate = next(iter(prepared["candidates"]))
        self.assertEqual(candidate["catalog_status"], "resolved")
        self.assertNotIn("pricing_status", candidate["amazon_listings"][0])

    def test_cache_index_progress_flows_through_preparation_callback(self):
        events = []
        cache = FakeCache({self.identifiers[0]: resolved_cache()})
        prepare_incremental_job(
            self.state("progress", budget=1),
            supplier_store=FakeSupplierStore(self.identifiers[:1]),
            rotation_store=self.rotation, amazon_cache=cache,
            freshness_policy=AmazonFreshnessPolicy(),
            progress=lambda phase, current, total: events.append(
                (phase, current, total)
            ),
        )
        cache_events = [row for row in events if row[0] == "preparing_cache"]
        self.assertEqual(cache_events, [
            ("preparing_cache", 0, 1), ("preparing_cache", 1, 1),
        ])


class CacheIndexRegressionTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.store = DiscoveryIncrementalStore(
            Path(self.temporary.name) / "incremental.sqlite3"
        )
        self.rotation = DiscoveryRotationStore(
            Path(self.temporary.name) / "rotation.sqlite3"
        )
        self.identifiers = [valid_gtin(value) for value in range(3)]
        candidates = []
        for index, identifier in enumerate(self.identifiers):
            listing = {
                "asin": f"B{index:09d}", "bsr_beauty": 100 + index,
                "pricing_status": "success", "competition_status": "passed",
                "reference_price": 20 + index, "currency": "EUR",
                "catalog_observed_at": FRESH,
            }
            if index == 0:
                listing["amazon_observation_id"] = "observation-modern"
            candidates.append({
                "canonical_ean": identifier, "gtin": identifier,
                "catalog_status": "resolved", "scenarios": [],
                "amazon_listings": [listing],
            })
        self.store.create_job(
            {"job_id": "source", "status": "completed", "phase": "completed",
             "filters": {}},
            candidates,
        )
        self.store.upsert_observations("source", [
            {
                "observation_id": "observation-modern",
                "canonical_ean": self.identifiers[0], "asin": "B000000000",
                "reference_price": 20, "currency": "EUR",
                "fee_status": "valid", "observed_at": FRESH,
                "fee_last_attempt_at": FRESH,
            },
            {
                "observation_id": "observation-legacy",
                "canonical_ean": self.identifiers[1], "asin": "B000000001",
                "reference_price": 21, "currency": "EUR",
                "fee_status": "unavailable", "observed_at": FRESH,
                "fee_last_attempt_at": FRESH,
            },
        ])

    @staticmethod
    def state(job_id, budget="all", margin=20):
        return {
            "job_id": job_id,
            "status": "initialized", "phase": "initialized",
            "selected_suppliers": ["qogita"], "run_budget": budget,
            "discovery_planner_version": "automatic_amazon_freshness_v1",
            "filters": {
                "bsr_min": 0, "bsr_max": 20000, "max_fba_sellers": 5,
                "max_total_sellers": 10, "minimum_margin": margin,
                "minimum_qogita_stock": 100,
            },
        }

    def tearDown(self):
        self.temporary.cleanup()

    def _marker(self):
        with self.store._connect() as connection:
            return connection.execute(
                "SELECT * FROM discovery_amazon_cache_indexed_jobs "
                "WHERE source_job_id='source'"
            ).fetchone()

    def test_observations_are_selected_once_and_both_lookup_paths_are_equivalent(self):
        statements = []
        original = self.store._new_connection

        def traced_connection():
            connection = original()
            connection.set_trace_callback(statements.append)
            return connection

        self.store._new_connection = traced_connection
        progress = []
        cache = DiscoveryAmazonCache(self.store)
        self.assertEqual(
            cache.index_completed_jobs(
                progress=lambda *row: progress.append(row), batch_size=2,
            ),
            3,
        )
        payload_selects = [
            sql for sql in statements
            if "SELECT observation_id,observation_json,updated_at" in sql
            and "FROM discovery_observations" in sql
        ]
        self.assertEqual(len(payload_selects), 1)
        modern = cache.get(self.identifiers[0])
        legacy = cache.get(self.identifiers[1])
        without_observation = cache.get(self.identifiers[2])
        self.assertEqual(modern["fee_status"], "valid")
        self.assertEqual(legacy["fee_status"], "unavailable")
        self.assertIsNone(without_observation["fee_status"])
        self.assertEqual(modern["pricing_observed_at"], FRESH)
        self.assertEqual(legacy["competition_observed_at"], FRESH)
        currents = [row[1] for row in progress]
        self.assertEqual(currents, sorted(currents))
        self.assertEqual(progress[0], ("preparing_cache", 0, 3))
        self.assertEqual(progress[-1], ("preparing_cache", 3, 3))
        self.assertIn(("preparing_cache", 2, 3), progress)
        with self.store._connect() as connection:
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM discovery_amazon_cache").fetchone()[0],
                3,
            )
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM discovery_amazon_fee_cache").fetchone()[0],
                2,
            )

    def test_stable_revision_ignores_job_only_updates_and_remains_idempotent(self):
        cache = DiscoveryAmazonCache(self.store)
        self.assertEqual(cache.index_completed_jobs(), 3)
        marker = str(self._marker()["source_updated_at"])
        self.assertTrue(marker.startswith("cache-v2:"))
        self.store.set_phase("source", "export_pending", status="completed")
        self.store.add_checkpoint_bytes("source", 123)
        self.store.set_phase("source", "completed", status="completed")
        self.assertEqual(cache.index_completed_jobs(), 0)
        self.assertEqual(str(self._marker()["source_updated_at"]), marker)
        with self.store._connect() as connection:
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM discovery_amazon_cache").fetchone()[0],
                3,
            )

    def test_each_cache_relevant_dataset_changes_revision_and_reindexes(self):
        cache = DiscoveryAmazonCache(self.store)
        self.assertEqual(cache.index_completed_jobs(), 3)
        updates = (
            ("discovery_catalog_results", "diagnostics_json='{}'"),
            ("discovery_listings", "listing_json=listing_json"),
            ("discovery_observations", "observation_json=observation_json"),
        )
        for offset, (table, assignment) in enumerate(updates, start=1):
            with self.store._connect() as connection:
                connection.execute(
                    f"UPDATE {table} SET {assignment},updated_at=? WHERE job_id='source'",
                    (f"2099-01-01T00:00:0{offset}Z",),
                )
                connection.commit()
            self.assertEqual(cache.index_completed_jobs(), 3)

    def test_legacy_marker_is_upgraded_without_false_reindex(self):
        cache = DiscoveryAmazonCache(self.store)
        self.assertEqual(cache.index_completed_jobs(), 3)
        with self.store._connect() as connection:
            connection.execute(
                "UPDATE discovery_amazon_cache_indexed_jobs "
                "SET source_updated_at='legacy-job-updated-at',"
                "indexed_at='2099-01-01T00:00:00Z' WHERE source_job_id='source'"
            )
            connection.commit()
        self.assertEqual(cache.index_completed_jobs(), 0)
        self.assertTrue(str(self._marker()["source_updated_at"]).startswith("cache-v2:"))

    def test_cache_index_is_additive_and_preserves_historical_job(self):
        root = Path(self.temporary.name)
        store = DiscoveryIncrementalStore(root / "incremental.sqlite3")
        metadata = {
            "job_id": "historical", "status": "completed", "phase": "completed",
            "filters": {}, "sentinel": "unchanged",
        }
        store.create_job(metadata, [{
            "canonical_ean": self.identifiers[0], "gtin": self.identifiers[0],
            "catalog_status": "not_found", "scenarios": [],
        }])
        before = store.summary("historical")
        DiscoveryAmazonCache(store).index_completed_jobs()
        after = store.summary("historical")
        self.assertEqual(before["sentinel"], after["sentinel"])
        self.assertEqual(before["catalog_status_counts"], after["catalog_status_counts"])

    def test_preview_is_read_only_and_does_not_misclassify_unindexed_history(self):
        root = Path(self.temporary.name)
        store = DiscoveryIncrementalStore(root / "preview.sqlite3")
        known, new = self.identifiers[:2]
        store.create_job(
            {"job_id": "historical", "status": "completed", "phase": "completed", "filters": {}},
            [{"canonical_ean": known, "gtin": known, "catalog_status": "not_found",
              "scenarios": []}],
        )
        cache = DiscoveryAmazonCache(store)
        counts = cache.preview_counts([known, new], AmazonFreshnessPolicy())
        self.assertEqual(counts["refresh_count"], 1)
        self.assertEqual(counts["new_lookup_count"], 1)
        with store._connect() as connection:
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM discovery_amazon_cache").fetchone()[0],
                0,
            )

    def test_completed_job_cache_materializes_exact_listing_and_fee_payloads(self):
        root = Path(self.temporary.name)
        store = DiscoveryIncrementalStore(root / "cache.sqlite3")
        identifier = self.identifiers[0]
        store.create_job(
            {"job_id": "source", "status": "completed", "phase": "completed", "filters": {}},
            [{
                "canonical_ean": identifier, "gtin": identifier,
                "catalog_status": "resolved", "scenarios": [],
                "amazon_listings": [{
                    "asin": "B000TEST", "bsr_beauty": 100,
                    "pricing_status": "success", "competition_status": "passed",
                    "reference_price": 20, "currency": "EUR",
                    "catalog_observed_at": FRESH, "pricing_observed_at": FRESH,
                    "competition_observed_at": FRESH,
                }],
            }],
        )
        store.upsert_observations("source", [{
            "observation_id": "fee-source", "canonical_ean": identifier,
            "asin": "B000TEST", "reference_price": 20, "currency": "EUR",
            "fee_status": "valid", "fee_estimate": {"total_fees": 5},
            "observed_at": FRESH, "fee_last_attempt_at": FRESH,
        }])
        cache = DiscoveryAmazonCache(store)
        cache.index_completed_jobs()
        cached = cache.get(identifier)
        self.assertEqual(
            plan_cached_product(cached, policy=AmazonFreshnessPolicy(), now=NOW)["primary_action"],
            PlanAction.CACHE_REUSE.value,
        )
        with store._connect() as connection:
            columns = {row["name"] for row in connection.execute(
                "PRAGMA table_info(discovery_amazon_cache)"
            )}
            listing_row = connection.execute(
                "SELECT listings_json,payload_schema_version,payload_sha256 "
                "FROM discovery_amazon_cache"
            ).fetchone()
            fee_row = connection.execute(
                "SELECT observation_json,payload_schema_version,payload_sha256 "
                "FROM discovery_amazon_fee_cache"
            ).fetchone()
        self.assertIn("listings_json", columns)
        self.assertEqual(
            listing_row["payload_schema_version"], CACHE_PAYLOAD_SCHEMA_VERSION,
        )
        self.assertEqual(
            hashlib.sha256(listing_row["listings_json"].encode()).hexdigest(),
            listing_row["payload_sha256"],
        )
        self.assertEqual(
            fee_row["payload_schema_version"], CACHE_PAYLOAD_SCHEMA_VERSION,
        )
        self.assertEqual(
            hashlib.sha256(fee_row["observation_json"].encode()).hexdigest(),
            fee_row["payload_sha256"],
        )
        self.assertEqual(json.loads(fee_row["observation_json"])["fee_estimate"], {
            "total_fees": 5,
        })

    def _fee_tie_winner(self, *, original_first=True, newer_reused=False):
        root = Path(self.temporary.name)
        store = DiscoveryIncrementalStore(
            root / f"fee-tie-{original_first}-{newer_reused}.sqlite3"
        )
        identifier = self.identifiers[0]
        fee_observed_at = "2026-08-01T10:00:00Z"
        jobs = [
            ("original", {
                "observation_id": "shared-observation",
                "canonical_ean": identifier,
                "asin": "B000FEETIE",
                "reference_price": 20,
                "currency": "EUR",
                "fee_status": "valid",
                "fee_estimate": {"total_fees": 5},
                "fee_last_attempt_at": fee_observed_at,
                "observed_at": "2026-08-01T10:00:00Z",
                "bsr_beauty": 100,
            }),
            ("reused", {
                "observation_id": "shared-observation",
                "canonical_ean": identifier,
                "asin": "B000FEETIE",
                "reference_price": 20,
                "currency": "EUR",
                "fee_status": "valid",
                "fee_estimate": {"total_fees": 5},
                "fee_last_attempt_at": (
                    "2026-08-02T10:00:00Z" if newer_reused else fee_observed_at
                ),
                "fee_cache_reused": True,
                "observed_at": "2026-08-03T10:00:00Z",
                "bsr_beauty": 999,
            }),
        ]
        for job_id, observation in jobs:
            store.create_job(
                {"job_id": job_id, "status": "completed", "phase": "completed",
                 "filters": {}},
                [{
                    "canonical_ean": identifier,
                    "gtin": identifier,
                    "catalog_status": "resolved",
                    "scenarios": [],
                    "amazon_listings": [{
                        "asin": "B000FEETIE",
                        "reference_price": 20,
                        "currency": "EUR",
                    }],
                }],
            )
            store.upsert_observations(job_id, [observation])
        ordered = ("original", "reused") if original_first else ("reused", "original")
        with store._connect() as connection:
            for offset, job_id in enumerate(ordered):
                connection.execute(
                    "UPDATE discovery_incremental_jobs SET updated_at=? WHERE job_id=?",
                    (f"2026-08-0{offset + 1}T00:00:00Z", job_id),
                )
            connection.commit()
        cache = DiscoveryAmazonCache(store)
        cache.index_completed_jobs()
        with store._connect() as connection:
            row = connection.execute(
                "SELECT source_job_id,fee_observed_at,observation_json "
                "FROM discovery_amazon_fee_cache"
            ).fetchone()
        return str(row["source_job_id"]), str(row["fee_observed_at"]), json.loads(
            row["observation_json"]
        )

    def test_fee_cache_newer_observation_wins_even_when_reused(self):
        source, observed_at, value = self._fee_tie_winner(
            original_first=True, newer_reused=True,
        )
        self.assertEqual(source, "reused")
        self.assertEqual(observed_at, "2026-08-02T10:00:00Z")
        self.assertTrue(value["fee_cache_reused"])

    def test_equal_fee_timestamp_original_beats_reused_in_either_replay_order(self):
        first = self._fee_tie_winner(original_first=True)
        second = self._fee_tie_winner(original_first=False)
        self.assertEqual(first, second)
        source, observed_at, value = first
        self.assertEqual(source, "original")
        self.assertEqual(observed_at, "2026-08-01T10:00:00Z")
        self.assertNotIn("fee_cache_reused", value)
        self.assertEqual(value["fee_estimate"], {"total_fees": 5})
        self.assertEqual(value["bsr_beauty"], 100)

    def test_equal_fee_timestamp_non_fee_updates_do_not_replace_original(self):
        _, _, value = self._fee_tie_winner(original_first=True)
        self.assertEqual(value["bsr_beauty"], 100)
        self.assertEqual(value["observed_at"], "2026-08-01T10:00:00Z")
        self.assertNotIn("fee_cache_reused", value)

    def test_additive_cache_migration_upgrades_legacy_schema_idempotently(self):
        root = Path(self.temporary.name)
        store = DiscoveryIncrementalStore(root / "legacy.sqlite3")
        store.initialize()
        with store._connect() as connection:
            connection.executescript("""
                CREATE TABLE discovery_amazon_cache (
                    canonical_identifier TEXT PRIMARY KEY, source_job_id TEXT NOT NULL,
                    catalog_status TEXT NOT NULL, catalog_observed_at TEXT NOT NULL,
                    freshness_json TEXT NOT NULL DEFAULT '{}', updated_at TEXT NOT NULL
                );
                CREATE TABLE discovery_amazon_fee_cache (
                    fee_cache_key TEXT PRIMARY KEY, source_job_id TEXT NOT NULL,
                    observation_id TEXT NOT NULL, fee_status TEXT NOT NULL,
                    fee_observed_at TEXT NOT NULL,
                    observation_json TEXT NOT NULL DEFAULT '{}', updated_at TEXT NOT NULL
                );
                CREATE TABLE discovery_amazon_cache_indexed_jobs (
                    source_job_id TEXT PRIMARY KEY, source_updated_at TEXT NOT NULL,
                    indexed_at TEXT NOT NULL
                );
            """)
        cache = DiscoveryAmazonCache(store)
        cache.initialize()
        cache.initialize()
        with store._connect() as connection:
            catalog_columns = {
                row["name"] for row in connection.execute(
                    "PRAGMA table_info(discovery_amazon_cache)"
                )
            }
            fee_columns = {
                row["name"] for row in connection.execute(
                    "PRAGMA table_info(discovery_amazon_fee_cache)"
                )
            }
            marker_columns = {
                row["name"] for row in connection.execute(
                    "PRAGMA table_info(discovery_amazon_cache_indexed_jobs)"
                )
            }
        self.assertTrue({
            "listings_json", "payload_schema_version", "payload_sha256",
            "materialized_at",
        }.issubset(catalog_columns))
        self.assertTrue({
            "payload_schema_version", "payload_sha256", "materialized_at",
        }.issubset(fee_columns))
        self.assertTrue({
            "materialization_version", "verification_state", "catalog_count",
            "listing_count", "fee_count", "aggregate_sha256", "verified_at",
        }.issubset(marker_columns))

    def test_materialized_get_get_many_and_fee_do_not_read_source_payload_tables(self):
        cache = DiscoveryAmazonCache(self.store)
        cache.index_completed_jobs()
        statements = []
        original = self.store._new_connection

        def traced_connection():
            connection = original()
            connection.set_trace_callback(statements.append)
            return connection

        self.store._new_connection = traced_connection
        expected = cache.get(self.identifiers[0])
        batch = dict(cache.get_many(self.identifiers))
        fee = cache.fee("B000000000", 20)
        source_reads = [
            sql for sql in statements
            if sql.lstrip().upper().startswith("SELECT")
            and (
                "FROM discovery_listings" in sql
                or "FROM discovery_observations" in sql
            )
        ]
        self.assertEqual(source_reads, [])
        self.assertEqual(batch[self.identifiers[0]], expected)
        self.assertEqual(fee["fee_status"], "valid")
        self.assertEqual(fee["canonical_ean"], self.identifiers[0])
        self.assertEqual(fee["asin"], "B000000000")

    def test_unverified_or_missing_job_marker_forces_catalog_source_fallback(self):
        for marker_state in ("failed", "pending", "missing"):
            with self.subTest(marker_state=marker_state):
                cache = DiscoveryAmazonCache(self.store)
                cache.index_completed_jobs()
                with self.store._connect() as connection:
                    if marker_state == "missing":
                        connection.execute(
                            "DELETE FROM discovery_amazon_cache_indexed_jobs "
                            "WHERE source_job_id='source'"
                        )
                    else:
                        connection.execute(
                            "UPDATE discovery_amazon_cache_indexed_jobs "
                            "SET verification_state=? WHERE source_job_id='source'",
                            (marker_state,),
                        )
                    connection.commit()
                statements = []
                original = self.store._new_connection

                def traced_connection():
                    connection = original()
                    connection.set_trace_callback(statements.append)
                    return connection

                self.store._new_connection = traced_connection
                try:
                    cached = cache.get(self.identifiers[0])
                finally:
                    self.store._new_connection = original
                self.assertEqual(cached["catalog_status"], "resolved")
                self.assertTrue(any(
                    "FROM discovery_listings" in sql for sql in statements
                ))
                with self.store._connect() as connection:
                    revision = cache._source_revision(connection, "source")[0]
                    connection.execute(
                        """INSERT INTO discovery_amazon_cache_indexed_jobs
                           (source_job_id,source_updated_at,indexed_at,
                            materialization_version,verification_state)
                           VALUES ('source',?,? ,?,'verified')
                           ON CONFLICT(source_job_id) DO UPDATE SET
                             source_updated_at=excluded.source_updated_at,
                             indexed_at=excluded.indexed_at,
                             materialization_version=excluded.materialization_version,
                             verification_state=excluded.verification_state""",
                        (revision, FRESH, CACHE_PAYLOAD_SCHEMA_VERSION),
                    )
                    connection.commit()

    def test_failed_job_marker_forces_fee_source_fallback(self):
        cache = DiscoveryAmazonCache(self.store)
        cache.index_completed_jobs()
        with self.store._connect() as connection:
            connection.execute(
                "UPDATE discovery_amazon_cache_indexed_jobs "
                "SET verification_state='failed' WHERE source_job_id='source'"
            )
            connection.commit()
        statements = []
        original = self.store._new_connection

        def traced_connection():
            connection = original()
            connection.set_trace_callback(statements.append)
            return connection

        self.store._new_connection = traced_connection
        try:
            fee = cache.fee("B000000000", 20)
        finally:
            self.store._new_connection = original
        self.assertEqual(fee["observation_id"], "observation-modern")
        self.assertTrue(any(
            "FROM discovery_observations" in sql for sql in statements
        ))

    def test_mixed_marker_batch_falls_back_only_for_unverified_subset(self):
        cache = DiscoveryAmazonCache(self.store)
        cache.index_completed_jobs()
        verified_identifier = valid_gtin(99)
        self.store.create_job(
            {"job_id": "verified-source", "status": "completed",
             "phase": "completed", "filters": {}},
            [{
                "canonical_ean": verified_identifier,
                "gtin": verified_identifier,
                "catalog_status": "resolved",
                "scenarios": [],
                "amazon_listings": [{
                    "asin": "B000VERIFIED", "reference_price": 30,
                    "currency": "EUR", "catalog_observed_at": FRESH,
                }],
            }],
        )
        cache.index_completed_jobs()
        with self.store._connect() as connection:
            connection.execute(
                "UPDATE discovery_amazon_cache_indexed_jobs "
                "SET verification_state='failed' WHERE source_job_id='source'"
            )
            connection.commit()
        statements = []
        original = self.store._new_connection

        def traced_connection():
            connection = original()
            connection.set_trace_callback(statements.append)
            return connection

        self.store._new_connection = traced_connection
        try:
            batch = dict(cache.get_many([
                self.identifiers[0], verified_identifier,
            ]))
        finally:
            self.store._new_connection = original
        source_reads = [
            sql for sql in statements if "FROM discovery_listings" in sql
        ]
        self.assertEqual(len(source_reads), 1)
        self.assertIn(self.identifiers[0], source_reads[0])
        self.assertNotIn(verified_identifier, source_reads[0])
        self.assertEqual(
            batch[verified_identifier]["amazon_listings"][0]["asin"],
            "B000VERIFIED",
        )

    def test_materialized_negative_and_ambiguous_preserve_order_and_taxonomy(self):
        root = Path(self.temporary.name)
        store = DiscoveryIncrementalStore(root / "contracts.sqlite3")
        negative, ambiguous = self.identifiers[:2]
        listings = [
            {
                "asin": "B000000001", "title": "First", "bsr_beauty": 101,
                "browse_classification": {
                    "classification_id": "6306900031",
                    "path_ids": ["3760911", "6306900031"],
                },
            },
            {
                "asin": "B000000002", "title": "Second", "bsr_beauty": 102,
                "browse_classification": {
                    "classification_id": "6306897031",
                    "path_ids": ["3760911", "6306897031"],
                },
            },
        ]
        store.create_job(
            {"job_id": "contracts", "status": "completed", "phase": "completed", "filters": {}},
            [
                {"canonical_ean": negative, "gtin": negative,
                 "catalog_status": "not_found", "scenarios": []},
                {"canonical_ean": ambiguous, "gtin": ambiguous,
                 "catalog_status": "ambiguous", "scenarios": [],
                 "amazon_listings": listings},
            ],
        )
        cache = DiscoveryAmazonCache(store)
        cache.index_completed_jobs()
        statements = []
        original = store._new_connection

        def traced_connection():
            connection = original()
            connection.set_trace_callback(statements.append)
            return connection

        store._new_connection = traced_connection
        self.assertEqual(cache.get(negative)["amazon_listings"], [])
        cached = cache.get(ambiguous)
        self.assertEqual([row["asin"] for row in cached["amazon_listings"]], [
            "B000000001", "B000000002",
        ])
        self.assertEqual(
            cached["amazon_listings"][1]["browse_classification"],
            listings[1]["browse_classification"],
        )
        self.assertFalse(any("FROM discovery_listings" in sql for sql in statements))

    def test_corrupt_materialized_payload_falls_back_to_legacy_source(self):
        cache = DiscoveryAmazonCache(self.store)
        cache.index_completed_jobs()
        expected = cache.get(self.identifiers[0])
        with self.store._connect() as connection:
            connection.execute(
                "UPDATE discovery_amazon_cache SET payload_sha256='corrupt' "
                "WHERE canonical_identifier=?",
                (self.identifiers[0],),
            )
            connection.commit()
        statements = []
        original = self.store._new_connection

        def traced_connection():
            connection = original()
            connection.set_trace_callback(statements.append)
            return connection

        self.store._new_connection = traced_connection
        self.assertEqual(cache.get(self.identifiers[0]), expected)
        self.assertTrue(any("FROM discovery_listings" in sql for sql in statements))

    def test_legacy_null_payload_and_fee_use_source_fallback(self):
        cache = DiscoveryAmazonCache(self.store)
        cache.index_completed_jobs()
        with self.store._connect() as connection:
            connection.execute(
                "UPDATE discovery_amazon_cache SET listings_json=NULL,"
                "payload_schema_version=NULL,payload_sha256=NULL "
                "WHERE canonical_identifier=?",
                (self.identifiers[0],),
            )
            connection.execute(
                "UPDATE discovery_amazon_fee_cache SET payload_schema_version=NULL,"
                "payload_sha256=NULL WHERE fee_cache_key=?",
                (fee_cache_key("B000000000", 20),),
            )
            connection.commit()
        statements = []
        original = self.store._new_connection

        def traced_connection():
            connection = original()
            connection.set_trace_callback(statements.append)
            return connection

        self.store._new_connection = traced_connection
        self.assertEqual(cache.get(self.identifiers[0])["catalog_status"], "resolved")
        self.assertEqual(cache.fee("B000000000", 20)["fee_status"], "valid")
        self.assertTrue(any("FROM discovery_listings" in sql for sql in statements))
        self.assertTrue(any("FROM discovery_observations" in sql for sql in statements))

    def test_corrupt_materialized_fee_falls_back_to_source(self):
        cache = DiscoveryAmazonCache(self.store)
        cache.index_completed_jobs()
        key = fee_cache_key("B000000000", 20)
        with self.store._connect() as connection:
            connection.execute(
                "UPDATE discovery_amazon_fee_cache SET payload_sha256='corrupt' "
                "WHERE fee_cache_key=?", (key,),
            )
            connection.commit()
        statements = []
        original = self.store._new_connection

        def traced_connection():
            connection = original()
            connection.set_trace_callback(statements.append)
            return connection

        self.store._new_connection = traced_connection
        self.assertEqual(cache.fee("B000000000", 20)["observation_id"], "observation-modern")
        self.assertTrue(any("FROM discovery_observations" in sql for sql in statements))

    def test_materialized_cache_serves_direct_lookup_without_source_reads(self):
        cache = DiscoveryAmazonCache(self.store)
        cache.index_completed_jobs()
        statements = []
        original = self.store._new_connection

        def traced_connection():
            connection = original()
            connection.set_trace_callback(statements.append)
            return connection

        self.store._new_connection = traced_connection
        resolver = DirectAmazonLookup(
            cache=cache,
            catalog_lookup=lambda *_: self.fail("Catalog must not be called"),
            pricing_lookup=lambda *_: self.fail("Pricing must not be called"),
            freshness_policy=AmazonFreshnessPolicy(), now=lambda: NOW,
        )
        result = resolver.lookup(self.identifiers[0])
        self.assertEqual(result["catalog_status"], "resolved")
        self.assertFalse(any(
            "FROM discovery_listings" in sql or "FROM discovery_observations" in sql
            for sql in statements
        ))

    def test_canonical_payload_hash_is_order_independent_and_semantic(self):
        from discovery_freshness import _canonical_payload

        first_payload, first_hash = _canonical_payload({"b": 2, "a": {"y": 1, "x": 0}})
        second_payload, second_hash = _canonical_payload({"a": {"x": 0, "y": 1}, "b": 2})
        _, changed_hash = _canonical_payload({"a": {"x": 0, "y": 2}, "b": 2})
        self.assertEqual(first_payload, second_payload)
        self.assertEqual(first_hash, second_hash)
        self.assertNotEqual(first_hash, changed_hash)

    def test_materialization_failure_preserves_valid_cache_and_unverified_marker(self):
        cache = DiscoveryAmazonCache(self.store)
        cache.index_completed_jobs()
        with self.store._connect() as connection:
            before = connection.execute(
                "SELECT listings_json,payload_sha256 FROM discovery_amazon_cache "
                "WHERE canonical_identifier=?", (self.identifiers[0],),
            ).fetchone()
            before_marker = dict(self._marker())
            connection.execute(
                "UPDATE discovery_listings SET updated_at='2099-01-01T00:00:00Z' "
                "WHERE job_id='source' AND canonical_identifier=?",
                (self.identifiers[0],),
            )
            connection.commit()
        with patch(
            "discovery_freshness._canonical_payload",
            side_effect=ValueError("materialization failed"),
        ):
            with self.assertRaisesRegex(ValueError, "materialization failed"):
                cache.index_completed_jobs()
        with self.store._connect() as connection:
            after = connection.execute(
                "SELECT listings_json,payload_sha256 FROM discovery_amazon_cache "
                "WHERE canonical_identifier=?", (self.identifiers[0],),
            ).fetchone()
            after_marker = dict(self._marker())
        self.assertEqual(tuple(before), tuple(after))
        self.assertEqual(before_marker, after_marker)

    def test_materialization_marker_is_verified_with_counts_and_stable_hash(self):
        cache = DiscoveryAmazonCache(self.store)
        cache.index_completed_jobs()
        marker = self._marker()
        self.assertEqual(marker["materialization_version"], CACHE_PAYLOAD_SCHEMA_VERSION)
        self.assertEqual(marker["verification_state"], "verified")
        self.assertEqual(marker["catalog_count"], 3)
        self.assertEqual(marker["listing_count"], 3)
        self.assertEqual(marker["fee_count"], 2)
        self.assertEqual(len(marker["aggregate_sha256"]), 64)
        self.assertTrue(marker["verified_at"])

    def test_materialized_and_legacy_results_have_identical_freshness_contract(self):
        cache = DiscoveryAmazonCache(self.store)
        cache.index_completed_jobs()
        materialized = dict(cache.get_many(self.identifiers))
        materialized_plans = {
            identifier: plan_cached_product(value, policy=AmazonFreshnessPolicy(), now=NOW)
            for identifier, value in materialized.items()
        }
        with self.store._connect() as connection:
            connection.execute(
                "UPDATE discovery_amazon_cache SET listings_json=NULL,"
                "payload_schema_version=NULL,payload_sha256=NULL"
            )
            connection.execute(
                "UPDATE discovery_amazon_fee_cache SET payload_schema_version=NULL,"
                "payload_sha256=NULL"
            )
            connection.commit()
        legacy = dict(cache.get_many(self.identifiers))
        legacy_plans = {
            identifier: plan_cached_product(value, policy=AmazonFreshnessPolicy(), now=NOW)
            for identifier, value in legacy.items()
        }
        self.assertEqual(materialized, legacy)
        self.assertEqual(materialized_plans, legacy_plans)

    def test_running_and_resumable_jobs_are_not_indexed_or_materialized(self):
        identifier = valid_gtin(999)
        self.store.create_job(
            {"job_id": "running", "status": "running", "phase": "catalog", "filters": {}},
            [{"canonical_ean": identifier, "gtin": identifier,
              "catalog_status": "not_found", "scenarios": []}],
        )
        cache = DiscoveryAmazonCache(self.store)
        cache.index_completed_jobs()
        with self.store._connect() as connection:
            self.assertIsNone(connection.execute(
                "SELECT 1 FROM discovery_amazon_cache WHERE canonical_identifier=?",
                (identifier,),
            ).fetchone())
            self.assertIsNone(connection.execute(
                "SELECT 1 FROM discovery_amazon_cache_indexed_jobs "
                "WHERE source_job_id='running'",
            ).fetchone())

    def test_one_thousand_fresh_cached_products_complete_without_amazon_calls(self):
        root = Path(self.temporary.name)
        store = DiscoveryIncrementalStore(root / "reuse.sqlite3")
        identifiers = [valid_gtin(value) for value in range(1_000)]
        store.create_job(
            {"job_id": "source", "status": "completed", "phase": "completed", "filters": {}},
            ({
                "canonical_ean": identifier, "gtin": identifier,
                "catalog_status": "resolved", "scenarios": [],
                "amazon_listings": [{
                    "asin": f"B{index:09d}", "compatibility_status": "compatible",
                    "beauty_status": "display_group_beauty", "bsr_beauty": 100,
                    "pricing_status": "success", "competition_status": "passed",
                    "fba_sellers": 1, "total_sellers": 2,
                    "reference_price": 25, "currency": "EUR",
                    "catalog_observed_at": FRESH, "pricing_observed_at": FRESH,
                    "competition_observed_at": FRESH,
                }],
            } for index, identifier in enumerate(identifiers)),
        )
        store.upsert_observations("source", ({
            "observation_id": f"fee-{index}", "canonical_ean": identifier,
            "asin": f"B{index:09d}", "reference_price": 25, "currency": "EUR",
            "fee_status": "valid", "fee_estimate": {
                "fba_fee": 5, "referral_fee": 3, "total_fees": 8,
            }, "observed_at": FRESH, "fee_last_attempt_at": FRESH,
        } for index, identifier in enumerate(identifiers)))
        cache = DiscoveryAmazonCache(store)
        cache.index_completed_jobs()
        prepared = prepare_incremental_job(
            self.state("reused"), supplier_store=FakeSupplierStore(identifiers),
            rotation_store=self.rotation, amazon_cache=cache,
            freshness_policy=AmazonFreshnessPolicy(),
        )
        self.assertEqual(prepared["metadata"]["requested_universe_count"], 1_000)
        self.assertEqual(prepared["metadata"]["cache_reuse_count"], 1_000)
        self.assertEqual(prepared["metadata"]["refresh_count"], 0)
        self.assertEqual(prepared["metadata"]["new_lookup_count"], 0)
        store.create_job(prepared["metadata"], prepared["candidates"])

        def forbidden(*args, **kwargs):
            raise AssertionError("fresh cache must not call Amazon")

        class Governor:
            def before_next_batch(self):
                return None

        result = run_incremental_discovery(
            "reused", store=store,
            metadata_store=LightweightCheckpointStore(root / "checkpoints"),
            catalog_batch=forbidden, pricing_batch=forbidden, fees_batch=forbidden,
            token_provider=None, rotation_store=self.rotation, amazon_cache=cache,
            freshness_policy=AmazonFreshnessPolicy(), resource_governor=Governor(),
            catalog_batch_interval=0, pricing_batch_interval=0, fee_batch_interval=0,
        )
        self.assertEqual(result["status"], "completed")


if __name__ == "__main__":
    unittest.main()
