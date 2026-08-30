import json
import os
import tempfile
import tracemalloc
import unittest
import requests
from pathlib import Path

from discovery_incremental import (
    DiscoveryIncrementalStore,
    IncrementalCandidateCollection,
    LightweightCheckpointStore,
    iter_json_array,
    read_legacy_metadata,
    prepare_incremental_job,
)
from discovery_resources import (
    DiscoveryResourceGovernor,
    ResourcePolicy,
    ResourceSnapshot,
)
from discovery_incremental_runner import run_incremental_discovery


def catalog_success(identifiers, *_):
    return {
        value: {
            "status": "resolved", "asin": f"B{index:09d}",
            "amazon_title": f"Amazon {value}", "amazon_brand": "Brand",
            "bsr_beauty": 5000, "beauty_status": "display_group_beauty",
            "product_type": "BEAUTY",
        }
        for index, value in enumerate(identifiers, start=1)
    }


def pricing_success(asins, *_):
    return {
        asin: {
            "status": "success", "Venditori FBA": 1, "Venditori totali": 2,
            "reference_price": 30, "price_source": "buy_box",
        }
        for asin in asins
    }


def fee_success(asin, identifier):
    return {"FeesEstimateResult": {
        "Status": "Success",
        "FeesEstimateIdentifier": {
            "IdValue": asin, "SellerInputIdentifier": identifier,
            "PriceToEstimateFees": {
                "ListingPrice": {"Amount": 30, "CurrencyCode": "EUR"}
            },
        },
        "FeesEstimate": {"FeeDetailList": [
            {"FeeType": "ReferralFee", "FinalFee": {"Amount": 4.5}},
            {"FeeType": "FBAFees", "FinalFee": {"Amount": 4}},
        ]},
    }}


def fee_internal_error(asin, identifier):
    return {"FeesEstimateResult": {
        "Status": "ServerError",
        "FeesEstimateIdentifier": {
            "IdValue": asin, "SellerInputIdentifier": identifier,
        },
        "Error": {
            "Code": "InternalError", "Type": "Receiver",
            "Message": "There is an internal service failure.",
        },
    }}


def candidate(index, status=None, listings=0):
    identifier = f"{index:013d}"
    row = {
        "canonical_ean": identifier, "gtin": identifier,
        "identifier_type": "EAN", "product_key": f"product-{index}",
        "title": f"Product {index}",
        "scenarios": [{
            "scenario_id": f"scenario-{index}", "supplier": "abw",
            "scenario_label": "Standard", "cost_gross_unit_eur": "1.00",
        }],
        "amazon_listings": [
            {"asin": f"ASIN{index:06d}{value}", "compatibility_status": "compatible"}
            for value in range(listings)
        ],
    }
    if status:
        row["catalog_status"] = status
    return row


class IncrementalStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.store = DiscoveryIncrementalStore(root / "discovery.sqlite3")
        self.checkpoints = LightweightCheckpointStore(root / "checkpoints")

    def tearDown(self):
        self.temp.cleanup()

    def test_batch_commit_is_idempotent_and_resume_skips_committed(self):
        self.store.create_job(
            {"job_id": "job", "filters": {}, "phase": "suppliers_loaded"},
            [candidate(1), candidate(2), candidate(3)],
        )
        batch = self.store.pending_catalog_batch("job", 2)
        for row in batch:
            row["catalog_status"] = "not_found"
        self.store.commit_catalog_batch("job", batch, batch_number=1)
        self.store.commit_catalog_batch("job", batch, batch_number=1)
        pending = self.store.pending_catalog_batch("job", 20)
        self.assertEqual([row["product_key"] for row in pending], ["product-3"])
        self.assertEqual(self.store.summary("job")["catalog_completed_count"], 2)

    def test_crash_before_commit_repeats_only_inflight_batch(self):
        self.store.create_job(
            {"job_id": "job", "filters": {}, "phase": "suppliers_loaded"},
            [candidate(1), candidate(2)],
        )
        first = self.store.pending_catalog_batch("job", 1)
        first[0]["catalog_status"] = "resolved"  # simulated process death here
        again = self.store.pending_catalog_batch("job", 1)
        self.assertEqual(again[0]["canonical_ean"], first[0]["canonical_ean"])

    def test_multi_listing_and_scenario_identity_have_no_duplicates(self):
        row = candidate(1, "ambiguous", listings=2)
        self.store.create_job(
            {"job_id": "job", "filters": {}, "phase": "suppliers_loaded"}, [row]
        )
        self.store.update_candidates("job", [row])
        self.store.update_candidates("job", [row])
        counts = self.store.counts("job")
        self.assertEqual(counts["scenarios"], 1)
        self.assertEqual(counts["listings"], 2)

    def test_lightweight_checkpoint_never_serializes_heavy_collections(self):
        state = {
            "job_id": "job", "phase": "catalog", "status": "running",
            "candidates": [candidate(value) for value in range(5000)],
            "rotation_selected_identifiers": [str(value) for value in range(5000)],
            "progress_current": 20, "progress_total": 5000,
        }
        total_written = sum(self.checkpoints.save(state) for _ in range(100))
        self.assertLess(self.checkpoints.path("job").stat().st_size, 4096)
        self.assertLess(total_written, 500_000)

    def test_large_120k_metadata_is_linear_and_checkpoint_is_small(self):
        tracemalloc.start()
        self.store.create_job(
            {"job_id": "large", "filters": {}, "phase": "suppliers_loaded"},
            (candidate(value) for value in range(120_000)),
        )
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        summary = self.store.summary("large")
        size = self.checkpoints.save(summary)
        self.assertEqual(summary["selected_count"], 120_000)
        self.assertLess(size, 16_384)
        self.assertLess(peak, 32 * 1024 * 1024)
        self.assertLess(self.store.file_sizes()["database_bytes"], 320 * 1024 * 1024)

    def test_streaming_legacy_migration_preserves_counts_and_file(self):
        legacy = Path(self.temp.name) / "legacy.json"
        payload = {
            "job_id": "legacy", "schema_version": 4, "status": "running",
            "phase": "suppliers_loaded", "filters": {"bsr_min": 1},
            "selected_suppliers": ["abw"],
            "supplier_snapshot_set": {"abw": {"snapshot_id": "snapshot"}},
            "sampled_identifier_count": 3,
            "rotation_scope": "scope", "rotation_cycle_id": 1,
            "candidates": [
                candidate(1, "resolved", 1), candidate(2, "not_found"), candidate(3),
            ],
        }
        legacy.write_text(json.dumps(payload), encoding="utf-8")
        metadata = read_legacy_metadata(legacy)
        summary = self.store.migrate_legacy_checkpoint(legacy, metadata)
        self.assertTrue(legacy.exists())
        self.assertEqual(summary["selected_count"], 3)
        self.assertEqual(summary["catalog_completed_count"], 2)
        self.assertEqual(summary["catalog_pending_count"], 1)
        self.assertEqual(summary["listing_count"], 1)
        self.assertEqual(len(list(iter_json_array(legacy, "candidates"))), 3)

    def test_incremental_collection_is_reiterable(self):
        self.store.create_job(
            {"job_id": "job", "filters": {}, "phase": "completed"},
            [candidate(1), candidate(2)],
        )
        collection = IncrementalCandidateCollection(self.store, "job")
        self.assertEqual(len(collection), 2)
        self.assertEqual(len(list(collection)), len(list(collection)))

    def test_incremental_runner_commits_catalog_once_and_completes_offline(self):
        filters = {
            "bsr_min": 1, "bsr_max": 30_000,
            "max_fba_sellers": 15, "max_total_sellers": 25,
            "minimum_margin": 10,
        }
        self.store.create_job(
            {"job_id": "job", "filters": filters, "phase": "suppliers_loaded"},
            [candidate(1), candidate(2)],
        )
        calls = []

        def catalog(identifiers, job_id, products):
            calls.extend(identifiers)
            return {value: {"status": "not_found"} for value in identifiers}

        result = run_incremental_discovery(
            "job", store=self.store, metadata_store=self.checkpoints,
            catalog_batch=catalog,
            pricing_batch=lambda *_: self.fail("Pricing must not be called"),
            fees_batch=lambda *_: self.fail("Fees must not be called"),
            token_provider=object(), sleep_func=lambda *_: None,
            resource_governor=DiscoveryResourceGovernor(
                policy=ResourcePolicy(
                    rss_soft_bytes=10**15, rss_hard_bytes=10**16,
                    available_soft_bytes=0, available_hard_bytes=0,
                    disk_free_hard_bytes=0, wal_soft_bytes=10**15,
                    wal_hard_bytes=10**16,
                    write_rate_soft_bytes_per_second=10**15,
                    write_rate_hard_bytes_per_second=10**16,
                ),
                database_path=self.store.path, sleep_func=lambda *_: None,
            ),
        )
        self.assertEqual(len(calls), 2)
        self.assertEqual(result["status"], "completed")
        self.assertEqual(self.store.summary("job")["catalog_pending_count"], 0)

    def test_http_200_element_failure_isolated_and_job_completes(self):
        filters = {
            "bsr_min": 1, "bsr_max": 30_000,
            "max_fba_sellers": 15, "max_total_sellers": 25,
            "minimum_margin": 10,
        }
        self.store.create_job(
            {"job_id": "job", "filters": filters, "phase": "suppliers_loaded"},
            [candidate(index) for index in range(1, 21)],
        )

        def fees(requests_, _token):
            return [
                fee_internal_error(row["asin"], row["identifier"])
                if row["asin"] == "B000000001"
                else fee_success(row["asin"], row["identifier"])
                for row in requests_
            ]

        class Token:
            def get(self):
                return "token"

        result = run_incremental_discovery(
            "job", store=self.store, metadata_store=self.checkpoints,
            catalog_batch=catalog_success, pricing_batch=pricing_success,
            fees_batch=fees, token_provider=Token(), sleep_func=lambda *_: None,
            catalog_batch_interval=0, pricing_batch_interval=0,
            fee_batch_interval=0,
        )
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["fee_target_count"], 20)
        self.assertEqual(result["fee_valid_count"], 19)
        self.assertEqual(result["fee_unavailable_count"], 1)
        self.assertEqual(
            result["fee_completion_status"],
            "fees_complete_with_partial_failures",
        )
        unavailable = [
            row for row in self.store.iter_candidates("job")
            for row in row.get("amazon_listings") or []
            if row.get("fee_status") == "unavailable"
        ]
        self.assertEqual(len(unavailable), 1)
        self.assertEqual(unavailable[0]["exclusion_reason"], "amazon_fee_unavailable")

    def test_request_level_fee_connection_outage_waits_for_retry(self):
        filters = {
            "bsr_min": 1, "bsr_max": 30_000,
            "max_fba_sellers": 15, "max_total_sellers": 25,
            "minimum_margin": 10,
        }
        self.store.create_job(
            {"job_id": "job", "filters": filters, "phase": "suppliers_loaded"},
            [candidate(1)],
        )

        class Token:
            def get(self):
                return "token"

        result = run_incremental_discovery(
            "job", store=self.store, metadata_store=self.checkpoints,
            catalog_batch=catalog_success, pricing_batch=pricing_success,
            fees_batch=lambda *_: (_ for _ in ()).throw(
                requests.ConnectionError("offline detail")
            ),
            token_provider=Token(), sleep_func=lambda *_: None,
            catalog_batch_interval=0, pricing_batch_interval=0,
            fee_batch_interval=0,
        )
        self.assertEqual(result["status"], "waiting_retry")
        self.assertEqual(result["progress_phase"], "fees_pending")
        self.assertTrue(result["resumable"])
        self.assertEqual(result["fee_outage_reason"], "ConnectionError")
        resumed = run_incremental_discovery(
            "job", store=self.store, metadata_store=self.checkpoints,
            catalog_batch=lambda *_: self.fail("Catalog must not repeat"),
            pricing_batch=lambda *_: self.fail("Pricing must not repeat"),
            fees_batch=lambda requests_, _token: [
                fee_success(row["asin"], row["identifier"]) for row in requests_
            ],
            token_provider=Token(), sleep_func=lambda *_: None,
            catalog_batch_interval=0, pricing_batch_interval=0,
            fee_batch_interval=0,
        )
        self.assertEqual(resumed["status"], "completed")
        self.assertEqual(resumed["fee_valid_count"], 1)

    def test_incremental_runner_commits_each_catalog_batch_to_rotation(self):
        filters = {
            "bsr_min": 1, "bsr_max": 30_000,
            "max_fba_sellers": 15, "max_total_sellers": 25,
            "minimum_margin": 10,
        }
        self.store.create_job(
            {
                "job_id": "job", "filters": filters, "phase": "suppliers_loaded",
                "rotation_scope": "supplier-scope-test",
            },
            [candidate(1), candidate(2)],
        )

        class Rotation:
            def __init__(self):
                self.commits = []

            def commit_catalog_results(self, job_id, statuses):
                self.commits.append((job_id, dict(statuses)))
                return {}

        rotation = Rotation()

        run_incremental_discovery(
            "job", store=self.store, metadata_store=self.checkpoints,
            catalog_batch=lambda identifiers, *_: {
                value: {"status": "not_found"} for value in identifiers
            },
            pricing_batch=lambda *_: self.fail("Pricing must not be called"),
            fees_batch=lambda *_: self.fail("Fees must not be called"),
            token_provider=object(), rotation_store=rotation,
            sleep_func=lambda *_: None,
            resource_governor=DiscoveryResourceGovernor(
                policy=ResourcePolicy(
                    rss_soft_bytes=10**15, rss_hard_bytes=10**16,
                    available_soft_bytes=0, available_hard_bytes=0,
                    disk_free_hard_bytes=0, wal_soft_bytes=10**15,
                    wal_hard_bytes=10**16,
                    write_rate_soft_bytes_per_second=10**15,
                    write_rate_hard_bytes_per_second=10**16,
                ),
                database_path=self.store.path, sleep_func=lambda *_: None,
            ),
        )

        self.assertEqual(len(rotation.commits), 1)
        self.assertEqual(rotation.commits[0][0], "job")
        self.assertEqual(set(rotation.commits[0][1].values()), {"not_found"})

    def test_incremental_resume_reconciles_rotation_before_new_catalog_work(self):
        filters = {
            "bsr_min": 1, "bsr_max": 30_000,
            "max_fba_sellers": 15, "max_total_sellers": 25,
            "minimum_margin": 10,
        }
        self.store.create_job(
            {
                "job_id": "job", "filters": filters, "phase": "suppliers_loaded",
                "rotation_scope": "supplier-scope-test",
            },
            [candidate(1), candidate(2)],
        )
        committed = self.store.pending_catalog_batch("job", 1)
        committed[0]["catalog_status"] = "not_found"
        self.store.commit_catalog_batch("job", committed, batch_number=1)

        class Rotation:
            def __init__(self):
                self.commits = []

            def commit_catalog_results(self, job_id, statuses):
                self.commits.append(dict(statuses))
                return {}

        rotation = Rotation()
        calls = []
        run_incremental_discovery(
            "job", store=self.store, metadata_store=self.checkpoints,
            catalog_batch=lambda identifiers, *_: (
                calls.extend(identifiers)
                or {value: {"status": "not_found"} for value in identifiers}
            ),
            pricing_batch=lambda *_: self.fail("Pricing must not be called"),
            fees_batch=lambda *_: self.fail("Fees must not be called"),
            token_provider=object(), rotation_store=rotation,
            sleep_func=lambda *_: None,
            resource_governor=DiscoveryResourceGovernor(
                policy=ResourcePolicy(
                    rss_soft_bytes=10**15, rss_hard_bytes=10**16,
                    available_soft_bytes=0, available_hard_bytes=0,
                    disk_free_hard_bytes=0, wal_soft_bytes=10**15,
                    wal_hard_bytes=10**16,
                    write_rate_soft_bytes_per_second=10**15,
                    write_rate_hard_bytes_per_second=10**16,
                ),
                database_path=self.store.path, sleep_func=lambda *_: None,
            ),
        )

        self.assertEqual(calls, [candidate(2)["gtin"]])
        self.assertEqual(rotation.commits[0], {candidate(1)["gtin"]: "not_found"})
        self.assertEqual(rotation.commits[1], {candidate(2)["gtin"]: "not_found"})

    def test_catalog_incomplete_stops_and_is_retried_once_on_resume(self):
        filters = {
            "bsr_min": 1, "bsr_max": 30_000,
            "max_fba_sellers": 15, "max_total_sellers": 25,
            "minimum_margin": 10,
        }
        self.store.create_job(
            {"job_id": "job", "filters": filters, "phase": "suppliers_loaded"},
            [candidate(1), candidate(2)],
        )
        calls = []

        def incomplete(identifiers, *_):
            calls.append(list(identifiers))
            return {value: {"status": "catalog_incomplete"} for value in identifiers}

        first = run_incremental_discovery(
            "job", store=self.store, metadata_store=self.checkpoints,
            catalog_batch=incomplete,
            pricing_batch=lambda *_: self.fail("Pricing must not be called"),
            fees_batch=lambda *_: self.fail("Fees must not be called"),
            token_provider=object(), sleep_func=lambda *_: None,
            resource_governor=DiscoveryResourceGovernor(
                policy=ResourcePolicy(
                    rss_soft_bytes=10**15, rss_hard_bytes=10**16,
                    available_soft_bytes=0, available_hard_bytes=0,
                    disk_free_hard_bytes=0, wal_soft_bytes=10**15,
                    wal_hard_bytes=10**16,
                    write_rate_soft_bytes_per_second=10**15,
                    write_rate_hard_bytes_per_second=10**16,
                ),
                database_path=self.store.path, sleep_func=lambda *_: None,
            ),
        )
        self.assertEqual(first["status"], "waiting_retry")
        self.assertEqual(len(calls), 1)

        second = run_incremental_discovery(
            "job", store=self.store, metadata_store=self.checkpoints,
            catalog_batch=lambda identifiers, *_: {
                value: {"status": "not_found"} for value in identifiers
            },
            pricing_batch=lambda *_: self.fail("Pricing must not be called"),
            fees_batch=lambda *_: self.fail("Fees must not be called"),
            token_provider=object(), sleep_func=lambda *_: None,
            resource_governor=DiscoveryResourceGovernor(
                policy=ResourcePolicy(
                    rss_soft_bytes=10**15, rss_hard_bytes=10**16,
                    available_soft_bytes=0, available_hard_bytes=0,
                    disk_free_hard_bytes=0, wal_soft_bytes=10**15,
                    wal_hard_bytes=10**16,
                    write_rate_soft_bytes_per_second=10**15,
                    write_rate_hard_bytes_per_second=10**16,
                ),
                database_path=self.store.path, sleep_func=lambda *_: None,
            ),
        )
        self.assertEqual(second["status"], "completed")
        self.assertEqual(self.store.summary("job")["catalog_pending_count"], 0)

    def test_supplier_first_preparation_freezes_only_selected_payloads(self):
        class SupplierStore:
            def serving_generation_metadata(self, supplier):
                return {
                    "run_id": f"run-{supplier}", "completed_at": "2026-08-27T00:00:00Z",
                    "product_count": 2, "scenario_count": 2,
                    "product_catalog_coverage_complete": True,
                    "product_catalog_coverage_type": "full_relevant_catalog",
                }

            def active_identifier_memberships(self, suppliers):
                return {"0000000000001": ("abw",), "0000000000002": ("abw",)}

            def active_candidates_for_identifier(self, supplier, identifier):
                return [{
                    "canonical_ean": identifier, "gtin": identifier,
                    "product_key": f"product-{identifier}",
                    "scenarios": [{
                        "scenario_id": f"scenario-{identifier}", "supplier": supplier,
                    }],
                }]

        class Rotation:
            def select(self, job_id, candidates, suppliers, budget, **kwargs):
                self.received = candidates
                return candidates[:budget], {
                    "rotation_scope": "scope", "rotation_cycle_id": 1,
                    "rotation_selected_identifiers": [candidates[0]["canonical_ean"]],
                }

        rotation = Rotation()
        prepared = prepare_incremental_job(
            {
                "job_id": "job", "selected_suppliers": ["abw"],
                "run_budget": 1, "filters": {},
            },
            supplier_store=SupplierStore(), rotation_store=rotation,
        )
        rows = list(prepared["candidates"])
        self.assertEqual(len(rotation.received), 2)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["scenarios"][0]["supplier"], "abw")


class ResourceGovernorTests(unittest.TestCase):
    def snapshot(self, **updates):
        values = dict(
            rss_bytes=100, available_memory_bytes=10_000, swap_used_bytes=0,
            disk_free_bytes=10_000, database_bytes=100, wal_bytes=100,
            write_bytes=100, write_rate_bytes_per_second=10,
            db_latency_ms=1,
            observed_monotonic=1.0,
        )
        values.update(updates)
        return ResourceSnapshot(**values)

    def test_soft_pressure_throttles_and_hard_pressure_pauses(self):
        policy = ResourcePolicy(
            rss_soft_bytes=500, rss_hard_bytes=1000,
            available_soft_bytes=500, available_hard_bytes=250,
            disk_free_hard_bytes=100, wal_soft_bytes=500, wal_hard_bytes=1000,
            write_rate_soft_bytes_per_second=500,
            write_rate_hard_bytes_per_second=1000,
        )
        governor = DiscoveryResourceGovernor(policy=policy)
        self.assertEqual(governor.evaluate(self.snapshot(rss_bytes=600))[0], "throttle")
        self.assertEqual(governor.evaluate(self.snapshot(rss_bytes=1200))[0], "pause")
        self.assertEqual(
            governor.evaluate(self.snapshot(available_memory_bytes=100))[0], "pause"
        )


if __name__ == "__main__":
    unittest.main()
