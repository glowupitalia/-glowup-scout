import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from qogita_korean_beauty import (
    CuratedProduct,
    QogitaKoreanBeautyCollector,
    QogitaMembershipReconciler,
    QogitaMembershipStore,
    compare_memberships,
    normalize_membership,
    parse_curated_page,
    refresh_korean_beauty_membership,
)


GTIN_A = "8809447256221"
GTIN_B = "8809525249565"
CANONICAL_A = "08809447256221"
CANONICAL_B = "08809525249565"


def rsc_page(page, total, products, *, page_size=2):
    rows = []
    for position, product in enumerate(products, start=1):
        value = {
            "name": product.get("name", "Product"),
            "gtin": product.get("gtin"),
            "categoryName": "Face Cream",
            "brandName": "Brand",
            "slug": "product",
            "fid": product.get("fid"),
            "imageUrl": "https://static.example/image.jpg",
            "position": position,
            "offerCount": 4,
        }
        rows.append('"product":' + json.dumps(value, separators=(",", ":")))
    payload = (
        f'"currentPage":{page},"pageSize":{page_size},'
        f'"totalResults":{total},' + ",".join(rows)
    )
    return payload.replace('"', '\\"')


class FakeResponse:
    def __init__(self, text, status_code=200):
        self.text = text
        self.status_code = status_code
        self.headers = {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeSession:
    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        page = int(parse_qs(urlsplit(url).query)["page"][0])
        return FakeResponse(self.pages[page])

    def close(self):
        pass


class RetrySession:
    def __init__(self, responses):
        self.responses = iter(responses)

    def get(self, *args, **kwargs):
        return next(self.responses)

    def close(self):
        pass


class KoreanBeautyParsingTests(unittest.TestCase):
    def test_parses_rsc_identity_and_pagination(self):
        parsed = parse_curated_page(rsc_page(
            1, 23, [{"gtin": GTIN_A, "fid": "FID-A"}], page_size=23,
        ))
        self.assertEqual(parsed["current_page"], 1)
        self.assertEqual(parsed["page_size"], 23)
        self.assertEqual(parsed["total_results"], 23)
        self.assertEqual(parsed["records"][0].canonical_gtin, CANONICAL_A)
        self.assertEqual(parsed["records"][0].variant_fid, "FID-A")

    def test_recovers_identity_from_malformed_surrounding_product_object(self):
        payload = (
            '"currentPage":1,"pageSize":1,"totalResults":1,'
            '"product":{"name":"Bad "quoted" name",'
            f'"gtin":"{GTIN_A}","fid":"FID-A"}}'
        )
        parsed = parse_curated_page(payload)
        self.assertEqual(parsed["parse_errors"], 0)
        self.assertEqual(parsed["fallback_parses"], 1)
        self.assertEqual(parsed["records"][0].canonical_gtin, CANONICAL_A)
        self.assertEqual(parsed["records"][0].variant_fid, "FID-A")

    def test_collector_paginates_and_tracks_totals(self):
        session = FakeSession({
            1: rsc_page(1, 3, [
                {"gtin": GTIN_A, "fid": "FID-A"},
                {"gtin": GTIN_B, "fid": "FID-B"},
            ]),
            2: rsc_page(2, 3, [{"gtin": GTIN_A, "fid": "FID-A"}]),
        })
        result = QogitaKoreanBeautyCollector(
            session=session, pacing_seconds=0, sleep_func=lambda _: None,
        ).collect()
        self.assertEqual(result["metrics"]["pages_requested"], 2)
        self.assertEqual(result["metrics"]["total_results_initial"], 3)
        self.assertEqual(result["metrics"]["total_results_final"], 3)
        self.assertEqual(result["metrics"]["records_raw"], 3)
        self.assertEqual(result["metrics"]["gtin_unique"], 2)
        self.assertEqual(result["metrics"]["duplicate_count"], 1)
        self.assertEqual(result["acquisition_status"], "complete")
        self.assertTrue(all(call[1]["headers"]["RSC"] == "1" for call in session.calls))

    def test_live_total_drift_is_complete_with_visible_anomaly(self):
        session = FakeSession({
            1: rsc_page(
                1, 2, [{"gtin": GTIN_A, "fid": "FID-A"}], page_size=2,
            ),
        })
        result = QogitaKoreanBeautyCollector(
            session=session, pacing_seconds=0, sleep_func=lambda _: None,
        ).collect()
        self.assertEqual(result["acquisition_status"], "complete_with_anomalies")
        self.assertEqual(result["metrics"]["reported_total_record_delta"], -1)

    def test_invalid_and_missing_fid_are_visible(self):
        normalized = normalize_membership([
            CuratedProduct("invalid", None, "FID-X"),
            CuratedProduct(GTIN_A, CANONICAL_A, None),
        ])
        self.assertEqual(normalized["metrics"]["invalid_gtin_count"], 1)
        self.assertEqual(normalized["metrics"]["fid_missing_count"], 1)
        self.assertEqual(normalized["entries"], [{
            "canonical_gtin": CANONICAL_A, "variant_fid": None,
        }])

    def test_missing_fid_marks_complete_acquisition_as_anomalous(self):
        session = FakeSession({
            1: rsc_page(1, 1, [{"gtin": GTIN_A, "fid": None}], page_size=1),
        })
        result = QogitaKoreanBeautyCollector(
            session=session, pacing_seconds=0, sleep_func=lambda _: None,
        ).collect()
        self.assertEqual(result["acquisition_status"], "complete_with_anomalies")
        self.assertEqual(result["metrics"]["fid_missing_count"], 1)

    def test_wrong_server_page_is_incomplete(self):
        session = FakeSession({
            1: rsc_page(2, 2, [{"gtin": GTIN_A, "fid": "FID-A"}], page_size=1),
        })
        result = QogitaKoreanBeautyCollector(
            session=session, pacing_seconds=0, sleep_func=lambda _: None,
        ).collect()
        self.assertEqual(result["acquisition_status"], "incomplete")
        self.assertEqual(result["metrics"]["parsing_error_count"], 1)

    def test_transient_http_error_retries_then_recovers(self):
        session = RetrySession([
            FakeResponse("temporary", status_code=500),
            FakeResponse(rsc_page(
                1, 1, [{"gtin": GTIN_A, "fid": "FID-A"}], page_size=1,
            )),
        ])
        result = QogitaKoreanBeautyCollector(
            session=session, pacing_seconds=0, max_attempts=2,
            sleep_func=lambda _: None,
        ).collect()
        self.assertEqual(result["acquisition_status"], "complete")
        self.assertEqual(result["metrics"]["http_retry_count"], 1)
        self.assertEqual(result["metrics"]["http_status_counts"], {"500": 1, "200": 1})

    def test_gtin_to_fid_conflict_is_not_selected(self):
        normalized = normalize_membership([
            CuratedProduct(GTIN_A, CANONICAL_A, "FID-A"),
            CuratedProduct(GTIN_A, CANONICAL_A, "FID-B"),
        ])
        self.assertEqual(normalized["entries"], [])
        self.assertEqual(normalized["metrics"]["gtin_fid_conflict_count"], 1)

    def test_fid_to_gtin_conflict_is_not_selected(self):
        normalized = normalize_membership([
            CuratedProduct(GTIN_A, CANONICAL_A, "FID-X"),
            CuratedProduct(GTIN_B, CANONICAL_B, "FID-X"),
        ])
        self.assertEqual(normalized["entries"], [])
        self.assertEqual(normalized["metrics"]["fid_gtin_conflict_count"], 1)


class KoreanBeautyPersistenceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.database = Path(self.temporary.name) / "catalog.sqlite3"
        self.store = QogitaMembershipStore(self.database)

    def tearDown(self):
        self.temporary.cleanup()

    def test_version_validation_and_atomic_active_pointer(self):
        first = self.store.create_version(
            source_generation_id="global-1", membership_version_id="membership-1",
        )
        self.assertEqual(first["status"], "collecting")
        finalized = self.store.finalize_version(
            "membership-1", entries=[{
                "canonical_gtin": CANONICAL_A,
                "canonical_product_key": "product-a",
                "variant_fid": "FID-A",
            }], acquisition_status="complete", metrics={},
        )
        self.assertEqual(finalized["status"], "valid")
        self.assertIsNone(self.store.active())
        active = self.store.activate("membership-1")
        self.assertEqual(active["membership_version_id"], "membership-1")

        self.store.create_version(
            source_generation_id="global-1", membership_version_id="membership-bad",
        )
        invalid = self.store.finalize_version(
            "membership-bad", entries=[], acquisition_status="incomplete",
            metrics={}, error_message="page failed",
        )
        self.assertEqual(invalid["status"], "invalid")
        with self.assertRaises(ValueError):
            self.store.activate("membership-bad")
        self.assertEqual(self.store.active()["membership_version_id"], "membership-1")

        self.store.create_version(
            source_generation_id="global-1", membership_version_id="membership-anomaly",
        )
        anomalous = self.store.finalize_version(
            "membership-anomaly", entries=[{
                "canonical_gtin": CANONICAL_A,
                "canonical_product_key": "product-a",
                "variant_fid": "FID-A",
            }], acquisition_status="complete_with_anomalies",
            metrics={"fid_missing_count": 1},
        )
        self.assertEqual(anomalous["status"], "invalid")
        with self.assertRaises(ValueError):
            self.store.activate("membership-anomaly")

    def test_catalog_bootstrap_and_serving_reconciliation(self):
        connection = sqlite3.connect(self.database)
        connection.executescript("""
            CREATE TABLE supplier_catalog_products (
                run_id TEXT, canonical_gtin TEXT, canonical_product_key TEXT,
                variant_fid TEXT
            );
            CREATE TABLE qogita_bootstrap_products (
                bootstrap_run_id TEXT, canonical_product_key TEXT, status TEXT
            );
            CREATE TABLE qogita_serving_memberships (
                serving_generation_id TEXT, canonical_product_key TEXT,
                scenario_count INTEGER
            );
        """)
        connection.execute(
            "INSERT INTO supplier_catalog_products VALUES (?,?,?,?)",
            ("global-1", CANONICAL_A, "product-a", "FID-A"),
        )
        connection.execute(
            "INSERT INTO qogita_bootstrap_products VALUES (?,?,?)",
            ("bootstrap-1", "product-a", "enriched"),
        )
        connection.execute(
            "INSERT INTO qogita_serving_memberships VALUES (?,?,?)",
            ("serving-1", "product-a", 7),
        )
        connection.commit()
        connection.close()
        result = QogitaMembershipReconciler(self.database).reconcile([
            {"canonical_gtin": CANONICAL_A, "variant_fid": "FID-A"},
            {"canonical_gtin": CANONICAL_B, "variant_fid": "FID-B"},
        ], source_generation_id="global-1", bootstrap_run_id="bootstrap-1",
           serving_generation_id="serving-1")
        metrics = result["metrics"]
        self.assertEqual(metrics["catalog_present_count"], 1)
        self.assertEqual(metrics["catalog_absent_count"], 1)
        self.assertEqual(len(result["entries"]), 2)
        self.assertIsNone(result["entries"][1]["canonical_product_key"])
        self.assertEqual(metrics["catalog_fid_equal_count"], 1)
        self.assertEqual(metrics["bootstrap_status_counts"], {"enriched": 1})
        self.assertEqual(metrics["serving_present_count"], 1)
        self.assertEqual(metrics["serving_scenario_count"], 7)

    def test_catalog_fid_mismatch_is_reported(self):
        connection = sqlite3.connect(self.database)
        connection.executescript("""
            CREATE TABLE supplier_catalog_products (
                run_id TEXT, canonical_gtin TEXT, canonical_product_key TEXT,
                variant_fid TEXT
            );
            CREATE TABLE qogita_bootstrap_products (
                bootstrap_run_id TEXT, canonical_product_key TEXT, status TEXT
            );
            CREATE TABLE qogita_serving_memberships (
                serving_generation_id TEXT, canonical_product_key TEXT,
                scenario_count INTEGER
            );
        """)
        connection.execute(
            "INSERT INTO supplier_catalog_products VALUES (?,?,?,?)",
            ("global-1", CANONICAL_A, "product-a", "CATALOG-FID"),
        )
        connection.commit()
        connection.close()
        result = QogitaMembershipReconciler(self.database).reconcile([
            {"canonical_gtin": CANONICAL_A, "variant_fid": "CURATED-FID"},
        ], source_generation_id="global-1", bootstrap_run_id="bootstrap-1",
           serving_generation_id="serving-1")
        self.assertEqual(result["metrics"]["catalog_fid_different_count"], 1)


class KoreanBeautyWeeklyRefreshTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.database = Path(self.temporary.name) / "catalog.sqlite3"
        connection = sqlite3.connect(self.database)
        connection.executescript("""
            CREATE TABLE supplier_catalog_products (
                run_id TEXT, canonical_gtin TEXT, canonical_product_key TEXT,
                variant_fid TEXT
            );
            CREATE TABLE qogita_bootstrap_products (
                bootstrap_run_id TEXT, canonical_product_key TEXT, status TEXT
            );
            CREATE TABLE qogita_serving_memberships (
                serving_generation_id TEXT, canonical_product_key TEXT,
                scenario_count INTEGER
            );
            CREATE TABLE qogita_serving_snapshots (
                serving_generation_id TEXT PRIMARY KEY,
                source_generation_id TEXT, bootstrap_run_id TEXT, status TEXT
            );
            CREATE TABLE qogita_serving_active (
                supplier TEXT PRIMARY KEY, serving_generation_id TEXT, updated_at TEXT
            );
            INSERT INTO qogita_serving_snapshots VALUES(
                'serving-1','global-1','bootstrap-1','valid'
            );
            INSERT INTO qogita_serving_active VALUES('qogita','serving-1','now');
            INSERT INTO supplier_catalog_products VALUES(
                'global-1','08809447256221','product-a','FID-A'
            );
            INSERT INTO supplier_catalog_products VALUES(
                'global-1','08809525249565','product-b','FID-B'
            );
            INSERT INTO qogita_bootstrap_products VALUES(
                'bootstrap-1','product-a','enriched'
            );
            INSERT INTO qogita_bootstrap_products VALUES(
                'bootstrap-1','product-b','pending'
            );
            INSERT INTO qogita_serving_memberships VALUES(
                'serving-1','product-a',7
            );
        """)
        connection.commit()
        connection.close()
        self.store = QogitaMembershipStore(self.database)
        self.store.create_version(
            source_generation_id="global-1", membership_version_id="membership-1",
        )
        self.store.finalize_version(
            "membership-1", entries=[{
                "canonical_gtin": CANONICAL_A,
                "canonical_product_key": "product-a",
                "variant_fid": "FID-A",
            }], acquisition_status="complete", metrics={},
        )
        self.store.activate("membership-1")

    def tearDown(self):
        self.temporary.cleanup()

    @staticmethod
    def acquisition(entries, *, status="complete", metrics=None):
        return {
            "acquisition_status": status,
            "entries": entries,
            "metrics": {
                "pages_requested": 1, "http_retry_count": 0,
                "http_status_counts": {"200": 1},
                "gtin_fid_conflict_count": 0, "fid_gtin_conflict_count": 0,
                "invalid_gtin_count": 0, "fid_missing_count": 0,
                **(metrics or {}),
            },
            "gtin_fid_conflicts": {}, "fid_gtin_conflicts": {},
        }

    class Collector:
        def __init__(self, result):
            self.result = result
            self.calls = 0

        def collect(self, **_kwargs):
            self.calls += 1
            return self.result

    def test_membership_diff_reports_gtin_and_fid_changes(self):
        result = compare_memberships(
            [{"canonical_gtin": "1", "variant_fid": "A"},
             {"canonical_gtin": "2", "variant_fid": "B"}],
            [{"canonical_gtin": "1", "variant_fid": "C"},
             {"canonical_gtin": "3", "variant_fid": "D"}],
        )
        self.assertEqual(result["gtin_added_count"], 1)
        self.assertEqual(result["gtin_removed_count"], 1)
        self.assertEqual(result["gtin_unchanged_count"], 1)
        self.assertEqual(result["fid_changed_count"], 1)

    def test_dry_run_reads_active_and_never_persists_or_switches(self):
        collector = self.Collector(self.acquisition([
            {"canonical_gtin": CANONICAL_A, "variant_fid": "FID-A"},
            {"canonical_gtin": CANONICAL_B, "variant_fid": "FID-B"},
        ]))
        result = refresh_korean_beauty_membership(
            path=self.database, collector=collector,
        )
        self.assertEqual(collector.calls, 1)
        self.assertFalse(result["production_writes"])
        self.assertTrue(result["would_activate"])
        self.assertEqual(result["previous_membership_version_id"], "membership-1")
        self.assertEqual(result["membership_diff"]["gtin_added_count"], 1)
        self.assertEqual(self.store.active()["membership_version_id"], "membership-1")
        with sqlite3.connect(self.database) as connection:
            count = connection.execute(
                "SELECT COUNT(*) FROM qogita_membership_versions"
            ).fetchone()[0]
        self.assertEqual(count, 1)

    def test_valid_refresh_activates_new_version_and_preserves_old(self):
        result = refresh_korean_beauty_membership(
            path=self.database,
            collector=self.Collector(self.acquisition([
                {"canonical_gtin": CANONICAL_A, "variant_fid": "FID-A"},
                {"canonical_gtin": CANONICAL_B, "variant_fid": "FID-B"},
            ])),
            persist=True, activate=True, membership_version_id="membership-2",
        )
        self.assertTrue(result["membership_activation"])
        self.assertEqual(self.store.active()["membership_version_id"], "membership-2")
        self.assertEqual(self.store.version("membership-1")["status"], "valid")
        self.assertEqual(result["catalog_bootstrap_serving"]["serving_present_count"], 1)
        self.assertEqual(
            result["catalog_bootstrap_serving"]["bootstrap_status_counts"],
            {"enriched": 1, "pending": 1},
        )

    def test_catalog_absent_product_is_membership_only_and_can_activate(self):
        result = refresh_korean_beauty_membership(
            path=self.database,
            collector=self.Collector(self.acquisition([
                {"canonical_gtin": CANONICAL_A, "variant_fid": "FID-A"},
                {"canonical_gtin": "00000000000000", "variant_fid": "FID-X"},
            ])),
            persist=True, activate=True, membership_version_id="membership-absent",
        )
        self.assertTrue(result["membership_activation"])
        self.assertEqual(result["catalog_bootstrap_serving"]["catalog_absent_count"], 1)
        rows = self.store.entries("membership-absent")
        self.assertIsNone(next(row for row in rows if row["canonical_gtin"] == "00000000000000")["canonical_product_key"])

    def test_failed_acquisition_preserves_active_and_serving_pointer(self):
        result = refresh_korean_beauty_membership(
            path=self.database,
            collector=self.Collector(self.acquisition(
                [], status="incomplete", metrics={"http_error_count": 1},
            )),
            persist=True, activate=True, membership_version_id="membership-bad",
        )
        self.assertFalse(result["membership_activation"])
        self.assertEqual(self.store.active()["membership_version_id"], "membership-1")
        self.assertEqual(self.store.version("membership-bad")["status"], "invalid")
        with sqlite3.connect(self.database) as connection:
            serving = connection.execute(
                "SELECT serving_generation_id FROM qogita_serving_active"
            ).fetchone()[0]
        self.assertEqual(serving, "serving-1")

    def test_invalid_dry_run_is_explicit_and_remains_write_free(self):
        result = refresh_korean_beauty_membership(
            path=self.database,
            collector=self.Collector(self.acquisition([], status="incomplete")),
        )
        self.assertEqual(result["status"], "dry_run_invalid")
        self.assertFalse(result["production_writes"])
        self.assertFalse(result["would_activate"])

    def test_identity_conflict_prevents_activation(self):
        result = refresh_korean_beauty_membership(
            path=self.database,
            collector=self.Collector(self.acquisition(
                [{"canonical_gtin": CANONICAL_A, "variant_fid": "FID-A"}],
                status="complete_with_anomalies",
                metrics={"gtin_fid_conflict_count": 1},
            )),
            persist=True, activate=True, membership_version_id="membership-conflict",
        )
        self.assertFalse(result["membership_activation"])
        self.assertIn("gtin_fid_conflict_count=1", result["validation_errors"])
        self.assertEqual(self.store.active()["membership_version_id"], "membership-1")

    def test_known_live_total_drift_remains_acceptable(self):
        result = refresh_korean_beauty_membership(
            path=self.database,
            collector=self.Collector(self.acquisition(
                [{"canonical_gtin": CANONICAL_A, "variant_fid": "FID-A"}],
                status="complete_with_anomalies",
                metrics={"reported_total_record_delta": -1},
            )),
        )
        self.assertTrue(result["would_activate"])


if __name__ == "__main__":
    unittest.main()
