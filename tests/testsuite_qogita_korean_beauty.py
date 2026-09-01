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
    normalize_membership,
    parse_curated_page,
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


if __name__ == "__main__":
    unittest.main()
