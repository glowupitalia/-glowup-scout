import time
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from streamlit.testing.v1 import AppTest

from direct_lookup import DirectAmazonLookup, format_eur, run_direct_lookup
from discovery_freshness import AmazonFreshnessPolicy


EAN = "8809562191179"
NOW = datetime(2026, 8, 31, 12, 0, tzinfo=timezone.utc)


def stamp(delta=timedelta()):
    return (NOW - delta).isoformat().replace("+00:00", "Z")


def listing(asin="B0DIRECT01", *, pricing_at=None):
    value = {
        "asin": asin,
        "title": "Amazon Product",
        "brand": "Amazon Brand",
        "main_image": "https://example.invalid/image.jpg",
        "product_type": "BEAUTY",
        "browse_classification": {
            "classificationId": "6306900031", "displayName": "Trucco",
        },
        "bsr_beauty": 1234,
        "compatibility_status": "compatible",
        "reference_price": 30.98,
        "price_source": "buy_box",
        "min_fba_price": 31.20,
        "min_fbm_price": 30.98,
        "fba_sellers": 2,
        "total_sellers": 5,
    }
    if pricing_at:
        value["pricing_observed_at"] = pricing_at
    return value


def cached(*, status="resolved", catalog_at=None, pricing_at=None, listings=None):
    return {
        "catalog_status": status,
        "catalog_observed_at": catalog_at or stamp(),
        "pricing_observed_at": pricing_at,
        "amazon_listings": (
            [listing(pricing_at=pricing_at)] if listings is None else listings
        ),
    }


def catalog_found(identifiers, _context):
    return {
        identifier: {
            "status": "resolved", "asin": "B0DIRECT01",
            "listings": [listing()],
        }
        for identifier in identifiers
    }


def pricing_found(asins, _context):
    return {
        asin: {
            "status": "success",
            "Buy Box Amount": 30.98,
            "Prezzo minimo FBA Amount": 31.20,
            "Prezzo minimo FBM Amount": 30.98,
            "Venditori FBA": 2,
            "Venditori totali": 5,
            "Seller count source": "summary_number_of_offers",
            "reference_price": 30.98,
            "price_source": "buy_box",
        }
        for asin in asins
    }


class FakeCache:
    def __init__(self, value):
        self.value = value
        self.calls = []

    def get(self, identifier):
        self.calls.append(identifier)
        return self.value


class DirectAmazonLookupTests(unittest.TestCase):
    def resolver(self, cache_value, calls=None):
        calls = calls if calls is not None else {"catalog": 0, "pricing": 0}

        def catalog(*args):
            calls["catalog"] += 1
            return catalog_found(*args)

        def pricing(*args):
            calls["pricing"] += 1
            return pricing_found(*args)

        return DirectAmazonLookup(
            cache=FakeCache(cache_value),
            catalog_lookup=catalog,
            pricing_lookup=pricing,
            freshness_policy=AmazonFreshnessPolicy(),
            now=lambda: NOW,
        ), calls

    def test_full_cache_hit_uses_zero_amazon_calls(self):
        resolver, calls = self.resolver(cached(pricing_at=stamp()))
        started = time.perf_counter()
        result = resolver.lookup(EAN)
        elapsed = time.perf_counter() - started
        self.assertEqual(calls, {"catalog": 0, "pricing": 0})
        self.assertEqual(result["cache_status"], "full_cache_hit")
        self.assertLess(elapsed, 0.05)

    def test_stale_pricing_calls_pricing_only(self):
        resolver, calls = self.resolver(
            cached(pricing_at=stamp(timedelta(days=1)))
        )
        result = resolver.lookup(EAN)
        self.assertEqual(calls, {"catalog": 0, "pricing": 1})
        self.assertEqual(
            result["cache_status"], "catalog_cache_hit_pricing_refreshed"
        )
        self.assertEqual(result["fbm_sellers"], 3)

    def test_missing_or_stale_catalog_calls_catalog_then_pricing(self):
        for cache_value in (
            None,
            cached(
                catalog_at=stamp(timedelta(days=31)),
                pricing_at=stamp(timedelta(days=31)),
            ),
        ):
            with self.subTest(cache=cache_value is not None):
                resolver, calls = self.resolver(cache_value)
                result = resolver.lookup(EAN)
                self.assertEqual(calls, {"catalog": 1, "pricing": 1})
                self.assertEqual(result["catalog_status"], "resolved")

    def test_fresh_negative_catalog_returns_not_found_without_live_calls(self):
        resolver, calls = self.resolver(
            cached(status="not_found", listings=[], pricing_at=None)
        )
        result = resolver.lookup(EAN)
        self.assertEqual(calls, {"catalog": 0, "pricing": 0})
        self.assertEqual(result["catalog_status"], "not_found")
        self.assertEqual(result["cache_status"], "negative_cache_hit")

    def test_ambiguous_never_selects_first_listing_or_calls_pricing(self):
        rows = [listing("B0FIRST"), listing("B0SECOND")]
        resolver, calls = self.resolver(
            cached(status="ambiguous", listings=rows)
        )
        result = resolver.lookup(EAN)
        self.assertEqual(result["catalog_status"], "ambiguous")
        self.assertIsNone(result["asin"])
        self.assertEqual([row["asin"] for row in result["listings"]], [
            "B0FIRST", "B0SECOND",
        ])
        self.assertEqual(calls, {"catalog": 0, "pricing": 0})

    def test_resolved_contract_contains_only_amazon_fields(self):
        resolver, _ = self.resolver(cached(pricing_at=stamp()))
        result = resolver.lookup(EAN)
        expected = {
            "requested_ean", "canonical_ean", "catalog_status", "asin", "title",
            "brand", "image_url", "category", "bsr_beauty", "buy_box_price",
            "reference_price", "min_fba_price", "min_fbm_price", "total_sellers",
            "fba_sellers", "fbm_sellers", "amazon_product_url",
            "amazon_offers_url", "observed_at", "cache_status",
        }
        self.assertTrue(expected.issubset(result))
        self.assertFalse({
            "scenarios", "fees", "economics", "score", "recommended_supplier",
            "opportunity_combinations",
        } & set(result))
        self.assertEqual(result["fbm_sellers"], 3)

    def test_invalid_identifier_stops_before_cache(self):
        cache = FakeCache(cached(pricing_at=stamp()))
        resolver = DirectAmazonLookup(
            cache=cache, catalog_lookup=catalog_found,
            pricing_lookup=pricing_found, now=lambda: NOW,
        )
        with self.assertRaisesRegex(ValueError, "non valido"):
            resolver.lookup("123")
        self.assertEqual(cache.calls, [])

    def test_no_supplier_qogita_fees_jobs_rotation_or_planner_access(self):
        probes = {
            "supplier": "supplier_catalog.SupplierCatalogStore.__init__",
            "qogita": "qogita_serving.QogitaServingStore.__init__",
            "fees": "product_fees.search_product_fees_batch",
            "job": "discovery.DiscoveryCheckpointStore.save",
            "incremental_job": "discovery_incremental.DiscoveryIncrementalStore.create_job",
            "registry": "discovery_jobs.DiscoveryJobRegistry.register_checkpoint",
            "rotation": "discovery_rotation.DiscoveryRotationStore.commit_catalog_results",
            "planner": "discovery_freshness.plan_cached_product",
        }
        patches = {
            key: patch(target, side_effect=AssertionError(key))
            for key, target in probes.items()
        }
        started = []
        try:
            for key, value in patches.items():
                value.start()
                started.append(value)
            result = run_direct_lookup(
                EAN,
                cache=FakeCache(cached(pricing_at=stamp())),
                catalog_batch=lambda *_: self.fail("Catalog must be cached"),
                pricing_batch=lambda *_: self.fail("Pricing must be cached"),
                now=lambda: NOW,
            )
        finally:
            for value in reversed(started):
                value.stop()
        self.assertEqual(result["catalog_status"], "resolved")

    def test_money_formatting_is_decimal_and_italian(self):
        self.assertEqual(format_eur(30.979999999999997), "€30,98")
        self.assertEqual(format_eur(1234.5), "€1.234,50")


class DirectLookupUiTests(unittest.TestCase):
    @staticmethod
    def resolved_result():
        resolver = DirectAmazonLookup(
            cache=FakeCache(cached(pricing_at=stamp())),
            catalog_lookup=catalog_found, pricing_lookup=pricing_found,
            now=lambda: NOW,
        )
        return resolver.lookup(EAN)

    @staticmethod
    def app_with_result(result, status="found"):
        app = AppTest.from_file("app_glowup.py", default_timeout=20).run()
        app.session_state["ui_state"] = "single_result"
        app.session_state["single_status"] = status
        app.session_state["single_product_result"] = result
        return app.run()

    def test_home_has_simple_input_and_analyze_button(self):
        app = AppTest.from_file("app_glowup.py", default_timeout=20).run()
        self.assertFalse(app.exception)
        self.assertTrue(any(row.label == "EAN prodotto" for row in app.text_input))
        self.assertTrue(any(row.label == "Analizza" for row in app.button))

    def test_resolved_ui_is_amazon_only(self):
        app = self.app_with_result(self.resolved_result())
        self.assertFalse(app.exception)
        metrics = {row.label for row in app.metric}
        self.assertTrue({
            "BSR Beauty", "Prezzo riferimento", "Buy Box", "Minimo FBA",
            "Minimo FBM", "Venditori FBA", "Venditori FBM", "Venditori totali",
        }.issubset(metrics))
        links = {row.proto.label for row in app.get("link_button")}
        self.assertIn("Apri scheda Amazon", links)
        self.assertIn("Vedi offerte Amazon", links)
        visible = " ".join(
            str(row.value) for collection in (
                app.markdown, app.subheader, app.caption, app.info, app.warning,
            ) for row in collection
        ).casefold()
        for forbidden in (
            "migliore opzione", "confronto fornitori", "qogita", "umma", "abw",
            "qudo", "score", "profitto", "margine", "mov", "fornitore",
        ):
            self.assertNotIn(forbidden, visible)
        self.assertEqual(len(app.dataframe), 0)

    def test_not_found_ui_is_simple(self):
        result = {
            "catalog_status": "not_found", "listings": [],
            "requested_ean": EAN, "canonical_ean": EAN,
        }
        app = self.app_with_result(result, status="not_found")
        self.assertFalse(app.exception)
        self.assertTrue(any(
            "Nessun prodotto Amazon trovato" in str(row.value)
            for row in app.info
        ))

    def test_ambiguous_ui_lists_compatible_amazon_results(self):
        result = {
            "catalog_status": "ambiguous", "requested_ean": EAN,
            "canonical_ean": EAN,
            "listings": [listing("B0FIRST"), listing("B0SECOND")],
        }
        app = self.app_with_result(result)
        self.assertFalse(app.exception)
        self.assertTrue(any(
            row.value == "Più risultati Amazon trovati" for row in app.warning
        ))
        self.assertEqual(
            sum(
                row.proto.label == "Apri scheda Amazon"
                for row in app.get("link_button")
            ), 2
        )


if __name__ == "__main__":
    unittest.main()
