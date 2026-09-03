import unittest
from datetime import datetime, timezone

from direct_lookup import DirectAmazonLookup
from discovery_amazon import parse_item_offers_batch
from manager_canonical import ManagerCanonicalClient


EAN = "3359997562006"
ASIN = "B09Y5977HD"
NOW = datetime(2026, 9, 3, 12, tzinfo=timezone.utc)


def manager_product(*, title="ORLANE B21 EXTRAORDINAIRE CREMA DE CUELLO 50ML"):
    return {
        "status": "known",
        "identity": {
            "amazon_product_id": 232743, "marketplace_id": "APJ6JRA9NG5V4",
            "asin": ASIN, "ean": EAN, "title": title, "brand": "Orlane",
            "image_url": "https://m.media-amazon.com/images/I/test.jpg",
        },
        "bsr": {
            "status": "available", "rank": 43679,
            "category_name": "Cancelleria e prodotti per ufficio",
            "category_id": "office_product_display_on_website",
            "business_date": "2026-09-03", "observed_at": "2026-09-03T08:00:00Z",
            "source": "keepa_backfill", "observed_days": 25,
        },
        "lowest_new": {
            "status": "available", "current_price": 75.85,
            "business_date": "2026-09-01", "observed_at": "2026-09-01T10:00:00Z",
            "source": "keepa_backfill", "price_basis": "lowest_new_landed_total",
            "observed_days": 1060,
        },
    }


def live_pricing(asins, _context):
    return {asin: {"status": "success"} for asin in asins}


class CanonicalFirstTests(unittest.TestCase):
    def lookup(self, manager_lookup, *, catalog=None, pricing=live_pricing):
        calls = {"catalog": 0, "pricing": 0}
        def catalog_lookup(*args):
            calls["catalog"] += 1
            return catalog(*args) if catalog else {EAN: {"status": "not_found", "listings": []}}
        def pricing_lookup(*args):
            calls["pricing"] += 1
            return pricing(*args)
        resolver = DirectAmazonLookup(
            cache=None, catalog_lookup=catalog_lookup, pricing_lookup=pricing_lookup,
            manager_lookup=manager_lookup, now=lambda: NOW,
        )
        return resolver.lookup(EAN), calls

    def test_manager_known_is_canonical_first_and_uses_live_only_for_missing_pricing(self):
        result, calls = self.lookup(lambda _: manager_product())
        self.assertEqual(calls, {"catalog": 0, "pricing": 1})
        self.assertEqual(result["asin"], ASIN)
        self.assertEqual(result["bsr_rank"], 43679)
        self.assertEqual(result["bsr_category_label"], "Cancelleria e prodotti per ufficio")
        self.assertEqual(result["lowest_new_price"], 75.85)
        self.assertEqual(result["total_sellers"], None)
        self.assertEqual(result["cache_status"], "manager_canonical")

    def test_manager_unknown_falls_back_to_existing_live_pipeline(self):
        def catalog(_identifiers, _context):
            return {EAN: {"status": "resolved", "listings": [{
                "asin": ASIN, "title": "Live", "brand": "Brand",
                "main_image": "image", "compatibility_status": "compatible",
            }]}}
        result, calls = self.lookup(lambda _: None, catalog=catalog)
        self.assertEqual(calls, {"catalog": 1, "pricing": 1})
        self.assertEqual(result["asin"], ASIN)
        self.assertNotEqual(result["cache_status"], "manager_canonical")

    def test_manager_unavailable_falls_back_to_existing_live_pipeline(self):
        def unavailable(_):
            raise RuntimeError("offline")
        def catalog(_identifiers, _context):
            return {EAN: {"status": "resolved", "listings": [{
                "asin": ASIN, "title": "Live", "brand": "Brand",
                "main_image": "image", "compatibility_status": "compatible",
            }]}}
        result, calls = self.lookup(unavailable, catalog=catalog)
        self.assertEqual(calls, {"catalog": 1, "pricing": 1})
        self.assertEqual(result["catalog_status"], "resolved")

    def test_missing_manager_metadata_calls_catalog_but_never_changes_canonical_asin(self):
        def catalog(_identifiers, _context):
            return {EAN: {"status": "resolved", "listings": [
                {"asin": "B000OTHER1", "title": "Wrong", "compatibility_status": "compatible"},
                {"asin": ASIN, "title": "Recovered", "main_image": "image", "compatibility_status": "compatible"},
            ]}}
        result, calls = self.lookup(lambda _: manager_product(title=None), catalog=catalog)
        self.assertEqual(calls, {"catalog": 1, "pricing": 1})
        self.assertEqual(result["asin"], ASIN)
        self.assertEqual(result["title"], "Recovered")

    def test_pilot_contract_is_office_not_beauty(self):
        result, _ = self.lookup(lambda _: manager_product())
        self.assertEqual(result["canonical_ean"], EAN)
        self.assertEqual(result["bsr_rank"], 43679)
        self.assertIsNone(result["bsr_beauty"])
        self.assertEqual(result["lowest_new_observed_date"], "2026-09-01")


class PricingSemanticsTests(unittest.TestCase):
    def parse(self, payload):
        return parse_item_offers_batch([{
            "status": {"statusCode": 200},
            "request": {"uri": f"/products/pricing/v0/items/{ASIN}/offers"},
            "body": {"payload": payload},
        }])[ASIN]

    def test_missing_seller_evidence_is_unavailable_not_zero(self):
        result = self.parse({})
        self.assertIsNone(result["Venditori totali"])
        self.assertIsNone(result["Venditori FBA"])
        self.assertEqual(result["Seller count source"], "unavailable")

    def test_explicit_summary_zero_is_real_zero(self):
        result = self.parse({"Summary": {"NumberOfOffers": [
            {"condition": "new", "fulfillmentChannel": "Amazon", "OfferCount": 0},
            {"condition": "new", "fulfillmentChannel": "Merchant", "OfferCount": 0},
        ]}})
        self.assertEqual(result["Venditori totali"], 0)
        self.assertEqual(result["Venditori FBA"], 0)

    def test_shipping_missing_is_discarded_and_explicit_zero_is_valid(self):
        result = self.parse({"Offers": [
            {"IsFulfilledByAmazon": True, "ListingPrice": {"Amount": 10, "CurrencyCode": "EUR"}},
            {"IsFulfilledByAmazon": False, "ListingPrice": {"Amount": 12, "CurrencyCode": "EUR"}, "Shipping": {"Amount": 0, "CurrencyCode": "EUR"}},
        ]})
        self.assertIsNone(result["Prezzo minimo FBA Amount"])
        self.assertEqual(result["Prezzo minimo FBM Amount"], 12)

    def test_landed_price_wins_and_currency_mismatch_is_discarded(self):
        result = self.parse({"Offers": [
            {"IsFulfilledByAmazon": True, "BuyingPrice": {"LandedPrice": {"Amount": 9, "CurrencyCode": "EUR"}}, "ListingPrice": {"Amount": 4, "CurrencyCode": "EUR"}},
            {"IsFulfilledByAmazon": False, "ListingPrice": {"Amount": 8, "CurrencyCode": "EUR"}, "Shipping": {"Amount": 1, "CurrencyCode": "USD"}},
        ]})
        self.assertEqual(result["Prezzo minimo FBA Amount"], 9)
        self.assertIsNone(result["Prezzo minimo FBM Amount"])


class ManagerCanonicalClientTests(unittest.TestCase):
    def test_authenticated_lookup_and_not_found(self):
        calls = []
        class Response:
            def __init__(self, status): self.status_code = status
            def raise_for_status(self):
                if self.status_code >= 400: raise RuntimeError(self.status_code)
            def json(self): return manager_product()
        def request(url, **kwargs):
            calls.append((url, kwargs)); return Response(200)
        client = ManagerCanonicalClient(
            base_url="http://manager", token_loader=lambda: "secret", request_get=request,
        )
        self.assertEqual(client.lookup(EAN)["identity"]["asin"], ASIN)
        self.assertEqual(calls[0][1]["headers"]["Authorization"], "Bearer secret")
        missing = ManagerCanonicalClient(
            base_url="http://manager", token_loader=lambda: "secret",
            request_get=lambda *_args, **_kwargs: Response(404),
        )
        self.assertIsNone(missing.lookup(EAN))


if __name__ == "__main__":
    unittest.main()
