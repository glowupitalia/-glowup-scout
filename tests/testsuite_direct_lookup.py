import json
import sqlite3
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from streamlit.testing.v1 import AppTest

from discovery import DiscoveryCheckpointStore
from direct_lookup import (
    direct_scenario_rows,
    format_eur,
    load_direct_supplier_context,
    run_direct_lookup,
)
from purchase_scenarios import product_key
from product_fees import ProductFeeBatchResults
from qogita_bootstrap import QogitaBootstrapStore
from qogita_serving import QogitaServingStore
from supplier_catalog import (
    SupplierCatalogGeneration,
    SupplierCatalogStore,
    candidates_to_cache_records,
)


EAN = "8809562191179"


def scenario(supplier, *, suffix="standard", cost="10", snapshot="2026-08-20T10:00:00Z"):
    scenario_type = f"{supplier}_{suffix}"
    scenario_id = f"scenario-{supplier}-{suffix}"
    return {
        "scenario_id": scenario_id, "product_key": product_key(EAN),
        "canonical_ean": EAN, "identifier_type": "EAN", "supplier": supplier,
        "supplier_product_id": f"{supplier}-product",
        "supplier_offer_id": f"{supplier}-offer", "variant_id": suffix,
        "brand": "Arencia", "title": "Black Tea & Yuzu Rice Mochi Cleanser 120 g",
        "scenario_type": scenario_type,
        "scenario_label": suffix.replace("_", " ").title(), "scenario_order": 1,
        "account_mov": Decimal("300"), "account_mov_currency": "EUR",
        "minimum_product_quantity": 1, "selling_unit": 1,
        "cost_net_unit_eur": Decimal(cost), "vat_rate": Decimal("0.22"),
        "vat_amount_unit": Decimal(cost) * Decimal("0.22"),
        "cost_gross_unit_eur": Decimal(cost) * Decimal("1.22"),
        "stock_quantity": 12, "availability_status": "in_stock",
        "snapshot_id": f"{supplier}-generation", "snapshot_at": snapshot,
        "freshness_status": "fresh", "tier_is_active": True,
    }


def candidate(supplier, scenarios=None):
    return {
        "product_key": product_key(EAN), "canonical_ean": EAN,
        "identifier_type": "EAN", "brand": "Arencia",
        "title": "Black Tea & Yuzu Rice Mochi Cleanser 120 g",
        "scenarios": scenarios or [scenario(supplier)],
    }


def publish(store, supplier, scenarios=None):
    products, scenario_rows = candidates_to_cache_records([
        candidate(supplier, scenarios=scenarios)
    ])
    generation = SupplierCatalogGeneration(
        supplier=supplier, coverage_type="full_relevant_catalog",
        coverage_description="direct lookup fixture", coverage_complete=True,
        products=products, scenarios=scenario_rows,
        completeness_status="full_relevant_catalog",
        product_catalog_coverage_type="full_relevant_catalog",
        product_catalog_coverage_complete=True,
        scenario_enrichment_status="full", scenario_enrichment_count=len(scenario_rows),
        export_generated_at="2026-08-20T10:00:00Z",
    )
    run_id = store.start_run(
        supplier, coverage_type="full_relevant_catalog",
        coverage_description="direct lookup fixture", coverage_complete=True,
        sampled=False,
    )
    store.publish(run_id, generation, elapsed_seconds=1)
    return run_id


def publish_qogita_serving(store):
    scenarios = [scenario("qogita", suffix="mov_500", cost="7")]
    products, scenario_rows = candidates_to_cache_records([
        candidate("qogita", scenarios=scenarios)
    ])
    generation = SupplierCatalogGeneration(
        supplier="qogita", coverage_type="full_account_catalog",
        coverage_description="full catalog fixture", coverage_complete=True,
        products=products, scenarios=scenario_rows,
        completeness_status="full_account_catalog",
        product_catalog_coverage_type="full_account_catalog",
        product_catalog_coverage_complete=True,
        scenario_enrichment_status="partial", scenario_enrichment_count=1,
    )
    run_id = store.start_run(
        "qogita", coverage_type="full_account_catalog",
        coverage_description="fixture", coverage_complete=True, sampled=False,
    )
    store.publish(run_id, generation, elapsed_seconds=1, promote=False)
    bootstrap_store = QogitaBootstrapStore(store.path)
    bootstrap = bootstrap_store.create_production_bootstrap(run_id)
    with sqlite3.connect(store.path) as connection:
        connection.execute(
            """UPDATE supplier_catalog_products SET variant_fid='FID-QOGITA',
                      variant_fid_source='fixture',enrichment_status='enriched',
                      offer_tier_observed_at='2026-08-27T10:00:00Z'
                WHERE run_id=?""", (run_id,),
        )
        connection.execute(
            """UPDATE qogita_bootstrap_products SET status='enriched',
                      variant_fid='FID-QOGITA',scenario_count=1,
                      completed_at='2026-08-27T10:00:00Z'
                WHERE bootstrap_run_id=?""", (bootstrap["bootstrap_run_id"],),
        )
    return QogitaServingStore(store.path).build_snapshot(
        bootstrap["bootstrap_run_id"], window_number=1, bootstrap_state="running",
    )


def catalog_found(gtins, _job_id, _products=None):
    return {gtin: {
        "status": "resolved", "asin": "B0DVBR1VT9",
        "amazon_title": "Arencia Black Tea & Yuzu Rice Mochi Cleanser 120 g",
        "amazon_brand": "Arencia", "bsr_beauty": 9000,
        "beauty_status": "display_group_beauty", "product_type": "BEAUTY",
    } for gtin in gtins}


def catalog_missing(gtins, _job_id, _products=None):
    return {gtin: {"status": "not_found", "listings": []} for gtin in gtins}


def pricing_found(asins, _job_id):
    return {asin: {
        "status": "success", "Venditori FBA": 2, "Venditori totali": 4,
        "Seller count source": "summary_number_of_offers",
        "Buy Box Amount": 30.98, "Prezzo minimo FBA Amount": 31.20,
        "Prezzo minimo FBM Amount": 30.979999999999997,
        "reference_price": 30.98, "price_source": "buy_box",
    } for asin in asins}


def fee_batch(requests_, _token, *, fba="4", referral="4.65"):
    rows = []
    for request in requests_:
        rows.append({"FeesEstimateResult": {
            "Status": "Success",
            "FeesEstimateIdentifier": {
                "IdValue": request["asin"],
                "SellerInputIdentifier": request["identifier"],
                "PriceToEstimateFees": {"ListingPrice": {
                    "Amount": request["price"], "CurrencyCode": "EUR",
                }},
            },
            "FeesEstimate": {"FeeDetailList": [
                {"FeeType": "ReferralFee", "FinalFee": {
                    "Amount": referral, "CurrencyCode": "EUR",
                }},
                {"FeeType": "FBAFees", "FinalFee": {
                    "Amount": fba, "CurrencyCode": "EUR",
                }},
            ]},
        }})
    return ProductFeeBatchResults(rows)


class DirectLookupTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.store = SupplierCatalogStore(Path(self.temporary.name) / "catalog.sqlite3")
        self.checkpoints = DiscoveryCheckpointStore(Path(self.temporary.name) / "jobs")
        self.token = SimpleNamespace(get=lambda: "token")

    def tearDown(self):
        self.temporary.cleanup()

    def test_direct_lookup_merges_cross_supplier_scenarios_without_qogita(self):
        publish(self.store, "abw", [scenario("abw"), scenario("abw", suffix="bulk_box", cost="8")])
        publish(self.store, "umma")
        publish(self.store, "qudo")
        context = load_direct_supplier_context(EAN, store=self.store)
        self.assertEqual(context["supplier_memberships"], ["abw", "qudo", "umma"])
        self.assertEqual(len(context["candidate"]["scenarios"]), 4)
        self.assertEqual(context["supplier_snapshot_set"]["qogita"]["availability_status"], "unavailable")

    def test_each_operational_supplier_can_be_loaded_alone(self):
        for supplier in ("abw", "umma", "qudo"):
            with self.subTest(supplier=supplier):
                isolated = SupplierCatalogStore(
                    Path(self.temporary.name) / f"{supplier}.sqlite3"
                )
                publish(isolated, supplier)
                context = load_direct_supplier_context(EAN, store=isolated)
                self.assertEqual(context["supplier_memberships"], [supplier])
                self.assertEqual(len(context["candidate"]["scenarios"]), 1)

    def test_direct_lookup_reads_enriched_qogita_from_serving_not_latest_success(self):
        snapshot = publish_qogita_serving(self.store)
        self.assertIsNone(self.store.active_generation_metadata("qogita"))
        context = load_direct_supplier_context(EAN, store=self.store)
        self.assertIn("qogita", context["supplier_memberships"])
        self.assertEqual(
            context["supplier_snapshot_set"]["qogita"]["snapshot_id"],
            snapshot["serving_generation_id"],
        )
        self.assertEqual(context["supplier_snapshot_set"]["qogita"]["scenario_count"], 1)

    def test_active_identifier_lookup_handles_zero_padded_gtin(self):
        publish(self.store, "qudo")
        result = self.store.active_candidates_for_identifier("qudo", "0" + EAN)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["canonical_ean"], EAN)

    def test_direct_pipeline_deduplicates_amazon_and_fans_out_all_scenarios(self):
        publish(self.store, "abw", [scenario("abw"), scenario("abw", suffix="bulk_box", cost="8")])
        publish(self.store, "umma")
        calls = {"catalog": 0, "pricing": 0, "fees": 0}
        def catalog(*args):
            calls["catalog"] += 1
            return catalog_found(*args)
        def pricing(*args):
            calls["pricing"] += 1
            return pricing_found(*args)
        def fees(*args, **kwargs):
            calls["fees"] += 1
            return fee_batch(*args, **kwargs)
        state = run_direct_lookup(
            EAN, store=self.store, checkpoint_store=self.checkpoints,
            catalog_batch=catalog, pricing_batch=pricing, fees_batch=fees,
            token_provider=self.token,
        )
        product = state["candidates"][0]
        self.assertEqual(calls, {"catalog": 1, "pricing": 1, "fees": 1})
        self.assertEqual(len(state["amazon_observations"]), 1)
        self.assertEqual(len(product["opportunity_combinations"]), 3)
        self.assertIsNotNone(product["recommended_combination"])
        self.assertIsNone(state.get("rotation_scope"))
        self.assertEqual(state["sampling_strategy"], "explicit_direct_identifier_v1")

    def test_multiple_scenarios_below_or_negative_margin_remain_visible(self):
        publish(self.store, "abw", [scenario("abw", cost="20"), scenario("abw", suffix="bulk_box", cost="18")])
        state = run_direct_lookup(
            EAN, store=self.store, checkpoint_store=self.checkpoints,
            catalog_batch=catalog_found, pricing_batch=pricing_found,
            fees_batch=lambda requests, token: fee_batch(requests, token, fba="12", referral="9"),
            token_provider=self.token,
        )
        rows = direct_scenario_rows(state)
        self.assertEqual(len(rows), 2)
        self.assertTrue(any(row["Stato"] == "margin_below_threshold" for row in rows))
        self.assertTrue(any(str(row["Margine"]).startswith("-") for row in rows))

    def test_supplier_scenarios_survive_amazon_not_found(self):
        publish(self.store, "qudo")
        calls = {"pricing": 0, "fees": 0}
        state = run_direct_lookup(
            EAN, store=self.store, checkpoint_store=self.checkpoints,
            catalog_batch=catalog_missing,
            pricing_batch=lambda *_: calls.__setitem__("pricing", calls["pricing"] + 1),
            fees_batch=lambda *_: calls.__setitem__("fees", calls["fees"] + 1),
            token_provider=self.token,
        )
        self.assertEqual(len(state["candidates"][0]["scenarios"]), 1)
        self.assertEqual(direct_scenario_rows(state)[0]["Stato"], "economics_unavailable")
        self.assertEqual(calls, {"pricing": 0, "fees": 0})

    def test_amazon_lookup_still_runs_without_supplier_and_no_manager_fallback(self):
        state = run_direct_lookup(
            EAN, store=self.store, checkpoint_store=self.checkpoints,
            catalog_batch=catalog_found, pricing_batch=pricing_found,
            fees_batch=fee_batch, token_provider=self.token,
        )
        self.assertEqual(state["candidates"][0]["scenarios"], [])
        self.assertEqual(len(state["candidates"][0]["amazon_listings"]), 1)
        self.assertEqual(state["supplier_memberships"], [])

    def test_freshness_and_carried_forward_timestamp_are_preserved(self):
        value = scenario("qudo", snapshot="2026-07-01T10:00:00Z")
        value["freshness_status"] = "carried_forward"
        publish(self.store, "qudo", [value])
        context = load_direct_supplier_context(EAN, store=self.store)
        row = context["candidate"]["scenarios"][0]
        self.assertEqual(row["snapshot_at"], "2026-07-01T10:00:00Z")
        self.assertEqual(row["freshness_status"], "carried_forward")

    def test_money_formatting_is_decimal_and_italian(self):
        self.assertEqual(format_eur(30.979999999999997), "€30,98")
        self.assertEqual(format_eur(1234.5), "€1.234,50")

    def test_checkpoint_contains_direct_audit_fields(self):
        publish(self.store, "qudo")
        state = run_direct_lookup(
            EAN, store=self.store, checkpoint_store=self.checkpoints,
            catalog_batch=catalog_missing, pricing_batch=pricing_found,
            fees_batch=fee_batch, token_provider=self.token,
        )
        saved = self.checkpoints.load(state["job_id"])
        self.assertEqual(saved["lookup_type"], "direct_ean")
        self.assertEqual(saved["ean_requested"], EAN)
        self.assertEqual(saved["supplier_memberships"], ["qudo"])
        self.assertIsNone(saved["rotation_scope"])

    def test_streamlit_direct_result_prioritizes_supplier_comparison(self):
        publish(self.store, "abw", [scenario("abw"), scenario("abw", suffix="bulk_box", cost="8")])
        publish(self.store, "umma")
        state = run_direct_lookup(
            EAN, store=self.store, checkpoint_store=self.checkpoints,
            catalog_batch=catalog_found, pricing_batch=pricing_found,
            fees_batch=fee_batch, token_provider=self.token,
        )
        app = AppTest.from_file("app_glowup.py", default_timeout=20).run()
        app.session_state["ui_state"] = "single_result"
        app.session_state["single_status"] = "found"
        app.session_state["single_product_result"] = {
            "state": json.loads(json.dumps(state, default=str)),
        }
        app.run()
        self.assertFalse(app.exception)
        self.assertTrue(any(
            element.value == "Migliore opzione di acquisto"
            for element in app.subheader
        ))
        self.assertTrue(any(
            element.value == "Confronto fornitori" for element in app.subheader
        ))
        self.assertEqual(len(app.dataframe), 1)
        self.assertEqual(len(app.dataframe[0].value), 3)
        self.assertIn("€30,98", app.dataframe[0].value["Prezzo Amazon"].tolist())


if __name__ == "__main__":
    unittest.main()
