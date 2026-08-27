import sqlite3
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from openpyxl import Workbook

from purchase_scenarios import product_key, scenario_key
from supplier_catalog import (
    SupplierCatalogError,
    SupplierCatalogGeneration,
    SupplierCatalogStore,
    canonical_gtin14,
    candidates_to_cache_records,
    generation_is_fresh,
    run_supplier_sync,
    supplier_generation_delta,
    supplier_promotion_gate,
    supplier_product_cache_key,
    supplier_catalog_lock,
)
from supplier_catalog_collectors import (
    AbwCatalogCollector,
    ABW_COVERAGE,
    QOGITA_COVERAGE,
    QOGITA_EXPORT_COLUMNS,
    QOGITA_EXPORT_COVERAGE,
    QogitaCatalogExportReader,
    SupplierCollectorError,
    QUDO_COVERAGE,
    UMMA_COVERAGE,
    _qudo_page_identifier_evidence,
    _qudo_page_gtins,
    compare_umma_enumerations,
    qogita_catalog_fingerprint,
    umma_enumeration_proof,
)


EAN = "8809738600238"


def scenario(supplier="qudo", ean=EAN, suffix="one"):
    identifier = scenario_key(
        supplier=supplier, supplier_alias=f"{supplier}-catalog",
        supplier_product_id="product-not-in-manager",
        supplier_offer_id="offer-1", variant_id=suffix,
        canonical_ean=ean, scenario_type=f"{supplier}_standard",
        account_mov=Decimal("300"),
    )
    return {
        "scenario_id": identifier, "product_key": product_key(ean),
        "canonical_ean": ean, "identifier_type": "EAN", "supplier": supplier,
        "supplier_alias": f"{supplier}-catalog",
        "supplier_product_id": "product-not-in-manager",
        "supplier_offer_id": "offer-1", "variant_id": suffix,
        "brand": "Round Lab", "title": "Birch Juice Cleanser 150 ml",
        "scenario_type": f"{supplier}_standard", "scenario_label": supplier.upper(),
        "scenario_order": 1, "account_mov": Decimal("300"),
        "account_mov_currency": "EUR", "account_mov_eur": Decimal("300"),
        "selling_unit": 1, "cost_net_unit_eur": Decimal("6"),
        "vat_rate": Decimal("0.22"), "vat_amount_unit": Decimal("1.32"),
        "cost_gross_unit_eur": Decimal("7.32"), "stock": 20,
        "snapshot_id": "generation", "snapshot_at": "2026-08-25T05:00:00Z",
        "freshness_status": "fresh", "tier_is_active": True,
        "supplier_barcode_raw": ean, "minimum_product_quantity": 1,
        "maximum_product_quantity": 20, "availability_status": "in_stock",
        "supplier_sku": "QUDO-ROUND-9", "product_url": "https://supplier.invalid/product",
    }


def candidate(supplier="qudo", ean=EAN):
    return {
        "product_key": product_key(ean), "canonical_ean": ean,
        "identifier_type": "EAN", "brand": "Round Lab",
        "title": "Birch Juice Cleanser 150 ml", "category": "Beauty",
        "image_url": "", "scenarios": [scenario(supplier, ean)],
    }


def generation(supplier="qudo", *, coverage_complete=True, duplicate=False):
    candidates = [candidate(supplier)]
    products, scenarios = candidates_to_cache_records(candidates)
    if duplicate:
        products = products + products
    return SupplierCatalogGeneration(
        supplier=supplier, coverage_type="full_supplier_catalog",
        coverage_description="fixture independent from Manager tracked products",
        coverage_complete=coverage_complete, products=products,
        scenarios=scenarios, page_count=3, request_count=7,
        retry_count=1, rate_limit_count=1,
        source_type="fixture_export", source_count=1,
        enumerated_count=1, unique_count=1,
        completeness_status="full_account_catalog",
        completeness_reason="Fixture proves complete enumeration",
        export_generated_at="2026-08-25T04:00:00Z",
        upstream_catalog_version="fixture-v1",
        diagnostics={"source_universe": "supplier"},
    )


class SupplierCatalogStoreTests(unittest.TestCase):
    def test_qudo_product_identity_ignores_display_sku_prefix(self):
        raw = supplier_product_cache_key(
            "qudo", "53907", supplier_option_id="53908", supplier_sku="abc123",
            fallback_identifier="8809738600238",
        )
        display = supplier_product_cache_key(
            "qudo", "53907", supplier_option_id="53908", supplier_sku="QUDO-abc123",
            fallback_identifier="8809738600238",
        )
        self.assertEqual(raw, display)

    def test_qudo_product_identity_uses_authoritative_product_and_variation_ids(self):
        page = supplier_product_cache_key(
            "qudo", "7640", supplier_option_id="46323", supplier_sku="HEIMISH-34",
            fallback_identifier="8809481760524",
        )
        offer = supplier_product_cache_key(
            "qudo", "7640", supplier_option_id="46323", supplier_sku="QUDO-HEIMISH-888",
            fallback_identifier="8809481760524",
        )
        self.assertEqual(page, offer)

    def test_qudo_gate_uses_canonical_identifiers_not_merely_extracted_values(self):
        value = generation("qudo")
        invalid_product = dict(value.products[0])
        invalid_product["canonical_product_key"] = "invalid"
        invalid_product["canonical_ean"] = "12345"
        invalid_product["canonical_gtin"] = None
        value = replace(
            value,
            products=(value.products[0], invalid_product),
            diagnostics={
                "global_catalog_total": 2,
                "qudo_offer_products": 2,
                "valid_gtin_products": 2,
                "normalizer": {"qudo_scenarios": len(value.scenarios)},
            },
        )
        gate = supplier_promotion_gate("qudo", value)
        self.assertIn("qudo_identifier_coverage_anomalous", gate["reasons"])

    def test_active_identifier_universe_uses_scenario_ids_and_gs1_validation(self):
        value = generation("qudo")
        invalid_product = dict(value.products[0])
        invalid_product.update({
            "canonical_product_key": "invalid-product",
            "canonical_ean": "12345",
            "canonical_gtin": None,
        })
        invalid_scenario = dict(value.scenarios[0])
        invalid_scenario.update({
            "scenario_id": "invalid-scenario",
            "canonical_product_key": "invalid-product",
            "canonical_ean": "12345",
        })
        invalid_scenario["payload"] = {
            **invalid_scenario["payload"], "canonical_ean": "12345",
        }
        value = replace(
            value,
            products=(*value.products, invalid_product),
            scenarios=(*value.scenarios, invalid_scenario),
        )
        run_id = self.store.start_run(
            "qudo", coverage_type=value.coverage_type,
            coverage_description=value.coverage_description,
            coverage_complete=True, sampled=False,
        )
        self.store.publish(run_id, value, elapsed_seconds=1)
        self.assertEqual(
            self.store.active_identifier_universe(["qudo"]),
            {"total": 2, "eligible": 1},
        )
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.database = Path(self.temporary.name) / "supplier.sqlite3"
        self.locks = Path(self.temporary.name) / "locks"
        self.store = SupplierCatalogStore(self.database)

    def tearDown(self):
        self.temporary.cleanup()

    def publish(self, supplier="qudo", value=None):
        value = value or generation(supplier)
        run_id = self.store.start_run(
            supplier, coverage_type=value.coverage_type,
            coverage_description=value.coverage_description,
            coverage_complete=value.coverage_complete, sampled=False,
        )
        self.store.publish(run_id, value, elapsed_seconds=1.5)
        return run_id

    def abw_export_fixture(self):
        path = Path(self.temporary.name) / "abw_fixture.xlsx"
        workbook = Workbook()
        sheet = workbook.active
        sheet.title = "ABW Beauty Product Catalog"
        sheet.append(["Date", "25-Aug-2026"])
        sheet.append([
            "ABW \nCatalog No.", "UPC", "Brand", "Product Name", "Option Name",
            "Wholesale Unit \nWeight / pc (g) \n#", "ABW Selling \nPrice (EUR)",
            "Availability", "Box Qty 1", "Box Selling \nPrice (EUR)",
            "Price / piece \n(EUR)", "Box Qty 1 \nAvailability", "Box Qty 2",
            "Box Selling \nPrice (EUR)", "Price / piece \n(EUR)",
            "Box Qty 2 \nAvailability", "Box Qty 3", "Box Selling \nPrice (EUR)",
            "Price / piece \n(EUR)", "Box Qty 3 \nAvailability", "Box Qty 4",
            "Box Selling \nPrice (EUR)", "Price / piece \n(EUR)",
            "Box Qty 4 \nAvailability",
        ])
        sheet.append([
            "ABW-1", EAN, "Round Lab", "Birch Juice Cleanser", "150 ml", 180,
            7.50, "1 to 2 days", 15, 90, 6, "1 to 2 days", 60, 330, 5.50,
            "21 days", None, None, None, None, None, None, None, None,
        ])
        sheet.append([
            "ABW-2", "8809562191070", "Brand", "Product", "Option", 10, 2,
            "21 days", 12, None, 1.5, "21 days", None, None, None, None,
            None, None, None, None, None, None, None, None,
        ])
        sheet.append([
            "ABW-3", "No Barcode", "Brand", "Unidentified Product", "Option", 10, 2,
            "21 days", None, None, None, None, None, None, None, None,
            None, None, None, None, None, None, None, None,
        ])
        sheet.append([
            "ABW-4", f"{EAN} x 15", "Round Lab", "Birch Juice Cleanser (x15) (Bulk Box)",
            "150 ml x 15", 2700, 90, "1 to 2 days", 15, None, 6,
            None, None, None, None, None, None, None, None, None, None, None, None, None,
        ])
        workbook.save(path)
        workbook.close()
        return path

    def qogita_export_fixture(self, rows=None):
        path = Path(self.temporary.name) / "qogita_fixture.csv"
        rows = rows or [
            ["5", "8022297071411", "Defining Wax 75ml", "Wax", "Alfaparf Milano",
             "6.74", "1", "22", "No", "", "2", "24", ""],
            ["15", "5010724533635", "Dry Shampoo 200 ml", "Dry Shampoo", "Batiste",
             "2.67", "1", "214", "No", "", "5", "594",
             "https://www.qogita.com/products/YKOV26/dry-shampoo/"],
        ]
        content = [
            "QOGITA CATALOG SAMPLE FOR CODEX",
            "Original data rows,439553",
            "Catalog as of,Catalog As Of 2026-08-25T14-57-22",
            "",
            ",".join(("Source Row",) + QOGITA_EXPORT_COLUMNS),
        ]
        content.extend(",".join(map(str, row)) for row in rows)
        path.write_text("\n".join(content) + "\n", encoding="utf-8")
        return path

    def test_qogita_export_parser_streams_aggregate_gtin_rows(self):
        reader = QogitaCatalogExportReader(self.qogita_export_fixture())
        iterator = reader.products()
        self.assertFalse(hasattr(iterator, "__len__"))
        products = list(iterator)
        self.assertEqual(len(products), 2)
        self.assertEqual(products[0]["canonical_gtin"], "08022297071411")
        self.assertEqual(products[0]["metadata"]["number_of_offers"], 2)
        self.assertEqual(products[0]["metadata"]["lowest_priced_offer_inventory"], 22)
        self.assertEqual(products[0]["enrichment_status"], "unresolved_variant")
        self.assertEqual(products[1]["variant_fid"], "YKOV26")
        self.assertEqual(products[1]["enrichment_status"], "enrichment_pending")
        self.assertNotIn("scenario_type", products[0])

    def test_qogita_export_metadata_preserves_authoritative_full_file_count(self):
        metadata = QogitaCatalogExportReader(self.qogita_export_fixture()).metadata()
        self.assertEqual(metadata["Original data rows"], "439553")
        self.assertEqual(metadata["Catalog as of"], "Catalog As Of 2026-08-25T14-57-22")
        self.assertIn("Product Link", metadata["columns"])

    def test_qogita_fingerprint_detects_catalog_signal_changes_not_tier_freshness(self):
        base = {
            "GTIN": "8022297071411", "Name": "Wax", "Category": "Wax", "Brand": "Brand",
            "€ Lowest Price inc. shipping": "6.74", "Unit": "1",
            "Lowest Priced Offer Inventory": "22", "Is a pre-order?": "No",
            "Estimated Delivery Time (weeks)": "", "Number of Offers": "2",
            "Total Inventory of All Offers": "24", "Product Link": "",
        }
        changed = {**base, "Total Inventory of All Offers": "25"}
        self.assertNotEqual(qogita_catalog_fingerprint(base), qogita_catalog_fingerprint(changed))

    def test_streaming_product_publish_separates_product_and_scenario_coverage(self):
        reader = QogitaCatalogExportReader(self.qogita_export_fixture())
        run_id = self.store.start_run(
            "qogita", coverage_type="filtered_catalog",
            coverage_description="filtered official export",
            coverage_complete=False, sampled=False,
        )
        result = self.store.publish_product_catalog_stream(
            run_id, supplier="qogita", products=reader.products(), elapsed_seconds=1,
            product_catalog_coverage_type="filtered_catalog",
            product_catalog_coverage_complete=False,
            scenario_enrichment_status="none", source_count=439553,
            source_type="official_qogita_async_catalog_export",
        )
        latest = self.store.latest_success("qogita")
        self.assertEqual(result["product_count"], 2)
        self.assertEqual(latest["source_count"], 439553)
        self.assertEqual(latest["product_catalog_coverage_type"], "filtered_catalog")
        self.assertFalse(latest["product_catalog_coverage_complete"])
        self.assertEqual(latest["scenario_enrichment_status"], "none")
        self.assertEqual(latest["scenario_count"], 0)

    def test_439k_metadata_path_accepts_single_pass_iterable_without_len(self):
        class SinglePassProducts:
            iterated = False

            def __len__(self):
                raise AssertionError("stream must not be materialized or sized")

            def __iter__(self):
                if self.iterated:
                    raise AssertionError("stream must not be consumed twice")
                self.iterated = True
                for index in range(1000):
                    yield {
                        "canonical_product_key": f"qogita-stream-{index}",
                        "canonical_ean": None, "canonical_gtin": None,
                        "identifier_type": "GTIN", "raw_identifiers": [],
                        "supplier_product_id": str(index), "brand": "Brand",
                        "title": f"Product {index}",
                        "catalog_fingerprint": f"fingerprint-{index}",
                        "metadata": {},
                    }

        source = SinglePassProducts()
        run_id = self.store.start_run(
            "qogita", coverage_type="filtered_catalog", coverage_description="fixture",
            coverage_complete=False, sampled=False,
        )
        self.store.publish_product_catalog_stream(
            run_id, supplier="qogita", products=source, elapsed_seconds=1,
            product_catalog_coverage_type="filtered_catalog",
            product_catalog_coverage_complete=False, source_count=439553,
        )
        latest = self.store.latest_success("qogita")
        self.assertEqual(latest["source_count"], 439553)
        self.assertEqual(latest["product_count"], 1000)
        self.assertEqual(sum(1 for _ in self.store.iter_products(run_id, fetch_size=73)), 1000)

    def test_streaming_publish_computes_new_changed_removed_without_loading_previous(self):
        first_path = self.qogita_export_fixture()
        first_run = self.store.start_run(
            "qogita", coverage_type="filtered_catalog", coverage_description="fixture",
            coverage_complete=False, sampled=False,
        )
        self.store.publish_product_catalog_stream(
            first_run, supplier="qogita",
            products=QogitaCatalogExportReader(first_path).products(), elapsed_seconds=1,
            product_catalog_coverage_type="filtered_catalog",
            product_catalog_coverage_complete=False,
        )
        second_path = self.qogita_export_fixture(rows=[
            ["5", "8022297071411", "Defining Wax 75ml", "Wax", "Alfaparf Milano",
             "6.80", "1", "22", "No", "", "2", "24", ""],
            ["16", "5706710002871", "Gaven", "Eau De Parfum", "Palladium",
             "3.55", "3", "75", "No", "", "1", "75", ""],
        ])
        second_run = self.store.start_run(
            "qogita", coverage_type="filtered_catalog", coverage_description="fixture",
            coverage_complete=False, sampled=False,
        )
        result = self.store.publish_product_catalog_stream(
            second_run, supplier="qogita",
            products=QogitaCatalogExportReader(second_path).products(), elapsed_seconds=1,
            product_catalog_coverage_type="filtered_catalog",
            product_catalog_coverage_complete=False,
        )
        self.assertEqual(result["generation_delta"], {
            "new": 1, "changed": 1, "removed": 1, "unchanged": 0,
        })

    def test_unchanged_scenarios_are_carried_with_original_tier_freshness(self):
        product = next(QogitaCatalogExportReader(self.qogita_export_fixture()).products(limit=1))
        product = {
            **product, "enrichment_status": "full",
            "offer_tier_observed_at": "2026-08-24T12:00:00Z",
        }
        payload = scenario("qogita", ean="8022297071411")
        scenario_row = {
            "scenario_id": payload["scenario_id"],
            "canonical_product_key": product["canonical_product_key"],
            "canonical_ean": payload["canonical_ean"],
            "scenario_type": payload["scenario_type"],
            "scenario_label": payload["scenario_label"],
            "payload": payload,
        }
        first_generation = SupplierCatalogGeneration(
            supplier="qogita", coverage_type="filtered_catalog",
            coverage_description="fixture", coverage_complete=False,
            products=(product,), scenarios=(scenario_row,),
            completeness_status="filtered_catalog",
        )
        first_run = self.store.start_run(
            "qogita", coverage_type="filtered_catalog", coverage_description="fixture",
            coverage_complete=False, sampled=False,
        )
        self.store.publish(first_run, first_generation, elapsed_seconds=1)
        second_run = self.store.start_run(
            "qogita", coverage_type="filtered_catalog", coverage_description="fixture",
            coverage_complete=False, sampled=False,
        )
        result = self.store.publish_product_catalog_stream(
            second_run, supplier="qogita",
            products=QogitaCatalogExportReader(self.qogita_export_fixture()).products(limit=1),
            elapsed_seconds=1, product_catalog_coverage_type="filtered_catalog",
            product_catalog_coverage_complete=False,
            reuse_unchanged_scenarios_after="2026-08-24T00:00:00Z",
        )
        self.assertEqual(result["reused_scenarios"], 1)
        carried = next(self.store.iter_products(second_run))
        self.assertEqual(carried["enrichment_status"], "carried_forward")
        self.assertEqual(carried["offer_tier_observed_at"], "2026-08-24T12:00:00Z")
        metadata = self.store.run_status(second_run)
        self.assertEqual(metadata["scenario_enrichment_status"], "partial")
        self.assertEqual(metadata["scenario_enrichment_count"], 1)

    def test_explicit_unpromoted_generation_can_be_comparison_baseline(self):
        fixture = self.qogita_export_fixture()
        first_run = self.store.start_run(
            "qogita", coverage_type="filtered_catalog", coverage_description="fixture",
            coverage_complete=False, sampled=False,
        )
        self.store.publish_product_catalog_stream(
            first_run, supplier="qogita",
            products=QogitaCatalogExportReader(fixture).products(), elapsed_seconds=1,
            product_catalog_coverage_type="filtered_catalog",
            product_catalog_coverage_complete=False, promote=False,
        )
        self.assertIsNone(self.store.active_generation_metadata("qogita"))
        second_run = self.store.start_run(
            "qogita", coverage_type="filtered_catalog", coverage_description="fixture",
            coverage_complete=False, sampled=False,
        )
        result = self.store.publish_product_catalog_stream(
            second_run, supplier="qogita",
            products=QogitaCatalogExportReader(fixture).products(), elapsed_seconds=1,
            product_catalog_coverage_type="filtered_catalog",
            product_catalog_coverage_complete=False, promote=False,
            previous_run_id=first_run,
        )
        self.assertEqual(result["generation_delta"], {
            "new": 0, "changed": 0, "removed": 0, "unchanged": 2,
        })
        self.assertIsNone(self.store.active_generation_metadata("qogita"))

    def test_variant_identity_is_reused_but_changed_offer_state_is_not(self):
        product = next(QogitaCatalogExportReader(self.qogita_export_fixture()).products(limit=1))
        product.update({
            "variant_fid": "FID-STABLE",
            "variant_fid_source": "qogita_product_link_redirect",
            "enrichment_status": "enriched",
            "offer_tier_observed_at": "2026-08-24T12:00:00Z",
        })
        first = SupplierCatalogGeneration(
            supplier="qogita", coverage_type="filtered_catalog",
            coverage_description="fixture", coverage_complete=False,
            products=(product,), scenarios=(), completeness_status="filtered_catalog",
        )
        first_run = self.store.start_run(
            "qogita", coverage_type="filtered_catalog", coverage_description="fixture",
            coverage_complete=False, sampled=False,
        )
        self.store.publish(first_run, first, elapsed_seconds=1, promote=False)
        changed_path = self.qogita_export_fixture(rows=[
            ["5", "8022297071411", "Defining Wax 75ml", "Wax", "Alfaparf Milano",
             "7.10", "1", "22", "No", "", "2", "24", ""],
        ])
        second_run = self.store.start_run(
            "qogita", coverage_type="filtered_catalog", coverage_description="fixture",
            coverage_complete=False, sampled=False,
        )
        self.store.publish_product_catalog_stream(
            second_run, supplier="qogita",
            products=QogitaCatalogExportReader(changed_path).products(), elapsed_seconds=1,
            product_catalog_coverage_type="filtered_catalog",
            product_catalog_coverage_complete=False, promote=False,
            previous_run_id=first_run,
            reuse_unchanged_scenarios_after="2026-08-24T00:00:00Z",
        )
        current = next(self.store.iter_products(second_run))
        self.assertEqual(current["catalog_delta_status"], "changed")
        self.assertEqual(current["variant_fid"], "FID-STABLE")
        self.assertEqual(current["variant_fid_source"], "qogita_product_link_redirect")
        self.assertEqual(current["enrichment_status"], "enrichment_pending")
        self.assertIsNone(current["offer_tier_observed_at"])

    def test_qogita_export_coverage_is_not_overstated_as_full(self):
        self.assertEqual(QOGITA_EXPORT_COVERAGE["type"], "filtered_catalog")
        self.assertFalse(QOGITA_EXPORT_COVERAGE["complete"])

    def test_abw_official_export_preserves_rows_and_builds_authoritative_boxes(self):
        value = AbwCatalogCollector(source_file=self.abw_export_fixture())(
            run_id="abw-export", dry_run=True,
        )
        self.assertEqual(value.source_type, "official_abw_beauty_xlsx")
        self.assertEqual(value.source_count, 4)
        self.assertEqual(value.enumerated_count, 4)
        self.assertTrue(value.coverage_complete)
        self.assertEqual(value.product_catalog_coverage_type, "full_relevant_catalog")
        self.assertEqual(value.scenario_enrichment_status, "partial")
        self.assertEqual(len(value.products), 4)
        self.assertEqual(len(value.scenarios), 5)
        modes = [row["scenario_type"] for row in value.scenarios]
        self.assertEqual(modes.count("abw_standard"), 2)
        self.assertEqual(modes.count("abw_bulk_box"), 3)
        boxes = sorted(
            (row for row in value.scenarios if row["scenario_type"] == "abw_bulk_box"),
            key=lambda row: row["payload"]["bundle_quantity"],
        )
        self.assertEqual(boxes[0]["payload"]["bundle_quantity"], 15)
        self.assertEqual(boxes[0]["payload"]["cost_net_unit_eur"], Decimal("6"))
        self.assertEqual(boxes[0]["payload"]["cost_gross_unit_eur"], Decimal("7.32"))
        box_60 = next(row for row in boxes if row["payload"]["bundle_quantity"] == 60)
        self.assertEqual(box_60["payload"]["source_pack_total_price"], Decimal("330"))
        composite = next(
            row for row in boxes
            if row["payload"]["source_metadata"]["price_source"]
            == "official_abw_catalog_export_bulk_product_total"
        )
        self.assertEqual(composite["payload"]["source_pack_total_price"], Decimal("90"))
        self.assertEqual(composite["payload"]["cost_net_unit_eur"], Decimal("6"))
        self.assertEqual(composite["payload"]["source_metadata"]["displayed_unit_price"], "6")
        raw_product = next(row for row in value.products if row["supplier_product_id"] == "ABW-4")
        self.assertEqual(raw_product["raw_identifiers"][0]["value"], f"{EAN} x 15")
        self.assertEqual(raw_product["raw_identifiers"][0]["type"], "EAN_X_QUANTITY")

    def test_abw_export_keeps_raw_invalid_identifier_and_skips_unproven_box_total(self):
        value = AbwCatalogCollector(source_file=self.abw_export_fixture())(
            run_id="abw-export", dry_run=True,
        )
        invalid = next(row for row in value.products if row["supplier_product_id"] == "ABW-3")
        self.assertIsNone(invalid["canonical_ean"])
        self.assertEqual(invalid["raw_identifiers"][0]["value"], "No Barcode")
        self.assertEqual(len(value.diagnostics["bulk_boxes_missing_authoritative_total"]), 1)
        self.assertEqual(value.diagnostics["bulk_boxes_missing_authoritative_total"][0]["catalog_no"], "ABW-2")

    def test_success_generation_is_latest_and_round_trips_purchase_scenario(self):
        run_id = self.publish()
        latest = self.store.latest_success("qudo")
        self.assertEqual(latest["run_id"], run_id)
        self.assertEqual(latest["product_count"], 1)
        self.assertEqual(latest["scenario_count"], 1)
        self.assertEqual(latest["scenarios"][0]["cost_gross_unit_eur"], Decimal("7.32"))
        self.assertEqual(latest["products"][0]["raw_identifiers"][0]["value"], EAN)
        self.assertEqual(latest["products"][0]["canonical_gtin"], "08809738600238")
        self.assertEqual(latest["retry_count"], 1)
        self.assertEqual(latest["rate_limit_count"], 1)
        self.assertEqual(latest["source_type"], "fixture_export")
        self.assertEqual(latest["source_count"], 1)
        self.assertEqual(latest["enumerated_count"], 1)
        self.assertEqual(latest["unique_count"], 1)
        self.assertEqual(latest["completeness_status"], "full_account_catalog")
        self.assertEqual(latest["upstream_catalog_version"], "fixture-v1")

    def test_failed_generation_is_not_promoted_and_previous_remains_active(self):
        first = self.publish()

        class Failing:
            coverage = {"type": "full_supplier_catalog", "description": "fixture", "complete": True}
            def __call__(self, **_):
                raise RuntimeError("page 2 failed")

        result = run_supplier_sync(
            "qudo", Failing(), store=self.store, lock_directory=self.locks,
        )
        self.assertEqual(result["status"], "failed")
        self.assertEqual(self.store.latest_success("qudo")["run_id"], first)
        self.assertEqual(self.store.run_status(result["run_id"])["status"], "failed")

    def test_collector_failure_diagnostics_are_persisted(self):
        class Failing:
            coverage = {"type": "full_supplier_catalog", "description": "fixture", "complete": True}
            def __call__(self, **_):
                raise SupplierCollectorError(
                    "Qudo collector failed during catalog_index",
                    code="remote_error",
                    diagnostics={"phase": "catalog_index", "page_number": 12, "remote_status": 503},
                )

        result = run_supplier_sync(
            "qudo", Failing(), store=self.store, lock_directory=self.locks,
        )
        persisted = self.store.run_status(result["run_id"])
        self.assertEqual(result["diagnostics"]["page_number"], 12)
        self.assertEqual(persisted["diagnostics"]["remote_status"], 503)

    def test_atomic_publish_rejects_duplicate_product_without_replacing_latest(self):
        first = self.publish()

        class Duplicate:
            coverage = {"type": "full_supplier_catalog", "description": "fixture", "complete": True}
            def __call__(self, **_):
                return generation(duplicate=True)

        result = run_supplier_sync(
            "qudo", Duplicate(), store=self.store, lock_directory=self.locks,
        )
        self.assertEqual(result["status"], "failed")
        self.assertEqual(self.store.latest_success("qudo")["run_id"], first)
        with sqlite3.connect(self.database) as connection:
            count = connection.execute(
                "SELECT COUNT(*) FROM supplier_catalog_products WHERE run_id=?",
                (result["run_id"],),
            ).fetchone()[0]
        self.assertEqual(count, 0)

    def test_sample_limit_is_saved_but_never_promoted(self):
        class Collector:
            coverage = {"type": "full_supplier_catalog", "description": "fixture", "complete": True}
            def __call__(self, **_):
                return generation()

        result = run_supplier_sync(
            "qudo", Collector(), store=self.store, limit=1,
            lock_directory=self.locks,
        )
        self.assertEqual(result["status"], "sample_success")
        self.assertFalse(result["promoted"])
        self.assertIsNone(self.store.latest_success("qudo"))

    def test_complete_generation_can_be_inspected_then_promoted_explicitly(self):
        class Collector:
            coverage = {"type": "full_supplier_catalog", "description": "fixture", "complete": True}
            def __call__(self, **_):
                value = generation()
                return SupplierCatalogGeneration(
                    **{
                        **value.__dict__,
                        "diagnostics": {
                            "global_catalog_total": 1,
                            "qudo_offer_products": 1,
                            "scenario_products": 1,
                        },
                    }
                )

        result = run_supplier_sync(
            "qudo", Collector(), store=self.store, promote=False,
            lock_directory=self.locks,
        )
        self.assertTrue(result["promotion_authorized"])
        self.assertFalse(result["promoted"])
        self.assertIsNone(self.store.latest_success("qudo"))
        self.store.promote_run(result["run_id"])
        self.assertEqual(self.store.latest_success("qudo")["run_id"], result["run_id"])

    def test_dry_run_does_not_create_database(self):
        class Collector:
            coverage = {"type": "full_supplier_catalog", "description": "fixture", "complete": True}
            def __call__(self, **_):
                return generation()

        result = run_supplier_sync(
            "qudo", Collector(), store=self.store, dry_run=True,
            lock_directory=self.locks,
        )
        self.assertEqual(result["status"], "dry_run")
        self.assertFalse(self.database.exists())

    def test_generation_delta_detects_new_changed_removed_without_snapshot_noise(self):
        current = generation()
        unchanged_product = dict(current.products[0])
        unchanged_scenario = dict(current.scenarios[0])
        unchanged_scenario["payload"] = {
            **unchanged_scenario["payload"],
            "snapshot_id": "old-run", "snapshot_at": "2026-08-24T01:00:00Z",
        }
        removed_product = {**unchanged_product, "canonical_product_key": "removed-product"}
        removed_scenario = {
            **unchanged_scenario,
            "scenario_id": "removed-scenario",
            "payload": {**unchanged_scenario["payload"], "scenario_id": "removed-scenario"},
        }
        changed_product = {**unchanged_product, "title": "Old title"}
        previous = {
            "products": [
                {**changed_product, "run_id": "old-run", "supplier": "qudo"},
                {**removed_product, "run_id": "old-run", "supplier": "qudo"},
            ],
            "scenarios": [unchanged_scenario["payload"], removed_scenario["payload"]],
        }

        delta = supplier_generation_delta(previous, current)

        self.assertEqual(delta["counts"]["products_changed"], 1)
        self.assertEqual(delta["counts"]["products_removed"], 1)
        self.assertEqual(delta["counts"]["scenarios_changed"], 0)
        self.assertEqual(delta["counts"]["scenarios_removed"], 1)

    def test_successful_sync_persists_generation_delta_against_active_generation(self):
        class Collector:
            coverage = {"type": "full_supplier_catalog", "description": "fixture", "complete": True}
            def __call__(self, **_):
                return generation()

        first = run_supplier_sync(
            "qudo", Collector(), store=self.store, lock_directory=self.locks,
        )
        second = run_supplier_sync(
            "qudo", Collector(), store=self.store, lock_directory=self.locks,
        )

        self.assertEqual(first["status"], "success")
        self.assertEqual(second["status"], "success")
        delta = self.store.latest_success("qudo")["diagnostics"]["generation_delta"]
        self.assertEqual(delta["counts"], {
            "products_new": 0, "products_changed": 0, "products_removed": 0,
            "scenarios_new": 0, "scenarios_changed": 0, "scenarios_removed": 0,
        })

    def test_nonblocking_lock_skips_second_sync(self):
        with supplier_catalog_lock("qudo", lock_directory=self.locks) as acquired:
            self.assertTrue(acquired)
            with supplier_catalog_lock("qudo", lock_directory=self.locks) as second:
                self.assertFalse(second)

    def test_freshness_boundary(self):
        now = datetime(2026, 8, 25, 12, tzinfo=timezone.utc)
        completed = (now - timedelta(hours=48)).isoformat()
        self.assertTrue(generation_is_fresh({"status": "success", "completed_at": completed}, 48, now=now))
        self.assertFalse(generation_is_fresh({"status": "success", "completed_at": completed}, 24, now=now))

    def test_product_not_in_manager_is_persisted_from_supplier_universe(self):
        self.publish()
        latest = self.store.latest_success("qudo")
        self.assertEqual(latest["products"][0]["supplier_product_id"], "product-not-in-manager")
        self.assertEqual(latest["diagnostics"]["source_universe"], "supplier")

    def test_latest_candidates_are_built_only_from_scout_generation(self):
        run_id = self.publish()

        candidates = self.store.latest_candidates("qudo")

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["canonical_ean"], "8809738600238")
        self.assertEqual(candidates[0]["supplier_catalog_run_id"], run_id)
        self.assertEqual(candidates[0]["scenarios"][0]["supplier"], "qudo")
        self.assertIsInstance(
            candidates[0]["scenarios"][0]["cost_gross_unit_eur"], Decimal
        )

    def test_raw_ean_and_distinct_ed_barcode_are_preserved(self):
        normal = candidate("umma", "8809657116544")
        normal["scenarios"][0]["supplier_barcode_raw"] = "8809657116544ED"
        products, scenarios = candidates_to_cache_records([normal])
        self.assertEqual(products[0]["raw_identifiers"][0]["value"], "8809657116544ED")
        self.assertEqual(scenarios[0]["canonical_ean"], "8809657116544")
        self.assertEqual(products[0]["canonical_gtin"], "08809657116544")

    def test_canonical_gtin_accepts_upc_ean_gtin_and_rejects_bad_check_digit(self):
        self.assertEqual(canonical_gtin14("085805268848"), "00085805268848")
        self.assertEqual(canonical_gtin14(EAN), "0" + EAN)
        self.assertEqual(canonical_gtin14("0" + EAN), "0" + EAN)
        self.assertIsNone(canonical_gtin14("8809738600239"))

    def test_same_physical_name_with_different_supplier_eans_stays_independent(self):
        q = candidate("qudo", "8809738600238")
        u = candidate("umma", "8809657116544")
        products, scenarios = candidates_to_cache_records([q, u])
        self.assertEqual(len(products), 2)
        self.assertEqual({row["canonical_ean"] for row in products}, {"8809738600238", "8809657116544"})
        self.assertEqual(len(scenarios), 2)

    def test_distinct_supplier_products_with_same_ean_are_not_collapsed(self):
        first = candidate("umma", EAN)
        second = candidate("umma", EAN)
        second["scenarios"][0]["supplier_product_id"] = "another-product"
        second["scenarios"][0]["scenario_id"] = scenario_key(
            supplier="umma", supplier_alias="umma-catalog",
            supplier_product_id="another-product", supplier_offer_id="offer-1",
            variant_id="one", canonical_ean=EAN,
            scenario_type="umma_standard", account_mov=Decimal("300"),
        )

        products, scenarios = candidates_to_cache_records([first, second])

        self.assertEqual(len(products), 2)
        self.assertEqual(len(scenarios), 2)
        self.assertEqual({row["canonical_ean"] for row in products}, {EAN})

    def test_qogita_tiers_share_supplier_product_but_remain_scenarios(self):
        value = candidate("qogita", EAN)
        second = dict(value["scenarios"][0])
        second["scenario_id"] = scenario_key(
            supplier="qogita", supplier_alias="qogita-catalog",
            supplier_product_id="product-not-in-manager",
            supplier_offer_id="offer-1", variant_id="one",
            canonical_ean=EAN, scenario_type="qogita_standard",
            account_mov=Decimal("500"),
        )
        second["account_mov"] = Decimal("500")
        value["scenarios"].append(second)

        products, scenarios = candidates_to_cache_records([value])

        self.assertEqual(len(products), 1)
        self.assertEqual(len(scenarios), 2)

    def test_qogita_multiple_seller_offers_and_tiers_are_not_collapsed(self):
        first = candidate("qogita", EAN)
        first["scenarios"][0]["supplier_alias"] = "seller-a"
        tier = dict(first["scenarios"][0])
        tier["account_mov"] = Decimal("500")
        tier["scenario_id"] = scenario_key(
            supplier="qogita", supplier_alias="seller-a",
            supplier_product_id="product-not-in-manager", supplier_offer_id="offer-1",
            variant_id="one", canonical_ean=EAN,
            scenario_type="qogita_standard", account_mov=Decimal("500"),
        )
        first["scenarios"].append(tier)
        second = candidate("qogita", EAN)
        second["scenarios"][0]["supplier_alias"] = "seller-b"
        second["scenarios"][0]["supplier_offer_id"] = "offer-2"
        second["scenarios"][0]["scenario_id"] = scenario_key(
            supplier="qogita", supplier_alias="seller-b",
            supplier_product_id="product-not-in-manager", supplier_offer_id="offer-2",
            variant_id="two", canonical_ean=EAN,
            scenario_type="qogita_standard", account_mov=Decimal("300"),
        )

        products, scenarios = candidates_to_cache_records([first, second])

        self.assertEqual(len(products), 2)
        self.assertEqual(len(scenarios), 3)
        self.assertEqual({row["supplier_offer_id"] for row in scenarios}, {"offer-1", "offer-2"})

    def test_partial_export_count_above_ten_thousand_is_persisted_without_full_claim(self):
        value = generation()
        value = SupplierCatalogGeneration(
            **{
                **value.__dict__,
                "coverage_complete": False,
                "source_type": "official_catalog_export",
                "source_count": 10005,
                "enumerated_count": 10005,
                "unique_count": 10005,
                "completeness_status": "partial_catalog",
                "completeness_reason": "Offer enrichment not yet executed",
            }
        )
        run_id = self.store.start_run(
            "qudo", coverage_type=value.coverage_type,
            coverage_description=value.coverage_description,
            coverage_complete=False, sampled=False,
        )
        self.store.publish(run_id, value, elapsed_seconds=1)
        latest = self.store.latest_success("qudo")
        self.assertEqual(latest["source_count"], 10005)
        self.assertEqual(latest["enumerated_count"], 10005)
        self.assertFalse(latest["coverage_complete"])

    def test_coverage_contract_does_not_overstate_unproven_sources(self):
        self.assertEqual(QOGITA_COVERAGE["type"], "partial_catalog")
        self.assertFalse(QOGITA_COVERAGE["complete"])
        self.assertEqual(UMMA_COVERAGE["type"], "partial_catalog")
        self.assertFalse(UMMA_COVERAGE["complete"])
        self.assertTrue(ABW_COVERAGE["complete"])
        self.assertEqual(QUDO_COVERAGE["type"], "full_relevant_catalog")
        self.assertTrue(QUDO_COVERAGE["complete"])

    def test_partial_proof_cannot_be_published_as_complete(self):
        value = generation()
        value = SupplierCatalogGeneration(
            **{**value.__dict__, "completeness_status": "partial_catalog"}
        )
        run_id = self.store.start_run(
            "qudo", coverage_type=value.coverage_type,
            coverage_description=value.coverage_description,
            coverage_complete=True, sampled=False,
        )
        with self.assertRaisesRegex(SupplierCatalogError, "FULL completeness proof"):
            self.store.publish(run_id, value, elapsed_seconds=1)

    def test_legacy_run_schema_is_extended_with_completeness_proof_columns(self):
        with sqlite3.connect(self.database) as connection:
            connection.execute('''CREATE TABLE supplier_catalog_runs (
                run_id TEXT PRIMARY KEY, supplier TEXT NOT NULL, started_at TEXT NOT NULL,
                completed_at TEXT, status TEXT NOT NULL, product_count INTEGER DEFAULT 0,
                scenario_count INTEGER DEFAULT 0, page_count INTEGER DEFAULT 0,
                request_count INTEGER DEFAULT 0, retry_count INTEGER DEFAULT 0,
                rate_limit_count INTEGER DEFAULT 0, server_error_count INTEGER DEFAULT 0,
                elapsed_seconds REAL, coverage_type TEXT NOT NULL,
                coverage_description TEXT NOT NULL, coverage_complete INTEGER DEFAULT 0,
                sampled INTEGER DEFAULT 0, error_code TEXT, error_message TEXT,
                diagnostics_json TEXT DEFAULT '{}'
            )''')
        self.store.initialize()
        with sqlite3.connect(self.database) as connection:
            columns = {row[1] for row in connection.execute("PRAGMA table_info(supplier_catalog_runs)")}
        self.assertTrue({
            "source_type", "source_count", "enumerated_count", "unique_count",
            "completeness_status", "completeness_reason", "export_generated_at",
            "upstream_catalog_version",
        }.issubset(columns))
        with sqlite3.connect(self.database) as connection:
            product_columns = {
                row[1] for row in connection.execute("PRAGMA table_info(supplier_catalog_products)")
            }
        self.assertIn("canonical_gtin", product_columns)

    def test_umma_enumeration_proof_detects_duplicates_and_gap(self):
        proof = umma_enumeration_proof(5, ["5", "4", "4", "2"])
        self.assertEqual(proof["enumerated_count"], 4)
        self.assertEqual(proof["unique_count"], 3)
        self.assertEqual(proof["duplicate_count"], 1)
        self.assertEqual(proof["enumeration_gap"], 2)
        self.assertFalse(proof["monotonic_desc"])

    def test_umma_stable_enumeration_comparison(self):
        stable = compare_umma_enumerations(["3", "2", "1"], ["1", "2", "3"])
        changed = compare_umma_enumerations(["3", "2"], ["3", "1"])
        self.assertTrue(stable["same_set"])
        self.assertEqual(stable["union_count"], 3)
        self.assertFalse(changed["same_set"])
        self.assertEqual(changed["only_first"], 1)
        self.assertEqual(changed["only_second"], 1)

    def test_qudo_product_owned_ean_precedes_json_ld(self):
        page = '''
        <h2 class="qudo-ean">EAN: 8809738600238</h2>
        <script type="application/ld+json">{"@type":"Product","gtin13":"8800000000000"}</script>
        '''
        self.assertEqual(_qudo_page_gtins(page), ["8809738600238"])

    def test_qudo_json_ld_is_fallback_when_product_marker_is_absent(self):
        page = '<script type="application/ld+json">{"@type":"Product","gtin13":"8809738600238"}</script>'
        self.assertEqual(_qudo_page_gtins(page), ["8809738600238"])

    def test_qudo_identifier_provenance_preserves_marker_and_json_ld(self):
        page = (
            '<h2 class="qudo-ean">EAN: 8809738600238</h2>'
            '<script type="application/ld+json">'
            '{"@type":"Product","gtin13":"8809738600238"}</script>'
        )
        evidence = _qudo_page_identifier_evidence(page)
        self.assertEqual(evidence["identifier_source"], "html_marker")
        self.assertEqual(evidence["marker_gtins"], ["8809738600238"])
        self.assertEqual(evidence["json_ld_gtins"], ["8809738600238"])
        self.assertEqual(evidence["selected_gtins"], ["8809738600238"])

    def test_qudo_identifier_provenance_marks_json_ld_fallback(self):
        page = '<script type="application/ld+json">{"@type":"Product","gtin13":"8809738600238"}</script>'
        evidence = _qudo_page_identifier_evidence(page)
        self.assertEqual(evidence["identifier_source"], "json_ld")
        self.assertEqual(evidence["marker_gtins"], [])
        self.assertEqual(evidence["selected_gtins"], ["8809738600238"])

    def test_qudo_invalid_marker_falls_back_to_valid_json_ld_upc(self):
        page = (
            '<h2 class="qudo-ean">EAN: 000000310651</h2>'
            '<script type="application/ld+json">'
            '{"@type":"Product","upc":"085805268848"}</script>'
        )
        evidence = _qudo_page_identifier_evidence(page)
        self.assertEqual(evidence["identifier_source"], "json_ld")
        self.assertEqual(evidence["marker_gtins"], ["000000310651"])
        self.assertEqual(evidence["json_ld_gtins"], ["085805268848"])
        self.assertEqual(evidence["selected_gtins"], ["085805268848"])

    def test_abw_full_run_is_blocked_until_official_download_is_integrated(self):
        collector = AbwCatalogCollector(manager_root=Path(self.temporary.name))
        with self.assertRaisesRegex(RuntimeError, "All Products catalog download"):
            collector(run_id="run", limit=None)

    def test_abw_sample_cannot_cross_unvalidated_page_boundary(self):
        collector = AbwCatalogCollector(manager_root=Path(self.temporary.name))
        with self.assertRaisesRegex(ValueError, "first 36"):
            collector(run_id="run", limit=37)


if __name__ == "__main__":
    unittest.main()
