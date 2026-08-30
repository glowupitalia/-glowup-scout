import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch
import streamlit as st
from streamlit.testing.v1 import AppTest

from discovery import DiscoveryCheckpointStore, default_filters, run_discovery, validate_filters
from supplier_preparation import (
    DISCOVERY_SAMPLING_STRATEGY,
    SUPPORTED_SUPPLIERS,
    inspect_supplier_rows,
    normalize_selected_suppliers,
    prepare_suppliers,
    refresh_manager_supplier,
    sample_discovery_candidates,
)
from supplier_catalog import SupplierCatalogGeneration, SupplierCatalogStore, candidates_to_cache_records
from discovery_rotation import DiscoveryRotationStore


NOW = datetime(2026, 8, 24, 12, tzinfo=timezone.utc)


def valid_ean(index):
    prefix = f"{index:012d}"
    total = sum(
        int(char) * (1 if position % 2 == 0 else 3)
        for position, char in enumerate(prefix)
    )
    return prefix + str((10 - total % 10) % 10)


def scenario(supplier, ean, suffix="one"):
    return {
        "scenario_id": f"{supplier}|{ean}|{suffix}",
        "product_key": f"ean|{ean}", "canonical_ean": ean,
        "identifier_type": "EAN", "supplier": supplier,
        "supplier_alias": supplier, "supplier_product_id": suffix,
        "scenario_type": f"{supplier}_standard", "scenario_label": supplier.upper(),
        "scenario_order": 1, "account_mov": Decimal("100"),
        "account_mov_currency": "EUR", "selling_unit": 1,
        "cost_net_unit_eur": Decimal("10"), "vat_rate": Decimal("0.22"),
        "vat_amount_unit": Decimal("2.2"), "cost_gross_unit_eur": Decimal("12.2"),
        "stock": 10, "snapshot_id": "run-1",
        "snapshot_at": "2026-08-24T10:00:00Z", "freshness_status": "fresh",
    }


def candidate(supplier, ean="8809532220748"):
    return {
        "product_key": f"ean|{ean}", "canonical_ean": ean, "gtin": ean,
        "identifier_type": "EAN", "brand": "Brand", "title": "Product",
        "scenarios": [scenario(supplier, ean)],
    }


def fresh_row(ean="8809532220748"):
    return {
        "gtin": ean, "run_id": "run-1", "seller_alias": "qogita_primary",
        "observed_at": "2026-08-24T10:00:00Z",
    }


def component(supplier, *, ean="8809532220748", rows=None):
    values = list(rows or [fresh_row(ean)])
    return (
        lambda: list(values),
        lambda _rows, now=None, **_kwargs: (
            [candidate(supplier, ean)],
            {
                f"{supplier}_products": 1,
                f"{supplier}_scenarios": 1,
                "supplier_products_total": 1,
                "supplier_scenarios_total": 1,
            },
        ),
    )


class SupplierPreparationTests(unittest.TestCase):
    def test_budget_sample_is_deterministic_and_exact(self):
        rows = [candidate("umma", valid_ean(index)) for index in range(700)]
        first, metadata = sample_discovery_candidates(rows, ["umma"], 500)
        second, second_metadata = sample_discovery_candidates(list(reversed(rows)), ["umma"], 500)
        self.assertEqual([row["canonical_ean"] for row in first], [row["canonical_ean"] for row in second])
        self.assertEqual(len(first), 500)
        self.assertEqual(metadata, second_metadata)
        self.assertEqual(metadata["sampling_strategy"], DISCOVERY_SAMPLING_STRATEGY)

    def test_budget_preserves_cross_supplier_stratum(self):
        rows = [candidate("umma", valid_ean(index)) for index in range(20)]
        shared = candidate("umma", valid_ean(999999))
        shared["scenarios"].append(scenario("abw", shared["canonical_ean"]))
        sampled, _ = sample_discovery_candidates(rows + [shared], ["umma", "abw"], 5)
        self.assertIn(valid_ean(999999), {row["canonical_ean"] for row in sampled})

    def test_all_catalog_keeps_entire_eligible_union(self):
        rows = [candidate("umma", valid_ean(index)) for index in range(7)]
        sampled, metadata = sample_discovery_candidates(rows, ["umma"], "all")
        self.assertEqual(len(sampled), 7)
        self.assertEqual(metadata["run_budget"], "all")

    def test_selection_is_supported_deduplicated_and_ordered(self):
        self.assertEqual(
            normalize_selected_suppliers(["UMMA", "qogita", "umma", "other"]),
            ["umma", "qogita"],
        )
        self.assertEqual(len(SUPPORTED_SUPPLIERS), 4)

    def test_bsr_ui_defaults_and_validation(self):
        self.assertEqual(default_filters()["bsr_min"], 0)
        self.assertEqual(validate_filters(default_filters())["bsr_max"], 20000)
        invalid = default_filters() | {"bsr_max": 0}
        with self.assertRaisesRegex(ValueError, "BSR"):
            validate_filters(invalid)

    def test_48_hour_boundary_is_fresh_and_older_is_stale(self):
        boundary = (NOW - timedelta(hours=48)).isoformat()
        self.assertTrue(inspect_supplier_rows("umma", [{"observed_at": boundary}], now=NOW)["fresh"])
        older = (NOW - timedelta(hours=48, seconds=1)).isoformat()
        self.assertFalse(inspect_supplier_rows("umma", [{"observed_at": older}], now=NOW)["fresh"])

    def test_default_all_supplier_union_and_shared_ean(self):
        components = {supplier: component(supplier) for supplier in SUPPORTED_SUPPLIERS}
        result = prepare_suppliers(
            SUPPORTED_SUPPLIERS, default_filters(), now=NOW, components=components,
        )
        self.assertEqual(len(result["candidates"]), 1)
        self.assertEqual(len(result["candidates"][0]["scenarios"]), 4)
        self.assertEqual(result["coverage"]["unique_eans"], 1)
        self.assertEqual(result["coverage"]["shared_eans"], 1)
        self.assertEqual(result["usable_suppliers"], list(SUPPORTED_SUPPLIERS))

    def test_supplier_only_eans_are_union_not_intersection(self):
        result = prepare_suppliers(
            ["qogita", "abw"], default_filters(), now=NOW,
            components={
                "qogita": component("qogita", ean="8809532220748"),
                "abw": component("abw", ean="8809562191070"),
            },
        )
        self.assertEqual(len(result["candidates"]), 2)
        self.assertEqual(result["coverage"]["shared_eans"], 0)

    def test_one_supplier_failure_isolated_and_coverage_is_explicit(self):
        def broken():
            raise RuntimeError("offline")
        result = prepare_suppliers(
            ["qogita", "abw"], default_filters(), now=NOW,
            components={"qogita": component("qogita"), "abw": (broken, lambda rows: rows)},
        )
        self.assertEqual(result["usable_suppliers"], ["qogita"])
        self.assertEqual(result["supplier_snapshot_set"]["abw"]["availability_status"], "unavailable")
        self.assertEqual(
            result["supplier_snapshot_set"]["abw"]["coverage_type"],
            "manager_tracked_products",
        )

    def test_production_path_uses_only_active_scout_generation(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = SupplierCatalogStore(Path(temporary) / "catalog.sqlite3")
            products, scenarios = candidates_to_cache_records([candidate("umma")])
            value = SupplierCatalogGeneration(
                supplier="umma", coverage_type="partial_catalog",
                coverage_description="4.185 di 4.212 prodotti enumerati",
                coverage_complete=False, products=products, scenarios=scenarios,
                source_count=4212, enumerated_count=4185, unique_count=4185,
                completeness_status="partial_catalog",
                product_catalog_coverage_type="partial_catalog",
                product_catalog_coverage_complete=False,
                scenario_enrichment_status="partial",
                scenario_enrichment_count=1,
                diagnostics={"search_total_count": 4212, "enumeration_gap": 27},
            )
            run_id = store.start_run(
                "umma", coverage_type="partial_catalog",
                coverage_description=value.coverage_description,
                coverage_complete=False, sampled=False,
            )
            store.publish(run_id, value, elapsed_seconds=1)
            result = prepare_suppliers(
                ["umma", "qogita"], default_filters(), now=datetime.now(timezone.utc),
                store=store,
            )
        self.assertEqual(result["usable_suppliers"], ["umma"])
        self.assertEqual(result["supplier_snapshot_set"]["umma"]["snapshot_id"], run_id)
        self.assertEqual(result["supplier_snapshot_set"]["umma"]["source_count"], 4212)
        self.assertEqual(
            result["supplier_snapshot_set"]["qogita"]["refresh_status"],
            "baseline_missing",
        )

    def test_stale_refresh_success_reloads_new_generation(self):
        stale = [{"gtin": "8809532220748", "run_id": "old", "observed_at": "2026-08-20T10:00:00Z"}]
        fresh = [fresh_row()]
        calls = {"loads": 0, "refresh": 0}
        def loader():
            calls["loads"] += 1
            return stale if calls["loads"] == 1 else fresh
        def refresh():
            calls["refresh"] += 1
            return {"status": "success", "duration_seconds": 1.5}
        result = prepare_suppliers(
            ["umma"], default_filters(), now=NOW,
            components={"umma": (loader, component("umma")[1])},
            refreshers={"umma": refresh},
        )
        self.assertEqual(calls, {"loads": 2, "refresh": 1})
        self.assertEqual(result["supplier_snapshot_set"]["umma"]["refresh_status"], "refreshed")

    def test_all_failed_stops_before_amazon(self):
        prepared = {
            "selected_suppliers": ["umma"], "supplier_snapshot_set": {
                "umma": {"availability_status": "unavailable", "error": "offline"}
            }, "supplier_diagnostics": {}, "supplier_warnings": ["UMMA unavailable"],
            "coverage": {"products_by_supplier": {"umma": 0}, "scenarios_by_supplier": {"umma": 0}},
            "usable_suppliers": [], "candidates": [],
        }
        with tempfile.TemporaryDirectory() as temporary:
            calls = []
            state = run_discovery(
                default_filters(), checkpoint_store=DiscoveryCheckpointStore(temporary),
                catalog_batch=lambda *args: calls.append("catalog"),
                pricing_batch=lambda *args: calls.append("pricing"),
                fees_batch=lambda *args: calls.append("fees"), token_provider=object(),
                selected_suppliers=["umma"],
                supplier_preparer=lambda *args, **kwargs: prepared,
            )
        self.assertEqual(state["status"], "supplier_preparation_failed")
        self.assertEqual(calls, [])

    def test_resume_rejects_changed_supplier_or_filters(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = DiscoveryCheckpointStore(temporary)
            state = store.create(default_filters())
            state["selected_suppliers"] = ["qogita"]
            store.save(state)
            with self.assertRaisesRegex(ValueError, "fornitori differenti"):
                run_discovery(
                    default_filters(), checkpoint_store=store,
                    catalog_batch=lambda *args: {}, pricing_batch=lambda *args: {},
                    fees_batch=lambda *args: [], token_provider=object(),
                    selected_suppliers=["umma"], job_id=state["job_id"],
                )

    def test_resume_rejects_changed_budget(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = DiscoveryCheckpointStore(temporary)
            state = store.create(default_filters())
            state.update({"selected_suppliers": ["umma"], "run_budget": 500})
            store.save(state)
            with self.assertRaisesRegex(ValueError, "budget Discovery differente"):
                run_discovery(
                    default_filters(), checkpoint_store=store,
                    catalog_batch=lambda *args: {}, pricing_batch=lambda *args: {},
                    fees_batch=lambda *args: [], token_provider=object(),
                    selected_suppliers=["umma"], run_budget=250,
                    job_id=state["job_id"],
                )

    def test_resume_after_frozen_snapshot_does_not_prepare_again(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = DiscoveryCheckpointStore(temporary)
            state = store.create(default_filters())
            state.update({
                "phase": "suppliers_loaded", "selected_suppliers": ["umma"],
                "supplier_snapshot_set": {
                    "umma": {"snapshot_id": ["run-1"], "availability_status": "available"}
                },
                "candidates": [candidate("umma")],
                "funnel": {
                    "supplier_products_total": 1, "supplier_scenarios_total": 1,
                },
            })
            store.save(state)
            resumed = run_discovery(
                default_filters(), checkpoint_store=store,
                catalog_batch=lambda identifiers, job_id, products=None: {
                    identifier: {"status": "not_found"} for identifier in identifiers
                },
                pricing_batch=lambda *args: {}, fees_batch=lambda *args: [],
                token_provider=object(), selected_suppliers=["umma"],
                supplier_preparer=lambda *args, **kwargs: self.fail("preparation repeated"),
                job_id=state["job_id"], catalog_batch_interval=0,
                pricing_batch_interval=0, fee_batch_interval=0,
            )
        self.assertEqual(resumed["status"], "completed")
        self.assertEqual(resumed["supplier_snapshot_set"]["umma"]["snapshot_id"], ["run-1"])

    def test_official_refresh_preserves_environment_and_pathsep(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "src").mkdir()
            (root / ".venv" / "bin").mkdir(parents=True)
            (root / ".venv" / "bin" / "python").touch()
            (root / "scripts").mkdir()
            (root / "scripts" / "sync_umma_purchase_prices.py").touch()
            captured = {}
            def runner(*args, **kwargs):
                captured.update(kwargs)
                return type("Completed", (), {"returncode": 0, "stdout": '{"status":"success"}\n'})()
            with patch.dict(os.environ, {"PYTHONPATH": "existing", "KEEP_ME": "yes"}, clear=False), patch(
                "supplier_preparation.subprocess.run", side_effect=runner
            ):
                result = refresh_manager_supplier("umma", manager_root=root)
            self.assertEqual(result["status"], "success")
            self.assertEqual(captured["env"]["KEEP_ME"], "yes")
            self.assertEqual(
                captured["env"]["PYTHONPATH"],
                f"{(root / 'src').resolve()}{os.pathsep}existing"
            )


class DiscoveryUiConfigurationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        root = Path(self.temporary.name)
        self.catalog_path = root / "catalog.sqlite3"
        self.rotation_path = root / "rotation.sqlite3"
        self.environment = patch.dict(
            os.environ,
            {
                "DISCOVERY_ROTATION_DATABASE": str(self.rotation_path),
                "DISCOVERY_INCREMENTAL_DATABASE": str(root / "incremental.sqlite3"),
            },
        )
        self.environment.start()
        original_init = SupplierCatalogStore.__init__

        def isolated_catalog_init(instance, path=None):
            original_init(instance, path or self.catalog_path)

        self.catalog_init = patch.object(
            SupplierCatalogStore, "__init__", isolated_catalog_init,
        )
        self.catalog_init.start()
        store = SupplierCatalogStore()
        for index, supplier in enumerate(("umma", "abw", "qudo"), start=1):
            products, scenarios = candidates_to_cache_records([
                candidate(supplier, valid_ean(index))
            ])
            generation = SupplierCatalogGeneration(
                supplier=supplier,
                coverage_type="full_relevant_catalog",
                coverage_description="isolated UI fixture",
                coverage_complete=True,
                products=products,
                scenarios=scenarios,
                source_count=1,
                enumerated_count=1,
                unique_count=1,
                completeness_status="full_relevant_catalog",
                product_catalog_coverage_type="full_relevant_catalog",
                product_catalog_coverage_complete=True,
                scenario_enrichment_status="full",
                scenario_enrichment_count=1,
            )
            run_id = store.start_run(
                supplier,
                coverage_type=generation.coverage_type,
                coverage_description=generation.coverage_description,
                coverage_complete=True,
                sampled=False,
            )
            store.publish(run_id, generation, elapsed_seconds=0)
        st.cache_data.clear()

    def tearDown(self):
        st.cache_data.clear()
        self.catalog_init.stop()
        self.environment.stop()
        self.temporary.cleanup()

    def discovery_app(self):
        app = AppTest.from_file("app_glowup.py", default_timeout=10).run()
        app.session_state["ui_state"] = "discovery"
        return app.run()

    def test_configuration_defaults_all_suppliers_and_filters(self):
        app = self.discovery_app()
        self.assertEqual(len(app.exception), 0)
        qogita_available = bool(
            SupplierCatalogStore().serving_generation_metadata("qogita")
        )
        self.assertEqual(
            [
                (row.label, row.value) for row in app.checkbox
                if row.label in {"Tutti", "Qogita", "UMMA", "ABW", "Qudo"}
            ],
            [
                ("Tutti", True), ("Qogita", qogita_available),
                ("UMMA", True), ("ABW", True), ("Qudo", True),
            ],
        )
        self.assertEqual(
            next(row for row in app.checkbox if row.label == "Qogita").disabled,
            not qogita_available,
        )
        values = {row.label: row.value for row in app.number_input}
        self.assertEqual(values["BSR minimo"], 0)
        self.assertEqual(values["BSR massimo"], 20000)
        self.assertEqual(values["Margine minimo %"], 15)
        self.assertEqual(
            next(row for row in app.selectbox if row.label == "Prodotti da analizzare in questa ricerca").value,
            "500",
        )
        self.assertTrue(any(row.label == "Nuovo ciclo Discovery" for row in app.button))
        budget_select = next(
            row for row in app.selectbox
            if row.label == "Prodotti da analizzare in questa ricerca"
        )
        self.assertTrue(any(
            str(option).startswith("Tutto il catalogo —")
            for option in budget_select.options
        ))
        self.assertTrue(any(
            str(option).endswith("prodotti") and "rimanenti" not in str(option)
            for option in budget_select.options
        ))
        self.assertTrue(any(row.label == "Dettagli tecnici" for row in app.expander))

    def test_tutti_tracks_individual_selection(self):
        app = self.discovery_app()
        app.checkbox[2].set_value(False).run()
        values = {row.label: row.value for row in app.checkbox}
        self.assertFalse(values["Tutti"])
        self.assertFalse(values["UMMA"])
        app.checkbox[2].set_value(True).run()
        values = {row.label: row.value for row in app.checkbox}
        self.assertTrue(values["Tutti"])

    def test_new_cycle_ui_requires_explicit_second_confirmation(self):
        selected_suppliers = ("umma", "abw", "qudo")
        rotation = DiscoveryRotationStore()
        ean = "8809562191179"
        rotation.sync_universe(
            [{
                "canonical_ean": ean,
                "scenarios": [
                    {"supplier": supplier} for supplier in selected_suppliers
                ],
            }],
            selected_suppliers,
        )
        st.cache_data.clear()
        app = self.discovery_app()
        new_cycle = next(
            row for row in app.button if row.label == "Nuovo ciclo Discovery"
        )
        self.assertFalse(new_cycle.disabled)
        app = new_cycle.click().run()
        self.assertEqual(rotation.status(selected_suppliers)["rotation_cycle_id"], 1)
        confirm = next(
            row for row in app.button if row.label == "Conferma nuovo ciclo"
        )
        app = confirm.click().run()
        self.assertEqual(len(app.exception), 0)
        self.assertEqual(rotation.status(selected_suppliers)["rotation_cycle_id"], 2)

    def test_zero_suppliers_disables_start_before_any_api(self):
        app = self.discovery_app()
        app.checkbox[0].set_value(False).run()
        button = next(row for row in app.button if row.label == "Trova opportunità")
        self.assertTrue(button.disabled)
        self.assertTrue(any("Seleziona almeno un fornitore" in row.value for row in app.markdown))

    def test_running_view_does_not_start_when_not_ready(self):
        with tempfile.TemporaryDirectory() as temporary, patch.dict(
            os.environ,
            {"DISCOVERY_JOB_DATABASE": str(Path(temporary) / "runtime.sqlite3")},
        ):
            app = AppTest.from_file("app_glowup.py", default_timeout=10).run()
            app.session_state["ui_state"] = "discovery_running"
            app.session_state["discovery_status"] = "paused"
            app.run()
        self.assertEqual(len(app.exception), 0)
        self.assertTrue(any(row.value == "Scopri opportunità" for row in app.subheader))

    def test_zero_results_and_supplier_warning_render_without_api(self):
        app = AppTest.from_file("app_glowup.py", default_timeout=10).run()
        app.session_state["ui_state"] = "discovery_result"
        app.session_state["discovery_status"] = "completed"
        app.session_state["discovery_result"] = {
            "output_bytes": b"xlsx",
            "state": {
                "job_id": "job-ui", "discovery_schema_version": "supplier_multi_listing_v1",
                "checkpoint_compatibility": "compatible", "status": "completed",
                "phase": "completed", "filters": default_filters(),
                "selected_suppliers": ["qogita", "abw"],
                "supplier_snapshot_set": {
                    "qogita": {"availability_status": "available", "products_count": 2},
                    "abw": {"availability_status": "unavailable", "products_count": 0},
                },
                "supplier_warnings": ["ABW non disponibile per questa ricerca"],
                "candidates": [], "results": [], "amazon_listings": [],
                "amazon_observations": [], "opportunity_combinations": [],
                "funnel": {"supplier_products_total": 2, "supplier_scenarios_total": 5},
            },
        }
        app.run()
        self.assertEqual(len(app.exception), 0)
        rendered = " ".join(
            [row.value for row in app.markdown]
            + [row.value for row in app.caption]
        )
        self.assertIn("Nessuna opportunità", rendered)
        self.assertIn("ABW non disponibile", rendered)
        self.assertIn("restano disponibili nell'Excel", rendered)
        self.assertTrue(any(row.label == "← Nuova ricerca" for row in app.button))
        self.assertTrue(any(
            row.proto.label == "Scarica Discovery Excel"
            for row in app.get("download_button")
        ))

    def test_new_search_returns_to_configuration_without_running(self):
        app = AppTest.from_file("app_glowup.py", default_timeout=10).run()
        app.session_state["ui_state"] = "discovery_result"
        app.session_state["discovery_status"] = "completed"
        app.session_state["discovery_result"] = {
            "output_bytes": b"xlsx",
            "state": {
                "job_id": "job-ui", "discovery_schema_version": "supplier_multi_listing_v1",
                "checkpoint_compatibility": "compatible", "status": "completed",
                "phase": "completed", "filters": default_filters(),
                "selected_suppliers": ["qogita"], "supplier_snapshot_set": {},
                "candidates": [], "results": [], "amazon_listings": [],
                "amazon_observations": [], "opportunity_combinations": [], "funnel": {},
            },
        }
        app.run()
        next(row for row in app.button if row.label == "← Nuova ricerca").click().run()
        self.assertEqual(len(app.exception), 0)
        self.assertTrue(any(row.label == "Trova opportunità" for row in app.button))


if __name__ == "__main__":
    unittest.main()
