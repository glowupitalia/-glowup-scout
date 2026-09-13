import json
import tempfile
import unittest
from pathlib import Path

from supplier_incremental import SupplierIncrementalStore
from supplier_weekly import IncrementalWeeklyHandler, SupplierRatePolicy, WeeklySupplierStore
from supplier_weekly_adapters import (
    QudoWeeklyFailure,
    _baseline_seed,
    _catalog_generation,
    classify_qudo_weekly_failure,
)


def product(index, *, valid=True, changed=False):
    return {
        "canonical_product_key": f"product-{index}",
        "supplier_product_id": f"parent-{index}",
        "supplier_option_id": f"variation-{index}",
        "identifier_valid": valid,
        "canonical_ean": f"8800000{index:06d}" if valid else None,
        "index_name": f"Product {index}{' changed' if changed else ''}",
    }


def scenario(index):
    return {
        "scenario_id": f"scenario-{index}",
        "canonical_product_key": f"product-{index}",
        "unit_price": "10.00",
        "enriched_at": "2026-08-26T18:39:26Z",
    }


class QudoWeeklyPolicyTests(unittest.TestCase):
    def test_qudo_identity_bridge_uses_product_and_variation(self):
        current = [
            {**product(1), "canonical_product_key": "current-1"},
            {**product(2), "canonical_product_key": "current-2"},
            product(3),
        ]

        class Store:
            @staticmethod
            def latest_success(_supplier):
                return {
                    "completed_at": "2026-08-26T00:00:00Z",
                    "products": [
                        {**product(1), "canonical_product_key": "legacy-1"},
                        {**product(2), "canonical_product_key": "legacy-2"},
                    ],
                    "scenarios": [{
                        **scenario(1), "supplier_catalog_product_key": "legacy-1",
                    }],
                }

        previous, scenarios = _baseline_seed(Store(), "qudo", current)
        self.assertEqual(
            {row["canonical_product_key"] for row in previous},
            {"current-1", "current-2"},
        )
        self.assertEqual(list(scenarios), ["current-1"])
        self.assertEqual(scenarios["current-1"][0]["canonical_product_key"], "current-1")

        with tempfile.TemporaryDirectory() as temporary:
            store = SupplierIncrementalStore(Path(temporary) / "catalog.sqlite3")
            store.compose_generation("baseline-v2", "qudo", previous,
                                     scenarios_by_product=scenarios)
            counts = store.compose_generation("weekly", "qudo", current,
                                              previous_run_id="baseline-v2")
        self.assertEqual(counts["unchanged"], 2)
        self.assertEqual(counts["new"], 1)
        self.assertEqual(counts["removed"], 0)

    def test_carried_forward_product_is_rechecked_next_week(self):
        current = [{**product(1), "canonical_product_key": "current-1"}]

        class Store:
            @staticmethod
            def latest_success(_supplier):
                return {
                    "completed_at": "2026-09-06T00:00:00Z",
                    "products": [{
                        **product(1), "canonical_product_key": "legacy-1",
                        "weekly_resolution": {
                            "status": "carry_forward", "carried_forward": True,
                            "source_generation": "baseline-v1",
                        },
                    }],
                    "scenarios": [],
                }

        previous, _scenarios = _baseline_seed(Store(), "qudo", current)
        self.assertTrue(previous[0]["weekly_recheck_required"])
        with tempfile.TemporaryDirectory() as temporary:
            store = SupplierIncrementalStore(Path(temporary) / "catalog.sqlite3")
            store.compose_generation("baseline-v2", "qudo", previous)
            store.compose_generation(
                "weekly", "qudo", previous, previous_run_id="baseline-v2",
            )
            queue = store.enrichment_queue("weekly")
            self.assertEqual(queue[0]["queue_reason"], "enrichment_failed")

    def test_failure_classifier_is_category_based_and_unknown_fails_closed(self):
        cases = {
            "Qudo variation price is invalid": "PRICE_UNAVAILABLE",
            "Qudo product page expected GTIN is invalid": "IDENTIFIER_INVALID",
            "Qudo identifier unresolved": "IDENTIFIER_INVALID",
            "Qudo JSON-LD GTIN does not match the requested product": "SOURCE_CONFLICT",
        }
        for message, category in cases.items():
            error = QudoWeeklyFailure(
                message, category=category, reason_code="reason",
                evidence={"raw_identifier": "123"},
            )
            result = classify_qudo_weekly_failure(error, product(1))
            self.assertEqual(result["category"], category)
            self.assertEqual(result["evidence"]["raw_identifier"], "123")
            self.assertEqual(result["source_product_identity"]["supplier_option_id"], "variation-1")
        self.assertIsNone(classify_qudo_weekly_failure(RuntimeError("other"), product(1)))

    def test_carry_forward_and_quarantine_are_terminal_and_audited(self):
        with tempfile.TemporaryDirectory() as temporary:
            catalog = SupplierIncrementalStore(Path(temporary) / "catalog.sqlite3")
            weekly = WeeklySupplierStore(Path(temporary) / "weekly.sqlite3")
            prior = [product(1), product(2)]
            catalog.compose_generation(
                "active-v2", "qudo", prior,
                scenarios_by_product={"product-1": [scenario(1)]},
            )
            current = [product(1, changed=True), product(2, changed=True), product(3)]
            published = []

            def enrich(*, product, **_kwargs):
                index = product["supplier_product_id"].split("-")[-1]
                if index == "1":
                    raise QudoWeeklyFailure(
                        "Qudo variation price is invalid", category="PRICE_UNAVAILABLE",
                        reason_code="variation_price_invalid",
                    )
                if index == "2":
                    raise QudoWeeklyFailure(
                        "Qudo JSON-LD GTIN does not match the requested product",
                        category="SOURCE_CONFLICT", reason_code="json_ld_gtin_mismatch",
                    )
                raise QudoWeeklyFailure(
                    "Qudo identifier unresolved", category="IDENTIFIER_INVALID",
                    reason_code="identifier_unresolved",
                )

            handler = IncrementalWeeklyHandler(
                "qudo",
                enumerate_catalog=lambda **_: {
                    "products": current,
                    "previous_reference_run_id": "active-v2",
                },
                enrich_product=enrich,
                publish_generation=lambda **kwargs: published.append(kwargs) or {
                    "run_id": kwargs["run_id"], "promotion_result": "promoted",
                },
                previous_run_id=lambda: "active",
                failure_classifier=classify_qudo_weekly_failure,
                incremental_store=catalog,
            )
            run_id = weekly.start_run(trigger_type="manual")
            result = handler(
                run_id=run_id, source=None, policy=SupplierRatePolicy(), work_store=weekly,
            )
            summary = weekly.queue_summary(run_id, "qudo")
            products, scenarios = catalog.generation_records(f"{run_id}-qudo")
            rows = weekly._rows if hasattr(weekly, "_rows") else None

            self.assertEqual(result["status"], "success")
            self.assertEqual(summary, {"carry_forward": 1, "quarantined": 2})
            self.assertEqual(len(published), 1)
            self.assertEqual([row["canonical_product_key"] for row in products], ["product-1"])
            self.assertTrue(products[0]["weekly_resolution"]["carried_forward"])
            self.assertEqual(len(scenarios), 1)
            self.assertTrue(scenarios[0]["weekly_resolution"]["carried_forward"])
            self.assertIsNone(rows)

            import sqlite3
            connection = sqlite3.connect(weekly.path)
            audits = [json.loads(row[0]) for row in connection.execute(
                "SELECT failure_json FROM supplier_sync_work_items ORDER BY canonical_product_key"
            )]
            connection.close()
            self.assertEqual([row["resolution"] for row in audits], [
                "carry_forward", "quarantined", "quarantined",
            ])

    def test_unknown_failure_preserves_previous_baseline(self):
        with tempfile.TemporaryDirectory() as temporary:
            catalog = SupplierIncrementalStore(Path(temporary) / "catalog.sqlite3")
            weekly = WeeklySupplierStore(Path(temporary) / "weekly.sqlite3")
            published = []
            handler = IncrementalWeeklyHandler(
                "qudo",
                enumerate_catalog=lambda **_: {"products": [product(1)]},
                enrich_product=lambda **_: (_ for _ in ()).throw(RuntimeError("unknown")),
                publish_generation=lambda **kwargs: published.append(kwargs),
                previous_run_id=lambda: "active",
                failure_classifier=classify_qudo_weekly_failure,
                incremental_store=catalog,
            )
            run_id = weekly.start_run(trigger_type="manual")
            result = handler(
                run_id=run_id, source=None, policy=SupplierRatePolicy(), work_store=weekly,
            )
            self.assertEqual(result["status"], "failed")
            self.assertEqual(result["error_code"], "incremental_queue_incomplete")
            self.assertEqual(weekly.queue_summary(run_id, "qudo"), {"permanent_failure": 1})
            self.assertEqual(published, [])

    def test_classified_failures_do_not_trip_global_consecutive_error_guard(self):
        with tempfile.TemporaryDirectory() as temporary:
            catalog = SupplierIncrementalStore(Path(temporary) / "catalog.sqlite3")
            weekly = WeeklySupplierStore(Path(temporary) / "weekly.sqlite3")
            published = []
            products = [product(index) for index in range(12)]
            handler = IncrementalWeeklyHandler(
                "qudo",
                enumerate_catalog=lambda **_: {"products": products},
                enrich_product=lambda **_: (_ for _ in ()).throw(QudoWeeklyFailure(
                    "Qudo variation price is invalid", category="PRICE_UNAVAILABLE",
                    reason_code="variation_price_invalid",
                )),
                publish_generation=lambda **kwargs: published.append(kwargs) or {
                    "run_id": kwargs["run_id"], "promotion_result": "promoted",
                },
                previous_run_id=lambda: None,
                failure_classifier=classify_qudo_weekly_failure,
                incremental_store=catalog,
                batch_size=12,
            )
            run_id = weekly.start_run(trigger_type="manual")
            result = handler(
                run_id=run_id, source=None, policy=SupplierRatePolicy(), work_store=weekly,
            )
            self.assertEqual(result["status"], "success")
            self.assertEqual(weekly.queue_summary(run_id, "qudo"), {"quarantined": 12})
            self.assertEqual(len(published), 1)

    def test_0609_scale_contract(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = SupplierIncrementalStore(Path(temporary) / "catalog.sqlite3")
            baseline = [product(index) for index in range(6880)]
            scenario_indexes = [*range(3), *range(47, 51), *range(53, 3948)]
            scenarios = {f"product-{index}": [scenario(index)] for index in scenario_indexes}
            self.assertEqual(len(scenarios), 3902)
            store.compose_generation(
                "baseline-v2", "qudo", baseline, scenarios_by_product=scenarios,
            )
            current = baseline + [product(index) for index in range(6880, 6927)]
            counts = store.compose_generation(
                "weekly", "qudo", current, previous_run_id="baseline-v2",
            )
            self.assertEqual((counts["unchanged"], counts["new"], counts["removed"]),
                             (6880, 47, 0))

            # Current successful enrichment removed 41 stale scenarios.
            for index in range(53, 94):
                store.persist_enrichment("weekly", "qudo", f"product-{index}", [])
            for index in range(47):
                self.assertEqual(store.resolve_product_failure(
                    "weekly", "qudo", f"product-{index}",
                    previous_run_id="baseline-v2", category="PRICE_UNAVAILABLE",
                ), "carry_forward")
            for index in range(47, 53):
                self.assertEqual(store.resolve_product_failure(
                    "weekly", "qudo", f"product-{index}",
                    previous_run_id="baseline-v2", category="SOURCE_CONFLICT",
                ), "quarantined")
            for index in range(6880, 6918):
                self.assertEqual(store.resolve_product_failure(
                    "weekly", "qudo", f"product-{index}",
                    previous_run_id="baseline-v2", category="IDENTIFIER_INVALID",
                ), "quarantined")
            final_products, final_scenarios = store.generation_records("weekly")
            summary = store.generation_summary("weekly")
            self.assertEqual(len(final_products), 6883)
            self.assertEqual(len(final_scenarios), 3857)
            self.assertEqual(summary["product_states"]["carry_forward"], 47)
            self.assertEqual(summary["product_states"]["quarantined"], 44)

            class PublicationStore:
                promoted = None

                @staticmethod
                def active_generation_metadata(_supplier):
                    return {"run_id": "active-qudo"}

                @staticmethod
                def start_run(*_args, **_kwargs):
                    return None

                @classmethod
                def publish(cls, _run_id, _generation, **kwargs):
                    cls.promoted = kwargs["promote"]

            result = _catalog_generation(
                "qudo", "weekly", {"diagnostics": {
                    "global_catalog_total": 7113,
                    "qudo_offer_products": 6927,
                    "canonical_gtin_products": 6883,
                    "normalizer": {"qudo_scenarios": 3857},
                }}, store, previous_run_id="active-qudo",
                catalog_store=PublicationStore(),
            )
            self.assertEqual(result["promotion_result"], "promoted")
            self.assertTrue(PublicationStore.promoted)

    def test_promotion_accounts_for_intentional_quarantine(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = SupplierIncrementalStore(Path(temporary) / "catalog.sqlite3")
            baseline = [product(index, changed=False) for index in range(26)]
            current = [product(index, changed=True) for index in range(162)]
            store.compose_generation("baseline-v2", "qudo", baseline)
            store.compose_generation(
                "weekly", "qudo", current, previous_run_id="baseline-v2",
            )
            for index in range(26):
                self.assertEqual(store.resolve_product_failure(
                    "weekly", "qudo", f"product-{index}",
                    previous_run_id="baseline-v2", category="PRICE_UNAVAILABLE",
                ), "carry_forward")
            for index in range(126, 162):
                self.assertEqual(store.resolve_product_failure(
                    "weekly", "qudo", f"product-{index}",
                    previous_run_id="baseline-v2", category="PRICE_UNAVAILABLE",
                ), "quarantined")
            for index in range(26, 126):
                store.persist_enrichment(
                    "weekly", "qudo", f"product-{index}", [scenario(index)],
                )

            class PublicationStore:
                promoted = None

                @staticmethod
                def active_generation_metadata(_supplier):
                    return {"run_id": "active-qudo"}

                @staticmethod
                def start_run(*_args, **_kwargs):
                    return None

                @classmethod
                def publish(cls, _run_id, _generation, **kwargs):
                    cls.promoted = kwargs["promote"]

            diagnostics = {
                "global_catalog_total": 180,
                "qudo_offer_products": 162,
                "canonical_gtin_products": 150,
                "normalizer": {"qudo_scenarios": 100},
            }
            result = _catalog_generation(
                "qudo", "weekly", {"diagnostics": diagnostics}, store,
                previous_run_id="active-qudo", catalog_store=PublicationStore(),
            )
            products, _ = store.generation_records("weekly")
            summary = store.generation_summary("weekly")["product_states"]

            self.assertEqual(len(products), 126)
            self.assertEqual(summary.get("carry_forward"), 26)
            self.assertEqual(summary.get("quarantined"), 36)
            self.assertIsNone(summary.get("removed"))
            self.assertEqual(result["promotion_result"], "promoted")
            self.assertTrue(PublicationStore.promoted)

            with self.assertRaisesRegex(
                RuntimeError, "qudo_persisted_product_identity_mismatch",
            ):
                _catalog_generation(
                    "qudo", "weekly", {"diagnostics": {
                        **diagnostics, "qudo_offer_products": 163,
                    }}, store, previous_run_id="active-qudo",
                    catalog_store=PublicationStore(),
                )


if __name__ == "__main__":
    unittest.main()
