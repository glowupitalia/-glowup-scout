import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from supplier_weekly import (
    IncrementalWeeklyHandler, QOGITA_KOREAN_BEAUTY_STEP, SupplierRatePolicy,
    WEEKLY_STEPS, WEEKLY_SUPPLIERS, WeeklySupplierOrchestrator,
    WeeklySupplierStore, next_weekly_refresh, schedule_key,
)
from supplier_incremental import SupplierIncrementalStore
from supplier_weekly_adapters import (
    QudoIncrementalAdapter, UmmaIncrementalAdapter, build_weekly_handlers,
    validate_umma_gap,
)


class WeeklyScheduleTests(unittest.TestCase):
    def test_calendar_schedule_is_sunday_two_local(self):
        value = next_weekly_refresh(datetime(2026, 1, 5, 12, tzinfo=timezone.utc))
        self.assertEqual(value.weekday(), 6)
        self.assertEqual(value.hour, 2)
        self.assertEqual(value.tzname(), "CET")

    def test_spring_dst_uses_first_valid_hour(self):
        value = next_weekly_refresh(datetime(2026, 3, 28, 12, tzinfo=timezone.utc))
        self.assertEqual(value.strftime("%Y-%m-%d %H:%M %Z"), "2026-03-29 03:00 CEST")

    def test_schedule_key_is_calendar_based(self):
        first = datetime(2026, 10, 25, 0, 30, tzinfo=timezone.utc)
        second = first + timedelta(hours=1)
        self.assertEqual(schedule_key(first), schedule_key(second))

    def test_autumn_duplicate_invocations_share_one_calendar_key(self):
        first = datetime(2026, 10, 25, 0, 30, tzinfo=timezone.utc)
        second = datetime(2026, 10, 25, 2, 30, tzinfo=timezone.utc)
        self.assertEqual(schedule_key(first), schedule_key(second))


class WeeklyStoreTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.store = WeeklySupplierStore(Path(self.temporary.name) / "weekly.sqlite3")

    def tearDown(self):
        self.temporary.cleanup()

    def test_sequential_failure_isolation_and_abw_waiting(self):
        calls = []
        def umma(**kwargs):
            calls.append("umma")
            raise RuntimeError("upstream")
        def qudo(**kwargs):
            calls.append("qudo")
            return {"status": "success", "new": 2, "baseline_after": "q2"}
        def korean_beauty(**kwargs):
            calls.append(QOGITA_KOREAN_BEAUTY_STEP)
            return {"status": "success", "baseline_after": "kb2"}
        result = WeeklySupplierOrchestrator(
            {"umma": umma, "qudo": qudo,
             QOGITA_KOREAN_BEAUTY_STEP: korean_beauty}, store=self.store,
            baseline_provider=lambda supplier: {
                "abw": "a1", "umma": "u1", "qudo": "q1",
                QOGITA_KOREAN_BEAUTY_STEP: "kb1",
            }[supplier],
        ).run()
        states = {row["supplier"]: row for row in result["suppliers"]}
        self.assertEqual(calls, ["umma", "qudo", QOGITA_KOREAN_BEAUTY_STEP])
        self.assertEqual(states["abw"]["status"], "waiting_for_source")
        self.assertEqual(states["abw"]["baseline_after"], "a1")
        self.assertEqual(states["umma"]["promotion_result"], "baseline_preserved")
        self.assertEqual(states["qudo"]["baseline_after"], "q2")
        self.assertEqual(states[QOGITA_KOREAN_BEAUTY_STEP]["baseline_after"], "kb2")
        self.assertEqual(result["status"], "partial_success")

    def test_rate_policy_is_persisted(self):
        policy = SupplierRatePolicy(reconciliation_days=60, reconciliation_budget=12)
        handlers = {
            supplier: (lambda **kwargs: {"status": "success"})
            for supplier in WEEKLY_STEPS
        }
        result = WeeklySupplierOrchestrator(
            handlers, store=self.store, policies={"qudo": policy},
        ).run(sources={"abw": "catalog.xlsx"})
        qudo = next(row for row in result["suppliers"] if row["supplier"] == "qudo")
        self.assertIn('"reconciliation_days": 60', qudo["rate_policy_json"])
        self.assertIn('"reconciliation_budget": 12', qudo["rate_policy_json"])

    def test_scheduled_run_persists_local_date_audit(self):
        scheduled = datetime(2026, 3, 29, 1, 0, tzinfo=timezone.utc)
        run_id = self.store.start_run(
            trigger_type="scheduled", scheduled_at=scheduled.isoformat(),
        )
        import json, sqlite3
        with sqlite3.connect(self.store.path) as connection:
            raw = connection.execute(
                "SELECT diagnostics_json FROM supplier_weekly_runs WHERE run_id=?", (run_id,),
            ).fetchone()[0]
        value = json.loads(raw)
        self.assertEqual(value["scheduled_local_date"], "2026-03-29")
        self.assertEqual(value["schedule_key"], "2026-03-29-weekly-supplier-sync")

    def test_second_sunday_trigger_skips_completed_calendar_run(self):
        first = datetime(2026, 9, 6, 0, 0, tzinfo=timezone.utc)
        run_id = self.store.start_run(
            trigger_type="scheduled", scheduled_at=first.isoformat(),
        )
        with sqlite3.connect(self.store.path) as connection:
            connection.execute(
                "UPDATE supplier_weekly_runs SET status='partial_success' WHERE run_id=?",
                (run_id,),
            )
        self.assertTrue(self.store.has_completed_schedule(first + timedelta(hours=1)))

    def test_qudo_queue_checkpoint_resume_and_zero_double_claim(self):
        run_id = self.store.start_run(trigger_type="manual")
        self.store.enqueue(run_id, "qudo", [
            {"canonical_product_key": "p1", "product_state": "new"},
            {"canonical_product_key": "p2", "product_state": "changed"},
        ])
        first = self.store.claim(run_id, "qudo", "worker-a", batch_size=1)
        second = self.store.claim(run_id, "qudo", "worker-b", batch_size=2)
        self.assertEqual([row["canonical_product_key"] for row in first], ["p1"])
        self.assertEqual([row["canonical_product_key"] for row in second], ["p2"])
        self.assertTrue(self.store.complete_item(run_id, "qudo", "p1", "worker-a"))
        self.assertEqual(self.store.queue_summary(run_id, "qudo"), {"claimed": 1, "complete": 1})

    def test_expired_lease_is_resumable_after_restart(self):
        run_id = self.store.start_run(trigger_type="manual")
        self.store.enqueue(run_id, "qudo", [{"canonical_product_key": "p1", "product_state": "new"}])
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.store.claim(run_id, "qudo", "dead", lease_seconds=1, now=now)
        reopened = WeeklySupplierStore(self.store.path)
        claimed = reopened.claim(
            run_id, "qudo", "resume", batch_size=1,
            now=now + timedelta(seconds=2),
        )
        self.assertEqual(claimed[0]["canonical_product_key"], "p1")
        self.assertEqual(claimed[0]["attempts"], 2)

    def test_retryable_and_permanent_failures_are_distinct(self):
        run_id = self.store.start_run(trigger_type="manual")
        self.store.enqueue(run_id, "qudo", [
            {"canonical_product_key": "p1", "product_state": "new"},
            {"canonical_product_key": "p2", "product_state": "new"},
        ])
        self.store.claim(run_id, "qudo", "worker", batch_size=2)
        self.store.fail_item(run_id, "qudo", "p1", "worker", retryable=True, error_class="timeout")
        self.store.fail_item(run_id, "qudo", "p2", "worker", retryable=False, error_class="invalid")
        self.assertEqual(self.store.queue_summary(run_id, "qudo"), {"pending": 1, "permanent_failure": 1})

    def test_incremental_handler_enriches_new_and_is_idempotent(self):
        incremental = SupplierIncrementalStore(Path(self.temporary.name) / "incremental.sqlite3")
        published = []
        def enumerate_catalog(**kwargs):
            return {"products": [{
                "canonical_product_key": "qudo-product-1", "product_id": "1",
                "variation_id": "2", "identifier_valid": True,
            }], "requests": 1}
        def enrich_product(**kwargs):
            return {"scenarios": [{
                "scenario_id": "qudo-offer-2", "supplier": "qudo",
                "canonical_product_key": kwargs["canonical_product_key"],
            }], "requests": 2}
        handler = IncrementalWeeklyHandler(
            "qudo", enumerate_catalog=enumerate_catalog,
            enrich_product=enrich_product,
            publish_generation=lambda **kwargs: published.append(kwargs) or {
                "run_id": kwargs["run_id"], "promotion_result": "promoted"
            },
            previous_run_id=lambda: None, incremental_store=incremental,
        )
        result = handler(
            run_id="weekly-1", source=None, policy=SupplierRatePolicy(), work_store=self.store,
        )
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["enriched"], 1)
        self.assertEqual(incremental.generation_summary("weekly-1-qudo")["scenario_refs"], 1)
        self.assertEqual(len(published), 1)

    def test_unchanged_catalog_does_not_trigger_full_detail(self):
        for supplier, size in (("umma", 120), ("qudo", 120)):
            with self.subTest(supplier=supplier):
                incremental = SupplierIncrementalStore(
                    Path(self.temporary.name) / f"{supplier}-incremental.sqlite3"
                )
                products = [{"canonical_product_key": f"{supplier}-{n}",
                             "product_id": str(n), "variation_id": str(n),
                             "identifier_valid": True} for n in range(size)]
                scenarios = {row["canonical_product_key"]: [{
                    "scenario_id": f"scenario-{row['canonical_product_key']}",
                    "canonical_product_key": row["canonical_product_key"],
                    "enriched_at": "2026-08-26T00:00:00Z",
                }] for row in products}
                incremental.compose_generation(
                    f"active-{supplier}", supplier, products,
                    scenarios_by_product=scenarios,
                )
                calls = []
                handler = IncrementalWeeklyHandler(
                    supplier,
                    enumerate_catalog=lambda **_: {"products": products},
                    enrich_product=lambda **kwargs: calls.append(kwargs) or [],
                    publish_generation=lambda **kwargs: {
                        "run_id": kwargs["run_id"], "promotion_result": "promoted"
                    },
                    previous_run_id=lambda: f"active-{supplier}",
                    incremental_store=incremental,
                )
                result = handler(
                    run_id=f"weekly-{supplier}", source=None,
                    policy=SupplierRatePolicy(reconciliation_days=60, reconciliation_budget=3),
                    work_store=self.store,
                )
                self.assertEqual(result["status"], "success")
                self.assertEqual(calls, [])

    def test_production_routes_use_incremental_adapters_not_legacy_full_collectors(self):
        handlers = build_weekly_handlers()
        self.assertEqual(set(handlers), set(WEEKLY_STEPS))
        self.assertEqual(WEEKLY_SUPPLIERS, ("abw", "umma", "qudo"))
        self.assertNotIn("qogita", WEEKLY_STEPS)
        self.assertIsInstance(handlers["umma"].adapter, UmmaIncrementalAdapter)
        self.assertIsInstance(handlers["qudo"].adapter, QudoIncrementalAdapter)
        self.assertIsInstance(handlers["qudo"].incremental_handler, IncrementalWeeklyHandler)

    def test_korean_beauty_handler_only_runs_membership_refresh(self):
        expected = {
            "membership_activation": True,
            "previous_membership_version_id": "membership-1",
            "active_membership": {"membership_version_id": "membership-2"},
            "membership_diff": {
                "gtin_added_count": 2, "fid_changed_count": 1,
                "gtin_unchanged_count": 8, "gtin_removed_count": 3,
            },
            "curated": {
                "pages_requested": 4, "http_retry_count": 1,
                "http_status_counts": {"200": 4},
            },
            "validation_errors": [],
        }
        with patch(
            "supplier_weekly_adapters.refresh_korean_beauty_membership",
            return_value=expected,
        ) as refresh:
            handler = build_weekly_handlers()[QOGITA_KOREAN_BEAUTY_STEP]
            result = handler(
                run_id="weekly", source=None, policy=SupplierRatePolicy(),
                work_store=self.store,
            )
        refresh.assert_called_once()
        kwargs = refresh.call_args.kwargs
        self.assertTrue(kwargs["persist"])
        self.assertTrue(kwargs["activate"])
        self.assertEqual(result["promotion_result"], "membership_activated")
        self.assertEqual(result["baseline_after"], "membership-2")
        self.assertEqual(result["new"], 2)

    def test_korean_beauty_runs_after_supplier_failures_without_changing_policy(self):
        calls = []
        handlers = {
            "abw": lambda **_: {"status": "success"},
            "umma": lambda **_: (_ for _ in ()).throw(RuntimeError("umma failed")),
            "qudo": lambda **_: (_ for _ in ()).throw(RuntimeError("qudo failed")),
            QOGITA_KOREAN_BEAUTY_STEP: lambda **_: (
                calls.append(QOGITA_KOREAN_BEAUTY_STEP) or {
                    "status": "success", "baseline_after": "membership-2",
                }
            ),
        }
        result = WeeklySupplierOrchestrator(
            handlers, store=self.store, baseline_provider=lambda _: None,
        ).run(sources={"abw": "catalog.xlsx"})
        self.assertEqual(calls, [QOGITA_KOREAN_BEAUTY_STEP])
        states = {row["supplier"]: row for row in result["suppliers"]}
        self.assertEqual(states[QOGITA_KOREAN_BEAUTY_STEP]["status"], "success")
        self.assertEqual(states["umma"]["status"], "failed")
        self.assertEqual(states["qudo"]["status"], "failed")

    def test_umma_gap_guard_tolerates_known_gap_and_quarantines_jump(self):
        validate_umma_gap(26, 26)
        with self.assertRaisesRegex(RuntimeError, "gap increased"):
            validate_umma_gap(100, 26)


if __name__ == "__main__":
    unittest.main()
