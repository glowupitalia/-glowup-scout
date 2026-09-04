import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from discovery_jobs import DiscoveryJobRegistry
from storage_gc import (
    DB1E11_JOB_ID,
    GIB,
    archive_volume_status,
    classify_storage_watermark,
    collect_storage_metrics,
    append_storage_audit_event,
    evaluate_discovery_admission,
    evaluate_qogita_window_admission,
    plan_discovery_retention,
    storage_admission_decision,
)
from supplier_weekly import SupplierRatePolicy, WeeklySupplierOrchestrator, WeeklySupplierStore


def metrics(free_gib, *, total_gib=200, freelist_gib=0):
    watermark = classify_storage_watermark(
        filesystem_total_bytes=total_gib * GIB,
        filesystem_free_bytes=free_gib * GIB,
    )
    return {
        "reliable": True,
        "filesystem_total_bytes": total_gib * GIB,
        "filesystem_free_bytes": free_gib * GIB,
        "filesystem_free_percent": free_gib * 100 / total_gib,
        "discovery_sqlite_freelist_bytes": freelist_gib * GIB,
        "watermark": watermark,
    }


def eligible_job(number, *, scope="scope-a", **overrides):
    row = {
        "job_id": f"job-{number:02d}", "status": "completed",
        "created_at": f"2026-08-{number:02d}T00:00:00Z",
        "completed_at": f"2026-08-{number:02d}T01:00:00Z",
        "scope": scope, "retention_mode": "full", "resumable": False,
        "terminal_summary_valid": True, "cache_verification_state": "verified",
        "outbox_statuses": ["sent"], "rotation_committed": True,
        "operational_export_preserved": True, "references_known": True,
        "estimated_sqlite_reclaim_bytes": 1000,
        "estimated_filesystem_reclaim_bytes": 100,
    }
    row.update(overrides)
    return row


class WatermarkTests(unittest.TestCase):
    def test_absolute_boundaries_are_inclusive(self):
        total = 100 * GIB
        expected = ((50, "NORMAL"), (40, "PREVENTIVE"), (25, "PRESSURE"),
                    (15, "CRITICAL"), (14, "EMERGENCY"))
        for free, state in expected:
            with self.subTest(free=free):
                self.assertEqual(classify_storage_watermark(
                    filesystem_total_bytes=total,
                    filesystem_free_bytes=free * GIB,
                )["state"], state)

    def test_percentage_boundaries_and_prudent_dimension(self):
        total = 400 * GIB
        for ratio, state in ((20, "NORMAL"), (16, "PREVENTIVE"),
                             (10, "PRESSURE"), (6, "CRITICAL"), (5, "EMERGENCY")):
            with self.subTest(ratio=ratio):
                result = classify_storage_watermark(
                    filesystem_total_bytes=total,
                    filesystem_free_bytes=int(total * ratio / 100),
                )
                self.assertEqual(result["state"], state)
        prudent = classify_storage_watermark(
            filesystem_total_bytes=400 * GIB,
            filesystem_free_bytes=50 * GIB,
        )
        self.assertEqual(prudent["absolute_state"], "NORMAL")
        self.assertEqual(prudent["percentage_state"], "PRESSURE")
        self.assertEqual(prudent["state"], "PRESSURE")

    def test_unreadable_metrics_are_unknown(self):
        result = classify_storage_watermark(
            filesystem_total_bytes=None, filesystem_free_bytes=None,
        )
        self.assertEqual(result["state"], "UNKNOWN")
        self.assertFalse(result["reliable"])

    def test_future_archive_volume_has_no_path_fallback_and_fails_closed(self):
        unavailable = archive_volume_status("volume-uuid")
        self.assertFalse(unavailable["archive_available"])
        self.assertIn("fail_closed", unavailable["archive_reason"])
        verified = archive_volume_status("volume-uuid", verifier=lambda value: {
            "volume_uuid": value, "mounted": True, "verified": True,
        })
        self.assertTrue(verified["archive_available"])


class AdmissionTests(unittest.TestCase):
    def test_full_boundaries_and_freelist_scope(self):
        self.assertTrue(evaluate_discovery_admission(
            ["abw", "umma", "qudo"], metrics=metrics(50),
        )["allowed"])
        self.assertFalse(evaluate_discovery_admission(
            ["abw", "umma", "qudo"], metrics=metrics(44),
        )["allowed"])
        self.assertFalse(evaluate_discovery_admission(
            ["abw", "umma", "qudo"], metrics=metrics(40),
        )["allowed"])
        credited = evaluate_discovery_admission(
            ["abw", "umma", "qudo"], metrics=metrics(44, freelist_gib=1),
        )
        self.assertEqual(credited["discovery_freelist_credit_bytes"], GIB)
        qogita = evaluate_qogita_window_admission(metrics=metrics(44, freelist_gib=5))
        self.assertEqual(qogita["discovery_freelist_credit_bytes"], 0)

    def test_small_and_qogita_window_admission(self):
        self.assertTrue(evaluate_discovery_admission(
            ["qogita"], "korean_beauty", metrics=metrics(30),
        )["allowed"])
        self.assertFalse(evaluate_discovery_admission(
            ["qogita"], "korean_beauty", metrics=metrics(25),
        )["allowed"])
        self.assertTrue(evaluate_qogita_window_admission(metrics=metrics(45))["allowed"])
        self.assertTrue(evaluate_qogita_window_admission(metrics=metrics(30))["allowed"])
        self.assertFalse(evaluate_qogita_window_admission(metrics=metrics(20))["allowed"])

    def test_metric_failure_is_fail_closed(self):
        decision = storage_admission_decision(
            {"reliable": False, "watermark": {"state": "UNKNOWN"}},
            "discovery_full",
        )
        self.assertFalse(decision["allowed"])
        self.assertEqual(decision["status"], "admission_blocked_storage")

    def test_worker_side_recheck_can_block_after_ui_precheck(self):
        self.assertTrue(evaluate_discovery_admission(
            ["abw", "umma", "qudo"], metrics=metrics(50),
        )["allowed"])
        self.assertFalse(evaluate_discovery_admission(
            ["abw", "umma", "qudo"], metrics=metrics(40),
        )["allowed"])


class RetentionPlannerTests(unittest.TestCase):
    def test_preventive_policy_keeps_ten_global_and_scope_representative(self):
        jobs = [eligible_job(number) for number in range(1, 13)]
        jobs.append(eligible_job(13, scope="scope-b"))
        plan = plan_discovery_retention(jobs, "PREVENTIVE")
        rows = {row["job_id"]: row for row in plan["jobs"]}
        self.assertEqual(plan["policy"], "E_KEEP_GLOBAL_10_SCOPE_1")
        self.assertEqual(rows["job-13"]["classification"], "KEEP_SCOPE_REPRESENTATIVE")
        self.assertEqual(rows["job-12"]["classification"], "KEEP_SCOPE_REPRESENTATIVE")
        self.assertEqual(rows["job-01"]["classification"], "FINAL_ONLY_ELIGIBLE")
        self.assertEqual(plan["summary"]["delete_operations"], 0)
        self.assertEqual(plan["summary"]["final_only_changes"], 0)

    def test_pressure_policy_and_all_fail_safe_states(self):
        jobs = [eligible_job(number) for number in range(1, 9)]
        jobs[0]["references_known"] = False
        jobs[1]["cache_verification_state"] = "unverified"
        jobs.append(eligible_job(20, job_id=DB1E11_JOB_ID))
        plan = plan_discovery_retention(jobs, "PRESSURE")
        rows = {row["job_id"]: row for row in plan["jobs"]}
        self.assertEqual(plan["policy"], "D_KEEP_GLOBAL_5_SCOPE_1")
        self.assertEqual(rows["job-01"]["classification"], "UNKNOWN_KEEP")
        self.assertEqual(rows["job-02"]["classification"], "KEEP_BLOCKED")
        self.assertEqual(rows[DB1E11_JOB_ID]["classification"], "KEEP_BLOCKED")
        self.assertFalse(plan["execution_supported"])

    def test_normal_only_reports_and_never_proposes_action(self):
        plan = plan_discovery_retention(
            [eligible_job(number) for number in range(1, 15)], "NORMAL",
        )
        self.assertEqual(plan["summary"]["candidate_count"], 0)
        self.assertTrue(all(
            row["classification"] != "FINAL_ONLY_ELIGIBLE" for row in plan["jobs"]
        ))


class IntegrationTests(unittest.TestCase):
    def test_metrics_are_bounded_and_separate_freelist(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = root / "data"
            data.mkdir()
            for name in ("discovery_incremental.sqlite3", "supplier_catalog.sqlite3",
                         "discovery_rotation.sqlite3"):
                with sqlite3.connect(data / name) as connection:
                    connection.execute("CREATE TABLE sample(value TEXT)")
            usage = SimpleNamespace(total=100 * GIB, used=40 * GIB, free=60 * GIB)
            result = collect_storage_metrics(root, disk_usage=lambda _: usage)
            self.assertTrue(result["reliable"])
            self.assertEqual(result["watermark"]["state"], "NORMAL")
            self.assertEqual(
                result["discovery_sqlite_freelist_bytes"],
                result["databases"]["discovery"]["freelist_bytes"],
            )
            self.assertIsNone(result["archive_volume_uuid"])
            self.assertFalse(result["archive_available"])

    def test_workload_audit_is_capped_and_exposes_observed_growth_inputs(self):
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "audit.jsonl"
            before = metrics(50)
            before["databases"] = {
                "discovery": {"file_size_bytes": 100, "freelist_bytes": 20, "wal_bytes": 5}
            }
            after = metrics(49)
            after["databases"] = {
                "discovery": {"file_size_bytes": 180, "freelist_bytes": 10, "wal_bytes": 25}
            }
            for number in range(3):
                self.assertTrue(append_storage_audit_event({
                    "event": "workload_completed", "workload_type": "discovery_full",
                    "workload_id": str(number), "universe_size": 1000,
                    "elapsed_seconds": 12.5, "success": True,
                    "storage_before": before, "storage_after": after,
                }, destination, max_records=2))
            rows = [json.loads(line) for line in destination.read_text().splitlines()]
            self.assertEqual([row["workload_id"] for row in rows], ["1", "2"])
            observed = rows[-1]["workload_metrics"]
            self.assertEqual(observed["relevant_database"], "discovery")
            self.assertEqual(observed["relevant_db_pre_bytes"], 100)
            self.assertEqual(observed["relevant_db_post_bytes"], 180)
            self.assertEqual(observed["wal_peak_observed_bytes"], 25)

    def test_registry_records_distinct_storage_block_without_claim(self):
        with tempfile.TemporaryDirectory() as directory:
            registry = DiscoveryJobRegistry(path=Path(directory) / "runtime.sqlite3")
            registry.register_checkpoint({
                "job_id": "job", "phase": "initialized",
                "selected_suppliers": ["qogita"], "filters": {},
            })
            registry.admission_blocked("job", decision={
                "watermark": {"state": "PRESSURE"}, "reason": "headroom",
            })
            row = registry.get("job")
            self.assertEqual(row["status"], "admission_blocked_storage")
            self.assertEqual(row["phase"], "admission_blocked_storage")
            self.assertTrue(row["resumable"])
            self.assertIsNone(row["worker_pid"])
            self.assertEqual(json.loads(row["error"])["reason"], "headroom")

    @patch("discovery_worker.append_storage_audit_event")
    @patch("discovery_worker.production_retention_plan")
    @patch("discovery_worker.evaluate_discovery_admission")
    @patch("discovery_worker.collect_storage_metrics")
    @patch("discovery_worker.load_env")
    def test_worker_authoritatively_blocks_before_claim(
        self, load_env, collect, evaluate, retention, audit,
    ):
        from discovery_worker import execute

        registry = MagicMock()
        registry.get.return_value = {"selected_suppliers": ["abw", "umma", "qudo"]}
        checkpoint = MagicMock()
        checkpoint.state_path.return_value = Path("/definitely/missing/state.json")
        checkpoint.path.return_value = Path("/definitely/missing/legacy.json")
        collect.return_value = metrics(40)
        evaluate.return_value = {
            "allowed": False, "status": "admission_blocked_storage",
            "reason": "post_run_floor_not_met",
            "watermark": {"state": "PREVENTIVE"},
        }
        retention.return_value = {"summary": {"candidate_count": 2}}

        result = execute("job", registry=registry, checkpoint_store=checkpoint)

        self.assertEqual(result["status"], "admission_blocked_storage")
        registry.admission_blocked.assert_called_once()
        registry.claim.assert_not_called()
        audit.assert_called_once()

    def test_weekly_block_is_not_supplier_failure_and_next_step_is_evaluated(self):
        with tempfile.TemporaryDirectory() as directory:
            store = WeeklySupplierStore(Path(directory) / "weekly.sqlite3")
            called = []

            def handler(**kwargs):
                called.append(kwargs)
                return {"status": "success"}

            decisions = {"abw": False, "umma": True, "qudo": True,
                         "qogita_korean_beauty": True}
            orchestrator = WeeklySupplierOrchestrator(
                {name: handler for name in decisions}, store=store,
                policies={name: SupplierRatePolicy() for name in decisions},
                baseline_provider=lambda supplier: f"baseline-{supplier}",
                storage_metrics=lambda: metrics(50),
                admission_check=lambda supplier, observed: {
                    "allowed": decisions[supplier],
                    "status": "admitted" if decisions[supplier] else "admission_blocked_storage",
                    "reason": "test",
                },
            )
            result = orchestrator.run(sources={"abw": object()})
            rows = {row["supplier"]: row for row in result["suppliers"]}
            self.assertEqual(rows["abw"]["status"], "admission_blocked_storage")
            self.assertEqual(rows["abw"]["baseline_after"], "baseline-abw")
            self.assertEqual(len(called), 3)
            self.assertEqual(result["status"], "partial_success")


if __name__ == "__main__":
    unittest.main()
