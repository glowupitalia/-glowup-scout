import hashlib
import json
import os
import sqlite3
import tempfile
import threading
import time
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from discovery_freshness import DiscoveryAmazonCache
from discovery_incremental import DiscoveryIncrementalStore
from discovery_jobs import DiscoveryJobRegistry
from discovery_rotation import DiscoveryRotationStore
from notifications import NotificationOutbox
from storage_retention import (
    APPLY_CONFIRMATION,
    RetentionAbort,
    RetentionExecutor,
    _digest,
    _incremental_manifest,
    main,
)
from storage_maintenance import StorageMaintenanceLock


JOB_ID = "eligible-job"


def file_digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


class RetentionExecutorTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.data = self.root / "data"
        self.data.mkdir()
        self.incremental = self.data / "discovery_incremental.sqlite3"
        self.runtime = self.data / "discovery_jobs.sqlite3"
        self.rotation = self.data / "discovery_rotation.sqlite3"
        self.supplier = self.data / "supplier_catalog.sqlite3"
        self.export = self.data / "discovery_jobs" / f"{JOB_ID}.operational.xlsx"
        self.export.parent.mkdir()
        self.export.write_bytes(b"operational")
        self.store = DiscoveryIncrementalStore(self.incremental)
        self.store.initialize()
        DiscoveryAmazonCache(self.store).initialize()
        DiscoveryJobRegistry(self.runtime).initialize()
        NotificationOutbox(self.runtime).initialize()
        DiscoveryRotationStore(self.rotation).initialize()
        with sqlite3.connect(self.supplier) as connection:
            connection.execute("CREATE TABLE storage_fixture(value TEXT)")
        self._populate()
        self.executor = RetentionExecutor(self.root)
        self.planner_patch = patch(
            "storage_retention.production_retention_plan",
            side_effect=self._planner,
        )
        self.planner_patch.start()

    def tearDown(self):
        self.planner_patch.stop()
        self.temporary.cleanup()

    def _populate(self):
        now = "2026-09-01T00:00:00Z"
        final_product = {
            "canonical_ean": "00000000000001", "is_final_result": True,
            "amazon_title": "Final", "best_purchase_scenario": "scenario-final",
            "recommended_combination": {
                "combination_id": "combination-final", "scenario_id": "scenario-final",
                "asin": "B000FINAL1", "profit": 2.5, "margin_percent": 25.0,
            },
        }
        nonfinal_product = {
            "canonical_ean": "00000000000002", "is_final_result": False,
        }
        with sqlite3.connect(self.incremental) as connection:
            connection.execute(
                """INSERT INTO discovery_incremental_jobs
                   (job_id,schema_version,status,phase,metadata_json,selected_count,
                    catalog_completed_count,created_at,updated_at)
                   VALUES(?,1,'completed','completed','{}',2,2,?,?)""",
                (JOB_ID, now, now),
            )
            for sequence, identifier, product in (
                (0, "00000000000001", final_product),
                (1, "00000000000002", nonfinal_product),
            ):
                connection.execute(
                    """INSERT INTO discovery_job_items
                       (job_id,sequence_no,canonical_identifier,identifier_type,product_json,
                        catalog_status,pricing_status,fees_status,terminal_status,updated_at)
                       VALUES(?,?,?,?,?,'resolved','valid','valid','completed',?)""",
                    (JOB_ID, sequence, identifier, "gtin14", json.dumps(product), now),
                )
                connection.execute(
                    "INSERT INTO discovery_catalog_results VALUES(?,?,?,'{}',?)",
                    (JOB_ID, identifier, "resolved", now),
                )
            scenarios = (
                ("00000000000001", "scenario-final", {"scenario_id": "scenario-final", "supplier": "qudo"}),
                ("00000000000002", "scenario-old", {"scenario_id": "scenario-old", "supplier": "qudo"}),
            )
            for identifier, scenario_id, payload in scenarios:
                connection.execute(
                    "INSERT INTO discovery_purchase_scenarios VALUES(?,?,?,?,?)",
                    (JOB_ID, identifier, scenario_id, json.dumps(payload), "qudo"),
                )
            listings = (
                ("00000000000001", "B000FINAL1", {"asin": "B000FINAL1", "amazon_observation_id": "obs-final"}),
                ("00000000000002", "B000OLD001", {"asin": "B000OLD001", "amazon_observation_id": "obs-old"}),
            )
            for identifier, asin, payload in listings:
                connection.execute(
                    "INSERT INTO discovery_listings VALUES(?,?,?,?,?)",
                    (JOB_ID, identifier, asin, json.dumps(payload), now),
                )
                connection.execute(
                    """INSERT INTO discovery_listing_classifications
                       VALUES(?,?,?,'A1','path','node',NULL,0,'Beauty',1)""",
                    (JOB_ID, identifier, asin),
                )
            combinations = (
                ("combination-final", "00000000000001", {
                    "combination_id": "combination-final", "scenario_id": "scenario-final",
                    "asin": "B000FINAL1", "amazon_observation_id": "obs-final", "profit": 2.5,
                }),
                ("combination-old", "00000000000002", {
                    "combination_id": "combination-old", "scenario_id": "scenario-old",
                    "asin": "B000OLD001", "amazon_observation_id": "obs-old", "profit": -1,
                }),
            )
            for combination_id, identifier, payload in combinations:
                connection.execute(
                    "INSERT INTO discovery_combinations VALUES(?,?,?,?,?)",
                    (JOB_ID, combination_id, identifier, json.dumps(payload), now),
                )
            observations = (
                ("obs-final", {"observation_id": "obs-final", "canonical_ean": "00000000000001", "fee_status": "valid", "reference_price": 20, "bsr_beauty": 100}),
                ("obs-old", {"observation_id": "obs-old", "canonical_ean": "00000000000002", "fee_status": "valid", "reference_price": 10, "bsr_beauty": 200}),
            )
            for observation_id, payload in observations:
                connection.execute(
                    "INSERT INTO discovery_observations VALUES(?,?,?,?)",
                    (JOB_ID, observation_id, json.dumps(payload), now),
                )
            connection.execute(
                """INSERT INTO discovery_resource_events
                   (job_id,level,reason,metrics_json,observed_at)
                   VALUES(?,'info','debug','{}',?)""", (JOB_ID, now),
            )
            connection.execute(
                """INSERT INTO discovery_amazon_cache
                   (canonical_identifier,source_job_id,catalog_status,catalog_observed_at,
                    freshness_json,listings_json,payload_schema_version,payload_sha256,
                    materialized_at,updated_at)
                   VALUES('00000000000001',?,'resolved',?,'{}','[]','v1','hash',?,?)""",
                (JOB_ID, now, now, now),
            )
            connection.execute(
                """INSERT INTO discovery_amazon_fee_cache
                   (fee_cache_key,source_job_id,observation_id,fee_status,fee_observed_at,
                    observation_json,payload_schema_version,payload_sha256,materialized_at,updated_at)
                   VALUES('fee',?,'obs-final','valid',?,'{}','v1','hash',?,?)""",
                (JOB_ID, now, now, now),
            )
            connection.execute(
                """INSERT INTO discovery_amazon_cache_indexed_jobs
                   VALUES(?,?,?,?,?,?,?,?,?,?)""",
                (JOB_ID, "revision", now, "cache_v2", "verified", 2, 2, 1, "aggregate", now),
            )
            connection.commit()

        with sqlite3.connect(self.runtime) as connection:
            connection.execute(
                """INSERT INTO discovery_job_runtime
                   (job_id,status,phase,started_at,updated_at,completed_at,resumable,
                    selected_suppliers_json,filters_json,export_path)
                   VALUES(?,'completed','completed',?,?,?,0,'[\"qudo\"]','{}',?)""",
                (JOB_ID, now, now, now, str(self.export)),
            )
            connection.execute(
                """INSERT INTO notification_outbox
                   (entity_id,event_type,channel,status,created_at,updated_at,attempt_count,
                    attachment_status)
                   VALUES(?,'discovery_completed','email','sent',?,?,1,'sent')""",
                (JOB_ID, now, now),
            )
            connection.commit()
        with sqlite3.connect(self.rotation) as connection:
            connection.execute(
                """INSERT INTO discovery_rotation_selections
                   (job_id,scope_key,cycle_id,canonical_identifier,selected_at,status,
                    catalog_status,analyzed_at)
                   VALUES(?,'scope',1,'00000000000001',?,'analyzed','resolved',?)""",
                (JOB_ID, now, now),
            )
            connection.commit()

        with self.store._connect() as connection:
            state = {
                "completed_at": now,
                "operational_export": {
                    "path": str(self.export), "status": "completed", "valid": True,
                },
            }
            terminal = self.store._terminal_summary_payload(
                JOB_ID, connection=connection, state=state,
            )
            metadata = {
                "retention_mode": "full", "terminal_summary_version": 1,
                "terminal_summary": terminal,
                "terminal_summary_sha256": _digest(terminal),
            }
            connection.execute(
                "UPDATE discovery_incremental_jobs SET metadata_json=? WHERE job_id=?",
                (json.dumps(metadata, sort_keys=True, separators=(",", ":")), JOB_ID),
            )
            connection.commit()

    def _planner(self, watermark, _root):
        return {
            "version": "storage_retention_policy_v1", "policy": "D_KEEP_GLOBAL_5_SCOPE_1",
            "inventory_reliable": True, "inventory_errors": [],
            "jobs": [{
                "job_id": JOB_ID, "classification": "FINAL_ONLY_ELIGIBLE",
                "scope": "scope", "completed_at": "2026-09-01T00:00:00Z",
                "estimated_logical_reclaim_bytes": 4096,
            }],
        }

    def plan(self):
        return self.executor.build_plan("PRESSURE", job_ids=[JOB_ID])

    def apply(self, plan=None):
        plan = plan or self.plan()
        return self.executor.apply_plan(
            plan, apply=True, confirmed_job_ids=[JOB_ID], confirmation=APPLY_CONFIRMATION,
        )

    def test_dry_run_is_default_and_performs_zero_writes(self):
        plan = self.plan()
        before = file_digest(self.incremental)
        # Preview never participates in maintenance exclusion and therefore
        # cannot delay a concurrent Discovery start/claim.
        lock = StorageMaintenanceLock(self.data / "discovery-maintenance.lock")
        with lock.discovery_start_guard():
            result = self.executor.apply_plan(plan)
        self.assertEqual(result["mode"], "dry-run")
        self.assertEqual(result["writes_performed"], 0)
        self.assertEqual(result["results"][0]["status"], "ELIGIBLE")
        self.assertEqual(file_digest(self.incremental), before)

    def test_plan_contains_authoritative_expected_state_and_closure(self):
        candidate = self.plan()["candidates"][0]
        self.assertEqual(candidate["expected_retention_mode"], "full")
        self.assertTrue(candidate["expected_terminal_summary_sha256"])
        self.assertEqual(candidate["expected_cache_marker"]["verification_state"], "verified")
        self.assertTrue(candidate["expected_reference_state_sha256"])
        self.assertEqual(candidate["manifest"]["final_identifier_count"], 1)
        self.assertEqual(candidate["manifest"]["tables"]["discovery_job_items"]["rows_remove"], 1)
        self.assertFalse(candidate["external_files"]["operational_export"]["execution"])

    def test_apply_requires_explicit_phrase_and_every_candidate(self):
        plan = self.plan()
        with self.assertRaisesRegex(RetentionAbort, "explicit_apply_confirmation"):
            self.executor.apply_plan(plan, apply=True, confirmed_job_ids=[JOB_ID])
        with self.assertRaisesRegex(RetentionAbort, "explicit_apply_confirmation"):
            self.executor.apply_plan(plan, apply=True, confirmation=APPLY_CONFIRMATION)

    def test_apply_prunes_only_nonfinal_rows_and_preserves_dependencies(self):
        cache_before = self.executor._reference_snapshot(JOB_ID)
        result = self.apply()["results"][0]
        self.assertEqual(result["status"], "APPLIED")
        self.assertEqual(result["vacuum_operations"], 0)
        self.assertEqual(result["external_file_operations"], 0)
        self.assertTrue(self.export.is_file())
        with sqlite3.connect(self.incremental) as connection:
            metadata = json.loads(connection.execute(
                "SELECT metadata_json FROM discovery_incremental_jobs WHERE job_id=?", (JOB_ID,),
            ).fetchone()[0])
            self.assertEqual(metadata["retention_mode"], "final_only")
            self.assertFalse(metadata["exact_replay_capable"])
            for table in (
                "discovery_job_items", "discovery_purchase_scenarios",
                "discovery_catalog_results", "discovery_listings",
                "discovery_listing_classifications", "discovery_combinations",
                "discovery_observations",
            ):
                self.assertEqual(connection.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE job_id=?", (JOB_ID,),
                ).fetchone()[0], 1)
            self.assertEqual(connection.execute(
                "SELECT COUNT(*) FROM discovery_resource_events WHERE job_id=?", (JOB_ID,),
            ).fetchone()[0], 0)
        self.assertEqual(self.executor._reference_snapshot(JOB_ID), cache_before)
        summary = self.store.summary(JOB_ID)
        self.assertEqual(summary["final_opportunity_count"], 1)
        self.assertFalse(summary["exact_replay_capable"])
        audit_rows = [
            json.loads(line) for line in self.executor.audit_path.read_text().splitlines()
        ]
        self.assertEqual(audit_rows[-1]["result"], "applied")
        self.assertEqual(audit_rows[-1]["actor"], "manual")

    def test_second_apply_is_idempotent_noop_with_same_audit(self):
        plan = self.plan()
        self.apply(plan)
        with sqlite3.connect(self.incremental) as connection:
            before = connection.execute(
                "SELECT metadata_json FROM discovery_incremental_jobs WHERE job_id=?", (JOB_ID,),
            ).fetchone()[0]
        result = self.apply(plan)["results"][0]
        self.assertEqual(result["status"], "NOOP_ALREADY_APPLIED")
        with sqlite3.connect(self.incremental) as connection:
            after = connection.execute(
                "SELECT metadata_json FROM discovery_incremental_jobs WHERE job_id=?", (JOB_ID,),
            ).fetchone()[0]
        self.assertEqual(before, after)

    def test_stale_plan_aborts_after_cache_marker_changes(self):
        plan = self.plan()
        with sqlite3.connect(self.incremental) as connection:
            connection.execute(
                "UPDATE discovery_amazon_cache_indexed_jobs SET source_updated_at='changed'"
            )
            connection.commit()
        preview = self.executor.preview(plan)
        self.assertEqual(preview["results"][0]["status"], "STALE_PLAN")
        with self.assertRaisesRegex(RetentionAbort, "stale_or_ineligible"):
            self.apply(plan)

    def test_active_or_resumable_job_aborts_apply(self):
        plan = self.plan()
        with sqlite3.connect(self.runtime) as connection:
            connection.execute(
                "UPDATE discovery_job_runtime SET status='running',resumable=1 WHERE job_id=?",
                (JOB_ID,),
            )
            connection.commit()
        with self.assertRaisesRegex(RetentionAbort, "active_or_resumable"):
            self.apply(plan)
        audit = json.loads(self.executor.audit_path.read_text().splitlines()[-1])
        self.assertEqual(audit["outcome"], "RETENTION_BLOCKED_ACTIVE_WORKLOAD")

    def test_invalid_summary_is_blocked(self):
        with sqlite3.connect(self.incremental) as connection:
            metadata = json.loads(connection.execute(
                "SELECT metadata_json FROM discovery_incremental_jobs WHERE job_id=?", (JOB_ID,),
            ).fetchone()[0])
            metadata["terminal_summary_sha256"] = "invalid"
            connection.execute(
                "UPDATE discovery_incremental_jobs SET metadata_json=? WHERE job_id=?",
                (json.dumps(metadata), JOB_ID),
            )
            connection.commit()
        candidate = self.plan()["candidates"][0]
        self.assertIn("terminal_summary_missing_or_invalid", candidate["blockers"])

    def test_unverified_cache_is_blocked(self):
        with sqlite3.connect(self.incremental) as connection:
            connection.execute(
                "UPDATE discovery_amazon_cache_indexed_jobs SET verification_state='unverified'"
            )
            connection.commit()
        self.assertIn("cache_not_verified", self.plan()["candidates"][0]["blockers"])

    def test_pending_outbox_is_blocked(self):
        with sqlite3.connect(self.runtime) as connection:
            connection.execute("UPDATE notification_outbox SET status='pending'")
            connection.commit()
        self.assertTrue(any(
            value.startswith("outbox_non_terminal")
            for value in self.plan()["candidates"][0]["blockers"]
        ))

    def test_rotation_uncommitted_is_blocked(self):
        with sqlite3.connect(self.rotation) as connection:
            connection.execute("UPDATE discovery_rotation_selections SET status='selected'")
            connection.commit()
        self.assertIn("rotation_not_committed", self.plan()["candidates"][0]["blockers"])

    def test_missing_export_is_blocked(self):
        self.export.unlink()
        self.assertIn("operational_export_missing", self.plan()["candidates"][0]["blockers"])

    def test_unknown_reference_is_blocked(self):
        with sqlite3.connect(self.incremental) as connection:
            connection.execute("DELETE FROM discovery_amazon_cache_indexed_jobs")
            connection.commit()
        blockers = self.plan()["candidates"][0]["blockers"]
        self.assertIn("reference_state_unknown", blockers)

    def test_unresolved_final_reference_is_blocked(self):
        with sqlite3.connect(self.incremental) as connection:
            connection.execute(
                "DELETE FROM discovery_purchase_scenarios WHERE scenario_id='scenario-final'"
            )
            connection.commit()
        blockers = self.plan()["candidates"][0]["blockers"]
        self.assertTrue(any(value.startswith("missing_scenario") for value in blockers))

    def test_rollback_restores_all_rows_on_post_verification_failure(self):
        plan = self.plan()
        before = file_digest(self.incremental)
        original = _incremental_manifest
        calls = 0

        def corrupt_post(connection, job_id):
            nonlocal calls
            calls += 1
            value = original(connection, job_id)
            if calls == 3:
                value["tables"]["discovery_job_items"]["full_sha256"] = "mismatch"
            return value

        with patch("storage_retention._incremental_manifest", side_effect=corrupt_post):
            with self.assertRaisesRegex(RetentionAbort, "post_closure_mismatch"):
                self.apply(plan)
        self.assertEqual(file_digest(self.incremental), before)
        self.assertEqual(self.store.counts(JOB_ID)["items"], 2)
        self.assertEqual(self.store.terminal_summary_status(JOB_ID)["retention_mode"], "full")
        audit = [
            json.loads(line) for line in self.executor.audit_path.read_text().splitlines()
        ]
        self.assertTrue(any(
            row.get("outcome") == "RETENTION_ABORT_VERIFICATION" for row in audit
        ))

    def test_db1e11_is_always_blocked(self):
        blocked_id = "db1e11b8d6294342b811a343ca4a4142"
        with sqlite3.connect(self.incremental) as connection:
            for table in (
                "discovery_incremental_jobs", "discovery_job_items",
                "discovery_purchase_scenarios", "discovery_catalog_results",
                "discovery_listings", "discovery_listing_classifications",
                "discovery_combinations", "discovery_observations",
                "discovery_resource_events",
            ):
                connection.execute(
                    f"UPDATE {table} SET job_id=? WHERE job_id=?", (blocked_id, JOB_ID),
                )
            connection.execute(
                "UPDATE discovery_amazon_cache SET source_job_id=? WHERE source_job_id=?",
                (blocked_id, JOB_ID),
            )
            connection.execute(
                "UPDATE discovery_amazon_fee_cache SET source_job_id=? WHERE source_job_id=?",
                (blocked_id, JOB_ID),
            )
            connection.execute(
                "UPDATE discovery_amazon_cache_indexed_jobs SET source_job_id=? WHERE source_job_id=?",
                (blocked_id, JOB_ID),
            )
            connection.commit()
        with sqlite3.connect(self.runtime) as connection:
            connection.execute(
                "UPDATE discovery_job_runtime SET job_id=? WHERE job_id=?", (blocked_id, JOB_ID),
            )
            connection.execute(
                "UPDATE notification_outbox SET entity_id=? WHERE entity_id=?", (blocked_id, JOB_ID),
            )
            connection.commit()
        with sqlite3.connect(self.rotation) as connection:
            connection.execute(
                "UPDATE discovery_rotation_selections SET job_id=? WHERE job_id=?", (blocked_id, JOB_ID),
            )
            connection.commit()
        planner = self._planner("PRESSURE", self.root)
        planner["jobs"][0]["job_id"] = blocked_id
        with patch("storage_retention.production_retention_plan", return_value=planner):
            candidate = self.executor.build_plan(
                "PRESSURE", job_ids=[blocked_id],
            )["candidates"][0]
        self.assertFalse(candidate["eligible"])
        self.assertIn("explicit_db1e11_exclusion", candidate["blockers"])

    def test_cli_execute_defaults_to_dry_run(self):
        plan = self.plan()
        path = self.root / "plan.json"
        path.write_text(json.dumps(plan), encoding="utf-8")
        before = file_digest(self.incremental)
        output = StringIO()
        with redirect_stdout(output):
            self.assertEqual(main([
                "--project-root", str(self.root), "execute", "--plan", str(path),
            ]), 0)
        self.assertIn('"mode": "dry-run"', output.getvalue())
        self.assertEqual(file_digest(self.incremental), before)

    def test_plan_becoming_stale_while_waiting_for_lock_aborts_after_acquisition(self):
        plan = self.plan()
        lock = StorageMaintenanceLock(self.data / "discovery-maintenance.lock")
        self.executor.maintenance_lock = lock
        outcome = {}

        def apply_after_wait():
            try:
                self.executor.apply_plan(
                    plan, apply=True, confirmation=APPLY_CONFIRMATION,
                    confirmed_job_ids=[JOB_ID], lock_timeout_seconds=1,
                )
            except Exception as error:  # assertion captures the worker outcome
                outcome["error"] = error

        with lock.discovery_start_guard():
            worker = threading.Thread(target=apply_after_wait)
            worker.start()
            time.sleep(0.05)
            with sqlite3.connect(self.incremental) as connection:
                connection.execute(
                    "UPDATE discovery_amazon_cache_indexed_jobs "
                    "SET source_updated_at='changed-while-waiting' WHERE source_job_id=?",
                    (JOB_ID,),
                )
                connection.commit()
        worker.join(2)
        self.assertFalse(worker.is_alive())
        self.assertIsInstance(outcome.get("error"), RetentionAbort)
        self.assertIn("stale_or_ineligible_plan", str(outcome["error"]))
        self.assertEqual(
            self.store.summary(JOB_ID)["retention_mode"], "full",
        )

    def test_watermark_change_while_waiting_for_lock_is_rechecked(self):
        plan = self.plan()
        state = {"value": "NORMAL"}

        def observed_metrics():
            value = state["value"]
            return {
                "reliable": True,
                "watermark": {"reliable": True, "state": value, "version": "v1"},
            }

        lock = StorageMaintenanceLock(self.data / "discovery-maintenance.lock")
        self.executor.maintenance_lock = lock
        self.executor.metrics_provider = observed_metrics
        outcome = {}

        def apply_after_wait():
            try:
                self.executor.apply_plan(
                    plan, apply=True, confirmation=APPLY_CONFIRMATION,
                    confirmed_job_ids=[JOB_ID], lock_timeout_seconds=1,
                )
            except Exception as error:  # assertion captures the worker outcome
                outcome["error"] = error

        with lock.discovery_start_guard():
            worker = threading.Thread(target=apply_after_wait)
            worker.start()
            time.sleep(0.05)
            state["value"] = "PRESSURE"
        worker.join(2)
        self.assertFalse(worker.is_alive())
        self.assertIsInstance(outcome.get("error"), RetentionAbort)
        self.assertIn("watermark_changed_while_waiting", str(outcome["error"]))
        self.assertEqual(self.store.summary(JOB_ID)["retention_mode"], "full")

    def test_discovery_claim_wins_then_retention_aborts_without_apply(self):
        plan = self.plan()
        lock = StorageMaintenanceLock(self.data / "discovery-maintenance.lock")
        self.executor.maintenance_lock = lock
        registry = DiscoveryJobRegistry(self.runtime)
        outcome = {}

        def apply_after_claim():
            try:
                self.executor.apply_plan(
                    plan, apply=True, confirmation=APPLY_CONFIRMATION,
                    confirmed_job_ids=[JOB_ID], lock_timeout_seconds=1,
                )
            except Exception as error:
                outcome["error"] = error

        with lock.discovery_start_guard():
            self.assertTrue(registry.claim(JOB_ID, pid=os.getpid()))
            worker = threading.Thread(target=apply_after_claim)
            worker.start()
            time.sleep(0.05)
        worker.join(2)
        self.assertFalse(worker.is_alive())
        self.assertIsInstance(outcome.get("error"), RetentionAbort)
        self.assertIn("active_or_resumable_discovery_present", str(outcome["error"]))
        self.assertEqual(self.store.summary(JOB_ID)["retention_mode"], "full")


if __name__ == "__main__":
    unittest.main()
