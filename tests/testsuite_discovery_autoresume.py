import os
import sqlite3
import tempfile
import threading
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock, patch

import requests

from discovery import DiscoveryCheckpointStore, default_filters
from discovery_amazon import AmazonBatchError, RefreshingTokenProvider, _request_with_retry
from discovery_autoresume import evaluate_autoresume
from discovery_jobs import DiscoveryJobRegistry
from discovery_recovery import (
    DISCOVERY_RECOVERY_COOLDOWN_SECONDS,
    DiscoveryFailure,
    RETRYABLE_PROVIDER_FAILURE,
    classify_retryable_failure,
    progress_fingerprint,
)
from discovery_recovery_supervisor import run_once


UTC = timezone.utc


class Response:
    def __init__(self, status):
        self.status_code = status
        self.headers = {}

    def raise_for_status(self):
        if self.status_code >= 400:
            response = requests.Response()
            response.status_code = self.status_code
            raise requests.HTTPError(response=response)


class DiscoveryFailureClassificationTests(unittest.TestCase):
    def exhaust(self, request):
        with self.assertRaises(AmazonBatchError) as raised:
            _request_with_retry(
                "GET", "https://example.invalid",
                token_provider=RefreshingTokenProvider(lambda: "token"),
                request_func=request, sleep_func=lambda _seconds: None,
                random_func=lambda: 0, phase="pricing",
            )
        return raised.exception

    def test_429_exhaustion_is_retryable(self):
        failure = classify_retryable_failure(
            self.exhaust(lambda *_args, **_kwargs: Response(429)), phase="pricing",
        )
        self.assertEqual(failure.category, RETRYABLE_PROVIDER_FAILURE)
        self.assertEqual(failure.status_code, 429)

    def test_5xx_exhaustion_is_retryable(self):
        failure = classify_retryable_failure(
            self.exhaust(lambda *_args, **_kwargs: Response(503)), phase="pricing",
        )
        self.assertEqual(failure.status_code, 503)

    def test_network_exhaustion_is_retryable(self):
        def offline(*_args, **_kwargs):
            raise requests.ConnectionError("offline")

        error = self.exhaust(offline)
        failure = classify_retryable_failure(error, phase="pricing")
        self.assertEqual(failure.transport_class, "ConnectionError")

    def test_deterministic_and_unknown_failures_are_not_retryable(self):
        self.assertIsNone(classify_retryable_failure(ValueError("bad data"), phase="pricing"))
        self.assertIsNone(classify_retryable_failure(RuntimeError("unknown"), phase="pricing"))

        with self.assertRaises(requests.exceptions.InvalidURL):
            _request_with_retry(
                "GET", "invalid", token_provider=RefreshingTokenProvider(lambda: "token"),
                request_func=lambda *_args, **_kwargs: (_ for _ in ()).throw(
                    requests.exceptions.InvalidURL("invalid")
                ),
                sleep_func=lambda _seconds: None, random_func=lambda: 0,
                phase="pricing",
            )

    def test_signature_is_stable_and_excludes_message_and_time(self):
        first = DiscoveryFailure(
            RETRYABLE_PROVIDER_FAILURE, "amazon", "pricing", "AmazonBatchError", 429,
        )
        second = DiscoveryFailure(
            RETRYABLE_PROVIDER_FAILURE, "amazon", "pricing", "AmazonBatchError", 429,
        )
        self.assertEqual(first.signature, second.signature)


class DiscoveryRecoveryRegistryTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.registry = DiscoveryJobRegistry(self.root / "jobs.sqlite3")
        self.checkpoints = DiscoveryCheckpointStore(self.root / "checkpoints")
        self.now = datetime(2026, 10, 3, 20, 19, 58, tzinfo=UTC)

    def tearDown(self):
        self.temporary.cleanup()

    def state(self, job_id="4157d3e7700e44f78be002f55fb73ff3", current=300, total=788):
        state = self.checkpoints.create(default_filters())
        old = self.checkpoints.path(state["job_id"])
        state.update({
            "job_id": job_id, "status": "running", "phase": "pricing",
            "started_at": "2026-10-03T16:57:48Z",
            "selected_suppliers": ["qogita", "umma", "abw", "qudo"],
            "run_budget": "all", "sampled_identifier_count": total,
            "progress_current": current, "progress_total": total,
        })
        if old.exists():
            old.unlink()
        self.checkpoints.save(state)
        self.registry.register_checkpoint(state)
        with self.registry._connect() as connection:
            connection.execute(
                "UPDATE discovery_job_runtime SET phase='pricing',progress_current=?,progress_total=? WHERE job_id=?",
                (current, total, job_id),
            )
            connection.commit()
        return state

    @staticmethod
    def failure(status=429):
        return DiscoveryFailure(
            RETRYABLE_PROVIDER_FAILURE, "amazon", "pricing", "AmazonBatchError", status,
        )

    def record_failure(self, job_id, *, now=None):
        runtime = self.registry.get(job_id)
        self.registry.fail(
            job_id, "Amazon temporary status 429", failure=self.failure(),
            progress_fingerprint_value=progress_fingerprint(runtime),
            observed_at=now or self.now,
        )
        return self.registry.get(job_id)

    def auto_launch(self, job_id, when, pid=4242):
        runtime = self.registry.get(job_id)
        with (
            patch("discovery_jobs.subprocess.Popen", return_value=Mock(pid=pid)),
            patch("discovery_jobs.DEFAULT_LOG_DIR", self.root / "logs"),
            patch("discovery_jobs.process_alive", return_value=False),
        ):
            return self.registry.launch(
                job_id, auto_resume=True,
                expected_failure_signature=runtime["failure_signature"], observed_at=when,
            )

    def test_schema_migration_and_retryable_failure_persist_across_restart(self):
        state = self.state()
        runtime = self.record_failure(state["job_id"])
        reopened = DiscoveryJobRegistry(self.registry.path).get(state["job_id"])
        self.assertEqual(reopened["failure_category"], RETRYABLE_PROVIDER_FAILURE)
        self.assertEqual(reopened["failure_phase"], "pricing")
        self.assertEqual(reopened["failure_signature"], runtime["failure_signature"])
        self.assertFalse(reopened["manual_intervention_required"])
        self.assertEqual(reopened["total_auto_resume_count"], 0)

    def test_additive_migration_upgrades_an_existing_registry(self):
        legacy = self.root / "legacy.sqlite3"
        with sqlite3.connect(legacy) as connection:
            connection.execute(
                """CREATE TABLE discovery_job_runtime (
                   job_id TEXT PRIMARY KEY,status TEXT NOT NULL,phase TEXT NOT NULL,
                   started_at TEXT,updated_at TEXT NOT NULL,completed_at TEXT,budget TEXT,
                   progress_current INTEGER NOT NULL DEFAULT 0,
                   progress_total INTEGER NOT NULL DEFAULT 0,
                   resumable INTEGER NOT NULL DEFAULT 1,error TEXT,worker_pid INTEGER,
                   lease_expires_at TEXT,selected_suppliers_json TEXT NOT NULL DEFAULT '[]',
                   filters_json TEXT NOT NULL DEFAULT '{}',checkpoint_path TEXT,export_path TEXT
                )"""
            )
        migrated = DiscoveryJobRegistry(legacy)
        migrated.initialize()
        with migrated._connect() as connection:
            columns = {
                row[1] for row in connection.execute(
                    "PRAGMA table_info(discovery_job_runtime)"
                )
            }
        self.assertTrue({
            "failure_category", "failure_signature", "progress_fingerprint",
            "consecutive_auto_resume_count", "total_auto_resume_count",
            "cooldown_until", "manual_intervention_required",
        }.issubset(columns))

    def test_cooldown_is_exactly_five_minutes_and_preserves_job(self):
        state = self.state()
        runtime = self.record_failure(state["job_id"])
        self.assertEqual(
            runtime["cooldown_until"],
            (self.now + timedelta(seconds=DISCOVERY_RECOVERY_COOLDOWN_SECONDS))
            .isoformat().replace("+00:00", "Z"),
        )
        with self.assertRaisesRegex(RuntimeError, "not eligible"):
            self.auto_launch(state["job_id"], self.now + timedelta(seconds=299))
        self.assertEqual(
            self.auto_launch(state["job_id"], self.now + timedelta(seconds=300)), 4242,
        )
        self.assertEqual(self.registry.get(state["job_id"])["job_id"], state["job_id"])
        self.assertEqual(len(self.registry.recent(10)), 1)

    def test_live_pid_never_auto_resumes(self):
        state = self.state()
        self.record_failure(state["job_id"])
        with self.registry._connect() as connection:
            connection.execute(
                "UPDATE discovery_job_runtime SET worker_pid=? WHERE job_id=?",
                (os.getpid(), state["job_id"]),
            )
            connection.commit()
        runtime = self.registry.get(state["job_id"])
        with (
            patch("discovery_jobs.subprocess.Popen", return_value=Mock(pid=4242)),
            patch("discovery_jobs.DEFAULT_LOG_DIR", self.root / "logs"),
            patch("discovery_jobs.process_alive", return_value=True),
        ):
            with self.assertRaisesRegex(RuntimeError, "already running|not eligible"):
                self.registry.launch(
                    state["job_id"], auto_resume=True,
                    expected_failure_signature=runtime["failure_signature"],
                    observed_at=self.now + timedelta(minutes=5),
                )

    def test_three_no_progress_resumes_then_manual_intervention(self):
        state = self.state()
        job_id = state["job_id"]
        self.record_failure(job_id)
        when = self.now + timedelta(minutes=5)
        for attempt in range(1, 4):
            pid = 5000 + attempt
            self.auto_launch(job_id, when, pid=pid)
            self.assertTrue(self.registry.claim(job_id, pid=pid))
            failed = self.record_failure(job_id, now=when + timedelta(seconds=1))
            self.assertEqual(failed["consecutive_auto_resume_count"], attempt)
            self.assertEqual(failed["total_auto_resume_count"], attempt)
            when = when + timedelta(minutes=5, seconds=1)
        runtime = self.registry.get(job_id)
        self.assertEqual(runtime["status"], "manual_intervention_required")
        self.assertTrue(runtime["manual_intervention_required"])
        with self.assertRaisesRegex(RuntimeError, "not eligible"):
            self.auto_launch(job_id, self.now + timedelta(hours=1), pid=6000)

    def test_persisted_progress_resets_consecutive_but_not_total(self):
        state = self.state()
        job_id = state["job_id"]
        self.record_failure(job_id)
        when = self.now + timedelta(minutes=5)
        self.auto_launch(job_id, when, pid=5101)
        self.assertTrue(self.registry.claim(job_id, pid=5101))
        self.registry.heartbeat(job_id, pid=5101, phase="pricing", current=320, total=788)
        runtime = self.registry.get(job_id)
        self.assertEqual(runtime["consecutive_auto_resume_count"], 0)
        self.assertEqual(runtime["total_auto_resume_count"], 1)
        failed = self.record_failure(job_id, now=when + timedelta(minutes=10))
        self.assertEqual(failed["consecutive_auto_resume_count"], 0)
        self.assertEqual(failed["total_auto_resume_count"], 1)
        self.auto_launch(job_id, when + timedelta(minutes=15), pid=5102)
        runtime = self.registry.get(job_id)
        self.assertEqual(runtime["consecutive_auto_resume_count"], 1)
        self.assertEqual(runtime["total_auto_resume_count"], 2)

    def test_manual_resume_clears_loop_guard_and_preserves_total(self):
        state = self.state()
        job_id = state["job_id"]
        self.record_failure(job_id)
        with self.registry._connect() as connection:
            connection.execute(
                "UPDATE discovery_job_runtime SET status='manual_intervention_required',"
                "manual_intervention_required=1,consecutive_auto_resume_count=3,"
                "total_auto_resume_count=3 WHERE job_id=?", (job_id,),
            )
            connection.commit()
        with (
            patch("discovery_jobs.subprocess.Popen", return_value=Mock(pid=6200)),
            patch("discovery_jobs.DEFAULT_LOG_DIR", self.root / "logs"),
        ):
            self.registry.launch(job_id)
        runtime = self.registry.get(job_id)
        self.assertFalse(runtime["manual_intervention_required"])
        self.assertEqual(runtime["consecutive_auto_resume_count"], 0)
        self.assertEqual(runtime["total_auto_resume_count"], 3)

    def test_unclassified_and_completed_jobs_are_never_candidates(self):
        state = self.state()
        self.registry.fail(state["job_id"], "ValueError")
        self.assertEqual(self.registry.recovery_candidates(), [])

    def test_evaluate_autoresume_honors_cooldown_and_same_job_checkpoint(self):
        state = self.state()
        job_id = state["job_id"]
        self.record_failure(job_id)
        store = Mock()
        store.has_job.return_value = True
        governor = Mock()
        governor.sample.return_value = {}
        governor.evaluate.return_value = ("continue", "healthy", {})
        checkpoint = Mock()
        checkpoint.load.return_value = {"supplier_snapshot_set": {}}
        with patch("discovery_autoresume.DiscoveryCheckpointStore", return_value=checkpoint):
            self.assertEqual(
                evaluate_autoresume(
                    job_id, registry=self.registry, store=store, governor=governor,
                    automatic=True, observed_at=self.now + timedelta(seconds=299),
                ),
                (False, "cooldown_active"),
            )
            self.assertEqual(
                evaluate_autoresume(
                    job_id, registry=self.registry, store=store, governor=governor,
                    automatic=True, observed_at=self.now + timedelta(seconds=300),
                ),
                (True, "resumable"),
            )
        with self.registry._connect() as connection:
            connection.execute(
                "UPDATE discovery_job_runtime SET status='completed',resumable=0,"
                "failure_category=? WHERE job_id=?",
                (RETRYABLE_PROVIDER_FAILURE, state["job_id"]),
            )
            connection.commit()
        self.assertEqual(self.registry.recovery_candidates(), [])

    def test_two_concurrent_auto_launches_have_one_winner(self):
        state = self.state()
        job_id = state["job_id"]
        runtime = self.record_failure(job_id)
        barrier = threading.Barrier(2)
        outcomes = []

        def launch():
            barrier.wait()
            try:
                with (
                    patch("discovery_jobs.subprocess.Popen", return_value=Mock(pid=7000)),
                    patch("discovery_jobs.DEFAULT_LOG_DIR", self.root / "logs"),
                ):
                    self.registry.launch(
                        job_id, auto_resume=True,
                        expected_failure_signature=runtime["failure_signature"],
                        observed_at=self.now + timedelta(minutes=5),
                    )
                outcomes.append("launched")
            except RuntimeError:
                outcomes.append("rejected")

        workers = [threading.Thread(target=launch) for _ in range(2)]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(3)
        self.assertEqual(sorted(outcomes), ["launched", "rejected"])


class DiscoverySupervisorTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.registry = DiscoveryJobRegistry(self.root / "jobs.sqlite3")
        self.now = datetime(2026, 10, 3, 20, 25, tzinfo=UTC)
        state = {
            "job_id": "real-job-copy", "status": "running", "phase": "pricing",
            "started_at": "2026-10-03T16:57:48Z", "run_budget": "all",
            "progress_current": 300, "progress_total": 788,
            "sampled_identifier_count": 788, "selected_suppliers": [], "filters": {},
        }
        self.registry.register_checkpoint(state)
        failure = DiscoveryFailure(
            RETRYABLE_PROVIDER_FAILURE, "amazon", "pricing", "AmazonBatchError", 429,
        )
        self.registry.fail(
            state["job_id"], "429", failure=failure,
            progress_fingerprint_value=progress_fingerprint(state),
            observed_at=self.now - timedelta(minutes=5),
        )

    def tearDown(self):
        self.temporary.cleanup()

    def test_supervisor_is_one_shot_and_launches_same_job(self):
        with (
            patch("discovery_jobs.subprocess.Popen", return_value=Mock(pid=8001)),
            patch("discovery_jobs.DEFAULT_LOG_DIR", self.root / "logs"),
        ):
            result = run_once(
                registry=self.registry, store=Mock(path=self.root / "incremental.sqlite3"),
                governor=Mock(), observed_at=self.now,
                evaluator=lambda *_args, **_kwargs: (True, "resumable"),
            )
        self.assertEqual(result, {
            "action": "launched", "job_id": "real-job-copy", "worker_pid": 8001,
        })
        self.assertEqual(len(self.registry.recent(10)), 1)

    def test_supervisor_before_cooldown_does_nothing(self):
        result = run_once(
            registry=self.registry, store=Mock(path=self.root / "incremental.sqlite3"),
            governor=Mock(), observed_at=self.now - timedelta(seconds=1),
            evaluator=lambda *_args, **_kwargs: (False, "cooldown_active"),
        )
        self.assertEqual(result["action"], "none")
        self.assertEqual(result["reason"], "cooldown_active")

    def test_supervisor_entrypoint_and_plist_are_one_shot(self):
        root = Path(__file__).resolve().parents[1]
        source = (root / "discovery_recovery_supervisor.py").read_text(encoding="utf-8")
        ui_source = (root / "app_glowup.py").read_text(encoding="utf-8")
        plist = (root / "launchd/com.glowup.scout.discovery-autoresume.plist").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("while True", source)
        self.assertIn("<integer>60</integer>", plist)
        self.assertNotIn("<key>KeepAlive</key>", plist)
        self.assertIn("discovery_recovery_supervisor.py", plist)
        self.assertIn("Auto-recovery eseguito:", ui_source)
        self.assertIn("Discovery: intervento manuale richiesto", ui_source)
        self.assertIn("resume_current_discovery", ui_source)

    def test_real_incident_fixture_300_then_680_can_complete_without_new_job(self):
        job_id = "real-job-copy"
        with (
            patch("discovery_jobs.subprocess.Popen", return_value=Mock(pid=8101)),
            patch("discovery_jobs.DEFAULT_LOG_DIR", self.root / "logs"),
        ):
            self.registry.launch(
                job_id, auto_resume=True,
                expected_failure_signature=self.registry.get(job_id)["failure_signature"],
                observed_at=self.now,
            )
        self.registry.claim(job_id, pid=8101)
        self.registry.heartbeat(job_id, pid=8101, phase="pricing", current=680, total=788)
        failure = DiscoveryFailure(
            RETRYABLE_PROVIDER_FAILURE, "amazon", "pricing", "AmazonBatchError", 429,
        )
        self.registry.fail(
            job_id, "429", failure=failure,
            progress_fingerprint_value=progress_fingerprint(self.registry.get(job_id)),
            observed_at=self.now + timedelta(minutes=1),
        )
        with (
            patch("discovery_jobs.subprocess.Popen", return_value=Mock(pid=8102)),
            patch("discovery_jobs.DEFAULT_LOG_DIR", self.root / "logs"),
        ):
            self.registry.launch(
                job_id, auto_resume=True,
                expected_failure_signature=self.registry.get(job_id)["failure_signature"],
                observed_at=self.now + timedelta(minutes=6),
            )
        self.registry.claim(job_id, pid=8102)
        self.registry.heartbeat(job_id, pid=8102, phase="pricing", current=788, total=788)
        runtime = self.registry.get(job_id)
        self.assertEqual(runtime["consecutive_auto_resume_count"], 0)
        self.assertEqual(runtime["total_auto_resume_count"], 2)
        self.assertEqual(len(self.registry.recent(10)), 1)


if __name__ == "__main__":
    unittest.main()
