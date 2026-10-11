import os
import tempfile
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

from discovery_archive_lifecycle import DiscoveryArchiveLifecycle
from discovery_jobs import DiscoveryJobRegistry
from discovery_recovery_supervisor import run_once
from discovery_resources import (
    DISCOVERY_LIVE_STORAGE_FLOOR_BYTES,
    GIB,
    DiscoveryResourceGovernor,
    ResourcePolicy,
    ResourceSnapshot,
)


UTC = timezone.utc


def snapshot(free_bytes):
    return ResourceSnapshot(
        rss_bytes=100, available_memory_bytes=10 * GIB, swap_used_bytes=0,
        disk_free_bytes=free_bytes, database_bytes=100, wal_bytes=100,
        write_bytes=100, write_rate_bytes_per_second=1, db_latency_ms=1,
        observed_monotonic=1.0,
    )


def metrics(free_bytes, *, file_bytes=1000, freelist_bytes=0):
    return {
        "filesystem_free_bytes": free_bytes,
        "watermark": {"state": "PRESSURE" if free_bytes < 40 * GIB else "NORMAL"},
        "databases": {"discovery": {
            "file_bytes": file_bytes, "page_count": file_bytes // 4096,
            "freelist_bytes": freelist_bytes,
        }},
    }


def lifecycle_metrics(*, backlog=1, reclaimable=4 * GIB, sqlite_bytes=8 * GIB):
    return {
        "archive_backlog_jobs": backlog,
        "archive_backlog_estimated_bytes": reclaimable,
        "reclaimable_bytes": reclaimable,
        "sqlite_bytes": sqlite_bytes,
    }


class LiveStorageFloorTests(unittest.TestCase):
    def test_default_live_floor_is_forty_gib(self):
        self.assertEqual(ResourcePolicy().disk_free_hard_bytes, 40 * GIB)
        self.assertEqual(DISCOVERY_LIVE_STORAGE_FLOOR_BYTES, 40 * GIB)
        governor = DiscoveryResourceGovernor(policy=ResourcePolicy())
        self.assertEqual(governor.evaluate(snapshot(40 * GIB))[0], "continue")
        action, reason, threshold = governor.evaluate(snapshot((40 * GIB) - 1))
        self.assertEqual((action, reason, threshold), (
            "pause", "disk_free_hard", 40 * GIB,
        ))


class StorageRecoverySupervisorTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.registry = DiscoveryJobRegistry(self.root / "jobs.sqlite3")
        self.now = datetime(2026, 10, 10, 10, 0, tzinfo=UTC)
        self.job_id = "storage-paused-job"
        self.registry.register_checkpoint({
            "job_id": self.job_id, "status": "running", "phase": "pricing",
            "started_at": "2026-10-10T09:00:00Z", "run_budget": "all",
            "progress_current": 440, "progress_total": 14129,
            "sampled_identifier_count": 447439,
            "selected_suppliers": ["qogita", "qudo", "umma", "abw"],
            "filters": {},
        })
        self.registry.resource_pause(
            self.job_id, reason="disk_free_hard",
            metrics={"disk_free_bytes": 39 * GIB}, phase="pricing",
        )

    @staticmethod
    def admission(_suppliers, _universe, *, metrics):
        allowed = metrics["filesystem_free_bytes"] >= 44 * GIB
        return {
            "allowed": allowed,
            "reason": "admission_headroom_satisfied" if allowed else "post_run_floor_not_met",
        }

    def run_supervisor(self, *, metric_values, recovery, lifecycle_values=None, pid=9001):
        metric_values = iter(metric_values)
        lifecycle_values = iter(lifecycle_values or [lifecycle_metrics()] * 4)
        with (
            patch("discovery_jobs.subprocess.Popen", return_value=Mock(pid=pid)),
            patch("discovery_jobs.DEFAULT_LOG_DIR", self.root / "logs"),
        ):
            return run_once(
                registry=self.registry,
                store=Mock(path=self.root / "incremental.sqlite3"),
                governor=Mock(), observed_at=self.now,
                evaluator=lambda *_args, **_kwargs: (False, "not_provider"),
                storage_recovery=recovery,
                metrics_collector=lambda: next(metric_values),
                admission_evaluator=self.admission,
                lifecycle_metrics_collector=lambda: next(lifecycle_values),
            )

    def test_resource_pause_recovers_and_resumes_same_job_checkpoint(self):
        recovery = Mock(return_value={"status": "COMPLETED", "recovered": True})
        result = self.run_supervisor(
            metric_values=[metrics(39 * GIB), metrics(48 * GIB)], recovery=recovery,
        )
        self.assertEqual(result["action"], "launched")
        self.assertEqual(result["job_id"], self.job_id)
        self.assertEqual(result["reason"], "storage_recovered")
        runtime = self.registry.get(self.job_id)
        self.assertEqual(runtime["phase"], "pricing")
        self.assertEqual(runtime["progress_current"], 440)
        self.assertEqual(runtime["progress_total"], 14129)
        self.assertEqual(len(self.registry.recent(10)), 1)
        recovery.assert_called_once_with()

    def test_admission_blocked_recovers_without_recreating_job(self):
        self.registry.admission_blocked(
            self.job_id, decision={"reason": "post_run_floor_not_met"},
        )
        result = self.run_supervisor(
            metric_values=[metrics(35 * GIB), metrics(49 * GIB)],
            recovery=Mock(return_value={"status": "COMPLETED"}),
        )
        self.assertEqual((result["action"], result["job_id"]), (
            "launched", self.job_id,
        ))
        self.assertEqual(len(self.registry.recent(10)), 1)

    def test_no_reclaimable_space_is_persistently_suppressed_until_state_changes(self):
        recovery = Mock(return_value={"status": "COMPLETED", "recovered": False})
        first = self.run_supervisor(
            metric_values=[metrics(35 * GIB), metrics(35 * GIB)], recovery=recovery,
            lifecycle_values=[lifecycle_metrics(backlog=0, reclaimable=0)] * 2,
        )
        self.assertEqual(first["reason"], "storage_recovery_insufficient")
        runtime = self.registry.get(self.job_id)
        self.assertTrue(runtime["storage_recovery_outcome"].startswith("blocked:"))
        second = self.run_supervisor(
            metric_values=[metrics(35 * GIB)], recovery=recovery,
            lifecycle_values=[lifecycle_metrics(backlog=0, reclaimable=0)],
        )
        self.assertEqual(second["reason"], "storage_unchanged_since_failed_recovery")
        recovery.assert_called_once_with()

    def test_interrupted_recovery_is_retried_only_after_reclaim_state_changes(self):
        first = self.run_supervisor(
            metric_values=[metrics(35 * GIB), metrics(35 * GIB)],
            recovery=Mock(side_effect=RuntimeError("interrupted")),
            lifecycle_values=[lifecycle_metrics(backlog=1)] * 2,
        )
        self.assertEqual(first["reason"], "storage_recovery_insufficient")
        recovery = Mock(return_value={"status": "COMPLETED", "recovered": True})
        second = self.run_supervisor(
            metric_values=[
                metrics(35 * GIB, file_bytes=1200),
                metrics(49 * GIB, file_bytes=600),
            ],
            recovery=recovery,
            lifecycle_values=[
                lifecycle_metrics(backlog=2), lifecycle_metrics(backlog=0, reclaimable=0),
            ],
            pid=9002,
        )
        self.assertEqual(second["action"], "launched")
        recovery.assert_called_once_with()

    def test_storage_launch_claim_is_atomic_and_allows_only_one_worker(self):
        barrier = threading.Barrier(2)
        outcomes = []

        def launch():
            barrier.wait()
            try:
                with (
                    patch("discovery_jobs.subprocess.Popen", return_value=Mock(pid=9100)),
                    patch("discovery_jobs.DEFAULT_LOG_DIR", self.root / "logs"),
                ):
                    self.registry.launch(
                        self.job_id, storage_resume=True, observed_at=self.now,
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

    def test_live_pid_blocks_storage_resume(self):
        with self.registry._connect() as connection:
            connection.execute(
                "UPDATE discovery_job_runtime SET worker_pid=? WHERE job_id=?",
                (os.getpid(), self.job_id),
            )
            connection.commit()
        with patch("discovery_jobs.process_alive", return_value=True):
            with self.assertRaisesRegex(RuntimeError, "storage resume"):
                self.registry.launch(
                    self.job_id, storage_resume=True, observed_at=self.now,
                )


class CompactionWorkerExclusionTests(unittest.TestCase):
    def test_compaction_is_forbidden_when_worker_is_alive(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        lifecycle = DiscoveryArchiveLifecycle(
            database=root / "incremental.sqlite3",
            runtime_database=root / "jobs.sqlite3",
            rotation_database=root / "rotation.sqlite3",
            state_path=root / "state.json", mount_path=root / "x9",
            archive_root=root / "x9/archive", staging_root=root / "x9/staging",
        )
        before = lifecycle_metrics(reclaimable=3 * GIB)
        before["internal_free_bytes"] = 30 * GIB
        with (
            patch.object(lifecycle, "metrics", return_value=before),
            patch.object(lifecycle, "compaction_due", return_value=True),
            patch("discovery_archive_lifecycle._runtime_rows", return_value={
                "active": {"status": "resource_paused", "worker_pid": 1234},
            }),
            patch("discovery_archive_lifecycle.process_alive", return_value=True),
        ):
            result = lifecycle.compact()
        self.assertEqual(result["status"], "BLOCKED_ACTIVE_DISCOVERY")
        self.assertEqual(result["active"], ["active"])


if __name__ == "__main__":
    unittest.main()
