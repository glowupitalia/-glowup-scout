import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from streamlit.testing.v1 import AppTest

from discovery import DiscoveryCheckpointStore, default_filters
from discovery_jobs import DiscoveryJobRegistry


class DiscoveryJobRegistryTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.registry = DiscoveryJobRegistry(self.root / "jobs.sqlite3")
        self.checkpoints = DiscoveryCheckpointStore(self.root / "checkpoints")

    def tearDown(self):
        self.temporary.cleanup()

    def state(self, job_id=None):
        state = self.checkpoints.create(default_filters())
        if job_id is not None:
            original = self.checkpoints.path(state["job_id"])
            state["job_id"] = job_id
            original.unlink()
        state.update({
            "selected_suppliers": ["abw", "umma", "qudo"],
            "run_budget": 5000,
            "sampled_identifier_count": 5000,
            "rotation_selected_identifiers": [f"ean-{index}" for index in range(5)],
        })
        self.checkpoints.save(state)
        return state

    def test_browser_session_recreation_detects_running_job(self):
        state = self.state()
        self.registry.register_checkpoint(state)
        self.assertTrue(self.registry.claim(state["job_id"], pid=os.getpid()))
        reopened = DiscoveryJobRegistry(self.registry.path)
        active = reopened.latest_active()
        self.assertEqual(active["job_id"], state["job_id"])
        self.assertEqual(active["status"], "running")

    def test_running_job_cannot_be_claimed_or_resumed_twice(self):
        state = self.state()
        self.registry.register_checkpoint(state)
        self.assertTrue(self.registry.claim(state["job_id"], pid=os.getpid()))
        self.assertFalse(self.registry.claim(state["job_id"], pid=os.getpid() + 1))

    def test_progress_is_persisted(self):
        state = self.state()
        self.registry.register_checkpoint(state)
        self.registry.claim(state["job_id"], pid=os.getpid())
        self.registry.heartbeat(
            state["job_id"], pid=os.getpid(), phase="catalog", current=320, total=5000,
        )
        status = DiscoveryJobRegistry(self.registry.path).get(state["job_id"])
        self.assertEqual((status["phase"], status["progress_current"], status["progress_total"]), ("catalog", 320, 5000))

    def test_dead_running_job_becomes_resumable(self):
        state = self.state()
        self.registry.register_checkpoint(state)
        with self.registry._connect() as connection:
            connection.execute(
                "UPDATE discovery_job_runtime SET status='running',worker_pid=999999999,lease_expires_at='2000-01-01T00:00:00Z' WHERE job_id=?",
                (state["job_id"],),
            )
        self.registry.reconcile()
        status = self.registry.get(state["job_id"])
        self.assertEqual(status["status"], "resumable")
        self.assertTrue(status["resumable"])

    def test_resume_preserves_same_job_and_selection(self):
        state = self.state("same-job")
        selected = list(state["rotation_selected_identifiers"])
        self.registry.register_checkpoint(state)
        self.registry.claim(state["job_id"], pid=os.getpid())
        state["status"] = "waiting_retry"
        state["phase"] = "fees_pending"
        self.registry.finish(state["job_id"], state)
        self.assertEqual(self.registry.get(state["job_id"])["status"], "resumable")
        reloaded = self.checkpoints.load("same-job")
        self.assertEqual(reloaded["rotation_selected_identifiers"], selected)

    def test_completed_job_is_not_resumable(self):
        state = self.state()
        self.registry.register_checkpoint(state)
        self.registry.claim(state["job_id"], pid=os.getpid())
        state.update({"status": "completed", "phase": "completed", "completed_at": "2026-08-27T10:00:00Z"})
        self.registry.finish(state["job_id"], state)
        status = self.registry.get(state["job_id"])
        self.assertEqual(status["status"], "completed")
        self.assertFalse(status["resumable"])

    def test_launch_is_detached_and_duplicate_launch_is_rejected(self):
        state = self.state()
        self.registry.register_checkpoint(state)
        process = Mock(pid=4242)
        with patch("discovery_jobs.subprocess.Popen", return_value=process) as popen:
            pid = self.registry.launch(state["job_id"])
            self.assertEqual(pid, 4242)
            self.assertTrue(popen.call_args.kwargs["start_new_session"])
            with self.assertRaisesRegex(RuntimeError, "already"):
                self.registry.launch(state["job_id"])


class DiscoveryProgressTests(unittest.TestCase):
    def test_catalog_progress_survives_checkpoint(self):
        from discovery import run_discovery

        with tempfile.TemporaryDirectory() as temporary:
            store = DiscoveryCheckpointStore(temporary)
            states = []
            candidate = {
                "product_key": "ean|8809562191179", "canonical_ean": "8809562191179",
                "gtin": "8809562191179", "identifier_type": "EAN", "scenarios": [],
            }

            def preparer(*_args, **_kwargs):
                return {
                    "selected_suppliers": ["abw"], "supplier_snapshot_set": {"abw": {}},
                    "supplier_warnings": [], "coverage": {"products_by_supplier": {"abw": 1}, "scenarios_by_supplier": {"abw": 0}, "unique_eans": 1, "shared_eans": 0},
                    "supplier_diagnostics": {}, "usable_suppliers": ["abw"], "candidates": [candidate],
                    "total_supplier_ean_universe": 1, "eligible_identifier_count": 1,
                    "run_budget": 1, "sampled_identifier_count": 1,
                    "sampling_strategy": "test", "rotation_selected_identifiers": ["8809562191179"],
                }

            result = run_discovery(
                default_filters(), checkpoint_store=store,
                catalog_batch=lambda *_args: {"8809562191179": {"status": "not_found"}},
                pricing_batch=lambda *_args: {}, fees_batch=lambda *_args: [],
                token_provider=Mock(), selected_suppliers=["abw"], run_budget=1,
                supplier_preparer=preparer, progress=lambda phase, state: states.append((phase, state.get("progress_current"), state.get("progress_total"))),
                sleep_func=lambda *_args: None,
            )
            self.assertIn(("catalog", 1, 1), states)
            self.assertEqual(result["status"], "completed")


class DiscoveryJobUiTests(unittest.TestCase):
    def _running_registry(self, root):
        registry = DiscoveryJobRegistry(Path(root) / "runtime.sqlite3")
        state = {
            "job_id": "ui-running-job", "status": "running", "phase": "catalog",
            "started_at": "2026-08-27T09:20:27Z", "completed_at": None,
            "run_budget": 5000, "sampled_identifier_count": 5000,
            "progress_current": 1200, "progress_total": 5000,
            "selected_suppliers": ["abw", "umma", "qudo"],
            "filters": default_filters(),
        }
        registry.register_checkpoint(state)
        registry.claim(state["job_id"], pid=os.getpid())
        return registry

    def test_home_detects_server_side_running_job(self):
        with tempfile.TemporaryDirectory() as temporary:
            registry = self._running_registry(temporary)
            with patch.dict(os.environ, {"DISCOVERY_JOB_DATABASE": str(registry.path)}):
                app = AppTest.from_file("app_glowup.py", default_timeout=20).run()
            self.assertEqual(len(app.exception), 0)
            self.assertTrue(any(row.value == "Discovery in corso" for row in app.subheader))
            self.assertTrue(any(row.label == "Apri Discovery" for row in app.button))

    def test_running_job_shows_open_progress_and_not_resume(self):
        with tempfile.TemporaryDirectory() as temporary:
            registry = self._running_registry(temporary)
            with patch.dict(os.environ, {"DISCOVERY_JOB_DATABASE": str(registry.path)}):
                app = AppTest.from_file("app_glowup.py", default_timeout=20).run()
                app.session_state["ui_state"] = "discovery"
                app = app.run()
            labels = [row.label for row in app.button]
            self.assertIn("Apri avanzamento", labels)
            self.assertNotIn("Riprendi ultima Discovery incompleta", labels)

    def test_completed_job_is_discoverable_after_new_browser_session(self):
        with tempfile.TemporaryDirectory() as temporary:
            registry = self._running_registry(temporary)
            state = {
                "job_id": "ui-running-job", "status": "completed", "phase": "completed",
                "started_at": "2026-08-27T09:20:27Z",
                "completed_at": "2026-08-27T10:20:27Z", "run_budget": 5000,
                "sampled_identifier_count": 5000, "progress_current": 5000,
                "progress_total": 5000, "selected_suppliers": ["abw", "umma", "qudo"],
                "filters": default_filters(), "errors": [],
            }
            registry.finish(state["job_id"], state)
            with patch.dict(os.environ, {"DISCOVERY_JOB_DATABASE": str(registry.path)}):
                app = AppTest.from_file("app_glowup.py", default_timeout=20).run()
            self.assertEqual(len(app.exception), 0)
            self.assertTrue(any(row.value == "Ultima Discovery completata" for row in app.subheader))
            self.assertTrue(any(row.label == "Apri risultati Discovery" for row in app.button))


if __name__ == "__main__":
    unittest.main()
