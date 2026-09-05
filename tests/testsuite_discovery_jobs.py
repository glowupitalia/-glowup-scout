import json
import os
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from streamlit.testing.v1 import AppTest

from discovery import DiscoveryCheckpointStore, default_filters
from discovery_jobs import DiscoveryJobRegistry, reconcile_discovery_state
from notifications import DEFAULT_RECIPIENT
from storage_maintenance import MaintenanceLockUnavailable, StorageMaintenanceLock
from supplier_catalog import SupplierCatalogStore


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

    def test_heartbeat_and_lease_cover_every_long_running_phase(self):
        state = self.state()
        self.registry.register_checkpoint(state)
        self.assertTrue(self.registry.claim(state["job_id"], pid=os.getpid()))
        phases = (
            "preparing", "preparing_cache", "catalog", "pricing",
            "competition", "fees", "economics",
        )
        for index, phase in enumerate(phases, start=1):
            self.registry.heartbeat(
                state["job_id"], pid=os.getpid(), phase=phase,
                current=index, total=len(phases),
            )
            runtime = self.registry.get(state["job_id"])
            self.assertEqual(runtime["phase"], phase)
            self.assertEqual(runtime["worker_pid"], os.getpid())
            self.assertIsNotNone(runtime["lease_expires_at"])
            self.assertIsNotNone(runtime["phase_started_at"])

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

    def test_stale_lease_with_live_pid_never_allows_second_worker(self):
        state = self.state()
        self.registry.register_checkpoint(state)
        with self.registry._connect() as connection:
            connection.execute(
                "UPDATE discovery_job_runtime SET status='running',worker_pid=?,"
                "lease_expires_at='2000-01-01T00:00:00Z' WHERE job_id=?",
                (os.getpid(), state["job_id"]),
            )
            connection.commit()
        self.registry.reconcile()
        runtime = self.registry.get(state["job_id"])
        self.assertEqual(runtime["status"], "running")
        self.assertEqual(runtime["worker_pid"], os.getpid())
        self.assertFalse(
            self.registry.claim(state["job_id"], pid=os.getpid() + 100_000)
        )

    def test_authoritative_completed_state_overrides_legacy_running(self):
        state = reconcile_discovery_state(
            legacy={
                "job_id": "qudo", "status": "running", "phase": "initialized",
                "progress_current": 0, "progress_total": 3902,
                "updated_at": "2026-08-31T15:54:56Z",
            },
            compact={
                "job_id": "qudo", "status": "completed", "phase": "completed",
                "progress_current": 3902, "progress_total": 3902,
                "updated_at": "2026-08-31T16:30:00Z",
            },
            registry={
                "job_id": "qudo", "status": "completed", "phase": "completed",
                "progress_current": 3902, "progress_total": 3902,
                "resumable": False, "updated_at": "2026-08-31T16:30:01Z",
            },
        )
        self.assertEqual(state["status"], "completed")
        self.assertEqual(state["phase"], "completed")
        self.assertEqual(state["progress_current"], 3902)
        self.assertFalse(state["resumable"])
        self.assertEqual(state["state_source"], "registry")

    def test_real_resumable_state_remains_resumable_without_live_worker(self):
        state = reconcile_discovery_state(
            legacy={"status": "running", "phase": "initialized"},
            incremental={
                "status": "running", "phase": "catalog", "selected_count": 3902,
                "updated_at": "2026-08-31T16:00:00Z",
            },
            registry={
                "status": "resumable", "phase": "catalog", "resumable": True,
                "worker_pid": None, "updated_at": "2026-08-31T16:01:00Z",
            },
        )
        self.assertEqual(state["status"], "resumable")
        self.assertTrue(state["resumable"])

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

    def test_registration_fails_closed_while_retention_is_exclusive(self):
        state = self.state()
        lock = StorageMaintenanceLock(self.root / "discovery-maintenance.lock")
        with lock.retention_apply_guard():
            with self.assertRaises(MaintenanceLockUnavailable):
                self.registry.register_checkpoint(state, maintenance_lock=lock)
        self.assertIsNone(self.registry.get(state["job_id"]))

    def test_launch_is_retry_safe_when_retention_is_exclusive(self):
        state = self.state()
        lock = StorageMaintenanceLock(self.root / "discovery-maintenance.lock")
        self.registry.register_checkpoint(state, maintenance_lock=lock)
        with lock.retention_apply_guard():
            with self.assertRaisesRegex(RuntimeError, "blocked by storage maintenance"):
                self.registry.launch(state["job_id"], maintenance_lock=lock)
        runtime = self.registry.get(state["job_id"])
        self.assertEqual(runtime["status"], "maintenance_blocked")
        self.assertTrue(runtime["resumable"])
        self.assertIsNone(runtime["worker_pid"])

    def test_two_simultaneous_worker_claims_have_one_winner(self):
        state = self.state()
        lock = StorageMaintenanceLock(self.root / "discovery-maintenance.lock")
        self.registry.register_checkpoint(state, maintenance_lock=lock)
        barrier = threading.Barrier(2)
        outcomes = []

        def claim():
            barrier.wait()
            with lock.discovery_start_guard():
                outcomes.append(self.registry.claim(state["job_id"], pid=os.getpid()))

        workers = [threading.Thread(target=claim) for _ in range(2)]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(2)
        self.assertEqual(sorted(outcomes), [False, True])


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
            self.assertTrue(any(row.value == "DISCOVERY IN CORSO" for row in app.subheader))
            labels = [row.label for row in app.button]
            self.assertIn("Apri avanzamento", labels)
            self.assertNotIn("Apri Discovery", labels)

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

    def test_completed_qudo_job_overrides_stale_legacy_after_new_browser_session(self):
        with tempfile.TemporaryDirectory() as temporary:
            checkpoint_root = Path(temporary) / "checkpoints"
            checkpoint_root.mkdir()
            registry = self._running_registry(temporary)
            state = {
                "job_id": "ui-running-job", "status": "completed", "phase": "completed",
                "started_at": "2026-08-27T09:20:27Z",
                "completed_at": "2026-08-27T10:20:27Z", "run_budget": "all",
                "sampled_identifier_count": 3902, "progress_current": 3902,
                "progress_total": 3902, "selected_suppliers": ["qudo"],
                "filters": default_filters(), "errors": [],
            }
            registry.finish(state["job_id"], state)
            (checkpoint_root / "ui-running-job.state.json").write_text(json.dumps({
                **state, "selected_count": 3902, "final_opportunity_count": 11,
                "fee_target_count": 142, "fee_valid_count": 142,
                "fee_unavailable_count": 0,
            }), encoding="utf-8")
            (checkpoint_root / "ui-running-job.json").write_text(json.dumps({
                **state, "status": "running", "phase": "initialized",
                "progress_current": 0, "progress_total": 3902,
                "updated_at": "2026-08-27T09:20:28Z",
            }), encoding="utf-8")
            with patch.dict(os.environ, {
                "DISCOVERY_JOB_DATABASE": str(registry.path),
                "DISCOVERY_CHECKPOINT_ROOT": str(checkpoint_root),
                "DISCOVERY_INCREMENTAL_DATABASE": str(
                    Path(temporary) / "incremental.sqlite3"
                ),
            }):
                app = AppTest.from_file("app_glowup.py", default_timeout=20).run()
            self.assertEqual(len(app.exception), 0)
            self.assertTrue(any(row.value == "DISCOVERY COMPLETATA" for row in app.subheader))
            labels = [row.label for row in app.button]
            self.assertIn("Visualizza risultati / Scarica Excel", labels)
            self.assertIn("Nuova ricerca", labels)
            self.assertNotIn("Riprendi Discovery", labels)
            rendered = " ".join(row.value for row in app.caption)
            self.assertIn("11 opportunità", rendered)
            self.assertIn("100.00%", rendered)
            self.assertTrue(any(row.value == "Discovery recenti" for row in app.subheader))

    def test_completed_home_defers_result_hydration_until_results_are_opened(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            checkpoint_root = root / "checkpoints"
            checkpoint_root.mkdir()
            registry = self._running_registry(temporary)
            export_path = root / "operational.xlsx"
            export_path.write_bytes(b"operational workbook")
            state = {
                "job_id": "ui-running-job", "status": "completed",
                "phase": "completed", "started_at": "2026-08-27T09:20:27Z",
                "completed_at": "2026-08-27T10:20:27Z", "run_budget": "all",
                "sampled_identifier_count": 3902, "selected_count": 3902,
                "progress_current": 3902, "progress_total": 3902,
                "selected_suppliers": ["qudo"], "filters": default_filters(),
                "final_opportunity_count": 11, "fee_target_count": 142,
                "fee_valid_count": 142, "fee_unavailable_count": 0,
                "retention_mode": "final_only", "exact_replay_capable": False,
                "operational_export": {
                    "path": str(export_path), "file_name": export_path.name,
                },
                "discovery_schema_version": "supplier_multi_listing_v1",
                "persistence": "incremental_sqlite_v1", "errors": [],
                "results": [],
            }
            registry.finish(
                state["job_id"], state, export_path=str(export_path),
            )
            (checkpoint_root / "ui-running-job.state.json").write_text(
                json.dumps(state), encoding="utf-8",
            )
            with patch.dict(os.environ, {
                "DISCOVERY_JOB_DATABASE": str(registry.path),
                "DISCOVERY_CHECKPOINT_ROOT": str(checkpoint_root),
                "DISCOVERY_INCREMENTAL_DATABASE": str(root / "incremental.sqlite3"),
            }), patch.object(
                DiscoveryCheckpointStore, "load", autospec=True,
                return_value=state,
            ) as hydrate:
                app = AppTest.from_file("app_glowup.py", default_timeout=20).run()
                self.assertEqual(hydrate.call_count, 0)
                rendered = " ".join(row.value for row in app.caption)
                self.assertIn("11 opportunità", rendered)
                self.assertIn("3.902 prodotti valutati", rendered)
                self.assertIn("Storico compatto", rendered)
                open_results = next(
                    row for row in app.button
                    if row.label == "Visualizza risultati / Scarica Excel"
                )
                app = open_results.click().run()
                if not app.subheader:
                    app = app.run()

            self.assertEqual(hydrate.call_count, 1)
            self.assertEqual(len(app.exception), 0)
            self.assertTrue(any(
                row.value == "DISCOVERY COMPLETATA" for row in app.subheader
            ))
            self.assertIn(
                "SCARICA EXCEL",
                [row.proto.label for row in app.get("download_button")],
            )

    def test_no_job_home_offers_discovery_start(self):
        with tempfile.TemporaryDirectory() as temporary, patch.dict(
            os.environ,
            {"DISCOVERY_JOB_DATABASE": str(Path(temporary) / "runtime.sqlite3")},
        ):
            app = AppTest.from_file("app_glowup.py", default_timeout=20).run()
        self.assertEqual(len(app.exception), 0)
        self.assertIn("Apri Discovery", [row.label for row in app.button])

    def test_storage_admission_block_is_distinct_from_discovery_failure(self):
        with tempfile.TemporaryDirectory() as temporary:
            registry = DiscoveryJobRegistry(Path(temporary) / "runtime.sqlite3")
            registry.register_checkpoint({
                "job_id": "storage-blocked", "phase": "initialized",
                "selected_suppliers": ["qogita"], "filters": default_filters(),
            })
            registry.admission_blocked("storage-blocked", decision={
                "watermark": {
                    "state": "PRESSURE", "filesystem_free_bytes": 30 * 1024 ** 3,
                },
                "reason": "post_run_floor_not_met",
            })
            with patch.dict(os.environ, {
                "DISCOVERY_JOB_DATABASE": str(registry.path),
            }):
                app = AppTest.from_file("app_glowup.py", default_timeout=20).run()
            self.assertEqual(len(app.exception), 0)
            self.assertIn(
                "WORKLOAD BLOCCATO PER SPAZIO",
                [row.value for row in app.subheader],
            )
            self.assertNotIn("DISCOVERY COMPLETATA", [row.value for row in app.subheader])

    def test_discovery_configuration_shows_notification_status(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            registry = DiscoveryJobRegistry(root / "runtime.sqlite3")
            catalog_path = root / "catalog.sqlite3"
            original_init = SupplierCatalogStore.__init__

            def isolated_catalog_init(instance, path=None):
                original_init(instance, path or catalog_path)

            SupplierCatalogStore(catalog_path).initialize()
            with patch.dict(
                os.environ,
                {
                    "DISCOVERY_JOB_DATABASE": str(registry.path),
                    "DISCOVERY_ROTATION_DATABASE": str(root / "rotation.sqlite3"),
                    "GLOWUP_SCOUT_EMAIL_ENABLED": "false",
                },
                clear=False,
            ), patch.object(SupplierCatalogStore, "__init__", isolated_catalog_init):
                app = AppTest.from_file("app_glowup.py", default_timeout=20).run()
                app.session_state["ui_state"] = "discovery"
                app = app.run()
            self.assertEqual(len(app.exception), 0)
            self.assertTrue(any(
                "Notifiche email: disattive" in row.value
                and DEFAULT_RECIPIENT in row.value
                for row in app.caption
            ))


if __name__ == "__main__":
    unittest.main()
