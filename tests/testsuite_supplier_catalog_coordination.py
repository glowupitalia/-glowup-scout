import io
import multiprocessing
import tempfile
import time
import unittest
from contextlib import contextmanager, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from supplier_catalog_coordination import (
    SupplierCatalogCoordinationTimeout,
    supplier_catalog_writer_lock,
    weekly_intent_active,
    weekly_supplier_catalog_intent,
)
from weekly_supplier_sync import main as weekly_main


def _hold_writer(path, ready, release):
    with supplier_catalog_writer_lock(path, timeout_seconds=1):
        ready.set()
        release.wait(10)


def _hold_intent(path, ready, release):
    with weekly_supplier_catalog_intent(path):
        ready.set()
        release.wait(10)


def _qogita_handoff(writer_path, intent_path, active, drained, release_item):
    with supplier_catalog_writer_lock(writer_path, timeout_seconds=1):
        active.set()
        while not weekly_intent_active(intent_path):
            time.sleep(0.005)
        release_item.wait(10)
        drained.set()


class SupplierCatalogCoordinationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        root = Path(self.temporary.name)
        self.writer = root / "supplier-catalog-writer.lock"
        self.intent = root / "supplier-catalog-weekly-intent.lock"
        self.context = multiprocessing.get_context("fork")

    def tearDown(self):
        self.temporary.cleanup()

    def test_writer_lock_is_exclusive_and_bounded(self):
        ready, release = self.context.Event(), self.context.Event()
        process = self.context.Process(target=_hold_writer, args=(self.writer, ready, release))
        process.start()
        self.assertTrue(ready.wait(2))
        with self.assertRaises(SupplierCatalogCoordinationTimeout):
            with supplier_catalog_writer_lock(self.writer, timeout_seconds=0.03, poll_seconds=0.005):
                self.fail("second writer acquired the lock")
        release.set()
        process.join(2)
        self.assertEqual(process.exitcode, 0)

    def test_weekly_intent_is_live_ownership_not_file_existence(self):
        self.intent.parent.mkdir(parents=True, exist_ok=True)
        self.intent.touch()
        self.assertFalse(weekly_intent_active(self.intent))
        ready, release = self.context.Event(), self.context.Event()
        process = self.context.Process(target=_hold_intent, args=(self.intent, ready, release))
        process.start()
        self.assertTrue(ready.wait(2))
        self.assertTrue(weekly_intent_active(self.intent))
        release.set()
        process.join(2)
        self.assertFalse(weekly_intent_active(self.intent))

    def test_crashed_owner_releases_writer_lock(self):
        ready, release = self.context.Event(), self.context.Event()
        process = self.context.Process(target=_hold_writer, args=(self.writer, ready, release))
        process.start()
        self.assertTrue(ready.wait(2))
        process.terminate()
        process.join(2)
        with supplier_catalog_writer_lock(self.writer, timeout_seconds=0.2):
            pass

    def test_weekly_intent_drains_inflight_then_gets_writer(self):
        active = self.context.Event()
        drained = self.context.Event()
        release_item = self.context.Event()
        process = self.context.Process(
            target=_qogita_handoff,
            args=(self.writer, self.intent, active, drained, release_item),
        )
        process.start()
        self.assertTrue(active.wait(2))
        with weekly_supplier_catalog_intent(self.intent):
            self.assertTrue(weekly_intent_active(self.intent))
            release_item.set()
            self.assertTrue(drained.wait(2))
            with supplier_catalog_writer_lock(self.writer, timeout_seconds=1):
                pass
        process.join(2)
        self.assertEqual(process.exitcode, 0)

    def test_weekly_in_rest_gets_writer_immediately(self):
        with weekly_supplier_catalog_intent(self.intent):
            with supplier_catalog_writer_lock(self.writer, timeout_seconds=0.1):
                self.assertTrue(weekly_intent_active(self.intent))

    def test_long_weekly_prevents_qogita_window_start(self):
        with weekly_supplier_catalog_intent(self.intent):
            with supplier_catalog_writer_lock(self.writer, timeout_seconds=0.1):
                self.assertTrue(weekly_intent_active(self.intent))
                with self.assertRaises(SupplierCatalogCoordinationTimeout):
                    with supplier_catalog_writer_lock(
                        self.writer, timeout_seconds=0.02, poll_seconds=0.005,
                    ):
                        pass
        self.assertFalse(weekly_intent_active(self.intent))

    def test_exception_releases_both_weekly_locks(self):
        with self.assertRaisesRegex(RuntimeError, "boom"):
            with weekly_supplier_catalog_intent(self.intent):
                with supplier_catalog_writer_lock(self.writer, timeout_seconds=0.1):
                    raise RuntimeError("boom")
        self.assertFalse(weekly_intent_active(self.intent))
        with supplier_catalog_writer_lock(self.writer, timeout_seconds=0.1):
            pass

    def test_weekly_timeout_reports_distinct_state_without_running_pipeline(self):
        @contextmanager
        def available_role_lock():
            yield

        @contextmanager
        def available_intent():
            yield

        @contextmanager
        def blocked_writer(*args, **kwargs):
            raise SupplierCatalogCoordinationTimeout("blocked")
            yield

        output = io.StringIO()
        database = Path(self.temporary.name) / "weekly.sqlite3"
        with patch("weekly_supplier_sync.weekly_lock", available_role_lock), \
             patch("weekly_supplier_sync.weekly_supplier_catalog_intent", available_intent), \
             patch("weekly_supplier_sync.supplier_catalog_writer_lock", blocked_writer), \
             patch("weekly_supplier_sync._handlers") as handlers, redirect_stdout(output):
            code = weekly_main(["run", "--database", str(database)])
        self.assertEqual(code, 1)
        self.assertFalse(handlers.called)
        self.assertIn('"status": "coordination_blocked_supplier_catalog"', output.getvalue())
        self.assertIn('"baseline_preserved": true', output.getvalue())


if __name__ == "__main__":
    unittest.main()
