import multiprocessing
import os
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path

from storage_maintenance import MaintenanceLockUnavailable, StorageMaintenanceLock


def _contend_lock(lock_path, mode, start, result):
    lock = StorageMaintenanceLock(lock_path)
    start.wait(2)
    try:
        guard = (
            lock.retention_apply_guard()
            if mode == "retention"
            else lock.discovery_start_guard()
        )
        with guard:
            result.put((mode, "acquired"))
            time.sleep(0.2)
    except MaintenanceLockUnavailable:
        result.put((mode, "blocked"))


def _crash_with_lock(lock_path, ready):
    with StorageMaintenanceLock(lock_path).retention_apply_guard():
        ready.set()
        os._exit(17)


def _crash_during_transaction(lock_path, database_path, ready):
    with StorageMaintenanceLock(lock_path).retention_apply_guard():
        connection = sqlite3.connect(database_path)
        connection.execute("BEGIN IMMEDIATE")
        connection.execute("DELETE FROM sample")
        ready.set()
        os._exit(19)


class StorageMaintenanceLockTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.lock_path = self.root / "maintenance.lock"
        self.lock = StorageMaintenanceLock(self.lock_path)

    def tearDown(self):
        self.temporary.cleanup()

    def test_retention_excludes_discovery_and_release_allows_start(self):
        with self.lock.retention_apply_guard():
            with self.assertRaises(MaintenanceLockUnavailable):
                with self.lock.discovery_start_guard():
                    self.fail("Discovery acquired an exclusive retention lock")
        with self.lock.discovery_start_guard():
            pass

    def test_discovery_excludes_retention_and_release_allows_apply(self):
        with self.lock.discovery_start_guard():
            with self.assertRaises(MaintenanceLockUnavailable):
                with self.lock.retention_apply_guard():
                    self.fail("Retention acquired while Discovery was claiming")
        with self.lock.retention_apply_guard():
            pass

    def test_two_retention_executors_allow_only_one_apply(self):
        first = StorageMaintenanceLock(self.lock_path)
        second = StorageMaintenanceLock(self.lock_path)
        with first.retention_apply_guard():
            with self.assertRaises(MaintenanceLockUnavailable):
                with second.retention_apply_guard():
                    self.fail("A second executor acquired the exclusive lock")

    def test_same_instant_race_has_one_winner(self):
        context = multiprocessing.get_context("fork")
        start = context.Event()
        result = context.Queue()
        processes = [
            context.Process(
                target=_contend_lock,
                args=(str(self.lock_path), mode, start, result),
            )
            for mode in ("retention", "discovery")
        ]
        for process in processes:
            process.start()
        start.set()
        outcomes = [result.get(timeout=3) for _ in processes]
        for process in processes:
            process.join(3)
        self.assertEqual(
            sorted(status for _, status in outcomes), ["acquired", "blocked"],
        )

    def test_dead_process_releases_kernel_lock(self):
        context = multiprocessing.get_context("fork")
        ready = context.Event()
        process = context.Process(
            target=_crash_with_lock, args=(str(self.lock_path), ready),
        )
        process.start()
        self.assertTrue(ready.wait(timeout=3))
        process.join(3)
        self.assertEqual(process.exitcode, 17)
        with self.lock.retention_apply_guard():
            pass

    def test_crash_rolls_back_sqlite_and_releases_lock(self):
        database = self.root / "transaction.sqlite3"
        with sqlite3.connect(database) as connection:
            connection.execute("CREATE TABLE sample(value INTEGER)")
            connection.executemany("INSERT INTO sample VALUES(?)", ((1,), (2,)))
        context = multiprocessing.get_context("fork")
        ready = context.Event()
        process = context.Process(
            target=_crash_during_transaction,
            args=(str(self.lock_path), str(database), ready),
        )
        process.start()
        self.assertTrue(ready.wait(timeout=3))
        process.join(3)
        self.assertEqual(process.exitcode, 19)
        with sqlite3.connect(database) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM sample").fetchone()[0], 2)
        with self.lock.retention_apply_guard():
            pass


if __name__ == "__main__":
    unittest.main()
