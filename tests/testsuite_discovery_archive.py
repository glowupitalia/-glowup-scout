import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from discovery_archive import (
    ARCHIVE_ROLE,
    DEFAULT_VOLUME_UUID,
    ArchiveIntegrityError,
    ArchiveStorageUnavailable,
    ArchivedJobReadOnlyError,
    create_job_archive,
    delete_internal_job_rows,
    job_row_counts,
    logical_job_checksum,
    register_archive_segment,
    remove_archive_registration,
    restore_job_from_archive,
    validate_archive_volume,
    archive_catalog_record,
)
from discovery_incremental import DiscoveryIncrementalStore
from discovery_archive_lifecycle import (
    DiscoveryArchiveLifecycle,
    plan_archive_candidates,
)
from storage_gc import discovery_gc_plan


class DiscoveryArchiveTierTests(unittest.TestCase):
    JOB_ID = "historical-job"
    CURRENT_JOB_ID = "current-job"

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.mount = self.root / "Glow Up Data"
        self.mount.mkdir()
        self.archive_root = self.mount / "Archive" / "Discovery"
        self.sentinel = self.mount / ".glowup-history-volume.json"
        self.sentinel.write_text(json.dumps({
            "contract_version": "1",
            "schema_version": 2,
            "volume_uuid": DEFAULT_VOLUME_UUID,
            "role": ARCHIVE_ROLE,
            "initialized_at": "2026-09-22T00:00:00Z",
        }), encoding="utf-8")
        self.mount_patch = mock.patch(
            "discovery_archive.os.path.ismount", return_value=True,
        )
        self.mount_patch.start()
        self.database = self.root / "discovery.sqlite3"
        self.store = DiscoveryIncrementalStore(
            self.database,
            archive_mount_path=self.mount,
            archive_root=self.archive_root,
            archive_volume_uuid=DEFAULT_VOLUME_UUID,
            archive_uuid_probe=lambda _: DEFAULT_VOLUME_UUID,
        )
        self.store.initialize()
        self._insert_job(self.JOB_ID, completed=True)
        self._insert_job(self.CURRENT_JOB_ID, completed=False)

    def tearDown(self):
        self.mount_patch.stop()
        self.temporary.cleanup()

    def _insert_job(self, job_id, *, completed):
        status = "completed" if completed else "running"
        phase = "completed" if completed else "catalog"
        metadata = {
            "job_id": job_id,
            "status": status,
            "phase": phase,
            "retention_mode": "full",
            "operational_export": {"path": f"/tmp/{job_id}.xlsx"},
        }
        with sqlite3.connect(self.database) as connection:
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute(
                """INSERT INTO discovery_incremental_jobs
                   (job_id,schema_version,status,phase,metadata_json,selected_count,
                    catalog_completed_count,last_completed_batch,
                    checkpoint_bytes_written,created_at,updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    job_id, 1, status, phase, json.dumps(metadata), 2,
                    2 if completed else 0, 1, 20,
                    "2026-09-01T00:00:00Z", "2026-09-01T01:00:00Z",
                ),
            )
            for sequence, identifier in enumerate(("001", "002")):
                product = {
                    "canonical_ean": identifier,
                    "title": f"Product {identifier}",
                    "is_final_result": sequence == 0,
                    "recommended_combination": {
                        "scenario_id": f"scenario-{identifier}", "score": 10 - sequence,
                    },
                }
                connection.execute(
                    """INSERT INTO discovery_job_items
                       (job_id,sequence_no,canonical_identifier,identifier_type,
                        product_json,catalog_status,pricing_status,fees_status,
                        terminal_status,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?)""",
                    (
                        job_id, sequence, identifier, "ean", json.dumps(product),
                        "resolved", "valid", "valid", "final",
                        "2026-09-01T01:00:00Z",
                    ),
                )
                connection.execute(
                    """INSERT INTO discovery_purchase_scenarios
                       VALUES (?,?,?,?,?)""",
                    (
                        job_id, identifier, f"scenario-{identifier}",
                        json.dumps({"scenario_id": f"scenario-{identifier}", "supplier": "abw"}),
                        "abw",
                    ),
                )
                connection.execute(
                    """INSERT INTO discovery_catalog_results
                       VALUES (?,?,?,?,?)""",
                    (job_id, identifier, "resolved", "{}", "2026-09-01T01:00:00Z"),
                )
                observation_id = f"observation-{identifier}"
                connection.execute(
                    """INSERT INTO discovery_listings VALUES (?,?,?,?,?)""",
                    (
                        job_id, identifier, f"ASIN{identifier}",
                        json.dumps({
                            "asin": f"ASIN{identifier}",
                            "amazon_observation_id": observation_id,
                        }),
                        "2026-09-01T01:00:00Z",
                    ),
                )
                connection.execute(
                    """INSERT INTO discovery_listing_classifications
                       VALUES (?,?,?,?,?,?,?,?,?,?)""",
                    (
                        job_id, identifier, f"ASIN{identifier}", "APJ6JRA9NG5V4",
                        f"path-{identifier}", "beauty", None, 0, "Beauty", 1,
                    ),
                )
                connection.execute(
                    """INSERT INTO discovery_observations VALUES (?,?,?,?)""",
                    (
                        job_id, observation_id,
                        json.dumps({
                            "observation_id": observation_id, "fee_status": "valid",
                            "reference_price": "10.00", "bsr_beauty": 100,
                        }),
                        "2026-09-01T01:00:00Z",
                    ),
                )
                connection.execute(
                    """INSERT INTO discovery_combinations VALUES (?,?,?,?,?)""",
                    (
                        job_id, f"combination-{identifier}", identifier,
                        json.dumps({
                            "combination_id": f"combination-{identifier}",
                            "amazon_observation_id": observation_id,
                        }),
                        "2026-09-01T01:00:00Z",
                    ),
                )
            connection.execute(
                """INSERT INTO discovery_resource_events
                   (job_id,level,reason,metrics_json,observed_at)
                   VALUES (?,?,?,?,?)""",
                (job_id, "info", "done", "{}", "2026-09-01T01:00:00Z"),
            )

    def _archive(self):
        return create_job_archive(
            source_path=self.database,
            job_id=self.JOB_ID,
            archive_root=self.archive_root,
            mount_path=self.mount,
            expected_uuid=DEFAULT_VOLUME_UUID,
            uuid_probe=lambda _: DEFAULT_VOLUME_UUID,
            batch_size=1,
        )

    def _delete(self):
        return delete_internal_job_rows(
            self.database, self.JOB_ID,
            mount_path=self.mount,
            archive_root=self.archive_root,
            uuid_probe=lambda _: DEFAULT_VOLUME_UUID,
        )

    def _register(self, archive):
        register_archive_segment(
            self.database, archive,
            mount_path=self.mount,
            archive_root=self.archive_root,
            uuid_probe=lambda _: DEFAULT_VOLUME_UUID,
        )

    def _reference_databases(self):
        runtime = self.root / "runtime.sqlite3"
        rotation = self.root / "rotation.sqlite3"
        historical_export = self.root / "historical.xlsx"
        current_export = self.root / "current.xlsx"
        historical_export.write_bytes(b"historical")
        current_export.write_bytes(b"current")
        with sqlite3.connect(runtime) as connection:
            connection.executescript("""
                CREATE TABLE discovery_job_runtime (
                    job_id TEXT PRIMARY KEY,status TEXT,phase TEXT,resumable INTEGER,
                    export_path TEXT,created_at TEXT,updated_at TEXT
                );
                CREATE TABLE notification_outbox (entity_id TEXT,status TEXT);
            """)
            connection.executemany(
                "INSERT INTO discovery_job_runtime VALUES (?,?,?,?,?,?,?)",
                [
                    (self.JOB_ID, "completed", "completed", 0, str(historical_export), "2026-09-01", "2026-09-01"),
                    (self.CURRENT_JOB_ID, "running", "catalog", 1, str(current_export), "2026-09-02", "2026-09-02"),
                ],
            )
        with sqlite3.connect(rotation) as connection:
            connection.executescript("""
                CREATE TABLE discovery_rotation_selections (job_id TEXT);
                CREATE TABLE discovery_rotation_global_history (last_job_id TEXT);
            """)
        with sqlite3.connect(self.database) as connection:
            connection.execute(
                "UPDATE discovery_incremental_jobs SET created_at='2026-09-01T00:00:00Z' WHERE job_id=?",
                (self.JOB_ID,),
            )
            connection.execute(
                "UPDATE discovery_incremental_jobs SET created_at='2026-09-02T00:00:00Z' WHERE job_id=?",
                (self.CURRENT_JOB_ID,),
            )
        return runtime, rotation

    def test_copy_equivalence_and_checksum(self):
        with sqlite3.connect(self.database) as source:
            source.row_factory = sqlite3.Row
            expected_counts = job_row_counts(source, self.JOB_ID)
            expected_checksum = logical_job_checksum(source, self.JOB_ID)
        archive = self._archive()
        self.assertTrue(archive["equivalent"])
        self.assertEqual(expected_counts, archive["row_counts"])
        self.assertEqual(expected_checksum, archive["content_sha256"])
        self.assertTrue(Path(archive["archive_path"]).is_file())

    def test_dual_reader_delete_and_export_are_equivalent(self):
        before_summary = self.store.summary(self.JOB_ID)
        before_counts = self.store.counts(self.JOB_ID)
        before_export = list(self.store.iter_export_candidates(self.JOB_ID))
        archive = self._archive()
        self._register(archive)
        with self.assertRaises(ArchivedJobReadOnlyError):
            self.store.update_job(self.JOB_ID, phase="changed")
        deleted = self._delete()
        self.assertEqual(archive["row_counts"], deleted)
        self.assertEqual(before_summary, self.store.summary(self.JOB_ID))
        self.assertEqual(before_counts, self.store.counts(self.JOB_ID))
        self.assertEqual(before_export, list(self.store.iter_export_candidates(self.JOB_ID)))
        with sqlite3.connect(self.database) as connection:
            self.assertEqual(
                0, connection.execute(
                    "SELECT COUNT(*) FROM discovery_job_items WHERE job_id=?", (self.JOB_ID,),
                ).fetchone()[0],
            )

    def test_archive_unavailable_does_not_break_current_job(self):
        archive = self._archive()
        self._register(archive)
        self._delete()
        with mock.patch("discovery_archive.os.path.ismount", return_value=False):
            with self.assertRaisesRegex(ArchiveStorageUnavailable, "archive_storage_unavailable"):
                self.store.summary(self.JOB_ID)
            self.assertEqual("running", self.store.summary(self.CURRENT_JOB_ID)["status"])

    def test_gc_is_archive_aware_after_internal_cleanup(self):
        runtime, rotation = self._reference_databases()
        archive = self._archive()
        self._register(archive)
        self._delete()
        plan = discovery_gc_plan(self.database, runtime, rotation)
        row = next(value for value in plan["jobs"] if value["job_id"] == self.JOB_ID)
        self.assertEqual("archive", row["authority"])
        self.assertEqual("ARCHIVED_AUTHORITATIVE", row["classification"])
        self.assertTrue(all(value["decision"] == "KEEP" for value in row["components"]))

    def test_automatic_planner_and_lifecycle_archive_only_historical(self):
        runtime, rotation = self._reference_databases()
        plan = plan_archive_candidates(
            database=self.database, runtime_database=runtime, rotation_database=rotation,
        )
        self.assertEqual([self.JOB_ID], [value["job_id"] for value in plan["candidates"]])
        current = next(value for value in plan["blocked"] if value["job_id"] == self.CURRENT_JOB_ID)
        self.assertIn("current_latest_job", current["blockers"])
        lifecycle = DiscoveryArchiveLifecycle(
            database=self.database, runtime_database=runtime, rotation_database=rotation,
            state_path=self.root / "lifecycle.json", mount_path=self.mount,
            archive_root=self.archive_root, expected_uuid=DEFAULT_VOLUME_UUID,
            uuid_probe=lambda _: DEFAULT_VOLUME_UUID,
        )
        result = lifecycle.run(max_jobs=1)
        self.assertEqual("ARCHIVED", result["results"][0]["state"])
        self.assertEqual("completed", self.store.summary(self.JOB_ID)["status"])
        # Same-day/reboot-style resume is idempotent: no candidate is replayed.
        self.assertEqual(0, lifecycle.run(max_jobs=1)["planned"])

    def test_reference_change_blocks_atomic_repoint(self):
        runtime, rotation = self._reference_databases()
        lifecycle = DiscoveryArchiveLifecycle(
            database=self.database, runtime_database=runtime, rotation_database=rotation,
            state_path=self.root / "lifecycle.json", mount_path=self.mount,
            archive_root=self.archive_root, expected_uuid=DEFAULT_VOLUME_UUID,
            uuid_probe=lambda _: DEFAULT_VOLUME_UUID,
        )
        candidate = plan_archive_candidates(
            database=self.database, runtime_database=runtime, rotation_database=rotation,
        )["candidates"][0]
        with mock.patch.object(lifecycle, "_fresh_candidate", side_effect=ArchiveIntegrityError("changed")):
            result = lifecycle.process_job(candidate)
        self.assertEqual("BLOCKED", result["state"])
        self.assertIsNone(archive_catalog_record(self.database, self.JOB_ID))

    def test_resume_from_copied_boundary_without_recopy(self):
        runtime, rotation = self._reference_databases()
        lifecycle = DiscoveryArchiveLifecycle(
            database=self.database, runtime_database=runtime, rotation_database=rotation,
            state_path=self.root / "lifecycle.json", mount_path=self.mount,
            archive_root=self.archive_root, expected_uuid=DEFAULT_VOLUME_UUID,
            uuid_probe=lambda _: DEFAULT_VOLUME_UUID,
        )
        candidate = plan_archive_candidates(
            database=self.database, runtime_database=runtime, rotation_database=rotation,
        )["candidates"][0]
        archive = self._archive()
        lifecycle._save_job(
            self.JOB_ID, "COPIED", archive=archive,
            reference_hash=candidate["reference_hash"],
        )
        with mock.patch("discovery_archive_lifecycle.create_job_archive") as recopy:
            result = lifecycle.run(max_jobs=1)
        recopy.assert_not_called()
        self.assertEqual("ARCHIVED", result["results"][0]["state"])

    def test_resume_after_cleanup_commit_is_idempotent(self):
        runtime, rotation = self._reference_databases()
        lifecycle = DiscoveryArchiveLifecycle(
            database=self.database, runtime_database=runtime, rotation_database=rotation,
            state_path=self.root / "lifecycle.json", mount_path=self.mount,
            archive_root=self.archive_root, expected_uuid=DEFAULT_VOLUME_UUID,
            uuid_probe=lambda _: DEFAULT_VOLUME_UUID,
        )
        candidate = plan_archive_candidates(
            database=self.database, runtime_database=runtime, rotation_database=rotation,
        )["candidates"][0]
        archive = self._archive()
        self._register(archive)
        self._delete()
        lifecycle._save_job(
            self.JOB_ID, "INTERNAL_CLEANUP", archive=archive,
            reference_hash=candidate["reference_hash"],
        )
        result = lifecycle.run(max_jobs=1)
        self.assertEqual("ARCHIVED", result["results"][0]["state"])

    def test_compaction_atomic_swap_and_current_reader(self):
        runtime, rotation = self._reference_databases()
        with sqlite3.connect(runtime) as connection:
            connection.execute("UPDATE discovery_job_runtime SET status='completed',resumable=0")
        with sqlite3.connect(self.database) as connection:
            connection.execute("CREATE TABLE compaction_junk(value BLOB)")
            connection.executemany("INSERT INTO compaction_junk VALUES (?)", [(b"x" * 4096,)] * 512)
            connection.execute("DELETE FROM compaction_junk")
        lifecycle = DiscoveryArchiveLifecycle(
            database=self.database, runtime_database=runtime, rotation_database=rotation,
            state_path=self.root / "lifecycle.json", mount_path=self.mount,
            archive_root=self.archive_root, expected_uuid=DEFAULT_VOLUME_UUID,
            uuid_probe=lambda _: DEFAULT_VOLUME_UUID,
        )
        with mock.patch.object(lifecycle, "compaction_due", return_value=True):
            result = lifecycle.compact()
        self.assertEqual("COMPACTED", result["status"])
        self.assertLess(result["after"]["sqlite_bytes"], result["before"]["sqlite_bytes"])
        self.assertEqual("running", self.store.summary(self.CURRENT_JOB_ID)["status"])
        self.assertFalse(list(self.root.glob("*.precompact-*")))

    def test_compaction_post_swap_failure_restores_original(self):
        runtime, rotation = self._reference_databases()
        with sqlite3.connect(runtime) as connection:
            connection.execute("UPDATE discovery_job_runtime SET status='completed',resumable=0")
        with sqlite3.connect(self.database) as connection:
            connection.execute("CREATE TABLE compaction_junk(value BLOB)")
            connection.executemany("INSERT INTO compaction_junk VALUES (?)", [(b"x" * 4096,)] * 64)
            connection.execute("DELETE FROM compaction_junk")
        lifecycle = DiscoveryArchiveLifecycle(
            database=self.database, runtime_database=runtime, rotation_database=rotation,
            state_path=self.root / "lifecycle.json", mount_path=self.mount,
            archive_root=self.archive_root, expected_uuid=DEFAULT_VOLUME_UUID,
            uuid_probe=lambda _: DEFAULT_VOLUME_UUID,
        )
        real_connect = sqlite3.connect

        def fail_smoke(path, *args, **kwargs):
            if Path(path) == self.database and list(self.root.glob("*.precompact-*")):
                raise sqlite3.DatabaseError("simulated post-swap failure")
            return real_connect(path, *args, **kwargs)

        with mock.patch.object(lifecycle, "compaction_due", return_value=True), mock.patch(
            "discovery_archive_lifecycle.sqlite3.connect", side_effect=fail_smoke,
        ):
            with self.assertRaisesRegex(sqlite3.DatabaseError, "simulated"):
                lifecycle.compact()
        with sqlite3.connect(self.database) as connection:
            self.assertEqual("ok", connection.execute("PRAGMA quick_check").fetchone()[0])

    def test_copy_crash_leaves_internal_authority_and_no_final_segment(self):
        with mock.patch(
            "discovery_archive._copy_job_table", side_effect=RuntimeError("simulated crash"),
        ):
            with self.assertRaisesRegex(RuntimeError, "simulated crash"):
                self._archive()
        self.assertEqual("completed", self.store.summary(self.JOB_ID)["status"])
        self.assertIsNone(archive_catalog_record(self.database, self.JOB_ID))
        segment = self.archive_root / "v1" / f"discovery-{self.JOB_ID}-v1"
        self.assertFalse((segment / "discovery-job.sqlite3").exists())
        self.assertFalse(list(segment.glob("*.tmp-*")) if segment.exists() else [])

    def test_bad_uuid_sentinel_symlink_and_fake_mount_fail_closed(self):
        with self.assertRaises(ArchiveStorageUnavailable):
            validate_archive_volume(
                mount_path=self.mount, expected_uuid="wrong",
                uuid_probe=lambda _: DEFAULT_VOLUME_UUID,
            )
        original = self.sentinel.read_text(encoding="utf-8")
        self.sentinel.write_text(json.dumps({"volume_uuid": DEFAULT_VOLUME_UUID}), encoding="utf-8")
        with self.assertRaises(ArchiveStorageUnavailable):
            validate_archive_volume(
                mount_path=self.mount, expected_uuid=DEFAULT_VOLUME_UUID,
                uuid_probe=lambda _: DEFAULT_VOLUME_UUID,
            )
        self.sentinel.write_text(original, encoding="utf-8")
        link = self.root / "archive-link"
        link.symlink_to(self.mount, target_is_directory=True)
        with self.assertRaises(ArchiveStorageUnavailable):
            validate_archive_volume(
                mount_path=link, expected_uuid=DEFAULT_VOLUME_UUID,
                uuid_probe=lambda _: DEFAULT_VOLUME_UUID,
            )
        with mock.patch("discovery_archive.os.path.ismount", return_value=False):
            with self.assertRaises(ArchiveStorageUnavailable):
                validate_archive_volume(
                    mount_path=self.mount, expected_uuid=DEFAULT_VOLUME_UUID,
                    uuid_probe=lambda _: DEFAULT_VOLUME_UUID,
                )

    def test_corrupt_archive_fails_closed(self):
        archive = self._archive()
        self._register(archive)
        with Path(archive["archive_path"]).open("ab") as output:
            output.write(b"corruption")
        with self.assertRaisesRegex(ArchiveStorageUnavailable, "checksum mismatch"):
            self.store.summary(self.JOB_ID)

    def test_registration_rollback_and_post_delete_restore(self):
        archive = self._archive()
        self._register(archive)
        remove_archive_registration(self.database, self.JOB_ID)
        self.assertEqual("completed", self.store.summary(self.JOB_ID)["status"])
        self._register(archive)
        self._delete()
        restored = restore_job_from_archive(
            self.database, archive["archive_path"], self.JOB_ID,
        )
        self.assertEqual(archive["row_counts"], restored)
        remove_archive_registration(self.database, self.JOB_ID)
        self.assertEqual("completed", self.store.summary(self.JOB_ID)["status"])


if __name__ == "__main__":
    unittest.main()
