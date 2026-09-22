import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from discovery_archive import ARCHIVE_ROLE, DEFAULT_VOLUME_UUID, ArchiveIntegrityError, ArchiveStorageUnavailable
from qogita_serving import QogitaServingStore
from qogita_bootstrap import QogitaBootstrapStore
from supplier_archive import (
    archive_catalog_record, create_archive_segment, verify_archive_segment,
)
from supplier_archive_lifecycle import (
    SupplierArchiveLifecycle, build_reference_graph, plan_supplier_archive,
)
from supplier_catalog import SupplierCatalogStore
from storage_gc import qogita_snapshot_plan, supplier_generation_plan


class SupplierArchiveTierTests(unittest.TestCase):
    ACTIVE_RUN = "active-qogita"
    HISTORICAL_RUN = "historical-qogita"
    ACTIVE_SNAPSHOT = "active-serving"
    HISTORICAL_SNAPSHOT = "historical-serving"

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.database = self.root / "supplier.sqlite3"
        self.mount = self.root / "Glow Up Data"
        self.mount.mkdir()
        (self.mount / ".glowup-history-volume.json").write_text(json.dumps({
            "contract_version": "1", "schema_version": 2,
            "volume_uuid": DEFAULT_VOLUME_UUID, "role": ARCHIVE_ROLE,
            "initialized_at": "2026-09-22T00:00:00Z",
        }), encoding="utf-8")
        self.mount_patch = mock.patch("discovery_archive.os.path.ismount", return_value=True)
        self.mount_patch.start()
        self.store = SupplierCatalogStore(
            self.database, archive_mount_path=self.mount,
            archive_volume_uuid=DEFAULT_VOLUME_UUID,
            archive_uuid_probe=lambda _: DEFAULT_VOLUME_UUID,
        )
        self.store.initialize()
        QogitaBootstrapStore(self.database).initialize()
        QogitaServingStore(self.database).initialize()
        self._seed()
        self.discovery_patch = mock.patch(
            "supplier_archive_lifecycle.discovery_gc_plan",
            return_value={"snapshot_roots": [], "generation_roots": [], "unknowns": []},
        )
        self.discovery_patch.start()

    def tearDown(self):
        self.discovery_patch.stop()
        self.mount_patch.stop()
        self.temporary.cleanup()

    def _seed(self):
        with sqlite3.connect(self.database) as connection:
            connection.execute("PRAGMA foreign_keys=ON")
            for run_id in (self.ACTIVE_RUN, self.HISTORICAL_RUN):
                connection.execute(
                    """INSERT INTO supplier_catalog_runs
                       (run_id,supplier,started_at,completed_at,status,product_count,scenario_count,
                        coverage_type,coverage_description,coverage_complete)
                       VALUES (?,'qogita','2026-01-01','2026-01-02','success',2,2,'full','full',1)""",
                    (run_id,),
                )
                for index in range(2):
                    key = f"{run_id}-product-{index}"
                    connection.execute(
                        """INSERT INTO supplier_catalog_products
                           (run_id,supplier,canonical_product_key,canonical_ean,raw_identifiers_json,
                            enrichment_status,metadata_json) VALUES (?,'qogita',?,?,'[]','enriched','{}')""",
                        (run_id, key, f"800000000000{index}"),
                    )
                    connection.execute(
                        """INSERT INTO supplier_catalog_scenarios
                           (run_id,supplier,scenario_id,canonical_product_key,canonical_ean,
                            scenario_type,payload_json) VALUES (?,'qogita',?,?,?,'offer','{}')""",
                        (run_id, f"{run_id}-scenario-{index}", key, f"800000000000{index}"),
                    )
            connection.execute(
                "INSERT INTO supplier_catalog_active_generations VALUES ('qogita',?,'2026-01-02')",
                (self.ACTIVE_RUN,),
            )
            connection.execute(
                """INSERT INTO qogita_bootstrap_runs
                   (bootstrap_run_id,staging_run_id,started_at,updated_at,status,target_count,batch_size,
                    sample_strategy,run_mode) VALUES
                   ('bootstrap-active',?,'2026-01-01','2026-01-02','awaiting_promotion_review',2,1,'all','production')""",
                (self.ACTIVE_RUN,),
            )
            for snapshot_id, window, active in (
                (self.HISTORICAL_SNAPSHOT, 1, False), (self.ACTIVE_SNAPSHOT, 2, True),
            ):
                connection.execute(
                    """INSERT INTO qogita_serving_snapshots
                       (serving_generation_id,supplier,source_generation_id,bootstrap_run_id,created_at,
                        bootstrap_window_number,status,product_catalog_count,enriched_product_count,
                        usable_identifier_count,scenario_count,pending_count,failed_count,coverage_percent,
                        product_catalog_coverage_type,product_catalog_coverage_complete,
                        scenario_enrichment_status,bootstrap_state,diagnostics_json)
                       VALUES (?,'qogita',?,'bootstrap-active','2026-01-02',?,'valid',2,2,2,2,0,0,100,
                               'full_account_catalog',1,'full','completed','{}')""",
                    (snapshot_id, self.ACTIVE_RUN, window),
                )
                for index in range(2):
                    connection.execute(
                        "INSERT INTO qogita_serving_memberships VALUES (?,?,1,'2026-01-02')",
                        (snapshot_id, f"{self.ACTIVE_RUN}-product-{index}"),
                    )
                if active:
                    connection.execute(
                        "INSERT INTO qogita_serving_active VALUES ('qogita',?,'2026-01-02')",
                        (snapshot_id,),
                    )

    def lifecycle(self):
        return SupplierArchiveLifecycle(
            database=self.database, state_path=self.root / "lifecycle.json",
            mount_path=self.mount, expected_volume_uuid=DEFAULT_VOLUME_UUID,
            uuid_probe=lambda _: DEFAULT_VOLUME_UUID,
        )

    def test_reference_graph_protects_current_serving_and_source(self):
        graph = build_reference_graph(self.database)
        self.assertEqual(graph["active_serving"], [self.ACTIVE_SNAPSHOT])
        self.assertIn(self.ACTIVE_RUN, graph["serving_sources"])
        plan = plan_supplier_archive(self.database)
        candidates = {(x["segment_type"], x["object_id"]) for x in plan["candidates"]}
        self.assertIn(("qogita_serving_snapshot", self.HISTORICAL_SNAPSHOT), candidates)
        self.assertIn(("supplier_generation", self.HISTORICAL_RUN), candidates)
        self.assertNotIn(("qogita_serving_snapshot", self.ACTIVE_SNAPSHOT), candidates)
        self.assertNotIn(("supplier_generation", self.ACTIVE_RUN), candidates)

    def test_discovery_reference_is_archivable_through_dual_reader(self):
        with mock.patch("supplier_archive_lifecycle.discovery_gc_plan", return_value={
            "snapshot_roots": [self.HISTORICAL_SNAPSHOT],
            "generation_roots": [self.HISTORICAL_RUN], "unknowns": [],
        }):
            plan = plan_supplier_archive(self.database)
        rows = {(x["segment_type"], x["object_id"]): x for x in plan["candidates"]}
        self.assertEqual(rows[("qogita_serving_snapshot", self.HISTORICAL_SNAPSHOT)]["classification"],
                         "HISTORICAL_REFERENCED")
        self.assertEqual(rows[("supplier_generation", self.HISTORICAL_RUN)]["classification"],
                         "HISTORICAL_REFERENCED")

    def test_snapshot_copy_verify_repoint_cleanup_and_dual_reader(self):
        candidate = next(x for x in plan_supplier_archive(self.database)["candidates"]
                         if x["object_id"] == self.HISTORICAL_SNAPSHOT)
        result = self.lifecycle().process(candidate)
        self.assertEqual(result["state"], "ARCHIVED")
        with sqlite3.connect(self.database) as connection:
            self.assertEqual(connection.execute(
                "SELECT COUNT(*) FROM qogita_serving_snapshots WHERE serving_generation_id=?",
                (self.HISTORICAL_SNAPSHOT,),
            ).fetchone()[0], 0)
            self.assertEqual(connection.execute(
                "SELECT serving_generation_id FROM qogita_serving_active"
            ).fetchone()[0], self.ACTIVE_SNAPSHOT)
        reader = QogitaServingStore(
            self.database, archive_mount_path=self.mount,
            archive_volume_uuid=DEFAULT_VOLUME_UUID,
            archive_uuid_probe=lambda _: DEFAULT_VOLUME_UUID,
        )
        self.assertEqual(reader.snapshot(self.HISTORICAL_SNAPSHOT)["membership_count"], 2)
        self.assertEqual(reader.active_snapshot()["serving_generation_id"], self.ACTIVE_SNAPSHOT)

    def test_generation_archive_preserves_products_and_scenarios(self):
        candidate = next(x for x in plan_supplier_archive(self.database)["candidates"]
                         if x["object_id"] == self.HISTORICAL_RUN)
        result = self.lifecycle().process(candidate)
        self.assertEqual(result["state"], "ARCHIVED")
        generation = self.store.historical_generation(self.HISTORICAL_RUN)
        self.assertEqual(len(generation["products"]), 2)
        self.assertEqual(len(generation["scenarios"]), 2)
        self.assertEqual(self.store.run_status(self.HISTORICAL_RUN)["status"], "success")
        self.assertEqual(self.store.active_generation_metadata("qogita")["run_id"], self.ACTIVE_RUN)

    def test_x9_unavailable_is_retryable_and_keeps_internal(self):
        candidate = next(x for x in plan_supplier_archive(self.database)["candidates"]
                         if x["object_id"] == self.HISTORICAL_SNAPSHOT)
        with mock.patch("discovery_archive.os.path.ismount", return_value=False):
            result = self.lifecycle().process(candidate)
        self.assertEqual(result["state"], "RETRYABLE")
        with sqlite3.connect(self.database) as connection:
            self.assertEqual(connection.execute(
                "SELECT COUNT(*) FROM qogita_serving_snapshots WHERE serving_generation_id=?",
                (self.HISTORICAL_SNAPSHOT,),
            ).fetchone()[0], 1)

    def test_checksum_mismatch_is_detected(self):
        archive = create_archive_segment(
            self.database, "qogita_serving_snapshot", self.HISTORICAL_SNAPSHOT,
            mount_path=self.mount, expected_volume_uuid=DEFAULT_VOLUME_UUID,
            uuid_probe=lambda _: DEFAULT_VOLUME_UUID,
        )
        path = Path(archive["archive_path"])
        with path.open("ab") as handle:
            handle.write(b"corrupt")
        with self.assertRaises(ArchiveIntegrityError):
            verify_archive_segment(
                archive, mount_path=self.mount, expected_volume_uuid=DEFAULT_VOLUME_UUID,
                uuid_probe=lambda _: DEFAULT_VOLUME_UUID,
            )

    def test_archive_path_cannot_escape_validated_volume_or_use_symlink(self):
        archive = create_archive_segment(
            self.database, "qogita_serving_snapshot", self.HISTORICAL_SNAPSHOT,
            mount_path=self.mount, expected_volume_uuid=DEFAULT_VOLUME_UUID,
            uuid_probe=lambda _: DEFAULT_VOLUME_UUID,
        )
        original = Path(archive["archive_path"])
        outside = self.root / "outside.sqlite3"
        outside.write_bytes(original.read_bytes())
        escaped = dict(archive, archive_path=str(outside))
        with self.assertRaises(ArchiveStorageUnavailable):
            verify_archive_segment(
                escaped, mount_path=self.mount, expected_volume_uuid=DEFAULT_VOLUME_UUID,
                uuid_probe=lambda _: DEFAULT_VOLUME_UUID,
            )
        link = original.with_name("linked.sqlite3")
        link.symlink_to(original)
        linked = dict(archive, archive_path=str(link))
        with self.assertRaises(ArchiveStorageUnavailable):
            verify_archive_segment(
                linked, mount_path=self.mount, expected_volume_uuid=DEFAULT_VOLUME_UUID,
                uuid_probe=lambda _: DEFAULT_VOLUME_UUID,
            )

    def test_cleanup_rolls_back_when_unknown_foreign_key_would_orphan(self):
        with sqlite3.connect(self.database) as connection:
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute(
                "CREATE TABLE external_reference (run_id TEXT NOT NULL, product_key TEXT NOT NULL, "
                "FOREIGN KEY(run_id,product_key) REFERENCES "
                "supplier_catalog_products(run_id,canonical_product_key))"
            )
            connection.execute(
                "INSERT INTO external_reference VALUES (?,?)",
                (self.HISTORICAL_RUN, f"{self.HISTORICAL_RUN}-product-0"),
            )
        candidate = next(x for x in plan_supplier_archive(self.database)["candidates"]
                         if x["object_id"] == self.HISTORICAL_RUN)
        result = self.lifecycle().process(candidate)
        self.assertEqual(result["state"], "CORRUPT")
        self.assertIn("foreign key violation", result["error"])
        with sqlite3.connect(self.database) as connection:
            self.assertEqual(connection.execute(
                "SELECT COUNT(*) FROM supplier_catalog_products WHERE run_id=?",
                (self.HISTORICAL_RUN,),
            ).fetchone()[0], 2)

    def test_resume_from_copied_uses_existing_archive(self):
        candidate = next(x for x in plan_supplier_archive(self.database)["candidates"]
                         if x["object_id"] == self.HISTORICAL_SNAPSHOT)
        lifecycle = self.lifecycle()
        archive = create_archive_segment(
            self.database, candidate["segment_type"], candidate["object_id"],
            mount_path=self.mount, expected_volume_uuid=DEFAULT_VOLUME_UUID,
            uuid_probe=lambda _: DEFAULT_VOLUME_UUID,
        )
        lifecycle._save(candidate, "COPIED", archive=archive, error="transient volume probe")
        with mock.patch("supplier_archive_lifecycle.create_archive_segment") as recopy:
            result = lifecycle.process(candidate)
        recopy.assert_not_called()
        self.assertEqual(result["state"], "ARCHIVED")
        self.assertIsNone(result["error"])

    def test_run_resumes_cleanup_after_repoint_without_recopy(self):
        candidate = next(x for x in plan_supplier_archive(self.database)["candidates"]
                         if x["object_id"] == self.HISTORICAL_SNAPSHOT)
        lifecycle = self.lifecycle()
        with mock.patch(
            "supplier_archive_lifecycle._delete_internal",
            side_effect=KeyboardInterrupt("simulated termination"),
        ):
            with self.assertRaises(KeyboardInterrupt):
                lifecycle.process(candidate)
        self.assertIsNotNone(archive_catalog_record(
            self.database, candidate["segment_type"], candidate["object_id"],
        ))
        with mock.patch("supplier_archive_lifecycle.create_archive_segment") as recopy:
            result = lifecycle.run(max_objects=1)
        recopy.assert_not_called()
        self.assertEqual(result["results"][0]["state"], "ARCHIVED")

    def test_reference_change_before_repoint_blocks_cleanup(self):
        candidate = next(x for x in plan_supplier_archive(self.database)["candidates"]
                         if x["object_id"] == self.HISTORICAL_SNAPSHOT)
        lifecycle = self.lifecycle()
        archive = create_archive_segment(
            self.database, candidate["segment_type"], candidate["object_id"],
            mount_path=self.mount, expected_volume_uuid=DEFAULT_VOLUME_UUID,
            uuid_probe=lambda _: DEFAULT_VOLUME_UUID,
        )
        lifecycle._save(candidate, "VERIFIED", archive=archive)
        with mock.patch.object(lifecycle, "_current_candidate", return_value=None):
            result = lifecycle.process(candidate)
        self.assertEqual(result["state"], "BLOCKED")
        self.assertIsNone(archive_catalog_record(
            self.database, candidate["segment_type"], candidate["object_id"],
        ))

    def test_gc_plans_keep_archived_authority(self):
        candidate = next(x for x in plan_supplier_archive(self.database)["candidates"]
                         if x["object_id"] == self.HISTORICAL_SNAPSHOT)
        self.assertEqual(self.lifecycle().process(candidate)["state"], "ARCHIVED")
        plan = qogita_snapshot_plan(self.database)
        archived = next(x for x in plan["snapshots"] if x["snapshot_id"] == self.HISTORICAL_SNAPSHOT)
        self.assertEqual(archived["classification"], "ARCHIVED_AUTHORITATIVE")
        self.assertEqual(archived["decision"], "KEEP")

    def test_compaction_reclaims_deleted_pages_and_preserves_pointers(self):
        lifecycle = self.lifecycle()
        self.database.chmod(0o600)
        candidate = next(x for x in plan_supplier_archive(self.database)["candidates"]
                         if x["object_id"] == self.HISTORICAL_SNAPSHOT)
        self.assertEqual(lifecycle.process(candidate)["state"], "ARCHIVED")
        with mock.patch.object(lifecycle, "compaction_due", return_value=True):
            result = lifecycle.compact()
        self.assertEqual(result["status"], "COMPACTED")
        self.assertEqual(self.database.stat().st_mode & 0o777, 0o600)
        self.assertEqual(QogitaServingStore(self.database).active_snapshot()["serving_generation_id"],
                         self.ACTIVE_SNAPSHOT)

    def test_compaction_failure_restores_original(self):
        lifecycle = self.lifecycle()
        original = sqlite3.connect
        database_calls = {"n": 0}

        class FailingSmoke:
            def __init__(self, connection):
                self.connection = connection
            def execute(self, sql, *args):
                if str(sql).casefold().startswith("pragma quick_check"):
                    raise RuntimeError("simulated post-swap crash")
                return self.connection.execute(sql, *args)
            def close(self):
                self.connection.close()

        def connect(path, *args, **kwargs):
            value = original(path, *args, **kwargs)
            if Path(str(path)) == self.database:
                database_calls["n"] += 1
                if database_calls["n"] == 3:
                    return FailingSmoke(value)
            return value

        with (
            mock.patch.object(lifecycle, "compaction_due", return_value=True),
            mock.patch("supplier_archive_lifecycle.sqlite3.connect", side_effect=connect),
        ):
            with self.assertRaisesRegex(RuntimeError, "simulated post-swap"):
                lifecycle.compact()
        with sqlite3.connect(self.database) as connection:
            self.assertEqual(connection.execute("PRAGMA quick_check").fetchone()[0], "ok")
            self.assertEqual(connection.execute(
                "SELECT serving_generation_id FROM qogita_serving_active"
            ).fetchone()[0], self.ACTIVE_SNAPSHOT)

    def test_medium_selection_is_automatic(self):
        lifecycle = self.lifecycle()
        with mock.patch.object(lifecycle, "process", side_effect=lambda row: row) as process:
            result = lifecycle.run(max_objects=1, medium_first=True)
        self.assertEqual(result["planned"], 1)
        self.assertEqual(process.call_count, 1)

    def test_unknown_queue_generation_is_protected(self):
        with sqlite3.connect(self.database) as connection:
            connection.execute(
                "INSERT INTO qogita_enrichment_queue "
                "(run_id,canonical_product_key,task_type,reason,priority,status,created_at) "
                "VALUES (?,?,?,?,?,'unknown','2026-01-01')",
                (self.HISTORICAL_RUN, "key", "offers", "retry", 1),
            )
        candidates = {x["object_id"] for x in plan_supplier_archive(self.database)["candidates"]}
        self.assertNotIn(self.HISTORICAL_RUN, candidates)

    def test_stale_pending_queue_on_unreferenced_terminal_generation_is_archivable(self):
        with sqlite3.connect(self.database) as connection:
            connection.execute(
                "INSERT INTO qogita_enrichment_queue "
                "(run_id,canonical_product_key,task_type,reason,priority,status,created_at) "
                "VALUES (?,?,?,?,?,'pending','2026-01-01')",
                (self.HISTORICAL_RUN, "key", "offers", "stale", 1),
            )
        candidates = {x["object_id"] for x in plan_supplier_archive(self.database)["candidates"]}
        self.assertIn(self.HISTORICAL_RUN, candidates)

    def test_archived_reader_fails_closed_without_x9(self):
        candidate = next(x for x in plan_supplier_archive(self.database)["candidates"]
                         if x["object_id"] == self.HISTORICAL_SNAPSHOT)
        self.assertEqual(self.lifecycle().process(candidate)["state"], "ARCHIVED")
        reader = QogitaServingStore(
            self.database, archive_mount_path=self.mount,
            archive_volume_uuid=DEFAULT_VOLUME_UUID,
            archive_uuid_probe=lambda _: DEFAULT_VOLUME_UUID,
        )
        with mock.patch("discovery_archive.os.path.ismount", return_value=False):
            with self.assertRaises(ArchiveStorageUnavailable):
                reader.snapshot(self.HISTORICAL_SNAPSHOT)
        self.assertEqual(reader.active_snapshot()["serving_generation_id"], self.ACTIVE_SNAPSHOT)


if __name__ == "__main__":
    unittest.main()
