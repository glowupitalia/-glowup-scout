import hashlib
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from storage_gc import (
    ARCHIVE,
    DELETE,
    KEEP,
    REPOINT,
    append_monitor_snapshot,
    build_storage_gc_dry_run,
    classify_disk,
    classify_swap,
    discovery_gc_plan,
    qogita_snapshot_plan,
    storage_monitor_snapshot,
    supplier_generation_plan,
)


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


class StorageGcTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.data = self.root / "data"
        self.jobs = self.data / "discovery_jobs"
        self.jobs.mkdir(parents=True)
        self.incremental = self.data / "discovery_incremental.sqlite3"
        self.runtime = self.data / "discovery_jobs.sqlite3"
        self.rotation = self.data / "discovery_rotation.sqlite3"
        self.supplier = self.data / "supplier_catalog.sqlite3"
        self._create_discovery()
        self._create_runtime()
        self._create_rotation()
        self._create_supplier()

    def tearDown(self):
        self.temp.cleanup()

    def _create_discovery(self):
        with sqlite3.connect(self.incremental) as connection:
            connection.executescript("""
                CREATE TABLE discovery_incremental_jobs(
                    job_id TEXT PRIMARY KEY,status TEXT,metadata_json TEXT,
                    legacy_checkpoint_path TEXT,created_at TEXT);
                CREATE TABLE discovery_job_items(job_id TEXT,product_json TEXT);
                CREATE TABLE discovery_purchase_scenarios(job_id TEXT,scenario_json TEXT);
                CREATE TABLE discovery_listing_classifications(job_id TEXT,display_name TEXT);
                CREATE TABLE discovery_catalog_results(job_id TEXT,diagnostics_json TEXT);
                CREATE TABLE discovery_listings(job_id TEXT,listing_json TEXT);
                CREATE TABLE discovery_observations(job_id TEXT,observation_json TEXT);
                CREATE TABLE discovery_combinations(job_id TEXT,combination_json TEXT);
                CREATE TABLE discovery_resource_events(job_id TEXT,metrics_json TEXT);
                CREATE TABLE discovery_amazon_cache(canonical_identifier TEXT,source_job_id TEXT);
                CREATE TABLE discovery_amazon_fee_cache(fee_cache_key TEXT,source_job_id TEXT);
            """)
            metadata_a = json.dumps({"supplier_snapshot_set": {"qogita": {"supplier": "qogita", "snapshot_id": "snapshot-history", "source_generation_id": "generation-a"}}})
            for job_id, metadata in (("job-a", metadata_a), ("job-b", "{}"), ("job-c", "{}")):
                connection.execute(
                    "INSERT INTO discovery_incremental_jobs VALUES(?,?,?,?,?)",
                    (job_id, "completed", metadata, None, "2026-01-01T00:00:00Z"),
                )
                for table, column in (
                    ("discovery_job_items", "product_json"),
                    ("discovery_purchase_scenarios", "scenario_json"),
                    ("discovery_listing_classifications", "display_name"),
                    ("discovery_catalog_results", "diagnostics_json"),
                    ("discovery_listings", "listing_json"),
                    ("discovery_observations", "observation_json"),
                    ("discovery_combinations", "combination_json"),
                    ("discovery_resource_events", "metrics_json"),
                ):
                    connection.execute(f"INSERT INTO {table}(job_id,{column}) VALUES(?,?)", (job_id, "{}"))
            connection.execute("INSERT INTO discovery_amazon_cache VALUES('001','job-a')")
            connection.execute("INSERT INTO discovery_amazon_fee_cache VALUES('fee','job-c')")

    def _create_runtime(self):
        with sqlite3.connect(self.runtime) as connection:
            connection.executescript("""
                CREATE TABLE discovery_job_runtime(job_id TEXT,status TEXT,export_path TEXT);
                CREATE TABLE notification_outbox(entity_id TEXT,event_type TEXT);
            """)
            for job in ("job-a", "job-b", "job-c"):
                connection.execute("INSERT INTO discovery_job_runtime VALUES(?,?,NULL)", (job, "completed"))

    def _create_rotation(self):
        with sqlite3.connect(self.rotation) as connection:
            connection.executescript("""
                CREATE TABLE discovery_rotation_selections(job_id TEXT,status TEXT);
                CREATE TABLE discovery_rotation_global_history(last_job_id TEXT);
            """)

    def _create_supplier(self):
        with sqlite3.connect(self.supplier) as connection:
            connection.executescript("""
                CREATE TABLE qogita_serving_snapshots(
                    serving_generation_id TEXT,source_generation_id TEXT,
                    bootstrap_window_number INTEGER,created_at TEXT,
                    enriched_product_count INTEGER);
                CREATE TABLE qogita_serving_active(supplier TEXT,serving_generation_id TEXT);
                CREATE TABLE qogita_bootstrap_duty_cycles(last_serving_generation_id TEXT);
                CREATE TABLE supplier_catalog_runs(
                    run_id TEXT,supplier TEXT,status TEXT,started_at TEXT,
                    product_count INTEGER,scenario_count INTEGER);
                CREATE TABLE supplier_catalog_active_generations(supplier TEXT,run_id TEXT);
                CREATE TABLE qogita_bootstrap_runs(staging_run_id TEXT);
            """)
            for snapshot_id, generation, window in (
                ("snapshot-active", "generation-active", 3),
                ("snapshot-history", "generation-a", 2),
                ("snapshot-free", "generation-free", 1),
            ):
                connection.execute(
                    "INSERT INTO qogita_serving_snapshots VALUES(?,?,?,?,?)",
                    (snapshot_id, generation, window, f"2026-01-0{window}T00:00:00Z", 10),
                )
            connection.execute("INSERT INTO qogita_serving_active VALUES('qogita','snapshot-active')")
            connection.execute("INSERT INTO qogita_bootstrap_duty_cycles VALUES('snapshot-active')")
            for generation, status, products in (
                ("generation-active", "completed", 10),
                ("generation-a", "completed", 10),
                ("generation-free", "completed", 10),
                ("generation-orphan", "completed", 10),
                ("generation-empty", "failed", 0),
            ):
                connection.execute(
                    "INSERT INTO supplier_catalog_runs VALUES(?,?,?,?,?,?)",
                    (generation, "qogita", status, "2026-01-01T00:00:00Z", products, products * 2),
                )
            connection.execute("INSERT INTO supplier_catalog_active_generations VALUES('qogita','generation-active')")
            connection.execute("INSERT INTO qogita_bootstrap_runs VALUES('generation-active')")

    def test_dependency_graph_protects_cache_and_fee_only_requires_repoint(self):
        plan = discovery_gc_plan(self.incremental, self.runtime, self.rotation)
        jobs = {row["job_id"]: row for row in plan["jobs"]}
        self.assertEqual(jobs["job-a"]["classification"], "PROTECTED")
        self.assertTrue(all(
            component["decision"] == KEEP
            for component in jobs["job-a"]["components"]
            if component["name"] in {"discovery_job_items", "discovery_catalog_results", "discovery_listings"}
        ))
        self.assertEqual(jobs["job-c"]["classification"], "PARTIALLY_PROTECTED")
        self.assertEqual(
            next(row for row in jobs["job-c"]["components"] if row["name"] == "discovery_observations")["decision"],
            KEEP,
        )
        self.assertTrue(any(row["decision"] == REPOINT for row in jobs["job-c"]["components"]))
        self.assertTrue(all(row["decision"] == DELETE for row in jobs["job-b"]["components"]))

    def test_outbox_rotation_and_operational_export_protect_deletion_gate(self):
        with sqlite3.connect(self.runtime) as connection:
            connection.execute("INSERT INTO notification_outbox VALUES('job-b','completed')")
        plan = discovery_gc_plan(self.incremental, self.runtime, self.rotation)
        job = next(row for row in plan["jobs"] if row["job_id"] == "job-b")
        self.assertTrue(all(row["decision"] == ARCHIVE for row in job["components"]))
        (self.jobs / "job-b.operational.xlsx").write_bytes(b"xlsx")
        files = build_storage_gc_dry_run(self.root)["files"]["files"]
        operational = next(row for row in files if row["path"].endswith("operational.xlsx"))
        self.assertEqual(operational["decision"], KEEP)

    def test_snapshot_active_history_and_unreferenced(self):
        plan = qogita_snapshot_plan(self.supplier, historical_snapshot_roots={"snapshot-history"})
        values = {row["snapshot_id"]: row for row in plan["snapshots"]}
        self.assertEqual(values["snapshot-active"]["decision"], KEEP)
        self.assertEqual(values["snapshot-history"]["decision"], ARCHIVE)
        self.assertEqual(values["snapshot-free"]["decision"], DELETE)

    def test_supplier_generation_roots_and_candidates(self):
        plan = supplier_generation_plan(self.supplier, discovery_generation_roots={"generation-a"})
        values = {row["run_id"]: row for row in plan["generations"]}
        self.assertEqual(values["generation-active"]["decision"], KEEP)
        self.assertEqual(values["generation-a"]["decision"], KEEP)
        self.assertEqual(values["generation-free"]["decision"], KEEP)
        self.assertEqual(values["generation-orphan"]["decision"], ARCHIVE)
        self.assertEqual(values["generation-empty"]["decision"], DELETE)

    def test_legacy_checkpoint_with_compact_state_requires_repoint(self):
        (self.jobs / "job-b.json").write_text("{}", encoding="utf-8")
        (self.jobs / "job-b.state.json").write_text("{}", encoding="utf-8")
        plan = build_storage_gc_dry_run(self.root)
        values = {row["path"]: row for row in plan["files"]["files"]}
        self.assertEqual(values["discovery_jobs/job-b.json"]["decision"], REPOINT)
        self.assertEqual(values["discovery_jobs/job-b.state.json"]["decision"], KEEP)

    def test_unknown_dependency_or_unreadable_metadata_fails_closed_to_keep(self):
        missing_runtime = self.data / "missing-runtime.sqlite3"
        plan = discovery_gc_plan(self.incremental, missing_runtime, self.rotation)
        job_b = next(row for row in plan["jobs"] if row["job_id"] == "job-b")
        self.assertTrue(all(row["decision"] == "UNKNOWN_KEEP" for row in job_b["components"]))
        (self.jobs / "job-b.state.json").write_text("not-json", encoding="utf-8")
        (self.jobs / "job-b.xlsx").write_bytes(b"xlsx")
        files = build_storage_gc_dry_run(self.root)["files"]["files"]
        workbook = next(row for row in files if row["path"].endswith("job-b.xlsx"))
        self.assertEqual(workbook["decision"], "UNKNOWN_KEEP")

    def test_dry_run_performs_zero_writes(self):
        before = {path: (digest(path), path.stat().st_mtime_ns) for path in self.data.glob("*.sqlite3")}
        plan = build_storage_gc_dry_run(self.root)
        after = {path: (digest(path), path.stat().st_mtime_ns) for path in self.data.glob("*.sqlite3")}
        self.assertEqual(before, after)
        self.assertEqual(plan["writes_performed"], 0)
        self.assertEqual(plan["mode"], "dry-run")

    def test_monitor_storage_is_capped(self):
        destination = self.data / "monitor.jsonl"
        for number in range(5):
            append_monitor_snapshot(destination, {"number": number}, max_records=3)
        rows = [json.loads(line) for line in destination.read_text().splitlines()]
        self.assertEqual([row["number"] for row in rows], [2, 3, 4])

    def test_disk_and_swap_classification(self):
        gib = 1024 ** 3
        self.assertEqual(classify_disk(41 * gib), "GREEN")
        self.assertEqual(classify_disk(30 * gib), "YELLOW")
        self.assertEqual(classify_disk(20 * gib), "RED")
        self.assertEqual(classify_disk(10 * gib), "CRITICAL")
        self.assertEqual(classify_swap(15 * gib, 16 * gib, 1), "RED")
        self.assertEqual(classify_swap(15 * gib, 16 * gib, None), "YELLOW")
        self.assertEqual(classify_swap(12 * gib, 16 * gib, 0), "YELLOW")

    @patch("storage_gc.shutil.disk_usage")
    def test_monitor_uses_small_aggregates_and_parses_host_metrics(self, disk_usage):
        gib = 1024 ** 3
        disk_usage.return_value = SimpleNamespace(total=100 * gib, used=50 * gib, free=50 * gib)

        def runner(command, **kwargs):
            joined = " ".join(command)
            if "vm.swapusage" in joined:
                output = "vm.swapusage: total = 16.00G  used = 8.00G  free = 8.00G"
            elif command[0] == "vm_stat":
                output = "Mach Virtual Memory Statistics: (page size of 4096 bytes)\nPages free: 100.\nPages speculative: 10.\nPages occupied by compressor: 20.\n"
            elif command[0] == "memory_pressure":
                output = "System-wide memory free percentage: 40%"
            else:
                output = "1 200 python\n2 100 scout\n"
            return SimpleNamespace(stdout=output, stderr="", returncode=0)

        snapshot = storage_monitor_snapshot(self.root, runner=runner)
        self.assertEqual(snapshot["disk"]["status"], "GREEN")
        self.assertEqual(snapshot["swap"]["used_bytes"], 8 * gib)
        self.assertEqual(snapshot["qogita"]["snapshot_count"], 3)
        self.assertEqual(snapshot["top_process_rss"][0]["command"], "python")


if __name__ == "__main__":
    unittest.main()
