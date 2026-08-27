import json
import tempfile
import unittest
from pathlib import Path

from discovery_rotation import DiscoveryRotationStore, rotation_scope_key
from discovery import DISCOVERY_CHECKPOINT_SCHEMA_VERSION, DiscoveryCheckpointStore
from discovery_excel import _run_metadata


def valid_ean(index):
    prefix = f"{index:012d}"
    total = sum(
        int(char) * (1 if position % 2 == 0 else 3)
        for position, char in enumerate(prefix)
    )
    return prefix + str((10 - total % 10) % 10)


def candidate(index, suppliers=("abw",)):
    ean = valid_ean(index)
    return {
        "canonical_ean": ean,
        "gtin": ean,
        "scenarios": [
            {"supplier": supplier, "scenario_id": f"{supplier}-{ean}"}
            for supplier in suppliers
        ],
    }


class DiscoveryRotationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "rotation.sqlite3"
        self.store = DiscoveryRotationStore(self.path)
        self.rows = [candidate(index) for index in range(12)]

    def tearDown(self):
        self.temporary.cleanup()

    @staticmethod
    def ids(rows):
        return {row["canonical_ean"] for row in rows}

    def commit(self, job, rows, status="not_found"):
        return self.store.commit_catalog_results(
            job, {row["canonical_ean"]: status for row in rows}
        )

    def test_two_bounded_runs_have_zero_overlap(self):
        first, _ = self.store.select("job-1", self.rows, ["abw"], 5)
        self.commit("job-1", first)
        second, _ = self.store.select("job-2", self.rows, ["abw"], 5)
        self.assertFalse(self.ids(first) & self.ids(second))

    def test_last_chunk_is_smaller_and_next_run_starts_new_cycle(self):
        first, _ = self.store.select("job-1", self.rows, ["abw"], 5)
        self.commit("job-1", first)
        second, _ = self.store.select("job-2", self.rows, ["abw"], 5)
        self.commit("job-2", second)
        third, third_meta = self.store.select("job-3", self.rows, ["abw"], 5)
        self.assertEqual(len(third), 2)
        self.commit("job-3", third)
        fourth, fourth_meta = self.store.select("job-4", self.rows, ["abw"], 5)
        self.assertEqual(len(fourth), 5)
        self.assertEqual(fourth_meta["rotation_cycle_id"], third_meta["rotation_cycle_id"] + 1)

    def test_restart_persistence_and_resume_same_selection(self):
        first, metadata = self.store.select("job-1", self.rows, ["abw"], 5)
        reopened = DiscoveryRotationStore(self.path)
        resumed, resumed_metadata = reopened.select("job-1", list(reversed(self.rows)), ["abw"], 5)
        self.assertEqual(self.ids(first), self.ids(resumed))
        self.assertEqual(metadata["rotation_cycle_id"], resumed_metadata["rotation_cycle_id"])

    def test_new_product_has_priority_over_never_analyzed(self):
        initial = self.rows[:6]
        first, _ = self.store.select("job-1", initial, ["abw"], 2)
        self.commit("job-1", first)
        new = candidate(999)
        selected, _ = self.store.select("job-2", initial + [new], ["abw"], 1)
        self.assertEqual(selected[0]["canonical_ean"], new["canonical_ean"])

    def test_removed_product_leaves_active_universe_but_history_remains(self):
        self.store.select("job-1", self.rows, ["abw"], 2)
        removed = self.rows[-1]["canonical_ean"]
        self.store.sync_universe(self.rows[:-1], ["abw"])
        status = self.store.status(["abw"])
        self.assertEqual(status["rotation_universe_count"], len(self.rows) - 1)
        with self.store._connect() as connection:
            row = connection.execute(
                """SELECT active FROM discovery_rotation_items
                   WHERE scope_key=? AND canonical_identifier=?""",
                (rotation_scope_key(["abw"]), removed),
            ).fetchone()
        self.assertEqual(row["active"], 0)

    def test_cross_supplier_identifier_is_selected_once(self):
        shared = candidate(42, ("abw", "umma"))
        selected, _ = self.store.select("job", [shared], ["abw", "umma"], 10)
        self.assertEqual(len(selected), 1)

    def test_scope_is_stable_when_supplier_order_changes(self):
        self.assertEqual(
            rotation_scope_key(["umma", "abw"]),
            rotation_scope_key(["abw", "umma"]),
        )
        self.assertNotEqual(
            rotation_scope_key(["abw"]), rotation_scope_key(["abw", "umma"])
        )

    def test_failed_before_catalog_is_not_consumed(self):
        first, _ = self.store.select("job-1", self.rows, ["abw"], 5)
        second, _ = self.store.select("job-2", self.rows, ["abw"], 12)
        self.assertTrue(self.ids(first).issubset(self.ids(second)))
        self.assertEqual(self.store.status(["abw"])["rotation_analyzed_count"], 0)

    def test_definitive_catalog_status_consumes_identifier(self):
        selected, _ = self.store.select("job", self.rows, ["abw"], 2)
        result = self.commit("job", selected, "resolved")
        self.assertEqual(result["rotation_analyzed_this_run"], 2)

    def test_catalog_incomplete_is_not_consumed(self):
        selected, _ = self.store.select("job", self.rows, ["abw"], 2)
        result = self.commit("job", selected, "catalog_incomplete")
        self.assertEqual(result["rotation_analyzed_this_run"], 0)
        self.assertEqual(result["rotation_remaining_after_run"], len(self.rows))

    def test_manual_new_cycle_requires_confirmation(self):
        self.store.sync_universe(self.rows, ["abw"])
        with self.assertRaises(ValueError):
            self.store.start_new_cycle(["abw"], confirmed=False)
        result = self.store.start_new_cycle(["abw"], confirmed=True)
        self.assertEqual(result["rotation_cycle_id"], 2)

    def test_all_means_all_remaining(self):
        first, _ = self.store.select("job-1", self.rows, ["abw"], 3)
        self.commit("job-1", first)
        remaining, metadata = self.store.select("job-2", self.rows, ["abw"], None)
        self.assertEqual(len(remaining), len(self.rows) - 3)
        self.assertEqual(metadata["run_budget"], "all")

    def test_membership_and_generation_are_persisted(self):
        shared = candidate(42, ("abw", "umma"))
        self.store.sync_universe(
            [shared], ["umma", "abw"],
            supplier_snapshot_set={
                "abw": {"snapshot_id": "abw-1"}, "umma": {"snapshot_id": "umma-1"},
            },
        )
        with self.store._connect() as connection:
            row = connection.execute(
                "SELECT * FROM discovery_rotation_items"
            ).fetchone()
        self.assertEqual(json.loads(row["supplier_membership_json"]), ["abw", "umma"])
        self.assertEqual(
            json.loads(row["last_seen_catalog_generation_json"]),
            {"abw": "abw-1", "umma": "umma-1"},
        )

    def test_checkpoint_and_excel_include_rotation_metadata(self):
        state = DiscoveryCheckpointStore(Path(self.temporary.name) / "jobs").create(
            {"minimum_margin": 15}
        )
        state.update({
            "selected_suppliers": ["abw", "umma"],
            "rotation_scope": "scope", "rotation_cycle_id": 3,
            "rotation_analyzed_before_run": 500,
            "rotation_analyzed_this_run": 480,
            "rotation_remaining_after_run": 120,
            "supplier_snapshot_set": {
                "abw": {"freshness": "fresh", "snapshot_at": "2026-08-27T00:00:00Z"},
                "umma": {"freshness": "stale", "snapshot_at": "2026-08-20T00:00:00Z"},
            },
        })
        self.assertEqual(state["schema_version"], DISCOVERY_CHECKPOINT_SCHEMA_VERSION)
        metadata = dict(_run_metadata(state, [], []))
        self.assertEqual(metadata["Ciclo rotazione"], 3)
        self.assertEqual(metadata["Analizzati in questa run"], 480)
        self.assertIn("fresh", metadata["Freshness ABW"])


if __name__ == "__main__":
    unittest.main()
