import unittest
from datetime import datetime, timedelta, timezone

from discovery_ui import (
    discovery_phase_eta_seconds,
    discovery_phase_key,
    discovery_phase_progress,
    discovery_phase_steps,
)


class DiscoveryUiStateTests(unittest.TestCase):
    def test_registry_phase_wins_over_stale_internal_phase(self):
        runtime = {"status": "running", "phase": "catalog"}
        self.assertEqual(discovery_phase_key(runtime, "suppliers_loaded"), "catalog")

    def test_technical_phases_map_to_user_facing_sequence(self):
        cases = {
            "preparing_plan": "preparing",
            "catalog": "catalog",
            "bsr_filtered": "pricing",
            "competition": "competition",
            "fees_pending": "fees",
            "economics": "economics",
            "export_rows": "export",
            "completed": "completed",
        }
        for raw, expected in cases.items():
            with self.subTest(raw=raw):
                self.assertEqual(
                    discovery_phase_key({"status": "running", "phase": raw}),
                    expected,
                )

    def test_missing_numeric_progress_does_not_render_false_zero_percent(self):
        progress = discovery_phase_progress({"status": "running", "phase": "competition"})
        self.assertFalse(progress["numeric"])
        self.assertIsNone(progress["fraction"])

    def test_phase_progress_and_step_indicator(self):
        runtime = {
            "status": "running", "phase": "fees",
            "progress_current": 200, "progress_total": 500,
        }
        progress = discovery_phase_progress(runtime)
        self.assertEqual(progress["fraction"], 0.4)
        states = {row["key"]: row["state"] for row in discovery_phase_steps(runtime)}
        self.assertEqual(states["competition"], "complete")
        self.assertEqual(states["fees"], "current")
        self.assertEqual(states["export"], "pending")

    def test_eta_requires_real_phase_local_sample(self):
        now = datetime(2026, 8, 31, 10, 0, tzinfo=timezone.utc)
        runtime = {
            "status": "running", "phase": "catalog",
            "progress_current": 300, "progress_total": 600,
            "phase_progress_start": 100,
            "phase_started_at": (now - timedelta(minutes=10)).isoformat(),
        }
        self.assertEqual(discovery_phase_eta_seconds(runtime, now=now), 900)
        runtime.pop("phase_started_at")
        self.assertIsNone(discovery_phase_eta_seconds(runtime, now=now))


if __name__ == "__main__":
    unittest.main()
