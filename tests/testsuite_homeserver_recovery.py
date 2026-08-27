import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import homeserver_watchdog
from qogita_webhook_server import load_signing_secret


class HomeServerRecoveryTests(unittest.TestCase):
    def test_watchdog_rejects_phantom_connected_profile(self):
        profile = type("Result", (), {"returncode": 0, "stdout": "Connected\n"})()
        stopped = type(
            "Result", (), {"returncode": 1, "stdout": "Tailscale is stopped.\n"},
        )()
        with patch.object(homeserver_watchdog, "command", side_effect=[profile, stopped]):
            self.assertFalse(homeserver_watchdog.tailscale_connected())

    def test_watchdog_respects_cooldown(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state.json"
            with patch.object(homeserver_watchdog, "ethernet_ready", return_value=True), \
                 patch.object(homeserver_watchdog, "internet_ready", return_value=True), \
                 patch.object(homeserver_watchdog, "tailscale_connected", return_value=False), \
                 patch.object(homeserver_watchdog, "command") as command, \
                 patch.object(homeserver_watchdog.time, "sleep"):
                first = homeserver_watchdog.recover(
                    state_path=state, cooldown_seconds=900, now=1000,
                )
                second = homeserver_watchdog.recover(
                    state_path=state, cooldown_seconds=900, now=1100,
                )
            self.assertTrue(first["attempted"])
            self.assertFalse(second["attempted"])
            self.assertEqual(command.call_count, 1)

    def test_watchdog_does_nothing_without_healthy_ethernet(self):
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(homeserver_watchdog, "ethernet_ready", return_value=False), \
                 patch.object(homeserver_watchdog, "internet_ready", return_value=True), \
                 patch.object(homeserver_watchdog, "tailscale_connected", return_value=False), \
                 patch.object(homeserver_watchdog, "command") as command:
                result = homeserver_watchdog.recover(
                    state_path=Path(directory) / "state.json", now=1000,
                )
            self.assertFalse(result["attempted"])
            command.assert_not_called()

    def test_watchdog_stops_after_max_attempts(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state.json"
            state.write_text('{"last_attempt_at": 1000, "retry_count": 3}')
            with patch.object(homeserver_watchdog, "ethernet_ready", return_value=True), \
                 patch.object(homeserver_watchdog, "internet_ready", return_value=True), \
                 patch.object(homeserver_watchdog, "tailscale_connected", return_value=False), \
                 patch.object(homeserver_watchdog, "command") as command:
                result = homeserver_watchdog.recover(
                    state_path=state, cooldown_seconds=1, max_attempts=3, now=2000,
                )
            self.assertFalse(result["attempted"])
            self.assertEqual(result["retry_count"], 3)
            command.assert_not_called()

    def test_secret_file_loader_removes_only_one_final_newline(self):
        with tempfile.TemporaryDirectory() as directory:
            secret = Path(directory) / "secret"
            secret.write_text("safe-value\n", encoding="utf-8")
            with patch.dict(
                "os.environ",
                {"QOGITA_WEBHOOK_SIGNING_SECRET_FILE": str(secret)}, clear=False,
            ):
                self.assertEqual(load_signing_secret(), "safe-value")


if __name__ == "__main__":
    unittest.main()
