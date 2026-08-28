import ast
import json
import os
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

import homeserver_watchdog as watchdog


HEALTHY_LOCAL = {
    "backend_port": True,
    "backend_status": 200,
    "backend_error": None,
    "backend_healthy": True,
    "receiver_port": True,
    "receiver_status": 405,
    "receiver_error": None,
    "receiver_healthy": True,
}


def public(status):
    return {
        "addresses": ["192.0.2.10"],
        "attempts": [],
        "status": status,
        "transport_ok": status is not None,
        "expected_status": status,
        "expected_ok": status is not None,
    }


def public_edges(healthy, total):
    attempts = []
    for index in range(total):
        ok = index < healthy
        attempts.append({
            "address": f"192.0.2.{index + 10}",
            "returncode": 0 if ok else 28,
            "status": 200 if ok else None,
            "error": None if ok else "SSL connection timeout",
        })
    return {
        "addresses": [item["address"] for item in attempts],
        "attempts": attempts,
        "status": 200 if healthy else None,
        "transport_ok": healthy == total and total > 0,
        "expected_status": 200,
        "expected_ok": healthy == total and total > 0,
    }


class WatchdogTests(unittest.TestCase):
    def run_case(self, state, now, *, public_status=200, local=None,
                 public_result=None, config=None, connected=True,
                 backend_running=True, **kwargs):
        local = dict(local or HEALTHY_LOCAL)
        config = config or watchdog.EXPECTED_FUNNEL_CONFIG
        with ExitStack() as stack:
            stack.enter_context(patch.object(watchdog, "ethernet_ready", return_value=True))
            stack.enter_context(patch.object(watchdog, "internet_ready", return_value=True))
            stack.enter_context(patch.object(
                watchdog, "stable_tailscale_connected", return_value=connected,
            ))
            stack.enter_context(patch.object(
                watchdog, "tailscale_backend_running", return_value=backend_running,
            ))
            stack.enter_context(patch.object(watchdog, "funnel_config", return_value=config))
            stack.enter_context(patch.object(watchdog, "local_upstreams", return_value=local))
            stack.enter_context(patch.object(
                watchdog, "public_https_status",
                return_value=(
                    public_result
                    if public_result is not None else public(public_status)
                ),
            ))
            stack.enter_context(patch.object(watchdog, "qogita_run_active", return_value=True))
            return watchdog.recover(state_path=state, now=now, **kwargs)

    def test_phantom_connected_profile_is_rejected(self):
        profile = type("Result", (), {"returncode": 0, "stdout": "Connected\n"})()
        stopped = type(
            "Result", (), {"returncode": 1, "stdout": "Tailscale is stopped.\n"},
        )()
        with patch.object(watchdog, "command", side_effect=[profile, stopped]):
            self.assertFalse(watchdog.tailscale_connected())

    def test_all_healthy_means_no_action(self):
        with tempfile.TemporaryDirectory() as directory:
            result = self.run_case(Path(directory) / "state.json", 1000)
        self.assertEqual(result["status"], "HEALTHY")
        self.assertEqual(result["reason"], "all_checks_healthy")
        self.assertFalse(result["funnel_recovery_attempted"])
        self.assertTrue(result["installation_acceptance"]["passed"])

    def test_three_of_three_public_edges_are_healthy(self):
        with tempfile.TemporaryDirectory() as directory:
            result = self.run_case(
                Path(directory) / "state.json", 1000,
                public_result=public_edges(3, 3),
            )
        self.assertEqual(result["status"], "HEALTHY")
        self.assertEqual(result["public_healthy_edges"], 3)
        self.assertEqual(result["public_total_edges"], 3)
        self.assertTrue(result["installation_acceptance"]["passed"])

    def test_two_of_three_public_edges_are_acceptable_degradation(self):
        with tempfile.TemporaryDirectory() as directory:
            result = self.run_case(
                Path(directory) / "state.json", 1000,
                public_result=public_edges(2, 3),
            )
        self.assertEqual(result["status"], "DEGRADED_PUBLIC")
        self.assertEqual(result["consecutive_public_failures"], 1)
        self.assertEqual(result["public_healthy_edges"], 2)
        self.assertFalse(result["funnel_recovery_authorized"])
        self.assertTrue(result["installation_acceptance"]["passed"])
        self.assertEqual(
            result["installation_acceptance"]["reason"],
            "transient_public_degradation",
        )

    def test_one_of_three_public_edges_stays_below_recovery_threshold(self):
        with tempfile.TemporaryDirectory() as directory:
            result = self.run_case(
                Path(directory) / "state.json", 1000,
                public_result=public_edges(1, 3),
            )
        self.assertEqual(result["status"], "DEGRADED_PUBLIC")
        self.assertEqual(result["consecutive_public_failures"], 1)
        self.assertFalse(result["funnel_recovery_authorized"])
        self.assertFalse(result["funnel_recovery_attempted"])
        self.assertTrue(result["installation_acceptance"]["passed"])

    def test_zero_of_three_public_edges_is_first_public_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            result = self.run_case(
                Path(directory) / "state.json", 1000,
                public_result=public_edges(0, 3),
            )
        self.assertEqual(result["status"], "DEGRADED_PUBLIC")
        self.assertEqual(result["consecutive_public_failures"], 1)
        self.assertFalse(result["funnel_recovery_authorized"])
        self.assertFalse(result["installation_acceptance"]["passed"])

    def test_public_degradation_then_healthy_resets_counter(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state.json"
            first = self.run_case(
                state, 1000, public_result=public_edges(2, 3),
            )
            second = self.run_case(
                state, 1100, public_result=public_edges(3, 3),
            )
            saved = json.loads(state.read_text())
        self.assertEqual(first["status"], "DEGRADED_PUBLIC")
        self.assertEqual(second["status"], "HEALTHY")
        self.assertEqual(second["consecutive_public_failures"], 0)
        self.assertEqual(saved["consecutive_public_failures"], 0)
        self.assertFalse(second["funnel_recovery_attempted"])

    def test_one_and_two_failures_do_not_recover(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state.json"
            first = self.run_case(state, 1000, public_status=None)
            second = self.run_case(state, 1100, public_status=None)
        self.assertEqual(first["consecutive_public_failures"], 1)
        self.assertEqual(second["consecutive_public_failures"], 2)
        self.assertFalse(first["funnel_recovery_authorized"])
        self.assertFalse(second["funnel_recovery_authorized"])

    def test_failures_must_be_spaced(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state.json"
            self.run_case(state, 1000, public_status=None)
            second = self.run_case(state, 1020, public_status=None)
        self.assertEqual(second["consecutive_public_failures"], 1)

    def test_three_failures_authorize_only_dry_run(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state.json"
            self.run_case(state, 1000, public_status=None, dry_run=True)
            self.run_case(state, 1100, public_status=None, dry_run=True)
            third = self.run_case(state, 1200, public_status=None, dry_run=True)
        self.assertTrue(third["funnel_recovery_authorized"])
        self.assertEqual(third["status"], "DEGRADED_PUBLIC")
        self.assertFalse(third["installation_acceptance"]["passed"])
        self.assertFalse(third["funnel_recovery_attempted"])
        self.assertTrue(third["would_recover"])
        self.assertEqual(third["reason"], "dry_run_recovery_would_execute")
        self.assertEqual(len(third["would_execute"]), 3)
        self.assertTrue(all(
            operation["executed"] is False
            for operation in third["recovery_operations"]
        ))

    def test_dry_run_disconnected_records_scutil_without_subprocess(self):
        with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
            stack.enter_context(patch.object(watchdog, "ethernet_ready", return_value=True))
            stack.enter_context(patch.object(watchdog, "internet_ready", return_value=True))
            stack.enter_context(patch.object(
                watchdog, "stable_tailscale_connected", return_value=False,
            ))
            stack.enter_context(patch.object(watchdog, "qogita_run_active", return_value=True))
            subprocess_run = stack.enter_context(patch.object(watchdog.subprocess, "run"))
            result = watchdog.recover(
                state_path=Path(directory) / "state.json", now=1000,
                dry_run=True,
            )
        subprocess_run.assert_not_called()
        self.assertTrue(result["connection_recovery_authorized"])
        self.assertFalse(result["attempted"])
        self.assertTrue(result["would_recover"])
        self.assertEqual(
            result["would_execute"],
            [{"argv": ["/usr/sbin/scutil", "--nc", "start", "Tailscale"]}],
        )

    def test_dry_run_funnel_actions_never_reach_subprocess(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state.json"
            state.write_text(json.dumps({
                "consecutive_public_failures": 2,
                "last_public_failure_at": 1000,
            }))
            with patch.object(watchdog.subprocess, "run") as subprocess_run:
                result = self.run_case(
                    state, 1100, public_status=None, dry_run=True,
                )
        subprocess_run.assert_not_called()
        self.assertTrue(result["funnel_recovery_authorized"])
        self.assertFalse(result["funnel_recovery_attempted"])
        self.assertEqual(len(result["would_execute"]), 3)
        flattened = [part for item in result["would_execute"] for part in item["argv"]]
        self.assertIn("off", flattened)
        self.assertIn("--set-path=/webhooks/qogita", flattened)

    def test_read_only_runner_rejects_mutation_without_guard(self):
        with patch.object(watchdog.subprocess, "run") as subprocess_run:
            with self.assertRaisesRegex(RuntimeError, "MutationGuard"):
                watchdog.command(
                    ["/usr/sbin/scutil", "--nc", "start", "Tailscale"],
                )
            with self.assertRaisesRegex(RuntimeError, "MutationGuard"):
                watchdog.command([
                    watchdog.TAILSCALE_CLI, "funnel", "--https=443", "--yes", "off",
                ])
            with self.assertRaisesRegex(RuntimeError, "MutationGuard"):
                watchdog.command([
                    watchdog.CURL, "--request", "POST", "--output", "/dev/null",
                    f"https://{watchdog.PUBLIC_HOSTNAME}/future-endpoint",
                ])
        subprocess_run.assert_not_called()

    def test_subprocess_execution_remains_centralized(self):
        source = Path(watchdog.__file__).read_text(encoding="utf-8")
        tree = ast.parse(source)
        owners = []

        class RunVisitor(ast.NodeVisitor):
            def __init__(self):
                self.functions = []

            def visit_FunctionDef(self, node):
                self.functions.append(node.name)
                self.generic_visit(node)
                self.functions.pop()

            def visit_Call(self, node):
                function = node.func
                if (
                    isinstance(function, ast.Attribute)
                    and isinstance(function.value, ast.Name)
                    and function.value.id == "subprocess"
                    and function.attr == "run"
                ):
                    owners.append(self.functions[-1] if self.functions else None)
                self.generic_visit(node)

        RunVisitor().visit(tree)
        self.assertEqual(owners, ["command"])
        self.assertEqual(source.count("_permit=_MUTATION_PERMIT"), 1)

    def test_all_tailscale_commands_force_cli_environment(self):
        outcome = watchdog.subprocess.CompletedProcess(
            [], 0, stdout="", stderr="",
        )
        with patch.dict(os.environ, {"BASELINE": "present"}, clear=True), \
             patch.object(watchdog.subprocess, "run", return_value=outcome) as run:
            watchdog.command([watchdog.TAILSCALE_CLI, "status", "--json"])
            watchdog.command([
                watchdog.TAILSCALE_CLI, "funnel", "status", "--json",
            ])
            watchdog._perform_funnel_recovery()
        self.assertEqual(run.call_count, 5)
        for call in run.call_args_list:
            self.assertEqual(call.kwargs["env"]["TAILSCALE_BE_CLI"], "1")
            self.assertEqual(call.kwargs["env"]["BASELINE"], "present")

    def test_non_tailscale_command_does_not_force_cli_environment(self):
        outcome = watchdog.subprocess.CompletedProcess(
            [], 0, stdout="192.168.1.32\n", stderr="",
        )
        with patch.dict(os.environ, {"BASELINE": "present"}, clear=True), \
             patch.object(watchdog.subprocess, "run", return_value=outcome) as run:
            watchdog.command(["/usr/sbin/ipconfig", "getifaddr", "en0"])
        self.assertNotIn("TAILSCALE_BE_CLI", run.call_args.kwargs["env"])
        self.assertEqual(run.call_args.kwargs["env"]["BASELINE"], "present")

    def test_json_command_accepts_valid_status_object(self):
        value = {"BackendState": "Running", "Health": []}
        outcome = watchdog.subprocess.CompletedProcess(
            [], 0, stdout=json.dumps(value), stderr="",
        )
        with patch.object(watchdog, "command", return_value=outcome):
            result = watchdog._json_command([
                watchdog.TAILSCALE_CLI, "status", "--json",
            ])
        self.assertEqual(result, {"value": value, "error": None})

    def test_json_command_accepts_valid_funnel_object(self):
        value = watchdog.EXPECTED_FUNNEL_CONFIG
        outcome = watchdog.subprocess.CompletedProcess(
            [], 0, stdout=json.dumps(value), stderr="",
        )
        with patch.object(watchdog, "command", return_value=outcome):
            result = watchdog._json_command([
                watchdog.TAILSCALE_CLI, "funnel", "status", "--json",
            ])
        self.assertEqual(result, {"value": value, "error": None})

    def test_json_command_reports_sanitized_non_json_stdout(self):
        outcome = watchdog.subprocess.CompletedProcess(
            [], 0,
            stdout=(
                "The Tailscale GUI failed token=secret-value "
                "Authorization: Bearer secret-bearer"
            ),
            stderr="",
        )
        with patch.object(watchdog, "command", return_value=outcome):
            result = watchdog._json_command([
                watchdog.TAILSCALE_CLI, "status", "--json",
            ])
        self.assertIsNone(result["value"])
        self.assertEqual(result["error"]["kind"], "cli_invalid_json_stdout")
        self.assertEqual(result["error"]["exit_code"], 0)
        self.assertNotIn("secret-value", result["error"]["stdout"])
        self.assertNotIn("secret-bearer", result["error"]["stdout"])
        self.assertIn("[REDACTED]", result["error"]["stdout"])

    def test_non_json_funnel_cli_error_blocks_recovery(self):
        invalid = watchdog.subprocess.CompletedProcess(
            [], 0, stdout="The Tailscale GUI failed: CLIError error 3", stderr="",
        )
        valid_status = watchdog.subprocess.CompletedProcess(
            [], 0, stdout=json.dumps({"BackendState": "Running"}), stderr="",
        )
        with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
            stack.enter_context(patch.object(watchdog, "ethernet_ready", return_value=True))
            stack.enter_context(patch.object(watchdog, "internet_ready", return_value=True))
            stack.enter_context(patch.object(
                watchdog, "stable_tailscale_connected", return_value=True,
            ))
            stack.enter_context(patch.object(
                watchdog, "local_upstreams", return_value=dict(HEALTHY_LOCAL),
            ))
            stack.enter_context(patch.object(watchdog, "qogita_run_active", return_value=True))
            stack.enter_context(patch.object(
                watchdog, "command", side_effect=[invalid, valid_status],
            ))
            result = watchdog.recover(
                state_path=Path(directory) / "state.json", now=1000,
            )
        self.assertEqual(result["status"], "DEGRADED_INTERNAL")
        self.assertEqual(result["reason"], "funnel_prerequisite_failed")
        self.assertTrue(result["prerequisites"]["tailscale_backend_running"])
        self.assertFalse(result["prerequisites"]["funnel_config_exact"])
        self.assertEqual(
            result["prerequisites"]["funnel_status_cli_error"]["kind"],
            "cli_invalid_json_stdout",
        )
        self.assertFalse(result["funnel_recovery_authorized"])
        self.assertFalse(result["funnel_recovery_attempted"])
        self.assertEqual(result["would_execute"], [])
        self.assertFalse(result["installation_acceptance"]["passed"])

    def test_dry_run_keeps_read_only_funnel_probes(self):
        with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
            state = Path(directory) / "state.json"
            state.write_text(json.dumps({
                "consecutive_public_failures": 2,
                "last_public_failure_at": 1000,
            }))
            stack.enter_context(patch.object(watchdog, "ethernet_ready", return_value=True))
            stack.enter_context(patch.object(watchdog, "internet_ready", return_value=True))
            stack.enter_context(patch.object(
                watchdog, "stable_tailscale_connected", return_value=True,
            ))
            prerequisites = stack.enter_context(patch.object(
                watchdog, "_funnel_prerequisites",
                return_value=({"all_healthy": True}, watchdog.EXPECTED_FUNNEL_CONFIG),
            ))
            public_probe = stack.enter_context(patch.object(
                watchdog, "public_https_status", return_value=public(None),
            ))
            stack.enter_context(patch.object(watchdog, "qogita_run_active", return_value=True))
            result = watchdog.recover(state_path=state, now=1100, dry_run=True)
        self.assertEqual(prerequisites.call_count, 2)
        public_probe.assert_called_once_with("/openapi.json", expected_status=200)
        self.assertEqual(result["reason"], "dry_run_recovery_would_execute")

    def test_backend_down_blocks_funnel_recovery(self):
        local = dict(HEALTHY_LOCAL, backend_port=False, backend_healthy=False)
        with tempfile.TemporaryDirectory() as directory:
            result = self.run_case(
                Path(directory) / "state.json", 1000,
                public_status=None, local=local,
            )
        self.assertEqual(result["reason"], "funnel_prerequisite_failed")
        self.assertFalse(result["funnel_recovery_attempted"])
        self.assertEqual(result["status"], "DEGRADED_INTERNAL")
        self.assertFalse(result["installation_acceptance"]["passed"])

    def test_receiver_down_blocks_funnel_recovery(self):
        local = dict(HEALTHY_LOCAL, receiver_status=None, receiver_healthy=False)
        with tempfile.TemporaryDirectory() as directory:
            result = self.run_case(
                Path(directory) / "state.json", 1000,
                public_status=None, local=local,
            )
        self.assertEqual(result["reason"], "funnel_prerequisite_failed")
        self.assertFalse(result["funnel_recovery_attempted"])
        self.assertEqual(result["status"], "DEGRADED_INTERNAL")
        self.assertFalse(result["installation_acceptance"]["passed"])

    def test_tailscale_backend_not_running_blocks_funnel_recovery(self):
        with tempfile.TemporaryDirectory() as directory:
            result = self.run_case(
                Path(directory) / "state.json", 1000,
                public_status=None, backend_running=False,
            )
        self.assertEqual(result["reason"], "funnel_prerequisite_failed")
        self.assertEqual(result["status"], "DEGRADED_INTERNAL")
        self.assertFalse(result["installation_acceptance"]["passed"])

    def test_tailscale_disconnected_uses_only_existing_recovery(self):
        with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
            stack.enter_context(patch.object(watchdog, "ethernet_ready", return_value=True))
            stack.enter_context(patch.object(watchdog, "internet_ready", return_value=True))
            stack.enter_context(patch.object(
                watchdog, "stable_tailscale_connected", return_value=False,
            ))
            stack.enter_context(patch.object(watchdog, "qogita_run_active", return_value=True))
            command = stack.enter_context(patch.object(watchdog, "command"))
            stack.enter_context(patch.object(watchdog.time, "sleep"))
            result = watchdog.recover(
                state_path=Path(directory) / "state.json", now=1000,
            )
        self.assertTrue(result["attempted"])
        self.assertFalse(result["funnel_recovery_attempted"])
        self.assertIn("scutil", command.call_args.args[0][0])

    def test_route_mismatch_blocks_funnel_recovery(self):
        bad = json.loads(json.dumps(watchdog.EXPECTED_FUNNEL_CONFIG))
        bad["Web"][watchdog.EXPECTED_WEB_KEY]["Handlers"]["/"]["Proxy"] = (
            "http://127.0.0.1:9999"
        )
        with tempfile.TemporaryDirectory() as directory:
            result = self.run_case(
                Path(directory) / "state.json", 1000,
                public_status=None, config=bad,
            )
        self.assertFalse(result["prerequisites"]["funnel_config_exact"])
        self.assertFalse(result["funnel_recovery_attempted"])
        self.assertEqual(result["status"], "DEGRADED_INTERNAL")
        self.assertFalse(result["installation_acceptance"]["passed"])

    def test_successful_recovery_is_healthy_and_records_timestamp(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state.json"
            state.write_text(json.dumps({
                "consecutive_public_failures": 2,
                "last_public_failure_at": 1000,
            }))
            with ExitStack() as stack:
                stack.enter_context(patch.object(
                    watchdog, "_perform_funnel_recovery", return_value=[],
                ))
                stack.enter_context(patch.object(
                    watchdog, "_acceptance", return_value={"healthy": True},
                ))
                stack.enter_context(patch.object(
                    watchdog, "_snapshot", return_value=Path("snapshot.json"),
                ))
                stack.enter_context(patch.object(watchdog.time, "sleep"))
                result = self.run_case(state, 1100, public_status=None)
            saved = json.loads(state.read_text())
        self.assertEqual(result["status"], "HEALTHY")
        self.assertTrue(result["funnel_recovery_attempted"])
        self.assertEqual(saved["last_funnel_recovery_at"], 1100)

    def test_failed_recovery_is_not_repeated_for_same_incident(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state.json"
            state.write_text(json.dumps({
                "consecutive_public_failures": 2,
                "last_public_failure_at": 1000,
            }))
            with ExitStack() as stack:
                recovery = stack.enter_context(patch.object(
                    watchdog, "_perform_funnel_recovery", return_value=[],
                ))
                stack.enter_context(patch.object(
                    watchdog, "_acceptance", return_value={"healthy": False},
                ))
                stack.enter_context(patch.object(
                    watchdog, "_snapshot", return_value=Path("snapshot.json"),
                ))
                stack.enter_context(patch.object(watchdog.time, "sleep"))
                first = self.run_case(state, 1100, public_status=None)
                second = self.run_case(state, 1200, public_status=None)
        self.assertEqual(first["status"], "RECOVERY_FAILED")
        self.assertEqual(second["reason"], "incident_recovery_already_attempted")
        self.assertEqual(recovery.call_count, 1)

    def test_cooldown_blocks_recovery(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state.json"
            state.write_text(json.dumps({
                "consecutive_public_failures": 2,
                "last_public_failure_at": 1000,
                "last_funnel_recovery_at": 1000,
            }))
            result = self.run_case(state, 1100, public_status=None)
        self.assertEqual(result["reason"], "funnel_recovery_cooldown")

    def test_lock_allows_only_one_execution(self):
        with tempfile.TemporaryDirectory() as directory:
            lock = Path(directory) / "watchdog.lock"
            with watchdog.exclusive_lock(lock) as acquired:
                self.assertTrue(acquired)
                with patch.object(watchdog, "recover") as recover:
                    result = watchdog.run_locked(lock_path=lock, now=1000)
        self.assertEqual(result["reason"], "lock_busy")
        recover.assert_not_called()

    def test_recovery_commands_touch_only_funnel_443(self):
        outcome = type(
            "Result", (), {"returncode": 0, "stdout": "", "stderr": ""},
        )()
        with patch.object(watchdog, "command", return_value=outcome) as command:
            watchdog._perform_funnel_recovery()
        commands = [call.args[0] for call in command.call_args_list]
        self.assertEqual(len(commands), 3)
        self.assertTrue(all(args[1] == "funnel" for args in commands))
        self.assertTrue(all("--https=443" in args for args in commands))
        self.assertFalse(any("reset" in args for args in commands))
        self.assertFalse(any("serve" in args for args in commands))

    def test_public_probe_filters_magicdns_address(self):
        answer = type("Result", (), {
            "returncode": 0,
            "stdout": "185.40.234.37\n100.125.21.70\nnot-an-address\n",
            "stderr": "",
        })()
        with patch.object(watchdog, "command", return_value=answer):
            addresses = watchdog.public_ipv4_addresses()
        self.assertEqual(addresses, ["185.40.234.37"])

    def test_dashboard_token_is_passed_via_stdin_not_process_arguments(self):
        outcome = type(
            "Result", (), {"returncode": 0, "stdout": "200", "stderr": ""},
        )()
        with patch.object(watchdog, "public_ipv4_addresses", return_value=["192.0.2.10"]), \
             patch.object(watchdog, "command", return_value=outcome) as command:
            result = watchdog.public_https_status(
                "/api/mobile/dashboard", expected_status=200,
                api_token="secret-token",
            )
        args = command.call_args.args[0]
        self.assertNotIn("secret-token", " ".join(args))
        self.assertIn("secret-token", command.call_args.kwargs["input_text"])
        self.assertEqual(result["status"], 200)

    def test_one_degraded_public_edge_fails_the_probe(self):
        healthy = type(
            "Result", (), {"returncode": 0, "stdout": "200", "stderr": ""},
        )()
        degraded = type(
            "Result", (), {"returncode": 0, "stdout": "502", "stderr": ""},
        )()
        with patch.object(
            watchdog, "public_ipv4_addresses",
            return_value=["192.0.2.10", "192.0.2.11"],
        ), patch.object(watchdog, "command", side_effect=[healthy, degraded]):
            result = watchdog.public_https_status(
                "/openapi.json", expected_status=200,
            )
        self.assertTrue(result["transport_ok"])
        self.assertFalse(result["expected_ok"])

    def test_connection_recovery_still_respects_original_cooldown(self):
        with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
            state = Path(directory) / "state.json"
            stack.enter_context(patch.object(watchdog, "ethernet_ready", return_value=True))
            stack.enter_context(patch.object(watchdog, "internet_ready", return_value=True))
            stack.enter_context(patch.object(
                watchdog, "stable_tailscale_connected", return_value=False,
            ))
            stack.enter_context(patch.object(watchdog, "qogita_run_active", return_value=True))
            command = stack.enter_context(patch.object(watchdog, "command"))
            stack.enter_context(patch.object(watchdog.time, "sleep"))
            first = watchdog.recover(state_path=state, now=1000)
            second = watchdog.recover(state_path=state, now=1100)
        self.assertTrue(first["attempted"])
        self.assertFalse(second["attempted"])
        self.assertEqual(command.call_count, 1)

    def test_no_amazon_or_operational_endpoints_are_called(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state.json"
            with patch.object(watchdog, "public_https_status", return_value=public(200)) as probe:
                with ExitStack() as stack:
                    stack.enter_context(patch.object(watchdog, "ethernet_ready", return_value=True))
                    stack.enter_context(patch.object(watchdog, "internet_ready", return_value=True))
                    stack.enter_context(patch.object(watchdog, "stable_tailscale_connected", return_value=True))
                    stack.enter_context(patch.object(watchdog, "tailscale_backend_running", return_value=True))
                    stack.enter_context(patch.object(watchdog, "funnel_config", return_value=watchdog.EXPECTED_FUNNEL_CONFIG))
                    stack.enter_context(patch.object(watchdog, "local_upstreams", return_value=dict(HEALTHY_LOCAL)))
                    stack.enter_context(patch.object(watchdog, "qogita_run_active", return_value=True))
                    watchdog.recover(state_path=state, now=1000)
        paths = [call.args[0] for call in probe.call_args_list]
        self.assertEqual(paths, ["/openapi.json"])


if __name__ == "__main__":
    unittest.main()
