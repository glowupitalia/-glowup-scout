"""Conservative HomeServer Tailscale and Funnel recovery watchdog.

This staged watchdog owns the existing bounded Tailscale connection recovery.
It can also repair one narrowly defined failure mode: Tailscale is Running, the
exact expected Funnel configuration and both local upstreams are healthy, but
three spaced probes through public DNS cannot reach the Funnel ingress.

It never resets Serve/Funnel, restarts Tailscale, changes DNS, or calls Amazon,
Qogita, Discovery, Buy Box, or Maintenance APIs.
"""

from __future__ import annotations

import argparse
import fcntl
import ipaddress
import json
import os
import re
import socket
import subprocess
import time
import urllib.error
import urllib.request
from contextlib import contextmanager
from pathlib import Path


ROOT = Path(__file__).resolve().parent
DEFAULT_STATE = ROOT / "data" / "tailscale-watchdog.json"
DEFAULT_LOCK = ROOT / "data" / "tailscale-watchdog.lock"
DEFAULT_SNAPSHOT_DIR = ROOT / "data" / "tailscale-funnel-snapshots"
TAILSCALE_CLI = "/Applications/Tailscale.app/Contents/MacOS/Tailscale"
DIG = "/usr/bin/dig"
CURL = "/usr/bin/curl"
MANAGER_ENV = Path("/Users/casaloria/Developer/Glow-Up-Manager/.env")
PUBLIC_HOSTNAME = "homeserver.tail3d24fa.ts.net"
EXPECTED_WEB_KEY = f"{PUBLIC_HOSTNAME}:443"
EXPECTED_HANDLERS = {
    "/": {"Proxy": "http://127.0.0.1:8000"},
    "/webhooks/qogita": {
        "Proxy": "http://127.0.0.1:8511/webhooks/qogita",
    },
}
EXPECTED_FUNNEL_CONFIG = {
    "TCP": {"443": {"HTTPS": True}},
    "Web": {EXPECTED_WEB_KEY: {"Handlers": EXPECTED_HANDLERS}},
    "AllowFunnel": {EXPECTED_WEB_KEY: True},
}
_MUTATION_PERMIT = object()


def _read_only_command(args) -> bool:
    """Allow only the watchdog's explicitly audited read-only subprocesses."""
    argv = list(args)
    if not argv:
        return False
    executable = argv[0]
    if executable == "/usr/sbin/ipconfig":
        return argv == ["/usr/sbin/ipconfig", "getifaddr", "en0"]
    if executable == "/usr/sbin/scutil":
        return argv == ["/usr/sbin/scutil", "--nc", "status", "Tailscale"]
    if executable == TAILSCALE_CLI:
        return argv in (
            [TAILSCALE_CLI, "status"],
            [TAILSCALE_CLI, "status", "--json"],
            [TAILSCALE_CLI, "funnel", "status", "--json"],
        )
    if executable == DIG:
        return argv == [DIG, "+short", "@1.1.1.1", PUBLIC_HOSTNAME, "A"]
    if executable == CURL:
        mutating_options = {
            "--data", "--data-ascii", "--data-binary", "--data-raw",
            "--form", "--json", "--request", "--upload-file",
            "-F", "-T", "-X", "-d",
        }
        if any(
            item in mutating_options
            or any(item.startswith(option + "=") for option in mutating_options)
            for item in argv[1:]
        ):
            return False
        if "--output" not in argv:
            return False
        output_index = argv.index("--output")
        return (
            output_index + 1 < len(argv)
            and argv[output_index + 1] == "/dev/null"
            and argv[-1].startswith(f"https://{PUBLIC_HOSTNAME}/")
        )
    if executable == "/usr/bin/pgrep":
        return argv == ["/usr/bin/pgrep", "-f", "qogita_production_bootstrap.py"]
    return False


def command(args, *, timeout=10, input_text=None, _permit=None):
    if not _read_only_command(args) and _permit is not _MUTATION_PERMIT:
        raise RuntimeError(
            "non-read-only command must pass through MutationGuard",
        )
    environment = os.environ.copy()
    if args and args[0] == TAILSCALE_CLI:
        environment["TAILSCALE_BE_CLI"] = "1"
    try:
        return subprocess.run(
            args, capture_output=True, text=True, timeout=timeout,
            input=input_text, check=False, env=environment,
        )
    except subprocess.TimeoutExpired as exc:
        return subprocess.CompletedProcess(
            args, 124, stdout=exc.stdout or "", stderr="command timed out",
        )


class MutationGuard:
    """The only gateway for operational commands.

    Dry-run records the exact action without allowing it to reach subprocess.
    Live mode preserves the existing command execution behavior.
    """

    def __init__(self, *, dry_run: bool):
        self.dry_run = bool(dry_run)
        self.would_execute = []

    def run(self, args, *, timeout=10, input_text=None):
        action = {"argv": list(args)}
        self.would_execute.append(action)
        if self.dry_run:
            return subprocess.CompletedProcess(
                args, 0, stdout="", stderr="dry-run: not executed",
            ), False
        return command(
            args, timeout=timeout, input_text=input_text,
            _permit=_MUTATION_PERMIT,
        ), True


def ethernet_ready() -> bool:
    result = command(["/usr/sbin/ipconfig", "getifaddr", "en0"])
    return result.returncode == 0 and bool(result.stdout.strip())


def internet_ready() -> bool:
    try:
        with socket.create_connection(("1.1.1.1", 443), timeout=3):
            return True
    except OSError:
        return False


def tailscale_connected() -> bool:
    profile = command(["/usr/sbin/scutil", "--nc", "status", "Tailscale"])
    if profile.returncode != 0 or profile.stdout.splitlines()[:1] != ["Connected"]:
        return False
    client = command([TAILSCALE_CLI, "status"])
    return client.returncode == 0 and "tailscale is stopped" not in client.stdout.lower()


def stable_tailscale_connected(*, delay_seconds=2) -> bool:
    if not tailscale_connected():
        return False
    time.sleep(delay_seconds)
    return tailscale_connected()


def _sanitize_cli_output(value: str | None, *, limit=240) -> str | None:
    if not value:
        return None
    text = " ".join(str(value).split())
    text = re.sub(r"(?i)\bbearer\s+\S+", "Bearer [REDACTED]", text)
    text = re.sub(
        r"(?i)\b(authorization|token|password|cookie|secret|api[_-]?key)\b"
        r"\s*[:=]\s*(?:\"[^\"]*\"|\S+)",
        r"\1=[REDACTED]",
        text,
    )
    return text[:limit]


def _json_command(args) -> dict:
    result = command(args)
    if result.returncode != 0:
        return {
            "value": None,
            "error": {
                "kind": "cli_nonzero_exit",
                "exit_code": result.returncode,
                "stderr": _sanitize_cli_output(result.stderr),
            },
        }
    try:
        value = json.loads(result.stdout)
    except (TypeError, json.JSONDecodeError):
        return {
            "value": None,
            "error": {
                "kind": "cli_invalid_json_stdout",
                "exit_code": result.returncode,
                "stdout": _sanitize_cli_output(result.stdout),
            },
        }
    if not isinstance(value, dict):
        return {
            "value": None,
            "error": {
                "kind": "cli_json_not_object",
                "exit_code": result.returncode,
            },
        }
    return {"value": value, "error": None}


def tailscale_backend_running(*, diagnostic=None) -> bool:
    outcome = _json_command([TAILSCALE_CLI, "status", "--json"])
    if diagnostic is not None and outcome["error"]:
        diagnostic.update(outcome["error"])
    status = outcome["value"]
    return bool(status and status.get("BackendState") == "Running")


def funnel_config(*, diagnostic=None) -> dict | None:
    outcome = _json_command([TAILSCALE_CLI, "funnel", "status", "--json"])
    if diagnostic is not None and outcome["error"]:
        diagnostic.update(outcome["error"])
    return outcome["value"]


def funnel_config_is_exact(config: dict | None) -> bool:
    if not isinstance(config, dict):
        return False
    relevant = {
        "TCP": config.get("TCP"),
        "Web": config.get("Web"),
        "AllowFunnel": config.get("AllowFunnel"),
    }
    return relevant == EXPECTED_FUNNEL_CONFIG


def tcp_ready(host: str, port: int, *, timeout=2) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def http_status(url: str, *, timeout=5) -> tuple[int | None, str | None]:
    request = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return int(response.status), None
    except urllib.error.HTTPError as exc:
        return int(exc.code), None
    except (OSError, urllib.error.URLError, ValueError) as exc:
        return None, type(exc).__name__


def local_upstreams() -> dict:
    backend_port = tcp_ready("127.0.0.1", 8000)
    backend_status, backend_error = http_status(
        "http://127.0.0.1:8000/openapi.json",
    )
    receiver_port = tcp_ready("127.0.0.1", 8511)
    receiver_status, receiver_error = http_status(
        "http://127.0.0.1:8511/webhooks/qogita",
    )
    return {
        "backend_port": backend_port,
        "backend_status": backend_status,
        "backend_error": backend_error,
        "backend_healthy": backend_port and backend_status == 200,
        "receiver_port": receiver_port,
        "receiver_status": receiver_status,
        "receiver_error": receiver_error,
        "receiver_healthy": receiver_port and receiver_status == 405,
    }


def public_ipv4_addresses() -> list[str]:
    result = command([DIG, "+short", "@1.1.1.1", PUBLIC_HOSTNAME, "A"])
    if result.returncode != 0:
        return []
    addresses = []
    for line in result.stdout.splitlines():
        candidate = line.strip()
        try:
            parsed = ipaddress.ip_address(candidate)
        except ValueError:
            continue
        if parsed.version == 4 and parsed.is_global and candidate not in addresses:
            addresses.append(candidate)
    return addresses[:3]


def _curl_config(api_token: str | None) -> str | None:
    if not api_token:
        return None
    escaped = api_token.replace("\\", "\\\\").replace('"', '\\"')
    return f'header = "Authorization: Bearer {escaped}"\n'


def public_https_status(path: str, *, expected_status=None, api_token=None, timeout=8) -> dict:
    """Probe public Funnel edges while preserving hostname and TLS SNI.

    System DNS is intentionally bypassed: MagicDNS resolves this hostname to a
    private 100.x address on HomeServer, which would not test Funnel ingress.
    """
    addresses = public_ipv4_addresses()
    attempts = []
    for address in addresses:
        args = [
            CURL, "--silent", "--show-error", "--output", "/dev/null",
            "--connect-timeout", "4", "--max-time", str(int(timeout)),
            "--resolve", f"{PUBLIC_HOSTNAME}:443:{address}",
            "--write-out", "%{http_code}",
        ]
        input_text = _curl_config(api_token)
        if input_text:
            args.extend(["--config", "-"])
        args.append(f"https://{PUBLIC_HOSTNAME}{path}")
        response = command(args, timeout=timeout + 2, input_text=input_text)
        raw_status = response.stdout.strip()[-3:]
        status = int(raw_status) if raw_status.isdigit() else None
        attempts.append({
            "address": address,
            "returncode": response.returncode,
            "status": status,
            "error": None if response.returncode == 0 else _sanitize(response.stderr),
        })
    valid = [
        item for item in attempts
        if item["returncode"] == 0 and item["status"] is not None
    ]
    expected_ok = bool(attempts) and expected_status is not None and all(
        item["returncode"] == 0 and item["status"] == expected_status
        for item in attempts
    )
    return {
        "addresses": addresses,
        "attempts": attempts,
        "status": valid[0]["status"] if valid else None,
        "transport_ok": len(valid) == len(attempts) and bool(attempts),
        "expected_status": expected_status,
        "expected_ok": expected_ok,
    }


def _sanitize(value: str | None, *, limit=300) -> str | None:
    if not value:
        return None
    return " ".join(str(value).split())[:limit]


def manager_api_token() -> str | None:
    direct = os.environ.get("GLOWUP_API_TOKEN")
    if direct:
        return direct
    try:
        from dotenv import dotenv_values
        value = dotenv_values(MANAGER_ENV).get("GLOWUP_API_TOKEN")
    except (ImportError, OSError):
        return None
    return str(value) if value else None


def qogita_run_active() -> bool:
    result = command(["/usr/bin/pgrep", "-f", "qogita_production_bootstrap.py"])
    return result.returncode == 0 and bool(result.stdout.strip())


def _load_state(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _write_json(path: Path, value: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")
    os.replace(temporary, path)


@contextmanager
def exclusive_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
            return
        try:
            yield True
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _snapshot(config: dict, directory: Path, observed: float) -> Path:
    timestamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(observed))
    path = directory / f"funnel-pre-recovery-{timestamp}.json"
    _write_json(path, {"checked_at": observed, "config": config})
    return path


def _command_summary(result) -> dict:
    return {
        "returncode": result.returncode,
        "stdout": _sanitize(result.stdout),
        "stderr": _sanitize(result.stderr),
    }


def _perform_funnel_recovery(actions=None) -> list[dict]:
    actions = actions or MutationGuard(dry_run=False)
    operations = [
        [TAILSCALE_CLI, "funnel", "--https=443", "--yes", "off"],
        [
            TAILSCALE_CLI, "funnel", "--bg", "--https=443", "--yes",
            "http://127.0.0.1:8000",
        ],
        [
            TAILSCALE_CLI, "funnel", "--bg", "--https=443",
            "--set-path=/webhooks/qogita", "--yes",
            "http://127.0.0.1:8511/webhooks/qogita",
        ],
    ]
    results = []
    for args in operations:
        outcome, executed = actions.run(args, timeout=15)
        results.append({
            "operation": "off" if args[-1] == "off" else "configure",
            "path": "/webhooks/qogita" if "--set-path=/webhooks/qogita" in args else "/",
            "executed": executed,
            **_command_summary(outcome),
        })
    return results


def _funnel_prerequisites() -> tuple[dict, dict | None]:
    funnel_cli_error = {}
    status_cli_error = {}
    config = funnel_config(diagnostic=funnel_cli_error)
    local = local_upstreams()
    values = {
        "tailscale_backend_running": tailscale_backend_running(
            diagnostic=status_cli_error,
        ),
        "funnel_config_exact": funnel_config_is_exact(config),
        "tailscale_status_cli_error": status_cli_error or None,
        "funnel_status_cli_error": funnel_cli_error or None,
        **local,
    }
    values["all_healthy"] = all((
        values["tailscale_backend_running"],
        values["funnel_config_exact"],
        values["backend_healthy"],
        values["receiver_healthy"],
    ))
    return values, config


def _acceptance() -> dict:
    local = local_upstreams()
    public_openapi = public_https_status("/openapi.json", expected_status=200)
    token = manager_api_token()
    public_dashboard = public_https_status(
        "/api/mobile/dashboard", expected_status=200, api_token=token,
    ) if token else {"status": None, "transport_ok": False, "error": "token_unavailable"}
    public_webhook = public_https_status("/webhooks/qogita", expected_status=405)
    result = {
        **local,
        "public_openapi": public_openapi,
        "public_dashboard": public_dashboard,
        "public_webhook": public_webhook,
        "api_token_available": bool(token),
        "funnel_config_exact": funnel_config_is_exact(funnel_config()),
    }
    result["healthy"] = all((
        result["backend_healthy"], result["receiver_healthy"],
        public_openapi.get("expected_ok"),
        public_dashboard.get("expected_ok"),
        public_webhook.get("expected_ok"),
        result["funnel_config_exact"],
    ))
    return result


def _public_edge_counts(public: dict | None) -> tuple[int, int]:
    attempts = (public or {}).get("attempts") or []
    expected = (public or {}).get("expected_status")
    healthy = sum(
        1 for attempt in attempts
        if attempt.get("returncode") == 0 and attempt.get("status") == expected
    )
    return healthy, len(attempts)


def _installation_acceptance(result: dict, *, failure_threshold: int) -> dict:
    status = result.get("status")
    prerequisites = result.get("prerequisites") or {}
    no_cli_error = (
        prerequisites.get("tailscale_status_cli_error") is None
        and prerequisites.get("funnel_status_cli_error") is None
    )
    no_mutation = not any((
        result.get("attempted"),
        result.get("funnel_recovery_attempted"),
        result.get("would_execute"),
        result.get("would_recover"),
    ))
    internal_healthy = all((
        prerequisites.get("tailscale_backend_running"),
        prerequisites.get("funnel_config_exact"),
        prerequisites.get("backend_healthy"),
        prerequisites.get("receiver_healthy"),
        no_cli_error,
        no_mutation,
    ))
    if status == "HEALTHY" and internal_healthy:
        return {"passed": True, "reason": "healthy"}
    if status == "DEGRADED_PUBLIC":
        count = int(result.get("consecutive_public_failures") or 0)
        passed = all((
            internal_healthy,
            int(result.get("public_healthy_edges") or 0) >= 1,
            count < int(failure_threshold),
            not result.get("funnel_recovery_authorized"),
            not result.get("funnel_recovery_attempted"),
        ))
        return {
            "passed": passed,
            "reason": (
                "transient_public_degradation"
                if passed else "public_degradation_not_acceptable"
            ),
        }
    return {"passed": False, "reason": "internal_or_recovery_failure"}


def _finish(result: dict, state: dict, path: Path, *, failure_threshold: int) -> dict:
    result["installation_acceptance"] = _installation_acceptance(
        result, failure_threshold=failure_threshold,
    )
    _write_json(path, state)
    return result


def recover(*, state_path=DEFAULT_STATE, snapshot_dir=DEFAULT_SNAPSHOT_DIR,
            cooldown_seconds=900, max_attempts=3, funnel_cooldown_seconds=1800,
            failure_threshold=3, failure_sample_min_interval_seconds=60,
            dry_run=False, now=None) -> dict:
    observed = float(now if now is not None else time.time())
    path = Path(state_path)
    state = _load_state(path)
    actions = MutationGuard(dry_run=dry_run)
    result = {
        "checked_at": observed,
        "status": "DEGRADED_INTERNAL",
        "ethernet_ready": ethernet_ready(),
        "internet_ready": internet_ready(),
        "connected_before": stable_tailscale_connected(),
        "attempted": False,
        "connection_recovery_authorized": False,
        "connected_after": False,
        "retry_count": int(state.get("retry_count") or 0),
        "max_attempts": int(max_attempts),
        "funnel_recovery_attempted": False,
        "funnel_recovery_authorized": False,
        "dry_run": bool(dry_run),
        "would_execute": actions.would_execute,
        "would_recover": False,
        "qogita_run_active": qogita_run_active(),
    }

    if result["connected_before"]:
        result["connected_after"] = True
        state["retry_count"] = 0
    elif result["ethernet_ready"] and result["internet_ready"]:
        last = float(state.get("last_attempt_at") or 0)
        attempts = int(state.get("retry_count") or 0)
        if attempts < max_attempts and observed - last >= cooldown_seconds:
            result["connection_recovery_authorized"] = True
            _, executed = actions.run(
                ["/usr/sbin/scutil", "--nc", "start", "Tailscale"],
            )
            result["attempted"] = executed
            result["would_recover"] = not executed
            if not executed:
                result["reason"] = "dry_run_connection_recovery_would_execute"
                state["consecutive_public_failures"] = 0
                return _finish(
                    result, state, path, failure_threshold=failure_threshold,
                )
            time.sleep(3)
            result["connected_after"] = stable_tailscale_connected()
            state["last_attempt_at"] = observed
            state["last_attempt_succeeded"] = result["connected_after"]
            state["retry_count"] = 0 if result["connected_after"] else attempts + 1
            result["retry_count"] = int(state["retry_count"])

    if not result["connected_after"]:
        result["reason"] = "tailscale_not_connected"
        state["consecutive_public_failures"] = 0
        return _finish(result, state, path, failure_threshold=failure_threshold)

    prerequisites, config = _funnel_prerequisites()
    result["prerequisites"] = prerequisites
    if not prerequisites["all_healthy"]:
        result["reason"] = "funnel_prerequisite_failed"
        state["consecutive_public_failures"] = 0
        result["consecutive_public_failures"] = 0
        return _finish(result, state, path, failure_threshold=failure_threshold)

    public = public_https_status("/openapi.json", expected_status=200)
    result["public_openapi"] = public
    healthy_edges, total_edges = _public_edge_counts(public)
    result["public_healthy_edges"] = healthy_edges
    result["public_total_edges"] = total_edges
    if public.get("expected_ok"):
        state["consecutive_public_failures"] = 0
        state["incident_recovery_attempted"] = False
        state.pop("incident_started_at", None)
        state.pop("last_public_failure_at", None)
        result["status"] = "HEALTHY"
        result["reason"] = "all_checks_healthy"
        result["consecutive_public_failures"] = 0
        return _finish(result, state, path, failure_threshold=failure_threshold)

    result["status"] = "DEGRADED_PUBLIC"
    previous_failure = float(state.get("last_public_failure_at") or 0)
    count = int(state.get("consecutive_public_failures") or 0)
    if not previous_failure or observed - previous_failure >= failure_sample_min_interval_seconds:
        count += 1
        state["last_public_failure_at"] = observed
        state.setdefault("incident_started_at", observed)
    state["consecutive_public_failures"] = count
    result["consecutive_public_failures"] = count

    if count < failure_threshold:
        result["reason"] = "public_failure_below_threshold"
        return _finish(result, state, path, failure_threshold=failure_threshold)
    if state.get("incident_recovery_attempted"):
        result["reason"] = "incident_recovery_already_attempted"
        return _finish(result, state, path, failure_threshold=failure_threshold)
    last_recovery = float(state.get("last_funnel_recovery_at") or 0)
    if last_recovery and observed - last_recovery < funnel_cooldown_seconds:
        result["reason"] = "funnel_recovery_cooldown"
        return _finish(result, state, path, failure_threshold=failure_threshold)

    result["funnel_recovery_authorized"] = True
    immediate, current_config = _funnel_prerequisites()
    result["immediate_preflight"] = immediate
    if not immediate["all_healthy"] or current_config != config:
        result["status"] = "DEGRADED_INTERNAL"
        result["reason"] = "immediate_preflight_changed"
        return _finish(result, state, path, failure_threshold=failure_threshold)

    if not dry_run:
        snapshot_path = _snapshot(current_config, Path(snapshot_dir), observed)
        result["snapshot_path"] = str(snapshot_path)
    result["recovery_operations"] = _perform_funnel_recovery(actions)
    result["would_recover"] = dry_run
    if dry_run:
        result["reason"] = "dry_run_recovery_would_execute"
        return _finish(result, state, path, failure_threshold=failure_threshold)
    state["incident_recovery_attempted"] = True
    state["last_funnel_recovery_at"] = observed
    result["funnel_recovery_attempted"] = True
    time.sleep(2)
    acceptance = _acceptance()
    result["acceptance"] = acceptance
    if acceptance["healthy"]:
        state["consecutive_public_failures"] = 0
        state["incident_recovery_attempted"] = False
        state.pop("incident_started_at", None)
        state.pop("last_public_failure_at", None)
        state["last_funnel_recovery_succeeded"] = True
        result["status"] = "HEALTHY"
        result["reason"] = "funnel_recovery_succeeded"
    else:
        state["last_funnel_recovery_succeeded"] = False
        result["status"] = "RECOVERY_FAILED"
        result["reason"] = "funnel_recovery_acceptance_failed"
    return _finish(result, state, path, failure_threshold=failure_threshold)


def run_locked(*, lock_path=DEFAULT_LOCK, **kwargs) -> dict:
    with exclusive_lock(Path(lock_path)) as acquired:
        if not acquired:
            result = {
                "checked_at": float(kwargs.get("now") or time.time()),
                "status": "DEGRADED_INTERNAL",
                "reason": "lock_busy",
                "funnel_recovery_attempted": False,
            }
            result["installation_acceptance"] = _installation_acceptance(
                result,
                failure_threshold=int(kwargs.get("failure_threshold") or 3),
            )
            return result
        return recover(**kwargs)


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--cooldown", type=int, default=900)
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--funnel-cooldown", type=int, default=1800)
    parser.add_argument("--failure-threshold", type=int, default=3)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    result = run_locked(
        cooldown_seconds=args.cooldown,
        max_attempts=args.max_attempts,
        funnel_cooldown_seconds=args.funnel_cooldown,
        failure_threshold=args.failure_threshold,
        dry_run=args.dry_run,
    )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
