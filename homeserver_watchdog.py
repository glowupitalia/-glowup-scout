"""Conservative HomeServer Tailscale recovery watchdog.

It never changes VPN/Funnel configuration and never reboots the host.  When
Ethernet and Internet are healthy but the pre-existing Tailscale service is
disconnected, it performs at most one ordinary start attempt per cooldown.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import time
from pathlib import Path


DEFAULT_STATE = Path(__file__).resolve().parent / "data" / "tailscale-watchdog.json"
TAILSCALE_CLI = "/Applications/Tailscale.app/Contents/MacOS/Tailscale"


def command(args, *, timeout=10):
    return subprocess.run(args, capture_output=True, text=True, timeout=timeout)


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


def recover(*, state_path=DEFAULT_STATE, cooldown_seconds=900, max_attempts=3,
            now=None) -> dict:
    observed = float(now if now is not None else time.time())
    path = Path(state_path)
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        state = {}
    result = {
        "checked_at": observed, "ethernet_ready": ethernet_ready(),
        "internet_ready": internet_ready(), "connected_before": stable_tailscale_connected(),
        "attempted": False, "connected_after": False,
        "retry_count": int(state.get("retry_count") or 0),
        "max_attempts": int(max_attempts),
    }
    if result["connected_before"]:
        result["connected_after"] = True
        state["retry_count"] = 0
    elif result["ethernet_ready"] and result["internet_ready"]:
        last = float(state.get("last_attempt_at") or 0)
        attempts = int(state.get("retry_count") or 0)
        if attempts < max_attempts and observed - last >= cooldown_seconds:
            result["attempted"] = True
            command(["/usr/sbin/scutil", "--nc", "start", "Tailscale"])
            time.sleep(3)
            result["connected_after"] = stable_tailscale_connected()
            state["last_attempt_at"] = observed
            state["last_attempt_succeeded"] = result["connected_after"]
            state["retry_count"] = 0 if result["connected_after"] else attempts + 1
            result["retry_count"] = int(state["retry_count"])
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(state, sort_keys=True), encoding="utf-8")
    os.replace(temporary, path)
    return result


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--cooldown", type=int, default=900)
    parser.add_argument("--max-attempts", type=int, default=3)
    args = parser.parse_args(argv)
    result = recover(cooldown_seconds=args.cooldown, max_attempts=args.max_attempts)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
