"""Freshness checks and isolated reuse of Manager's Qogita seller refresh."""

from __future__ import annotations

import json
import logging
import os
import re
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path


logger = logging.getLogger(__name__)
QOGITA_CACHE_TTL = timedelta(hours=24)
_SELLER_ALIAS = re.compile(r"^[A-Za-z0-9_-]{2,64}$")


class QogitaRefreshConfigurationError(RuntimeError):
    def __init__(self, code):
        super().__init__(code)
        self.code = code


def _manager_refresh_paths(manager_root=None):
    root = Path(
        manager_root
        or Path(__file__).resolve().parent.parent / "Glow-Up-Manager"
    ).expanduser().resolve()
    source = root / "src"
    python = root / ".venv" / "bin" / "python"
    script = root / "scripts" / "sync_qogita_seller_catalog.py"
    validations = (
        (root.is_dir(), "manager_repository_missing"),
        (source.is_dir(), "manager_src_missing"),
        (python.is_file(), "manager_python_missing"),
        (script.is_file(), "manager_script_missing"),
    )
    for valid, code in validations:
        if not valid:
            raise QogitaRefreshConfigurationError(code)
    return root, source, python, script


def build_manager_subprocess_environment(manager_root=None, *, environ=None):
    """Copy the parent environment and prepend Manager's source directory."""
    _, source, _, _ = _manager_refresh_paths(manager_root)
    environment = dict(os.environ if environ is None else environ)
    existing = environment.get("PYTHONPATH")
    environment["PYTHONPATH"] = (
        f"{source}{os.pathsep}{existing}" if existing else str(source)
    )
    return environment


def _parse_timestamp(value):
    try:
        parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def inspect_qogita_cache(rows, *, now=None, ttl=QOGITA_CACHE_TTL):
    """Describe the newest successful cached generation represented by each alias."""
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    snapshots = {}
    for row in rows or []:
        alias = str(row.get("seller_alias") or "").strip()
        observed_at = str(row.get("observed_at") or "").strip()
        parsed = _parse_timestamp(observed_at)
        if not alias or parsed is None:
            continue
        previous = snapshots.get(alias)
        if previous is None or parsed > previous[0]:
            snapshots[alias] = (parsed, observed_at)

    snapshot_values = {
        alias: value[1] for alias, value in sorted(snapshots.items())
    }
    stale_aliases = [
        alias for alias, (observed, _) in sorted(snapshots.items())
        if current - observed >= ttl
    ]
    return {
        "seller_aliases": sorted(snapshots),
        "snapshots": snapshot_values,
        "stale_aliases": stale_aliases,
        "fresh": bool(snapshots) and not stale_aliases,
        "ttl_hours": int(ttl.total_seconds() // 3600),
    }


def snapshots_advanced(before, after, aliases):
    """Require every refreshed alias to publish a strictly newer generation."""
    for alias in aliases:
        previous = _parse_timestamp((before or {}).get(alias))
        current = _parse_timestamp((after or {}).get(alias))
        if current is None or (previous is not None and current <= previous):
            return False
    return True


def _result_payload(stdout):
    for line in reversed(str(stdout or "").splitlines()):
        try:
            payload = json.loads(line)
        except (TypeError, ValueError):
            continue
        if isinstance(payload, dict):
            return payload
    return None


def refresh_qogita_seller_catalogs(
    seller_aliases,
    *,
    manager_root=None,
    runner=subprocess.run,
    timeout_seconds=600,
):
    """Invoke Manager's generation job sequentially, preserving its auth and lock."""
    aliases = sorted({str(value or "").strip() for value in seller_aliases})
    if not aliases or any(not _SELLER_ALIAS.fullmatch(alias) for alias in aliases):
        return {
            "status": "failed", "updated_aliases": [],
            "error_code": "invalid_seller_aliases",
        }

    try:
        root, source, python, script = _manager_refresh_paths(manager_root)
        environment = build_manager_subprocess_environment(
            root, environ=os.environ,
        )
    except QogitaRefreshConfigurationError as exc:
        return {
            "status": "failed", "updated_aliases": [],
            "error_code": exc.code,
        }

    logger.info(
        "DISCOVERY QOGITA RUNTIME | manager_root=%s manager_src=%s script=%s",
        root, source, script,
    )

    started = time.monotonic()
    outcomes = []
    updated = []
    for alias in aliases:
        alias_started = time.monotonic()
        try:
            completed = runner(
                [str(python), str(script), alias],
                cwd=str(root), capture_output=True, text=True,
                check=False, timeout=timeout_seconds, env=environment,
            )
        except subprocess.TimeoutExpired:
            outcomes.append({
                "seller_alias": alias, "status": "failed",
                "error_code": "refresh_timeout",
            })
            continue
        except OSError:
            outcomes.append({
                "seller_alias": alias, "status": "failed",
                "error_code": "manager_refresh_unavailable",
            })
            continue

        payload = _result_payload(completed.stdout) or {}
        status = str(payload.get("status") or "failed")
        if completed.returncode == 0 and status == "success":
            updated.append(alias)
            error_code = None
        elif status == "skipped" and payload.get("reason") == "already_running":
            status = "failed"
            error_code = "already_running"
        else:
            status = "failed"
            error_code = str(payload.get("error_code") or "refresh_failed")
        outcomes.append({
            "seller_alias": alias,
            "status": status,
            "error_code": error_code,
            "duration_seconds": round(time.monotonic() - alias_started, 3),
        })
        logger.info(
            "DISCOVERY QOGITA REFRESH | seller_alias=%s status=%s duration=%s",
            alias, status, outcomes[-1]["duration_seconds"],
        )

    failures = [row for row in outcomes if row["status"] != "success"]
    return {
        "status": "failed" if failures else "success",
        "updated_aliases": updated,
        "results": outcomes,
        "error_code": failures[0]["error_code"] if failures else None,
        "duration_seconds": round(time.monotonic() - started, 3),
    }
