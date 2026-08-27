"""Resource guardrails for long-running Discovery workers on the HomeServer."""

from __future__ import annotations

import os
import resource
import shutil
import sqlite3
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


MIB = 1024 * 1024
GIB = 1024 * MIB


@dataclass(frozen=True)
class ResourcePolicy:
    rss_soft_bytes: int = 1024 * MIB
    rss_hard_bytes: int = 1280 * MIB
    available_soft_bytes: int = 512 * MIB
    available_hard_bytes: int = 256 * MIB
    disk_free_hard_bytes: int = 10 * GIB
    wal_soft_bytes: int = 512 * MIB
    wal_hard_bytes: int = 1024 * MIB
    write_rate_soft_bytes_per_second: int = 50 * MIB
    write_rate_hard_bytes_per_second: int = 100 * MIB
    db_latency_soft_ms: float = 250.0
    db_latency_hard_ms: float = 2000.0
    soft_delay_seconds: float = 5.0


@dataclass(frozen=True)
class ResourceSnapshot:
    rss_bytes: int
    available_memory_bytes: int | None
    swap_used_bytes: int | None
    disk_free_bytes: int
    database_bytes: int
    wal_bytes: int
    write_bytes: int | None
    write_rate_bytes_per_second: float | None
    db_latency_ms: float | None
    observed_monotonic: float

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


class ResourcePause(RuntimeError):
    def __init__(self, reason: str, snapshot: ResourceSnapshot, threshold: int | float):
        super().__init__(reason)
        self.reason = reason
        self.snapshot = snapshot
        self.threshold = threshold


def _rss_bytes() -> int:
    value = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return value if sys.platform == "darwin" else value * 1024


def _vm_stat() -> tuple[int | None, int | None]:
    if sys.platform != "darwin":
        return None, None
    available = None
    try:
        result = subprocess.run(
            ["/usr/bin/vm_stat"], capture_output=True, text=True, timeout=3, check=True,
        )
        page_size = 16384
        available_pages = 0
        for line in result.stdout.splitlines():
            if "page size of" in line:
                page_size = int(line.split("page size of", 1)[1].split("bytes", 1)[0])
            if line.startswith(("Pages free:", "Pages inactive:", "Pages speculative:")):
                available_pages += int(line.split(":", 1)[1].strip().rstrip("."))
        available = available_pages * page_size
    except Exception:
        pass
    used = None
    try:
        swap = subprocess.run(
            ["/usr/sbin/sysctl", "-n", "vm.swapusage"],
            capture_output=True, text=True, timeout=3, check=True,
        ).stdout
        marker = "used = "
        if marker in swap:
            used = int(float(swap.split(marker, 1)[1].split("M", 1)[0]) * MIB)
    except Exception:
        pass
    return available, used


def _process_write_bytes() -> int | None:
    # ru_oublock is cumulative 512-byte output blocks on macOS/Linux.
    try:
        return int(resource.getrusage(resource.RUSAGE_SELF).ru_oublock) * 512
    except Exception:
        return None


class DiscoveryResourceGovernor:
    def __init__(
        self, *, policy: ResourcePolicy | None = None,
        database_path: str | Path | None = None,
        disk_path: str | Path | None = None,
        sleep_func=time.sleep,
    ):
        self.policy = policy or ResourcePolicy()
        self.database_path = Path(database_path) if database_path else None
        self.disk_path = Path(disk_path or database_path or ".")
        self.sleep_func = sleep_func
        self._previous: ResourceSnapshot | None = None

    def sample(self) -> ResourceSnapshot:
        available, swap = _vm_stat()
        database = self.database_path
        database_bytes = database.stat().st_size if database and database.exists() else 0
        wal_path = Path(f"{database}-wal") if database else None
        wal_bytes = wal_path.stat().st_size if wal_path and wal_path.exists() else 0
        write_bytes = _process_write_bytes()
        db_latency_ms = None
        if database and database.exists():
            started = time.monotonic()
            try:
                with sqlite3.connect(database, timeout=2) as connection:
                    connection.execute("PRAGMA busy_timeout=2000")
                    connection.execute("SELECT 1").fetchone()
                db_latency_ms = (time.monotonic() - started) * 1000
            except sqlite3.Error:
                db_latency_ms = 10_000.0
        observed = time.monotonic()
        rate = None
        if self._previous and write_bytes is not None and self._previous.write_bytes is not None:
            elapsed = observed - self._previous.observed_monotonic
            if elapsed > 0:
                rate = max(0, write_bytes - self._previous.write_bytes) / elapsed
        snapshot = ResourceSnapshot(
            rss_bytes=_rss_bytes(), available_memory_bytes=available,
            swap_used_bytes=swap,
            disk_free_bytes=shutil.disk_usage(self.disk_path).free,
            database_bytes=database_bytes, wal_bytes=wal_bytes,
            write_bytes=write_bytes, write_rate_bytes_per_second=rate,
            db_latency_ms=db_latency_ms,
            observed_monotonic=observed,
        )
        self._previous = snapshot
        return snapshot

    def evaluate(self, snapshot: ResourceSnapshot) -> tuple[str, str | None, int | float | None]:
        policy = self.policy
        hard = (
            ("rss_hard", snapshot.rss_bytes, policy.rss_hard_bytes),
            ("available_memory_hard", snapshot.available_memory_bytes, policy.available_hard_bytes),
            ("disk_free_hard", snapshot.disk_free_bytes, policy.disk_free_hard_bytes),
            ("wal_hard", snapshot.wal_bytes, policy.wal_hard_bytes),
            ("write_rate_hard", snapshot.write_rate_bytes_per_second, policy.write_rate_hard_bytes_per_second),
            ("db_latency_hard", snapshot.db_latency_ms, policy.db_latency_hard_ms),
        )
        for reason, value, threshold in hard:
            if value is None:
                continue
            breached = value < threshold if reason in {"available_memory_hard", "disk_free_hard"} else value > threshold
            if breached:
                return "pause", reason, threshold
        soft = (
            ("rss_soft", snapshot.rss_bytes, policy.rss_soft_bytes),
            ("available_memory_soft", snapshot.available_memory_bytes, policy.available_soft_bytes),
            ("wal_soft", snapshot.wal_bytes, policy.wal_soft_bytes),
            ("write_rate_soft", snapshot.write_rate_bytes_per_second, policy.write_rate_soft_bytes_per_second),
            ("db_latency_soft", snapshot.db_latency_ms, policy.db_latency_soft_ms),
        )
        for reason, value, threshold in soft:
            if value is None:
                continue
            breached = value < threshold if reason == "available_memory_soft" else value > threshold
            if breached:
                return "throttle", reason, threshold
        return "continue", None, None

    def before_next_batch(self) -> ResourceSnapshot:
        snapshot = self.sample()
        action, reason, threshold = self.evaluate(snapshot)
        if action == "pause":
            raise ResourcePause(str(reason), snapshot, threshold or 0)
        if action == "throttle":
            self.sleep_func(self.policy.soft_delay_seconds)
        return snapshot
