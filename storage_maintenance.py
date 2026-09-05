"""Cross-process coordination for Discovery start and storage maintenance.

The lock file is only an inode used by ``flock``.  Ownership lives in the
kernel, so a crashed process cannot leave an active stale lock behind.
"""

from __future__ import annotations

import fcntl
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, TextIO


DEFAULT_MAINTENANCE_LOCK = Path(__file__).resolve().parent / "data" / "discovery-maintenance.lock"


class MaintenanceLockUnavailable(RuntimeError):
    """The shared maintenance state could not be acquired safely."""


class StorageMaintenanceLock:
    def __init__(self, path: str | Path = DEFAULT_MAINTENANCE_LOCK):
        self.path = Path(path).expanduser().resolve()

    @contextmanager
    def _hold(self, operation: int, *, timeout_seconds: float = 0.0) -> Iterator[TextIO]:
        deadline = time.monotonic() + max(0.0, float(timeout_seconds))
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            handle = self.path.open("a+")
        except OSError as error:
            raise MaintenanceLockUnavailable(
                f"maintenance_lock_unavailable:{type(error).__name__}:{error}"
            ) from error
        acquired = False
        try:
            while True:
                try:
                    fcntl.flock(handle.fileno(), operation | fcntl.LOCK_NB)
                    acquired = True
                    break
                except BlockingIOError as error:
                    if time.monotonic() >= deadline:
                        raise MaintenanceLockUnavailable("maintenance_lock_busy") from error
                    time.sleep(min(0.025, max(0.0, deadline - time.monotonic())))
                except OSError as error:
                    raise MaintenanceLockUnavailable(
                        f"maintenance_lock_error:{type(error).__name__}:{error}"
                    ) from error
            yield handle
        finally:
            if acquired:
                try:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                except OSError:
                    pass
            handle.close()

    def discovery_start_guard(self, *, timeout_seconds: float = 0.0):
        """Shared guard held only until a Discovery claim is visible."""
        return self._hold(fcntl.LOCK_SH, timeout_seconds=timeout_seconds)

    def retention_apply_guard(self, *, timeout_seconds: float = 0.0):
        """Exclusive guard held for all APPLY checks and transactions."""
        return self._hold(fcntl.LOCK_EX, timeout_seconds=timeout_seconds)
