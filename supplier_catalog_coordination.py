"""Cross-process coordination for writers of supplier_catalog.sqlite3.

The files are stable rendezvous points only. Ownership is held exclusively by
the kernel (``flock``), so a crash, SIGTERM, or reboot cannot leave a stale
owner behind.
"""

from __future__ import annotations

import fcntl
import os
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


ROOT = Path(__file__).resolve().parent
DEFAULT_WRITER_LOCK_PATH = ROOT / "data" / "supplier-catalog-writer.lock"
DEFAULT_WEEKLY_INTENT_PATH = ROOT / "data" / "supplier-catalog-weekly-intent.lock"
DEFAULT_WEEKLY_HANDOFF_TIMEOUT_SECONDS = 15 * 60


class SupplierCatalogCoordinationTimeout(RuntimeError):
    """The shared writer could not be acquired before the bounded deadline."""


def _open_lock(path: str | Path) -> int:
    resolved = Path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    return os.open(resolved, os.O_CREAT | os.O_RDWR, 0o600)


@contextmanager
def _exclusive_lock(
    path: str | Path,
    *,
    timeout_seconds: float | None,
    poll_seconds: float = 0.1,
) -> Iterator[None]:
    descriptor = _open_lock(path)
    deadline = None if timeout_seconds is None else time.monotonic() + max(0.0, timeout_seconds)
    try:
        while True:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError as exc:
                if deadline is not None and time.monotonic() >= deadline:
                    raise SupplierCatalogCoordinationTimeout(
                        f"supplier_catalog_lock_timeout:{Path(path).name}"
                    ) from exc
                time.sleep(max(0.001, poll_seconds))
        yield
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


def weekly_intent_active(path: str | Path = DEFAULT_WEEKLY_INTENT_PATH) -> bool:
    """Return whether a live weekly process owns the intent lock.

    File existence is deliberately ignored: only a live kernel lock is an
    intent, making stale lock files harmless.
    """
    descriptor = _open_lock(path)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        return False
    finally:
        os.close(descriptor)


@contextmanager
def weekly_supplier_catalog_intent(
    path: str | Path = DEFAULT_WEEKLY_INTENT_PATH,
) -> Iterator[None]:
    """Publish crash-safe weekly priority after the weekly role lock."""
    with _exclusive_lock(path, timeout_seconds=0):
        yield


@contextmanager
def supplier_catalog_writer_lock(
    path: str | Path = DEFAULT_WRITER_LOCK_PATH,
    *,
    timeout_seconds: float | None = 0,
    poll_seconds: float = 0.1,
) -> Iterator[None]:
    """Serialize authoritative supplier catalog writers across processes."""
    with _exclusive_lock(
        path, timeout_seconds=timeout_seconds, poll_seconds=poll_seconds,
    ):
        yield
