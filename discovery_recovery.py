"""Structured, fail-closed recovery metadata for Discovery workers."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any


RETRYABLE_PROVIDER_FAILURE = "retryable_provider_failure"
DISCOVERY_RECOVERY_COOLDOWN_SECONDS = 5 * 60
DISCOVERY_MAX_CONSECUTIVE_AUTO_RESUMES = 3

_PHASE_ORDER = {
    "initialized": 0,
    "preparing": 1,
    "suppliers_loaded": 2,
    "catalog": 3,
    "catalog_complete": 4,
    "catalog_filtering": 5,
    "bsr_filtered": 6,
    "pricing": 7,
    "pricing_complete": 8,
    "competition": 9,
    "competition_filtered": 10,
    "fees": 11,
    "fees_complete": 12,
    "economics": 13,
    "completed": 14,
    "export_pending": 15,
    "export_running": 16,
    "notification_pending": 17,
}


@dataclass(frozen=True)
class DiscoveryFailure:
    category: str
    provider: str
    operation: str
    exception_class: str
    status_code: int | None = None
    transport_class: str | None = None

    @property
    def signature(self) -> str:
        stable = json.dumps(
            asdict(self), sort_keys=True, separators=(",", ":"),
        ).encode("utf-8")
        return "sha256:" + hashlib.sha256(stable).hexdigest()

    def as_dict(self) -> dict[str, Any]:
        return {**asdict(self), "signature": self.signature}


def classify_retryable_failure(error: BaseException, *, phase: str) -> DiscoveryFailure | None:
    """Return structured metadata only for provider failures known to be transient."""
    if not bool(getattr(error, "retryable", False)):
        return None
    status = getattr(error, "status_code", None)
    try:
        status = int(status) if status is not None else None
    except (TypeError, ValueError):
        return None
    transport = getattr(error, "transport_class", None)
    if status != 429 and not (status is not None and 500 <= status <= 599) and not transport:
        return None
    return DiscoveryFailure(
        category=RETRYABLE_PROVIDER_FAILURE,
        provider=str(getattr(error, "provider", None) or "amazon"),
        operation=str(getattr(error, "operation", None) or phase or "unknown"),
        exception_class=type(error).__name__,
        status_code=status,
        transport_class=str(transport) if transport else None,
    )


def progress_fingerprint(
    runtime: dict[str, Any] | None,
    incremental: dict[str, Any] | None = None,
) -> str:
    """Serialize only authoritative persisted progress, never process-start state."""
    runtime = runtime or {}
    incremental = incremental or {}
    value = {
        "phase": str(runtime.get("phase") or incremental.get("phase") or "unknown"),
        "progress_current": int(runtime.get("progress_current") or 0),
        "progress_total": int(runtime.get("progress_total") or 0),
        "catalog_completed_count": int(incremental.get("catalog_completed_count") or 0),
        "last_completed_batch": int(incremental.get("last_completed_batch") or 0),
        "selected_count": int(incremental.get("selected_count") or 0),
    }
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def progress_advanced(previous: str | None, current: str | None) -> bool:
    """Recognize monotonic persisted progress across or within pipeline phases."""
    if not previous or not current:
        return False
    try:
        before = json.loads(previous)
        after = json.loads(current)
    except (TypeError, ValueError, json.JSONDecodeError):
        return False
    before_phase = str(before.get("phase") or "unknown")
    after_phase = str(after.get("phase") or "unknown")
    before_rank = _PHASE_ORDER.get(before_phase)
    after_rank = _PHASE_ORDER.get(after_phase)
    if before_rank is not None and after_rank is not None and after_rank > before_rank:
        return True
    if before_phase == after_phase and int(after.get("progress_current") or 0) > int(
        before.get("progress_current") or 0
    ):
        return True
    for field in ("catalog_completed_count", "last_completed_batch"):
        if int(after.get(field) or 0) > int(before.get(field) or 0):
            return True
    return False
