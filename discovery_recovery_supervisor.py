"""One-shot launchd supervisor for retryable stopped Discovery jobs."""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from discovery_autoresume import evaluate_autoresume
from discovery_incremental import DiscoveryIncrementalStore
from discovery_jobs import DiscoveryJobRegistry
from discovery_resources import DiscoveryResourceGovernor
from discovery_archive_lifecycle import DiscoveryArchiveLifecycle, run_storage_recovery_once
from storage_gc import collect_storage_metrics, evaluate_discovery_admission


logger = logging.getLogger(__name__)


def _qogita_universe(runtime: dict) -> str:
    """Read only the bounded persisted hint used by the worker admission path."""
    path = Path(str(runtime.get("checkpoint_path") or ""))
    try:
        if path.is_file() and path.stat().st_size <= 1024 * 1024:
            value = json.loads(path.read_text(encoding="utf-8"))
            return str(value.get("qogita_universe") or "full")
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        pass
    return "full"


def _storage_recovery_fingerprint(metrics: dict, lifecycle_metrics: dict) -> str:
    """Fingerprint reclaim opportunity, intentionally excluding volatile free bytes."""
    discovery = ((metrics.get("databases") or {}).get("discovery") or {})
    payload = {
        "watermark_state": (metrics.get("watermark") or {}).get("state"),
        "discovery_file_bytes": discovery.get("file_bytes"),
        "discovery_page_count": discovery.get("page_count"),
        "discovery_freelist_bytes": discovery.get("freelist_bytes"),
        "archive_backlog_jobs": lifecycle_metrics.get("archive_backlog_jobs"),
        "archive_backlog_estimated_bytes": lifecycle_metrics.get(
            "archive_backlog_estimated_bytes"
        ),
        "reclaimable_bytes": lifecycle_metrics.get("reclaimable_bytes"),
        "sqlite_bytes": lifecycle_metrics.get("sqlite_bytes"),
        "archive_probe_error": lifecycle_metrics.get("archive_probe_error"),
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def _lifecycle_metrics() -> dict:
    try:
        lifecycle = DiscoveryArchiveLifecycle()
        values = lifecycle.metrics()
        try:
            from discovery_archive import validate_archive_volume

            validate_archive_volume(
                mount_path=lifecycle.mount_path,
                expected_uuid=lifecycle.expected_uuid,
                uuid_probe=lifecycle.uuid_probe,
            )
            values["archive_validator"] = "PASS"
        except Exception as error:
            values["archive_probe_error"] = f"{type(error).__name__}: {error}"
        return values
    except Exception as error:
        return {"archive_probe_error": f"{type(error).__name__}: {error}"}


def run_once(
    *, registry=None, store=None, governor=None, observed_at=None,
    evaluator=evaluate_autoresume, storage_recovery=run_storage_recovery_once,
    metrics_collector=collect_storage_metrics,
    admission_evaluator=evaluate_discovery_admission,
    lifecycle_metrics_collector=_lifecycle_metrics,
):
    """Evaluate persisted candidates once and launch at most one same-job resume."""
    registry = registry or DiscoveryJobRegistry()
    store = store or DiscoveryIncrementalStore()
    governor = governor or DiscoveryResourceGovernor(database_path=store.path)
    now = observed_at or datetime.now(timezone.utc)
    registry.reconcile()
    candidates = registry.recovery_candidates()
    last_reason = "no_eligible_job"
    for runtime in candidates:
        job_id = runtime["job_id"]
        allowed, reason = evaluator(
            job_id, registry=registry, store=store, governor=governor,
            automatic=True, observed_at=now,
        )
        if not allowed:
            last_reason = reason
            continue
        try:
            pid = registry.launch(
                job_id, auto_resume=True,
                expected_failure_signature=runtime.get("failure_signature"),
                observed_at=now,
            )
        except (RuntimeError, ValueError) as error:
            logger.info(
                "DISCOVERY AUTO-RESUME SKIPPED | job_id=%s reason=%s",
                job_id, type(error).__name__,
            )
            return {
                "action": "none", "job_id": job_id,
                "reason": "atomic_launch_rejected",
            }
        logger.info("DISCOVERY AUTO-RESUMED | job_id=%s pid=%s", job_id, pid)
        return {"action": "launched", "job_id": job_id, "worker_pid": int(pid)}

    storage_candidates = registry.storage_recovery_candidates()
    if not storage_candidates:
        return {
            "action": "none",
            "reason": last_reason if candidates else "no_retryable_jobs",
        }
    for runtime in storage_candidates:
        job_id = runtime["job_id"]
        active = registry.latest_active()
        if active and active.get("job_id") != job_id:
            last_reason = "another_discovery_active"
            continue
        metrics_before = metrics_collector()
        universe = _qogita_universe(runtime)
        decision = admission_evaluator(
            runtime.get("selected_suppliers") or [], universe, metrics=metrics_before,
        )
        if decision.get("allowed"):
            try:
                pid = registry.launch(job_id, storage_resume=True, observed_at=now)
            except (RuntimeError, ValueError):
                return {
                    "action": "none", "job_id": job_id,
                    "reason": "atomic_storage_launch_rejected",
                }
            logger.info("DISCOVERY STORAGE AUTO-RESUMED | job_id=%s pid=%s", job_id, pid)
            return {
                "action": "launched", "reason": "storage_admitted",
                "job_id": job_id, "worker_pid": int(pid),
            }

        lifecycle_before = lifecycle_metrics_collector()
        fingerprint = _storage_recovery_fingerprint(metrics_before, lifecycle_before)
        if (
            runtime.get("storage_recovery_fingerprint") == fingerprint
            and str(runtime.get("storage_recovery_outcome") or "").startswith("blocked:")
        ):
            last_reason = "storage_unchanged_since_failed_recovery"
            continue
        try:
            recovery = storage_recovery()
        except Exception as error:
            recovery = {
                "status": "RECOVERY_FAILED",
                "error": f"{type(error).__name__}: {error}",
            }
        metrics_after = metrics_collector()
        lifecycle_after = lifecycle_metrics_collector()
        fingerprint_after = _storage_recovery_fingerprint(metrics_after, lifecycle_after)
        decision_after = admission_evaluator(
            runtime.get("selected_suppliers") or [], universe, metrics=metrics_after,
        )
        if not decision_after.get("allowed"):
            reason = str(decision_after.get("reason") or recovery.get("status") or "unknown")
            registry.record_storage_recovery(
                job_id, fingerprint=fingerprint_after,
                outcome=f"blocked:{reason}", observed_at=now,
            )
            logger.warning(
                "DISCOVERY STORAGE RECOVERY INSUFFICIENT | job_id=%s reason=%s",
                job_id, reason,
            )
            return {
                "action": "none", "job_id": job_id,
                "reason": "storage_recovery_insufficient", "recovery": recovery,
            }
        registry.record_storage_recovery(
            job_id, fingerprint=fingerprint_after,
            outcome="recovered:admitted", observed_at=now,
        )
        try:
            pid = registry.launch(job_id, storage_resume=True, observed_at=now)
        except (RuntimeError, ValueError):
            return {
                "action": "none", "job_id": job_id,
                "reason": "atomic_storage_launch_rejected", "recovery": recovery,
            }
        logger.info("DISCOVERY STORAGE AUTO-RESUMED | job_id=%s pid=%s", job_id, pid)
        return {
            "action": "launched", "reason": "storage_recovered",
            "job_id": job_id, "worker_pid": int(pid), "recovery": recovery,
        }
    return {"action": "none", "reason": last_reason}


def main(argv=None):
    del argv
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    result = run_once()
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
