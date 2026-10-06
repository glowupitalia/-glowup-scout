"""One-shot launchd supervisor for retryable stopped Discovery jobs."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from discovery_autoresume import evaluate_autoresume
from discovery_incremental import DiscoveryIncrementalStore
from discovery_jobs import DiscoveryJobRegistry
from discovery_resources import DiscoveryResourceGovernor


logger = logging.getLogger(__name__)


def run_once(
    *, registry=None, store=None, governor=None, observed_at=None,
    evaluator=evaluate_autoresume,
):
    """Evaluate persisted candidates once and launch at most one same-job resume."""
    registry = registry or DiscoveryJobRegistry()
    store = store or DiscoveryIncrementalStore()
    governor = governor or DiscoveryResourceGovernor(database_path=store.path)
    now = observed_at or datetime.now(timezone.utc)
    registry.reconcile()
    candidates = registry.recovery_candidates()
    if not candidates:
        return {"action": "none", "reason": "no_retryable_jobs"}

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
