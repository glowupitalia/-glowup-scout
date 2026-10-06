"""Boot-time reconciliation for an already persisted Discovery job.

This command is intentionally not enabled by default.  It resumes only the same
incremental job after validating persistence, snapshot references and resources.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone

from discovery import DiscoveryCheckpointStore
from discovery_incremental import DiscoveryIncrementalStore
from discovery_jobs import DiscoveryJobRegistry, _parse_time, process_alive
from discovery_recovery import (
    DISCOVERY_MAX_CONSECUTIVE_AUTO_RESUMES,
    RETRYABLE_PROVIDER_FAILURE,
)
from discovery_resources import DiscoveryResourceGovernor
from supplier_catalog import SupplierCatalogStore


BLOCKED_STATUSES = {
    "completed", "failed", "manual_paused", "manual_intervention_required",
}


def evaluate_autoresume(
    job_id: str, *, registry=None, store=None, governor=None,
    automatic: bool = False, observed_at: datetime | None = None,
):
    registry = registry or DiscoveryJobRegistry()
    store = store or DiscoveryIncrementalStore()
    runtime = registry.get(job_id)
    if not runtime:
        return False, "job_not_registered"
    if runtime.get("status") in BLOCKED_STATUSES or not runtime.get("resumable"):
        return False, "status_not_resumable"
    if automatic:
        now = observed_at or datetime.now(timezone.utc)
        if runtime.get("failure_category") != RETRYABLE_PROVIDER_FAILURE:
            return False, "failure_not_retryable"
        if runtime.get("manual_intervention_required"):
            return False, "manual_intervention_required"
        if process_alive(runtime.get("worker_pid")):
            return False, "worker_alive"
        cooldown = _parse_time(runtime.get("cooldown_until"))
        if cooldown and cooldown > now:
            return False, "cooldown_active"
        if int(runtime.get("consecutive_auto_resume_count") or 0) >= (
            DISCOVERY_MAX_CONSECUTIVE_AUTO_RESUMES
        ):
            return False, "recovery_limit_reached"
        active = registry.latest_active()
        if active and active.get("job_id") != job_id:
            return False, "another_job_active"
    if not store.has_job(job_id):
        return False, "incremental_store_missing"
    state = DiscoveryCheckpointStore().load(job_id)
    supplier_store = SupplierCatalogStore()
    for supplier, snapshot in (state.get("supplier_snapshot_set") or {}).items():
        expected = snapshot.get("snapshot_id")
        if not expected:
            continue
        current = supplier_store.serving_generation_metadata(supplier)
        # Frozen scenario payloads are in the job store, but a missing source
        # snapshot is still a recovery-integrity warning and blocks auto-resume.
        if current is None:
            return False, f"supplier_snapshot_missing:{supplier}"
    governor = governor or DiscoveryResourceGovernor(database_path=store.path)
    action, reason, _ = governor.evaluate(governor.sample())
    if action != "continue":
        return False, f"resource_unsafe:{reason}"
    return True, "resumable"


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    registry = DiscoveryJobRegistry()
    registry.reconcile()
    allowed, reason = evaluate_autoresume(args.job_id, registry=registry)
    result = {"job_id": args.job_id, "allowed": allowed, "reason": reason, "launched": False}
    if allowed and args.execute:
        result["worker_pid"] = registry.launch(args.job_id)
        result["launched"] = True
    print(json.dumps(result, sort_keys=True))
    return 0 if allowed or not args.execute else 1


if __name__ == "__main__":
    raise SystemExit(main())
