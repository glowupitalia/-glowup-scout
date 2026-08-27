"""Boot-time reconciliation for an already persisted Discovery job.

This command is intentionally not enabled by default.  It resumes only the same
incremental job after validating persistence, snapshot references and resources.
"""

from __future__ import annotations

import argparse
import json

from discovery import DiscoveryCheckpointStore
from discovery_incremental import DiscoveryIncrementalStore
from discovery_jobs import DiscoveryJobRegistry
from discovery_resources import DiscoveryResourceGovernor
from supplier_catalog import SupplierCatalogStore


BLOCKED_STATUSES = {"completed", "failed", "manual_paused"}


def evaluate_autoresume(job_id: str, *, registry=None, store=None, governor=None):
    registry = registry or DiscoveryJobRegistry()
    store = store or DiscoveryIncrementalStore()
    runtime = registry.get(job_id)
    if not runtime:
        return False, "job_not_registered"
    if runtime.get("status") in BLOCKED_STATUSES or not runtime.get("resumable"):
        return False, "status_not_resumable"
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
