"""Restart-safe, API-free finalization for a persisted Discovery job."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from discovery_excel import write_discovery_excel, write_discovery_operational_excel
from discovery_incremental import (
    DiscoveryIncrementalStore,
    IncrementalCandidateCollection,
    IncrementalObservationCollection,
    LightweightCheckpointStore,
)
from discovery_jobs import DiscoveryJobRegistry, PROJECT_ROOT
from discovery_resources import DiscoveryResourceGovernor, ResourcePause
from notifications import send_discovery_terminal_notification
from storage_gc import append_storage_audit_event, collect_storage_metrics
from storage_retention import run_automatic_retention


logger = logging.getLogger(__name__)


def load_env(path: Path = PROJECT_ROOT / ".env"):
    """Load configuration without logging secret names or values."""
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return
    for line in lines:
        if "=" in line and not line.lstrip().startswith("#"):
            key, value = line.strip().split("=", 1)
            os.environ.setdefault(key, value)


def export_metadata(path: Path, state: dict) -> dict:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    details = path.stat()
    created_at = datetime.fromtimestamp(
        details.st_mtime, tz=timezone.utc,
    ).isoformat().replace("+00:00", "Z")
    return {
        "status": "completed",
        "valid": True,
        "job_id": state["job_id"],
        "file_name": path.name,
        "path": str(path),
        "file_size": details.st_size,
        "sha256": digest.hexdigest(),
        "created_at": created_at,
        "generated_at": created_at,
        "result_products": int(state.get("final_products") or 0),
    }


def finalization_state(
    job_id: str, store: DiscoveryIncrementalStore,
    checkpoints: LightweightCheckpointStore,
) -> dict:
    persisted = store.summary(job_id)
    compact = checkpoints.load(job_id)
    notification = (
        {} if persisted.get("retention_mode") == "final_only"
        else store.notification_summary(job_id)
    )
    state = {
        **persisted, **compact, **notification,
        "job_id": job_id,
        "retention_mode": persisted.get("retention_mode") or "full",
        "exact_replay_capable": persisted.get("exact_replay_capable", True),
    }
    if int(state.get("selected_count") or 0) <= 0:
        raise ValueError("Discovery finalization has no persisted selection")
    if int(state.get("catalog_pending_count") or 0) != 0:
        raise ValueError("Discovery computation is not complete")
    if store.counts(job_id)["observations"] <= 0:
        raise ValueError("Discovery finalization has no persisted observations")
    return state


def export_payload(state: dict, store: DiscoveryIncrementalStore, job_id: str) -> dict:
    return {
        **state,
        "candidates": IncrementalCandidateCollection(store, job_id),
        "results": IncrementalCandidateCollection(store, job_id, final_only=True),
        "amazon_observations": IncrementalObservationCollection(store, job_id),
    }


def export_offline(
    job_id: str, output_path: str | Path, *, store=None, checkpoints=None,
    governor=None,
) -> dict:
    """Generate a diagnostic workbook without changing job or notification state."""
    store = store or DiscoveryIncrementalStore()
    checkpoints = checkpoints or LightweightCheckpointStore()
    governor = governor or DiscoveryResourceGovernor(
        database_path=store.path, disk_path=Path(output_path).parent,
    )
    state = finalization_state(job_id, store, checkpoints)
    if state.get("retention_mode") == "final_only":
        raise ValueError(
            "Technical export is unavailable for FINAL_ONLY result history"
        )

    def progress(_phase, _current, _total):
        governor.before_next_batch()

    target = Path(output_path).expanduser().resolve()
    write_discovery_excel(
        export_payload(state, store, job_id), str(target), progress=progress,
    )
    final_snapshot = governor.before_next_batch()
    return {
        "path": str(target), "export_state": export_metadata(target, state),
        "technical_export": export_metadata(target, state),
        "resource_snapshot": final_snapshot.as_dict(),
    }


def export_operational_offline(
    job_id: str, output_path: str | Path, *, store=None, checkpoints=None,
    governor=None,
) -> dict:
    """Generate the daily-use workbook without changing persisted job state."""
    store = store or DiscoveryIncrementalStore()
    checkpoints = checkpoints or LightweightCheckpointStore()
    governor = governor or DiscoveryResourceGovernor(
        database_path=store.path, disk_path=Path(output_path).parent,
    )
    state = finalization_state(job_id, store, checkpoints)

    def progress(_phase, _current, _total):
        governor.before_next_batch()

    target = Path(output_path).expanduser().resolve()
    write_discovery_operational_excel(
        export_payload(state, store, job_id), str(target), progress=progress,
    )
    final_snapshot = governor.before_next_batch()
    return {
        "path": str(target), "operational_export": export_metadata(target, state),
        "resource_snapshot": final_snapshot.as_dict(),
    }


def finalize(
    job_id: str, *, registry=None, store=None, checkpoints=None,
    governor=None, send_notification=True, output_path=None,
    automatic_retention=run_automatic_retention,
) -> dict:
    """Export and notify from SQLite only; never invokes Amazon or suppliers."""
    load_env()
    registry = registry or DiscoveryJobRegistry()
    store = store or DiscoveryIncrementalStore()
    checkpoints = checkpoints or LightweightCheckpointStore()
    state = finalization_state(job_id, store, checkpoints)
    runtime = registry.get(job_id)
    if not runtime:
        raise KeyError(job_id)
    if runtime.get("status") not in {
        "computed", "export_pending", "export_running", "export_resource_paused",
        "export_complete", "notification_pending",
    }:
        registry.prepare_finalization(job_id, state)
        runtime = registry.get(job_id)
    pid = os.getpid()
    if not registry.claim_finalization(job_id, pid=pid):
        raise RuntimeError(f"Discovery finalization {job_id} is already owned")
    governor = governor or DiscoveryResourceGovernor(
        database_path=store.path,
        disk_path=PROJECT_ROOT / "data" / "discovery_jobs",
    )
    target = Path(output_path or (
        PROJECT_ROOT / "data" / "discovery_jobs" / f"{job_id}.operational.xlsx"
    )).expanduser().resolve()

    def heartbeat(phase, current, total):
        governor.before_next_batch()
        registry.heartbeat(
            job_id, pid=pid, phase=phase, current=current, total=total,
        )

    try:
        existing_state = dict(state.get("operational_export") or {})
        export_valid = (
            target.exists()
            and existing_state.get("status") == "completed"
            and existing_state.get("sha256") == export_metadata(target, state)["sha256"]
        )
        if not export_valid:
            store.set_phase(job_id, "export_running", status="computed")
            state.update({"status": "computed", "phase": "export_running"})
            checkpoints.save(state)
            heartbeat("export_running", 0, int(state["selected_count"]))
            write_discovery_operational_excel(
                export_payload(state, store, job_id), str(target), progress=heartbeat,
            )
            state["operational_export"] = export_metadata(target, state)
            # Legacy readers continue to resolve the primary workbook here.
            state["export_state"] = dict(state["operational_export"])
            state["finalization_metrics"] = governor.before_next_batch().as_dict()
            state.update({"status": "export_complete", "phase": "export_complete"})
            checkpoints.save(state)
            store.set_phase(job_id, "export_complete", status="export_complete")
        registry.mark_export_complete(job_id, pid=pid, export_path=str(target))
        state.update({"status": "completed", "phase": "completed"})
        checkpoints.save(state)
        heartbeat("notification_pending", int(state["selected_count"]), int(state["selected_count"]))
        notification = None
        if send_notification:
            notification = send_discovery_terminal_notification(
                state, database_path=registry.path, runtime=registry.get(job_id),
            )
        state = store.complete_with_terminal_summary(job_id, state)
        checkpoints.save(state)
        registry.finish(job_id, state, export_path=str(target))
        append_storage_audit_event({
            "event": "workload_finalized", "workload_id": job_id,
            "workload_type": "discovery",
            "universe_size": int(state.get("selected_count") or 0),
            "export_bytes": target.stat().st_size if target.exists() else 0,
            "success": True, "storage_after": collect_storage_metrics(),
            "retention_execution": False,
        }, path=Path(registry.path).parent / "storage-workload-metrics.jsonl")
        try:
            retention_root = Path(registry.path).parent
            if retention_root.name == "data":
                retention_root = retention_root.parent
            retention = automatic_retention(
                trigger_event="discovery_completed", trigger_id=job_id,
                project_root=retention_root,
            )
        except Exception as error:
            logger.exception(
                "RETENTION AUTOMATION CALLBACK FAILED | job_id=%s", job_id,
            )
            retention = {"status": f"RETENTION_ABORT_VERIFICATION:{type(error).__name__}"}
        logger.info(
            "DISCOVERY FINALIZATION COMPLETED | job_id=%s export=%s notification=%s retention=%s",
            job_id, target.name,
            (notification or {}).get("status") if isinstance(notification, dict) else "disabled",
            retention.get("status"),
        )
        return {**state, "notification": notification}
    except ResourcePause as exc:
        metrics = exc.snapshot.as_dict()
        store.record_resource_event(
            job_id, "hard", f"export:{exc.reason}",
            {**metrics, "threshold": exc.threshold},
        )
        store.set_phase(job_id, "export_resource_paused", status="computed")
        paused = {
            **state,
            "status": "export_resource_paused",
            "phase": "export_resource_paused",
            "resource_pause": {
                "reason": exc.reason, "metric": exc.reason,
                "observed": metrics, "threshold": exc.threshold,
                "resumable": True,
            },
        }
        checkpoints.save(paused)
        registry.export_resource_pause(
            job_id, reason=exc.reason, metrics=metrics,
            phase="export_resource_paused",
        )
        logger.warning(
            "DISCOVERY EXPORT RESOURCE PAUSED | job_id=%s reason=%s",
            job_id, exc.reason,
        )
        return paused
    except Exception as exc:
        store.set_phase(job_id, "export_pending", status="computed")
        registry.fail(job_id, f"Discovery finalization failed: {exc}")
        logger.exception("DISCOVERY FINALIZATION FAILED | job_id=%s", job_id)
        raise


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--offline-output")
    parser.add_argument("--no-notification", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    if args.offline_output:
        result = export_offline(args.job_id, args.offline_output)
        print(json.dumps(result, sort_keys=True, default=str))
    else:
        finalize(args.job_id, send_notification=not args.no_notification)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
