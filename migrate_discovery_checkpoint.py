"""Migrate one immutable legacy Discovery checkpoint to incremental SQLite."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from discovery_incremental import (
    DiscoveryIncrementalStore,
    LightweightCheckpointStore,
    read_legacy_metadata,
)
from discovery_rotation import DiscoveryRotationStore
from discovery_jobs import DiscoveryJobRegistry


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--checkpoint", required=True)
    parser.add_argument("--expected-sha256")
    parser.add_argument("--database")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    checkpoint = Path(args.checkpoint).resolve()
    metadata = read_legacy_metadata(checkpoint)
    if args.dry_run:
        print(json.dumps({
            "job_id": metadata["job_id"], "phase": metadata.get("phase"),
            "selected": metadata.get("sampled_identifier_count"),
            "checkpoint": str(checkpoint), "would_migrate": True,
        }, sort_keys=True))
        return 0
    store = DiscoveryIncrementalStore(args.database)
    summary = store.migrate_legacy_checkpoint(
        checkpoint, metadata, expected_sha256=args.expected_sha256,
    )
    compact = {
        **summary,
        "legacy_checkpoint_read_only": True,
        "legacy_checkpoint_preserved": True,
    }
    bytes_written = LightweightCheckpointStore(checkpoint.parent).save(compact)
    store.update_job(
        metadata["job_id"], checkpoint_bytes_written=bytes_written,
    )
    rotation = DiscoveryRotationStore().commit_catalog_results(
        metadata["job_id"], store.definitive_catalog_statuses(metadata["job_id"]),
    )
    DiscoveryJobRegistry().update_recovery_progress(
        metadata["job_id"], phase="catalog",
        current=summary["catalog_completed_count"], total=summary["selected_count"],
    )
    print(json.dumps({
        "job_id": summary["job_id"],
        "selected": summary["selected_count"],
        "catalog_completed": summary["catalog_completed_count"],
        "catalog_pending": summary["catalog_pending_count"],
        "listings": summary["listing_count"],
        "compact_checkpoint_bytes": bytes_written,
        "legacy_preserved": checkpoint.exists(),
        "rotation_analyzed": rotation.get("rotation_analyzed_this_run"),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
