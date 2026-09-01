"""Incremental, restart-safe persistence for large Discovery jobs.

The legacy Discovery checkpoint embeds every supplier scenario and Amazon result
in one JSON document.  This store keeps the immutable selection and each phase's
records in SQLite so completing one batch never rewrites prior batches.
"""

from __future__ import annotations

import hashlib
import json
import logging
import mmap
import os
import random
import sqlite3
import tempfile
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from itertools import islice
from pathlib import Path
from typing import Any, Iterable, Iterator

from discovery_taxonomy import projection_rows


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_DATABASE = PROJECT_ROOT / "data" / "discovery_incremental.sqlite3"
SCHEMA_VERSION = 1
TERMINAL_CATALOG_STATUSES = {
    "resolved", "ambiguous", "not_found", "invalid_identifier",
}
SQLITE_LOCK_RETRY_ATTEMPTS = 5
SQLITE_LOCK_RETRY_BASE_SECONDS = 0.05
SQLITE_LOCK_RETRY_MAX_SECONDS = 0.5
SQLITE_LOCK_RETRY_JITTER_FRACTION = 0.2


logger = logging.getLogger(__name__)


def _transient_sqlite_lock(error: BaseException) -> bool:
    """Return true only for SQLite BUSY/LOCKED acquisition failures."""
    if not isinstance(error, sqlite3.OperationalError):
        return False
    code = getattr(error, "sqlite_errorcode", None)
    if code is not None and (int(code) & 0xFF) in {
        sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED,
    }:
        return True
    message = str(error).casefold()
    return any(value in message for value in (
        "database is locked", "database table is locked",
        "database schema is locked",
    ))


SCHEMA = """
CREATE TABLE IF NOT EXISTS discovery_incremental_jobs (
    job_id TEXT PRIMARY KEY,
    schema_version INTEGER NOT NULL,
    status TEXT NOT NULL,
    phase TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    legacy_checkpoint_path TEXT,
    legacy_checkpoint_sha256 TEXT,
    selected_count INTEGER NOT NULL DEFAULT 0,
    catalog_completed_count INTEGER NOT NULL DEFAULT 0,
    last_completed_batch INTEGER NOT NULL DEFAULT 0,
    checkpoint_bytes_written INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS discovery_job_items (
    job_id TEXT NOT NULL,
    sequence_no INTEGER NOT NULL,
    canonical_identifier TEXT NOT NULL,
    identifier_type TEXT,
    product_json TEXT NOT NULL,
    catalog_status TEXT,
    pricing_status TEXT,
    fees_status TEXT,
    terminal_status TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (job_id, canonical_identifier),
    UNIQUE (job_id, sequence_no),
    FOREIGN KEY (job_id) REFERENCES discovery_incremental_jobs(job_id)
);
CREATE TABLE IF NOT EXISTS discovery_purchase_scenarios (
    job_id TEXT NOT NULL,
    canonical_identifier TEXT NOT NULL,
    scenario_id TEXT NOT NULL,
    scenario_json TEXT NOT NULL,
    supplier TEXT,
    PRIMARY KEY (job_id, canonical_identifier, scenario_id)
);
CREATE TABLE IF NOT EXISTS discovery_listing_classifications (
    job_id TEXT NOT NULL,
    canonical_identifier TEXT NOT NULL,
    asin TEXT NOT NULL,
    marketplace_id TEXT NOT NULL,
    path_hash TEXT NOT NULL,
    classification_id TEXT NOT NULL,
    parent_id TEXT,
    depth INTEGER NOT NULL,
    display_name TEXT,
    is_leaf INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (
        job_id,canonical_identifier,asin,marketplace_id,path_hash,classification_id
    )
);
CREATE TABLE IF NOT EXISTS discovery_catalog_results (
    job_id TEXT NOT NULL,
    canonical_identifier TEXT NOT NULL,
    catalog_status TEXT NOT NULL,
    diagnostics_json TEXT NOT NULL DEFAULT '{}',
    updated_at TEXT NOT NULL,
    PRIMARY KEY (job_id, canonical_identifier)
);
CREATE TABLE IF NOT EXISTS discovery_listings (
    job_id TEXT NOT NULL,
    canonical_identifier TEXT NOT NULL,
    asin TEXT NOT NULL,
    listing_json TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (job_id, canonical_identifier, asin)
);
CREATE TABLE IF NOT EXISTS discovery_observations (
    job_id TEXT NOT NULL,
    observation_id TEXT NOT NULL,
    observation_json TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (job_id, observation_id)
);
CREATE TABLE IF NOT EXISTS discovery_combinations (
    job_id TEXT NOT NULL,
    combination_id TEXT NOT NULL,
    canonical_identifier TEXT NOT NULL,
    combination_json TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (job_id, combination_id)
);
CREATE TABLE IF NOT EXISTS discovery_resource_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL,
    level TEXT NOT NULL,
    reason TEXT NOT NULL,
    metrics_json TEXT NOT NULL,
    observed_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_discovery_items_catalog_pending
ON discovery_job_items(job_id, catalog_status, sequence_no);
CREATE INDEX IF NOT EXISTS idx_discovery_listings_asin
ON discovery_listings(job_id, asin);
CREATE INDEX IF NOT EXISTS idx_discovery_scenarios_supplier
ON discovery_purchase_scenarios(job_id,supplier,canonical_identifier);
CREATE INDEX IF NOT EXISTS idx_discovery_taxonomy_node_identifier
ON discovery_listing_classifications(marketplace_id,classification_id,canonical_identifier);
CREATE INDEX IF NOT EXISTS idx_discovery_taxonomy_identifier
ON discovery_listing_classifications(canonical_identifier,marketplace_id);
CREATE INDEX IF NOT EXISTS idx_discovery_taxonomy_job_identifier
ON discovery_listing_classifications(job_id,canonical_identifier);
"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _dump(value: Any) -> str:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        default=str,
    )


def _scenario_identity(candidate_identifier: str, scenario: dict[str, Any], index: int) -> str:
    explicit = scenario.get("scenario_id") or scenario.get("id")
    if explicit:
        return str(explicit)
    stable = {
        key: scenario.get(key)
        for key in (
            "supplier", "scenario", "scenario_type", "supplier_product_id",
            "supplier_option_id", "supplier_sku", "offer_qid", "offerQid",
            "seller", "seller_alias", "tier", "minimum_order_quantity",
            "moq", "selling_unit", "warehouse", "mode",
        )
    }
    digest = hashlib.sha256(
        f"{candidate_identifier}|{index}|{_dump(stable)}".encode("utf-8")
    ).hexdigest()[:24]
    return f"scenario_{digest}"


def iter_json_array(path: str | Path, key: str, *, chunk_size: int = 1024 * 1024) -> Iterator[dict[str, Any]]:
    """Stream objects from a named top-level array without loading the file."""
    marker = f'"{key}"'.encode("utf-8")
    decoder = json.JSONDecoder()
    with Path(path).open("rb") as source:
        buffer = b""
        while True:
            chunk = source.read(chunk_size)
            if not chunk:
                raise ValueError(f"JSON array {key!r} not found")
            buffer += chunk
            position = buffer.find(marker)
            if position >= 0:
                bracket = buffer.find(b"[", position + len(marker))
                if bracket >= 0:
                    buffer = buffer[bracket + 1:]
                    break
            if len(buffer) > len(marker) + 32:
                buffer = buffer[-(len(marker) + 32):]
        text = buffer.decode("utf-8")
        eof = False
        while True:
            text = text.lstrip()
            if text.startswith("]"):
                return
            if text.startswith(","):
                text = text[1:].lstrip()
            while True:
                try:
                    value, end = decoder.raw_decode(text)
                    break
                except json.JSONDecodeError:
                    chunk = source.read(chunk_size)
                    if not chunk:
                        eof = True
                        break
                    text += chunk.decode("utf-8")
            if eof:
                raise ValueError(f"Truncated JSON array {key!r}")
            if not isinstance(value, dict):
                raise ValueError(f"Expected object in JSON array {key!r}")
            yield value
            text = text[end:]


def read_legacy_metadata(path: str | Path) -> dict[str, Any]:
    """Read selected top-level metadata without materializing heavy arrays."""
    fields = (
        "job_id", "schema_version", "discovery_schema_version", "status", "phase",
        "filters", "created_at", "started_at", "completed_at", "updated_at",
        "selected_suppliers", "supplier_snapshot_set", "supplier_warnings",
        "supplier_coverage", "usable_suppliers", "run_budget",
        "sampled_identifier_count", "total_supplier_ean_universe",
        "rotation_scope", "rotation_cycle_id", "rotation_universe_count",
        "rotation_analyzed_before_run", "rotation_analyzed_this_run",
        "rotation_remaining_after_run", "sampling_strategy", "funnel",
        "progress_phase", "progress_current", "progress_total", "errors",
        "export_state",
        "qogita_category_filter_enabled",
        "qogita_category_filter_mode",
        "qogita_category_selected_parent_ids",
        "qogita_category_child_overrides",
        "qogita_category_include_unknown", "qogita_category_only_beauty",
        "qogita_taxonomy_schema_version", "qogita_category_marketplace_id",
    )
    decoder = json.JSONDecoder()
    result: dict[str, Any] = {}
    with Path(path).open("rb") as source, mmap.mmap(
        source.fileno(), 0, access=mmap.ACCESS_READ
    ) as mapped:
        for field in fields:
            marker = f'"{field}":'.encode("utf-8")
            position = mapped.rfind(marker)
            if position < 0:
                continue
            start = position + len(marker)
            size = 4096
            while size <= 64 * 1024 * 1024:
                sample = mapped[start:min(len(mapped), start + size)].decode("utf-8")
                try:
                    value, _ = decoder.raw_decode(sample.lstrip())
                    result[field] = value
                    break
                except json.JSONDecodeError:
                    size *= 2
            else:
                raise ValueError(f"Legacy metadata field {field!r} exceeds safety limit")
    if not result.get("job_id"):
        raise ValueError("Legacy checkpoint has no job_id")
    return result


class DiscoveryIncrementalStore:
    def __init__(
        self, path: str | Path | None = None, *,
        lock_retry_attempts: int = SQLITE_LOCK_RETRY_ATTEMPTS,
        lock_retry_base_seconds: float = SQLITE_LOCK_RETRY_BASE_SECONDS,
        lock_retry_max_seconds: float = SQLITE_LOCK_RETRY_MAX_SECONDS,
        lock_retry_jitter_fraction: float = SQLITE_LOCK_RETRY_JITTER_FRACTION,
        busy_timeout_ms: int = 30_000, sleep_func=time.sleep,
        random_func=random.random,
    ):
        configured = path or os.environ.get("DISCOVERY_INCREMENTAL_DATABASE")
        self.path = Path(configured or DEFAULT_DATABASE).expanduser().resolve()
        self.lock_retry_attempts = max(1, int(lock_retry_attempts))
        self.lock_retry_base_seconds = max(0.0, float(lock_retry_base_seconds))
        self.lock_retry_max_seconds = max(
            self.lock_retry_base_seconds, float(lock_retry_max_seconds)
        )
        self.lock_retry_jitter_fraction = max(
            0.0, float(lock_retry_jitter_fraction)
        )
        self.busy_timeout_ms = max(0, int(busy_timeout_ms))
        self._sleep = sleep_func
        self._random = random_func

    def _new_connection(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(
            self.path, timeout=max(0.001, self.busy_timeout_ms / 1000.0)
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
        connection.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
        return connection

    @contextmanager
    def _connect(self):
        """Yield one connection and always close it after the unit of work.

        ``sqlite3.Connection.__exit__`` commits or rolls back but deliberately
        does not close the connection.  The old ``with self._connect()`` usage
        therefore retained thousands of connections until cyclic GC ran.
        """
        connection = self._new_connection()
        try:
            yield connection
        finally:
            connection.close()

    def initialize(self):
        with self._connect() as connection:
            columns = {
                str(row[1]) for row in connection.execute(
                    "PRAGMA table_info(discovery_purchase_scenarios)"
                )
            }
            if columns and "supplier" not in columns:
                connection.execute(
                    "ALTER TABLE discovery_purchase_scenarios ADD COLUMN supplier TEXT"
                )
            connection.executescript(SCHEMA)

    def _immediate_transaction(self, operation, *, job_id: str, name: str):
        """Run one idempotent write unit with bounded BUSY/LOCKED recovery."""
        started = time.monotonic()
        for attempt in range(1, self.lock_retry_attempts + 1):
            try:
                with self._connect() as connection:
                    try:
                        connection.execute("BEGIN IMMEDIATE")
                        result = operation(connection)
                        connection.commit()
                        return result
                    except BaseException:
                        if connection.in_transaction:
                            connection.rollback()
                        raise
            except sqlite3.OperationalError as error:
                if not _transient_sqlite_lock(error):
                    raise
                elapsed = time.monotonic() - started
                if attempt >= self.lock_retry_attempts:
                    logger.error(
                        "DISCOVERY SQLITE LOCK EXHAUSTED | job_id=%s operation=%s "
                        "attempt=%s elapsed=%.3f",
                        job_id, name, attempt, elapsed,
                    )
                    raise
                base_delay = min(
                    self.lock_retry_base_seconds * (2 ** (attempt - 1)),
                    self.lock_retry_max_seconds,
                )
                jitter = (
                    base_delay * self.lock_retry_jitter_fraction * self._random()
                )
                delay = min(base_delay + jitter, self.lock_retry_max_seconds)
                logger.warning(
                    "DISCOVERY SQLITE LOCK RETRY | job_id=%s operation=%s "
                    "attempt=%s elapsed=%.3f backoff=%.3f",
                    job_id, name, attempt, elapsed, delay,
                )
                self._sleep(delay)
        raise AssertionError("unreachable SQLite retry state")

    @staticmethod
    def _replace_classification_projection(
        connection, job_id: str, identifier: str,
        listings: Iterable[dict[str, Any]],
    ) -> None:
        connection.execute(
            "DELETE FROM discovery_listing_classifications WHERE job_id=? AND canonical_identifier=?",
            (job_id, identifier),
        )
        rows = [
            row for listing in listings
            for row in projection_rows(job_id, identifier, listing)
        ]
        if rows:
            connection.executemany(
                """INSERT INTO discovery_listing_classifications
                   (job_id,canonical_identifier,asin,marketplace_id,path_hash,
                    classification_id,parent_id,depth,display_name,is_leaf)
                   VALUES (?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT DO UPDATE SET parent_id=excluded.parent_id,
                     depth=excluded.depth,display_name=excluded.display_name,
                     is_leaf=excluded.is_leaf""",
                rows,
            )

    @classmethod
    def _replace_classification_projection_batch(
        cls, connection, job_id: str,
        candidates: Iterable[dict[str, Any]],
    ) -> None:
        values = list(candidates)
        identifiers = [
            str(value.get("canonical_ean") or value.get("gtin") or "").strip()
            for value in values
        ]
        identifiers = [value for value in identifiers if value]
        if identifiers:
            placeholders = ",".join("?" for _ in identifiers)
            connection.execute(
                f"""DELETE FROM discovery_listing_classifications
                    WHERE job_id=? AND canonical_identifier IN ({placeholders})""",
                (job_id, *identifiers),
            )
        rows = [
            row for candidate in values
            for listing in candidate.get("amazon_listings") or []
            for row in projection_rows(
                job_id,
                str(candidate.get("canonical_ean") or candidate.get("gtin") or "").strip(),
                listing,
            )
        ]
        if rows:
            connection.executemany(
                """INSERT INTO discovery_listing_classifications
                   (job_id,canonical_identifier,asin,marketplace_id,path_hash,
                    classification_id,parent_id,depth,display_name,is_leaf)
                   VALUES (?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT DO UPDATE SET parent_id=excluded.parent_id,
                     depth=excluded.depth,display_name=excluded.display_name,
                     is_leaf=excluded.is_leaf""",
                rows,
            )

    def has_job(self, job_id: str) -> bool:
        self.initialize()
        with self._connect() as connection:
            return connection.execute(
                "SELECT 1 FROM discovery_incremental_jobs WHERE job_id=?", (job_id,)
            ).fetchone() is not None

    def create_job(
        self, metadata: dict[str, Any], candidates: Iterable[dict[str, Any]], *,
        legacy_checkpoint_path: str | Path | None = None,
        legacy_checkpoint_sha256: str | None = None,
        batch_size: int = 500, progress=None, resource_governor=None,
    ) -> dict[str, Any]:
        self.initialize()
        job_id = str(metadata["job_id"])
        target_phase = str(metadata.get("phase") or "suppliers_loaded")
        target_status = str(metadata.get("status") or "running")
        total = int(metadata.get("prepared_total") or metadata.get("sampled_identifier_count") or 0)
        compact_metadata = dict(metadata)
        compact_metadata.pop("rotation_selected_identifiers", None)
        compact_metadata.update({
            "phase": "preparing", "status": "running",
            "prepared_total": total, "preparation_complete": False,
        })
        observed = _now()
        def initialize_job(connection):
            connection.execute(
                """INSERT INTO discovery_incremental_jobs
                   (job_id,schema_version,status,phase,metadata_json,
                    legacy_checkpoint_path,legacy_checkpoint_sha256,created_at,updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(job_id) DO NOTHING""",
                (
                    job_id, SCHEMA_VERSION, "running", "preparing", _dump(compact_metadata),
                    str(legacy_checkpoint_path) if legacy_checkpoint_path else None,
                    legacy_checkpoint_sha256, metadata.get("created_at") or observed, observed,
                ),
            )
            job = connection.execute(
                "SELECT * FROM discovery_incremental_jobs WHERE job_id=?", (job_id,),
            ).fetchone()
            existing_metadata = json.loads(job["metadata_json"] or "{}")
            existing = int(job["selected_count"] or 0)
            completed = int(job["catalog_completed_count"] or 0)
            preparation_incomplete = (
                str(job["phase"]) == "preparing"
                or (
                    existing_metadata.get("prepared_total")
                    and not existing_metadata.get("preparation_complete")
                )
            )
            if existing and not preparation_incomplete:
                return True, existing, completed
            compact_metadata["prepared_current"] = existing
            connection.execute(
                """UPDATE discovery_incremental_jobs
                      SET status='running',phase='preparing',metadata_json=?,updated_at=?
                    WHERE job_id=?""",
                (_dump(compact_metadata), observed, job_id),
            )
            return False, existing, completed

        already_prepared, existing, completed = self._immediate_transaction(
            initialize_job, job_id=job_id, name="create_job.initialize",
        )
        if already_prepared:
            return self.summary(job_id)

        iterator = iter(candidates)
        supplied_start = int(metadata.get("preparation_start_sequence") or 0)
        if existing and supplied_start < existing:
            for _ in islice(iterator, existing - supplied_start):
                pass
        selected = existing
        while True:
            if resource_governor is not None:
                resource_governor.before_next_batch()
            rows = list(islice(iterator, max(1, int(batch_size))))
            if not rows:
                break
            observed = _now()
            item_rows = []
            scenario_rows = []
            catalog_rows = []
            listing_rows = []
            batch_completed = 0
            for offset, candidate in enumerate(rows):
                sequence_no = selected + offset
                identifier = str(
                    candidate.get("canonical_ean") or candidate.get("gtin") or ""
                ).strip()
                if not identifier:
                    raise ValueError(f"Candidate {sequence_no} has no canonical identifier")
                scenarios = list(candidate.get("scenarios") or [])
                listings = list(candidate.get("amazon_listings") or [])
                product = dict(candidate)
                product.pop("scenarios", None)
                product.pop("amazon_listings", None)
                product.pop("opportunity_combinations", None)
                catalog_status = candidate.get("catalog_status")
                item_rows.append((
                    job_id, sequence_no, identifier, candidate.get("identifier_type"),
                    _dump(product), catalog_status, candidate.get("pricing_status"),
                    candidate.get("fee_status"), candidate.get("evaluation_status"), observed,
                ))
                batch_completed += int(
                    str(catalog_status or "") in TERMINAL_CATALOG_STATUSES
                )
                for index, scenario in enumerate(scenarios):
                    scenario_id = _scenario_identity(identifier, scenario, index)
                    payload = dict(scenario)
                    payload.setdefault("scenario_id", scenario_id)
                    scenario_rows.append((
                        job_id, identifier, scenario_id, _dump(payload),
                        str(payload.get("supplier") or "").strip().lower() or None,
                    ))
                if catalog_status:
                    catalog_rows.append((
                        job_id, identifier, catalog_status,
                        _dump(candidate.get("catalog_diagnostics") or {}), observed,
                    ))
                for listing in listings:
                    asin = str(listing.get("asin") or "").strip()
                    if not asin:
                        continue
                    listing_rows.append((job_id, identifier, asin, _dump(listing), observed))
            next_selected = selected + len(rows)
            next_completed = completed + batch_completed
            compact_metadata["prepared_current"] = next_selected
            def write_preparation_batch(connection):
                connection.executemany(
                    """INSERT INTO discovery_job_items
                       (job_id,sequence_no,canonical_identifier,identifier_type,
                        product_json,catalog_status,pricing_status,fees_status,
                        terminal_status,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?)
                       ON CONFLICT(job_id,canonical_identifier) DO UPDATE SET
                         sequence_no=excluded.sequence_no,identifier_type=excluded.identifier_type,
                         product_json=excluded.product_json,catalog_status=excluded.catalog_status,
                         pricing_status=excluded.pricing_status,fees_status=excluded.fees_status,
                         terminal_status=excluded.terminal_status,updated_at=excluded.updated_at""",
                    item_rows,
                )
                connection.executemany(
                    """INSERT INTO discovery_purchase_scenarios
                       (job_id,canonical_identifier,scenario_id,scenario_json,supplier)
                       VALUES (?,?,?,?,?)
                       ON CONFLICT(job_id,canonical_identifier,scenario_id) DO UPDATE SET
                         scenario_json=excluded.scenario_json,supplier=excluded.supplier""",
                    scenario_rows,
                )
                connection.executemany(
                    """INSERT INTO discovery_catalog_results
                       (job_id,canonical_identifier,catalog_status,diagnostics_json,updated_at)
                       VALUES (?,?,?,?,?)
                       ON CONFLICT(job_id,canonical_identifier) DO UPDATE SET
                         catalog_status=excluded.catalog_status,
                         diagnostics_json=excluded.diagnostics_json,updated_at=excluded.updated_at""",
                    catalog_rows,
                )
                connection.executemany(
                    """INSERT INTO discovery_listings
                       (job_id,canonical_identifier,asin,listing_json,updated_at)
                       VALUES (?,?,?,?,?)
                       ON CONFLICT(job_id,canonical_identifier,asin) DO UPDATE SET
                         listing_json=excluded.listing_json,updated_at=excluded.updated_at""",
                    listing_rows,
                )
                self._replace_classification_projection_batch(connection, job_id, rows)
                connection.execute(
                    """UPDATE discovery_incremental_jobs
                         SET selected_count=?,catalog_completed_count=?,metadata_json=?,updated_at=?
                       WHERE job_id=?""",
                    (next_selected, next_completed, _dump(compact_metadata), observed, job_id),
                )
            self._immediate_transaction(
                write_preparation_batch, job_id=job_id,
                name="create_job.preparation_batch",
            )
            selected, completed = next_selected, next_completed
            if progress is not None:
                progress("preparing", selected, total or selected)

        if total and selected != total:
            raise ValueError(
                f"Preparation produced {selected} of {total} frozen candidates"
            )
        compact_metadata.update({
            "phase": target_phase, "status": target_status,
            "prepared_current": selected, "prepared_total": total or selected,
            "preparation_complete": True, "preparation_completed_at": _now(),
        })
        def complete_preparation(connection):
            connection.execute(
                """UPDATE discovery_incremental_jobs
                     SET status=?,phase=?,selected_count=?,catalog_completed_count=?,
                         metadata_json=?,updated_at=? WHERE job_id=?""",
                (
                    target_status, target_phase, selected, completed,
                    _dump(compact_metadata), _now(), job_id,
                ),
            )
        self._immediate_transaction(
            complete_preparation, job_id=job_id,
            name="create_job.complete_preparation",
        )
        return self.summary(job_id)

    def migrate_legacy_checkpoint(
        self, checkpoint_path: str | Path, metadata: dict[str, Any], *,
        expected_sha256: str | None = None,
    ) -> dict[str, Any]:
        path = Path(checkpoint_path).resolve()
        digest = hashlib.sha256()
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
        checksum = digest.hexdigest()
        if expected_sha256 and checksum != expected_sha256:
            raise ValueError("Legacy checkpoint checksum changed before migration")
        return self.create_job(
            metadata, iter_json_array(path, "candidates"),
            legacy_checkpoint_path=path, legacy_checkpoint_sha256=checksum,
        )

    def summary(self, job_id: str, *, connection=None) -> dict[str, Any]:
        self.initialize()
        owns = connection is None
        connection = connection or self._new_connection()
        try:
            job = connection.execute(
                "SELECT * FROM discovery_incremental_jobs WHERE job_id=?", (job_id,)
            ).fetchone()
            if not job:
                raise KeyError(job_id)
            statuses = {
                (row["catalog_status"] or "pending"): int(row["amount"])
                for row in connection.execute(
                    """SELECT catalog_status,COUNT(*) amount FROM discovery_job_items
                       WHERE job_id=? GROUP BY catalog_status""", (job_id,)
                )
            }
            listings = connection.execute(
                "SELECT COUNT(*) FROM discovery_listings WHERE job_id=?", (job_id,)
            ).fetchone()[0]
            scenarios = connection.execute(
                "SELECT COUNT(*) FROM discovery_purchase_scenarios WHERE job_id=?", (job_id,)
            ).fetchone()[0]
            metadata = json.loads(job["metadata_json"] or "{}")
            completed = sum(statuses.get(value, 0) for value in TERMINAL_CATALOG_STATUSES)
            return {
                **metadata,
                "job_id": job_id,
                "incremental_schema_version": int(job["schema_version"]),
                "incremental_store": str(self.path),
                "status": job["status"], "phase": job["phase"],
                "selected_count": int(job["selected_count"]),
                "catalog_completed_count": completed,
                "catalog_pending_count": int(job["selected_count"]) - completed,
                "catalog_status_counts": statuses,
                "listing_count": listings, "scenario_count": scenarios,
                "legacy_checkpoint_path": job["legacy_checkpoint_path"],
                "legacy_checkpoint_sha256": job["legacy_checkpoint_sha256"],
                "last_completed_batch": int(job["last_completed_batch"]),
                "checkpoint_bytes_written": int(job["checkpoint_bytes_written"]),
                "updated_at": job["updated_at"],
            }
        finally:
            if owns:
                connection.close()

    def pending_catalog_batch(self, job_id: str, limit: int = 20) -> list[dict[str, Any]]:
        self.initialize()
        with self._connect() as connection:
            rows = connection.execute(
                """SELECT * FROM discovery_job_items
                   WHERE job_id=? AND catalog_status IS NULL
                   ORDER BY sequence_no LIMIT ?""", (job_id, int(limit)),
            ).fetchall()
            return [self._hydrate_item(connection, row) for row in rows]

    def iter_definitive_catalog_status_batches(
        self, job_id: str, batch_size: int = 500,
    ) -> Iterator[dict[str, str]]:
        """Stream committed Catalog statuses for idempotent rotation recovery."""
        self.initialize()
        with self._connect() as connection:
            cursor = connection.execute(
                """SELECT canonical_identifier,catalog_status
                   FROM discovery_job_items
                   WHERE job_id=? AND catalog_status IN
                     ('resolved','ambiguous','not_found','invalid_identifier')
                   ORDER BY sequence_no""",
                (job_id,),
            )
            while rows := cursor.fetchmany(int(batch_size)):
                yield {
                    str(row["canonical_identifier"]): str(row["catalog_status"])
                    for row in rows
                }

    def requeue_catalog_incomplete(self, job_id: str) -> int:
        """Make prior incomplete Catalog results eligible for one resume attempt."""
        observed = _now()
        with self._connect() as connection:
            cursor = connection.execute(
                """UPDATE discovery_job_items SET catalog_status=NULL,updated_at=?
                   WHERE job_id=? AND catalog_status='catalog_incomplete'""",
                (observed, job_id),
            )
            connection.commit()
            return int(cursor.rowcount)

    def _hydrate_item(self, connection, row) -> dict[str, Any]:
        product = json.loads(row["product_json"])
        product["scenarios"] = [
            json.loads(value[0]) for value in connection.execute(
                """SELECT scenario_json FROM discovery_purchase_scenarios
                   WHERE job_id=? AND canonical_identifier=? ORDER BY scenario_id""",
                (row["job_id"], row["canonical_identifier"]),
            )
        ]
        product["amazon_listings"] = [
            json.loads(value[0]) for value in connection.execute(
                """SELECT listing_json FROM discovery_listings
                   WHERE job_id=? AND canonical_identifier=? ORDER BY asin""",
                (row["job_id"], row["canonical_identifier"]),
            )
        ]
        if row["catalog_status"]:
            product["catalog_status"] = row["catalog_status"]
        return product

    def iter_candidates(self, job_id: str, *, batch_size: int = 250) -> Iterator[dict[str, Any]]:
        for batch in self.iter_candidate_batches(job_id, batch_size=batch_size):
            yield from batch

    def iter_candidate_batches(
        self, job_id: str, *, batch_size: int = 250,
    ) -> Iterator[list[dict[str, Any]]]:
        """Hydrate one bounded page with three set-based reads, without N+1."""
        self.initialize()
        last_sequence = -1
        while True:
            with self._connect() as connection:
                rows = connection.execute(
                    """SELECT * FROM discovery_job_items WHERE job_id=?
                         AND sequence_no>?
                       ORDER BY sequence_no LIMIT ?""",
                    (job_id, last_sequence, int(batch_size)),
                ).fetchall()
                if not rows:
                    return
                identifiers = [str(row["canonical_identifier"]) for row in rows]
                placeholders = ",".join("?" for _ in identifiers)
                parameters = (job_id, *identifiers)
                scenarios: dict[str, list[dict[str, Any]]] = {}
                for scenario in connection.execute(
                    f"""SELECT canonical_identifier,scenario_json
                        FROM discovery_purchase_scenarios
                        WHERE job_id=? AND canonical_identifier IN ({placeholders})
                        ORDER BY canonical_identifier,scenario_id""", parameters,
                ):
                    scenarios.setdefault(str(scenario["canonical_identifier"]), []).append(
                        json.loads(scenario["scenario_json"])
                    )
                listings: dict[str, list[dict[str, Any]]] = {}
                for listing in connection.execute(
                    f"""SELECT canonical_identifier,listing_json
                        FROM discovery_listings
                        WHERE job_id=? AND canonical_identifier IN ({placeholders})
                        ORDER BY canonical_identifier,asin""", parameters,
                ):
                    listings.setdefault(str(listing["canonical_identifier"]), []).append(
                        json.loads(listing["listing_json"])
                    )
                hydrated = []
                for row in rows:
                    identifier = str(row["canonical_identifier"])
                    product = json.loads(row["product_json"])
                    product["scenarios"] = scenarios.get(identifier, [])
                    product["amazon_listings"] = listings.get(identifier, [])
                    if row["catalog_status"]:
                        product["catalog_status"] = row["catalog_status"]
                    hydrated.append(product)
                last_sequence = int(rows[-1]["sequence_no"])
            if not hydrated:
                return
            yield hydrated

    def classification_paths_for_identifiers(
        self, job_id: str, identifiers: Iterable[str],
    ) -> dict[str, list[tuple[str, ...]]]:
        """Read normalized taxonomy paths for a bounded identifier batch."""
        by_listing = self.classification_paths_for_listings(job_id, identifiers)
        return {
            identifier: [path for paths in listings.values() for path in paths]
            for identifier, listings in by_listing.items()
        }

    def classification_paths_for_listings(
        self, job_id: str, identifiers: Iterable[str],
    ) -> dict[str, dict[str, list[tuple[str, ...]]]]:
        """Read normalized paths grouped by product and Amazon listing."""
        values = list(dict.fromkeys(str(value) for value in identifiers if value))
        result: dict[str, dict[str, list[tuple[str, ...]]]] = {
            value: {} for value in values
        }
        if not values:
            return result
        placeholders = ",".join("?" for _ in values)
        with self._connect() as connection:
            rows = connection.execute(
                f"""SELECT canonical_identifier,asin,path_hash,classification_id,depth
                    FROM discovery_listing_classifications
                    WHERE job_id=? AND canonical_identifier IN ({placeholders})
                    ORDER BY canonical_identifier,path_hash,depth""",
                (job_id, *values),
            ).fetchall()
        grouped: dict[tuple[str, str, str], list[str]] = {}
        for row in rows:
            key = (
                str(row["canonical_identifier"]), str(row["asin"]),
                str(row["path_hash"]),
            )
            grouped.setdefault(key, []).append(str(row["classification_id"]))
        for (identifier, asin, _), path in grouped.items():
            result.setdefault(identifier, {}).setdefault(asin, []).append(tuple(path))
        return result

    def iter_export_candidates(
        self, job_id: str, *, batch_size: int = 250, final_only: bool = False,
    ) -> Iterator[dict[str, Any]]:
        """Stream fully hydrated export rows with bounded SQL prefetches.

        Export needs scenarios, listings, observations and combinations, but
        opening a connection for every product caused tens of thousands of
        live sqlite handles before cyclic GC.  This iterator owns exactly one
        connection per bounded page and bulk-loads every related table.
        """
        self.initialize()
        offset = 0
        while True:
            predicate = (
                " AND json_extract(product_json,'$.is_final_result')=1"
                if final_only else ""
            )
            with self._connect() as connection:
                item_rows = connection.execute(
                    f"""SELECT * FROM discovery_job_items WHERE job_id=?{predicate}
                        ORDER BY sequence_no LIMIT ? OFFSET ?""",
                    (job_id, int(batch_size), offset),
                ).fetchall()
                if not item_rows:
                    return
                identifiers = [str(row["canonical_identifier"]) for row in item_rows]
                placeholders = ",".join("?" for _ in identifiers)
                parameters = (job_id, *identifiers)
                scenario_rows = connection.execute(
                    f"""SELECT canonical_identifier,scenario_json
                        FROM discovery_purchase_scenarios
                        WHERE job_id=? AND canonical_identifier IN ({placeholders})
                        ORDER BY canonical_identifier,scenario_id""", parameters,
                ).fetchall()
                listing_rows = connection.execute(
                    f"""SELECT canonical_identifier,listing_json
                        FROM discovery_listings
                        WHERE job_id=? AND canonical_identifier IN ({placeholders})
                        ORDER BY canonical_identifier,asin""", parameters,
                ).fetchall()
                combination_rows = connection.execute(
                    f"""SELECT canonical_identifier,combination_json
                        FROM discovery_combinations
                        WHERE job_id=? AND canonical_identifier IN ({placeholders})
                        ORDER BY canonical_identifier,combination_id""", parameters,
                ).fetchall()

                scenarios: dict[str, list[dict[str, Any]]] = {}
                for row in scenario_rows:
                    scenarios.setdefault(str(row["canonical_identifier"]), []).append(
                        json.loads(row["scenario_json"])
                    )
                listings: dict[str, list[dict[str, Any]]] = {}
                observation_ids: set[str] = set()
                for row in listing_rows:
                    identifier = str(row["canonical_identifier"])
                    listing = json.loads(row["listing_json"])
                    listings.setdefault(identifier, []).append(listing)
                    if listing.get("amazon_observation_id"):
                        observation_ids.add(str(listing["amazon_observation_id"]))
                combinations: dict[str, list[dict[str, Any]]] = {}
                for row in combination_rows:
                    identifier = str(row["canonical_identifier"])
                    combination = json.loads(row["combination_json"])
                    combinations.setdefault(identifier, []).append(combination)
                    if combination.get("amazon_observation_id"):
                        observation_ids.add(str(combination["amazon_observation_id"]))

                observations: dict[str, dict[str, Any]] = {}
                if observation_ids:
                    observation_placeholders = ",".join("?" for _ in observation_ids)
                    for row in connection.execute(
                        f"""SELECT observation_id,observation_json
                            FROM discovery_observations
                            WHERE job_id=? AND observation_id IN ({observation_placeholders})""",
                        (job_id, *sorted(observation_ids)),
                    ):
                        observations[str(row["observation_id"])] = json.loads(
                            row["observation_json"]
                        )

                hydrated = []
                for row in item_rows:
                    identifier = str(row["canonical_identifier"])
                    product = json.loads(row["product_json"])
                    product["scenarios"] = scenarios.get(identifier, [])
                    product["amazon_listings"] = listings.get(identifier, [])
                    product["opportunity_combinations"] = combinations.get(identifier, [])
                    related_ids = {
                        str(value.get("amazon_observation_id"))
                        for value in (
                            product["amazon_listings"]
                            + product["opportunity_combinations"]
                        )
                        if value.get("amazon_observation_id")
                    }
                    product["amazon_observations"] = [
                        observations[value] for value in sorted(related_ids)
                        if value in observations
                    ]
                    if row["catalog_status"]:
                        product["catalog_status"] = row["catalog_status"]
                    hydrated.append(product)
            yield from hydrated
            offset += len(hydrated)

    def commit_catalog_batch(
        self, job_id: str, candidates: Iterable[dict[str, Any]], *, batch_number: int,
    ) -> dict[str, Any]:
        observed = _now()
        rows = list(candidates)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            for candidate in rows:
                identifier = str(
                    candidate.get("canonical_ean") or candidate.get("gtin") or ""
                )
                status = str(candidate.get("catalog_status") or "")
                if not status:
                    raise ValueError(f"Catalog result missing for {identifier}")
                connection.execute(
                    """INSERT INTO discovery_catalog_results
                       (job_id,canonical_identifier,catalog_status,diagnostics_json,updated_at)
                       VALUES (?,?,?,?,?) ON CONFLICT(job_id,canonical_identifier) DO UPDATE SET
                         catalog_status=excluded.catalog_status,
                         diagnostics_json=excluded.diagnostics_json,updated_at=excluded.updated_at""",
                    (
                        job_id, identifier, status,
                        _dump(candidate.get("catalog_diagnostics") or {}), observed,
                    ),
                )
                connection.execute(
                    "DELETE FROM discovery_listings WHERE job_id=? AND canonical_identifier=?",
                    (job_id, identifier),
                )
                for listing in candidate.get("amazon_listings") or []:
                    asin = str(listing.get("asin") or "").strip()
                    if asin:
                        connection.execute(
                            """INSERT INTO discovery_listings
                               (job_id,canonical_identifier,asin,listing_json,updated_at)
                               VALUES (?,?,?,?,?)""",
                            (job_id, identifier, asin, _dump(listing), observed),
                        )
                product = dict(candidate)
                product.pop("scenarios", None)
                product.pop("amazon_listings", None)
                product.pop("opportunity_combinations", None)
                connection.execute(
                    """UPDATE discovery_job_items SET product_json=?,catalog_status=?,updated_at=?
                       WHERE job_id=? AND canonical_identifier=?""",
                    (_dump(product), status, observed, job_id, identifier),
                )
            self._replace_classification_projection_batch(connection, job_id, rows)
            completed = connection.execute(
                """SELECT COUNT(*) FROM discovery_job_items
                   WHERE job_id=? AND catalog_status IN ('resolved','ambiguous','not_found','invalid_identifier')""",
                (job_id,),
            ).fetchone()[0]
            total = connection.execute(
                "SELECT COUNT(*) FROM discovery_job_items WHERE job_id=?", (job_id,)
            ).fetchone()[0]
            connection.execute(
                """UPDATE discovery_incremental_jobs SET catalog_completed_count=?,
                   last_completed_batch=?,phase=?,updated_at=? WHERE job_id=?""",
                (
                    completed, int(batch_number),
                    "catalog_complete" if completed == total else "suppliers_loaded",
                    observed, job_id,
                ),
            )
            connection.commit()
        return self.summary(job_id)

    def update_candidates(
        self, job_id: str, candidates: Iterable[dict[str, Any]], *,
        phase: str | None = None, replace_scenarios: bool = False,
        remove_qogita_scenarios: Iterable[str] | None = None,
    ) -> int:
        """Persist a bounded transformed candidate batch idempotently."""
        observed = _now()
        rows = list(candidates)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            for candidate in rows:
                identifier = str(
                    candidate.get("canonical_ean") or candidate.get("gtin") or ""
                )
                product = dict(candidate)
                scenarios = product.pop("scenarios", None)
                listings = product.pop("amazon_listings", [])
                combinations = product.pop("opportunity_combinations", [])
                connection.execute(
                    """UPDATE discovery_job_items SET product_json=?,catalog_status=?,
                       pricing_status=?,fees_status=?,terminal_status=?,updated_at=?
                       WHERE job_id=? AND canonical_identifier=?""",
                    (
                        _dump(product), candidate.get("catalog_status"),
                        candidate.get("pricing_status"), candidate.get("fee_status"),
                        candidate.get("evaluation_status"), observed, job_id, identifier,
                    ),
                )
                connection.execute(
                    "DELETE FROM discovery_listings WHERE job_id=? AND canonical_identifier=?",
                    (job_id, identifier),
                )
                for listing in listings:
                    asin = str(listing.get("asin") or "").strip()
                    if asin:
                        connection.execute(
                            """INSERT INTO discovery_listings
                               (job_id,canonical_identifier,asin,listing_json,updated_at)
                               VALUES (?,?,?,?,?)""",
                            (job_id, identifier, asin, _dump(listing), observed),
                        )
                if scenarios is not None and replace_scenarios:
                    connection.execute(
                        """DELETE FROM discovery_purchase_scenarios
                           WHERE job_id=? AND canonical_identifier=?""",
                        (job_id, identifier),
                    )
                    for index, scenario in enumerate(scenarios):
                        scenario_id = _scenario_identity(identifier, scenario, index)
                        payload = dict(scenario)
                        payload.setdefault("scenario_id", scenario_id)
                        connection.execute(
                            """INSERT INTO discovery_purchase_scenarios
                               (job_id,canonical_identifier,scenario_id,scenario_json,supplier)
                               VALUES (?,?,?,?,?)""",
                            (
                                job_id, identifier, scenario_id, _dump(payload),
                                str(payload.get("supplier") or "").strip().lower() or None,
                            ),
                        )
                for combination in combinations:
                    combination_id = str(
                        combination.get("combination_id")
                        or hashlib.sha256(_dump(combination).encode()).hexdigest()[:24]
                    )
                    connection.execute(
                        """INSERT INTO discovery_combinations
                           (job_id,combination_id,canonical_identifier,combination_json,updated_at)
                           VALUES (?,?,?,?,?) ON CONFLICT(job_id,combination_id) DO UPDATE SET
                             combination_json=excluded.combination_json,
                             updated_at=excluded.updated_at""",
                        (job_id, combination_id, identifier, _dump(combination), observed),
                    )
            identifiers_to_filter = list(dict.fromkeys(
                str(value) for value in (remove_qogita_scenarios or []) if value
            ))
            if identifiers_to_filter:
                placeholders = ",".join("?" for _ in identifiers_to_filter)
                connection.execute(
                    f"""DELETE FROM discovery_purchase_scenarios
                        WHERE job_id=? AND canonical_identifier IN ({placeholders})
                          AND (supplier='qogita' OR (
                            supplier IS NULL
                            AND lower(json_extract(scenario_json,'$.supplier'))='qogita'
                          ))""",
                    (job_id, *identifiers_to_filter),
                )
            if phase:
                connection.execute(
                    """UPDATE discovery_incremental_jobs SET phase=?,updated_at=?
                       WHERE job_id=?""", (phase, observed, job_id),
                )
            connection.commit()
        return len(rows)

    def backfill_classification_projection(
        self, job_id: str | None = None, *, batch_size: int = 500,
    ) -> int:
        """Idempotently derive normalized taxonomy rows from stored listings."""
        self.initialize()
        last_rowid = 0
        written = 0
        while True:
            with self._connect() as connection:
                if job_id:
                    rows = connection.execute(
                        """SELECT rowid,job_id,canonical_identifier,listing_json
                           FROM discovery_listings WHERE job_id=? AND rowid>?
                           ORDER BY rowid LIMIT ?""",
                        (job_id, last_rowid, int(batch_size)),
                    ).fetchall()
                else:
                    rows = connection.execute(
                        """SELECT rowid,job_id,canonical_identifier,listing_json
                           FROM discovery_listings WHERE rowid>?
                           ORDER BY rowid LIMIT ?""",
                        (last_rowid, int(batch_size)),
                    ).fetchall()
                if not rows:
                    return written
                projection = []
                for row in rows:
                    listing = json.loads(row["listing_json"])
                    projection.extend(projection_rows(
                        str(row["job_id"]), str(row["canonical_identifier"]), listing,
                    ))
                if projection:
                    before_changes = connection.total_changes
                    connection.executemany(
                        """INSERT INTO discovery_listing_classifications
                           (job_id,canonical_identifier,asin,marketplace_id,path_hash,
                           classification_id,parent_id,depth,display_name,is_leaf)
                           VALUES (?,?,?,?,?,?,?,?,?,?)
                           ON CONFLICT DO NOTHING""",
                        projection,
                    )
                    written += connection.total_changes - before_changes
                last_rowid = int(rows[-1]["rowid"])
                connection.commit()

    def set_phase(self, job_id: str, phase: str, *, status: str | None = None):
        values = {"phase": phase}
        if status is not None:
            values["status"] = status
        self.update_job(job_id, **values)

    def upsert_observations(self, job_id: str, observations: Iterable[dict[str, Any]]) -> int:
        observed = _now()
        count = 0
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            for observation in observations:
                observation_id = str(observation.get("observation_id") or "")
                if not observation_id:
                    continue
                current = connection.execute(
                    """SELECT observation_json FROM discovery_observations
                       WHERE job_id=? AND observation_id=?""", (job_id, observation_id),
                ).fetchone()
                payload = dict(observation)
                if current:
                    previous = json.loads(current[0])
                    previous_products = set(
                        (previous.get("diagnostics") or {}).get("product_keys") or []
                    )
                    current_products = set(
                        (payload.get("diagnostics") or {}).get("product_keys") or []
                    )
                    payload = {**previous, **payload}
                    payload.setdefault("diagnostics", {})["product_keys"] = sorted(
                        previous_products | current_products
                    )
                connection.execute(
                    """INSERT INTO discovery_observations
                       (job_id,observation_id,observation_json,updated_at) VALUES (?,?,?,?)
                       ON CONFLICT(job_id,observation_id) DO UPDATE SET
                         observation_json=excluded.observation_json,updated_at=excluded.updated_at""",
                    (job_id, observation_id, _dump(payload), observed),
                )
                count += 1
            connection.commit()
        return count

    def pending_observations(self, job_id: str, limit: int = 20) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT observation_json FROM discovery_observations WHERE job_id=?",
                (job_id,),
            ).fetchall()
        pending = []
        for row in rows:
            value = json.loads(row[0])
            if value.get("fee_status") in {None, "", "fee_pending", "retryable_error"}:
                pending.append(value)
                if len(pending) >= limit:
                    break
        return pending

    def observations_for_candidate(self, job_id: str, candidate: dict[str, Any]) -> dict[str, dict[str, Any]]:
        ids = {
            listing.get("amazon_observation_id")
            for listing in candidate.get("amazon_listings") or []
            if listing.get("amazon_observation_id")
        }
        if not ids:
            return {}
        placeholders = ",".join("?" for _ in ids)
        with self._connect() as connection:
            rows = connection.execute(
                f"""SELECT observation_id,observation_json FROM discovery_observations
                    WHERE job_id=? AND observation_id IN ({placeholders})""",
                (job_id, *sorted(ids)),
            ).fetchall()
        return {row["observation_id"]: json.loads(row["observation_json"]) for row in rows}

    def counts(self, job_id: str) -> dict[str, int]:
        with self._connect() as connection:
            return {
                "items": connection.execute(
                    "SELECT COUNT(*) FROM discovery_job_items WHERE job_id=?", (job_id,)
                ).fetchone()[0],
                "scenarios": connection.execute(
                    "SELECT COUNT(*) FROM discovery_purchase_scenarios WHERE job_id=?", (job_id,)
                ).fetchone()[0],
                "listings": connection.execute(
                    "SELECT COUNT(*) FROM discovery_listings WHERE job_id=?", (job_id,)
                ).fetchone()[0],
                "observations": connection.execute(
                    "SELECT COUNT(*) FROM discovery_observations WHERE job_id=?", (job_id,)
                ).fetchone()[0],
                "combinations": connection.execute(
                    "SELECT COUNT(*) FROM discovery_combinations WHERE job_id=?", (job_id,)
                ).fetchone()[0],
            }

    def pricing_progress_asins(self, job_id: str) -> tuple[set[str], set[str]]:
        """Return bounded phase-local Pricing identities from persisted listings."""
        with self._connect() as connection:
            rows = connection.execute(
                """SELECT asin,listing_json FROM discovery_listings
                   WHERE job_id=? AND json_extract(listing_json,'$.evaluation_status')='bsr_passed'""",
                (job_id,),
            ).fetchall()
        target: set[str] = set()
        completed: set[str] = set()
        for row in rows:
            asin = str(row["asin"] or "")
            if not asin:
                continue
            target.add(asin)
            listing = json.loads(row["listing_json"])
            if listing.get("pricing_status"):
                completed.add(asin)
        return target, completed

    def fee_progress_counts(self, job_id: str) -> tuple[int, int]:
        """Return terminal Fee observations and target without hydrating payloads."""
        terminal = ("valid", "unavailable", "invalid")
        placeholders = ",".join("?" for _ in terminal)
        with self._connect() as connection:
            row = connection.execute(
                f"""SELECT COUNT(*) total,
                           SUM(CASE WHEN json_extract(observation_json,'$.fee_status')
                             IN ({placeholders}) THEN 1 ELSE 0 END) completed
                    FROM discovery_observations WHERE job_id=?""",
                (*terminal, job_id),
            ).fetchone()
        return int(row["completed"] or 0), int(row["total"] or 0)

    def notification_summary(self, job_id: str) -> dict[str, Any]:
        """Return a bounded, authoritative terminal-notification projection.

        Terminal email rendering must not depend on an in-memory ``results``
        collection.  All counts come directly from the incremental job tables,
        and only the single best final product (plus its recommended scenario)
        is decoded.
        """
        self.initialize()
        with self._connect() as connection:
            job = connection.execute(
                """SELECT selected_count FROM discovery_incremental_jobs
                   WHERE job_id=?""", (job_id,),
            ).fetchone()
            if not job:
                raise KeyError(job_id)
            listing = connection.execute(
                """SELECT COUNT(*) listing_count,
                          COUNT(DISTINCT canonical_identifier) product_count
                   FROM discovery_listings WHERE job_id=?""", (job_id,),
            ).fetchone()
            observation = connection.execute(
                """SELECT COUNT(*) target_count,
                          SUM(CASE WHEN json_extract(observation_json,'$.fee_status')='valid'
                                   THEN 1 ELSE 0 END) valid_count,
                          SUM(CASE WHEN json_extract(observation_json,'$.fee_status')='unavailable'
                                   THEN 1 ELSE 0 END) unavailable_count,
                          SUM(CASE WHEN json_extract(observation_json,'$.reference_price') IS NOT NULL
                                   THEN 1 ELSE 0 END) pricing_valid_count,
                          SUM(CASE WHEN json_extract(observation_json,'$.bsr_beauty') IS NOT NULL
                                   THEN 1 ELSE 0 END) beauty_count
                   FROM discovery_observations WHERE job_id=?""", (job_id,),
            ).fetchone()
            combination_count = connection.execute(
                "SELECT COUNT(*) FROM discovery_combinations WHERE job_id=?", (job_id,),
            ).fetchone()[0]
            final_count = connection.execute(
                """SELECT COUNT(*) FROM discovery_job_items
                   WHERE job_id=? AND json_extract(product_json,'$.is_final_result')=1""",
                (job_id,),
            ).fetchone()[0]
            best = connection.execute(
                """SELECT canonical_identifier,product_json
                   FROM discovery_job_items
                   WHERE job_id=? AND json_extract(product_json,'$.is_final_result')=1
                   ORDER BY
                     CAST(json_extract(product_json,'$.recommended_combination.score') AS REAL) DESC,
                     CAST(json_extract(product_json,'$.recommended_combination.margin_percent') AS REAL) DESC,
                     CAST(json_extract(product_json,'$.recommended_combination.profit') AS REAL) DESC,
                     CAST(json_extract(product_json,'$.recommended_combination.cost_gross_unit_eur') AS REAL) ASC,
                     sequence_no ASC
                   LIMIT 1""", (job_id,),
            ).fetchone()
            best_opportunity = None
            if best:
                product = json.loads(best["product_json"])
                combination = dict(product.get("recommended_combination") or {})
                scenario = None
                scenario_id = combination.get("scenario_id") or product.get(
                    "best_purchase_scenario"
                )
                if scenario_id:
                    scenario_row = connection.execute(
                        """SELECT scenario_json FROM discovery_purchase_scenarios
                           WHERE job_id=? AND canonical_identifier=? AND scenario_id=?""",
                        (job_id, best["canonical_identifier"], str(scenario_id)),
                    ).fetchone()
                    if scenario_row:
                        scenario = json.loads(scenario_row[0])
                scenario = scenario or {}
                best_opportunity = {
                    "product": product.get("amazon_title") or product.get("title"),
                    "canonical_ean": product.get("canonical_ean")
                    or best["canonical_identifier"],
                    "asin": combination.get("asin") or product.get("asin"),
                    "supplier": combination.get("supplier") or scenario.get("supplier"),
                    "scenario": combination.get("scenario_label")
                    or scenario.get("scenario_label"),
                    "cost_gross_unit_eur": combination.get("cost_gross_unit_eur")
                    or scenario.get("cost_gross_unit_eur"),
                    "price_reference": combination.get("price_reference")
                    or product.get("reference_price"),
                    "margin_percent": combination.get("margin_percent")
                    or product.get("margin_percent"),
                    "profit": combination.get("profit"),
                    "score": combination.get("score")
                    if combination.get("score") is not None else product.get("score"),
                    "combination_id": combination.get("combination_id"),
                }
        target = int(observation["target_count"] or 0)
        valid = int(observation["valid_count"] or 0)
        unavailable = int(observation["unavailable_count"] or 0)
        return {
            "selected_count": int(job["selected_count"]),
            "sampled_identifier_count": int(job["selected_count"]),
            "amazon_found_count": int(listing["product_count"] or 0),
            "listing_count": int(listing["listing_count"] or 0),
            "observation_count": target,
            "pricing_valid_count": int(observation["pricing_valid_count"] or 0),
            "beauty_count": int(observation["beauty_count"] or 0),
            "bsr_passed_count": int(observation["beauty_count"] or 0),
            "competition_passed_count": target,
            "fee_target_count": target,
            "fee_valid_count": valid,
            "fee_unavailable_count": unavailable,
            "fee_pending_count": max(0, target - valid - unavailable),
            "fee_coverage_partial": unavailable > 0,
            "combination_count": int(combination_count),
            "final_opportunity_count": int(final_count),
            "final_products": int(final_count),
            "best_opportunity": best_opportunity,
        }

    def definitive_catalog_statuses(self, job_id: str) -> dict[str, str]:
        with self._connect() as connection:
            return {
                row["canonical_identifier"]: row["catalog_status"]
                for row in connection.execute(
                    """SELECT canonical_identifier,catalog_status
                       FROM discovery_job_items WHERE job_id=?
                         AND catalog_status IN
                           ('resolved','ambiguous','not_found','invalid_identifier')""",
                    (job_id,),
                )
            }

    def iter_observations(self, job_id: str, *, batch_size: int = 250):
        offset = 0
        while True:
            with self._connect() as connection:
                rows = connection.execute(
                    """SELECT observation_json FROM discovery_observations
                       WHERE job_id=? ORDER BY observation_id LIMIT ? OFFSET ?""",
                    (job_id, int(batch_size), offset),
                ).fetchall()
            if not rows:
                return
            for row in rows:
                yield json.loads(row[0])
            offset += len(rows)


    def update_job(self, job_id: str, **values):
        allowed = {"status", "phase", "last_completed_batch", "checkpoint_bytes_written"}
        values = {key: value for key, value in values.items() if key in allowed}
        if not values:
            return
        values["updated_at"] = _now()
        assignments = ",".join(f"{key}=?" for key in values)
        with self._connect() as connection:
            connection.execute(
                f"UPDATE discovery_incremental_jobs SET {assignments} WHERE job_id=?",
                [*values.values(), job_id],
            )
            connection.commit()

    def add_checkpoint_bytes(self, job_id: str, amount: int):
        with self._connect() as connection:
            connection.execute(
                """UPDATE discovery_incremental_jobs
                   SET checkpoint_bytes_written=checkpoint_bytes_written+?,updated_at=?
                   WHERE job_id=?""", (int(amount), _now(), job_id),
            )
            connection.commit()

    def record_resource_event(self, job_id: str, level: str, reason: str, metrics: dict[str, Any]):
        with self._connect() as connection:
            connection.execute(
                """INSERT INTO discovery_resource_events
                   (job_id,level,reason,metrics_json,observed_at) VALUES (?,?,?,?,?)""",
                (job_id, level, reason, _dump(metrics), _now()),
            )
            connection.commit()

    def passive_wal_checkpoint(self) -> tuple[int, int, int]:
        with self._connect() as connection:
            row = connection.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
            return tuple(int(value) for value in row)

    def file_sizes(self) -> dict[str, int]:
        return {
            "database_bytes": self.path.stat().st_size if self.path.exists() else 0,
            "wal_bytes": Path(f"{self.path}-wal").stat().st_size
            if Path(f"{self.path}-wal").exists() else 0,
            "shm_bytes": Path(f"{self.path}-shm").stat().st_size
            if Path(f"{self.path}-shm").exists() else 0,
        }


class IncrementalCandidateCollection:
    """Re-iterable bounded view used by the audit Excel exporter."""

    def __init__(self, store: DiscoveryIncrementalStore, job_id: str, *, final_only=False):
        self.store = store
        self.job_id = job_id
        self.final_only = final_only

    def __iter__(self):
        yield from self.store.iter_export_candidates(
            self.job_id, final_only=self.final_only,
        )

    def __len__(self):
        if not self.final_only:
            return self.store.counts(self.job_id)["items"]
        with self.store._connect() as connection:
            return connection.execute(
                """SELECT COUNT(*) FROM discovery_job_items
                   WHERE job_id=? AND json_extract(product_json,'$.is_final_result')=1""",
                (self.job_id,),
            ).fetchone()[0]


class IncrementalObservationCollection:
    def __init__(self, store: DiscoveryIncrementalStore, job_id: str):
        self.store = store
        self.job_id = job_id

    def __iter__(self):
        return self.store.iter_observations(self.job_id)

    def __len__(self):
        return self.store.counts(self.job_id)["observations"]


class LightweightCheckpointStore:
    """Atomic metadata checkpoint. Heavy records live in DiscoveryIncrementalStore."""

    HEAVY_KEYS = {
        "candidates", "amazon_listings", "amazon_observations",
        "opportunity_combinations", "results", "rotation_selected_identifiers",
    }

    def __init__(self, root: str | Path = PROJECT_ROOT / "data" / "discovery_jobs"):
        self.root = Path(root)

    def path(self, job_id: str) -> Path:
        return self.root / f"{job_id}.state.json"

    def save(self, state: dict[str, Any]) -> int:
        self.root.mkdir(parents=True, exist_ok=True)
        compact = {key: value for key, value in state.items() if key not in self.HEAVY_KEYS}
        coverage = compact.get("supplier_coverage")
        if isinstance(coverage, dict) and "rotation_selected_identifiers" in coverage:
            compact["supplier_coverage"] = {
                key: value for key, value in coverage.items()
                if key != "rotation_selected_identifiers"
            }
        compact["schema_version"] = max(int(compact.get("schema_version") or 0), 5)
        compact["persistence"] = "incremental_sqlite_v1"
        compact["updated_at"] = _now()
        payload = _dump(compact).encode("utf-8")
        descriptor, temporary = tempfile.mkstemp(
            prefix=f".{state['job_id']}.", suffix=".state.json", dir=self.root,
        )
        try:
            with os.fdopen(descriptor, "wb") as output:
                output.write(payload)
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary, self.path(str(state["job_id"])))
        except Exception:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass
            raise
        return len(payload)

    def load(self, job_id: str) -> dict[str, Any]:
        with self.path(job_id).open("r", encoding="utf-8") as source:
            return json.load(source)


def prepare_incremental_job(
    state: dict[str, Any], *, supplier_store, rotation_store, amazon_cache=None,
    freshness_policy=None, batch_size: int = 500, start_sequence: int = 0,
    progress=None, resource_governor=None,
) -> dict[str, Any]:
    """Freeze a supplier-first selection without loading the full union payload.

    Only identifier membership is held while rotation selects the immutable
    sample. Supplier scenarios are then loaded one identifier at a time and
    immediately written by ``create_job``.
    """
    from purchase_scenarios import merge_product_candidates
    from supplier_preparation import normalize_selected_suppliers

    selected = normalize_selected_suppliers(state.get("selected_suppliers") or [])
    snapshots: dict[str, Any] = {}
    source_metadata: dict[str, dict[str, Any]] = {}
    usable: list[str] = []
    for supplier in selected:
        metadata = supplier_store.serving_generation_metadata(supplier)
        if metadata:
            usable.append(supplier)
            source_metadata[supplier] = dict(metadata)
            snapshots[supplier] = {
                "supplier": supplier,
                "snapshot_id": metadata.get("run_id"),
                "snapshot_at": metadata.get("completed_at"),
                "freshness": "frozen",
                "refresh_status": "supplier_catalog_active",
                "products_count": int(metadata.get("product_count") or 0),
                "scenarios_count": int(metadata.get("scenario_count") or 0),
                "coverage_type": metadata.get("product_catalog_coverage_type")
                or metadata.get("coverage_type"),
                "coverage_complete": bool(
                    metadata.get("product_catalog_coverage_complete")
                ),
                "availability_status": "available",
            }
        else:
            snapshots[supplier] = {
                "supplier": supplier, "snapshot_id": None,
                "freshness": "unavailable", "refresh_status": "baseline_missing",
                "products_count": 0, "scenarios_count": 0,
                "coverage_complete": False, "availability_status": "unavailable",
            }
    if not usable:
        raise RuntimeError("No supplier-first baseline is available")
    frozen_selection = (
        rotation_store.frozen_selection(state["job_id"], usable)
        if hasattr(rotation_store, "frozen_selection") else None
    )
    if frozen_selection:
        frozen_stubs, frozen_rotation = frozen_selection
        memberships = {
            str(row["canonical_ean"]): tuple(row.get("suppliers") or ())
            for row in frozen_stubs
        }
    else:
        frozen_stubs = frozen_rotation = None
        memberships = supplier_store.active_identifier_memberships(usable)
    if progress is not None:
        progress("preparing_memberships", max(0, int(start_sequence)), len(memberships))
    if resource_governor is not None:
        resource_governor.before_next_batch()
    stubs = frozen_stubs or [
        {
            "canonical_ean": identifier, "gtin": identifier,
            "scenarios": [{"supplier": supplier} for supplier in suppliers],
            "suppliers": list(suppliers),
        }
        for identifier, suppliers in memberships.items()
    ]
    budget = state.get("run_budget")
    budget = None if budget in {None, "all"} else int(budget)
    planner_version = state.get("discovery_planner_version")
    planned: dict[str, dict[str, Any]] = {}
    if planner_version:
        from discovery_freshness import (
            AmazonFreshnessPolicy, DiscoveryAmazonCache, PlanAction,
            plan_cached_product, planning_counts,
        )

        policy = freshness_policy or AmazonFreshnessPolicy.from_environment()
        if amazon_cache is None:
            amazon_cache = DiscoveryAmazonCache(DiscoveryIncrementalStore())
        amazon_cache.index_completed_jobs()
        action_order = {
            PlanAction.NEW_LOOKUP.value: 0,
            PlanAction.REFRESH_CATALOG.value: 1,
            PlanAction.REFRESH_BSR.value: 1,
            PlanAction.REFRESH_PRICING.value: 2,
            PlanAction.REFRESH_FEES.value: 3,
            PlanAction.CACHE_REUSE.value: 4,
        }
        planned_count = 0
        for identifier, cached in amazon_cache.get_many(sorted(memberships)):
            planned[identifier] = plan_cached_product(cached, policy=policy)
            planned_count += 1
            if planned_count % max(1, int(batch_size)) == 0:
                if progress is not None:
                    progress("preparing_plan", max(int(start_sequence), planned_count), len(memberships))
                if resource_governor is not None:
                    resource_governor.before_next_batch()
        if frozen_stubs is not None:
            selected_stubs, rotation = frozen_stubs, frozen_rotation
        else:
            selected_stubs, rotation = rotation_store.select_current_universe(
                state["job_id"], stubs, usable, budget,
                supplier_snapshot_set=snapshots,
                action_priority={
                    identifier: action_order[value["primary_action"]]
                    for identifier, value in planned.items()
                },
            )
    else:
        policy = None
        if frozen_stubs is not None:
            selected_stubs, rotation = frozen_stubs, frozen_rotation
        else:
            selected_stubs, rotation = rotation_store.select(
                state["job_id"], stubs, usable, budget,
                supplier_snapshot_set=snapshots,
            )
    selected_identifiers = [row["canonical_ean"] for row in selected_stubs]
    if progress is not None:
        progress("preparing", max(0, int(start_sequence)), len(selected_identifiers))

    def candidates():
        pending_identifiers = selected_identifiers[max(0, int(start_sequence)):]
        for offset in range(0, len(pending_identifiers), max(1, int(batch_size))):
            batch = pending_identifiers[offset:offset + max(1, int(batch_size))]
            cached_by_identifier = (
                dict(amazon_cache.get_many(batch))
                if planner_version else {}
            )
            supplier_candidates: dict[str, dict[str, list[dict[str, Any]]]] = {}
            for supplier in usable:
                relevant = [
                    identifier for identifier in batch
                    if supplier in memberships.get(identifier, ())
                ]
                if not relevant:
                    continue
                grouped: dict[str, list[dict[str, Any]]] = {}
                if hasattr(supplier_store, "iter_active_candidates_for_identifiers"):
                    rows = supplier_store.iter_active_candidates_for_identifiers(
                        supplier, relevant, batch_size=len(relevant),
                        generation_metadata=source_metadata.get(supplier),
                    )
                    for row in rows:
                        grouped.setdefault(str(row.get("canonical_ean")), []).append(row)
                else:
                    for identifier in relevant:
                        grouped[identifier] = supplier_store.active_candidates_for_identifier(
                            supplier, identifier,
                        )
                supplier_candidates[supplier] = grouped
            for identifier in batch:
                collections = [
                    supplier_candidates.get(supplier, {}).get(identifier, [])
                    for supplier in memberships.get(identifier, ())
                ]
                merged = merge_product_candidates(*collections)
                if len(merged) != 1:
                    raise ValueError(
                        f"Frozen identifier {identifier} produced {len(merged)} candidates"
                    )
                candidate = merged[0]
                if planner_version:
                    plan = planned[identifier]
                    cached = cached_by_identifier.get(identifier) or {}
                    actions = set(plan["actions"])
                    candidate["amazon_plan"] = plan
                    if not ({"REFRESH_CATALOG", "REFRESH_BSR", "NEW_LOOKUP"} & actions):
                        candidate["catalog_status"] = cached.get("catalog_status")
                        candidate["catalog_diagnostics"] = {
                            "cache_source_job_id": cached.get("source_job_id"),
                            "cache_reused": True,
                        }
                        listings = [dict(row) for row in cached.get("amazon_listings") or []]
                        if "REFRESH_PRICING" in actions:
                            for listing in listings:
                                for key in (
                                    "pricing_status", "fba_sellers", "total_sellers",
                                    "seller_count_source", "reference_price", "price_source",
                                    "min_fba_price", "min_fbm_price", "competition_status",
                                    "pricing_observed_at", "competition_observed_at",
                                ):
                                    listing.pop(key, None)
                        candidate["amazon_listings"] = listings
                yield candidate

    metadata = {
        **state, **rotation,
        "selected_suppliers": selected,
        "usable_suppliers": usable,
        "supplier_snapshot_set": snapshots,
        "supplier_warnings": [
            f"{supplier.upper()}: baseline non disponibile"
            for supplier in selected if supplier not in usable
        ],
        "phase": "suppliers_loaded", "status": "running",
        "sampled_identifier_count": len(selected_identifiers),
        "total_supplier_ean_universe": len(memberships),
        "prepared_total": len(selected_identifiers),
        "preparation_start_sequence": max(0, int(start_sequence)),
    }
    if planner_version:
        selected_plans = [planned[identifier] for identifier in selected_identifiers]
        metadata.update(planning_counts(selected_plans))
        metadata.update({
            "requested_universe_count": (
                len(memberships) if budget is None else len(selected_identifiers)
            ),
            "freshness_policy_version": policy.version,
            "freshness_policy": policy.as_metadata(),
            "discovery_planner_version": planner_version,
        })
    return {"metadata": metadata, "candidates": candidates()}
