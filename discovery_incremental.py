"""Incremental, restart-safe persistence for large Discovery jobs.

The legacy Discovery checkpoint embeds every supplier scenario and Amazon result
in one JSON document.  This store keeps the immutable selection and each phase's
records in SQLite so completing one batch never rewrites prior batches.
"""

from __future__ import annotations

import hashlib
import json
import mmap
import os
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_DATABASE = PROJECT_ROOT / "data" / "discovery_incremental.sqlite3"
SCHEMA_VERSION = 1
TERMINAL_CATALOG_STATUSES = {
    "resolved", "ambiguous", "not_found", "invalid_identifier",
}


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
    PRIMARY KEY (job_id, canonical_identifier, scenario_id)
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
    def __init__(self, path: str | Path | None = None):
        configured = path or os.environ.get("DISCOVERY_INCREMENTAL_DATABASE")
        self.path = Path(configured or DEFAULT_DATABASE).expanduser().resolve()

    def _connect(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
        connection.execute("PRAGMA busy_timeout=30000")
        return connection

    def initialize(self):
        with self._connect() as connection:
            connection.executescript(SCHEMA)

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
    ) -> dict[str, Any]:
        self.initialize()
        job_id = str(metadata["job_id"])
        observed = _now()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """INSERT INTO discovery_incremental_jobs
                   (job_id,schema_version,status,phase,metadata_json,
                    legacy_checkpoint_path,legacy_checkpoint_sha256,created_at,updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(job_id) DO NOTHING""",
                (
                    job_id, SCHEMA_VERSION, metadata.get("status", "running"),
                    metadata.get("phase", "initialized"), _dump(metadata),
                    str(legacy_checkpoint_path) if legacy_checkpoint_path else None,
                    legacy_checkpoint_sha256, metadata.get("created_at") or observed, observed,
                ),
            )
            existing = connection.execute(
                "SELECT COUNT(*) FROM discovery_job_items WHERE job_id=?", (job_id,)
            ).fetchone()[0]
            if existing:
                connection.commit()
                return self.summary(job_id, connection=connection)
            selected = completed = 0
            for sequence_no, candidate in enumerate(candidates):
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
                connection.execute(
                    """INSERT INTO discovery_job_items
                       (job_id,sequence_no,canonical_identifier,identifier_type,
                        product_json,catalog_status,pricing_status,fees_status,
                        terminal_status,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?)""",
                    (
                        job_id, sequence_no, identifier, candidate.get("identifier_type"),
                        _dump(product), catalog_status, candidate.get("pricing_status"),
                        candidate.get("fee_status"), candidate.get("evaluation_status"), observed,
                    ),
                )
                for index, scenario in enumerate(scenarios):
                    scenario_id = _scenario_identity(identifier, scenario, index)
                    payload = dict(scenario)
                    payload.setdefault("scenario_id", scenario_id)
                    connection.execute(
                        """INSERT INTO discovery_purchase_scenarios
                           (job_id,canonical_identifier,scenario_id,scenario_json)
                           VALUES (?,?,?,?)""",
                        (job_id, identifier, scenario_id, _dump(payload)),
                    )
                if catalog_status:
                    connection.execute(
                        """INSERT INTO discovery_catalog_results
                           (job_id,canonical_identifier,catalog_status,diagnostics_json,updated_at)
                           VALUES (?,?,?,?,?)""",
                        (
                            job_id, identifier, catalog_status,
                            _dump(candidate.get("catalog_diagnostics") or {}), observed,
                        ),
                    )
                for listing in listings:
                    asin = str(listing.get("asin") or "").strip()
                    if not asin:
                        continue
                    connection.execute(
                        """INSERT INTO discovery_listings
                           (job_id,canonical_identifier,asin,listing_json,updated_at)
                           VALUES (?,?,?,?,?)""",
                        (job_id, identifier, asin, _dump(listing), observed),
                    )
                selected += 1
                completed += int(str(catalog_status or "") in TERMINAL_CATALOG_STATUSES)
            connection.execute(
                """UPDATE discovery_incremental_jobs
                   SET selected_count=?,catalog_completed_count=?,updated_at=? WHERE job_id=?""",
                (selected, completed, observed, job_id),
            )
            connection.commit()
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
        connection = connection or self._connect()
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
        self.initialize()
        offset = 0
        while True:
            with self._connect() as connection:
                rows = connection.execute(
                    """SELECT * FROM discovery_job_items WHERE job_id=?
                       ORDER BY sequence_no LIMIT ? OFFSET ?""",
                    (job_id, int(batch_size), offset),
                ).fetchall()
                hydrated = [self._hydrate_item(connection, row) for row in rows]
            if not hydrated:
                return
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
                               (job_id,canonical_identifier,scenario_id,scenario_json)
                               VALUES (?,?,?,?)""",
                            (job_id, identifier, scenario_id, _dump(payload)),
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
            if phase:
                connection.execute(
                    """UPDATE discovery_incremental_jobs SET phase=?,updated_at=?
                       WHERE job_id=?""", (phase, observed, job_id),
                )
            connection.commit()
        return len(rows)

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
            if value.get("fee_status") in {None, "", "fee_pending"}:
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
        for candidate in self.store.iter_candidates(self.job_id):
            if not self.final_only or candidate.get("is_final_result"):
                yield candidate

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
    state: dict[str, Any], *, supplier_store, rotation_store,
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
    usable: list[str] = []
    for supplier in selected:
        metadata = supplier_store.serving_generation_metadata(supplier)
        if metadata:
            usable.append(supplier)
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
    memberships = supplier_store.active_identifier_memberships(usable)
    stubs = [
        {
            "canonical_ean": identifier, "gtin": identifier,
            "scenarios": [{"supplier": supplier} for supplier in suppliers],
            "suppliers": list(suppliers),
        }
        for identifier, suppliers in memberships.items()
    ]
    budget = state.get("run_budget")
    budget = None if budget in {None, "all"} else int(budget)
    selected_stubs, rotation = rotation_store.select(
        state["job_id"], stubs, usable, budget,
        supplier_snapshot_set=snapshots,
    )
    selected_identifiers = [row["canonical_ean"] for row in selected_stubs]

    def candidates():
        for identifier in selected_identifiers:
            collections = []
            for supplier in memberships.get(identifier, ()):
                collections.append(
                    supplier_store.active_candidates_for_identifier(supplier, identifier)
                )
            merged = merge_product_candidates(*collections)
            if len(merged) != 1:
                raise ValueError(
                    f"Frozen identifier {identifier} produced {len(merged)} candidates"
                )
            yield merged[0]

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
    }
    return {"metadata": metadata, "candidates": candidates()}
