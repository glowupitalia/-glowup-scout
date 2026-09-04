"""Supplier-neutral Amazon freshness planning for Discovery V2.

This module is deliberately additive: persisted Discovery jobs remain the
authoritative payload store.  The cache tables below only point at immutable
per-job rows and can be rebuilt idempotently without changing historical jobs.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any, Iterable


POLICY_VERSION = "amazon_freshness_v1"
CACHE_PAYLOAD_SCHEMA_VERSION = "amazon_cache_payload_v1"


def _canonical_payload(value: Any) -> tuple[str, str]:
    payload = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
    )
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return payload, digest


def _verified_payload(payload: Any, version: Any, digest: Any) -> Any | None:
    if payload is None or version != CACHE_PAYLOAD_SCHEMA_VERSION or not digest:
        return None
    encoded = str(payload)
    if hashlib.sha256(encoded.encode("utf-8")).hexdigest() != str(digest):
        return None
    try:
        value = json.loads(encoded)
    except (TypeError, ValueError):
        return None
    return value


class PlanAction(str, Enum):
    CACHE_REUSE = "CACHE_REUSE"
    REFRESH_CATALOG = "REFRESH_CATALOG"
    REFRESH_PRICING = "REFRESH_PRICING"
    REFRESH_BSR = "REFRESH_BSR"
    REFRESH_FEES = "REFRESH_FEES"
    NEW_LOOKUP = "NEW_LOOKUP"


def _hours(name: str, default: int) -> timedelta:
    return timedelta(hours=int(os.environ.get(name, default)))


@dataclass(frozen=True)
class AmazonFreshnessPolicy:
    version: str = POLICY_VERSION
    catalog_resolved: timedelta = timedelta(days=30)
    catalog_negative: timedelta = timedelta(days=7)
    pricing: timedelta = timedelta(hours=6)
    competition: timedelta = timedelta(hours=6)
    bsr: timedelta = timedelta(hours=24)
    fees_valid: timedelta = timedelta(days=7)
    fees_negative: timedelta = timedelta(hours=24)

    @classmethod
    def from_environment(cls) -> "AmazonFreshnessPolicy":
        return cls(
            catalog_resolved=_hours("DISCOVERY_TTL_CATALOG_RESOLVED_HOURS", 720),
            catalog_negative=_hours("DISCOVERY_TTL_CATALOG_NEGATIVE_HOURS", 168),
            pricing=_hours("DISCOVERY_TTL_PRICING_HOURS", 6),
            competition=_hours("DISCOVERY_TTL_COMPETITION_HOURS", 6),
            bsr=_hours("DISCOVERY_TTL_BSR_HOURS", 24),
            fees_valid=_hours("DISCOVERY_TTL_FEES_VALID_HOURS", 168),
            fees_negative=_hours("DISCOVERY_TTL_FEES_NEGATIVE_HOURS", 24),
        )

    def as_metadata(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "catalog_resolved_hours": self.catalog_resolved.total_seconds() / 3600,
            "catalog_negative_hours": self.catalog_negative.total_seconds() / 3600,
            "pricing_hours": self.pricing.total_seconds() / 3600,
            "competition_hours": self.competition.total_seconds() / 3600,
            "bsr_hours": self.bsr.total_seconds() / 3600,
            "fees_valid_hours": self.fees_valid.total_seconds() / 3600,
            "fees_negative_hours": self.fees_negative.total_seconds() / 3600,
        }


def _parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None


def _fresh(value: Any, ttl: timedelta, now: datetime) -> bool:
    observed = _parse_time(value)
    return bool(observed and now - observed <= ttl)


def fee_cache_key(asin: str, reference_price: Any, currency: str = "EUR") -> str | None:
    try:
        price = Decimal(str(reference_price)).quantize(Decimal("0.01"))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if price <= 0 or not asin:
        return None
    return f"{str(asin).upper()}|{str(currency).upper()}|{price:.2f}"


def plan_cached_product(
    cached: dict[str, Any] | None, *, policy: AmazonFreshnessPolicy,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return deterministic per-dimension work without changing any state."""
    now = now or datetime.now(timezone.utc)
    if not cached:
        return {"primary_action": PlanAction.NEW_LOOKUP.value, "actions": [PlanAction.NEW_LOOKUP.value]}
    status = str(cached.get("catalog_status") or "")
    catalog_ttl = (
        policy.catalog_resolved if status == "resolved" else policy.catalog_negative
    )
    if not _fresh(cached.get("catalog_observed_at"), catalog_ttl, now):
        return {"primary_action": PlanAction.REFRESH_CATALOG.value, "actions": [PlanAction.REFRESH_CATALOG.value]}
    actions: list[str] = []
    if status == "resolved":
        if not _fresh(cached.get("bsr_observed_at"), policy.bsr, now):
            actions.append(PlanAction.REFRESH_BSR.value)
        if not _fresh(cached.get("pricing_observed_at"), policy.pricing, now):
            actions.append(PlanAction.REFRESH_PRICING.value)
        elif not _fresh(cached.get("competition_observed_at"), policy.competition, now):
            actions.append(PlanAction.REFRESH_PRICING.value)
        fee_ttl = (
            policy.fees_valid if cached.get("fee_status") == "valid"
            else policy.fees_negative
        )
        if not cached.get("fee_cache_key") or not _fresh(
            cached.get("fee_observed_at"), fee_ttl, now,
        ):
            actions.append(PlanAction.REFRESH_FEES.value)
    if not actions:
        actions.append(PlanAction.CACHE_REUSE.value)
    return {"primary_action": actions[0], "actions": list(dict.fromkeys(actions))}


def planning_counts(rows: Iterable[dict[str, Any]]) -> dict[str, int]:
    counts = {action.value: 0 for action in PlanAction}
    total = 0
    for row in rows:
        total += 1
        counts[str(row["primary_action"])] += 1
    refresh = sum(
        counts[action.value] for action in (
            PlanAction.REFRESH_CATALOG, PlanAction.REFRESH_PRICING,
            PlanAction.REFRESH_BSR, PlanAction.REFRESH_FEES,
        )
    )
    return {
        "requested_universe_count": total,
        "cache_reuse_count": counts[PlanAction.CACHE_REUSE.value],
        "refresh_count": refresh,
        "new_lookup_count": counts[PlanAction.NEW_LOOKUP.value],
        "planner_action_counts": counts,
    }


def reusable_fee(
    value: dict[str, Any] | None, policy: AmazonFreshnessPolicy, *,
    now: datetime | None = None,
) -> bool:
    if not value:
        return False
    now = now or datetime.now(timezone.utc)
    ttl = policy.fees_valid if value.get("fee_status") == "valid" else policy.fees_negative
    return _fresh(value.get("fee_observed_at"), ttl, now)


CACHE_SCHEMA = """
CREATE TABLE IF NOT EXISTS discovery_amazon_cache (
    canonical_identifier TEXT PRIMARY KEY,
    source_job_id TEXT NOT NULL,
    catalog_status TEXT NOT NULL,
    catalog_observed_at TEXT NOT NULL,
    freshness_json TEXT NOT NULL DEFAULT '{}',
    listings_json TEXT NULL,
    payload_schema_version TEXT NULL,
    payload_sha256 TEXT NULL,
    materialized_at TEXT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS discovery_amazon_fee_cache (
    fee_cache_key TEXT PRIMARY KEY,
    source_job_id TEXT NOT NULL,
    observation_id TEXT NOT NULL,
    fee_status TEXT NOT NULL,
    fee_observed_at TEXT NOT NULL,
    observation_json TEXT NOT NULL DEFAULT '{}',
    payload_schema_version TEXT NULL,
    payload_sha256 TEXT NULL,
    materialized_at TEXT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_discovery_amazon_cache_source
ON discovery_amazon_cache(source_job_id);
CREATE TABLE IF NOT EXISTS discovery_amazon_cache_indexed_jobs (
    source_job_id TEXT PRIMARY KEY,
    source_updated_at TEXT NOT NULL,
    indexed_at TEXT NOT NULL,
    materialization_version TEXT NULL,
    verification_state TEXT NULL,
    catalog_count INTEGER NULL,
    listing_count INTEGER NULL,
    fee_count INTEGER NULL,
    aggregate_sha256 TEXT NULL,
    verified_at TEXT NULL
);
"""


class DiscoveryAmazonCache:
    """Rebuildable references/materialized read model over completed jobs."""

    def __init__(self, incremental_store):
        self.store = incremental_store
        self._initialized = False

    def initialize(self):
        if self._initialized:
            return
        self.store.initialize()
        with self.store._connect() as connection:
            connection.executescript(CACHE_SCHEMA)
            additions = {
                "discovery_amazon_cache": {
                    "freshness_json": "TEXT NOT NULL DEFAULT '{}'",
                    "listings_json": "TEXT NULL",
                    "payload_schema_version": "TEXT NULL",
                    "payload_sha256": "TEXT NULL",
                    "materialized_at": "TEXT NULL",
                },
                "discovery_amazon_fee_cache": {
                    "payload_schema_version": "TEXT NULL",
                    "payload_sha256": "TEXT NULL",
                    "materialized_at": "TEXT NULL",
                },
                "discovery_amazon_cache_indexed_jobs": {
                    "materialization_version": "TEXT NULL",
                    "verification_state": "TEXT NULL",
                    "catalog_count": "INTEGER NULL",
                    "listing_count": "INTEGER NULL",
                    "fee_count": "INTEGER NULL",
                    "aggregate_sha256": "TEXT NULL",
                    "verified_at": "TEXT NULL",
                },
            }
            for table, definitions in additions.items():
                columns = {
                    row["name"] for row in connection.execute(
                        f"PRAGMA table_info({table})"
                    )
                }
                for name, definition in definitions.items():
                    if name not in columns:
                        try:
                            connection.execute(
                                f"ALTER TABLE {table} ADD COLUMN {name} {definition}"
                            )
                        except sqlite3.OperationalError:
                            current = {
                                row["name"] for row in connection.execute(
                                    f"PRAGMA table_info({table})"
                                )
                            }
                            if name not in current:
                                raise
        self._initialized = True

    @staticmethod
    def _source_revision(connection, job_id: str) -> tuple[str, str, int, list]:
        """Return a stable revision of only the rows used to build the cache."""
        sources = {}
        for name, table, predicate in (
            ("items", "discovery_job_items", " AND catalog_status IS NOT NULL"),
            ("catalog", "discovery_catalog_results", ""),
            ("listings", "discovery_listings", ""),
        ):
            row = connection.execute(
                f"SELECT COUNT(*),COALESCE(MAX(updated_at),'') FROM {table} "
                f"WHERE job_id=?{predicate}",
                (job_id,),
            ).fetchone()
            sources[name] = {"count": int(row[0]), "updated_at": str(row[1])}
        observation_rows = connection.execute(
            """SELECT observation_id,observation_json,updated_at
               FROM discovery_observations WHERE job_id=?""",
            (job_id,),
        ).fetchall()
        sources["observations"] = {
            "count": len(observation_rows),
            "updated_at": max(
                (str(row["updated_at"]) for row in observation_rows), default="",
            ),
        }
        payload = json.dumps(sources, sort_keys=True, separators=(",", ":"))
        token = "cache-v2:" + hashlib.sha256(payload.encode("utf-8")).hexdigest()
        latest = max(value["updated_at"] for value in sources.values())
        return token, latest, sources["items"]["count"], observation_rows

    @staticmethod
    def _legacy_marker_covers_source(marker, latest_source_update: str) -> bool:
        """Allow an indexed legacy marker to upgrade after metadata-only updates."""
        if not marker or str(marker["source_updated_at"]).startswith("cache-v2:"):
            return False
        return bool(
            latest_source_update
            and marker["indexed_at"]
            and latest_source_update <= str(marker["indexed_at"])
        )

    def index_completed_jobs(self, *, progress=None, batch_size: int = 500) -> int:
        """Index completed jobs idempotently; historical payload rows stay untouched."""
        self.initialize()
        indexed = 0
        observed = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        with self.store._connect() as connection:
            jobs = connection.execute(
                """SELECT job_id,updated_at FROM discovery_incremental_jobs
                   WHERE status='completed' ORDER BY updated_at"""
            ).fetchall()
            markers = {
                str(row["source_job_id"]): row for row in connection.execute(
                    "SELECT * FROM discovery_amazon_cache_indexed_jobs"
                )
            }
            pending_jobs = []
            marker_upgrades = []
            total = 0
            for job in jobs:
                job_id = str(job["job_id"])
                revision, latest_source_update, item_count, observation_rows = (
                    self._source_revision(connection, job_id)
                )
                marker = markers.get(job_id)
                if marker and str(marker["source_updated_at"]) == revision:
                    continue
                if self._legacy_marker_covers_source(marker, latest_source_update):
                    marker_upgrades.append((revision, observed, job_id))
                    continue
                pending_jobs.append((job, revision, item_count, observation_rows))
                total += item_count
            if marker_upgrades:
                connection.executemany(
                    """UPDATE discovery_amazon_cache_indexed_jobs
                       SET source_updated_at=?,indexed_at=? WHERE source_job_id=?""",
                    marker_upgrades,
                )
                connection.commit()
        completed = 0
        if progress is not None and pending_jobs:
            progress("preparing_cache", completed, total)
        for job, revision, _item_count, observation_rows in pending_jobs:
            job_id = str(job["job_id"])
            with self.store._connect() as connection:
                materialized_catalog = []
                materialized_fees = []
                listing_count = 0
                observations = []
                observations_by_id = {}
                observations_by_pair = {}
                for observation_row in observation_rows:
                    observation = json.loads(observation_row["observation_json"])
                    observations.append((observation_row, observation))
                    observation_id = str(observation_row["observation_id"] or "")
                    if observation_id:
                        observations_by_id[observation_id] = observation
                    identifier = str(observation.get("canonical_ean") or "")
                    asin = str(observation.get("asin") or "")
                    if identifier and asin:
                        observations_by_pair[(identifier, asin)] = observation
                rows = connection.execute(
                    """SELECT i.canonical_identifier,i.catalog_status,c.updated_at
                       FROM discovery_job_items i JOIN discovery_catalog_results c
                         ON c.job_id=i.job_id AND c.canonical_identifier=i.canonical_identifier
                       WHERE i.job_id=? AND i.catalog_status IS NOT NULL""",
                    (job_id,),
                ).fetchall()
                for row in rows:
                    identifier = str(row["canonical_identifier"])
                    source_listings = [
                        json.loads(value[0]) for value in connection.execute(
                            """SELECT listing_json FROM discovery_listings
                               WHERE job_id=? AND canonical_identifier=? ORDER BY asin""",
                            (job_id, identifier),
                        )
                    ]
                    listings = [dict(listing) for listing in source_listings]
                    for listing in listings:
                        listing.setdefault("catalog_observed_at", row["updated_at"])
                        asin = str(listing.get("asin") or "")
                        observation_id = str(listing.get("amazon_observation_id") or "")
                        observation = observations_by_id.get(observation_id)
                        if observation is None:
                            observation = observations_by_pair.get((identifier, asin))
                        if observation:
                            stamp = observation.get("observed_at")
                            listing.setdefault("pricing_observed_at", stamp)
                            listing.setdefault("competition_observed_at", stamp)
                            listing.setdefault("fee_status", observation.get("fee_status"))
                            listing.setdefault(
                                "fee_observed_at",
                                observation.get("fee_last_attempt_at") or stamp,
                            )
                            listing.setdefault(
                                "fee_cache_key",
                                fee_cache_key(
                                    observation.get("asin"),
                                    observation.get("reference_price"),
                                    observation.get("currency") or "EUR",
                                ),
                            )
                    freshness = {
                        "bsr_observed_at": max(
                            (item.get("catalog_observed_at") for item in listings
                             if item.get("catalog_observed_at")),
                            default=row["updated_at"],
                        ),
                        "pricing_observed_at": max(
                            (item.get("pricing_observed_at") for item in listings
                             if item.get("pricing_observed_at")), default=None,
                        ),
                        "competition_observed_at": max(
                            (item.get("competition_observed_at") for item in listings
                             if item.get("competition_observed_at")), default=None,
                        ),
                        "fee_cache_key": next(
                            (item.get("fee_cache_key") for item in listings if item.get("fee_cache_key")),
                            None,
                        ),
                        "fee_status": next(
                            (item.get("fee_status") for item in listings if item.get("fee_status")),
                            None,
                        ),
                        "fee_observed_at": max(
                            (item.get("fee_observed_at") for item in listings
                             if item.get("fee_observed_at")), default=None,
                        ),
                    }
                    listings_json, listings_sha256 = _canonical_payload(source_listings)
                    connection.execute(
                        """INSERT INTO discovery_amazon_cache
                           (canonical_identifier,source_job_id,catalog_status,catalog_observed_at,
                            freshness_json,listings_json,payload_schema_version,payload_sha256,
                            materialized_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?)
                           ON CONFLICT(canonical_identifier) DO UPDATE SET
                             source_job_id=excluded.source_job_id,
                             catalog_status=excluded.catalog_status,
                             catalog_observed_at=excluded.catalog_observed_at,
                             freshness_json=excluded.freshness_json,
                             listings_json=excluded.listings_json,
                             payload_schema_version=excluded.payload_schema_version,
                             payload_sha256=excluded.payload_sha256,
                             materialized_at=excluded.materialized_at,
                             updated_at=excluded.updated_at
                           WHERE excluded.catalog_observed_at >= discovery_amazon_cache.catalog_observed_at""",
                        (identifier, job_id, row["catalog_status"], row["updated_at"],
                         json.dumps(freshness, sort_keys=True, separators=(",", ":")),
                         listings_json, CACHE_PAYLOAD_SCHEMA_VERSION, listings_sha256,
                         observed, observed),
                    )
                    listing_count += len(source_listings)
                    materialized_catalog.append((identifier, listings_sha256))
                    indexed += 1
                    completed += 1
                    if progress is not None and (
                        completed % max(1, int(batch_size)) == 0 or completed == total
                    ):
                        progress("preparing_cache", completed, total)
                for observation_index, (row, value) in enumerate(observations, start=1):
                    key = fee_cache_key(
                        value.get("asin"), value.get("reference_price"), value.get("currency") or "EUR",
                    )
                    if not key or not value.get("fee_status"):
                        continue
                    fee_at = value.get("fee_last_attempt_at") or value.get("observed_at") or row["updated_at"]
                    observation_json, observation_sha256 = _canonical_payload(value)
                    connection.execute(
                        """INSERT INTO discovery_amazon_fee_cache
                           (fee_cache_key,source_job_id,observation_id,fee_status,fee_observed_at,
                            observation_json,payload_schema_version,payload_sha256,materialized_at,
                            updated_at) VALUES (?,?,?,?,?,?,?,?,?,?)
                           ON CONFLICT(fee_cache_key) DO UPDATE SET
                             source_job_id=excluded.source_job_id,
                             observation_id=excluded.observation_id,
                             fee_status=excluded.fee_status,
                             fee_observed_at=excluded.fee_observed_at,
                             observation_json=excluded.observation_json,
                             payload_schema_version=excluded.payload_schema_version,
                             payload_sha256=excluded.payload_sha256,
                             materialized_at=excluded.materialized_at,
                             updated_at=excluded.updated_at
                           WHERE excluded.fee_observed_at >= discovery_amazon_fee_cache.fee_observed_at""",
                        (key, job_id, row["observation_id"], value["fee_status"], fee_at,
                         observation_json, CACHE_PAYLOAD_SCHEMA_VERSION,
                         observation_sha256, observed, observed),
                    )
                    materialized_fees.append((key, observation_sha256))
                    if progress is not None and observation_index % max(1, int(batch_size)) == 0:
                        progress("preparing_cache", completed, total)
                aggregate_payload = {
                    "catalog": sorted(materialized_catalog),
                    "fees": sorted(materialized_fees),
                    "source_revision": revision,
                }
                _, aggregate_sha256 = _canonical_payload(aggregate_payload)
                connection.execute(
                    """INSERT INTO discovery_amazon_cache_indexed_jobs
                       (source_job_id,source_updated_at,indexed_at,materialization_version,
                        verification_state,catalog_count,listing_count,fee_count,
                        aggregate_sha256,verified_at) VALUES (?,?,?,?,?,?,?,?,?,?)
                       ON CONFLICT(source_job_id) DO UPDATE SET
                         source_updated_at=excluded.source_updated_at,
                         indexed_at=excluded.indexed_at,
                         materialization_version=excluded.materialization_version,
                         verification_state=excluded.verification_state,
                         catalog_count=excluded.catalog_count,
                         listing_count=excluded.listing_count,
                         fee_count=excluded.fee_count,
                         aggregate_sha256=excluded.aggregate_sha256,
                         verified_at=excluded.verified_at""",
                    (job_id, revision, observed, CACHE_PAYLOAD_SCHEMA_VERSION,
                     "verified", len(materialized_catalog), listing_count,
                     len(materialized_fees), aggregate_sha256, observed),
                )
                connection.commit()
        if progress is not None and pending_jobs:
            progress("preparing_cache", total, total)
        return indexed

    def get(self, identifier: str) -> dict[str, Any] | None:
        self.initialize()
        with self.store._connect() as connection:
            row = connection.execute(
                """SELECT source_job_id,catalog_status,catalog_observed_at,freshness_json,
                          listings_json,payload_schema_version,payload_sha256
                   FROM discovery_amazon_cache WHERE canonical_identifier=?""",
                (identifier,),
            ).fetchone()
            if not row:
                return None
            listings = _verified_payload(
                row["listings_json"], row["payload_schema_version"],
                row["payload_sha256"],
            )
            if not isinstance(listings, list):
                listings = [
                    json.loads(value["listing_json"])
                    for value in connection.execute(
                        """SELECT listing_json,updated_at FROM discovery_listings
                           WHERE job_id=? AND canonical_identifier=? ORDER BY asin""",
                        (row["source_job_id"], identifier),
                    )
                ]
            for listing in listings:
                listing.setdefault("catalog_observed_at", row["catalog_observed_at"])
                key = fee_cache_key(
                    listing.get("asin"), listing.get("reference_price"),
                    listing.get("currency") or "EUR",
                )
                if key:
                    fee_row = connection.execute(
                        """SELECT fee_status,fee_observed_at FROM discovery_amazon_fee_cache
                           WHERE fee_cache_key=?""", (key,),
                    ).fetchone()
                    if fee_row:
                        listing["fee_cache_key"] = key
                        listing["fee_status"] = fee_row["fee_status"]
                        listing["fee_observed_at"] = fee_row["fee_observed_at"]
        value = {
            "source_job_id": row["source_job_id"],
            "catalog_status": row["catalog_status"],
            "catalog_observed_at": row["catalog_observed_at"],
            "amazon_listings": listings,
            **json.loads(row["freshness_json"] or "{}"),
        }
        listings = value.get("amazon_listings") or []
        if listings:
            pricing_times = [row.get("pricing_observed_at") or row.get("observed_at") for row in listings]
            if any(pricing_times):
                value["pricing_observed_at"] = max(x for x in pricing_times if x)
                value["competition_observed_at"] = value["pricing_observed_at"]
            bsr_times = [row.get("catalog_observed_at") or value.get("catalog_observed_at") for row in listings]
            value["bsr_observed_at"] = max((x for x in bsr_times if x), default=None)
            fee_rows = [row for row in listings if row.get("fee_cache_key")]
            if fee_rows:
                newest = max(fee_rows, key=lambda row: row.get("fee_observed_at") or "")
                value["fee_cache_key"] = newest.get("fee_cache_key")
                value["fee_status"] = newest.get("fee_status")
                value["fee_observed_at"] = newest.get("fee_observed_at")
        return value

    def get_many(self, identifiers: Iterable[str], *, batch_size: int = 500):
        """Stream cached rows in bounded SQL pages."""
        self.initialize()
        values = iter(identifiers)
        while True:
            batch = []
            try:
                for _ in range(batch_size):
                    batch.append(next(values))
            except StopIteration:
                pass
            if not batch:
                return
            placeholders = ",".join("?" for _ in batch)
            with self.store._connect() as connection:
                rows = connection.execute(
                    f"""SELECT canonical_identifier,source_job_id,catalog_status,catalog_observed_at,
                               freshness_json,listings_json,payload_schema_version,payload_sha256
                        FROM discovery_amazon_cache
                        WHERE canonical_identifier IN ({placeholders})""",
                    tuple(batch),
                ).fetchall()
                materialized = {}
                legacy_identifiers = []
                for row in rows:
                    identifier = str(row["canonical_identifier"])
                    listings = _verified_payload(
                        row["listings_json"], row["payload_schema_version"],
                        row["payload_sha256"],
                    )
                    if isinstance(listings, list):
                        materialized[identifier] = listings
                    else:
                        legacy_identifiers.append(identifier)
                if legacy_identifiers:
                    legacy_placeholders = ",".join("?" for _ in legacy_identifiers)
                    listing_rows = connection.execute(
                        f"""SELECT l.job_id,l.canonical_identifier,l.listing_json,l.updated_at
                            FROM discovery_listings l JOIN discovery_amazon_cache c
                              ON c.source_job_id=l.job_id
                             AND c.canonical_identifier=l.canonical_identifier
                            WHERE c.canonical_identifier IN ({legacy_placeholders})
                            ORDER BY l.canonical_identifier,l.asin""",
                        tuple(legacy_identifiers),
                    ).fetchall()
                else:
                    listing_rows = []
                parsed_listing_rows = []
                fee_keys = set()
                for identifier, listings in materialized.items():
                    for listing in listings:
                        key = fee_cache_key(
                            listing.get("asin"), listing.get("reference_price"),
                            listing.get("currency") or "EUR",
                        )
                        if key:
                            fee_keys.add(key)
                        parsed_listing_rows.append((identifier, listing, key))
                for listing_row in listing_rows:
                    listing = json.loads(listing_row["listing_json"])
                    key = fee_cache_key(
                        listing.get("asin"), listing.get("reference_price"),
                        listing.get("currency") or "EUR",
                    )
                    if key:
                        fee_keys.add(key)
                    parsed_listing_rows.append((
                        str(listing_row["canonical_identifier"]), listing, key,
                    ))
                if fee_keys:
                    fee_placeholders = ",".join("?" for _ in fee_keys)
                    fee_rows = connection.execute(
                        f"""SELECT fee_cache_key,fee_status,fee_observed_at
                            FROM discovery_amazon_fee_cache
                            WHERE fee_cache_key IN ({fee_placeholders})""",
                        tuple(sorted(fee_keys)),
                    ).fetchall()
                else:
                    fee_rows = []
            found = {str(row["canonical_identifier"]): row for row in rows}
            fee_by_key = {str(row["fee_cache_key"]): row for row in fee_rows}
            listings_by_identifier: dict[str, list[dict[str, Any]]] = {}
            for listing_identifier, listing, key in parsed_listing_rows:
                fee_row = fee_by_key.get(key or "")
                if fee_row:
                    listing["fee_cache_key"] = key
                    listing["fee_status"] = fee_row["fee_status"]
                    listing["fee_observed_at"] = fee_row["fee_observed_at"]
                listings_by_identifier.setdefault(
                    listing_identifier, []
                ).append(listing)
            for identifier in batch:
                row = found.get(identifier)
                if not row:
                    yield identifier, None
                    continue
                value = {
                    "source_job_id": row["source_job_id"],
                    "catalog_status": row["catalog_status"],
                    "catalog_observed_at": row["catalog_observed_at"],
                    "amazon_listings": listings_by_identifier.get(identifier, []),
                    **json.loads(row["freshness_json"] or "{}"),
                }
                listings = value.get("amazon_listings") or []
                if listings:
                    pricing_times = [item.get("pricing_observed_at") for item in listings]
                    if any(pricing_times):
                        value["pricing_observed_at"] = max(
                            item for item in pricing_times if item
                        )
                    competition_times = [item.get("competition_observed_at") for item in listings]
                    if any(competition_times):
                        value["competition_observed_at"] = max(
                            item for item in competition_times if item
                        )
                    bsr_times = [
                        item.get("catalog_observed_at") or value.get("catalog_observed_at")
                        for item in listings
                    ]
                    value["bsr_observed_at"] = max(
                        (item for item in bsr_times if item), default=None,
                    )
                    fee_rows = [item for item in listings if item.get("fee_cache_key")]
                    if fee_rows:
                        newest = max(
                            fee_rows, key=lambda item: item.get("fee_observed_at") or "",
                        )
                        value["fee_cache_key"] = newest.get("fee_cache_key")
                        value["fee_status"] = newest.get("fee_status")
                        value["fee_observed_at"] = newest.get("fee_observed_at")
                yield identifier, value

    def preview_counts(
        self, identifiers: Iterable[str], policy: AmazonFreshnessPolicy,
    ) -> dict[str, int]:
        requested = sorted(set(identifiers))
        self.initialize()
        # Preview must not backfill or consume rotation.  Until the rebuildable
        # reference index has seen every historical job, classify a known
        # identifier conservatively as refresh (never as a false new lookup).
        with self.store._connect() as connection:
            known = {
                str(row[0]) for row in connection.execute(
                    """SELECT DISTINCT item.canonical_identifier
                       FROM discovery_job_items item
                       JOIN discovery_incremental_jobs job ON job.job_id=item.job_id
                       WHERE job.status IN ('completed','computed')
                         AND item.catalog_status IS NOT NULL"""
                )
            }

        def plans():
            for identifier, cached in self.get_many(requested):
                if cached is not None:
                    yield plan_cached_product(cached, policy=policy)
                elif identifier in known:
                    yield {
                        "primary_action": PlanAction.REFRESH_CATALOG.value,
                        "actions": [PlanAction.REFRESH_CATALOG.value],
                    }
                else:
                    yield {
                        "primary_action": PlanAction.NEW_LOOKUP.value,
                        "actions": [PlanAction.NEW_LOOKUP.value],
                    }

        return planning_counts(plans())

    def fee(self, asin: str, price: Any, currency: str = "EUR") -> dict[str, Any] | None:
        key = fee_cache_key(asin, price, currency)
        if not key:
            return None
        self.initialize()
        with self.store._connect() as connection:
            row = connection.execute(
                """SELECT source_job_id,observation_id,observation_json,fee_observed_at,
                          payload_schema_version,payload_sha256
                   FROM discovery_amazon_fee_cache WHERE fee_cache_key=?""",
                (key,),
            ).fetchone()
        if not row:
            return None
        value = _verified_payload(
            row["observation_json"], row["payload_schema_version"],
            row["payload_sha256"],
        )
        if not isinstance(value, dict):
            with self.store._connect() as connection:
                source = connection.execute(
                    """SELECT observation_json FROM discovery_observations
                       WHERE job_id=? AND observation_id=?""",
                    (row["source_job_id"], row["observation_id"]),
                ).fetchone()
            if not source:
                return None
            value = json.loads(source["observation_json"])
        value["fee_observed_at"] = row["fee_observed_at"]
        value["fee_cache_key"] = key
        return value


__all__ = [
    "AmazonFreshnessPolicy", "DiscoveryAmazonCache", "PlanAction",
    "CACHE_PAYLOAD_SCHEMA_VERSION", "POLICY_VERSION", "fee_cache_key",
    "plan_cached_product", "planning_counts",
    "reusable_fee",
]
