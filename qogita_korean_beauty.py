"""Versioned Qogita Korean Beauty membership acquisition and reconciliation.

This module deliberately owns no supplier enrichment.  Curated-search records
are reduced to GTIN/FID identity references and matched against the existing
global Qogita catalog, bootstrap and immutable serving snapshot.
"""

from __future__ import annotations

import html
import json
import math
import random
import re
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable
from urllib.parse import urlencode
from uuid import uuid4

import requests

from supplier_catalog import DEFAULT_DATABASE_PATH, canonical_gtin14, json_dumps


MEMBERSHIP_TYPE = "korean_beauty"
CURATED_PATH = "/categories/health-beauty/face/"
CURATED_SEARCH = "korean-beauty"
DEFAULT_BASE_URL = "https://www.qogita.com"


MEMBERSHIP_SCHEMA = """
CREATE TABLE IF NOT EXISTS qogita_membership_versions (
    membership_version_id TEXT PRIMARY KEY,
    membership_type TEXT NOT NULL,
    source_generation_id TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('collecting','valid','invalid')),
    acquisition_status TEXT,
    entry_count INTEGER NOT NULL DEFAULT 0,
    metrics_json TEXT NOT NULL DEFAULT '{}',
    error_message TEXT,
    created_at TEXT NOT NULL,
    completed_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_qogita_membership_versions_type_status
ON qogita_membership_versions(membership_type,status,observed_at DESC);

CREATE TABLE IF NOT EXISTS qogita_membership_entries (
    membership_version_id TEXT NOT NULL,
    canonical_gtin TEXT NOT NULL,
    canonical_product_key TEXT,
    variant_fid TEXT,
    PRIMARY KEY (membership_version_id, canonical_gtin),
    UNIQUE (membership_version_id, canonical_product_key),
    FOREIGN KEY (membership_version_id)
        REFERENCES qogita_membership_versions(membership_version_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_qogita_membership_entries_product
ON qogita_membership_entries(canonical_product_key,membership_version_id);

CREATE TABLE IF NOT EXISTS qogita_membership_active (
    membership_type TEXT PRIMARY KEY,
    membership_version_id TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (membership_version_id)
        REFERENCES qogita_membership_versions(membership_version_id)
);
"""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _connect(path: str | Path, *, read_only: bool = False) -> sqlite3.Connection:
    resolved = Path(path).expanduser().resolve()
    if read_only:
        connection = sqlite3.connect(f"file:{resolved}?mode=ro", uri=True, timeout=5)
    else:
        resolved.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(resolved, timeout=30)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    return connection


@dataclass(frozen=True)
class CuratedProduct:
    raw_gtin: str
    canonical_gtin: str | None
    variant_fid: str | None


def _normalized_rsc(value: str) -> str:
    # Full HTML responses embed the RSC stream in escaped script strings while
    # direct RSC responses expose nearly the same content without HTML entities.
    return html.unescape(str(value or "")).replace('\\"', '"')


def parse_curated_page(payload: str) -> dict[str, Any]:
    """Extract identity-only records and pagination metadata from HTML or RSC."""
    normalized = _normalized_rsc(payload)
    decoder = json.JSONDecoder()
    raw_products: list[dict[str, Any]] = []
    cursor = 0
    marker = '"product":'
    parse_errors = 0
    fallback_parses = 0
    while True:
        index = normalized.find(marker, cursor)
        if index < 0:
            break
        start = index + len(marker)
        next_index = normalized.find(marker, start)
        segment = normalized[start:next_index if next_index >= 0 else None]
        try:
            product, _ = decoder.raw_decode(normalized, start)
            if isinstance(product, dict) and (
                product.get("gtin") is not None or product.get("fid") is not None
            ):
                raw_products.append(product)
        except (json.JSONDecodeError, TypeError, ValueError):
            # A quoted marketing name can make the surrounding Flight object
            # unsuitable for whole-object decoding after HTML unescaping.  The
            # identity fields themselves are restricted tokens, so recover only
            # those two values from this product-bounded segment.
            gtin_match = re.search(r'"gtin"\s*:\s*"([^"]*)"', segment)
            fid_match = re.search(r'"fid"\s*:\s*(?:"([^"]*)"|null)', segment)
            if gtin_match:
                raw_products.append({
                    "gtin": gtin_match.group(1),
                    "fid": fid_match.group(1) if fid_match else None,
                })
                fallback_parses += 1
            else:
                parse_errors += 1
        cursor = start

    def integer(name: str) -> int | None:
        match = re.search(rf'"{re.escape(name)}"\s*:\s*(\d+)', normalized)
        return int(match.group(1)) if match else None

    records = [
        CuratedProduct(
            raw_gtin=str(row.get("gtin") or "").strip(),
            canonical_gtin=canonical_gtin14(row.get("gtin")),
            variant_fid=str(row.get("fid") or "").strip() or None,
        )
        for row in raw_products
    ]
    return {
        "records": records,
        "current_page": integer("currentPage"),
        "page_size": integer("pageSize"),
        "total_results": integer("totalResults"),
        "parse_errors": parse_errors,
        "fallback_parses": fallback_parses,
    }


def normalize_membership(records: Iterable[CuratedProduct]) -> dict[str, Any]:
    """Validate one acquisition without resolving identity conflicts arbitrarily."""
    materialized = list(records)
    valid_rows = [row for row in materialized if row.canonical_gtin]
    invalid_gtin = len(materialized) - len(valid_rows)
    fid_missing = sum(not row.variant_fid for row in valid_rows)

    gtin_fids: dict[str, set[str]] = {}
    fid_gtins: dict[str, set[str]] = {}
    pair_counts: dict[tuple[str, str | None], int] = {}
    for row in valid_rows:
        gtin = str(row.canonical_gtin)
        pair = (gtin, row.variant_fid)
        pair_counts[pair] = pair_counts.get(pair, 0) + 1
        if row.variant_fid:
            gtin_fids.setdefault(gtin, set()).add(row.variant_fid)
            fid_gtins.setdefault(row.variant_fid, set()).add(gtin)
        else:
            gtin_fids.setdefault(gtin, set())

    gtin_conflicts = {
        gtin: sorted(fids) for gtin, fids in gtin_fids.items() if len(fids) > 1
    }
    fid_conflicts = {
        fid: sorted(gtins) for fid, gtins in fid_gtins.items() if len(gtins) > 1
    }
    conflicted_gtins = set(gtin_conflicts)
    for values in fid_conflicts.values():
        conflicted_gtins.update(values)

    entries = []
    for gtin in sorted(gtin_fids):
        if gtin in conflicted_gtins:
            continue
        fids = gtin_fids[gtin]
        entries.append({
            "canonical_gtin": gtin,
            "variant_fid": next(iter(fids)) if fids else None,
        })

    duplicate_count = sum(max(0, count - 1) for count in pair_counts.values())
    return {
        "entries": entries,
        "metrics": {
            "records_raw": len(materialized),
            "gtin_valid": len(valid_rows),
            "gtin_unique": len(gtin_fids),
            "duplicate_count": duplicate_count,
            "invalid_gtin_count": invalid_gtin,
            "fid_present_count": sum(bool(row.variant_fid) for row in valid_rows),
            "fid_missing_count": fid_missing,
            "gtin_fid_conflict_count": len(gtin_conflicts),
            "fid_gtin_conflict_count": len(fid_conflicts),
            "excluded_conflicted_gtin_count": len(conflicted_gtins),
        },
        "gtin_fid_conflicts": gtin_conflicts,
        "fid_gtin_conflicts": fid_conflicts,
    }


class QogitaKoreanBeautyCollector:
    """Bounded page collector for the official Qogita curated-search RSC route."""

    def __init__(
        self, *, base_url: str = DEFAULT_BASE_URL, session=None,
        timeout_seconds: float = 30, pacing_seconds: float = 0.35,
        max_attempts: int = 3, sleep_func: Callable[[float], None] = time.sleep,
    ):
        self.base_url = base_url.rstrip("/")
        self.session = session or requests.Session()
        self.owns_session = session is None
        self.timeout_seconds = float(timeout_seconds)
        self.pacing_seconds = max(0.0, float(pacing_seconds))
        self.max_attempts = max(1, int(max_attempts))
        self.sleep_func = sleep_func
        self.retry_count = 0
        self.http_status_counts: dict[str, int] = {}
        self.last_request_error: str | None = None

    def close(self):
        if self.owns_session:
            self.session.close()

    def _url(self, page: int) -> str:
        return self.base_url + CURATED_PATH + "?" + urlencode({
            "curatedSearch": CURATED_SEARCH, "page": int(page),
        })

    def _request(self, page: int):
        last_error: BaseException | None = None
        for attempt in range(1, self.max_attempts + 1):
            response = None
            try:
                response = self.session.get(
                    self._url(page), headers={
                        "Accept": "text/x-component",
                        "RSC": "1",
                        "User-Agent": "GlowUp-Scout-Qogita-Membership/1.0",
                    }, timeout=self.timeout_seconds, allow_redirects=True,
                )
                status = int(response.status_code)
                self.http_status_counts[str(status)] = (
                    self.http_status_counts.get(str(status), 0) + 1
                )
                if status == 200:
                    return response
                if status != 429 and status < 500:
                    self.last_request_error = (
                        f"Qogita curated page {page} returned permanent HTTP {status}"
                    )
                    raise RuntimeError(self.last_request_error)
                last_error = requests.HTTPError(f"HTTP {status}")
            except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as exc:
                last_error = exc
            if attempt < self.max_attempts:
                self.retry_count += 1
                delay = min(8.0, 0.5 * (2 ** (attempt - 1)))
                retry_after = getattr(response, "headers", {}).get("Retry-After") if response else None
                if str(retry_after or "").replace(".", "", 1).isdigit():
                    delay = max(delay, float(retry_after))
                delay *= 1 + random.uniform(0, 0.15)
                self.sleep_func(delay)
        self.last_request_error = f"page={page} error={last_error}"
        raise RuntimeError(self.last_request_error) from last_error

    def collect(self, *, max_pages: int | None = None) -> dict[str, Any]:
        started = time.monotonic()
        records: list[CuratedProduct] = []
        http_errors = 0
        parsing_errors = 0
        fallback_parses = 0
        requested = 0
        first_total = final_total = page_size = expected_pages = None
        page = 1
        incomplete = False
        failed_page = None
        self.retry_count = 0
        self.http_status_counts = {}
        self.last_request_error = None
        while True:
            try:
                response = self._request(page)
            except RuntimeError:
                http_errors += 1
                incomplete = True
                failed_page = page
                break
            parsed = parse_curated_page(response.text)
            requested += 1
            parsing_errors += int(parsed["parse_errors"])
            fallback_parses += int(parsed["fallback_parses"])
            if parsed["current_page"] is not None and parsed["current_page"] != page:
                parsing_errors += 1
                incomplete = True
                break
            if first_total is None:
                first_total = parsed["total_results"]
                page_size = parsed["page_size"] or len(parsed["records"])
                if first_total is not None and page_size:
                    expected_pages = max(1, math.ceil(first_total / page_size))
            if parsed["total_results"] is not None:
                final_total = parsed["total_results"]
                if page_size:
                    observed_pages = max(1, math.ceil(final_total / page_size))
                    expected_pages = max(expected_pages or 0, observed_pages)
            if not parsed["records"]:
                parsing_errors += 1
                incomplete = True
                break
            records.extend(parsed["records"])
            if max_pages is not None and page >= int(max_pages):
                incomplete = bool(expected_pages and page < expected_pages)
                break
            if expected_pages is not None and page >= expected_pages:
                break
            page += 1
            if self.pacing_seconds:
                self.sleep_func(self.pacing_seconds)

        normalized = normalize_membership(records)
        reported_total_delta = (
            len(records) - int(final_total) if final_total is not None else None
        )
        metrics = {
            "total_results_initial": first_total,
            "total_results_final": final_total,
            "page_size": page_size,
            "pages_expected": expected_pages,
            "pages_requested": requested,
            "http_error_count": http_errors,
            "http_retry_count": self.retry_count,
            "http_status_counts": dict(self.http_status_counts),
            "failed_page": failed_page,
            "last_request_error": self.last_request_error,
            "parsing_error_count": parsing_errors,
            "fallback_parse_count": fallback_parses,
            "reported_total_record_delta": reported_total_delta,
            "duration_seconds": time.monotonic() - started,
            **normalized["metrics"],
        }
        identity_anomalies = (
            metrics["gtin_fid_conflict_count"]
            + metrics["fid_gtin_conflict_count"]
            + metrics["invalid_gtin_count"]
            + metrics["fid_missing_count"]
        )
        source_anomalies = int(reported_total_delta not in (None, 0))
        if incomplete or http_errors or parsing_errors:
            acquisition_status = "incomplete"
        elif identity_anomalies or source_anomalies:
            acquisition_status = "complete_with_anomalies"
        else:
            acquisition_status = "complete"
        return {
            "membership_type": MEMBERSHIP_TYPE,
            "acquisition_status": acquisition_status,
            "entries": normalized["entries"],
            "metrics": metrics,
            "gtin_fid_conflicts": normalized["gtin_fid_conflicts"],
            "fid_gtin_conflicts": normalized["fid_gtin_conflicts"],
        }


class QogitaMembershipReconciler:
    """Read-only comparison with global catalog/bootstrap/serving state."""

    def __init__(self, path: str | Path = DEFAULT_DATABASE_PATH):
        self.path = Path(path).expanduser().resolve()

    def reconcile(
        self, entries: Iterable[dict[str, Any]], *, source_generation_id: str,
        bootstrap_run_id: str, serving_generation_id: str, batch_size: int = 500,
    ) -> dict[str, Any]:
        materialized = [dict(row) for row in entries]
        reconciled: list[dict[str, Any]] = []
        catalog_missing = []
        status_counts: dict[str, int] = {}
        catalog_fid_equal = catalog_fid_different = catalog_fid_missing = 0
        serving_count = serving_scenarios = 0
        with _connect(self.path, read_only=True) as connection:
            for offset in range(0, len(materialized), max(1, int(batch_size))):
                batch = materialized[offset:offset + max(1, int(batch_size))]
                requested = {str(row["canonical_gtin"]): row for row in batch}
                placeholders = ",".join("?" for _ in requested)
                products = connection.execute(
                    f"""SELECT canonical_gtin,canonical_product_key,variant_fid
                           FROM supplier_catalog_products
                          WHERE run_id=? AND canonical_gtin IN ({placeholders})""",
                    (source_generation_id, *requested),
                ).fetchall()
                products_by_gtin = {str(row["canonical_gtin"]): row for row in products}
                keys = [str(row["canonical_product_key"]) for row in products]
                bootstrap_by_key = {}
                serving_by_key = {}
                if keys:
                    key_placeholders = ",".join("?" for _ in keys)
                    bootstrap_by_key = {
                        str(row["canonical_product_key"]): str(row["status"])
                        for row in connection.execute(
                            f"""SELECT canonical_product_key,status
                                   FROM qogita_bootstrap_products
                                  WHERE bootstrap_run_id=?
                                    AND canonical_product_key IN ({key_placeholders})""",
                            (bootstrap_run_id, *keys),
                        )
                    }
                    serving_by_key = {
                        str(row["canonical_product_key"]): int(row["scenario_count"] or 0)
                        for row in connection.execute(
                            f"""SELECT canonical_product_key,scenario_count
                                   FROM qogita_serving_memberships
                                  WHERE serving_generation_id=?
                                    AND canonical_product_key IN ({key_placeholders})""",
                            (serving_generation_id, *keys),
                        )
                    }
                for gtin, curated in requested.items():
                    product = products_by_gtin.get(gtin)
                    if not product:
                        catalog_missing.append(gtin)
                        reconciled.append({
                            "canonical_gtin": gtin,
                            "canonical_product_key": None,
                            "variant_fid": curated.get("variant_fid"),
                        })
                        continue
                    curated_fid = curated.get("variant_fid")
                    catalog_fid = str(product["variant_fid"] or "") or None
                    if catalog_fid is None:
                        catalog_fid_missing += 1
                    elif curated_fid == catalog_fid:
                        catalog_fid_equal += 1
                    elif curated_fid is not None:
                        catalog_fid_different += 1
                    key = str(product["canonical_product_key"])
                    status = bootstrap_by_key.get(key, "missing")
                    status_counts[status] = status_counts.get(status, 0) + 1
                    scenario_count = serving_by_key.get(key)
                    if scenario_count is not None:
                        serving_count += 1
                        serving_scenarios += scenario_count
                    reconciled.append({
                        "canonical_gtin": gtin,
                        "canonical_product_key": key,
                        "variant_fid": curated_fid,
                    })
        total = len(materialized)
        return {
            "entries": reconciled,
            "metrics": {
                "membership_gtin_unique": total,
                "catalog_present_count": total - len(catalog_missing),
                "catalog_absent_count": len(catalog_missing),
                "catalog_fid_equal_count": catalog_fid_equal,
                "catalog_fid_different_count": catalog_fid_different,
                "catalog_fid_missing_count": catalog_fid_missing,
                "bootstrap_status_counts": status_counts,
                "serving_present_count": serving_count,
                "serving_absent_count": total - serving_count,
                "serving_coverage_percent": (serving_count / total * 100.0) if total else 0.0,
                "serving_scenario_count": serving_scenarios,
            },
            "catalog_absent_gtins": catalog_missing,
        }


class QogitaMembershipStore:
    """Version and atomically publish identity-only memberships."""

    def __init__(self, path: str | Path = DEFAULT_DATABASE_PATH):
        self.path = Path(path).expanduser().resolve()

    def initialize(self):
        with _connect(self.path) as connection:
            connection.executescript(MEMBERSHIP_SCHEMA)

    def create_version(
        self, *, source_generation_id: str, observed_at: str | None = None,
        membership_version_id: str | None = None,
    ) -> dict[str, Any]:
        self.initialize()
        version_id = membership_version_id or uuid4().hex
        now = utc_now()
        with _connect(self.path) as connection:
            connection.execute(
                """INSERT INTO qogita_membership_versions (
                       membership_version_id,membership_type,source_generation_id,
                       observed_at,status,created_at
                   ) VALUES (?,?,?,?,'collecting',?)""",
                (version_id, MEMBERSHIP_TYPE, source_generation_id, observed_at or now, now),
            )
        return self.version(version_id)

    def finalize_version(
        self, membership_version_id: str, *, entries: Iterable[dict[str, Any]],
        acquisition_status: str, metrics: dict[str, Any], error_message: str | None = None,
    ) -> dict[str, Any]:
        materialized = [dict(row) for row in entries]
        blocking_anomalies = int(metrics.get("gtin_fid_conflict_count") or 0) + int(
            metrics.get("fid_gtin_conflict_count") or 0
        ) + int(metrics.get("catalog_fid_different_count") or 0) + int(
            metrics.get("invalid_gtin_count") or 0
        ) + int(metrics.get("fid_missing_count") or 0)
        valid = (
            acquisition_status in {"complete", "complete_with_anomalies"}
            and bool(materialized) and not blocking_anomalies and not error_message
        )
        status = "valid" if valid else "invalid"
        now = utc_now()
        connection = _connect(self.path)
        try:
            connection.execute("BEGIN IMMEDIATE")
            current = connection.execute(
                "SELECT status,source_generation_id FROM qogita_membership_versions "
                "WHERE membership_version_id=?", (membership_version_id,),
            ).fetchone()
            if not current or current["status"] != "collecting":
                raise ValueError("Membership version is not collecting")
            connection.executemany(
                """INSERT INTO qogita_membership_entries (
                       membership_version_id,canonical_gtin,canonical_product_key,variant_fid
                   ) VALUES (?,?,?,?)""",
                ((membership_version_id, row["canonical_gtin"],
                  row["canonical_product_key"], row.get("variant_fid"))
                 for row in materialized),
            )
            connection.execute(
                """UPDATE qogita_membership_versions
                      SET status=?,acquisition_status=?,entry_count=?,metrics_json=?,
                          error_message=?,completed_at=?
                    WHERE membership_version_id=?""",
                (status, acquisition_status, len(materialized), json_dumps(metrics),
                 error_message, now, membership_version_id),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return self.version(membership_version_id)

    def activate(self, membership_version_id: str) -> dict[str, Any]:
        """Atomically move only the logical membership pointer to a valid version."""
        now = utc_now()
        connection = _connect(self.path)
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """SELECT membership_type,status FROM qogita_membership_versions
                    WHERE membership_version_id=?""", (membership_version_id,),
            ).fetchone()
            if not row or row["status"] != "valid":
                raise ValueError("Only a valid membership version can be activated")
            connection.execute(
                """INSERT INTO qogita_membership_active (
                       membership_type,membership_version_id,updated_at
                   ) VALUES (?,?,?) ON CONFLICT(membership_type) DO UPDATE SET
                       membership_version_id=excluded.membership_version_id,
                       updated_at=excluded.updated_at""",
                (row["membership_type"], membership_version_id, now),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return self.active(MEMBERSHIP_TYPE)

    def version(self, membership_version_id: str) -> dict[str, Any] | None:
        with _connect(self.path) as connection:
            row = connection.execute(
                "SELECT * FROM qogita_membership_versions WHERE membership_version_id=?",
                (membership_version_id,),
            ).fetchone()
        if not row:
            return None
        result = dict(row)
        result["metrics"] = json.loads(result.pop("metrics_json") or "{}")
        return result

    def active(self, membership_type: str = MEMBERSHIP_TYPE) -> dict[str, Any] | None:
        with _connect(self.path) as connection:
            row = connection.execute(
                """SELECT version.* FROM qogita_membership_active active
                     JOIN qogita_membership_versions version
                       ON version.membership_version_id=active.membership_version_id
                    WHERE active.membership_type=?""", (membership_type,),
            ).fetchone()
        if not row:
            return None
        result = dict(row)
        result["metrics"] = json.loads(result.pop("metrics_json") or "{}")
        return result

    def entries(self, membership_version_id: str) -> list[dict[str, Any]]:
        with _connect(self.path, read_only=True) as connection:
            return [dict(row) for row in connection.execute(
                """SELECT canonical_gtin,canonical_product_key,variant_fid
                     FROM qogita_membership_entries
                    WHERE membership_version_id=? ORDER BY canonical_gtin""",
                (membership_version_id,),
            )]


def active_qogita_context(path: str | Path = DEFAULT_DATABASE_PATH) -> dict[str, str]:
    """Return the immutable global Qogita context used by membership refresh."""
    with _connect(path, read_only=True) as connection:
        row = connection.execute(
            """SELECT snapshot.source_generation_id,snapshot.bootstrap_run_id,
                      snapshot.serving_generation_id
                 FROM qogita_serving_active active
                 JOIN qogita_serving_snapshots snapshot
                   ON snapshot.serving_generation_id=active.serving_generation_id
                WHERE active.supplier='qogita' AND snapshot.status='valid'"""
        ).fetchone()
    if not row:
        raise RuntimeError("No active valid Qogita serving snapshot")
    return dict(row)


def compare_memberships(
    previous_entries: Iterable[dict[str, Any]],
    current_entries: Iterable[dict[str, Any]],
) -> dict[str, Any]:
    """Compare identity membership without interpreting catalog/enrichment state."""
    previous = {
        str(row["canonical_gtin"]): str(row.get("variant_fid") or "") or None
        for row in previous_entries
    }
    current = {
        str(row["canonical_gtin"]): str(row.get("variant_fid") or "") or None
        for row in current_entries
    }
    previous_gtins = set(previous)
    current_gtins = set(current)
    retained = previous_gtins & current_gtins
    fid_changed = sorted(gtin for gtin in retained if previous[gtin] != current[gtin])
    previous_count = len(previous_gtins)
    current_count = len(current_gtins)
    return {
        "previous_entry_count": previous_count,
        "current_entry_count": current_count,
        "gtin_added_count": len(current_gtins - previous_gtins),
        "gtin_removed_count": len(previous_gtins - current_gtins),
        "gtin_unchanged_count": len(retained),
        "fid_unchanged_count": len(retained) - len(fid_changed),
        "fid_changed_count": len(fid_changed),
        "fid_changed_gtins": fid_changed,
        "entry_delta_percent": (
            (current_count - previous_count) / previous_count * 100.0
            if previous_count else (100.0 if current_count else 0.0)
        ),
    }


def membership_validation_errors(
    acquisition_status: str, metrics: dict[str, Any], entries: Iterable[dict[str, Any]],
) -> list[str]:
    """Mirror the existing membership validation gate without persisting a version."""
    errors = []
    if acquisition_status not in {"complete", "complete_with_anomalies"}:
        errors.append(f"acquisition_status={acquisition_status}")
    if not list(entries):
        errors.append("membership_empty")
    for field in (
        "gtin_fid_conflict_count", "fid_gtin_conflict_count",
        "catalog_fid_different_count", "invalid_gtin_count", "fid_missing_count",
    ):
        if int(metrics.get(field) or 0):
            errors.append(f"{field}={int(metrics[field])}")
    return errors


def refresh_korean_beauty_membership(
    *, path: str | Path = DEFAULT_DATABASE_PATH,
    collector: QogitaKoreanBeautyCollector | None = None,
    persist: bool = False, activate: bool = False,
    max_pages: int | None = None,
    membership_version_id: str | None = None,
) -> dict[str, Any]:
    """Acquire, compare and optionally atomically publish the curated membership.

    ``persist=False`` is a production-safe dry run: all database access is
    read-only and the proposed version identifier is never inserted.
    """
    if activate and not persist:
        raise ValueError("activate requires persist")
    database = Path(path).expanduser().resolve()
    context = active_qogita_context(database)
    store = QogitaMembershipStore(database)
    previous = store.active()
    previous_entries = store.entries(previous["membership_version_id"]) if previous else []
    owned_collector = collector is None
    collector = collector or QogitaKoreanBeautyCollector()
    try:
        acquisition = collector.collect(max_pages=max_pages)
    finally:
        if owned_collector:
            collector.close()
    reconciliation = QogitaMembershipReconciler(database).reconcile(
        acquisition["entries"],
        source_generation_id=context["source_generation_id"],
        bootstrap_run_id=context["bootstrap_run_id"],
        serving_generation_id=context["serving_generation_id"],
    )
    combined_metrics = {**acquisition["metrics"], **reconciliation["metrics"]}
    membership_diff = compare_memberships(previous_entries, reconciliation["entries"])
    validation_errors = membership_validation_errors(
        acquisition["acquisition_status"], combined_metrics, reconciliation["entries"],
    )
    proposed_version_id = membership_version_id or uuid4().hex
    membership_version = None
    active_membership = None
    if persist:
        membership_version = store.create_version(
            source_generation_id=context["source_generation_id"],
            membership_version_id=proposed_version_id,
        )
        membership_version = store.finalize_version(
            proposed_version_id,
            entries=reconciliation["entries"],
            acquisition_status=acquisition["acquisition_status"],
            metrics={**combined_metrics, "membership_diff": membership_diff},
            error_message="; ".join(validation_errors) or None,
        )
        if activate and membership_version["status"] == "valid":
            active_membership = store.activate(proposed_version_id)
    return {
        "status": "membership_activated" if active_membership else (
            "membership_persisted" if membership_version else (
                "dry_run_invalid" if validation_errors else "dry_run_complete"
            )
        ),
        "production_writes": bool(persist),
        "membership_activation": bool(active_membership),
        "would_activate": not validation_errors,
        "proposed_membership_version_id": proposed_version_id,
        "previous_membership_version_id": (
            previous.get("membership_version_id") if previous else None
        ),
        **context,
        "acquisition_status": acquisition["acquisition_status"],
        "curated": acquisition["metrics"],
        "catalog_bootstrap_serving": reconciliation["metrics"],
        "catalog_absent_gtins": reconciliation["catalog_absent_gtins"],
        "gtin_fid_conflicts": acquisition["gtin_fid_conflicts"],
        "fid_gtin_conflicts": acquisition["fid_gtin_conflicts"],
        "membership_diff": membership_diff,
        "validation_errors": validation_errors,
        "membership_version": membership_version,
        "active_membership": active_membership,
    }
