"""Offline-first Qogita catalog request, webhook, download and staging pipeline."""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import csv
import sqlite3
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import requests

from supplier_catalog import DEFAULT_DATABASE_PATH, SupplierCatalogStore, json_dumps, utc_now
from supplier_catalog_collectors import QogitaCatalogExportReader


LOGGER = logging.getLogger(__name__)
QOGITA_SIGNATURE_HEADER = "X-Qogita-Signature"
QOGITA_WEBHOOK_EVENTS = {
    "webhook.test", "catalog_download.completed", "catalog_download.failed",
}
DEFAULT_SIGNATURE_TOLERANCE_SECONDS = 300
DEFAULT_MAX_DOWNLOAD_BYTES = 2 * 1024 * 1024 * 1024


class QogitaPipelineError(RuntimeError):
    code = "qogita_pipeline_error"


class WebhookAuthenticationError(QogitaPipelineError):
    code = "invalid_webhook_signature"


class WebhookPayloadError(QogitaPipelineError):
    code = "invalid_webhook_payload"


class UnknownCatalogRequest(QogitaPipelineError):
    code = "unknown_catalog_request"


class CatalogDownloadError(QogitaPipelineError):
    code = "catalog_download_failed"


class CatalogValidationError(QogitaPipelineError):
    code = "catalog_validation_failed"


PIPELINE_SCHEMA = """
CREATE TABLE IF NOT EXISTS qogita_catalog_requests (
    catalog_request_id TEXT PRIMARY KEY,
    supplier TEXT NOT NULL DEFAULT 'qogita',
    requested_at TEXT NOT NULL,
    request_mode TEXT NOT NULL,
    filters_json TEXT NOT NULL,
    request_body_json TEXT NOT NULL,
    status TEXT NOT NULL,
    webhook_received_at TEXT,
    completed_at TEXT,
    failed_at TEXT,
    failure_reason TEXT,
    download_url TEXT,
    download_started_at TEXT,
    download_completed_at TEXT,
    local_file_path TEXT,
    file_size INTEGER,
    checksum TEXT,
    catalog_as_of TEXT,
    row_count INTEGER,
    unique_gtin_count INTEGER,
    validation_json TEXT,
    staging_run_id TEXT,
    full_coverage_verified INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_qogita_catalog_requests_status
ON qogita_catalog_requests (status, requested_at);

CREATE TABLE IF NOT EXISTS qogita_webhook_events (
    event_key TEXT PRIMARY KEY,
    event_id TEXT,
    event_type TEXT NOT NULL,
    catalog_request_id TEXT NOT NULL,
    signature_timestamp INTEGER NOT NULL,
    received_at TEXT NOT NULL,
    payload_checksum TEXT NOT NULL,
    processing_status TEXT NOT NULL,
    FOREIGN KEY (catalog_request_id) REFERENCES qogita_catalog_requests(catalog_request_id)
);

CREATE TABLE IF NOT EXISTS qogita_enrichment_queue (
    run_id TEXT NOT NULL,
    canonical_product_key TEXT NOT NULL,
    variant_fid TEXT,
    task_type TEXT NOT NULL,
    reason TEXT NOT NULL,
    priority INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TEXT,
    source_observed_at TEXT,
    created_at TEXT NOT NULL,
    PRIMARY KEY (run_id, canonical_product_key, task_type)
);
CREATE INDEX IF NOT EXISTS idx_qogita_enrichment_queue_pending
ON qogita_enrichment_queue (run_id, status, priority DESC, created_at);
"""


def _connect(path: str | Path) -> sqlite3.Connection:
    absolute = Path(path).expanduser().resolve()
    absolute.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(absolute)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    try:
        os.chmod(absolute, 0o600)
    except FileNotFoundError:
        pass
    return connection


def sanitize_url(value: str) -> str:
    parsed = urlsplit(str(value or ""))
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))


def validate_download_url(value: str, *, allowed_hosts: set[str]) -> str:
    parsed = urlsplit(str(value or ""))
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
        raise CatalogDownloadError("Catalog download URL must be credential-free HTTPS")
    clean_hosts = {host.casefold() for host in allowed_hosts if host}
    if not clean_hosts or parsed.hostname.casefold() not in clean_hosts:
        raise CatalogDownloadError("Catalog download host is not allowlisted")
    return parsed.geturl()


def verify_qogita_signature(
    signature_header: str | None, raw_body: bytes, signing_secret: str,
    *, now_timestamp: int | None = None,
    tolerance_seconds: int = DEFAULT_SIGNATURE_TOLERANCE_SECONDS,
) -> int:
    if not signature_header or not signing_secret:
        raise WebhookAuthenticationError("Missing Qogita webhook signature")
    parts = {}
    for component in signature_header.split(","):
        key, separator, value = component.strip().partition("=")
        if separator and key and value:
            parts.setdefault(key, []).append(value)
    try:
        timestamp = int(parts["t"][0])
        signatures = parts["v1"]
    except (KeyError, IndexError, TypeError, ValueError) as exc:
        raise WebhookAuthenticationError("Malformed Qogita webhook signature") from exc
    now_value = int(now_timestamp if now_timestamp is not None else datetime.now(timezone.utc).timestamp())
    if abs(now_value - timestamp) > int(tolerance_seconds):
        raise WebhookAuthenticationError("Expired Qogita webhook timestamp")
    signed = str(timestamp).encode("ascii") + b"." + bytes(raw_body)
    expected = hmac.new(signing_secret.encode("utf-8"), signed, hashlib.sha256).hexdigest()
    if not any(hmac.compare_digest(expected, candidate) for candidate in signatures):
        raise WebhookAuthenticationError("Invalid Qogita webhook signature")
    return timestamp


def _payload_value(payload: dict[str, Any], *keys: str):
    data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
    object_data = data.get("object") if isinstance(data.get("object"), dict) else {}
    for key in keys:
        if payload.get(key) is not None:
            return payload[key]
        if data.get(key) is not None:
            return data[key]
        if object_data.get(key) is not None:
            return object_data[key]
    return None


def parse_qogita_event(raw_body: bytes) -> dict[str, Any]:
    try:
        payload = json.loads(raw_body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise WebhookPayloadError("Webhook body is not valid UTF-8 JSON") from exc
    if not isinstance(payload, dict):
        raise WebhookPayloadError("Webhook body must be a JSON object")
    event_type = str(_payload_value(payload, "eventType", "event_type", "type") or "")
    request_id = str(_payload_value(payload, "catalogRequestId", "catalog_request_id") or "")
    event_id = str(_payload_value(payload, "eventId", "event_id", "id") or "") or None
    event_created_at = str(_payload_value(payload, "createdAt", "created_at", "timestamp") or "")
    if event_type not in QOGITA_WEBHOOK_EVENTS:
        raise WebhookPayloadError("Unsupported webhook event")
    if event_type != "webhook.test" and not request_id:
        raise WebhookPayloadError("Catalog event has no catalogRequestId")
    download_url = _payload_value(payload, "downloadUrl", "download_url", "url")
    if event_type == "catalog_download.completed" and not str(download_url or "").strip():
        raise WebhookPayloadError("Completed event has no download URL")
    failure_reason = _payload_value(
        payload, "failureReason", "failure_reason", "error_message", "error", "message",
    )
    fallback_material = f"{event_type}|{request_id}|{event_created_at}".encode("utf-8")
    event_key = event_id or hashlib.sha256(fallback_material).hexdigest()
    return {
        "event_key": event_key, "event_id": event_id, "event_type": event_type,
        "catalog_request_id": request_id,
        "download_url": str(download_url or "").strip() or None,
        "failure_reason": str(failure_reason or "").strip() or None,
    }


class QogitaCatalogPipelineStore:
    def __init__(self, path: str | Path = DEFAULT_DATABASE_PATH):
        self.path = Path(path).expanduser().resolve()

    def initialize(self):
        with _connect(self.path) as connection:
            connection.executescript(PIPELINE_SCHEMA)
            columns = {
                row[1] for row in connection.execute(
                    "PRAGMA table_info(qogita_enrichment_queue)"
                )
            }
            if "source_observed_at" not in columns:
                connection.execute(
                    "ALTER TABLE qogita_enrichment_queue ADD COLUMN source_observed_at TEXT"
                )
        os.chmod(self.path, 0o600)

    def create_request(
        self, catalog_request_id: str, *, request_mode: str, filters: dict[str, Any],
        request_body: dict[str, Any], requested_at: str | None = None,
    ) -> dict[str, Any]:
        self.initialize()
        mode = str(request_mode or "").casefold()
        if mode not in {"full", "filtered"}:
            raise ValueError("request_mode must be full or filtered")
        filters = dict(filters or {})
        request_body = dict(request_body or {})
        if mode == "full" and (filters or request_body):
            raise ValueError("A full Qogita request requires explicitly empty filters and body")
        now = requested_at or utc_now()
        with _connect(self.path) as connection:
            connection.execute(
                """INSERT INTO qogita_catalog_requests (
                    catalog_request_id,requested_at,request_mode,filters_json,
                    request_body_json,status,created_at,updated_at
                ) VALUES (?,?,?,?,?,'requested',?,?)""",
                (str(catalog_request_id), now, mode, json_dumps(filters),
                 json_dumps(request_body), now, now),
            )
        return self.request(str(catalog_request_id))

    def request(self, catalog_request_id: str) -> dict[str, Any] | None:
        self.initialize()
        with _connect(self.path) as connection:
            row = connection.execute(
                "SELECT * FROM qogita_catalog_requests WHERE catalog_request_id=?",
                (str(catalog_request_id),),
            ).fetchone()
        if not row:
            return None
        result = dict(row)
        result["filters"] = json.loads(result.pop("filters_json") or "{}")
        result["request_body"] = json.loads(result.pop("request_body_json") or "{}")
        result["validation"] = json.loads(result.pop("validation_json") or "null")
        result["full_coverage_verified"] = bool(result["full_coverage_verified"])
        if result.get("download_url"):
            result["download_url"] = sanitize_url(result["download_url"])
        return result

    def pending_requests(self):
        self.initialize()
        with _connect(self.path) as connection:
            ids = [row[0] for row in connection.execute(
                """SELECT catalog_request_id FROM qogita_catalog_requests
                   WHERE status NOT IN ('failed','staged') ORDER BY requested_at"""
            )]
        return [self.request(value) for value in ids]

    def record_event(self, event: dict[str, Any], *, signature_timestamp: int,
                     raw_body: bytes, received_at: str | None = None):
        self.initialize()
        request_id = event["catalog_request_id"]
        received = received_at or utc_now()
        connection = _connect(self.path)
        try:
            connection.execute("BEGIN IMMEDIATE")
            duplicate = connection.execute(
                "SELECT 1 FROM qogita_webhook_events WHERE event_key=?",
                (event["event_key"],),
            ).fetchone()
            if duplicate:
                connection.rollback()
                return {"status": "duplicate", "catalog_request_id": request_id}
            request = connection.execute(
                "SELECT status FROM qogita_catalog_requests WHERE catalog_request_id=?",
                (request_id,),
            ).fetchone()
            if not request:
                raise UnknownCatalogRequest("Webhook does not match a local catalog request")
            connection.execute(
                """INSERT INTO qogita_webhook_events (
                    event_key,event_id,event_type,catalog_request_id,signature_timestamp,
                    received_at,payload_checksum,processing_status
                ) VALUES (?,?,?,?,?,?,?,'accepted')""",
                (event["event_key"], event.get("event_id"), event["event_type"], request_id,
                 signature_timestamp, received, hashlib.sha256(raw_body).hexdigest()),
            )
            if event["event_type"] == "catalog_download.completed":
                # The signed URL is persisted only in the chmod-0600 Scout cache DB and
                # is always redacted when returned or logged.
                if request["status"] not in {"downloaded", "validated", "staged"}:
                    connection.execute(
                        """UPDATE qogita_catalog_requests SET status='download_pending',
                           webhook_received_at=?,completed_at=?,download_url=?,updated_at=?
                           WHERE catalog_request_id=?""",
                        (received, received, event["download_url"], received, request_id),
                    )
            else:
                connection.execute(
                    """UPDATE qogita_catalog_requests SET status='failed',
                       webhook_received_at=?,failed_at=?,failure_reason=?,updated_at=?
                       WHERE catalog_request_id=?""",
                    (received, received, (event.get("failure_reason") or "Qogita export failed")[:500],
                     received, request_id),
                )
            connection.commit()
            return {"status": "accepted", "catalog_request_id": request_id,
                    "event_type": event["event_type"]}
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def claim_download(self, catalog_request_id: str) -> str:
        self.initialize()
        with _connect(self.path) as connection:
            row = connection.execute(
                "SELECT status,download_url FROM qogita_catalog_requests WHERE catalog_request_id=?",
                (catalog_request_id,),
            ).fetchone()
            if not row or row["status"] != "download_pending" or not row["download_url"]:
                raise CatalogDownloadError("Catalog request is not ready for download")
            connection.execute(
                """UPDATE qogita_catalog_requests SET status='downloading',
                   download_started_at=?,updated_at=? WHERE catalog_request_id=?""",
                (utc_now(), utc_now(), catalog_request_id),
            )
            return row["download_url"]

    def finish_download(self, catalog_request_id: str, result: dict[str, Any]):
        now = utc_now()
        with _connect(self.path) as connection:
            connection.execute(
                """UPDATE qogita_catalog_requests SET status='downloaded',download_url=NULL,
                   download_completed_at=?,local_file_path=?,file_size=?,checksum=?,updated_at=?
                   WHERE catalog_request_id=?""",
                (now, result["path"], result["file_size"], result["checksum"], now,
                 catalog_request_id),
            )

    def fail_download(self, catalog_request_id: str, reason: str):
        with _connect(self.path) as connection:
            connection.execute(
                """UPDATE qogita_catalog_requests SET status='download_failed',
                   failure_reason=?,updated_at=? WHERE catalog_request_id=?""",
                (str(reason)[:500], utc_now(), catalog_request_id),
            )

    def save_validation(self, catalog_request_id: str, summary: dict[str, Any]):
        status = "validated" if summary["status"] == "accepted" else summary["status"]
        with _connect(self.path) as connection:
            connection.execute(
                """UPDATE qogita_catalog_requests SET status=?,catalog_as_of=?,row_count=?,
                   unique_gtin_count=?,validation_json=?,full_coverage_verified=?,updated_at=?
                   WHERE catalog_request_id=?""",
                (status, summary.get("catalog_as_of"), summary.get("row_count"),
                 summary.get("unique_gtin_count"), json_dumps(summary),
                 int(summary.get("full_coverage_verified", False)), utc_now(),
                 catalog_request_id),
            )

    def mark_staged(self, catalog_request_id: str, run_id: str):
        with _connect(self.path) as connection:
            connection.execute(
                """UPDATE qogita_catalog_requests SET status='staged',staging_run_id=?,
                   updated_at=? WHERE catalog_request_id=?""",
                (run_id, utc_now(), catalog_request_id),
            )

    def populate_enrichment_queue(self, run_id: str):
        self.initialize()
        now = utc_now()
        with _connect(self.path) as connection:
            connection.execute(
                """INSERT OR IGNORE INTO qogita_enrichment_queue (
                    run_id,canonical_product_key,variant_fid,task_type,reason,
                    priority,status,source_observed_at,created_at
                )
                SELECT run_id,canonical_product_key,variant_fid,
                       CASE WHEN variant_fid IS NULL THEN 'resolve_variant'
                            ELSE 'offers_enrichment' END,
                       CASE WHEN COALESCE(catalog_delta_status,'new')='new' THEN 'new_product'
                            WHEN catalog_delta_status='changed' THEN 'catalog_changed'
                            WHEN enrichment_status='enrichment_failed'
                                THEN 'retryable_enrichment_failure'
                            WHEN enrichment_status='carried_forward' THEN 'old_carried_forward'
                            ELSE 'rotating_reconciliation' END,
                       CASE WHEN COALESCE(catalog_delta_status,'new')='new' THEN 100
                            WHEN catalog_delta_status='changed' THEN 90
                            WHEN enrichment_status='enrichment_failed' THEN 80
                            WHEN enrichment_status='carried_forward' THEN 60
                            ELSE 40 END,
                       'pending',offer_tier_observed_at,?
                FROM supplier_catalog_products WHERE run_id=?""",
                (now, run_id),
            )
            return connection.execute(
                "SELECT COUNT(*) FROM qogita_enrichment_queue WHERE run_id=?",
                (run_id,),
            ).fetchone()[0]

    def enrichment_queue(self, run_id: str, *, limit: int = 100):
        self.initialize()
        with _connect(self.path) as connection:
            return [dict(row) for row in connection.execute(
                """SELECT * FROM qogita_enrichment_queue WHERE run_id=? AND status='pending'
                   ORDER BY priority DESC,COALESCE(source_observed_at,''),created_at,
                            canonical_product_key LIMIT ?""",
                (run_id, int(limit)),
            )]


def receive_qogita_webhook(
    headers: dict[str, str], raw_body: bytes, *, signing_secret: str,
    store: QogitaCatalogPipelineStore, now_timestamp: int | None = None,
):
    signature = next(
        (value for key, value in headers.items() if key.casefold() == QOGITA_SIGNATURE_HEADER.casefold()),
        None,
    )
    timestamp = verify_qogita_signature(
        signature, raw_body, signing_secret, now_timestamp=now_timestamp,
    )
    event = parse_qogita_event(raw_body)
    if event["event_type"] == "webhook.test":
        return {
            "status": "accepted", "event_type": "webhook.test",
            "catalog_request_id": None,
        }
    return store.record_event(
        event, signature_timestamp=timestamp, raw_body=raw_body,
    )


class QogitaCatalogDownloader:
    def __init__(
        self, destination_directory: str | Path, *, allowed_hosts: set[str],
        client=None, timeout_seconds: float = 120,
        max_bytes: int = DEFAULT_MAX_DOWNLOAD_BYTES,
    ):
        self.destination_directory = Path(destination_directory).expanduser().resolve()
        self.allowed_hosts = set(allowed_hosts)
        self.timeout_seconds = timeout_seconds
        self.max_bytes = max_bytes
        self.client = client

    def download(self, url: str, *, catalog_request_id: str):
        safe_url = validate_download_url(url, allowed_hosts=self.allowed_hosts)
        self.destination_directory.mkdir(parents=True, exist_ok=True)
        os.chmod(self.destination_directory, 0o700)
        target = self.destination_directory / f"qogita-{catalog_request_id}.csv"
        handle = tempfile.NamedTemporaryFile(
            mode="wb", prefix=f".{target.name}.", suffix=".part",
            dir=self.destination_directory, delete=False,
        )
        temporary = Path(handle.name)
        os.chmod(temporary, 0o600)
        checksum = hashlib.sha256()
        size = 0
        owned_client = self.client is None
        client = self.client or requests.Session()
        try:
            with client.get(
                safe_url, timeout=self.timeout_seconds, stream=True, allow_redirects=False,
            ) as response:
                response.raise_for_status()
                content_length = response.headers.get("content-length")
                expected = int(content_length) if content_length and content_length.isdigit() else None
                for chunk in response.iter_content(1024 * 1024):
                    if not chunk:
                        continue
                    size += len(chunk)
                    if size > self.max_bytes:
                        raise CatalogDownloadError("Catalog download exceeded size limit")
                    handle.write(chunk)
                    checksum.update(chunk)
                handle.flush()
                os.fsync(handle.fileno())
                if expected is not None and size != expected:
                    raise CatalogDownloadError("Catalog download is truncated")
            handle.close()
            os.replace(temporary, target)
            os.chmod(target, 0o600)
            return {"path": str(target), "file_size": size, "checksum": checksum.hexdigest(),
                    "sanitized_url": sanitize_url(safe_url)}
        except Exception:
            handle.close()
            temporary.unlink(missing_ok=True)
            raise
        finally:
            if owned_client:
                client.close()


class QogitaCatalogRequestClient:
    """Small Scout-owned client for export requests and bounded offer probes."""

    def __init__(self, *, base_url: str, email: str, password: str,
                 client=None, timeout_seconds: float = 30):
        self.base_url = base_url.rstrip("/") + "/"
        self.email = email
        self.password = password
        self.timeout_seconds = timeout_seconds
        self.client = client or requests.Session()
        self._owned_client = client is None
        self.access_token = None

    def close(self):
        self.access_token = None
        if self._owned_client:
            self.client.close()

    def login(self):
        response = self.client.post(
            self.base_url + "auth/login/", json={"email": self.email, "password": self.password},
            timeout=self.timeout_seconds, allow_redirects=False,
        )
        response.raise_for_status()
        payload = response.json()
        def find_token(value):
            if isinstance(value, dict):
                for key, item in value.items():
                    if str(key).replace("_", "").casefold() in {
                        "access", "accesstoken", "token"
                    } and isinstance(item, str) and item:
                        return item
                for item in value.values():
                    found = find_token(item)
                    if found:
                        return found
            return None
        self.access_token = find_token(payload)
        if not self.access_token:
            raise QogitaPipelineError("Qogita login response has no access token")

    def _headers(self):
        if not self.access_token:
            self.login()
        return {"Authorization": f"Bearer {self.access_token}", "Accept": "application/json"}

    def request_full_catalog(self):
        response = self.client.post(
            self.base_url + "public/buyers/catalog-downloads/", json={}, headers=self._headers(),
            timeout=self.timeout_seconds, allow_redirects=False,
        )
        if response.status_code != 202:
            raise QogitaPipelineError(f"Qogita catalog request failed with HTTP {response.status_code}")
        payload = response.json()
        request_id = payload.get("catalogRequestId") or payload.get("catalog_request_id")
        if not request_id:
            raise QogitaPipelineError("Qogita catalog response has no catalogRequestId")
        return str(request_id)

    def rate_limit_probe(self, variant_fids: list[str], *, pacing_seconds: float = 0.5,
                         max_attempts: int = 3):
        if not 1 <= len(variant_fids) <= 50:
            raise ValueError("Rate-limit probe requires 1-50 variant FIDs")
        results = []
        for index, fid in enumerate(variant_fids):
            if index:
                time.sleep(max(0, pacing_seconds))
            attempts = 0
            while True:
                attempts += 1
                started = time.monotonic()
                response = self.client.get(
                    self.base_url + f"buyers/variants/{fid}/offers/", headers=self._headers(),
                    timeout=self.timeout_seconds, allow_redirects=False,
                )
                elapsed = time.monotonic() - started
                retryable = response.status_code == 429 or response.status_code >= 500
                if retryable and attempts < max_attempts:
                    time.sleep(min(2 ** (attempts - 1), 10))
                    continue
                payload = response.json() if response.headers.get("content-type", "").startswith(
                    "application/json"
                ) else {}
                offers = payload.get("offers") if isinstance(payload, dict) else []
                results.append({
                    "variant_fid": fid, "http_status": response.status_code,
                    "attempts": attempts, "elapsed_seconds": elapsed,
                    "rate_limit": response.headers.get("x-ratelimit-limit"),
                    "retry_after": response.headers.get("retry-after"),
                    "offer_count": len(offers or []),
                    "tier_count": sum(len(row.get("tieredPrices") or []) for row in offers or []),
                })
                break
        return results


@dataclass(frozen=True)
class CatalogValidationPolicy:
    minimum_rows: int = 1
    minimum_valid_gtin_ratio: float = 0.995
    maximum_row_drop_ratio: float = 0.30
    reject_duplicate_gtins: bool = True


def validate_qogita_catalog(
    path: str | Path, *, request_mode: str, filters: dict[str, Any],
    previous_row_count: int | None = None,
    policy: CatalogValidationPolicy = CatalogValidationPolicy(),
):
    reader = QogitaCatalogExportReader(path)
    metadata = reader.metadata()
    row_count = valid_count = invalid_count = duplicate_count = 0
    first_errors = []
    unique_db = tempfile.NamedTemporaryFile(suffix=".sqlite3", delete=False)
    unique_path = Path(unique_db.name)
    unique_db.close()
    connection = sqlite3.connect(unique_path)
    try:
        connection.execute("CREATE TABLE identifiers (gtin TEXT PRIMARY KEY)")
        try:
            for row_number, raw in reader.rows():
                row_count += 1
                try:
                    product = reader.product_from_row(raw, row_number=row_number)
                    connection.execute(
                        "INSERT INTO identifiers (gtin) VALUES (?)",
                        (product["canonical_gtin"],),
                    )
                    valid_count += 1
                except sqlite3.IntegrityError:
                    duplicate_count += 1
                except (TypeError, ValueError) as exc:
                    invalid_count += 1
                    if len(first_errors) < 20:
                        first_errors.append({"row": row_number, "error": str(exc)[:200]})
        except (UnicodeDecodeError, csv.Error, ValueError) as exc:
            raise CatalogValidationError("Catalog CSV is malformed") from exc
        unique_count = connection.execute("SELECT COUNT(*) FROM identifiers").fetchone()[0]
    finally:
        connection.close()
        unique_path.unlink(missing_ok=True)

    ratio = valid_count / row_count if row_count else 0
    reasons = []
    hard_failure = False
    if row_count < policy.minimum_rows:
        reasons.append("empty_or_too_small")
        hard_failure = True
    if ratio < policy.minimum_valid_gtin_ratio:
        reasons.append("valid_gtin_ratio_too_low")
        hard_failure = True
    if policy.reject_duplicate_gtins and duplicate_count:
        reasons.append("duplicate_gtins")
        hard_failure = True
    anomalous_drop = bool(
        previous_row_count and row_count < previous_row_count * (1 - policy.maximum_row_drop_ratio)
    )
    if anomalous_drop:
        reasons.append("anomalous_row_drop")
    original_name = str(metadata.get("Original file") or metadata.get("source_file_name") or "")
    metadata_filtered = "filtered" in original_name.casefold() or str(
        metadata.get("Request mode") or ""
    ).casefold() == "filtered"
    full_verified = (
        request_mode == "full" and dict(filters or {}) == {} and not metadata_filtered
        and not hard_failure and not anomalous_drop and invalid_count == 0
        and duplicate_count == 0
    )
    status = "rejected" if hard_failure else "quarantined" if anomalous_drop else "accepted"
    catalog_as_of = str(metadata.get("Catalog as of") or metadata.get("Catalog As Of") or "")
    catalog_as_of = catalog_as_of.replace("Catalog As Of ", "") or None
    return {
        "status": status, "reasons": reasons, "row_count": row_count,
        "valid_gtin_count": valid_count, "invalid_gtin_count": invalid_count,
        "unique_gtin_count": unique_count, "duplicate_gtin_count": duplicate_count,
        "valid_gtin_ratio": ratio, "catalog_as_of": catalog_as_of,
        "request_mode": request_mode, "filters": dict(filters or {}),
        "full_coverage_verified": full_verified,
        "product_catalog_coverage_type": (
            "full_account_catalog" if full_verified else "filtered_catalog"
            if request_mode == "filtered" or metadata_filtered else "partial_catalog"
        ),
        "scenario_enrichment_status": "none", "first_errors": first_errors,
        "metadata": metadata,
    }


def download_pending_catalog(
    catalog_request_id: str, *, store: QogitaCatalogPipelineStore,
    downloader: QogitaCatalogDownloader,
):
    url = store.claim_download(catalog_request_id)
    try:
        result = downloader.download(url, catalog_request_id=catalog_request_id)
        store.finish_download(catalog_request_id, result)
        LOGGER.info("Qogita catalog downloaded | request_id=%s url=%s bytes=%s",
                    catalog_request_id, result["sanitized_url"], result["file_size"])
        return result
    except Exception as exc:
        store.fail_download(catalog_request_id, str(exc))
        LOGGER.error("Qogita catalog download failed | request_id=%s url=%s",
                     catalog_request_id, sanitize_url(url))
        raise


def catalog_file_integrity(path: str | Path):
    file_path = Path(path).expanduser().resolve()
    checksum = hashlib.sha256()
    size = 0
    with file_path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            size += len(chunk)
            checksum.update(chunk)
    return {"file_size": size, "checksum": checksum.hexdigest()}


def prepare_staging_generation(
    catalog_request_id: str, *, pipeline_store: QogitaCatalogPipelineStore,
    catalog_store: SupplierCatalogStore, allow_quarantine: bool = False,
    previous_run_id: str | None = None,
    reuse_unchanged_scenarios_after: str | None = None,
):
    started = time.monotonic()
    request = pipeline_store.request(catalog_request_id)
    if not request or not request.get("local_file_path"):
        raise CatalogValidationError("Catalog request has no downloaded file")
    integrity = catalog_file_integrity(request["local_file_path"])
    if integrity["file_size"] != request.get("file_size") or integrity["checksum"] != request.get("checksum"):
        raise CatalogValidationError("Downloaded catalog checksum or size changed before staging")
    previous = (
        catalog_store.run_status(previous_run_id)
        if previous_run_id else catalog_store.active_generation_metadata("qogita")
    )
    if previous_run_id and (
        not previous or previous.get("supplier") != "qogita"
        or previous.get("status") not in {"success", "sample_success"}
    ):
        raise CatalogValidationError("Explicit Qogita comparison generation is not complete")
    summary = validate_qogita_catalog(
        request["local_file_path"], request_mode=request["request_mode"],
        filters=request["filters"], previous_row_count=(previous or {}).get("product_count"),
    )
    pipeline_store.save_validation(catalog_request_id, summary)
    if summary["status"] == "rejected" or (
        summary["status"] == "quarantined" and not allow_quarantine
    ):
        return {"status": summary["status"], "validation": summary, "staging_run_id": None}
    run_id = catalog_store.start_run(
        "qogita", coverage_type=summary["product_catalog_coverage_type"],
        coverage_description="Qogita async buyer catalog staged for manual review",
        coverage_complete=summary["full_coverage_verified"], sampled=False,
    )
    reader = QogitaCatalogExportReader(request["local_file_path"])
    try:
        staged = catalog_store.publish_product_catalog_stream(
            run_id, supplier="qogita", products=reader.products(skip_invalid=True),
            elapsed_seconds=time.monotonic() - started,
            product_catalog_coverage_type=summary["product_catalog_coverage_type"],
            product_catalog_coverage_complete=summary["full_coverage_verified"],
            scenario_enrichment_status="none",
            source_type="official_qogita_async_catalog_export",
            source_count=summary["row_count"], export_generated_at=summary["catalog_as_of"],
            upstream_catalog_version=request["checksum"],
            diagnostics={"validation": summary, "catalog_request_id": catalog_request_id},
            promote=False, previous_run_id=previous_run_id,
            reuse_unchanged_scenarios_after=reuse_unchanged_scenarios_after,
        )
    except Exception as exc:
        catalog_store.fail(run_id, error_code="staging_failed", error_message=str(exc),
                           elapsed_seconds=time.monotonic() - started)
        raise
    pipeline_store.mark_staged(catalog_request_id, run_id)
    queue_count = pipeline_store.populate_enrichment_queue(run_id)
    return {"status": "staged", "staging_run_id": run_id, "validation": summary,
            "delta": staged["generation_delta"], "queue_count": queue_count,
            "promoted": False}
