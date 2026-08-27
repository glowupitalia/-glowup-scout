"""Persistent, resumable Qogita variant and offer enrichment for staged catalogs."""

from __future__ import annotations

import json
import os
import re
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Callable
from urllib.parse import quote, urlsplit
from uuid import uuid4

import requests

from purchase_scenarios import PurchaseScenario, product_key, scenario_key
from supplier_catalog import DEFAULT_DATABASE_PATH, json_dumps, utc_now


PRODUCT_LINK_SOURCE = "qogita_product_link_redirect"
PRODUCT_LINK_HOST = "api.qogita.com"
PRODUCT_PAGE_HOST = "www.qogita.com"
FID_PATH = re.compile(r"^/products/(?P<fid>[A-Za-z0-9]+)/(?P<slug>[^/]+)/$")
VAT_RATE = Decimal("0.22")
CENT = Decimal("0.01")
FINAL_PRODUCT_STATES = {
    "enriched", "resolver_permanent", "offers_permanent", "parsing_failure",
}
RETRYABLE_PRODUCT_STATES = {"pending", "fid_resolved", "resolver_retryable", "offers_retryable"}


class QogitaBootstrapError(RuntimeError):
    def __init__(self, message: str, *, code: str, retryable: bool = False,
                 http_status: int | None = None, retry_after: str | None = None):
        super().__init__(message)
        self.code = code
        self.retryable = retryable
        self.http_status = http_status
        self.retry_after = retry_after


class QogitaFidConflict(QogitaBootstrapError):
    def __init__(self):
        super().__init__(
            "Qogita Product Link resolved to a different persisted FID",
            code="variant_fid_conflict", retryable=False,
        )


BOOTSTRAP_SCHEMA = """
CREATE TABLE IF NOT EXISTS qogita_bootstrap_runs (
    bootstrap_run_id TEXT PRIMARY KEY,
    staging_run_id TEXT NOT NULL,
    started_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    status TEXT NOT NULL,
    target_count INTEGER NOT NULL,
    batch_size INTEGER NOT NULL,
    sample_strategy TEXT NOT NULL,
    completed_batches INTEGER NOT NULL DEFAULT 0,
    products_attempted INTEGER NOT NULL DEFAULT 0,
    fid_resolved INTEGER NOT NULL DEFAULT 0,
    fid_failed INTEGER NOT NULL DEFAULT 0,
    offers_success INTEGER NOT NULL DEFAULT 0,
    offers_failed INTEGER NOT NULL DEFAULT 0,
    scenarios_written INTEGER NOT NULL DEFAULT 0,
    product_link_requests INTEGER NOT NULL DEFAULT 0,
    offers_requests INTEGER NOT NULL DEFAULT 0,
    retry_count INTEGER NOT NULL DEFAULT 0,
    rate_limit_count INTEGER NOT NULL DEFAULT 0,
    server_error_count INTEGER NOT NULL DEFAULT 0,
    resolver_elapsed_seconds REAL NOT NULL DEFAULT 0,
    offers_elapsed_seconds REAL NOT NULL DEFAULT 0,
    worker_count INTEGER NOT NULL DEFAULT 1,
    http_401_count INTEGER NOT NULL DEFAULT 0,
    auth_refresh_count INTEGER NOT NULL DEFAULT 0,
    sqlite_busy_count INTEGER NOT NULL DEFAULT 0,
    transaction_retry_count INTEGER NOT NULL DEFAULT 0,
    lock_wait_seconds REAL NOT NULL DEFAULT 0,
    write_latency_seconds REAL NOT NULL DEFAULT 0,
    wal_peak_bytes INTEGER NOT NULL DEFAULT 0,
    wall_elapsed_seconds REAL NOT NULL DEFAULT 0,
    excluded_bootstrap_runs_json TEXT NOT NULL DEFAULT '[]',
    run_mode TEXT NOT NULL DEFAULT 'pilot',
    source_product_count INTEGER,
    reusable_products INTEGER NOT NULL DEFAULT 0,
    initial_pending_products INTEGER NOT NULL DEFAULT 0,
    product_link_pacing REAL,
    offers_pacing REAL,
    stop_reason TEXT,
    health_json TEXT NOT NULL DEFAULT '{}',
    last_progress_json TEXT NOT NULL DEFAULT '{}',
    FOREIGN KEY (staging_run_id) REFERENCES supplier_catalog_runs(run_id)
);

CREATE TABLE IF NOT EXISTS qogita_bootstrap_products (
    bootstrap_run_id TEXT NOT NULL,
    staging_run_id TEXT NOT NULL,
    sequence_no INTEGER NOT NULL,
    canonical_product_key TEXT NOT NULL,
    gtin TEXT NOT NULL,
    csv_number_of_offers INTEGER,
    status TEXT NOT NULL DEFAULT 'pending',
    variant_fid TEXT,
    resolver_attempts INTEGER NOT NULL DEFAULT 0,
    offers_attempts INTEGER NOT NULL DEFAULT 0,
    endpoint_offer_count INTEGER,
    seller_count INTEGER,
    tier_count INTEGER,
    scenario_count INTEGER NOT NULL DEFAULT 0,
    resolver_elapsed_seconds REAL NOT NULL DEFAULT 0,
    offers_elapsed_seconds REAL NOT NULL DEFAULT 0,
    error_code TEXT,
    error_message TEXT,
    claimed_at TEXT,
    lease_expires_at TEXT,
    worker_id TEXT,
    claim_count INTEGER NOT NULL DEFAULT 0,
    completed_at TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (bootstrap_run_id, canonical_product_key),
    UNIQUE (bootstrap_run_id, sequence_no),
    FOREIGN KEY (bootstrap_run_id) REFERENCES qogita_bootstrap_runs(bootstrap_run_id),
    FOREIGN KEY (staging_run_id, canonical_product_key)
        REFERENCES supplier_catalog_products(run_id, canonical_product_key)
);
CREATE INDEX IF NOT EXISTS idx_qogita_bootstrap_products_next
ON qogita_bootstrap_products (bootstrap_run_id, status, sequence_no);

CREATE TABLE IF NOT EXISTS qogita_bootstrap_milestones (
    bootstrap_run_id TEXT NOT NULL,
    milestone INTEGER NOT NULL,
    reached_at TEXT NOT NULL,
    metrics_json TEXT NOT NULL,
    PRIMARY KEY (bootstrap_run_id, milestone),
    FOREIGN KEY (bootstrap_run_id) REFERENCES qogita_bootstrap_runs(bootstrap_run_id)
);
"""


def _connect(path: str | Path) -> sqlite3.Connection:
    absolute = Path(path).expanduser().resolve()
    absolute.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(absolute, timeout=60)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA busy_timeout=60000")
    return connection


def _as_datetime(value: str) -> datetime:
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)


def parse_product_link_redirect(gtin: str, status_code: int, location: str | None) -> dict[str, str]:
    """Validate the exact public Qogita redirect contract without following it."""
    if status_code != 302:
        retryable = status_code == 429 or status_code >= 500
        raise QogitaBootstrapError(
            f"Unexpected Qogita Product Link HTTP {status_code}",
            code="resolver_unexpected_http", retryable=retryable,
            http_status=status_code,
        )
    if not location:
        raise QogitaBootstrapError(
            "Qogita Product Link response has no Location",
            code="resolver_missing_location",
        )
    parsed = urlsplit(str(location))
    if parsed.scheme != "https" or parsed.hostname != PRODUCT_PAGE_HOST:
        raise QogitaBootstrapError(
            "Qogita Product Link Location has an untrusted origin",
            code="resolver_bad_location_origin",
        )
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise QogitaBootstrapError(
            "Qogita Product Link Location contains unsupported components",
            code="resolver_bad_location",
        )
    match = FID_PATH.fullmatch(parsed.path)
    if not match:
        raise QogitaBootstrapError(
            "Qogita Product Link Location path is malformed",
            code="resolver_bad_location_path",
        )
    fid = match.group("fid")
    if not fid:
        raise QogitaBootstrapError("Qogita FID is missing", code="resolver_missing_fid")
    return {
        "gtin": str(gtin), "variant_fid": fid,
        "variant_fid_source": PRODUCT_LINK_SOURCE,
        "location_host": PRODUCT_PAGE_HOST,
    }


def _positive_decimal(value: Any) -> Decimal | None:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() and parsed > 0 else None


def _positive_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _seller_alias(value: Any) -> str:
    if isinstance(value, dict):
        value = value.get("alias") or value.get("fid") or value.get("qid") or value.get("name")
    return str(value or "").strip()


def qogita_scenarios_from_offers(
    product: dict[str, Any], variant_fid: str, payload: Any, *,
    staging_run_id: str, observed_at: str,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Map every valid seller/offer/MOV tier to one stable PurchaseScenario."""
    if not isinstance(payload, dict) or not isinstance(payload.get("offers"), list):
        raise QogitaBootstrapError(
            "Qogita offers payload has no offers array", code="offers_parsing_failure",
        )
    gtin = str(product.get("gtin") or "").strip()
    brand = str(product.get("brand") or "").strip()
    title = str(product.get("title") or "").strip()
    product_url = str(product.get("product_url") or "").strip()
    scenarios = []
    seller_ids = set()
    offer_ids = set()
    raw_tiers = 0
    for offer in payload["offers"]:
        if not isinstance(offer, dict):
            continue
        seller = _seller_alias(offer.get("seller"))
        offer_qid = str(offer.get("qid") or offer.get("offerQid") or "").strip()
        selling_unit = _positive_int(offer.get("unit"))
        stock = _positive_int(offer.get("inventory"))
        tiers = offer.get("tieredPrices")
        if not isinstance(tiers, list):
            continue
        raw_tiers += len(tiers)
        if not seller or not offer_qid or selling_unit is None or stock is None:
            continue
        if stock < selling_unit:
            continue
        seller_ids.add(seller)
        offer_ids.add(offer_qid)
        delivery = offer.get("estimatedDeliveryTime")
        if delivery is None:
            delivery = offer.get("maxExpectedDeliveryTime")
        delivery_value = int(delivery) if str(delivery or "").isdigit() else None
        for tier in tiers:
            if not isinstance(tier, dict):
                continue
            price = tier.get("tierPrice")
            mov = tier.get("tierMov")
            if not isinstance(price, dict) or not isinstance(mov, dict):
                continue
            net = _positive_decimal(price.get("amount"))
            account_mov = _positive_decimal(mov.get("amount"))
            currency = str(price.get("currency") or "").strip().upper()
            mov_currency = str(mov.get("currency") or "").strip().upper()
            if net is None or account_mov is None or currency != "EUR":
                continue
            if mov_currency and mov_currency != currency:
                continue
            gross = (net * (Decimal("1") + VAT_RATE)).quantize(CENT, rounding=ROUND_HALF_UP)
            vat = (gross - net).quantize(CENT, rounding=ROUND_HALF_UP)
            identity = scenario_key(
                supplier="qogita", supplier_alias=seller,
                supplier_product_id=variant_fid, supplier_offer_id=offer_qid,
                variant_id=variant_fid, canonical_ean=gtin,
                scenario_type="qogita_mov", account_mov=account_mov,
            )
            scenario = PurchaseScenario(
                scenario_id=identity, product_key=product_key(gtin), canonical_ean=gtin,
                identifier_type="EAN" if len(gtin) == 13 else "UPC" if len(gtin) == 12 else "GTIN",
                supplier="qogita", supplier_alias=seller,
                supplier_product_id=variant_fid, supplier_offer_id=offer_qid,
                variant_id=variant_fid, brand=brand, title=title,
                scenario_type="qogita_mov", scenario_label=f"MOV EUR {account_mov}",
                scenario_order=0, account_mov=account_mov,
                account_mov_currency="EUR", account_mov_eur=account_mov,
                selling_unit=selling_unit, cost_net_unit_eur=net,
                vat_rate=VAT_RATE, vat_amount_unit=vat,
                cost_gross_unit_eur=gross, stock=stock,
                snapshot_id=staging_run_id, snapshot_at=observed_at,
                freshness_status="fresh", tier_is_active=True,
                lead_time=str(delivery_value) if delivery_value is not None else None,
                availability_status="available", product_url=product_url or None,
                source_metadata={
                    "variant_fid_source": PRODUCT_LINK_SOURCE,
                    "offer_tier_observed_at": observed_at,
                    "delivery_time": delivery_value,
                    "is_traceable": offer.get("isTraceable"),
                    "is_top_seller": offer.get("isTopSeller"),
                    "down_payment_percentage": offer.get("downPaymentPercentage"),
                },
            ).to_dict()
            scenarios.append({
                "scenario_id": identity, "canonical_product_key": product["canonical_product_key"],
                "canonical_ean": gtin, "raw_identifier": gtin,
                "raw_identifier_type": "GTIN", "supplier_product_id": variant_fid,
                "supplier_offer_id": offer_qid, "scenario_type": "qogita_mov",
                "scenario_label": scenario["scenario_label"], "price": str(net),
                "currency": currency, "stock": stock, "minimum_quantity": None,
                "maximum_quantity": None, "selling_unit": selling_unit,
                "account_mov": str(account_mov), "account_mov_currency": "EUR",
                "warehouse": None, "shipping_mode": None,
                "availability_status": "available",
                "lead_time": scenario["lead_time"], "payload": scenario,
            })
    ids = [row["scenario_id"] for row in scenarios]
    if len(ids) != len(set(ids)):
        raise QogitaBootstrapError(
            "Qogita offers contain duplicate commercial tier identities",
            code="offers_duplicate_scenario_identity",
        )
    scenarios.sort(key=lambda row: (
        str(row["payload"].get("supplier_alias") or "").casefold(),
        str(row["supplier_offer_id"]), Decimal(str(row["account_mov"])),
        row["scenario_id"],
    ))
    for order, row in enumerate(scenarios, start=1):
        row["payload"]["scenario_order"] = order
    return scenarios, {
        "offer_count": len(payload["offers"]), "seller_count": len(seller_ids),
        "offer_qid_count": len(offer_ids), "raw_tier_count": raw_tiers,
        "scenario_count": len(scenarios),
    }


class SharedRateLimiter:
    """Global monotonic pacing shared by all workers."""

    def __init__(self, *, product_link_interval: float = 0.6,
                 offers_interval: float = 1.0, sleep_func=time.sleep):
        self.intervals = {
            "product_link": max(0.0, float(product_link_interval)),
            "offers": max(0.0, float(offers_interval)),
        }
        self.sleep_func = sleep_func
        self._locks = {name: threading.Lock() for name in self.intervals}
        self._last_at = {name: None for name in self.intervals}
        self.wait_seconds = {name: 0.0 for name in self.intervals}

    def wait(self, channel: str):
        with self._locks[channel]:
            last = self._last_at[channel]
            delay = 0.0 if last is None else max(
                0.0, self.intervals[channel] - (time.monotonic() - last)
            )
            if delay:
                self.sleep_func(delay)
                self.wait_seconds[channel] += delay
            self._last_at[channel] = time.monotonic()

    def slow_down(self, channel: str, *, retry_after: str | None = None):
        """Adapt a shared channel after pressure; never makes it faster."""
        with self._locks[channel]:
            try:
                requested = float(retry_after) if retry_after else 0.0
            except (TypeError, ValueError):
                requested = 0.0
            current = self.intervals[channel]
            self.intervals[channel] = max(current, requested, current * 1.25)
            return self.intervals[channel]


class SharedQogitaAuth:
    """Single-flight buyer token acquisition/refresh for concurrent workers."""

    def __init__(self, *, base_url: str, email: str, password: str,
                 timeout_seconds: float = 30):
        self.base_url = base_url.rstrip("/") + "/"
        self.email = email
        self.password = password
        self.timeout_seconds = timeout_seconds
        self._token = None
        self._lock = threading.Lock()
        self.login_requests = 0
        self.refreshes = 0

    @staticmethod
    def _extract_token(payload: Any) -> str | None:
        stack = [payload]
        while stack:
            value = stack.pop()
            if isinstance(value, dict):
                for key, item in value.items():
                    if (str(key).replace("_", "").casefold() in
                            {"access", "accesstoken", "token"} and isinstance(item, str)):
                        return item
                    stack.append(item)
        return None

    def token(self, session, *, rejected_token: str | None = None):
        with self._lock:
            if self._token and (rejected_token is None or rejected_token != self._token):
                return self._token, False
            was_refresh = self._token is not None or rejected_token is not None
            response = session.post(
                self.base_url + "auth/login/",
                json={"email": self.email, "password": self.password},
                timeout=self.timeout_seconds, allow_redirects=False,
            )
            self.login_requests += 1
            if response.status_code != 200:
                raise QogitaBootstrapError(
                    f"Qogita login failed with HTTP {response.status_code}",
                    code="authentication_failed", retryable=response.status_code >= 500,
                    http_status=response.status_code,
                )
            token = self._extract_token(response.json())
            if not token:
                raise QogitaBootstrapError(
                    "Qogita login response has no access token", code="authentication_schema",
                )
            self._token = token
            if was_refresh:
                self.refreshes += 1
            return token, True


class QogitaBootstrapClient:
    """Sequential read-only client with strict redirect and response validation."""

    def __init__(self, *, base_url: str, email: str, password: str,
                 session=None, timeout_seconds: float = 30,
                 auth_manager: SharedQogitaAuth | None = None,
                 rate_limiter: SharedRateLimiter | None = None):
        self.base_url = base_url.rstrip("/") + "/"
        self.email = email
        self.password = password
        self.session = session or requests.Session()
        self.owns_session = session is None
        self.timeout_seconds = timeout_seconds
        self.access_token = None
        self.auth_manager = auth_manager
        self.rate_limiter = rate_limiter
        self.metrics = {
            "login_requests": 0, "product_link_requests": 0, "offers_requests": 0,
            "retries": 0, "http_401": 0, "auth_refreshes": 0,
            "http_429": 0, "http_5xx": 0,
            "resolver_elapsed_seconds": 0.0, "offers_elapsed_seconds": 0.0,
        }

    def close(self):
        self.access_token = None
        if self.owns_session:
            self.session.close()

    def _login(self, *, rejected_token: str | None = None):
        if self.auth_manager:
            token, did_login = self.auth_manager.token(
                self.session, rejected_token=rejected_token,
            )
            self.access_token = token
            if did_login:
                self.metrics["login_requests"] += 1
                if rejected_token is not None:
                    self.metrics["auth_refreshes"] += 1
            return
        if self.access_token:
            return
        self.metrics["login_requests"] += 1
        response = self.session.post(
            self.base_url + "auth/login/",
            json={"email": self.email, "password": self.password},
            timeout=self.timeout_seconds, allow_redirects=False,
        )
        if response.status_code != 200:
            raise QogitaBootstrapError(
                f"Qogita login failed with HTTP {response.status_code}",
                code="authentication_failed", retryable=response.status_code >= 500,
                http_status=response.status_code,
            )
        payload = response.json()
        stack = [payload]
        while stack and not self.access_token:
            value = stack.pop()
            if isinstance(value, dict):
                for key, item in value.items():
                    if str(key).replace("_", "").casefold() in {"access", "accesstoken", "token"} and isinstance(item, str):
                        self.access_token = item
                        break
                    stack.append(item)
        if not self.access_token:
            raise QogitaBootstrapError(
                "Qogita login response has no access token", code="authentication_schema",
            )

    def resolve_fid(self, gtin: str, product_url: str | None = None):
        expected = f"https://{PRODUCT_LINK_HOST}/variants/link/{quote(str(gtin), safe='')}/"
        if product_url:
            parsed = urlsplit(str(product_url))
            if parsed.scheme != "https" or parsed.hostname != PRODUCT_LINK_HOST or parsed.path != urlsplit(expected).path:
                raise QogitaBootstrapError(
                    "Stored Qogita Product Link does not match its GTIN",
                    code="resolver_product_link_mismatch",
                )
        started = time.monotonic()
        try:
            if self.rate_limiter:
                self.rate_limiter.wait("product_link")
            self.metrics["product_link_requests"] += 1
            response = self.session.get(
                expected, timeout=self.timeout_seconds, allow_redirects=False,
            )
        except (requests.Timeout, requests.ConnectionError) as exc:
            self.metrics["resolver_elapsed_seconds"] += time.monotonic() - started
            raise QogitaBootstrapError(
                "Qogita Product Link network failure", code="resolver_network",
                retryable=True,
            ) from exc
        elapsed = time.monotonic() - started
        self.metrics["resolver_elapsed_seconds"] += elapsed
        if response.status_code == 429:
            self.metrics["http_429"] += 1
            if self.rate_limiter:
                self.rate_limiter.slow_down(
                    "product_link", retry_after=response.headers.get("Retry-After"),
                )
        if response.status_code >= 500:
            self.metrics["http_5xx"] += 1
        try:
            result = parse_product_link_redirect(
                gtin, response.status_code, response.headers.get("Location"),
            )
        except QogitaBootstrapError as exc:
            if response.status_code == 429:
                exc.retry_after = response.headers.get("Retry-After")
            raise
        return {**result, "elapsed_seconds": elapsed,
                "retry_after": response.headers.get("Retry-After")}

    def _offers_request(self, variant_fid: str):
        self._login()
        token_used = self.access_token
        started = time.monotonic()
        try:
            if self.rate_limiter:
                self.rate_limiter.wait("offers")
            self.metrics["offers_requests"] += 1
            response = self.session.get(
                self.base_url + f"buyers/variants/{quote(variant_fid, safe='')}/offers/",
                headers={"Authorization": f"Bearer {self.access_token}", "Accept": "application/json"},
                timeout=self.timeout_seconds, allow_redirects=False,
            )
        except (requests.Timeout, requests.ConnectionError) as exc:
            self.metrics["offers_elapsed_seconds"] += time.monotonic() - started
            raise QogitaBootstrapError(
                "Qogita offers network failure", code="offers_network", retryable=True,
            ) from exc
        elapsed = time.monotonic() - started
        self.metrics["offers_elapsed_seconds"] += elapsed
        return response, elapsed, token_used

    def fetch_offers(self, variant_fid: str):
        response, elapsed, token_used = self._offers_request(variant_fid)
        if response.status_code == 401:
            # Buyer access tokens can expire during a long sequential bootstrap.
            # Refresh exactly once and retry only this read-only offers request.
            self.metrics["http_401"] += 1
            if self.auth_manager:
                self._login(rejected_token=token_used)
            else:
                self.access_token = None
            self.metrics["retries"] += 1
            response, retry_elapsed, _ = self._offers_request(variant_fid)
            elapsed += retry_elapsed
        if response.status_code == 429:
            self.metrics["http_429"] += 1
            if self.rate_limiter:
                self.rate_limiter.slow_down(
                    "offers", retry_after=response.headers.get("Retry-After"),
                )
        if response.status_code >= 500:
            self.metrics["http_5xx"] += 1
        if response.status_code != 200:
            raise QogitaBootstrapError(
                f"Qogita offers failed with HTTP {response.status_code}",
                code=("offers_authentication_failed" if response.status_code == 401 else "offers_http"),
                retryable=response.status_code == 429 or response.status_code >= 500,
                http_status=response.status_code, retry_after=response.headers.get("Retry-After"),
            )
        try:
            payload = response.json()
        except ValueError as exc:
            raise QogitaBootstrapError(
                "Qogita offers response is not JSON", code="offers_parsing_failure",
            ) from exc
        if not isinstance(payload, dict) or not isinstance(payload.get("offers"), list):
            raise QogitaBootstrapError(
                "Qogita offers response has no offers array", code="offers_parsing_failure",
            )
        return {"payload": payload, "elapsed_seconds": elapsed,
                "retry_after": response.headers.get("Retry-After")}


class QogitaBootstrapStore:
    def __init__(self, path: str | Path = DEFAULT_DATABASE_PATH):
        self.path = Path(path).expanduser().resolve()
        self._metrics_lock = threading.Lock()
        self.sqlite_metrics = {
            "sqlite_busy": 0, "transaction_retries": 0,
            "lock_wait_seconds": 0.0, "write_latency_seconds": 0.0,
        }

    def _begin_immediate(self, connection, *, attempts: int = 5):
        started = time.monotonic()
        for attempt in range(1, attempts + 1):
            try:
                connection.execute("BEGIN IMMEDIATE")
                with self._metrics_lock:
                    self.sqlite_metrics["lock_wait_seconds"] += time.monotonic() - started
                return
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc).casefold() or attempt >= attempts:
                    raise
                with self._metrics_lock:
                    self.sqlite_metrics["sqlite_busy"] += 1
                    self.sqlite_metrics["transaction_retries"] += 1
                time.sleep(min(0.05 * (2 ** (attempt - 1)), 0.5))

    def _record_write(self, started: float):
        with self._metrics_lock:
            self.sqlite_metrics["write_latency_seconds"] += time.monotonic() - started

    def initialize(self):
        from supplier_catalog import SupplierCatalogStore
        from qogita_catalog_pipeline import QogitaCatalogPipelineStore

        SupplierCatalogStore(self.path).initialize()
        QogitaCatalogPipelineStore(self.path).initialize()
        with _connect(self.path) as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.executescript(BOOTSTRAP_SCHEMA)
            columns = {row[1] for row in connection.execute("PRAGMA table_info(supplier_catalog_products)")}
            for name, declaration in {
                "variant_fid_source": "TEXT",
                "enrichment_error_code": "TEXT",
                "enrichment_error_message": "TEXT",
            }.items():
                if name not in columns:
                    connection.execute(f"ALTER TABLE supplier_catalog_products ADD COLUMN {name} {declaration}")
            run_columns = {
                row[1] for row in connection.execute("PRAGMA table_info(qogita_bootstrap_runs)")
            }
            for name, declaration in {
                "worker_count": "INTEGER NOT NULL DEFAULT 1",
                "http_401_count": "INTEGER NOT NULL DEFAULT 0",
                "auth_refresh_count": "INTEGER NOT NULL DEFAULT 0",
                "sqlite_busy_count": "INTEGER NOT NULL DEFAULT 0",
                "transaction_retry_count": "INTEGER NOT NULL DEFAULT 0",
                "lock_wait_seconds": "REAL NOT NULL DEFAULT 0",
                "write_latency_seconds": "REAL NOT NULL DEFAULT 0",
                "wal_peak_bytes": "INTEGER NOT NULL DEFAULT 0",
                "wall_elapsed_seconds": "REAL NOT NULL DEFAULT 0",
                "excluded_bootstrap_runs_json": "TEXT NOT NULL DEFAULT '[]'",
                "run_mode": "TEXT NOT NULL DEFAULT 'pilot'",
                "source_product_count": "INTEGER",
                "reusable_products": "INTEGER NOT NULL DEFAULT 0",
                "initial_pending_products": "INTEGER NOT NULL DEFAULT 0",
                "product_link_pacing": "REAL",
                "offers_pacing": "REAL",
                "stop_reason": "TEXT",
                "health_json": "TEXT NOT NULL DEFAULT '{}'",
            }.items():
                if name not in run_columns:
                    connection.execute(f"ALTER TABLE qogita_bootstrap_runs ADD COLUMN {name} {declaration}")
            selected_columns = {
                row[1] for row in connection.execute("PRAGMA table_info(qogita_bootstrap_products)")
            }
            for name, declaration in {
                "claimed_at": "TEXT", "lease_expires_at": "TEXT", "worker_id": "TEXT",
                "claim_count": "INTEGER NOT NULL DEFAULT 0", "completed_at": "TEXT",
            }.items():
                if name not in selected_columns:
                    connection.execute(f"ALTER TABLE qogita_bootstrap_products ADD COLUMN {name} {declaration}")

    def create_production_bootstrap(
        self, staging_run_id: str, *, batch_size: int = 100,
        workers: int = 2, product_link_pacing: float = 0.6,
        offers_pacing: float = 1.15, bootstrap_run_id: str | None = None,
    ):
        """Create the one full, resumable queue for an already validated source.

        Carried-forward enrichment is terminal in this queue and is never fetched
        merely because a production bootstrap was started. Everything else is
        ordered by catalog delta and reusable FID state.
        """
        if batch_size <= 0 or workers != 2:
            raise ValueError("Production bootstrap requires batch_size > 0 and workers=2")
        if float(offers_pacing) < 1.15:
            raise ValueError("Production Qogita offers pacing cannot be below 1.15 seconds")
        self.initialize()
        bootstrap_run_id = bootstrap_run_id or uuid4().hex
        now = utc_now()
        with _connect(self.path) as connection:
            source = connection.execute(
                """SELECT supplier,status,product_count,product_catalog_coverage_type,
                          product_catalog_coverage_complete
                     FROM supplier_catalog_runs WHERE run_id=?""",
                (staging_run_id,),
            ).fetchone()
            if not source or source["supplier"] != "qogita":
                raise ValueError("Qogita source generation not found")
            if (source["status"] != "success"
                    or source["product_catalog_coverage_type"] != "full_account_catalog"
                    or not int(source["product_catalog_coverage_complete"] or 0)):
                raise ValueError("Qogita source is not a validated full account catalog")
            existing = connection.execute(
                "SELECT * FROM qogita_bootstrap_runs WHERE bootstrap_run_id=?",
                (bootstrap_run_id,),
            ).fetchone()
            if existing:
                if existing["staging_run_id"] != staging_run_id:
                    raise ValueError("Bootstrap ID belongs to another source generation")
                return self.bootstrap(bootstrap_run_id)
            reusable = connection.execute(
                """SELECT COUNT(*) FROM supplier_catalog_products
                     WHERE run_id=? AND enrichment_status='carried_forward'""",
                (staging_run_id,),
            ).fetchone()[0]
            total = int(source["product_count"] or 0)
            write_started = time.monotonic()
            self._begin_immediate(connection)
            connection.execute(
                """INSERT INTO qogita_bootstrap_runs (
                       bootstrap_run_id,staging_run_id,started_at,updated_at,status,
                       target_count,batch_size,sample_strategy,worker_count,run_mode,
                       source_product_count,reusable_products,initial_pending_products,
                       product_link_pacing,offers_pacing
                   ) VALUES (?,?,?,?,'running',?,?,?,?,'production',?,?,?,?,?)""",
                (bootstrap_run_id, staging_run_id, now, now, total, batch_size,
                 "full_catalog_delta_priority_v1", workers, total, int(reusable),
                 total - int(reusable), float(product_link_pacing), float(offers_pacing)),
            )
            connection.execute(
                """INSERT INTO qogita_bootstrap_products (
                       bootstrap_run_id,staging_run_id,sequence_no,canonical_product_key,
                       gtin,csv_number_of_offers,status,variant_fid,scenario_count,
                       completed_at,updated_at
                   )
                   SELECT ?,?,ROW_NUMBER() OVER (ORDER BY
                       CASE
                         WHEN product.enrichment_status='carried_forward' THEN 90
                         WHEN product.catalog_delta_status='new' THEN 10
                         WHEN product.catalog_delta_status='changed' AND product.variant_fid IS NOT NULL THEN 20
                         WHEN product.catalog_delta_status='changed' THEN 30
                         WHEN product.enrichment_status='enrichment_failed' THEN 40
                         WHEN product.variant_fid IS NULL THEN 50
                         ELSE 60
                       END, product.rowid),
                       product.canonical_product_key,
                       COALESCE(product.canonical_ean,product.supplier_product_id),
                       CAST(json_extract(product.metadata_json,'$.number_of_offers') AS INTEGER),
                       CASE WHEN product.enrichment_status='carried_forward' THEN 'enriched'
                            WHEN product.variant_fid IS NOT NULL THEN 'fid_resolved'
                            ELSE 'pending' END,
                       product.variant_fid,COALESCE(scenario.count,0),
                       CASE WHEN product.enrichment_status='carried_forward' THEN ? ELSE NULL END,?
                   FROM supplier_catalog_products product
                   LEFT JOIN (
                       SELECT canonical_product_key,COUNT(*) count
                         FROM supplier_catalog_scenarios WHERE run_id=?
                         GROUP BY canonical_product_key
                   ) scenario ON scenario.canonical_product_key=product.canonical_product_key
                   WHERE product.run_id=?""",
                (bootstrap_run_id, staging_run_id, now, now, staging_run_id, staging_run_id),
            )
            inserted = connection.execute(
                "SELECT COUNT(*) FROM qogita_bootstrap_products WHERE bootstrap_run_id=?",
                (bootstrap_run_id,),
            ).fetchone()[0]
            if int(inserted) != total:
                connection.rollback()
                raise RuntimeError("Production bootstrap queue does not match source generation")
            connection.commit()
            self._record_write(write_started)
        self.checkpoint_concurrent(
            bootstrap_run_id, metrics={}, worker_count=workers,
            batch_attempted=0, wall_elapsed_seconds=0,
        )
        return self.bootstrap(bootstrap_run_id)

    def create_bootstrap(self, staging_run_id: str, *, target_count: int,
                         batch_size: int = 100, bootstrap_run_id: str | None = None,
                         exclude_bootstrap_run_ids: tuple[str, ...] = ()):
        if target_count <= 0 or batch_size <= 0:
            raise ValueError("target_count and batch_size must be positive")
        self.initialize()
        bootstrap_run_id = bootstrap_run_id or uuid4().hex
        now = utc_now()
        with _connect(self.path) as connection:
            staging = connection.execute(
                "SELECT supplier,product_count FROM supplier_catalog_runs WHERE run_id=?",
                (staging_run_id,),
            ).fetchone()
            if not staging or staging["supplier"] != "qogita":
                raise ValueError("Qogita staging generation not found")
            excluded_keys: set[str] = set()
            for excluded_run_id in exclude_bootstrap_run_ids:
                excluded_keys.update(row[0] for row in connection.execute(
                    "SELECT canonical_product_key FROM qogita_bootstrap_products WHERE bootstrap_run_id=?",
                    (excluded_run_id,),
                ))
            total = int(staging["product_count"]) - len(excluded_keys)
            if target_count > total:
                raise ValueError("target_count exceeds staging product count")
            existing = connection.execute(
                "SELECT * FROM qogita_bootstrap_runs WHERE bootstrap_run_id=?",
                (bootstrap_run_id,),
            ).fetchone()
            if existing:
                return self.bootstrap(bootstrap_run_id)
            offsets = {
                round(index * (total - 1) / (target_count - 1)) if target_count > 1 else 0
                for index in range(target_count)
            }
            write_started = time.monotonic()
            self._begin_immediate(connection)
            connection.execute(
                """INSERT INTO qogita_bootstrap_runs (
                    bootstrap_run_id,staging_run_id,started_at,updated_at,status,
                    target_count,batch_size,sample_strategy,excluded_bootstrap_runs_json
                ) VALUES (?,?,?,?,'running',?,?,?,?)""",
                (bootstrap_run_id, staging_run_id, now, now, target_count, batch_size,
                 "evenly_spaced_row_order_excluding_previous" if excluded_keys else
                 "evenly_spaced_row_order", json_dumps(list(exclude_bootstrap_run_ids))),
            )
            selected = 0
            eligible_offset = -1
            cursor = connection.execute(
                """SELECT canonical_product_key,supplier_product_id,raw_identifiers_json,
                          metadata_json FROM supplier_catalog_products
                   WHERE run_id=? ORDER BY rowid""",
                (staging_run_id,),
            )
            for row in cursor:
                if row["canonical_product_key"] in excluded_keys:
                    continue
                eligible_offset += 1
                if eligible_offset not in offsets:
                    continue
                raw = json.loads(row["raw_identifiers_json"] or "[]")
                gtin = str((raw[0] if raw else {}).get("value") or row["supplier_product_id"] or "")
                metadata = json.loads(row["metadata_json"] or "{}")
                selected += 1
                connection.execute(
                    """INSERT INTO qogita_bootstrap_products (
                        bootstrap_run_id,staging_run_id,sequence_no,canonical_product_key,
                        gtin,csv_number_of_offers,status,updated_at
                    ) VALUES (?,?,?,?,?,?,'pending',?)""",
                    (bootstrap_run_id, staging_run_id, selected, row["canonical_product_key"],
                     gtin, metadata.get("number_of_offers"), now),
                )
            if selected != target_count:
                raise RuntimeError("Deterministic Qogita sample selection was incomplete")
            connection.commit()
            self._record_write(write_started)
        return self.bootstrap(bootstrap_run_id)

    def bootstrap(self, bootstrap_run_id: str):
        self.initialize()
        with _connect(self.path) as connection:
            row = connection.execute(
                "SELECT * FROM qogita_bootstrap_runs WHERE bootstrap_run_id=?",
                (bootstrap_run_id,),
            ).fetchone()
        if not row:
            return None
        value = dict(row)
        value["last_progress"] = json.loads(value.pop("last_progress_json") or "{}")
        value["health"] = json.loads(value.pop("health_json") or "{}")
        return value

    def validate_production_source(self, bootstrap_run_id: str):
        """Fail closed if a persistent runner no longer sees its exact full source."""
        self.initialize()
        with _connect(self.path) as connection:
            row = connection.execute(
                """SELECT bootstrap.staging_run_id,bootstrap.source_product_count,
                          bootstrap.target_count,source.supplier,source.status,
                          source.product_count,source.product_catalog_coverage_type,
                          source.product_catalog_coverage_complete,
                          (SELECT COUNT(*) FROM qogita_bootstrap_products selected
                            WHERE selected.bootstrap_run_id=bootstrap.bootstrap_run_id) queue_count
                     FROM qogita_bootstrap_runs bootstrap
                     JOIN supplier_catalog_runs source ON source.run_id=bootstrap.staging_run_id
                    WHERE bootstrap.bootstrap_run_id=? AND bootstrap.run_mode='production'""",
                (bootstrap_run_id,),
            ).fetchone()
        if not row:
            raise ValueError("Production bootstrap not found")
        valid = (
            row["supplier"] == "qogita" and row["status"] == "success"
            and row["product_catalog_coverage_type"] == "full_account_catalog"
            and int(row["product_catalog_coverage_complete"] or 0) == 1
            and int(row["product_count"] or 0) == int(row["source_product_count"] or -1)
            and int(row["queue_count"] or 0) == int(row["target_count"] or -1)
        )
        if not valid:
            raise QogitaBootstrapError(
                "Qogita production source generation is inconsistent",
                code="source_generation_inconsistent",
            )
        return dict(row)

    def mark_stopped(self, bootstrap_run_id: str, reason: str, *, health=None):
        now = utc_now()
        with _connect(self.path) as connection:
            self._begin_immediate(connection)
            connection.execute(
                """UPDATE qogita_bootstrap_runs SET status='auto_stopped',stop_reason=?,
                          health_json=?,updated_at=? WHERE bootstrap_run_id=?""",
                (str(reason)[:300], json_dumps(health or {}), now, bootstrap_run_id),
            )
            connection.execute(
                """UPDATE qogita_bootstrap_products SET worker_id=NULL,claimed_at=NULL,
                          lease_expires_at=NULL WHERE bootstrap_run_id=?""",
                (bootstrap_run_id,),
            )
            connection.commit()
        return self.bootstrap(bootstrap_run_id)

    def update_health(self, bootstrap_run_id: str, health: dict[str, Any]):
        with _connect(self.path) as connection:
            connection.execute(
                """UPDATE qogita_bootstrap_runs SET health_json=?,updated_at=?
                     WHERE bootstrap_run_id=?""",
                (json_dumps(health), utc_now(), bootstrap_run_id),
            )
            connection.commit()

    def database_integrity(self):
        with _connect(self.path) as connection:
            result = connection.execute("PRAGMA quick_check").fetchone()[0]
            duplicates = connection.execute(
                """SELECT COUNT(*) FROM (
                       SELECT run_id,scenario_id,COUNT(*) count
                         FROM supplier_catalog_scenarios
                        GROUP BY run_id,scenario_id HAVING count>1
                   )"""
            ).fetchone()[0]
        return {"quick_check": str(result), "duplicate_scenario_identities": int(duplicates)}

    def resume_production(self, bootstrap_run_id: str):
        with _connect(self.path) as connection:
            self._begin_immediate(connection)
            row = connection.execute(
                "SELECT run_mode,status FROM qogita_bootstrap_runs WHERE bootstrap_run_id=?",
                (bootstrap_run_id,),
            ).fetchone()
            if not row or row["run_mode"] != "production":
                raise ValueError("Production bootstrap not found")
            if row["status"] == "awaiting_promotion_review":
                connection.rollback()
                return self.bootstrap(bootstrap_run_id)
            connection.execute(
                """UPDATE qogita_bootstrap_runs SET status='running',stop_reason=NULL,
                          updated_at=? WHERE bootstrap_run_id=?""",
                (utc_now(), bootstrap_run_id),
            )
            connection.commit()
        return self.bootstrap(bootstrap_run_id)

    def record_milestones(self, bootstrap_run_id: str, *, metrics: dict[str, Any],
                          milestones=(25000, 50000, 100000, 200000)):
        completed = int(metrics.get("offers_success") or 0)
        if int(metrics.get("remaining") or 0) == 0:
            milestones = (*milestones, int(metrics.get("selected") or completed))
        now = utc_now()
        recorded = []
        with _connect(self.path) as connection:
            self._begin_immediate(connection)
            for milestone in sorted(set(int(value) for value in milestones if int(value) > 0)):
                if completed < milestone:
                    continue
                cursor = connection.execute(
                    """INSERT OR IGNORE INTO qogita_bootstrap_milestones
                           (bootstrap_run_id,milestone,reached_at,metrics_json)
                       VALUES (?,?,?,?)""",
                    (bootstrap_run_id, milestone, now, json_dumps(metrics)),
                )
                if cursor.rowcount:
                    recorded.append(milestone)
            connection.commit()
        return recorded

    def milestones(self, bootstrap_run_id: str):
        with _connect(self.path) as connection:
            return [dict(row) for row in connection.execute(
                """SELECT * FROM qogita_bootstrap_milestones
                     WHERE bootstrap_run_id=? ORDER BY milestone""",
                (bootstrap_run_id,),
            )]

    def next_batch(self, bootstrap_run_id: str, *, limit: int | None = None,
                   after_sequence: int = 0):
        run = self.bootstrap(bootstrap_run_id)
        if not run:
            raise ValueError("Bootstrap run not found")
        batch = min(int(limit or run["batch_size"]), int(run["batch_size"]))
        placeholders = ",".join("?" for _ in RETRYABLE_PRODUCT_STATES)
        with _connect(self.path) as connection:
            rows = connection.execute(
                f"""SELECT selected.*,product.brand,product.title,product.product_url,
                            product.variant_fid AS persisted_variant_fid,
                            product.variant_fid_source
                     FROM qogita_bootstrap_products selected
                     JOIN supplier_catalog_products product
                       ON product.run_id=selected.staging_run_id
                      AND product.canonical_product_key=selected.canonical_product_key
                     WHERE selected.bootstrap_run_id=?
                       AND selected.status IN ({placeholders})
                       AND selected.sequence_no>?
                     ORDER BY selected.sequence_no LIMIT ?""",
                (bootstrap_run_id, *sorted(RETRYABLE_PRODUCT_STATES), int(after_sequence), batch),
            ).fetchall()
        return [dict(row) for row in rows]

    def claim_batch(self, bootstrap_run_id: str, *, worker_id: str, limit: int,
                    lease_seconds: int = 300, now: str | None = None):
        """Atomically lease work; expired claims are recoverable by another worker."""
        claimed_at = now or utc_now()
        lease_at = (_as_datetime(claimed_at) + timedelta(seconds=max(1, lease_seconds)))
        lease_expires = lease_at.isoformat().replace("+00:00", "Z")
        placeholders = ",".join("?" for _ in RETRYABLE_PRODUCT_STATES)
        write_started = time.monotonic()
        with _connect(self.path) as connection:
            self._begin_immediate(connection)
            connection.execute(
                f"""UPDATE qogita_bootstrap_products
                       SET worker_id=NULL,claimed_at=NULL,lease_expires_at=NULL
                     WHERE bootstrap_run_id=? AND status IN ({placeholders})
                       AND worker_id IS NOT NULL AND lease_expires_at<=?""",
                (bootstrap_run_id, *sorted(RETRYABLE_PRODUCT_STATES), claimed_at),
            )
            keys = [row[0] for row in connection.execute(
                f"""SELECT canonical_product_key FROM qogita_bootstrap_products
                     WHERE bootstrap_run_id=? AND status IN ({placeholders})
                       AND worker_id IS NULL ORDER BY sequence_no LIMIT ?""",
                (bootstrap_run_id, *sorted(RETRYABLE_PRODUCT_STATES), max(1, int(limit))),
            )]
            for key in keys:
                connection.execute(
                    """UPDATE qogita_bootstrap_products
                         SET worker_id=?,claimed_at=?,lease_expires_at=?,claim_count=claim_count+1
                       WHERE bootstrap_run_id=? AND canonical_product_key=? AND worker_id IS NULL""",
                    (worker_id, claimed_at, lease_expires, bootstrap_run_id, key),
                )
            connection.commit()
        self._record_write(write_started)
        if not keys:
            return []
        with _connect(self.path) as connection:
            placeholders_keys = ",".join("?" for _ in keys)
            rows = connection.execute(
                f"""SELECT selected.*,product.brand,product.title,product.product_url,
                            product.variant_fid AS persisted_variant_fid,
                            product.variant_fid_source
                     FROM qogita_bootstrap_products selected
                     JOIN supplier_catalog_products product
                       ON product.run_id=selected.staging_run_id
                      AND product.canonical_product_key=selected.canonical_product_key
                     WHERE selected.bootstrap_run_id=? AND selected.worker_id=?
                       AND selected.canonical_product_key IN ({placeholders_keys})
                     ORDER BY selected.sequence_no""",
                (bootstrap_run_id, worker_id, *keys),
            ).fetchall()
        return [dict(row) for row in rows]

    def release_worker_claims(self, bootstrap_run_id: str, worker_id: str) -> int:
        write_started = time.monotonic()
        with _connect(self.path) as connection:
            self._begin_immediate(connection)
            cursor = connection.execute(
                """UPDATE qogita_bootstrap_products
                      SET worker_id=NULL,claimed_at=NULL,lease_expires_at=NULL
                    WHERE bootstrap_run_id=? AND worker_id=?
                      AND status NOT IN ('enriched','resolver_permanent','offers_permanent','parsing_failure')""",
                (bootstrap_run_id, worker_id),
            )
            connection.commit()
        self._record_write(write_started)
        return int(cursor.rowcount)

    def claim_summary(self, bootstrap_run_id: str, *, now: str | None = None):
        observed = now or utc_now()
        with _connect(self.path) as connection:
            row = connection.execute(
                """SELECT SUM(worker_id IS NOT NULL) claimed,
                          SUM(worker_id IS NOT NULL AND lease_expires_at<=?) expired,
                          COALESCE(SUM(CASE WHEN claim_count>1 THEN claim_count-1 ELSE 0 END),0)
                            reclaimed
                     FROM qogita_bootstrap_products WHERE bootstrap_run_id=?""",
                (observed, bootstrap_run_id),
            ).fetchone()
        return {"claimed": int(row["claimed"] or 0), "expired": int(row["expired"] or 0),
                "reclaimed": int(row["reclaimed"] or 0)}

    def requeue_expired_auth_failures(self, bootstrap_run_id: str) -> int:
        """Recover rows written by clients that lacked token refresh on HTTP 401."""
        self.initialize()
        now = utc_now()
        with _connect(self.path) as connection:
            cursor = connection.execute(
                """UPDATE qogita_bootstrap_products
                      SET status='offers_retryable',
                          error_code='offers_authentication_expired',updated_at=?
                    WHERE bootstrap_run_id=? AND status='offers_permanent'
                      AND error_code='offers_http'
                      AND error_message='Qogita offers failed with HTTP 401'""",
                (now, bootstrap_run_id),
            )
            connection.commit()
            return int(cursor.rowcount)

    def persist_fid(self, bootstrap_run_id: str, canonical_product_key: str,
                    variant_fid: str, *, elapsed_seconds: float, attempts: int):
        now = utc_now()
        write_started = time.monotonic()
        with _connect(self.path) as connection:
            self._begin_immediate(connection)
            selected = connection.execute(
                """SELECT staging_run_id,gtin FROM qogita_bootstrap_products
                   WHERE bootstrap_run_id=? AND canonical_product_key=?""",
                (bootstrap_run_id, canonical_product_key),
            ).fetchone()
            if not selected:
                raise ValueError("Bootstrap product not found")
            product = connection.execute(
                """SELECT variant_fid,metadata_json FROM supplier_catalog_products
                   WHERE run_id=? AND canonical_product_key=?""",
                (selected["staging_run_id"], canonical_product_key),
            ).fetchone()
            current = str(product["variant_fid"] or "")
            if current and current != variant_fid:
                connection.execute(
                    """UPDATE supplier_catalog_products SET enrichment_status='enrichment_failed',
                       enrichment_error_code='variant_fid_conflict',
                       enrichment_error_message='Product Link FID conflicts with persisted FID'
                       WHERE run_id=? AND canonical_product_key=?""",
                    (selected["staging_run_id"], canonical_product_key),
                )
                connection.execute(
                    """UPDATE qogita_bootstrap_products SET status='resolver_permanent',
                       resolver_attempts=resolver_attempts+?,resolver_elapsed_seconds=resolver_elapsed_seconds+?,
                       error_code='variant_fid_conflict',error_message='FID conflict',updated_at=?,
                       worker_id=NULL,claimed_at=NULL,lease_expires_at=NULL,completed_at=?
                       WHERE bootstrap_run_id=? AND canonical_product_key=?""",
                    (attempts, elapsed_seconds, now, now, bootstrap_run_id, canonical_product_key),
                )
                connection.commit()
                self._record_write(write_started)
                raise QogitaFidConflict()
            metadata = json.loads(product["metadata_json"] or "{}")
            metadata["variant_fid"] = variant_fid
            metadata["variant_fid_source"] = PRODUCT_LINK_SOURCE
            connection.execute(
                """UPDATE supplier_catalog_products SET variant_fid=?,variant_fid_source=?,
                   enrichment_status='enrichment_pending',enrichment_error_code=NULL,
                   enrichment_error_message=NULL,metadata_json=?
                   WHERE run_id=? AND canonical_product_key=?""",
                (variant_fid, PRODUCT_LINK_SOURCE, json_dumps(metadata),
                 selected["staging_run_id"], canonical_product_key),
            )
            connection.execute(
                """UPDATE qogita_bootstrap_products SET status='fid_resolved',variant_fid=?,
                   resolver_attempts=resolver_attempts+?,resolver_elapsed_seconds=resolver_elapsed_seconds+?,
                   error_code=NULL,error_message=NULL,updated_at=?
                   WHERE bootstrap_run_id=? AND canonical_product_key=?""",
                (variant_fid, attempts, elapsed_seconds, now, bootstrap_run_id, canonical_product_key),
            )
            connection.execute(
                """UPDATE qogita_enrichment_queue SET status='completed',variant_fid=?
                   WHERE run_id=? AND canonical_product_key=? AND task_type='resolve_variant'""",
                (variant_fid, selected["staging_run_id"], canonical_product_key),
            )
            connection.execute(
                """INSERT INTO qogita_enrichment_queue (
                    run_id,canonical_product_key,variant_fid,task_type,reason,priority,
                    status,source_observed_at,created_at
                ) VALUES (?,?,?,'offers_enrichment','fid_resolved',100,'pending',NULL,?)
                ON CONFLICT(run_id,canonical_product_key,task_type) DO UPDATE SET
                    variant_fid=excluded.variant_fid,status=CASE
                        WHEN qogita_enrichment_queue.status='completed' THEN 'completed'
                        ELSE 'pending' END""",
                (selected["staging_run_id"], canonical_product_key, variant_fid, now),
            )
            connection.commit()
        self._record_write(write_started)
        return {"status": "fid_resolved", "variant_fid": variant_fid, "no_op": current == variant_fid}

    def persist_failure(self, bootstrap_run_id: str, canonical_product_key: str, *,
                        phase: str, error: QogitaBootstrapError, attempts: int,
                        elapsed_seconds: float):
        now = utc_now()
        status = (
            "parsing_failure" if phase == "offers" and (
                error.code.startswith("offers_parsing")
                or error.code.startswith("offers_duplicate")
            ) else f"{phase}_{'retryable' if error.retryable else 'permanent'}"
        )
        attempt_column = "resolver_attempts" if phase == "resolver" else "offers_attempts"
        elapsed_column = "resolver_elapsed_seconds" if phase == "resolver" else "offers_elapsed_seconds"
        write_started = time.monotonic()
        with _connect(self.path) as connection:
            self._begin_immediate(connection)
            row = connection.execute(
                """SELECT staging_run_id FROM qogita_bootstrap_products
                   WHERE bootstrap_run_id=? AND canonical_product_key=?""",
                (bootstrap_run_id, canonical_product_key),
            ).fetchone()
            connection.execute(
                f"""UPDATE qogita_bootstrap_products SET status=?,{attempt_column}={attempt_column}+?,
                    {elapsed_column}={elapsed_column}+?,error_code=?,error_message=?,updated_at=?,
                    worker_id=NULL,claimed_at=NULL,lease_expires_at=NULL,
                    completed_at=CASE WHEN ?=1 THEN ? ELSE NULL END
                    WHERE bootstrap_run_id=? AND canonical_product_key=?""",
                (status, attempts, elapsed_seconds, error.code, str(error)[:300], now,
                 int(not error.retryable), now,
                 bootstrap_run_id, canonical_product_key),
            )
            connection.execute(
                """UPDATE supplier_catalog_products SET enrichment_status='enrichment_failed',
                   enrichment_error_code=?,enrichment_error_message=?
                   WHERE run_id=? AND canonical_product_key=?""",
                (error.code, str(error)[:300], row["staging_run_id"], canonical_product_key),
            )
            task = "resolve_variant" if phase == "resolver" else "offers_enrichment"
            connection.execute(
                """UPDATE qogita_enrichment_queue SET status=?,attempt_count=attempt_count+?
                   WHERE run_id=? AND canonical_product_key=? AND task_type=?""",
                ("pending" if error.retryable else "failed", attempts,
                 row["staging_run_id"], canonical_product_key, task),
            )
            connection.commit()
        self._record_write(write_started)

    def persist_offers(self, bootstrap_run_id: str, canonical_product_key: str, *,
                       scenarios: list[dict[str, Any]], diagnostics: dict[str, int],
                       observed_at: str, elapsed_seconds: float, attempts: int):
        now = utc_now()
        write_started = time.monotonic()
        with _connect(self.path) as connection:
            self._begin_immediate(connection)
            row = connection.execute(
                """SELECT staging_run_id,variant_fid FROM qogita_bootstrap_products
                   WHERE bootstrap_run_id=? AND canonical_product_key=?""",
                (bootstrap_run_id, canonical_product_key),
            ).fetchone()
            if not row or not row["variant_fid"]:
                raise ValueError("Offers cannot be persisted before FID resolution")
            connection.execute(
                "DELETE FROM supplier_catalog_scenarios WHERE run_id=? AND canonical_product_key=?",
                (row["staging_run_id"], canonical_product_key),
            )
            for scenario in scenarios:
                connection.execute(
                    """INSERT INTO supplier_catalog_scenarios (
                        run_id,supplier,scenario_id,canonical_product_key,canonical_ean,
                        raw_identifier,raw_identifier_type,supplier_product_id,
                        supplier_offer_id,supplier_sku,scenario_type,scenario_label,
                        price,currency,stock,minimum_quantity,maximum_quantity,selling_unit,
                        account_mov,account_mov_currency,warehouse,shipping_mode,
                        availability_status,lead_time,payload_json
                    ) VALUES (?,'qogita',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (row["staging_run_id"], scenario["scenario_id"], canonical_product_key,
                     scenario["canonical_ean"], scenario["raw_identifier"],
                     scenario["raw_identifier_type"], scenario["supplier_product_id"],
                     scenario["supplier_offer_id"], None, scenario["scenario_type"],
                     scenario["scenario_label"], scenario["price"], scenario["currency"],
                     scenario["stock"], scenario["minimum_quantity"],
                     scenario["maximum_quantity"], scenario["selling_unit"],
                     scenario["account_mov"], scenario["account_mov_currency"],
                     scenario["warehouse"], scenario["shipping_mode"],
                     scenario["availability_status"], scenario["lead_time"],
                     json_dumps(scenario["payload"])),
                )
            product = connection.execute(
                "SELECT metadata_json FROM supplier_catalog_products WHERE run_id=? AND canonical_product_key=?",
                (row["staging_run_id"], canonical_product_key),
            ).fetchone()
            metadata = json.loads(product["metadata_json"] or "{}")
            metadata.update({
                "offers_endpoint_count": diagnostics["offer_count"],
                "offers_seller_count": diagnostics["seller_count"],
                "offers_offer_qid_count": diagnostics["offer_qid_count"],
                "offers_tier_count": diagnostics["raw_tier_count"],
                "offers_scenario_count": diagnostics["scenario_count"],
                "offer_tier_observed_at": observed_at,
            })
            connection.execute(
                """UPDATE supplier_catalog_products SET enrichment_status='enriched',
                   offer_tier_observed_at=?,enrichment_error_code=NULL,
                   enrichment_error_message=NULL,metadata_json=?
                   WHERE run_id=? AND canonical_product_key=?""",
                (observed_at, json_dumps(metadata), row["staging_run_id"], canonical_product_key),
            )
            connection.execute(
                """UPDATE qogita_bootstrap_products SET status='enriched',
                   offers_attempts=offers_attempts+?,offers_elapsed_seconds=offers_elapsed_seconds+?,
                   endpoint_offer_count=?,seller_count=?,tier_count=?,scenario_count=?,
                   error_code=NULL,error_message=NULL,updated_at=?,completed_at=?,
                   worker_id=NULL,claimed_at=NULL,lease_expires_at=NULL
                   WHERE bootstrap_run_id=? AND canonical_product_key=?""",
                (attempts, elapsed_seconds, diagnostics["offer_count"], diagnostics["seller_count"],
                 diagnostics["raw_tier_count"], diagnostics["scenario_count"], now, now,
                 bootstrap_run_id, canonical_product_key),
            )
            connection.execute(
                """UPDATE qogita_enrichment_queue SET status='completed',attempt_count=attempt_count+?,
                   source_observed_at=? WHERE run_id=? AND canonical_product_key=?
                   AND task_type='offers_enrichment'""",
                (attempts, observed_at, row["staging_run_id"], canonical_product_key),
            )
            connection.commit()
        self._record_write(write_started)

    def checkpoint_batch(self, bootstrap_run_id: str, *, client_metrics: dict[str, Any],
                         batch_attempted: int):
        now = utc_now()
        with _connect(self.path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            run = connection.execute(
                "SELECT staging_run_id,target_count FROM qogita_bootstrap_runs WHERE bootstrap_run_id=?",
                (bootstrap_run_id,),
            ).fetchone()
            counts = dict(connection.execute(
                """SELECT
                    COUNT(*) AS selected,
                    SUM(status<>'pending') AS attempted,
                    SUM(variant_fid IS NOT NULL) AS fid_resolved,
                    SUM(status IN ('resolver_retryable','resolver_permanent')) AS fid_failed,
                    SUM(status='enriched') AS offers_success,
                    SUM(status IN ('offers_retryable','offers_permanent','parsing_failure')) AS offers_failed,
                    SUM(status IN ('resolver_retryable','offers_retryable')) AS retryable,
                    SUM(status IN ('resolver_permanent','offers_permanent','parsing_failure')) AS terminal_failed,
                    COALESCE(SUM(scenario_count),0) AS scenarios_written
                   FROM qogita_bootstrap_products WHERE bootstrap_run_id=?""",
                (bootstrap_run_id,),
            ).fetchone())
            remaining = counts["selected"] - counts["offers_success"] - counts["terminal_failed"]
            status = "completed" if remaining == 0 else (
                "waiting_retry" if counts["retryable"] else "running"
            )
            progress = {**counts, "remaining": remaining, "last_batch_attempted": batch_attempted}
            connection.execute(
                """UPDATE qogita_bootstrap_runs SET updated_at=?,status=?,
                   completed_batches=completed_batches+1,products_attempted=?,fid_resolved=?,
                   fid_failed=?,offers_success=?,offers_failed=?,scenarios_written=?,
                   product_link_requests=product_link_requests+?,
                   offers_requests=offers_requests+?,retry_count=retry_count+?,
                   rate_limit_count=rate_limit_count+?,server_error_count=server_error_count+?,
                   resolver_elapsed_seconds=resolver_elapsed_seconds+?,
                   offers_elapsed_seconds=offers_elapsed_seconds+?,
                   last_progress_json=? WHERE bootstrap_run_id=?""",
                (now, status, counts["attempted"], counts["fid_resolved"], counts["fid_failed"],
                 counts["offers_success"], counts["offers_failed"], counts["scenarios_written"],
                 client_metrics["product_link_requests"], client_metrics["offers_requests"],
                 client_metrics["retries"], client_metrics["http_429"], client_metrics["http_5xx"],
                 client_metrics["resolver_elapsed_seconds"], client_metrics["offers_elapsed_seconds"],
                 json_dumps(progress), bootstrap_run_id),
            )
            scenario_count = connection.execute(
                "SELECT COUNT(*) FROM supplier_catalog_scenarios WHERE run_id=?",
                (run["staging_run_id"],),
            ).fetchone()[0]
            enriched_products = connection.execute(
                """SELECT COUNT(*) FROM supplier_catalog_products
                   WHERE run_id=? AND enrichment_status IN ('enriched','carried_forward')""",
                (run["staging_run_id"],),
            ).fetchone()[0]
            enrichment_status = "partial" if enriched_products else "none"
            connection.execute(
                """UPDATE supplier_catalog_runs SET scenario_count=?,scenario_enrichment_count=?,
                   scenario_enrichment_status=?,scenario_enrichment_observed_at=? WHERE run_id=?""",
                (scenario_count, enriched_products, enrichment_status,
                 now if enriched_products else None, run["staging_run_id"]),
            )
            connection.commit()
        return self.bootstrap(bootstrap_run_id)

    def checkpoint_concurrent(self, bootstrap_run_id: str, *, metrics: dict[str, Any],
                              worker_count: int, batch_attempted: int,
                              wall_elapsed_seconds: float):
        """Set concurrency metrics from a single aggregated snapshot."""
        now = utc_now()
        wal = Path(str(self.path) + "-wal")
        wal_size = wal.stat().st_size if wal.exists() else 0
        write_started = time.monotonic()
        with _connect(self.path) as connection:
            self._begin_immediate(connection)
            run = connection.execute(
                "SELECT staging_run_id,run_mode,status FROM qogita_bootstrap_runs WHERE bootstrap_run_id=?",
                (bootstrap_run_id,),
            ).fetchone()
            counts = dict(connection.execute(
                """SELECT COUNT(*) selected,SUM(status<>'pending') attempted,
                          SUM(variant_fid IS NOT NULL) fid_resolved,
                          SUM(status IN ('resolver_retryable','resolver_permanent')) fid_failed,
                          SUM(status='enriched') offers_success,
                          SUM(status IN ('offers_retryable','offers_permanent','parsing_failure')) offers_failed,
                          SUM(status IN ('resolver_retryable','offers_retryable')) retryable,
                          SUM(status IN ('resolver_permanent','offers_permanent','parsing_failure')) terminal_failed,
                          COALESCE(SUM(scenario_count),0) scenarios_written
                     FROM qogita_bootstrap_products WHERE bootstrap_run_id=?""",
                (bootstrap_run_id,),
            ).fetchone())
            remaining = counts["selected"] - counts["offers_success"] - counts["terminal_failed"]
            status = ("awaiting_promotion_review" if remaining == 0 and run["run_mode"] == "production"
                      else "completed" if remaining == 0 else (
                "waiting_retry" if counts["retryable"] else "running"
            ))
            if run["status"] == "auto_stopped" and remaining:
                status = "auto_stopped"
            progress = {**counts, "remaining": remaining,
                        "last_batch_attempted": batch_attempted,
                        "claims": self.claim_summary(bootstrap_run_id, now=now)}
            sqlite_values = dict(self.sqlite_metrics)
            connection.execute(
                """UPDATE qogita_bootstrap_runs SET updated_at=?,status=?,
                   completed_batches=completed_batches+1,products_attempted=?,fid_resolved=?,
                   fid_failed=?,offers_success=?,offers_failed=?,scenarios_written=?,worker_count=?,
                   product_link_requests=?,offers_requests=?,retry_count=?,http_401_count=?,
                   auth_refresh_count=?,rate_limit_count=?,server_error_count=?,
                   resolver_elapsed_seconds=?,offers_elapsed_seconds=?,sqlite_busy_count=?,
                   transaction_retry_count=?,lock_wait_seconds=?,write_latency_seconds=?,
                   wal_peak_bytes=MAX(wal_peak_bytes,?),wall_elapsed_seconds=?,last_progress_json=?
                   WHERE bootstrap_run_id=?""",
                (now, status, counts["attempted"], counts["fid_resolved"], counts["fid_failed"],
                 counts["offers_success"], counts["offers_failed"], counts["scenarios_written"],
                 worker_count, metrics.get("product_link_requests", 0),
                 metrics.get("offers_requests", 0), metrics.get("retries", 0),
                 metrics.get("http_401", 0), metrics.get("auth_refreshes", 0),
                 metrics.get("http_429", 0), metrics.get("http_5xx", 0),
                 metrics.get("resolver_elapsed_seconds", 0),
                 metrics.get("offers_elapsed_seconds", 0), sqlite_values["sqlite_busy"],
                 sqlite_values["transaction_retries"], sqlite_values["lock_wait_seconds"],
                 sqlite_values["write_latency_seconds"], wal_size, wall_elapsed_seconds,
                 json_dumps(progress), bootstrap_run_id),
            )
            scenario_count = connection.execute(
                "SELECT COUNT(*) FROM supplier_catalog_scenarios WHERE run_id=?",
                (run["staging_run_id"],),
            ).fetchone()[0]
            enriched_products = connection.execute(
                """SELECT COUNT(*) FROM supplier_catalog_products
                   WHERE run_id=? AND enrichment_status IN ('enriched','carried_forward')""",
                (run["staging_run_id"],),
            ).fetchone()[0]
            connection.execute(
                """UPDATE supplier_catalog_runs SET scenario_count=?,scenario_enrichment_count=?,
                   scenario_enrichment_status=?,scenario_enrichment_observed_at=? WHERE run_id=?""",
                (scenario_count, enriched_products, "partial" if enriched_products else "none",
                 now if enriched_products else None, run["staging_run_id"]),
            )
            connection.commit()
        self._record_write(write_started)
        return self.bootstrap(bootstrap_run_id)

    def products(self, bootstrap_run_id: str):
        self.initialize()
        with _connect(self.path) as connection:
            return [dict(row) for row in connection.execute(
                """SELECT * FROM qogita_bootstrap_products
                   WHERE bootstrap_run_id=? ORDER BY sequence_no""",
                (bootstrap_run_id,),
            )]


def _sleep_for_retry(error: QogitaBootstrapError, attempt: int, sleep_func: Callable[[float], None]):
    try:
        delay = float(error.retry_after) if error.retry_after else min(2 ** (attempt - 1), 10)
    except (TypeError, ValueError):
        delay = min(2 ** (attempt - 1), 10)
    sleep_func(max(0, delay))


def run_qogita_bootstrap(
    bootstrap_run_id: str, *, store: QogitaBootstrapStore,
    client: QogitaBootstrapClient, max_products: int | None = None,
    batch_size: int | None = None, product_link_pacing: float = 0.6,
    offers_pacing: float = 0.6, max_attempts: int = 3,
    sleep_func: Callable[[float], None] = time.sleep,
):
    """Process bounded batches and checkpoint after each batch; safe to call again."""
    run = store.bootstrap(bootstrap_run_id)
    if not run:
        raise ValueError("Bootstrap run not found")
    store.requeue_expired_auth_failures(bootstrap_run_id)
    run = store.bootstrap(bootstrap_run_id)
    remaining_budget = int(max_products) if max_products is not None else int(run["target_count"])
    if remaining_budget < 0:
        raise ValueError("max_products must be non-negative")
    effective_batch = min(int(batch_size or run["batch_size"]), int(run["batch_size"]))
    last_link_at = last_offers_at = None
    last_sequence = 0
    checkpoint_metrics = dict(client.metrics)
    invocation_start_metrics = dict(client.metrics)
    invocation_attempted = 0
    while remaining_budget > 0:
        rows = store.next_batch(
            bootstrap_run_id, limit=min(effective_batch, remaining_budget),
            after_sequence=last_sequence,
        )
        if not rows:
            break
        batch_attempted = 0
        for selected in rows:
            last_sequence = max(last_sequence, int(selected["sequence_no"]))
            batch_attempted += 1
            invocation_attempted += 1
            remaining_budget -= 1
            fid = str(selected.get("persisted_variant_fid") or selected.get("variant_fid") or "")
            if not fid:
                attempts = 0
                elapsed = 0.0
                while attempts < max_attempts:
                    attempts += 1
                    if last_link_at is not None:
                        sleep_func(max(0, product_link_pacing - (time.monotonic() - last_link_at)))
                    try:
                        result = client.resolve_fid(selected["gtin"], selected.get("product_url"))
                        last_link_at = time.monotonic()
                        elapsed += float(result["elapsed_seconds"])
                        fid = result["variant_fid"]
                        store.persist_fid(
                            bootstrap_run_id, selected["canonical_product_key"], fid,
                            elapsed_seconds=elapsed, attempts=attempts,
                        )
                        break
                    except QogitaFidConflict:
                        fid = ""
                        break
                    except QogitaBootstrapError as exc:
                        last_link_at = time.monotonic()
                        if exc.retryable and attempts < max_attempts:
                            client.metrics["retries"] += 1
                            _sleep_for_retry(exc, attempts, sleep_func)
                            continue
                        store.persist_failure(
                            bootstrap_run_id, selected["canonical_product_key"],
                            phase="resolver", error=exc, attempts=attempts,
                            elapsed_seconds=elapsed,
                        )
                        fid = ""
                        break
            if not fid:
                continue
            attempts = 0
            elapsed = 0.0
            while attempts < max_attempts:
                attempts += 1
                if last_offers_at is not None:
                    sleep_func(max(0, offers_pacing - (time.monotonic() - last_offers_at)))
                try:
                    response = client.fetch_offers(fid)
                    last_offers_at = time.monotonic()
                    elapsed += float(response["elapsed_seconds"])
                    observed_at = utc_now()
                    product = {
                        "canonical_product_key": selected["canonical_product_key"],
                        "gtin": selected["gtin"], "brand": selected.get("brand"),
                        "title": selected.get("title"), "product_url": selected.get("product_url"),
                    }
                    scenarios, diagnostics = qogita_scenarios_from_offers(
                        product, fid, response["payload"], staging_run_id=run["staging_run_id"],
                        observed_at=observed_at,
                    )
                    store.persist_offers(
                        bootstrap_run_id, selected["canonical_product_key"],
                        scenarios=scenarios, diagnostics=diagnostics,
                        observed_at=observed_at, elapsed_seconds=elapsed, attempts=attempts,
                    )
                    break
                except QogitaBootstrapError as exc:
                    last_offers_at = time.monotonic()
                    if exc.retryable and attempts < max_attempts:
                        client.metrics["retries"] += 1
                        _sleep_for_retry(exc, attempts, sleep_func)
                        continue
                    phase_error = exc
                    if exc.code.startswith("offers_parsing") or exc.code.startswith("offers_duplicate"):
                        phase_error = QogitaBootstrapError(str(exc), code=exc.code, retryable=False)
                    store.persist_failure(
                        bootstrap_run_id, selected["canonical_product_key"],
                        phase="offers", error=phase_error, attempts=attempts,
                        elapsed_seconds=elapsed,
                    )
                    break
            if remaining_budget <= 0:
                break
        metric_delta = {
            key: client.metrics[key] - checkpoint_metrics.get(key, 0)
            for key in client.metrics
        }
        store.checkpoint_batch(
            bootstrap_run_id, client_metrics=metric_delta,
            batch_attempted=batch_attempted,
        )
        checkpoint_metrics = dict(client.metrics)
    final = store.bootstrap(bootstrap_run_id)
    final["invocation_products_attempted"] = invocation_attempted
    final["invocation_metrics"] = {
        key: client.metrics[key] - invocation_start_metrics.get(key, 0)
        for key in client.metrics
    }
    return final


def _process_claimed_product(
    selected: dict[str, Any], *, bootstrap_run_id: str, staging_run_id: str,
    store: QogitaBootstrapStore, client: QogitaBootstrapClient,
    max_attempts: int, sleep_func: Callable[[float], None],
):
    fid = str(selected.get("persisted_variant_fid") or selected.get("variant_fid") or "")
    if not fid:
        elapsed = 0.0
        for attempts in range(1, max_attempts + 1):
            try:
                result = client.resolve_fid(selected["gtin"], selected.get("product_url"))
                elapsed += float(result["elapsed_seconds"])
                fid = result["variant_fid"]
                store.persist_fid(
                    bootstrap_run_id, selected["canonical_product_key"], fid,
                    elapsed_seconds=elapsed, attempts=attempts,
                )
                break
            except QogitaFidConflict:
                return {"status": "failed", "error_code": "variant_fid_conflict"}
            except QogitaBootstrapError as exc:
                if exc.retryable and attempts < max_attempts:
                    client.metrics["retries"] += 1
                    _sleep_for_retry(exc, attempts, sleep_func)
                    continue
                store.persist_failure(
                    bootstrap_run_id, selected["canonical_product_key"], phase="resolver",
                    error=exc, attempts=attempts, elapsed_seconds=elapsed,
                )
                return {"status": "failed", "error_code": exc.code,
                        "http_status": exc.http_status}
    elapsed = 0.0
    for attempts in range(1, max_attempts + 1):
        try:
            response = client.fetch_offers(fid)
            elapsed += float(response["elapsed_seconds"])
            observed_at = utc_now()
            product = {
                "canonical_product_key": selected["canonical_product_key"],
                "gtin": selected["gtin"], "brand": selected.get("brand"),
                "title": selected.get("title"), "product_url": selected.get("product_url"),
            }
            scenarios, diagnostics = qogita_scenarios_from_offers(
                product, fid, response["payload"], staging_run_id=staging_run_id,
                observed_at=observed_at,
            )
            store.persist_offers(
                bootstrap_run_id, selected["canonical_product_key"], scenarios=scenarios,
                diagnostics=diagnostics, observed_at=observed_at,
                elapsed_seconds=elapsed, attempts=attempts,
            )
            return {"status": "success", "scenario_count": diagnostics["scenario_count"]}
        except QogitaBootstrapError as exc:
            if exc.retryable and attempts < max_attempts:
                client.metrics["retries"] += 1
                _sleep_for_retry(exc, attempts, sleep_func)
                continue
            phase_error = exc
            if exc.code.startswith("offers_parsing") or exc.code.startswith("offers_duplicate"):
                phase_error = QogitaBootstrapError(str(exc), code=exc.code, retryable=False)
            store.persist_failure(
                bootstrap_run_id, selected["canonical_product_key"], phase="offers",
                error=phase_error, attempts=attempts, elapsed_seconds=elapsed,
            )
            return {"status": "failed", "error_code": phase_error.code,
                    "http_status": phase_error.http_status}
    return {"status": "failed", "error_code": "unexpected_processing_exit"}


def run_qogita_bootstrap_concurrent(
    bootstrap_run_id: str, *, store: QogitaBootstrapStore,
    client_factory: Callable[[SharedQogitaAuth, SharedRateLimiter], QogitaBootstrapClient],
    base_url: str, email: str, password: str, workers: int = 1,
    max_products: int | None = None, claim_size: int = 1, lease_seconds: int = 300,
    checkpoint_every: int = 100, product_link_pacing: float = 0.6,
    offers_pacing: float = 1.0, max_attempts: int = 3,
    sleep_func: Callable[[float], None] = time.sleep,
    health_callback: Callable[[dict[str, Any]], str | None] | None = None,
    checkpoint_callback: Callable[[dict[str, Any]], None] | None = None,
):
    """Bounded persistent worker pool; default remains one worker, maximum two."""
    if workers not in {1, 2}:
        raise ValueError("Qogita bootstrap workers must be 1 or 2")
    run = store.bootstrap(bootstrap_run_id)
    if not run:
        raise ValueError("Bootstrap run not found")
    if run.get("run_mode") == "production" and offers_pacing < 1.15:
        raise ValueError("Production Qogita offers pacing cannot be below 1.15 seconds")
    store.requeue_expired_auth_failures(bootstrap_run_id)
    run = store.bootstrap(bootstrap_run_id)
    budget = int(max_products if max_products is not None else run["target_count"])
    if budget < 0:
        raise ValueError("max_products must be non-negative")
    limiter = SharedRateLimiter(
        product_link_interval=product_link_pacing, offers_interval=offers_pacing,
        sleep_func=sleep_func,
    )
    auth = SharedQogitaAuth(
        base_url=base_url, email=email, password=password,
    )
    clients: list[QogitaBootstrapClient] = []
    clients_lock = threading.Lock()
    budget_lock = threading.Lock()
    checkpoint_lock = threading.Lock()
    progress_lock = threading.Lock()
    stop_event = threading.Event()
    stop_reason = None
    processed = 0
    queue_wait_seconds = 0.0
    invocation_started = time.monotonic()
    baseline = {
        "product_link_requests": int(run.get("product_link_requests") or 0),
        "offers_requests": int(run.get("offers_requests") or 0),
        "retries": int(run.get("retry_count") or 0),
        "http_401": int(run.get("http_401_count") or 0),
        "auth_refreshes": int(run.get("auth_refresh_count") or 0),
        "http_429": int(run.get("rate_limit_count") or 0),
        "http_5xx": int(run.get("server_error_count") or 0),
        "resolver_elapsed_seconds": float(run.get("resolver_elapsed_seconds") or 0),
        "offers_elapsed_seconds": float(run.get("offers_elapsed_seconds") or 0),
    }

    def aggregate_metrics():
        result = dict(baseline)
        with clients_lock:
            snapshot = list(clients)
        for client in snapshot:
            for key in result:
                result[key] += client.metrics.get(key, 0)
        return result

    def checkpoint(batch_attempted: int):
        with checkpoint_lock:
            result = store.checkpoint_concurrent(
                bootstrap_run_id, metrics=aggregate_metrics(), worker_count=workers,
                batch_attempted=batch_attempted,
                wall_elapsed_seconds=float(run.get("wall_elapsed_seconds") or 0) +
                (time.monotonic() - invocation_started),
            )
            if checkpoint_callback:
                checkpoint_callback(result)
            return result

    def reserve() -> bool:
        nonlocal budget
        with budget_lock:
            if budget <= 0:
                return False
            budget -= 1
            return True

    def worker(worker_number: int):
        nonlocal processed, queue_wait_seconds, budget, stop_reason
        worker_id = f"worker-{worker_number}-{uuid4().hex[:8]}"
        client = client_factory(auth, limiter)
        with clients_lock:
            clients.append(client)
        try:
            while not stop_event.is_set() and reserve():
                claimed = store.claim_batch(
                    bootstrap_run_id, worker_id=worker_id,
                    limit=max(1, int(claim_size)), lease_seconds=lease_seconds,
                )
                if not claimed:
                    with budget_lock:
                        budget += 1
                    break
                for selected in claimed:
                    claimed_at = selected.get("claimed_at")
                    if claimed_at:
                        with progress_lock:
                            queue_wait_seconds += max(
                                0.0, (datetime.now(timezone.utc) - _as_datetime(claimed_at)).total_seconds()
                            )
                    outcome = _process_claimed_product(
                        selected, bootstrap_run_id=bootstrap_run_id,
                        staging_run_id=run["staging_run_id"], store=store, client=client,
                        max_attempts=max_attempts, sleep_func=sleep_func,
                    )
                    should_checkpoint = False
                    with progress_lock:
                        processed += 1
                        should_checkpoint = processed % max(1, checkpoint_every) == 0
                        health_payload = {
                            "processed": processed, "outcome": outcome,
                            "metrics": aggregate_metrics(),
                            "offers_pacing": limiter.intervals["offers"],
                            "product_link_pacing": limiter.intervals["product_link"],
                        }
                        reason = health_callback(health_payload) if health_callback else None
                        if reason and not stop_reason:
                            stop_reason = str(reason)
                            stop_event.set()
                    if should_checkpoint:
                        checkpoint(checkpoint_every)
                    if stop_event.is_set():
                        break
        finally:
            store.release_worker_claims(bootstrap_run_id, worker_id)
            client.close()

    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="qogita-bootstrap") as executor:
        futures = [executor.submit(worker, number + 1) for number in range(workers)]
        for future in futures:
            future.result()
    final = checkpoint(processed % max(1, checkpoint_every))
    if stop_reason:
        final = store.mark_stopped(
            bootstrap_run_id, stop_reason,
            health={"metrics": aggregate_metrics(), "processed": processed,
                    "offers_pacing": limiter.intervals["offers"]},
        )
    final["invocation_products_attempted"] = processed
    final["invocation_metrics"] = {
        key: aggregate_metrics()[key] - baseline[key] for key in baseline
    }
    final["global_rate_wait_seconds"] = dict(limiter.wait_seconds)
    final["effective_pacing"] = dict(limiter.intervals)
    final["auto_stop_reason"] = stop_reason
    final["queue_wait_seconds"] = queue_wait_seconds
    final["claim_summary"] = store.claim_summary(bootstrap_run_id)
    return final
