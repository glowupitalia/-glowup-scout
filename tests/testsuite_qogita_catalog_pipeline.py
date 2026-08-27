import hashlib
import hmac
import io
import json
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path
from contextlib import redirect_stdout
from wsgiref.util import setup_testing_defaults

import requests

from qogita_catalog_pipeline import (
    CatalogDownloadError,
    CatalogValidationError,
    QogitaCatalogDownloader,
    QogitaCatalogPipelineStore,
    QogitaCatalogRequestClient,
    WebhookAuthenticationError,
    WebhookPayloadError,
    download_pending_catalog,
    prepare_staging_generation,
    receive_qogita_webhook,
    sanitize_url,
    validate_qogita_catalog,
    verify_qogita_signature,
)
from qogita_webhook_server import create_qogita_webhook_app
from qogita_catalog_cli import main as qogita_cli_main
from supplier_catalog import SupplierCatalogStore
from supplier_catalog_collectors import QOGITA_EXPORT_COLUMNS, QogitaCatalogExportReader


SECRET = "test-signing-secret-never-persist"
REQUEST_ID = "8b063353-d4d6-4e87-a667-f64807a321c3"
NOW = 1_787_700_000
GTINS = ("8022297071411", "5010724533635", "5706710002871")


class FakeResponse:
    def __init__(self, status=200, *, content=b"", headers=None, payload=None):
        self.status_code = status
        self._content = content
        self.headers = dict(headers or {})
        self._payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")

    def iter_content(self, size):
        for index in range(0, len(self._content), max(1, size)):
            yield self._content[index:index + size]

    def json(self):
        return self._payload if self._payload is not None else json.loads(self._content)


class FakeSession:
    def __init__(self, get_handler=None, post_handler=None):
        self.get_handler = get_handler
        self.post_handler = post_handler

    def get(self, url, **kwargs):
        return self.get_handler(url)

    def post(self, url, **kwargs):
        return self.post_handler(url, kwargs)

    def close(self):
        pass


def signed(body, timestamp=NOW, secret=SECRET):
    digest = hmac.new(
        secret.encode(), str(timestamp).encode() + b"." + body, hashlib.sha256,
    ).hexdigest()
    return f"t={timestamp},v1={digest}"


def event_body(event_type="catalog_download.completed", *, request_id=REQUEST_ID,
               event_id="event-1", url="https://downloads.qogita.test/catalog.csv?sig=secret"):
    data = {"catalog_request_id": request_id}
    if event_type == "catalog_download.completed":
        data["download_url"] = url
    else:
        data["error_message"] = "generation failed"
    return json.dumps({
        "id": event_id, "type": event_type,
        "created_at": "2026-08-26T05:30:00.000000Z",
        "data": {"object": data},
    }).encode()


class QogitaCatalogPipelineTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.database = self.root / "catalog.sqlite3"
        self.pipeline = QogitaCatalogPipelineStore(self.database)
        self.catalog = SupplierCatalogStore(self.database)

    def tearDown(self):
        self.temporary.cleanup()

    def create_request(self, *, mode="full", filters=None, body=None):
        return self.pipeline.create_request(
            REQUEST_ID, request_mode=mode, filters=filters or {}, request_body=body or {},
        )

    def csv_file(self, *, rows=None, metadata=(), header=True, name="catalog.csv"):
        path = self.root / name
        lines = list(metadata)
        if header:
            lines.append(",".join(QOGITA_EXPORT_COLUMNS))
        rows = rows if rows is not None else [
            [GTINS[0], "Wax", "Wax", "Brand A", "6.74", "1", "22", "No", "", "2", "24", ""],
            [GTINS[1], "Shampoo", "Hair", "Brand B", "2.67", "1", "214", "No", "", "5", "594",
             "https://www.qogita.com/products/YKOV26/shampoo/"],
        ]
        lines.extend(",".join(map(str, row)) for row in rows)
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return path

    def receive(self, body=None, **kwargs):
        body = body or event_body()
        return receive_qogita_webhook(
            {"X-Qogita-Signature": signed(body)}, body, signing_secret=SECRET,
            store=self.pipeline, now_timestamp=NOW, **kwargs,
        )

    def attach_download(self, path, *, mode="full", filters=None):
        self.create_request(mode=mode, filters=filters)
        payload = path.read_bytes()
        self.pipeline.finish_download(REQUEST_ID, {
            "path": str(path), "file_size": len(payload),
            "checksum": hashlib.sha256(payload).hexdigest(),
        })

    def test_valid_signature(self):
        body = event_body()
        self.assertEqual(verify_qogita_signature(signed(body), body, SECRET, now_timestamp=NOW), NOW)

    def test_invalid_signature(self):
        with self.assertRaises(WebhookAuthenticationError):
            verify_qogita_signature("t=1787700000,v1=bad", b"{}", SECRET, now_timestamp=NOW)

    def test_expired_signature_timestamp(self):
        body = event_body()
        with self.assertRaisesRegex(WebhookAuthenticationError, "Expired"):
            verify_qogita_signature(signed(body, NOW - 301), body, SECRET, now_timestamp=NOW)

    def test_malformed_signature_and_payload(self):
        with self.assertRaises(WebhookAuthenticationError):
            verify_qogita_signature("wrong", b"{}", SECRET, now_timestamp=NOW)
        body = b"not-json"
        with self.assertRaises(WebhookPayloadError):
            receive_qogita_webhook(
                {"X-Qogita-Signature": signed(body)}, body, signing_secret=SECRET,
                store=self.pipeline, now_timestamp=NOW,
            )
        missing_url = json.dumps({
            "eventType": "catalog_download.completed",
            "data": {"catalogRequestId": REQUEST_ID},
        }).encode()
        with self.assertRaises(WebhookPayloadError):
            receive_qogita_webhook(
                {"X-Qogita-Signature": signed(missing_url)}, missing_url,
                signing_secret=SECRET, store=self.pipeline, now_timestamp=NOW,
            )

    def test_completed_event_correlates_request_and_redacts_url(self):
        self.create_request()
        result = self.receive()
        request = self.pipeline.request(REQUEST_ID)
        self.assertEqual(result["status"], "accepted")
        self.assertEqual(request["status"], "download_pending")
        self.assertEqual(request["download_url"], "https://downloads.qogita.test/catalog.csv")
        self.assertNotIn("sig=secret", json.dumps(request))

    def test_duplicate_event_is_idempotent(self):
        self.create_request()
        self.assertEqual(self.receive()["status"], "accepted")
        self.assertEqual(self.receive()["status"], "duplicate")
        with sqlite3.connect(self.database) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM qogita_webhook_events").fetchone()[0], 1)

    def test_official_webhook_test_is_accepted_without_catalog_side_effects(self):
        body = json.dumps({
            "id": "test-event-1", "eventType": "webhook.test",
            "data": {"message": "Webhook endpoint verification"},
        }).encode()
        result = receive_qogita_webhook(
            {"X-Qogita-Signature": signed(body)}, body,
            signing_secret=SECRET, store=self.pipeline, now_timestamp=NOW,
        )
        self.assertEqual(result, {
            "status": "accepted", "event_type": "webhook.test",
            "catalog_request_id": None,
        })
        with sqlite3.connect(self.database) as connection:
            self.pipeline.initialize()
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM qogita_webhook_events").fetchone()[0],
                0,
            )

    def test_failed_event_marks_request_failed(self):
        self.create_request()
        body = event_body("catalog_download.failed")
        result = receive_qogita_webhook(
            {"X-Qogita-Signature": signed(body)}, body, signing_secret=SECRET,
            store=self.pipeline, now_timestamp=NOW,
        )
        self.assertEqual(result["event_type"], "catalog_download.failed")
        self.assertEqual(self.pipeline.request(REQUEST_ID)["status"], "failed")
        self.assertEqual(self.pipeline.request(REQUEST_ID)["failure_reason"], "generation failed")

    def test_unknown_request_is_rejected_without_event(self):
        with self.assertRaisesRegex(Exception, "local catalog request"):
            self.receive()
        with sqlite3.connect(self.database) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM qogita_webhook_events").fetchone()[0], 0)

    def test_wsgi_receiver_returns_safe_statuses(self):
        self.create_request()
        body = event_body()
        current = int(time.time())
        environ = {"REQUEST_METHOD": "POST", "PATH_INFO": "/webhooks/qogita",
                   "CONTENT_LENGTH": str(len(body)), "wsgi.input": io.BytesIO(body),
                   "HTTP_X_QOGITA_SIGNATURE": signed(body, current)}
        setup_testing_defaults(environ)
        observed = {}
        response = b"".join(create_qogita_webhook_app(
            store=self.pipeline, signing_secret=SECRET,
        )(environ, lambda status, headers: observed.update(status=status, headers=headers)))
        self.assertEqual(observed["status"], "200 OK")
        self.assertEqual(json.loads(response)["status"], "accepted")

    def test_full_request_requires_explicit_empty_filters_and_body(self):
        self.create_request()
        self.assertEqual(self.pipeline.request(REQUEST_ID)["request_mode"], "full")
        with self.assertRaises(ValueError):
            self.pipeline.create_request("another", request_mode="full", filters={"mov": 500}, request_body={})

    def test_full_request_client_sends_exact_empty_body(self):
        observed = []
        def post(url, kwargs):
            observed.append((url, kwargs.get("json")))
            if url.endswith("auth/login/"):
                return FakeResponse(payload={"tokens": {"accessToken": "access"}})
            return FakeResponse(status=202, payload={"catalogRequestId": REQUEST_ID})
        client = QogitaCatalogRequestClient(
            base_url="https://api.qogita.test", email="account@example.test",
            password="not-persisted", client=FakeSession(post_handler=post),
        )
        self.assertEqual(client.request_full_catalog(), REQUEST_ID)
        self.assertEqual(observed[-1], (
            "https://api.qogita.test/public/buyers/catalog-downloads/", {},
        ))

    def test_live_cli_operations_are_gated_without_execute(self):
        output = io.StringIO()
        with redirect_stdout(output):
            self.assertEqual(qogita_cli_main([
                "--database", str(self.database), "request-full",
            ]), 0)
        self.assertFalse(json.loads(output.getvalue())["remote_call_performed"])
        output = io.StringIO()
        with redirect_stdout(output):
            self.assertEqual(qogita_cli_main([
                "--database", str(self.database), "rate-limit-probe", "FID1",
            ]), 0)
        self.assertFalse(json.loads(output.getvalue())["remote_call_performed"])

    def test_filtered_request_never_becomes_full(self):
        path = self.csv_file()
        summary = validate_qogita_catalog(path, request_mode="filtered", filters={"mov": 500})
        self.assertEqual(summary["status"], "accepted")
        self.assertFalse(summary["full_coverage_verified"])
        self.assertEqual(summary["product_catalog_coverage_type"], "filtered_catalog")

    def test_download_streams_checksum_and_restricts_permissions(self):
        content = self.csv_file().read_bytes()
        client = FakeSession(lambda _: FakeResponse(200, content=content))
        downloader = QogitaCatalogDownloader(
            self.root / "downloads", allowed_hosts={"downloads.qogita.test"}, client=client,
        )
        result = downloader.download(
            "https://downloads.qogita.test/catalog.csv?signature=hidden",
            catalog_request_id=REQUEST_ID,
        )
        self.assertEqual(result["checksum"], hashlib.sha256(content).hexdigest())
        self.assertEqual(Path(result["path"]).read_bytes(), content)
        self.assertEqual(Path(result["path"]).stat().st_mode & 0o777, 0o600)
        self.assertNotIn("signature", result["sanitized_url"])

    def test_download_rejects_unapproved_or_insecure_host(self):
        downloader = QogitaCatalogDownloader(self.root, allowed_hosts={"allowed.test"})
        with self.assertRaises(CatalogDownloadError):
            downloader.download("http://allowed.test/file.csv", catalog_request_id=REQUEST_ID)
        with self.assertRaises(CatalogDownloadError):
            downloader.download("https://other.test/file.csv", catalog_request_id=REQUEST_ID)

    def test_truncated_download_is_removed(self):
        client = FakeSession(lambda _: FakeResponse(
            200, headers={"content-length": "100"}, content=b"short",
        ))
        downloader = QogitaCatalogDownloader(
            self.root / "downloads", allowed_hosts={"downloads.qogita.test"}, client=client,
        )
        with self.assertRaisesRegex(CatalogDownloadError, "truncated"):
            downloader.download("https://downloads.qogita.test/file.csv", catalog_request_id=REQUEST_ID)
        self.assertEqual(list((self.root / "downloads").glob("*")), [])

    def test_timeout_marks_request_download_failed(self):
        self.create_request()
        self.receive()
        def timeout(_):
            raise requests.ReadTimeout("timeout")
        client = FakeSession(timeout)
        downloader = QogitaCatalogDownloader(
            self.root / "downloads", allowed_hosts={"downloads.qogita.test"}, client=client,
        )
        with self.assertLogs("qogita_catalog_pipeline", level="ERROR") as logs:
            with self.assertRaises(requests.ReadTimeout):
                download_pending_catalog(REQUEST_ID, store=self.pipeline, downloader=downloader)
        self.assertNotIn("sig=secret", "\n".join(logs.output))
        self.assertEqual(self.pipeline.request(REQUEST_ID)["status"], "download_failed")

    def test_correct_headers_and_full_provenance_are_accepted(self):
        summary = validate_qogita_catalog(self.csv_file(), request_mode="full", filters={})
        self.assertEqual(summary["row_count"], 2)
        self.assertEqual(summary["unique_gtin_count"], 2)
        self.assertTrue(summary["full_coverage_verified"])
        self.assertEqual(summary["product_catalog_coverage_type"], "full_account_catalog")

    def test_filtered_filename_blocks_full_claim(self):
        path = self.csv_file(metadata=["Original file,Filtered_Catalog_Download.xlsx"])
        summary = validate_qogita_catalog(path, request_mode="full", filters={})
        self.assertFalse(summary["full_coverage_verified"])

    def test_missing_headers_and_empty_file_are_rejected(self):
        with self.assertRaises(CatalogValidationError):
            validate_qogita_catalog(self.csv_file(header=False, rows=[]), request_mode="full", filters={})
        empty = self.root / "empty.csv"
        empty.write_bytes(b"")
        with self.assertRaises(CatalogValidationError):
            validate_qogita_catalog(empty, request_mode="full", filters={})

    def test_malformed_rows_and_low_gtin_ratio_are_rejected(self):
        rows = [
            ["invalid", "Bad", "Cat", "Brand", "1", "1", "1", "No", "", "1", "1", ""],
            [GTINS[0], "Good", "Cat", "Brand", "1", "1", "1", "No", "", "1", "1", ""],
        ]
        summary = validate_qogita_catalog(self.csv_file(rows=rows), request_mode="full", filters={})
        self.assertEqual(summary["status"], "rejected")
        self.assertIn("valid_gtin_ratio_too_low", summary["reasons"])

    def test_duplicate_gtin_is_rejected(self):
        row = [GTINS[0], "Wax", "Wax", "Brand", "1", "1", "1", "No", "", "1", "1", ""]
        summary = validate_qogita_catalog(self.csv_file(rows=[row, row]), request_mode="full", filters={})
        self.assertEqual(summary["duplicate_gtin_count"], 1)
        self.assertEqual(summary["status"], "rejected")

    def test_large_fixture_is_validated_streaming(self):
        path = self.root / "large.csv"
        with path.open("w", encoding="utf-8") as handle:
            handle.write(",".join(QOGITA_EXPORT_COLUMNS) + "\n")
            # Repeated valid GTINs would be rejected; product parsing remains a
            # single-pass iterator, while uniqueness is held in temporary SQLite.
            for index in range(2000):
                gtin = f"{index:012d}"
                # Compute an EAN-13 check digit for the synthetic prefix.
                total = sum(int(char) * (1 if pos % 2 == 0 else 3) for pos, char in enumerate(gtin))
                value = gtin + str((10 - total % 10) % 10)
                handle.write(f"{value},P{index},C,B,1,1,1,No,,1,1,\n")
        summary = validate_qogita_catalog(path, request_mode="full", filters={})
        self.assertEqual(summary["unique_gtin_count"], 2000)
        self.assertEqual(summary["status"], "accepted")

    def test_anomalous_row_drop_is_quarantined(self):
        summary = validate_qogita_catalog(
            self.csv_file(), request_mode="full", filters={}, previous_row_count=100,
        )
        self.assertEqual(summary["status"], "quarantined")
        self.assertIn("anomalous_row_drop", summary["reasons"])
        self.assertFalse(summary["full_coverage_verified"])

    def test_staging_never_promotes_generation(self):
        path = self.csv_file()
        self.attach_download(path)
        result = prepare_staging_generation(
            REQUEST_ID, pipeline_store=self.pipeline, catalog_store=self.catalog,
        )
        self.assertEqual(result["status"], "staged")
        self.assertFalse(result["promoted"])
        self.assertIsNone(self.catalog.active_generation_metadata("qogita"))
        self.assertEqual(self.pipeline.request(REQUEST_ID)["staging_run_id"], result["staging_run_id"])

    def test_rejected_validation_does_not_create_staging_generation(self):
        row = [GTINS[0], "Wax", "Wax", "Brand", "1", "1", "1", "No", "", "1", "1", ""]
        path = self.csv_file(rows=[row, row])
        self.attach_download(path)
        result = prepare_staging_generation(
            REQUEST_ID, pipeline_store=self.pipeline, catalog_store=self.catalog,
        )
        self.assertEqual(result["status"], "rejected")
        self.assertIsNone(result["staging_run_id"])

    def test_staging_delta_and_queue_prioritize_new_and_unresolved(self):
        path = self.csv_file()
        self.attach_download(path)
        result = prepare_staging_generation(
            REQUEST_ID, pipeline_store=self.pipeline, catalog_store=self.catalog,
        )
        queue = self.pipeline.enrichment_queue(result["staging_run_id"])
        self.assertEqual(result["delta"]["new"], 2)
        self.assertEqual(len(queue), 2)
        self.assertEqual(queue[0]["priority"], 100)
        self.assertEqual({row["task_type"] for row in queue}, {"resolve_variant", "offers_enrichment"})

    def test_rate_limit_probe_is_bounded_and_reports_offers_without_payloads(self):
        responses = [FakeResponse(
            status=200, headers={"content-type": "application/json", "x-ratelimit-limit": "2"},
            payload={"offers": [{"tieredPrices": [{}, {}]}]},
        )]
        client = QogitaCatalogRequestClient(
            base_url="https://api.qogita.test", email="x", password="y",
            client=FakeSession(get_handler=lambda _: responses[0]),
        )
        client.access_token = "access"
        result = client.rate_limit_probe(["FID1"], pacing_seconds=0)
        self.assertEqual(result[0]["offer_count"], 1)
        self.assertEqual(result[0]["tier_count"], 2)
        self.assertNotIn("payload", result[0])
        with self.assertRaises(ValueError):
            client.rate_limit_probe([str(index) for index in range(51)])

    def test_queue_orders_retryable_failure_before_old_carried_forward(self):
        path = self.csv_file()
        self.attach_download(path)
        staged = prepare_staging_generation(
            REQUEST_ID, pipeline_store=self.pipeline, catalog_store=self.catalog,
        )
        run_id = staged["staging_run_id"]
        with sqlite3.connect(self.database) as connection:
            keys = [row[0] for row in connection.execute(
                "SELECT canonical_product_key FROM supplier_catalog_products WHERE run_id=? ORDER BY canonical_product_key",
                (run_id,),
            )]
            connection.execute("DELETE FROM qogita_enrichment_queue WHERE run_id=?", (run_id,))
            connection.execute(
                """UPDATE supplier_catalog_products SET catalog_delta_status='unchanged',
                   enrichment_status='enrichment_failed' WHERE run_id=? AND canonical_product_key=?""",
                (run_id, keys[0]),
            )
            connection.execute(
                """UPDATE supplier_catalog_products SET catalog_delta_status='unchanged',
                   enrichment_status='carried_forward',offer_tier_observed_at='2026-01-01T00:00:00Z'
                   WHERE run_id=? AND canonical_product_key=?""",
                (run_id, keys[1]),
            )
        self.pipeline.populate_enrichment_queue(run_id)
        queue = self.pipeline.enrichment_queue(run_id)
        self.assertEqual([row["priority"] for row in queue], [80, 60])
        self.assertEqual(queue[0]["reason"], "retryable_enrichment_failure")
        self.assertEqual(queue[1]["source_observed_at"], "2026-01-01T00:00:00Z")

    def test_latest_success_remains_untouched_by_new_staging(self):
        first = self.csv_file(name="first.csv")
        reader = QogitaCatalogExportReader(first)
        active_run = self.catalog.start_run(
            "qogita", coverage_type="partial_catalog", coverage_description="active",
            coverage_complete=False, sampled=False,
        )
        self.catalog.publish_product_catalog_stream(
            active_run, supplier="qogita", products=reader.products(), elapsed_seconds=0,
            product_catalog_coverage_type="partial_catalog",
            product_catalog_coverage_complete=False, promote=True,
        )
        changed = self.csv_file(rows=[
            [GTINS[0], "Wax", "Wax", "Brand A", "7.00", "1", "22", "No", "", "2", "24", ""],
            [GTINS[2], "New", "Cat", "Brand", "1", "1", "1", "No", "", "1", "1", ""],
        ], name="changed.csv")
        self.attach_download(changed)
        result = prepare_staging_generation(
            REQUEST_ID, pipeline_store=self.pipeline, catalog_store=self.catalog,
        )
        self.assertEqual(result["delta"], {"new": 1, "changed": 1, "removed": 1, "unchanged": 0})
        queue = self.pipeline.enrichment_queue(result["staging_run_id"])
        self.assertEqual({row["reason"] for row in queue}, {"new_product", "catalog_changed"})
        self.assertEqual(self.catalog.active_generation_metadata("qogita")["run_id"], active_run)

    def test_staging_can_compare_explicit_unpromoted_generation(self):
        first = self.csv_file(name="first.csv")
        self.attach_download(first)
        first_result = prepare_staging_generation(
            REQUEST_ID, pipeline_store=self.pipeline, catalog_store=self.catalog,
        )
        second_request_id = "ca7a820b-f4cb-4739-8e0a-198159ba1f31"
        second = self.csv_file(name="second.csv")
        payload = second.read_bytes()
        self.pipeline.create_request(
            second_request_id, request_mode="full", filters={}, request_body={},
        )
        self.pipeline.finish_download(second_request_id, {
            "path": str(second), "file_size": len(payload),
            "checksum": hashlib.sha256(payload).hexdigest(),
        })
        second_result = prepare_staging_generation(
            second_request_id, pipeline_store=self.pipeline, catalog_store=self.catalog,
            previous_run_id=first_result["staging_run_id"],
        )
        self.assertEqual(second_result["delta"], {
            "new": 0, "changed": 0, "removed": 0, "unchanged": 2,
        })
        self.assertIsNone(self.catalog.active_generation_metadata("qogita"))

    def test_modified_download_is_rejected_before_staging(self):
        path = self.csv_file()
        self.attach_download(path)
        path.write_text(path.read_text() + "tampered\n", encoding="utf-8")
        with self.assertRaisesRegex(CatalogValidationError, "checksum"):
            prepare_staging_generation(
                REQUEST_ID, pipeline_store=self.pipeline, catalog_store=self.catalog,
            )

    def test_secret_and_signed_url_are_not_serialized_in_events(self):
        self.create_request()
        self.receive()
        database_bytes = self.database.read_bytes()
        self.assertNotIn(SECRET.encode(), database_bytes)
        with sqlite3.connect(self.database) as connection:
            columns = [row[1] for row in connection.execute("PRAGMA table_info(qogita_webhook_events)")]
        self.assertNotIn("payload", columns)
        self.assertEqual(sanitize_url("https://host/file?signature=secret"), "https://host/file")


if __name__ == "__main__":
    unittest.main()
