#!/usr/bin/env python3
"""Dedicated WSGI receiver for signed Qogita catalog webhooks."""

from __future__ import annotations

import json
import logging
import os
from wsgiref.simple_server import make_server

from qogita_catalog_pipeline import (
    QogitaCatalogPipelineStore,
    QogitaPipelineError,
    UnknownCatalogRequest,
    WebhookAuthenticationError,
    WebhookPayloadError,
    receive_qogita_webhook,
)


MAX_WEBHOOK_BODY_BYTES = 1024 * 1024
LOGGER = logging.getLogger(__name__)


def create_qogita_webhook_app(*, store=None, signing_secret=None):
    pipeline_store = store or QogitaCatalogPipelineStore()
    secret = signing_secret if signing_secret is not None else os.environ.get(
        "QOGITA_WEBHOOK_SIGNING_SECRET"
    )

    def application(environ, start_response):
        if environ.get("PATH_INFO") != "/webhooks/qogita":
            start_response("404 Not Found", [("Content-Type", "application/json")])
            return [b'{"error":"not_found"}']
        if environ.get("REQUEST_METHOD") != "POST":
            start_response("405 Method Not Allowed", [
                ("Content-Type", "application/json"), ("Allow", "POST"),
            ])
            return [b'{"error":"method_not_allowed"}']
        try:
            length = int(environ.get("CONTENT_LENGTH") or "0")
        except ValueError:
            length = 0
        if length <= 0 or length > MAX_WEBHOOK_BODY_BYTES:
            start_response("400 Bad Request", [("Content-Type", "application/json")])
            return [b'{"error":"invalid_body_size"}']
        raw_body = environ["wsgi.input"].read(length)
        headers = {
            "X-Qogita-Signature": environ.get("HTTP_X_QOGITA_SIGNATURE", ""),
        }
        try:
            result = receive_qogita_webhook(
                headers, raw_body, signing_secret=secret or "", store=pipeline_store,
            )
            status = "200 OK"
            response = {"status": result["status"]}
        except WebhookAuthenticationError:
            status, response = "401 Unauthorized", {"error": "invalid_signature"}
        except WebhookPayloadError as exc:
            try:
                rejected = json.loads(raw_body.decode("utf-8"))
                payload_shape = {
                    "top_level_keys": sorted(rejected) if isinstance(rejected, dict) else [],
                    "data_keys": sorted(rejected.get("data", {}))
                    if isinstance(rejected, dict) and isinstance(rejected.get("data"), dict)
                    else [],
                }
            except (UnicodeDecodeError, json.JSONDecodeError):
                payload_shape = {"top_level_keys": [], "data_keys": []}
            LOGGER.warning(
                "Rejected Qogita webhook payload: %s shape=%s", exc, payload_shape,
            )
            status, response = "400 Bad Request", {"error": "invalid_payload"}
        except UnknownCatalogRequest:
            status, response = "404 Not Found", {"error": "unknown_catalog_request"}
        except QogitaPipelineError:
            status, response = "422 Unprocessable Entity", {"error": "webhook_rejected"}
        payload = json.dumps(response, separators=(",", ":")).encode("utf-8")
        start_response(status, [
            ("Content-Type", "application/json"),
            ("Content-Length", str(len(payload))),
            ("Cache-Control", "no-store"),
        ])
        return [payload]

    return application


def main():
    host = os.environ.get("QOGITA_WEBHOOK_BIND", "127.0.0.1")
    port = int(os.environ.get("QOGITA_WEBHOOK_PORT", "8511"))
    if not os.environ.get("QOGITA_WEBHOOK_SIGNING_SECRET"):
        raise SystemExit("QOGITA_WEBHOOK_SIGNING_SECRET is required")
    with make_server(host, port, create_qogita_webhook_app()) as server:
        server.serve_forever()


if __name__ == "__main__":
    main()
