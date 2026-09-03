"""Read-only bridge to the canonical Amazon projection owned by Manager."""

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import quote

import requests


DEFAULT_MANAGER_URL = "http://127.0.0.1:8000"
DEFAULT_MANAGER_ENV = Path(__file__).resolve().parent.parent / "Glow-Up-Manager" / ".env"


def _manager_token():
    token = os.environ.get("GLOWUP_API_TOKEN")
    if token:
        return token
    try:
        lines = DEFAULT_MANAGER_ENV.read_text(encoding="utf-8").splitlines()
    except OSError:
        return None
    for line in lines:
        key, separator, value = line.strip().partition("=")
        if separator and key.removeprefix("export ").strip() == "GLOWUP_API_TOKEN":
            return value.strip().strip("'\"") or None
    return None


class ManagerCanonicalClient:
    def __init__(self, *, base_url=None, token_loader=_manager_token, request_get=requests.get):
        self.base_url = str(base_url or os.environ.get("GLOWUP_MANAGER_API_URL") or DEFAULT_MANAGER_URL).rstrip("/")
        self.token_loader = token_loader
        self.request_get = request_get

    def lookup(self, ean):
        token = self.token_loader()
        if not token:
            raise RuntimeError("Manager API token unavailable")
        response = self.request_get(
            f"{self.base_url}/api/mobile/amazon/products/by-ean/{quote(str(ean), safe='')}",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
            timeout=5,
        )
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()


__all__ = ["ManagerCanonicalClient"]
