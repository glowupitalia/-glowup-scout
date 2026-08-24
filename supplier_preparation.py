"""Supplier-neutral cache preparation for Discovery.

The module deliberately keeps refresh mechanics outside Streamlit.  Manager
cache readers are read-only; refreshes, when needed, use only the official
Manager scripts and their existing locks.
"""

from __future__ import annotations

import json
import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

from abw_discovery import load_abw_cache_rows, normalize_abw_candidates
from purchase_scenarios import merge_product_candidates
from qogita_discovery import load_qogita_cache_rows, normalize_qogita_candidates
from qogita_refresh import (
    inspect_qogita_cache, refresh_qogita_seller_catalogs, snapshots_advanced,
)
from qudo_discovery import load_qudo_cache_rows, normalize_qudo_candidates
from umma_discovery import load_umma_cache_rows, normalize_umma_candidates


SUPPORTED_SUPPLIERS = ("qogita", "umma", "abw", "qudo")
SUPPLIER_TTL_HOURS = {"qogita": 24, "umma": 48, "abw": 48, "qudo": 48}
MANAGER_REFRESH_SCRIPTS = {
    "umma": "sync_umma_purchase_prices.py",
    "abw": "sync_abw_purchase_prices.py",
    "qudo": "sync_qudo_purchase_prices.py",
}
COVERAGE = {
    "qogita": (
        "qogita_seller_catalog",
        "Catalogo seller Qogita persistito per gli alias configurati",
    ),
    "umma": (
        "manager_tracked_products",
        "Prodotti UMMA attivi/tracciati dal Manager; Europe Direct solo se persistita",
    ),
    "abw": (
        "manager_tracked_products",
        "Prodotti ABW attivi/tracciati dal Manager",
    ),
    "qudo": (
        "manager_tracked_products",
        "Prodotti Qudo attivi/tracciati dal Manager",
    ),
}


def normalize_selected_suppliers(values):
    selected = []
    for value in values or []:
        supplier = str(value or "").strip().casefold()
        if supplier in SUPPORTED_SUPPLIERS and supplier not in selected:
            selected.append(supplier)
    return selected


def _parse_timestamp(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(
            timezone.utc
        )
    except (TypeError, ValueError):
        return None


def _row_snapshot(row):
    return str(row.get("run_id") or row.get("generation_id") or row.get("snapshot_id") or "")


def inspect_supplier_rows(supplier, rows, *, now=None):
    """Return freshness metadata without interpreting commercial eligibility."""
    now = now or datetime.now(timezone.utc)
    if supplier == "qogita":
        inspected = inspect_qogita_cache(rows, now=now)
        snapshots = inspected.get("snapshots") or {}
        latest = max((str(value) for value in snapshots.values()), default=None)
        return {
            "fresh": bool(inspected.get("fresh")),
            "snapshot_id": snapshots,
            "snapshot_at": latest,
            "row_count": len(rows or []),
            "seller_aliases": list(inspected.get("seller_aliases") or []),
        }
    timestamps = [
        _parse_timestamp(row.get("observed_at") or row.get("snapshot_at"))
        for row in (rows or [])
    ]
    timestamps = [value for value in timestamps if value is not None]
    latest = max(timestamps, default=None)
    ttl = SUPPLIER_TTL_HOURS[supplier]
    fresh = bool(latest and (now - latest).total_seconds() <= ttl * 3600)
    return {
        "fresh": fresh,
        "snapshot_id": sorted({_row_snapshot(row) for row in (rows or []) if _row_snapshot(row)}),
        "snapshot_at": latest.isoformat().replace("+00:00", "Z") if latest else None,
        "row_count": len(rows or []),
        "seller_aliases": [],
    }


def _manager_paths(manager_root=None):
    root = Path(
        manager_root or Path(__file__).resolve().parent.parent / "Glow-Up-Manager"
    ).resolve()
    source = root / "src"
    if not root.is_dir():
        raise FileNotFoundError(f"Repository Manager non trovato: {root}")
    if not source.is_dir():
        raise FileNotFoundError(f"Manager src non trovato: {source}")
    python = root / ".venv" / "bin" / "python"
    if not python.is_file():
        raise FileNotFoundError(f"Python Manager non trovato: {python}")
    return root, source, python


def refresh_manager_supplier(supplier, *, manager_root=None, timeout=3600):
    """Run one official Manager refresh process with its existing lock."""
    if supplier not in MANAGER_REFRESH_SCRIPTS:
        raise ValueError(f"Refresh non supportato: {supplier}")
    root, source, python = _manager_paths(manager_root)
    script = root / "scripts" / MANAGER_REFRESH_SCRIPTS[supplier]
    if not script.is_file():
        raise FileNotFoundError(f"Script refresh non trovato: {script}")
    environment = os.environ.copy()
    previous = environment.get("PYTHONPATH")
    environment["PYTHONPATH"] = (
        f"{source}{os.pathsep}{previous}" if previous else str(source)
    )
    started = time.monotonic()
    completed = subprocess.run(
        [str(python), str(script)], cwd=str(root), env=environment,
        capture_output=True, text=True, check=False, timeout=timeout,
    )
    payload = {}
    for line in reversed((completed.stdout or "").splitlines()):
        try:
            payload = json.loads(line)
            break
        except (TypeError, ValueError):
            continue
    status = str(payload.get("status") or "").casefold()
    locked = status in {"skipped", "already_running"}
    success = completed.returncode == 0 and status in {"success", "partial_success"}
    return {
        "status": "success" if success else ("already_running" if locked else "failed"),
        "manager_status": status or None,
        "duration_seconds": time.monotonic() - started,
        "error_code": None if success else (
            "already_running" if locked else f"refresh_exit_{completed.returncode}"
        ),
    }


def _default_components():
    return {
        "qogita": (load_qogita_cache_rows, normalize_qogita_candidates),
        "umma": (load_umma_cache_rows, normalize_umma_candidates),
        "abw": (load_abw_cache_rows, normalize_abw_candidates),
        "qudo": (load_qudo_cache_rows, normalize_qudo_candidates),
    }


def prepare_suppliers(
    selected_suppliers, filters, *, now=None, components=None, refreshers=None,
    progress=None,
):
    """Prepare an immutable supplier snapshot set and union its candidates."""
    selected = normalize_selected_suppliers(selected_suppliers)
    if not selected:
        raise ValueError("Seleziona almeno un fornitore")
    now = now or datetime.now(timezone.utc)
    components = {**_default_components(), **(components or {})}
    refreshers = dict(refreshers or {})
    collections = []
    statuses = {}
    diagnostics = {}
    warnings = []
    for supplier in selected:
        if progress:
            progress("supplier_checking", supplier)
        loader, normalizer = components[supplier]
        coverage_type, coverage_description = COVERAGE[supplier]
        started = time.monotonic()
        try:
            rows = loader()
            before = inspect_supplier_rows(supplier, rows, now=now)
            refresh_status = "cache_fresh"
            refresh_result = None
            if not before["fresh"]:
                refresh_status = "refresh_required"
                if progress:
                    progress("supplier_refreshing", supplier)
                refresher = refreshers.get(supplier)
                if refresher is None:
                    refresher = (
                        (lambda: refresh_qogita_seller_catalogs(before["seller_aliases"]))
                        if supplier == "qogita" else
                        (lambda supplier=supplier: refresh_manager_supplier(supplier))
                    )
                refresh_result = refresher()
                if refresh_result.get("status") != "success":
                    raise RuntimeError(refresh_result.get("error_code") or "refresh_failed")
                rows = loader()
                after = inspect_supplier_rows(supplier, rows, now=now)
                if not after["fresh"]:
                    raise RuntimeError("refreshed_cache_not_fresh")
                if supplier == "qogita" and (
                    set(after["seller_aliases"]) != set(before["seller_aliases"])
                    or not snapshots_advanced(
                        before["snapshot_id"], after["snapshot_id"],
                        before["seller_aliases"],
                    )
                ):
                    raise RuntimeError("refreshed_cache_not_advanced")
                refresh_status = "refreshed"
            else:
                after = before
            kwargs = {"now": now}
            if supplier == "qogita":
                kwargs["minimum_stock"] = filters.get("minimum_qogita_stock", 1)
            candidates, supplier_diagnostics = normalizer(rows, **kwargs)
            scenario_count = sum(len(row.get("scenarios") or []) for row in candidates)
            availability = "available" if candidates else "empty"
            if not candidates:
                warnings.append(f"{supplier.upper()}: nessun prodotto utilizzabile")
            else:
                collections.append(candidates)
            statuses[supplier] = {
                "supplier": supplier,
                "snapshot_id": after["snapshot_id"],
                "snapshot_at": after["snapshot_at"],
                "freshness": "fresh",
                "refresh_status": refresh_status,
                "refresh_duration_seconds": float((refresh_result or {}).get("duration_seconds") or 0),
                "products_count": len(candidates),
                "scenarios_count": scenario_count,
                "availability_status": availability,
                "coverage_type": coverage_type,
                "coverage_description": coverage_description,
            }
            diagnostics[supplier] = supplier_diagnostics
            if progress:
                progress("supplier_ready", supplier)
        except Exception as exc:
            statuses[supplier] = {
                "supplier": supplier,
                "snapshot_id": None,
                "snapshot_at": None,
                "freshness": "unavailable",
                "refresh_status": "refresh_failed",
                "refresh_duration_seconds": time.monotonic() - started,
                "products_count": 0,
                "scenarios_count": 0,
                "availability_status": "unavailable",
                "coverage_type": coverage_type,
                "coverage_description": coverage_description,
                "error": str(exc)[:200],
            }
            diagnostics[supplier] = {"error": str(exc)[:200]}
            warnings.append(f"{supplier.upper()} non disponibile per questa ricerca")
            if progress:
                progress("supplier_unavailable", supplier)
    candidates = merge_product_candidates(*collections)
    supplier_eans = {
        supplier: {
            row.get("canonical_ean")
            for row in candidates
            if supplier in (row.get("suppliers") or [])
        }
        for supplier in selected
    }
    unique_eans = {ean for values in supplier_eans.values() for ean in values if ean}
    shared_eans = sum(
        sum(ean in values for values in supplier_eans.values()) > 1
        for ean in unique_eans
    )
    return {
        "selected_suppliers": selected,
        "supplier_snapshot_set": statuses,
        "supplier_diagnostics": diagnostics,
        "supplier_warnings": warnings,
        "candidates": candidates,
        "coverage": {
            "unique_eans": len(unique_eans),
            "shared_eans": shared_eans,
            "products_by_supplier": {
                supplier: len(values) for supplier, values in supplier_eans.items()
            },
            "scenarios_by_supplier": {
                supplier: statuses[supplier]["scenarios_count"] for supplier in selected
            },
        },
        "usable_suppliers": [
            supplier for supplier in selected
            if statuses[supplier]["availability_status"] == "available"
        ],
    }
