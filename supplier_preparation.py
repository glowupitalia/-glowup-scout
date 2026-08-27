"""Supplier-neutral cache preparation for Discovery.

The production path reads only Scout-owned, atomically promoted supplier
catalog generations.  Legacy Manager components remain injectable solely for
offline regression fixtures; they are never a runtime fallback.
"""

from __future__ import annotations

import json
import hashlib
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
from supplier_catalog import SupplierCatalogStore, canonical_gtin14
from discovery_rotation import DiscoveryRotationStore, ROTATION_STRATEGY
from supplier_weekly import WeeklySupplierStore, next_weekly_refresh


SUPPORTED_SUPPLIERS = ("qogita", "umma", "abw", "qudo")
DISCOVERY_BUDGET_OPTIONS = (250, 500, 1000, 2500, 5000)
DEFAULT_DISCOVERY_RUN_BUDGET = 500
DISCOVERY_SAMPLING_STRATEGY = ROTATION_STRATEGY
SUPPLIER_TTL_HOURS = {"qogita": 24, "umma": 48, "abw": 48, "qudo": 48}
SUPPLIER_FIRST_REFRESH_HOURS = {"qogita": 24, "umma": 168, "abw": 168, "qudo": 168}
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


def normalize_run_budget(value):
    if value is None or str(value).strip().casefold() in {"all", "tutto", "tutto il catalogo"}:
        return None
    if isinstance(value, bool):
        raise ValueError("Il budget Discovery deve essere un numero positivo o Tutto il catalogo")
    budget = int(value)
    if budget <= 0:
        raise ValueError("Il budget Discovery deve essere positivo")
    return budget


def _candidate_membership(candidate, selected):
    scenario_suppliers = {
        str(row.get("supplier") or "").casefold()
        for row in candidate.get("scenarios") or []
    }
    return tuple(supplier for supplier in selected if supplier in scenario_suppliers)


def sample_discovery_candidates(candidates, selected_suppliers, run_budget):
    """Deterministic supplier-membership stratified sample with no economic signal."""
    selected = normalize_selected_suppliers(selected_suppliers)
    budget = normalize_run_budget(run_budget)
    eligible = [
        row for row in candidates
        if canonical_gtin14(row.get("canonical_ean")) is not None
    ]
    metadata = {
        "total_supplier_ean_universe": len(candidates),
        "eligible_identifier_count": len(eligible),
        "run_budget": "all" if budget is None else budget,
        "sampling_strategy": DISCOVERY_SAMPLING_STRATEGY,
    }
    if budget is None or budget >= len(eligible):
        metadata["sampled_identifier_count"] = len(eligible)
        return eligible, metadata

    strata = {}
    for candidate in eligible:
        membership = _candidate_membership(candidate, selected) or ("unknown",)
        strata.setdefault(membership, []).append(candidate)
    for membership, rows in strata.items():
        rows.sort(key=lambda row: (
            hashlib.sha256(
                ("|".join(membership) + ":" + str(row.get("canonical_ean"))).encode("utf-8")
            ).hexdigest(),
            str(row.get("canonical_ean")),
        ))

    sizes = {key: len(rows) for key, rows in strata.items()}
    total = sum(sizes.values())
    quotas = {key: 0 for key in strata}
    if budget >= len(strata):
        quotas = {key: 1 for key in strata}
    remaining = budget - sum(quotas.values())
    if remaining > 0:
        capacity = {key: sizes[key] - quotas[key] for key in strata}
        capacity_total = sum(capacity.values())
        exact = {
            key: (remaining * capacity[key] / capacity_total if capacity_total else 0)
            for key in strata
        }
        for key in strata:
            add = min(capacity[key], int(exact[key]))
            quotas[key] += add
        left = budget - sum(quotas.values())
        order = sorted(
            strata,
            key=lambda key: (-(exact[key] - int(exact[key])), "|".join(key)),
        )
        for key in order:
            if not left:
                break
            if quotas[key] < sizes[key]:
                quotas[key] += 1
                left -= 1

    sampled = [row for key in sorted(strata) for row in strata[key][:quotas[key]]]
    sampled.sort(key=lambda row: str(row.get("canonical_ean")))
    metadata["sampled_identifier_count"] = len(sampled)
    return sampled, metadata


def _apply_run_budget(
    prepared, selected, run_budget, *, rotation_store=None, rotation_job_id=None,
):
    universe_candidates = prepared.get("candidates") or []
    if rotation_store is not None:
        candidates, rotation_metadata = rotation_store.select(
            rotation_job_id, universe_candidates, selected,
            normalize_run_budget(run_budget),
            supplier_snapshot_set=prepared.get("supplier_snapshot_set") or {},
        )
        metadata = {
            "total_supplier_ean_universe": len(universe_candidates),
            "eligible_identifier_count": sum(
                canonical_gtin14(row.get("canonical_ean")) is not None
                for row in universe_candidates
            ),
            **rotation_metadata,
        }
    else:
        candidates, metadata = sample_discovery_candidates(
            universe_candidates, selected, run_budget,
        )
    coverage = dict(prepared.get("coverage") or {})
    coverage.update(metadata)
    coverage.setdefault("universe_products_by_supplier", coverage.get("products_by_supplier") or {})
    coverage.setdefault("universe_scenarios_by_supplier", coverage.get("scenarios_by_supplier") or {})
    coverage["unique_eans"] = len(candidates)
    coverage["shared_eans"] = sum(
        len(_candidate_membership(row, selected)) > 1 for row in candidates
    )
    coverage["products_by_supplier"] = {
        supplier: sum(supplier in _candidate_membership(row, selected) for row in candidates)
        for supplier in selected
    }
    coverage["scenarios_by_supplier"] = {
        supplier: sum(
            str(scenario.get("supplier") or "").casefold() == supplier
            for row in candidates for scenario in row.get("scenarios") or []
        )
        for supplier in selected
    }
    return {**prepared, **metadata, "candidates": candidates, "coverage": coverage}


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


def _prepare_injected_components(
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


def _active_supplier_status(supplier, generation, candidates, *, now):
    completed_at = _parse_timestamp(generation.get("completed_at"))
    age_hours = (
        (now - completed_at).total_seconds() / 3600
        if completed_at is not None else None
    )
    products = generation.get("products") or []
    diagnostics = generation.get("diagnostics") or {}
    valid_identifiers = sum(bool(row.get("canonical_gtin")) for row in products)
    scenario_count = len(generation.get("scenarios") or [])
    weekly_state = None
    if supplier in {"abw", "umma", "qudo"}:
        try:
            weekly_state = WeeklySupplierStore().latest_supplier_state(supplier)
        except Exception:
            weekly_state = None
    return {
        "supplier": supplier,
        "snapshot_id": generation["run_id"],
        "snapshot_at": generation.get("completed_at"),
        "freshness": (
            "fresh" if age_hours is not None
            and age_hours <= SUPPLIER_FIRST_REFRESH_HOURS[supplier] else "stale"
        ),
        "refresh_status": "supplier_catalog_latest_success",
        "refresh_duration_seconds": 0.0,
        "products_count": len(candidates),
        "catalog_products_count": int(
            generation.get("product_catalog_count") or len(products)
        ),
        "identifiers_count": valid_identifiers,
        "identifier_unresolved_count": len(products) - valid_identifiers,
        "scenarios_count": scenario_count,
        "availability_status": "available" if candidates else "empty",
        "coverage_type": generation.get("product_catalog_coverage_type")
        or generation.get("coverage_type"),
        "coverage_description": generation.get("coverage_description"),
        "coverage_complete": bool(
            generation.get("product_catalog_coverage_complete")
        ),
        "scenario_enrichment_status": generation.get("scenario_enrichment_status"),
        "scenario_enrichment_count": generation.get("scenario_enrichment_count"),
        "usable_identifier_count": generation.get("usable_identifier_count"),
        "coverage_percent": generation.get("coverage_percent"),
        "bootstrap_state": generation.get("bootstrap_state"),
        "bootstrap_window_number": generation.get("bootstrap_window_number"),
        "serving_generation_id": generation.get("serving_generation_id"),
        "source_count": generation.get("source_count"),
        "enumerated_count": generation.get("enumerated_count"),
        "age_hours": age_hours,
        "next_refresh_at": (
            next_weekly_refresh(now).isoformat()
            if supplier in {"abw", "umma", "qudo"} else None
        ),
        "last_sync_status": (weekly_state or {}).get("status"),
        "last_sync_error": (weekly_state or {}).get("error_message"),
        "coverage_diagnostics": {
            key: diagnostics.get(key) for key in (
                "search_total_count", "enumeration_gap", "qudo_offer_products",
                "invalid_identifier_count", "identifier_valid_count",
            ) if diagnostics.get(key) is not None
        },
    }


def _prepare_from_supplier_catalog(selected, *, now, progress, store):
    collections = []
    statuses = {}
    diagnostics = {}
    warnings = []
    for supplier in selected:
        if progress:
            progress("supplier_checking", supplier)
        generation = (
            store.latest_serving(supplier) if supplier == "qogita"
            else store.latest_success(supplier)
        )
        if not generation:
            message = (
                "Bootstrap scenari in corso; nessuno snapshot serving disponibile"
                if supplier == "qogita" else
                "Nessuna baseline supplier-first promossa"
            )
            statuses[supplier] = {
                "supplier": supplier, "snapshot_id": None, "snapshot_at": None,
                "freshness": "unavailable", "refresh_status": "baseline_missing",
                "refresh_duration_seconds": 0.0, "products_count": 0,
                "catalog_products_count": 0, "identifiers_count": 0,
                "identifier_unresolved_count": 0, "scenarios_count": 0,
                "availability_status": "unavailable", "coverage_type": None,
                "coverage_description": message, "coverage_complete": False,
                "scenario_enrichment_status": "none", "error": message,
            }
            diagnostics[supplier] = {"error": "active_generation_missing"}
            warnings.append(f"{supplier.upper()}: {message}")
            if progress:
                progress("supplier_unavailable", supplier)
            continue
        candidates = store.latest_candidates(supplier)
        status = _active_supplier_status(supplier, generation, candidates, now=now)
        statuses[supplier] = status
        diagnostics[supplier] = generation.get("diagnostics") or {}
        if status["freshness"] != "fresh":
            status["warning"] = "Baseline supplier-first oltre la cadenza settimanale"
            warnings.append(
                f"{supplier.upper()}: baseline non recente; ultima baseline valida preservata"
            )
        if not candidates:
            warnings.append(f"{supplier.upper()}: nessuno scenario utilizzabile")
        else:
            collections.append(candidates)
            if progress:
                progress("supplier_ready", supplier)
    candidates = merge_product_candidates(*collections)
    supplier_eans = {
        supplier: {
            row.get("canonical_ean") for row in candidates
            if supplier in (row.get("suppliers") or []) and row.get("canonical_ean")
        }
        for supplier in selected
    }
    unique_eans = {ean for values in supplier_eans.values() for ean in values}
    return {
        "selected_suppliers": selected,
        "supplier_snapshot_set": statuses,
        "supplier_diagnostics": diagnostics,
        "supplier_warnings": warnings,
        "candidates": candidates,
        "coverage": {
            "unique_eans": len(unique_eans),
            "shared_eans": sum(
                sum(ean in values for values in supplier_eans.values()) > 1
                for ean in unique_eans
            ),
            "products_by_supplier": {
                supplier: len(values) for supplier, values in supplier_eans.items()
            },
            "catalog_products_by_supplier": {
                supplier: statuses[supplier].get("catalog_products_count", 0)
                for supplier in selected
            },
            "scenarios_by_supplier": {
                supplier: statuses[supplier].get("scenarios_count", 0)
                for supplier in selected
            },
        },
        "usable_suppliers": [
            supplier for supplier in selected
            if statuses[supplier]["availability_status"] == "available"
        ],
    }


def prepare_suppliers(
    selected_suppliers, filters, *, now=None, components=None, refreshers=None,
    progress=None, store=None, run_budget=None, rotation_store=None,
    rotation_job_id=None,
):
    """Freeze active Scout supplier generations; never fall back to Manager."""
    selected = normalize_selected_suppliers(selected_suppliers)
    if not selected:
        raise ValueError("Seleziona almeno un fornitore")
    now = now or datetime.now(timezone.utc)
    if components is not None:
        prepared = _prepare_injected_components(
            selected, filters, now=now, components=components,
            refreshers=refreshers, progress=progress,
        )
    else:
        prepared = _prepare_from_supplier_catalog(
            selected, now=now, progress=progress,
            store=store or SupplierCatalogStore(),
        )
    return _apply_run_budget(
        prepared, selected, run_budget,
        rotation_store=rotation_store, rotation_job_id=rotation_job_id,
    )
