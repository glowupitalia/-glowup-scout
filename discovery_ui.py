"""Pure presentation helpers for persisted Discovery runtime state."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


DISCOVERY_PHASES = (
    ("preparing", "PREPARAZIONE"),
    ("catalog", "CATALOGO AMAZON"),
    ("pricing", "PREZZI"),
    ("competition", "CONCORRENZA"),
    ("fees", "COMMISSIONI AMAZON"),
    ("economics", "CALCOLO OPPORTUNITÀ"),
    ("export", "CREAZIONE EXCEL"),
    ("completed", "COMPLETATO"),
)

_PHASE_LABELS = dict(DISCOVERY_PHASES)


def _parse_time(value: object) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def discovery_phase_key(runtime: dict[str, Any], persisted_phase: str | None = None) -> str:
    """Map technical state to one stable user-facing phase.

    The registry is the UI authority while a process is active.  The persisted
    incremental phase is only a fallback, avoiding contradictory labels such as
    registry ``catalog`` versus internal ``suppliers_loaded``.
    """
    status = str(runtime.get("status") or "").lower()
    phase = str(runtime.get("phase") or persisted_phase or status).lower()
    if status == "completed" or phase == "completed":
        return "completed"
    if status in {
        "computed", "export_pending", "export_running", "export_resource_paused",
        "export_complete", "notification_pending",
    } or phase in {
        "computed", "export_pending", "export_running", "export_rows", "saving",
        "export_complete", "notification_pending",
    }:
        return "export"
    if phase.startswith("prepar") or phase in {
        "initialized", "supplier_preparing", "supplier_checking",
        "supplier_refreshing", "supplier_ready", "qogita_loaded",
    }:
        return "preparing"
    if phase in {
        "catalog", "catalog_filtering", "catalog_complete", "suppliers_loaded",
    }:
        return "catalog"
    if phase in {"pricing", "bsr_filtered", "pricing_complete"}:
        return "pricing"
    if phase in {"competition", "competition_filtered"}:
        return "competition"
    if phase in {"fees", "fees_pending", "fees_complete"}:
        return "fees"
    if phase in {"economics", "ranking"}:
        return "economics"
    return "preparing"


def discovery_phase_label(runtime: dict[str, Any], persisted_phase: str | None = None) -> str:
    return _PHASE_LABELS[discovery_phase_key(runtime, persisted_phase)]


def discovery_phase_progress(runtime: dict[str, Any]) -> dict[str, Any]:
    key = discovery_phase_key(runtime)
    raw_phase = str(runtime.get("phase") or "").lower()
    current = max(0, int(runtime.get("progress_current") or 0))
    total = max(0, int(runtime.get("progress_total") or 0))
    if key == "completed":
        current = total = max(total, current, 1)
    numeric = total > 0 and current <= total
    fraction = min(1.0, current / total) if numeric else None
    labels = {
        "preparing": "Prodotti preparati",
        "catalog": "Prodotti Catalogo definitivi",
        "pricing": "ASIN aggiornati",
        "competition": "Prodotti verificati",
        "fees": "Commissioni elaborate",
        "economics": "Prodotti valutati",
        "export": "Righe Excel elaborate",
        "completed": "Prodotti valutati",
    }
    if raw_phase == "preparing_cache":
        labels["preparing"] = "Preparazione cache Amazon"
    return {
        "phase_key": key,
        "phase_label": _PHASE_LABELS[key],
        "progress_label": labels[key],
        "current": current,
        "total": total,
        "fraction": fraction,
        "numeric": numeric,
    }


def discovery_phase_steps(runtime: dict[str, Any]) -> list[dict[str, str]]:
    current_key = discovery_phase_key(runtime)
    current_index = next(
        index for index, (key, _label) in enumerate(DISCOVERY_PHASES)
        if key == current_key
    )
    return [
        {
            "key": key,
            "label": label,
            "state": "complete" if index < current_index else (
                "current" if index == current_index else "pending"
            ),
        }
        for index, (key, label) in enumerate(DISCOVERY_PHASES)
    ]


def discovery_phase_eta_seconds(
    runtime: dict[str, Any], *, now: datetime | None = None,
) -> int | None:
    """Estimate only when persisted phase-local progress provides a real rate."""
    progress = discovery_phase_progress(runtime)
    if not progress["numeric"] or progress["fraction"] in {0.0, 1.0}:
        return 0 if progress["fraction"] == 1.0 else None
    started = _parse_time(runtime.get("phase_started_at"))
    if started is None:
        return None
    observed = now or datetime.now(timezone.utc)
    if observed.tzinfo is None:
        observed = observed.replace(tzinfo=timezone.utc)
    start_current = max(0, int(runtime.get("phase_progress_start") or 0))
    completed = progress["current"] - start_current
    elapsed = (observed - started).total_seconds()
    if completed <= 0 or elapsed < 30:
        return None
    rate = completed / elapsed
    if rate <= 0:
        return None
    return max(0, int(round((progress["total"] - progress["current"]) / rate)))


def format_eta(seconds: int | None) -> str:
    if seconds is None:
        return "Stima in calcolo…"
    if seconds <= 0:
        return "Fase completata"
    hours, remainder = divmod(seconds, 3600)
    minutes = max(1, round(remainder / 60))
    if hours:
        return f"Circa {hours} h {minutes} min rimanenti"
    return f"Circa {minutes} min rimanenti"


def format_phase_steps(runtime: dict[str, Any]) -> str:
    markers = {"complete": "✓", "current": "●", "pending": "○"}
    return "  ·  ".join(
        f"{markers[step['state']]} {step['label'].title()}"
        for step in discovery_phase_steps(runtime)
    )


__all__ = [
    "DISCOVERY_PHASES", "discovery_phase_eta_seconds", "discovery_phase_key",
    "discovery_phase_label", "discovery_phase_progress", "discovery_phase_steps",
    "format_eta", "format_phase_steps",
]
