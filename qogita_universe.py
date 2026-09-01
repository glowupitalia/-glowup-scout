"""Stable semantic universes for Qogita Discovery jobs."""

from __future__ import annotations

from typing import Any


QOGITA_UNIVERSE_FULL = "full"
QOGITA_UNIVERSE_KOREAN_BEAUTY = "korean_beauty"
QOGITA_UNIVERSES = {
    QOGITA_UNIVERSE_FULL,
    QOGITA_UNIVERSE_KOREAN_BEAUTY,
}


def normalize_qogita_universe(value: Any) -> str:
    """Default legacy jobs to the unchanged full Qogita serving universe."""
    normalized = str(value or QOGITA_UNIVERSE_FULL).strip().casefold()
    if normalized not in QOGITA_UNIVERSES:
        raise ValueError(f"Unsupported Qogita universe: {value!r}")
    return normalized
