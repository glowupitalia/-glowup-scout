"""Authoritative Qogita category-filter mode contract.

This small module intentionally has no application dependencies.  Streamlit
can therefore import the contract safely even when a long-lived process still
has an older ``discovery_taxonomy`` module in ``sys.modules`` during hot reload.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any


class QogitaCategoryMode(StrEnum):
    ALL = "all_categories"
    ONLY_BEAUTY = "only_beauty"
    MANUAL = "manual_selection"


MODE_ALL = QogitaCategoryMode.ALL.value
MODE_ONLY_BEAUTY = QogitaCategoryMode.ONLY_BEAUTY.value
MODE_MANUAL = QogitaCategoryMode.MANUAL.value
VALID_QOGITA_CATEGORY_MODES = frozenset({
    MODE_ALL,
    MODE_ONLY_BEAUTY,
    MODE_MANUAL,
})


def resolve_qogita_category_mode(config: dict[str, Any] | None) -> str:
    """Resolve explicit modes first, then derive a legacy-compatible mode."""
    raw = config or {}
    explicit = str(raw.get("qogita_category_filter_mode") or "").strip()
    if explicit in VALID_QOGITA_CATEGORY_MODES:
        return explicit
    if not bool(raw.get("qogita_category_filter_enabled", False)):
        return MODE_ALL
    if bool(raw.get("qogita_category_only_beauty", False)):
        return MODE_ONLY_BEAUTY
    return MODE_MANUAL
