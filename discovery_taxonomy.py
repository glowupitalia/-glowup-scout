"""Structured Amazon taxonomy support for Qogita Discovery scenarios.

The filter deliberately operates on Amazon classification identifiers, never
on product-title keywords.  Labels are presentation metadata only.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Iterable


MARKETPLACE_IT = "APJ6JRA9NG5V4"
TAXONOMY_SCHEMA_VERSION = "amazon_it_qogita_v1"
BEAUTY_DEPARTMENT_ID = "6198082031"

# Stable Amazon browse-node IDs.  This is intentionally presentation metadata;
# persisted selections contain only marketplace + node IDs.
QOGITA_CATEGORY_TREE: dict[str, dict[str, Any]] = {
    "4327902031": {"label": "Cura dei capelli", "children": {
        "27088078031": "Shampoo e balsamo", "4327915031": "Colore",
        "27088077031": "Styling", "6307213031": "Maschere", "4327909031": "Oli",
        "4327918031": "Colore permanente", "4327914031": "Shampoo",
        "4327904031": "Balsami", "4327905031": "Trattamenti profondi",
        "4327948031": "Lacche e spray", "4327919031": "Colore semipermanente",
        "4327947031": "Gel styling", "27088089031": "Mousse", "4327951031": "Sieri",
    }},
    "6306897031": {"label": "Cura della pelle", "children": {
        "6306911031": "Viso", "6306905031": "Corpo",
        "6306909031": "Protezione solare e abbronzatura",
        "6306908031": "Prodotti trattamento occhi", "6306907031": "Mani e piedi",
        "6306906031": "Labbra", "6307116031": "Creme viso",
        "20230625031": "Sieri e liquidi", "4331154031": "Creme corpo",
        "6306971031": "Maschere", "4331156031": "Lozioni e balsami corpo",
        "6307065031": "Protezione solare viso", "6307063031": "Protezione solare corpo",
        "6307069031": "Gel detergenti", "6307067031": "Creme e latte detergente",
        "6306972031": "Tonici", "6306959031": "Creme contorno occhi",
        "6307073031": "Scrub viso",
    }},
    "6306900031": {"label": "Trucco", "children": {
        "6306932031": "Viso", "6306927031": "Labbra", "6306928031": "Occhi",
        "6307039031": "Fondotinta", "6307022031": "Rossetti",
        "6307030031": "Ombretti", "6307018031": "Lucidalabbra",
        "6307035031": "Cipria", "6307028031": "Mascara",
        "6307036031": "Correttori viso", "6307026031": "Eyeliner",
        "6307024031": "Sopracciglia", "6307019031": "Matite labbra",
        "6307037031": "Fard", "6307020031": "Rossetti liquidi e pennarelli",
    }},
    "6306898031": {"label": "Fragranze e profumi", "children": {
        "6306914031": "Donna", "6306921031": "Uomo", "6306912031": "Bambini",
        "6306913031": "Profumi casa e candele", "6306974031": "Donna Eau de Parfum",
        "6306975031": "Donna Eau de Toilette", "6306973031": "Donna Acqua di Colonia",
        "6306981031": "Uomo Eau de Parfum", "6306982031": "Uomo Eau de Toilette",
        "6306980031": "Uomo Acqua di Colonia",
    }},
    "4327880031": {"label": "Bagno e corpo", "children": {}},
    "6306899031": {"label": "Manicure e pedicure", "children": {}},
    "6306896031": {"label": "Accessori e strumenti di bellezza", "children": {}},
    "27088076031": {"label": "Attrezzature per saloni e spa", "children": {}},
    "1571289031": {"label": "Salute e cura della persona", "children": {}},
    "1571286031": {"label": "Prima infanzia", "children": {}},
    "524015031": {"label": "Casa e cucina", "children": {}},
    "3606310031": {"label": "Cancelleria e prodotti per ufficio", "children": {}},
    "5512286031": {"label": "Moda", "children": {}},
    "2454160031": {"label": "Fai da te", "children": {}},
    "6198092031": {"label": "Alimentari e cura della casa", "children": {}},
    "12472499031": {"label": "Prodotti per animali domestici", "children": {}},
    "5866068031": {"label": "Commercio, industria e scienza", "children": {}},
    "425916031": {"label": "Informatica", "children": {}},
    "523997031": {"label": "Giochi e giocattoli", "children": {}},
    "1571280031": {"label": "Auto e moto", "children": {}},
    "635016031": {"label": "Giardino e giardinaggio", "children": {}},
    "412609031": {"label": "Elettronica", "children": {}},
    "524012031": {"label": "Sport e tempo libero", "children": {}},
    "412603031": {"label": "Videogiochi", "children": {}},
}

BEAUTY_PARENT_IDS = frozenset({
    "4327902031", "6306897031", "6306900031", "6306898031",
    "4327880031", "6306899031", "6306896031", "27088076031",
})
KNOWN_PARENT_IDS = frozenset(QOGITA_CATEGORY_TREE)


def default_qogita_category_filter() -> dict[str, Any]:
    return {
        "qogita_category_filter_enabled": False,
        "qogita_category_selected_parent_ids": [],
        "qogita_category_child_overrides": {},
        "qogita_category_include_unknown": True,
        "qogita_category_only_beauty": False,
        "qogita_taxonomy_schema_version": TAXONOMY_SCHEMA_VERSION,
        "qogita_category_marketplace_id": MARKETPLACE_IT,
    }


def normalize_qogita_category_filter(state: dict[str, Any] | None) -> dict[str, Any]:
    raw = state or {}
    result = default_qogita_category_filter()
    result["qogita_category_filter_enabled"] = bool(
        raw.get("qogita_category_filter_enabled", False)
    )
    result["qogita_category_only_beauty"] = bool(
        raw.get("qogita_category_only_beauty", False)
    )
    result["qogita_category_include_unknown"] = bool(
        raw.get("qogita_category_include_unknown", True)
    )
    result["qogita_category_selected_parent_ids"] = sorted({
        str(value) for value in raw.get("qogita_category_selected_parent_ids") or []
        if str(value) in KNOWN_PARENT_IDS
    })
    overrides = raw.get("qogita_category_child_overrides") or {}
    result["qogita_category_child_overrides"] = {
        str(parent): {"excluded_ids": sorted({
            str(value) for value in (value.get("excluded_ids") if isinstance(value, dict) else value) or []
        })}
        for parent, value in overrides.items() if str(parent) in KNOWN_PARENT_IDS
    }
    return result


def _classification_chain(leaf: dict[str, Any]) -> list[dict[str, str]]:
    chain: list[dict[str, str]] = []
    current: Any = leaf
    seen: set[str] = set()
    while isinstance(current, dict):
        node_id = str(current.get("classificationId") or "").strip()
        if not node_id or node_id in seen:
            break
        seen.add(node_id)
        chain.append({
            "classification_id": node_id,
            "display_name": str(current.get("displayName") or "").strip(),
        })
        current = current.get("parent")
    chain.reverse()
    return chain


def extract_listing_classification_paths(
    listing: dict[str, Any], *, marketplace_id: str = MARKETPLACE_IT,
) -> list[dict[str, Any]]:
    """Return unique root-to-leaf paths from persisted Catalog structures."""
    paths: list[dict[str, Any]] = []
    records = (listing.get("diagnostics") or {}).get("classification_records") or []
    for record in records:
        if not isinstance(record, dict):
            continue
        record_marketplace = str(record.get("marketplaceId") or marketplace_id)
        if record_marketplace != marketplace_id:
            continue
        for leaf in record.get("classifications") or []:
            chain = _classification_chain(leaf)
            if chain:
                paths.append({"marketplace_id": marketplace_id, "nodes": chain})
    if not paths:
        browse = listing.get("browse_classification") or listing.get("browseClassification") or {}
        node_id = str(browse.get("classificationId") or "").strip()
        if node_id:
            paths.append({"marketplace_id": marketplace_id, "nodes": [{
                "classification_id": node_id,
                "display_name": str(browse.get("displayName") or "").strip(),
            }]})
    unique: dict[str, dict[str, Any]] = {}
    for path in paths:
        path_ids = [node["classification_id"] for node in path["nodes"]]
        digest = hashlib.sha256("/".join(path_ids).encode()).hexdigest()[:24]
        unique[digest] = {**path, "path_hash": digest}
    return list(unique.values())


def projection_rows(
    job_id: str, canonical_identifier: str, listing: dict[str, Any],
) -> list[tuple[Any, ...]]:
    asin = str(listing.get("asin") or "")
    rows = []
    for path in extract_listing_classification_paths(listing):
        nodes = path["nodes"]
        for depth, node in enumerate(nodes):
            rows.append((
                job_id, canonical_identifier, asin, path["marketplace_id"],
                path["path_hash"], node["classification_id"],
                nodes[depth - 1]["classification_id"] if depth else None,
                depth, node["display_name"], int(depth == len(nodes) - 1),
            ))
    return rows


def classification_paths_allowed(
    paths: Iterable[Iterable[str]], config: dict[str, Any] | None,
) -> bool:
    """Conservative allow decision: any allowed path preserves Qogita."""
    normalized = normalize_qogita_category_filter(config)
    if not normalized["qogita_category_filter_enabled"]:
        return True
    materialized = [tuple(str(node) for node in path if node) for path in paths]
    if not materialized:
        return normalized["qogita_category_include_unknown"]
    selected = set(normalized["qogita_category_selected_parent_ids"])
    overrides = normalized["qogita_category_child_overrides"]
    for path in materialized:
        path_ids = set(path)
        if normalized["qogita_category_only_beauty"] and BEAUTY_DEPARTMENT_ID not in path_ids:
            continue
        known_on_path = path_ids & KNOWN_PARENT_IDS
        # Future categories remain included unless Solo Beauty is explicit.
        if known_on_path and not (known_on_path & selected):
            continue
        selected_on_path = known_on_path & selected
        if any(
            path_ids & set((overrides.get(parent) or {}).get("excluded_ids") or [])
            for parent in selected_on_path
        ):
            continue
        return True
    return False


def qogita_scenario(scenario: dict[str, Any]) -> bool:
    return str(scenario.get("supplier") or "").strip().lower() == "qogita"


def filter_qogita_scenarios(
    candidate: dict[str, Any], paths: Iterable[Iterable[str]],
    config: dict[str, Any] | None,
) -> bool:
    """Remove Qogita scenarios only; return whether a removal occurred."""
    scenarios = list(candidate.get("scenarios") or [])
    if not any(qogita_scenario(row) for row in scenarios):
        return False
    if classification_paths_allowed(paths, config):
        return False
    candidate["scenarios"] = [row for row in scenarios if not qogita_scenario(row)]
    candidate["qogita_category_filtered"] = True
    return True


def apply_qogita_listing_filter(
    candidate: dict[str, Any],
    paths_by_asin: dict[str, list[tuple[str, ...]]],
    config: dict[str, Any] | None,
) -> bool:
    """Apply per-listing exclusions and remove Qogita only when none remain."""
    scenarios = list(candidate.get("scenarios") or [])
    if not any(qogita_scenario(row) for row in scenarios):
        return False
    listings = list(candidate.get("amazon_listings") or [])
    allowed_values = []
    for listing in listings:
        allowed = classification_paths_allowed(
            paths_by_asin.get(str(listing.get("asin") or "")) or [], config,
        )
        allowed_values.append(allowed)
        excluded = {
            str(value).strip().lower()
            for value in listing.get("excluded_suppliers") or [] if value
        }
        if allowed:
            excluded.discard("qogita")
        else:
            excluded.add("qogita")
        if excluded:
            listing["excluded_suppliers"] = sorted(excluded)
        else:
            listing.pop("excluded_suppliers", None)
    if not listings:
        allowed_values.append(classification_paths_allowed([], config))
    if any(allowed_values):
        return False
    return filter_qogita_scenarios(
        candidate,
        [path for paths in paths_by_asin.values() for path in paths],
        config,
    )
