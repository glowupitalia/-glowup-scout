"""Persistent supplier-neutral rotation for bounded Discovery runs."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from supplier_catalog import canonical_gtin14


DEFAULT_ROTATION_DATABASE = (
    Path(__file__).resolve().parent / "data" / "discovery_rotation.sqlite3"
)
ROTATION_STRATEGY = "persistent_supplier_scope_rotation_v1"


SCHEMA = """
CREATE TABLE IF NOT EXISTS discovery_rotation_scopes (
    scope_key TEXT PRIMARY KEY,
    selected_suppliers_json TEXT NOT NULL,
    current_cycle_id INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS discovery_rotation_items (
    scope_key TEXT NOT NULL,
    canonical_identifier TEXT NOT NULL,
    supplier_membership_json TEXT NOT NULL,
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    last_seen_catalog_generation_json TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1,
    priority_new INTEGER NOT NULL DEFAULT 1,
    last_discovery_at TEXT,
    discovery_count INTEGER NOT NULL DEFAULT 0,
    last_analyzed_cycle INTEGER NOT NULL DEFAULT 0,
    last_job_id TEXT,
    PRIMARY KEY (scope_key, canonical_identifier),
    FOREIGN KEY (scope_key) REFERENCES discovery_rotation_scopes(scope_key)
);
CREATE TABLE IF NOT EXISTS discovery_rotation_selections (
    job_id TEXT NOT NULL,
    scope_key TEXT NOT NULL,
    cycle_id INTEGER NOT NULL,
    canonical_identifier TEXT NOT NULL,
    selected_at TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'selected',
    catalog_status TEXT,
    analyzed_at TEXT,
    PRIMARY KEY (job_id, canonical_identifier),
    FOREIGN KEY (scope_key, canonical_identifier)
      REFERENCES discovery_rotation_items(scope_key, canonical_identifier)
);
CREATE TABLE IF NOT EXISTS discovery_rotation_global_history (
    canonical_identifier TEXT PRIMARY KEY,
    first_discovery_at TEXT NOT NULL,
    last_discovery_at TEXT NOT NULL,
    discovery_count INTEGER NOT NULL DEFAULT 0,
    last_job_id TEXT
);
CREATE INDEX IF NOT EXISTS idx_discovery_rotation_remaining
ON discovery_rotation_items(scope_key,active,last_analyzed_cycle,priority_new,last_discovery_at);
CREATE INDEX IF NOT EXISTS idx_discovery_rotation_job
ON discovery_rotation_selections(job_id,status);
"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def normalize_rotation_suppliers(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(sorted({str(value or "").strip().casefold() for value in values if value}))


def rotation_scope_key(values: Iterable[str]) -> str:
    suppliers = normalize_rotation_suppliers(values)
    if not suppliers:
        raise ValueError("Rotation scope requires at least one supplier")
    digest = hashlib.sha256("|".join(suppliers).encode("utf-8")).hexdigest()[:20]
    return f"supplier-scope-{digest}"


def _candidate_membership(candidate: dict[str, Any], selected: tuple[str, ...]) -> tuple[str, ...]:
    present = {
        str(row.get("supplier") or "").casefold()
        for row in candidate.get("scenarios") or []
    }
    return tuple(value for value in selected if value in present)


def _definitive_catalog_status(value: Any) -> bool:
    status = str(value or "").strip().casefold()
    return bool(status) and status not in {
        "catalog_incomplete", "incomplete", "pending", "retry_pending",
        "request_failed", "error",
    }


class DiscoveryRotationStore:
    def __init__(self, path: str | Path | None = None):
        configured = path or os.environ.get("DISCOVERY_ROTATION_DATABASE")
        self.path = Path(configured or DEFAULT_ROTATION_DATABASE).expanduser().resolve()

    def _connect(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path, timeout=60)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA busy_timeout=60000")
        return connection

    def initialize(self):
        with self._connect() as connection:
            connection.executescript(SCHEMA)
            # Safe, idempotent migration from the original scope-local history.
            # Exact canonical identifiers are aggregated without changing cycles,
            # selections, checkpoints, or prior scope membership.
            connection.execute(
                """INSERT INTO discovery_rotation_global_history
                     (canonical_identifier,first_discovery_at,last_discovery_at,
                      discovery_count,last_job_id)
                   SELECT canonical_identifier,
                          MIN(COALESCE(last_discovery_at,first_seen_at)),
                          MAX(COALESCE(last_discovery_at,first_seen_at)),
                          SUM(discovery_count),MAX(last_job_id)
                     FROM discovery_rotation_items
                    WHERE discovery_count>0
                    GROUP BY canonical_identifier
                   ON CONFLICT(canonical_identifier) DO NOTHING"""
            )

    def sync_universe(
        self, candidates: Iterable[dict[str, Any]], selected_suppliers: Iterable[str],
        *, supplier_snapshot_set: dict[str, Any] | None = None, now: str | None = None,
    ) -> dict[str, Any]:
        """Upsert one active union without consuming any identifier."""
        self.initialize()
        observed = now or _now()
        selected = normalize_rotation_suppliers(selected_suppliers)
        scope = rotation_scope_key(selected)
        snapshots = supplier_snapshot_set or {}
        rows: dict[str, dict[str, Any]] = {}
        for candidate in candidates:
            identifier = str(candidate.get("canonical_ean") or "").strip()
            if canonical_gtin14(identifier) is None:
                continue
            membership = _candidate_membership(candidate, selected)
            rows[identifier] = {
                "membership": membership,
                "generations": {
                    supplier: (snapshots.get(supplier) or {}).get("snapshot_id")
                    for supplier in membership
                },
            }
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            initializing = self._scope_row(connection, scope) is None
            global_history = {
                row["canonical_identifier"]: row["last_discovery_at"]
                for row in connection.execute(
                    """SELECT canonical_identifier,last_discovery_at
                       FROM discovery_rotation_global_history"""
                )
            }
            previous_row, _ = self._previous_scope_context(connection, selected, scope)
            previous_identifiers = set()
            if initializing and previous_row:
                previous_identifiers = {
                    row["canonical_identifier"]
                    for row in connection.execute(
                        """SELECT canonical_identifier FROM discovery_rotation_items
                           WHERE scope_key=? AND active=1""",
                        (previous_row["scope_key"],),
                    )
                }
            connection.execute(
                """INSERT INTO discovery_rotation_scopes
                   (scope_key,selected_suppliers_json,current_cycle_id,created_at,updated_at)
                   VALUES (?,?,1,?,?)
                   ON CONFLICT(scope_key) DO UPDATE SET
                     selected_suppliers_json=excluded.selected_suppliers_json,
                     updated_at=excluded.updated_at""",
                (scope, _json(selected), observed, observed),
            )
            connection.execute(
                "UPDATE discovery_rotation_items SET active=0 WHERE scope_key=?", (scope,)
            )
            for identifier, value in rows.items():
                connection.execute(
                    """INSERT INTO discovery_rotation_items
                       (scope_key,canonical_identifier,supplier_membership_json,
                        first_seen_at,last_seen_at,last_seen_catalog_generation_json,
                        active,priority_new,last_discovery_at)
                       VALUES (?,?,?,?,?,?,1,?,?)
                       ON CONFLICT(scope_key,canonical_identifier) DO UPDATE SET
                         supplier_membership_json=excluded.supplier_membership_json,
                         last_seen_at=excluded.last_seen_at,
                         last_seen_catalog_generation_json=excluded.last_seen_catalog_generation_json,
                         active=1""",
                    (
                        scope, identifier, _json(value["membership"]), observed, observed,
                        _json(value["generations"]),
                        (
                            int(identifier not in previous_identifiers)
                            if initializing and previous_row else (0 if initializing else 1)
                        ),
                        global_history.get(identifier),
                    ),
                )
            connection.commit()
        return self.status(selected)

    def _scope_row(self, connection, scope: str):
        return connection.execute(
            "SELECT * FROM discovery_rotation_scopes WHERE scope_key=?", (scope,)
        ).fetchone()

    def _previous_scope_context(self, connection, selected: tuple[str, ...], scope: str):
        candidates = []
        selected_set = set(selected)
        for row in connection.execute(
            "SELECT * FROM discovery_rotation_scopes WHERE scope_key<>?", (scope,)
        ):
            previous = tuple(json.loads(row["selected_suppliers_json"]))
            if set(previous).issubset(selected_set):
                candidates.append((len(previous), row["updated_at"], row, previous))
        if not candidates:
            return None, ()
        _, _, row, previous = max(candidates, key=lambda value: (value[0], value[1]))
        return row, previous

    def status(
        self, selected_suppliers: Iterable[str], *,
        active_identifiers: Iterable[str] | None = None,
    ) -> dict[str, Any]:
        self.initialize()
        selected = normalize_rotation_suppliers(selected_suppliers)
        scope = rotation_scope_key(selected)
        supplied_identifiers = None
        if active_identifiers is not None:
            supplied_identifiers = {
                str(value).strip() for value in active_identifiers
                if canonical_gtin14(value) is not None
            }
        with self._connect() as connection:
            scope_row = self._scope_row(connection, scope)
            previous_row, previous_suppliers = self._previous_scope_context(
                connection, selected, scope,
            )
            global_identifiers = {
                row["canonical_identifier"]
                for row in connection.execute(
                    "SELECT canonical_identifier FROM discovery_rotation_global_history"
                )
            }
            if not scope_row:
                universe = len(supplied_identifiers or ())
                global_overlap = len((supplied_identifiers or set()) & global_identifiers)
                previous_identifiers = set()
                previous_analyzed = 0
                if previous_row:
                    previous_identifiers = {
                        row["canonical_identifier"]
                        for row in connection.execute(
                            """SELECT canonical_identifier FROM discovery_rotation_items
                               WHERE scope_key=? AND active=1""",
                            (previous_row["scope_key"],),
                        )
                    }
                    previous_analyzed = connection.execute(
                        """SELECT COUNT(*) FROM discovery_rotation_items
                           WHERE scope_key=? AND active=1 AND last_analyzed_cycle=?""",
                        (previous_row["scope_key"], previous_row["current_cycle_id"]),
                    ).fetchone()[0]
                return {
                    "rotation_scope": scope, "rotation_cycle_id": 1,
                    "rotation_scope_initialized": False,
                    "rotation_selected_suppliers": list(selected),
                    "rotation_universe_count": universe, "rotation_analyzed_count": 0,
                    "rotation_remaining_count": universe, "rotation_coverage_percent": 0.0,
                    "rotation_global_analyzed_count": global_overlap,
                    "rotation_never_analyzed_count": max(0, universe - global_overlap),
                    "rotation_new_identifier_count": len(
                        (supplied_identifiers or set()) - previous_identifiers
                    ) if previous_row else universe,
                    "rotation_previous_scope": previous_row["scope_key"] if previous_row else None,
                    "rotation_previous_cycle_id": (
                        int(previous_row["current_cycle_id"]) if previous_row else None
                    ),
                    "rotation_previous_analyzed_count": previous_analyzed,
                    "rotation_previous_suppliers": list(previous_suppliers),
                    "rotation_added_suppliers": sorted(set(selected) - set(previous_suppliers)),
                    "rotation_cycle_complete": False,
                }
            cycle = int(scope_row["current_cycle_id"])
            stored_rows = connection.execute(
                """SELECT canonical_identifier,last_analyzed_cycle,priority_new
                   FROM discovery_rotation_items WHERE scope_key=? AND active=1""",
                (scope,),
            ).fetchall()
            stored = {row["canonical_identifier"]: row for row in stored_rows}
            active = supplied_identifiers if supplied_identifiers is not None else set(stored)
            universe = len(active)
            analyzed = sum(
                identifier in stored and int(stored[identifier]["last_analyzed_cycle"]) == cycle
                for identifier in active
            )
            global_overlap = len(active & global_identifiers)
            pending_new = sum(
                identifier not in stored or int(stored[identifier]["priority_new"]) == 1
                for identifier in active
            )
            previous_analyzed = 0
            if previous_row:
                previous_analyzed = connection.execute(
                    """SELECT COUNT(*) FROM discovery_rotation_items
                       WHERE scope_key=? AND active=1 AND last_analyzed_cycle=?""",
                    (previous_row["scope_key"], previous_row["current_cycle_id"]),
                ).fetchone()[0]
        return {
            "rotation_scope": scope,
            "rotation_cycle_id": cycle,
            "rotation_scope_initialized": True,
            "rotation_selected_suppliers": list(selected),
            "rotation_universe_count": universe,
            "rotation_analyzed_count": analyzed,
            "rotation_remaining_count": max(0, universe - analyzed),
            "rotation_coverage_percent": (analyzed / universe * 100.0) if universe else 0.0,
            "rotation_global_analyzed_count": global_overlap,
            "rotation_never_analyzed_count": max(0, universe - global_overlap),
            "rotation_new_identifier_count": pending_new,
            "rotation_previous_scope": previous_row["scope_key"] if previous_row else None,
            "rotation_previous_cycle_id": (
                int(previous_row["current_cycle_id"]) if previous_row else None
            ),
            "rotation_previous_analyzed_count": previous_analyzed,
            "rotation_previous_suppliers": list(previous_suppliers),
            "rotation_added_suppliers": sorted(set(selected) - set(previous_suppliers)),
            "rotation_cycle_complete": bool(universe and analyzed == universe),
        }

    def select(
        self, job_id: str, candidates: list[dict[str, Any]],
        selected_suppliers: Iterable[str], budget: int | None, *,
        supplier_snapshot_set: dict[str, Any] | None = None, now: str | None = None,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        if not job_id:
            raise ValueError("Rotation selection requires a job_id")
        observed = now or _now()
        selected = normalize_rotation_suppliers(selected_suppliers)
        scope = rotation_scope_key(selected)
        self.sync_universe(
            candidates, selected, supplier_snapshot_set=supplier_snapshot_set, now=observed,
        )
        by_identifier = {
            str(row.get("canonical_ean")): row for row in candidates
            if canonical_gtin14(row.get("canonical_ean")) is not None
        }
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                """SELECT canonical_identifier,cycle_id FROM discovery_rotation_selections
                   WHERE job_id=? AND scope_key=? ORDER BY canonical_identifier""",
                (job_id, scope),
            ).fetchall()
            scope_row = self._scope_row(connection, scope)
            cycle = int(scope_row["current_cycle_id"])
            if existing:
                identifiers = [row["canonical_identifier"] for row in existing]
                cycle = int(existing[0]["cycle_id"])
            else:
                remaining = connection.execute(
                    """SELECT * FROM discovery_rotation_items
                       WHERE scope_key=? AND active=1 AND last_analyzed_cycle<>?""",
                    (scope, cycle),
                ).fetchall()
                ordered = sorted(
                    remaining,
                    key=lambda row: (
                        -int(row["priority_new"]),
                        row["last_discovery_at"] is not None,
                        row["last_discovery_at"] or "",
                        hashlib.sha256(
                            f"{scope}:{row['canonical_identifier']}".encode("utf-8")
                        ).hexdigest(),
                    ),
                )
                count = len(ordered) if budget is None else min(int(budget), len(ordered))
                identifiers = [row["canonical_identifier"] for row in ordered[:count]]
                connection.executemany(
                    """INSERT INTO discovery_rotation_selections
                       (job_id,scope_key,cycle_id,canonical_identifier,selected_at,status)
                       VALUES (?,?,?,?,?,'selected')""",
                    [(job_id, scope, cycle, identifier, observed) for identifier in identifiers],
                )
            universe = connection.execute(
                "SELECT COUNT(*) FROM discovery_rotation_items WHERE scope_key=? AND active=1",
                (scope,),
            ).fetchone()[0]
            analyzed_before = connection.execute(
                """SELECT COUNT(*) FROM discovery_rotation_items
                   WHERE scope_key=? AND active=1 AND last_analyzed_cycle=?""",
                (scope, cycle),
            ).fetchone()[0]
            connection.commit()
        selected_rows = [by_identifier[value] for value in identifiers if value in by_identifier]
        scope_status = self.status(selected)
        metadata = {
            **{
                key: value for key, value in scope_status.items()
                if key.startswith("rotation_")
            },
            "rotation_scope": scope,
            "rotation_cycle_id": cycle,
            "rotation_universe_count": universe,
            "rotation_analyzed_before_run": analyzed_before,
            "rotation_selected_identifiers": identifiers,
            "rotation_analyzed_this_run": 0,
            "rotation_remaining_after_run": max(0, universe - analyzed_before),
            "run_budget": "all" if budget is None else int(budget),
            "sampled_identifier_count": len(selected_rows),
            "sampling_strategy": ROTATION_STRATEGY,
        }
        return selected_rows, metadata

    def select_current_universe(
        self, job_id: str, candidates: list[dict[str, Any]],
        selected_suppliers: Iterable[str], budget: int | None, *,
        supplier_snapshot_set: dict[str, Any] | None = None,
        action_priority: dict[str, int] | None = None,
        now: str | None = None,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """Select from the whole active universe without resetting a cycle.

        This V2 entry point leaves ``select`` unchanged for legacy jobs.  The
        current cycle remains audit/fairness state, while filters and full-
        universe evaluation no longer require an artificial cycle reset.
        """
        if not job_id:
            raise ValueError("Rotation selection requires a job_id")
        observed = now or _now()
        selected = normalize_rotation_suppliers(selected_suppliers)
        scope = rotation_scope_key(selected)
        self.sync_universe(
            candidates, selected, supplier_snapshot_set=supplier_snapshot_set, now=observed,
        )
        by_identifier = {
            str(row.get("canonical_ean")): row for row in candidates
            if canonical_gtin14(row.get("canonical_ean")) is not None
        }
        priorities = action_priority or {}
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                """SELECT canonical_identifier,cycle_id FROM discovery_rotation_selections
                   WHERE job_id=? AND scope_key=? ORDER BY canonical_identifier""",
                (job_id, scope),
            ).fetchall()
            scope_row = self._scope_row(connection, scope)
            cycle = int(scope_row["current_cycle_id"])
            if existing:
                identifiers = [row["canonical_identifier"] for row in existing]
                cycle = int(existing[0]["cycle_id"])
            else:
                active = connection.execute(
                    "SELECT * FROM discovery_rotation_items WHERE scope_key=? AND active=1",
                    (scope,),
                ).fetchall()
                ordered = sorted(
                    active,
                    key=lambda row: (
                        int(priorities.get(str(row["canonical_identifier"]), 99)),
                        -int(row["priority_new"]),
                        row["last_discovery_at"] is not None,
                        row["last_discovery_at"] or "",
                        hashlib.sha256(
                            f"{scope}:{row['canonical_identifier']}".encode("utf-8")
                        ).hexdigest(),
                    ),
                )
                count = len(ordered) if budget is None else min(int(budget), len(ordered))
                identifiers = [row["canonical_identifier"] for row in ordered[:count]]
                connection.executemany(
                    """INSERT INTO discovery_rotation_selections
                       (job_id,scope_key,cycle_id,canonical_identifier,selected_at,status)
                       VALUES (?,?,?,?,?,'selected')""",
                    [(job_id, scope, cycle, identifier, observed) for identifier in identifiers],
                )
            universe = connection.execute(
                "SELECT COUNT(*) FROM discovery_rotation_items WHERE scope_key=? AND active=1",
                (scope,),
            ).fetchone()[0]
            analyzed_before = connection.execute(
                """SELECT COUNT(*) FROM discovery_rotation_items
                   WHERE scope_key=? AND active=1 AND last_analyzed_cycle=?""",
                (scope, cycle),
            ).fetchone()[0]
            connection.commit()
        selected_rows = [by_identifier[value] for value in identifiers if value in by_identifier]
        scope_status = self.status(selected)
        metadata = {
            **{key: value for key, value in scope_status.items() if key.startswith("rotation_")},
            "rotation_scope": scope,
            "rotation_cycle_id": cycle,
            "rotation_universe_count": universe,
            "rotation_analyzed_before_run": analyzed_before,
            "rotation_selected_identifiers": identifiers,
            "rotation_analyzed_this_run": 0,
            "rotation_remaining_after_run": max(0, universe - analyzed_before),
            "run_budget": "all" if budget is None else int(budget),
            "sampled_identifier_count": len(selected_rows),
            "sampling_strategy": "automatic_amazon_freshness_v1",
            "rotation_selection_mode": "current_universe",
        }
        return selected_rows, metadata

    def commit_catalog_results(
        self, job_id: str, statuses: dict[str, Any], *, now: str | None = None,
    ) -> dict[str, Any]:
        """Consume only identifiers with a definitive Catalog Items result."""
        self.initialize()
        observed = now or _now()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            selections = connection.execute(
                "SELECT * FROM discovery_rotation_selections WHERE job_id=?",
                (job_id,),
            ).fetchall()
            for row in selections:
                identifier = row["canonical_identifier"]
                status = statuses.get(identifier)
                if not _definitive_catalog_status(status):
                    continue
                connection.execute(
                    """UPDATE discovery_rotation_selections
                       SET status='analyzed',catalog_status=?,analyzed_at=?
                       WHERE job_id=? AND canonical_identifier=?""",
                    (str(status), observed, job_id, identifier),
                )
                item_update = connection.execute(
                    """UPDATE discovery_rotation_items SET
                         last_discovery_at=?,discovery_count=discovery_count+1,
                         last_analyzed_cycle=?,last_job_id=?,priority_new=0
                       WHERE scope_key=? AND canonical_identifier=?
                         AND last_analyzed_cycle<>?""",
                    (
                        observed, int(row["cycle_id"]), job_id, row["scope_key"],
                        identifier, int(row["cycle_id"]),
                    ),
                )
                if item_update.rowcount:
                    connection.execute(
                        """INSERT INTO discovery_rotation_global_history
                             (canonical_identifier,first_discovery_at,last_discovery_at,
                              discovery_count,last_job_id)
                           VALUES (?,?,?,?,?)
                           ON CONFLICT(canonical_identifier) DO UPDATE SET
                             last_discovery_at=excluded.last_discovery_at,
                             discovery_count=discovery_rotation_global_history.discovery_count+1,
                             last_job_id=excluded.last_job_id""",
                        (identifier, observed, observed, 1, job_id),
                    )
            first = selections[0] if selections else None
            if not first:
                connection.commit()
                return {}
            scope, cycle = first["scope_key"], int(first["cycle_id"])
            universe = connection.execute(
                "SELECT COUNT(*) FROM discovery_rotation_items WHERE scope_key=? AND active=1",
                (scope,),
            ).fetchone()[0]
            analyzed = connection.execute(
                """SELECT COUNT(*) FROM discovery_rotation_items
                   WHERE scope_key=? AND active=1 AND last_analyzed_cycle=?""",
                (scope, cycle),
            ).fetchone()[0]
            analyzed_run = connection.execute(
                """SELECT COUNT(*) FROM discovery_rotation_selections
                   WHERE job_id=? AND status='analyzed'""",
                (job_id,),
            ).fetchone()[0]
            connection.commit()
        return {
            "rotation_scope": scope,
            "rotation_cycle_id": cycle,
            "rotation_universe_count": universe,
            "rotation_analyzed_count": analyzed,
            "rotation_analyzed_this_run": analyzed_run,
            "rotation_remaining_after_run": max(0, universe - analyzed),
            "rotation_coverage_percent": (analyzed / universe * 100.0) if universe else 0.0,
            "rotation_cycle_complete": bool(universe and analyzed == universe),
        }

    def start_new_cycle(
        self, selected_suppliers: Iterable[str], *, confirmed: bool, now: str | None = None,
    ) -> dict[str, Any]:
        if not confirmed:
            raise ValueError("Explicit confirmation is required")
        self.initialize()
        scope = rotation_scope_key(selected_suppliers)
        observed = now or _now()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = self._scope_row(connection, scope)
            if not row:
                raise ValueError("Rotation scope has not been initialized")
            cycle = int(row["current_cycle_id"]) + 1
            connection.execute(
                "UPDATE discovery_rotation_scopes SET current_cycle_id=?,updated_at=? WHERE scope_key=?",
                (cycle, observed, scope),
            )
            connection.commit()
        return self.status(selected_suppliers)
