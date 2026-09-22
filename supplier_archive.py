"""Immutable Supplier/Qogita archive segments on the Glow Up Data tier."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from discovery_archive import (
    DEFAULT_MOUNT_PATH,
    DEFAULT_VOLUME_UUID,
    ArchiveIntegrityError,
    ArchiveStorageUnavailable,
    validate_archive_volume,
)


SCHEMA_VERSION = 1
SUPPLIER_ARCHIVE_ROOT = DEFAULT_MOUNT_PATH / "Archive" / "Supplier"
QOGITA_ARCHIVE_ROOT = DEFAULT_MOUNT_PATH / "Archive" / "Qogita"
STAGING_ROOT = DEFAULT_MOUNT_PATH / "Staging" / "SQLite Tiering"
SEGMENT_TYPES = {"supplier_generation", "qogita_serving_snapshot", "qogita_membership_snapshot"}

CATALOG_SCHEMA = """
CREATE TABLE IF NOT EXISTS supplier_archive_segments (
    segment_id TEXT PRIMARY KEY,
    segment_type TEXT NOT NULL,
    object_id TEXT NOT NULL,
    archive_path TEXT NOT NULL UNIQUE,
    volume_uuid TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    source_db_fingerprint TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    archive_file_sha256 TEXT NOT NULL,
    archive_file_size INTEGER NOT NULL,
    archive_mtime_ns INTEGER NOT NULL,
    row_counts_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('valid','invalid')),
    authority TEXT NOT NULL CHECK(authority IN ('archive')),
    created_at TEXT NOT NULL,
    verified_at TEXT NOT NULL,
    repointed_at TEXT,
    UNIQUE(segment_type, object_id)
);
CREATE INDEX IF NOT EXISTS idx_supplier_archive_object
ON supplier_archive_segments(segment_type, object_id, status);
"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def initialize_supplier_archive_catalog(connection: sqlite3.Connection) -> None:
    connection.executescript(CATALOG_SCHEMA)


def source_db_fingerprint(connection: sqlite3.Connection) -> str:
    rows = connection.execute(
        "SELECT type,name,tbl_name,COALESCE(sql,'') FROM sqlite_master "
        "WHERE type IN ('table','index') ORDER BY type,name"
    ).fetchall()
    return hashlib.sha256(_canonical([tuple(row) for row in rows]).encode()).hexdigest()


def archive_catalog_record(
    database: str | Path, segment_type: str, object_id: str,
) -> dict[str, Any] | None:
    connection = sqlite3.connect(f"file:{Path(database).resolve()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='supplier_archive_segments'"
        ).fetchone()
        if not exists:
            return None
        row = connection.execute(
            "SELECT * FROM supplier_archive_segments "
            "WHERE segment_type=? AND object_id=? AND status='valid'",
            (segment_type, object_id),
        ).fetchone()
        return dict(row) if row else None
    finally:
        connection.close()


def _object_spec(segment_type: str, object_id: str) -> list[tuple[str, str, tuple[Any, ...], str]]:
    """Return table, WHERE, params and deterministic ORDER BY clauses."""
    if segment_type == "qogita_serving_snapshot":
        return [
            ("qogita_serving_snapshots", "serving_generation_id=?", (object_id,), "serving_generation_id"),
            ("qogita_serving_memberships", "serving_generation_id=?", (object_id,),
             "serving_generation_id,canonical_product_key"),
        ]
    if segment_type == "qogita_membership_snapshot":
        return [
            ("qogita_membership_versions", "membership_version_id=?", (object_id,), "membership_version_id"),
            ("qogita_membership_entries", "membership_version_id=?", (object_id,),
             "membership_version_id,canonical_gtin"),
        ]
    if segment_type == "supplier_generation":
        return [
            ("supplier_catalog_runs", "run_id=?", (object_id,), "run_id"),
            ("supplier_catalog_products", "run_id=?", (object_id,), "run_id,canonical_product_key"),
            ("supplier_catalog_scenarios", "run_id=?", (object_id,), "run_id,scenario_id"),
            ("qogita_enrichment_queue", "run_id=?", (object_id,), "run_id,canonical_product_key,task_type"),
            ("supplier_generation_product_refs", "run_id=?", (object_id,), "run_id,canonical_product_key"),
            ("supplier_generation_scenario_refs", "run_id=?", (object_id,), "run_id,scenario_id"),
            ("qogita_webhook_events",
             "catalog_request_id IN (SELECT catalog_request_id FROM qogita_catalog_requests WHERE staging_run_id=?)",
             (object_id,), "event_key"),
            ("qogita_catalog_requests", "staging_run_id=?", (object_id,), "catalog_request_id"),
            ("qogita_bootstrap_runs", "staging_run_id=?", (object_id,), "bootstrap_run_id"),
            ("qogita_bootstrap_products", "staging_run_id=?", (object_id,),
             "bootstrap_run_id,canonical_product_key"),
            ("qogita_bootstrap_duty_cycles",
             "bootstrap_run_id IN (SELECT bootstrap_run_id FROM qogita_bootstrap_runs WHERE staging_run_id=?)",
             (object_id,), "bootstrap_run_id"),
            ("qogita_bootstrap_milestones",
             "bootstrap_run_id IN (SELECT bootstrap_run_id FROM qogita_bootstrap_runs WHERE staging_run_id=?)",
             (object_id,), "bootstrap_run_id,milestone"),
        ]
    raise ValueError(f"unsupported archive segment type: {segment_type}")


def _existing_spec(connection: sqlite3.Connection, segment_type: str, object_id: str):
    tables = {row[0] for row in connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )}
    return [item for item in _object_spec(segment_type, object_id) if item[0] in tables]


def object_row_counts(
    connection: sqlite3.Connection, segment_type: str, object_id: str,
) -> dict[str, int]:
    return {
        table: int(connection.execute(
            f"SELECT COUNT(*) FROM {table} WHERE {where}", params,
        ).fetchone()[0])
        for table, where, params, _order in _existing_spec(connection, segment_type, object_id)
    }


def object_tables(
    connection: sqlite3.Connection, segment_type: str, object_id: str,
) -> set[str]:
    return {
        table for table, _where, _params, _order
        in _existing_spec(connection, segment_type, object_id)
    }


def logical_object_checksum(
    connection: sqlite3.Connection, segment_type: str, object_id: str,
) -> str:
    digest = hashlib.sha256()
    for table, where, params, order in _existing_spec(connection, segment_type, object_id):
        columns = [str(row[1]) for row in connection.execute(f"PRAGMA table_info({table})")]
        digest.update(_canonical([table, columns]).encode())
        cursor = connection.execute(
            f"SELECT * FROM {table} WHERE {where} ORDER BY {order}", params,
        )
        while True:
            rows = cursor.fetchmany(1000)
            if not rows:
                break
            for row in rows:
                digest.update(_canonical(list(row)).encode())
                digest.update(b"\n")
    return digest.hexdigest()


def _copy_table_schema(source: sqlite3.Connection, target: sqlite3.Connection, table: str) -> None:
    row = source.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,),
    ).fetchone()
    if not row or not row[0]:
        raise ArchiveIntegrityError(f"source table schema missing: {table}")
    target.execute(str(row[0]))


def _copy_rows(
    source: sqlite3.Connection, target: sqlite3.Connection,
    table: str, where: str, params: tuple[Any, ...], order: str,
) -> int:
    columns = [str(row[1]) for row in source.execute(f"PRAGMA table_info({table})")]
    placeholders = ",".join("?" for _ in columns)
    quoted = ",".join(f'"{column}"' for column in columns)
    insert = f"INSERT INTO {table} ({quoted}) VALUES ({placeholders})"
    cursor = source.execute(f"SELECT {quoted} FROM {table} WHERE {where} ORDER BY {order}", params)
    count = 0
    while True:
        rows = cursor.fetchmany(2000)
        if not rows:
            return count
        target.executemany(insert, rows)
        count += len(rows)


def _archive_root(segment_type: str, mount_path: str | Path = DEFAULT_MOUNT_PATH) -> Path:
    branch = "Qogita" if segment_type.startswith("qogita_") else "Supplier"
    return Path(mount_path) / "Archive" / branch


def create_archive_segment(
    database: str | Path, segment_type: str, object_id: str, *,
    mount_path: str | Path = DEFAULT_MOUNT_PATH,
    expected_volume_uuid: str = DEFAULT_VOLUME_UUID,
    uuid_probe=None,
) -> dict[str, Any]:
    if segment_type not in SEGMENT_TYPES:
        raise ValueError(f"unsupported archive segment type: {segment_type}")
    validation = validate_archive_volume(
        mount_path=mount_path, expected_uuid=expected_volume_uuid, uuid_probe=uuid_probe,
    )
    root = _archive_root(segment_type, mount_path)
    staging = Path(mount_path) / "Staging" / "SQLite Tiering"
    root.mkdir(parents=True, exist_ok=True)
    staging.mkdir(parents=True, exist_ok=True)
    segment_id = f"{segment_type}-{object_id}-{uuid.uuid4().hex[:12]}"
    final_path = root / f"{segment_id}.sqlite3"
    temporary = staging / f".{segment_id}.tmp.sqlite3"
    source = sqlite3.connect(f"file:{Path(database).resolve()}?mode=ro", uri=True)
    target = None
    try:
        source.execute("BEGIN")
        fingerprint = source_db_fingerprint(source)
        source_counts = object_row_counts(source, segment_type, object_id)
        if not source_counts or not any(source_counts.values()):
            raise ArchiveIntegrityError(f"archive object is empty: {segment_type}:{object_id}")
        source_checksum = logical_object_checksum(source, segment_type, object_id)
        target = sqlite3.connect(temporary)
        target.execute("PRAGMA journal_mode=DELETE")
        target.execute("PRAGMA synchronous=FULL")
        target.execute("PRAGMA foreign_keys=OFF")
        for table, where, params, order in _existing_spec(source, segment_type, object_id):
            _copy_table_schema(source, target, table)
            copied = _copy_rows(source, target, table, where, params, order)
            if copied != source_counts[table]:
                raise ArchiveIntegrityError(f"row count changed during copy: {table}")
        target.execute(
            "CREATE TABLE supplier_archive_manifest ("
            "segment_id TEXT PRIMARY KEY,segment_type TEXT NOT NULL,object_id TEXT NOT NULL,"
            "schema_version INTEGER NOT NULL,source_db_fingerprint TEXT NOT NULL,"
            "volume_uuid TEXT NOT NULL,content_sha256 TEXT NOT NULL,row_counts_json TEXT NOT NULL,"
            "created_at TEXT NOT NULL)"
        )
        created = _now()
        target.execute(
            "INSERT INTO supplier_archive_manifest VALUES (?,?,?,?,?,?,?,?,?)",
            (segment_id, segment_type, object_id, SCHEMA_VERSION, fingerprint,
             expected_volume_uuid, source_checksum, _canonical(source_counts), created),
        )
        if segment_type == "qogita_serving_snapshot":
            target.execute("CREATE INDEX archive_qogita_membership_product ON qogita_serving_memberships(canonical_product_key)")
        elif segment_type == "supplier_generation":
            if "supplier_catalog_products" in source_counts:
                target.execute("CREATE INDEX archive_supplier_product ON supplier_catalog_products(canonical_product_key)")
            if "supplier_catalog_scenarios" in source_counts:
                target.execute("CREATE INDEX archive_supplier_scenario ON supplier_catalog_scenarios(canonical_product_key,scenario_id)")
        target.commit()
        target.close(); target = None
        integrity = sqlite3.connect(temporary)
        try:
            if integrity.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                raise ArchiveIntegrityError("archive integrity_check failed")
        finally:
            integrity.close()
        check = sqlite3.connect(f"file:{temporary}?mode=ro", uri=True)
        try:
            if object_row_counts(check, segment_type, object_id) != source_counts:
                raise ArchiveIntegrityError("archive row-count equivalence failed")
            if logical_object_checksum(check, segment_type, object_id) != source_checksum:
                raise ArchiveIntegrityError("archive logical checksum mismatch")
        finally:
            check.close()
        with temporary.open("rb") as handle:
            os.fsync(handle.fileno())
        os.replace(temporary, final_path)
        os.chmod(final_path, 0o640)
        directory = os.open(root, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        stat = final_path.stat()
        return {
            "segment_id": segment_id, "segment_type": segment_type, "object_id": object_id,
            "archive_path": str(final_path), "volume_uuid": validation["volume_uuid"],
            "schema_version": SCHEMA_VERSION, "source_db_fingerprint": fingerprint,
            "content_sha256": source_checksum, "archive_file_sha256": _sha256_file(final_path),
            "archive_file_size": stat.st_size, "archive_mtime_ns": stat.st_mtime_ns,
            "row_counts": source_counts, "created_at": created, "verified_at": _now(),
        }
    finally:
        if target is not None:
            target.close()
        source.close()
        if temporary.exists():
            temporary.unlink()


def verify_archive_segment(
    record: dict[str, Any], *, full_checksum: bool = True,
    mount_path: str | Path = DEFAULT_MOUNT_PATH,
    expected_volume_uuid: str = DEFAULT_VOLUME_UUID, uuid_probe=None,
) -> Path:
    validate_archive_volume(
        mount_path=mount_path, expected_uuid=expected_volume_uuid, uuid_probe=uuid_probe,
    )
    path = Path(str(record["archive_path"]))
    mount = Path(mount_path)
    if path.is_symlink() or not path.is_file():
        raise ArchiveStorageUnavailable(f"archive segment missing: {path}")
    try:
        path.resolve(strict=True).relative_to(mount.resolve(strict=True))
    except (OSError, ValueError) as error:
        raise ArchiveStorageUnavailable(
            f"archive segment is outside the validated volume: {path}"
        ) from error
    cursor = path.parent
    while cursor != mount:
        if cursor.is_symlink():
            raise ArchiveStorageUnavailable(f"archive path contains symlink: {cursor}")
        if cursor == cursor.parent:
            raise ArchiveStorageUnavailable(f"archive path escaped mounted volume: {path}")
        cursor = cursor.parent
    stat = path.stat()
    if int(record.get("archive_file_size") or -1) != stat.st_size:
        raise ArchiveIntegrityError("archive segment size changed")
    if int(record.get("archive_mtime_ns") or -1) != stat.st_mtime_ns:
        raise ArchiveIntegrityError("archive segment mtime changed")
    if full_checksum and _sha256_file(path) != record.get("archive_file_sha256"):
        raise ArchiveIntegrityError("archive segment SHA-256 mismatch")
    return path


def register_archive_segment(connection: sqlite3.Connection, archive: dict[str, Any]) -> None:
    initialize_supplier_archive_catalog(connection)
    connection.execute(
        """INSERT INTO supplier_archive_segments (
             segment_id,segment_type,object_id,archive_path,volume_uuid,schema_version,
             source_db_fingerprint,content_sha256,archive_file_sha256,archive_file_size,
             archive_mtime_ns,row_counts_json,status,authority,created_at,verified_at,repointed_at
           ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,'archive',?,?,?)""",
        (archive["segment_id"], archive["segment_type"], archive["object_id"],
         archive["archive_path"], archive["volume_uuid"], archive["schema_version"],
         archive["source_db_fingerprint"], archive["content_sha256"],
         archive["archive_file_sha256"], archive["archive_file_size"],
         archive["archive_mtime_ns"], _canonical(archive["row_counts"]), "valid",
         archive["created_at"], archive["verified_at"], _now()),
    )


@contextmanager
def archived_object_connection(
    database: str | Path, segment_type: str, object_id: str, *,
    mount_path: str | Path = DEFAULT_MOUNT_PATH,
    expected_volume_uuid: str = DEFAULT_VOLUME_UUID, uuid_probe=None,
) -> Iterator[sqlite3.Connection | None]:
    record = archive_catalog_record(database, segment_type, object_id)
    if not record:
        yield None
        return
    path = verify_archive_segment(
        record, full_checksum=False, mount_path=mount_path,
        expected_volume_uuid=expected_volume_uuid, uuid_probe=uuid_probe,
    )
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        yield connection
    finally:
        connection.close()
