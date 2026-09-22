"""Fail-closed archive tier for completed Discovery jobs.

The operational Discovery database remains authoritative for current work.  A
completed job may be copied to one immutable SQLite segment on the verified
Glow Up Data volume and then registered in the small internal archive catalog.
Readers select exactly one authority; they never merge internal and archive
rows.
"""

from __future__ import annotations

import hashlib
import json
import os
import plistlib
import sqlite3
import subprocess
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator
from urllib.parse import quote


ARCHIVE_SCHEMA_VERSION = 1
ARCHIVE_ROLE = "glowup_data_historical_tier"
DEFAULT_VOLUME_UUID = "4DA73D64-40B6-4D99-B30A-0715B49DBBDF"
DEFAULT_MOUNT_PATH = Path("/Volumes/Glow Up Data")
DEFAULT_ARCHIVE_ROOT = DEFAULT_MOUNT_PATH / "Archive" / "Discovery"
DEFAULT_SENTINEL_NAME = ".glowup-history-volume.json"

JOB_TABLE = "discovery_incremental_jobs"
COMPONENT_TABLES = (
    "discovery_job_items",
    "discovery_purchase_scenarios",
    "discovery_listing_classifications",
    "discovery_catalog_results",
    "discovery_listings",
    "discovery_observations",
    "discovery_combinations",
    "discovery_resource_events",
)
ARCHIVE_TABLES = (JOB_TABLE, *COMPONENT_TABLES)

CATALOG_SCHEMA = """
CREATE TABLE IF NOT EXISTS discovery_archive_segments (
    job_id TEXT PRIMARY KEY,
    segment_id TEXT NOT NULL UNIQUE,
    archive_path TEXT NOT NULL UNIQUE,
    volume_uuid TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    source_db_fingerprint TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    archive_file_sha256 TEXT NOT NULL,
    archive_file_size INTEGER,
    archive_mtime_ns INTEGER,
    row_counts_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('valid','invalid')),
    created_at TEXT NOT NULL,
    verified_at TEXT NOT NULL
);
"""

ARCHIVE_MANIFEST_SCHEMA = """
CREATE TABLE IF NOT EXISTS discovery_archive_manifest (
    segment_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL UNIQUE,
    schema_version INTEGER NOT NULL,
    source_db_fingerprint TEXT NOT NULL,
    volume_uuid TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    row_counts_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
"""


class ArchiveStorageUnavailable(RuntimeError):
    code = "archive_storage_unavailable"

    def __init__(self, detail: str):
        super().__init__(f"{self.code}: {detail}")
        self.detail = detail


class ArchiveIntegrityError(RuntimeError):
    pass


class ArchivedJobReadOnlyError(RuntimeError):
    pass


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _is_beneath(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _reject_symlinks(path: Path, stop: Path) -> None:
    current = path
    stop = Path(stop)
    while True:
        if current.is_symlink():
            raise ArchiveStorageUnavailable(f"symlink path rejected: {current}")
        if current == stop:
            return
        if current.parent == current:
            raise ArchiveStorageUnavailable(f"path escapes mount: {path}")
        current = current.parent


def _diskutil_volume_uuid(mount_path: Path) -> str:
    completed = subprocess.run(
        ["/usr/sbin/diskutil", "info", "-plist", str(mount_path)],
        check=False, capture_output=True,
    )
    if completed.returncode != 0:
        raise ArchiveStorageUnavailable("diskutil could not verify archive volume")
    try:
        payload = plistlib.loads(completed.stdout)
    except (plistlib.InvalidFileException, ValueError) as error:
        raise ArchiveStorageUnavailable("invalid diskutil volume metadata") from error
    return str(payload.get("VolumeUUID") or payload.get("DiskUUID") or "")


def validate_archive_volume(
    *, mount_path: str | Path = DEFAULT_MOUNT_PATH,
    expected_uuid: str = DEFAULT_VOLUME_UUID,
    uuid_probe: Callable[[Path], str] | None = None,
) -> dict[str, Any]:
    """Validate the real APFS volume and sentinel without creating fallback paths."""
    mount = Path(mount_path)
    if mount.is_symlink() or not mount.is_dir() or not os.path.ismount(mount):
        raise ArchiveStorageUnavailable("expected archive volume is not mounted")
    _reject_symlinks(mount, mount)
    observed_uuid = (uuid_probe or _diskutil_volume_uuid)(mount)
    if observed_uuid.casefold() != str(expected_uuid).casefold():
        raise ArchiveStorageUnavailable("archive volume UUID mismatch")
    sentinel_path = mount / DEFAULT_SENTINEL_NAME
    if sentinel_path.is_symlink() or not sentinel_path.is_file():
        raise ArchiveStorageUnavailable("archive sentinel missing or unsafe")
    try:
        sentinel = json.loads(sentinel_path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError) as error:
        raise ArchiveStorageUnavailable("archive sentinel is unreadable") from error
    if str(sentinel.get("volume_uuid") or "").casefold() != str(expected_uuid).casefold():
        raise ArchiveStorageUnavailable("archive sentinel UUID mismatch")
    if sentinel.get("role") != ARCHIVE_ROLE:
        raise ArchiveStorageUnavailable("archive sentinel role mismatch")
    if not sentinel.get("contract_version") or not sentinel.get("schema_version"):
        raise ArchiveStorageUnavailable("archive sentinel version missing")
    return {
        "mounted": True,
        "verified": True,
        "volume_uuid": observed_uuid,
        "mount_path": str(mount),
        "sentinel_path": str(sentinel_path),
    }


def readonly_connection(path: str | Path) -> sqlite3.Connection:
    resolved = Path(path).resolve(strict=True)
    uri = f"file:{quote(str(resolved), safe='/')}?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=5.0)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    connection.execute("PRAGMA foreign_keys=ON")
    return connection


def initialize_archive_catalog(connection: sqlite3.Connection) -> None:
    connection.executescript(CATALOG_SCHEMA)
    columns = {str(row[1]) for row in connection.execute(
        "PRAGMA table_info(discovery_archive_segments)"
    )}
    if "archive_file_size" not in columns:
        connection.execute(
            "ALTER TABLE discovery_archive_segments ADD COLUMN archive_file_size INTEGER"
        )
    if "archive_mtime_ns" not in columns:
        connection.execute(
            "ALTER TABLE discovery_archive_segments ADD COLUMN archive_mtime_ns INTEGER"
        )
    for row in connection.execute(
        "SELECT job_id,archive_path FROM discovery_archive_segments "
        "WHERE archive_file_size IS NULL OR archive_mtime_ns IS NULL"
    ):
        try:
            details = Path(str(row[1])).stat()
        except OSError:
            continue
        connection.execute(
            "UPDATE discovery_archive_segments SET archive_file_size=?,archive_mtime_ns=? "
            "WHERE job_id=?",
            (int(details.st_size), int(details.st_mtime_ns), str(row[0])),
        )


def archive_catalog_record(
    internal_path: str | Path, job_id: str,
) -> dict[str, Any] | None:
    connection = readonly_connection(internal_path)
    try:
        exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='discovery_archive_segments'"
        ).fetchone()
        if not exists:
            return None
        row = connection.execute(
            "SELECT * FROM discovery_archive_segments WHERE job_id=? AND status='valid'",
            (job_id,),
        ).fetchone()
    finally:
        connection.close()
    return dict(row) if row else None


def _table_columns(connection: sqlite3.Connection, table: str) -> list[str]:
    return [str(row[1]) for row in connection.execute(f'PRAGMA table_info("{table}")')]


def _table_order(connection: sqlite3.Connection, table: str) -> str:
    info = list(connection.execute(f'PRAGMA table_info("{table}")'))
    primary = [str(row[1]) for row in sorted(info, key=lambda value: int(value[5] or 0)) if row[5]]
    if primary:
        return ",".join(f'"{value}"' for value in primary)
    return "rowid"


def job_row_counts(connection: sqlite3.Connection, job_id: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table in ARCHIVE_TABLES:
        exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,),
        ).fetchone()
        counts[table] = int(connection.execute(
            f'SELECT COUNT(*) FROM "{table}" WHERE job_id=?', (job_id,),
        ).fetchone()[0]) if exists else 0
    return counts


def logical_job_checksum(connection: sqlite3.Connection, job_id: str) -> str:
    digest = hashlib.sha256()
    for table in ARCHIVE_TABLES:
        columns = _table_columns(connection, table)
        if not columns:
            continue
        order = _table_order(connection, table)
        cursor = connection.execute(
            f'SELECT * FROM "{table}" WHERE job_id=? ORDER BY {order}', (job_id,),
        )
        digest.update((table + "\0" + "\0".join(columns) + "\0").encode("utf-8"))
        while rows := cursor.fetchmany(500):
            for row in rows:
                encoded = json.dumps(
                    [row[column] for column in columns], ensure_ascii=False,
                    separators=(",", ":"), default=str,
                ).encode("utf-8")
                digest.update(len(encoded).to_bytes(8, "big"))
                digest.update(encoded)
    return digest.hexdigest()


def source_database_fingerprint(connection: sqlite3.Connection, source_path: Path) -> str:
    payload = {
        "path": str(source_path.resolve()),
        "schema": [
            tuple(row) for row in connection.execute(
                "SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name"
            )
        ],
        "page_size": int(connection.execute("PRAGMA page_size").fetchone()[0]),
        "schema_version": int(connection.execute("PRAGMA schema_version").fetchone()[0]),
    }
    return hashlib.sha256(json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str,
    ).encode("utf-8")).hexdigest()


def _create_archive_schema(source: sqlite3.Connection, target: sqlite3.Connection) -> None:
    for table in ARCHIVE_TABLES:
        row = source.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,),
        ).fetchone()
        if not row or not row[0]:
            raise ArchiveIntegrityError(f"source table missing: {table}")
        target.execute(str(row[0]))
    target.executescript(ARCHIVE_MANIFEST_SCHEMA)
    for table in ARCHIVE_TABLES:
        for row in source.execute(
            "SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name=? "
            "AND sql IS NOT NULL ORDER BY name", (table,),
        ):
            target.execute(str(row[0]))


def _copy_job_table(
    source: sqlite3.Connection, target: sqlite3.Connection,
    table: str, job_id: str, *, batch_size: int,
) -> int:
    columns = _table_columns(source, table)
    names = ",".join(f'"{column}"' for column in columns)
    placeholders = ",".join("?" for _ in columns)
    cursor = source.execute(
        f'SELECT {names} FROM "{table}" WHERE job_id=? ORDER BY {_table_order(source, table)}',
        (job_id,),
    )
    copied = 0
    while rows := cursor.fetchmany(max(1, int(batch_size))):
        target.executemany(
            f'INSERT INTO "{table}" ({names}) VALUES ({placeholders})',
            [tuple(row[column] for column in columns) for row in rows],
        )
        copied += len(rows)
    return copied


def create_job_archive(
    *, source_path: str | Path, job_id: str,
    archive_root: str | Path = DEFAULT_ARCHIVE_ROOT,
    mount_path: str | Path = DEFAULT_MOUNT_PATH,
    expected_uuid: str = DEFAULT_VOLUME_UUID,
    uuid_probe: Callable[[Path], str] | None = None,
    batch_size: int = 500,
) -> dict[str, Any]:
    volume = validate_archive_volume(
        mount_path=mount_path, expected_uuid=expected_uuid, uuid_probe=uuid_probe,
    )
    source_path = Path(source_path).resolve(strict=True)
    root = Path(archive_root)
    mount = Path(mount_path).resolve(strict=True)
    root_resolved = root.resolve(strict=False)
    if not _is_beneath(root_resolved, mount):
        raise ArchiveStorageUnavailable("archive root is outside verified volume")
    _reject_symlinks(root, Path(mount_path))
    segment_id = f"discovery-{job_id}-v{ARCHIVE_SCHEMA_VERSION}"
    segment_dir = root / f"v{ARCHIVE_SCHEMA_VERSION}" / segment_id
    segment_dir.mkdir(parents=True, mode=0o750, exist_ok=True)
    _reject_symlinks(segment_dir, Path(mount_path))
    final_path = segment_dir / "discovery-job.sqlite3"
    if final_path.exists():
        raise ArchiveIntegrityError(f"archive segment already exists: {final_path}")
    temp_path = segment_dir / f".{final_path.name}.tmp-{uuid.uuid4().hex}"
    created_at = _now()
    try:
        with readonly_connection(source_path) as source:
            job = source.execute(
                "SELECT status FROM discovery_incremental_jobs WHERE job_id=?", (job_id,),
            ).fetchone()
            if not job or str(job[0]) != "completed":
                raise ArchiveIntegrityError("only completed Discovery jobs can be archived")
            source_counts = job_row_counts(source, job_id)
            source_checksum = logical_job_checksum(source, job_id)
            source_fingerprint = source_database_fingerprint(source, source_path)
            target = sqlite3.connect(temp_path)
            target.row_factory = sqlite3.Row
            try:
                target.execute("PRAGMA journal_mode=DELETE")
                target.execute("PRAGMA synchronous=FULL")
                target.execute("PRAGMA foreign_keys=OFF")
                target.execute("BEGIN IMMEDIATE")
                _create_archive_schema(source, target)
                copied = {
                    table: _copy_job_table(
                        source, target, table, job_id, batch_size=batch_size,
                    ) for table in ARCHIVE_TABLES
                }
                target.execute(
                    """INSERT INTO discovery_archive_manifest
                       (segment_id,job_id,schema_version,source_db_fingerprint,
                        volume_uuid,content_sha256,row_counts_json,created_at)
                       VALUES (?,?,?,?,?,?,?,?)""",
                    (
                        segment_id, job_id, ARCHIVE_SCHEMA_VERSION, source_fingerprint,
                        expected_uuid, source_checksum,
                        json.dumps(source_counts, sort_keys=True, separators=(",", ":")),
                        created_at,
                    ),
                )
                target.commit()
                target.execute("PRAGMA foreign_keys=ON")
                if target.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                    raise ArchiveIntegrityError("archive integrity_check failed")
                archive_counts = job_row_counts(target, job_id)
                archive_checksum = logical_job_checksum(target, job_id)
                if copied != source_counts or archive_counts != source_counts:
                    raise ArchiveIntegrityError("archive row-count equivalence failed")
                if archive_checksum != source_checksum:
                    raise ArchiveIntegrityError("archive logical checksum mismatch")
            finally:
                target.close()
        with temp_path.open("rb") as stream:
            os.fsync(stream.fileno())
        os.replace(temp_path, final_path)
        os.chmod(final_path, 0o640)
        directory_fd = os.open(segment_dir, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        file_checksum = _sha256_file(final_path)
        details = final_path.stat()
        return {
            "segment_id": segment_id,
            "job_id": job_id,
            "schema_version": ARCHIVE_SCHEMA_VERSION,
            "source_db_fingerprint": source_fingerprint,
            "volume_uuid": volume["volume_uuid"],
            "content_sha256": source_checksum,
            "archive_file_sha256": file_checksum,
            "archive_file_size": int(details.st_size),
            "archive_mtime_ns": int(details.st_mtime_ns),
            "row_counts": source_counts,
            "archive_path": str(final_path),
            "created_at": created_at,
            "equivalent": True,
        }
    except BaseException:
        if temp_path.exists():
            temp_path.unlink()
        raise


def verify_archive_record(
    record: dict[str, Any], *,
    mount_path: str | Path = DEFAULT_MOUNT_PATH,
    archive_root: str | Path = DEFAULT_ARCHIVE_ROOT,
    uuid_probe: Callable[[Path], str] | None = None,
    full_checksum: bool = True,
) -> Path:
    expected_uuid = str(record["volume_uuid"])
    validate_archive_volume(
        mount_path=mount_path, expected_uuid=expected_uuid, uuid_probe=uuid_probe,
    )
    mount = Path(mount_path).resolve(strict=True)
    root = Path(archive_root).resolve(strict=True)
    path = Path(str(record["archive_path"]))
    if path.is_symlink() or not path.is_file():
        raise ArchiveStorageUnavailable("archive segment missing or unsafe")
    resolved = path.resolve(strict=True)
    if not _is_beneath(root, mount) or not _is_beneath(resolved, root):
        raise ArchiveStorageUnavailable("archive segment escaped verified root")
    _reject_symlinks(path, Path(mount_path))
    details = resolved.stat()
    if record.get("archive_file_size") is not None and int(
        record["archive_file_size"]
    ) != int(details.st_size):
        raise ArchiveStorageUnavailable("archive segment checksum mismatch: size changed")
    if record.get("archive_mtime_ns") is not None and int(
        record["archive_mtime_ns"]
    ) != int(details.st_mtime_ns):
        raise ArchiveStorageUnavailable("archive segment checksum mismatch: mtime changed")
    if full_checksum and _sha256_file(resolved) != str(record["archive_file_sha256"]):
        raise ArchiveStorageUnavailable("archive segment checksum mismatch")
    with readonly_connection(resolved) as connection:
        if full_checksum and connection.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise ArchiveStorageUnavailable("archive segment quick_check failed")
        manifest = connection.execute(
            "SELECT * FROM discovery_archive_manifest WHERE job_id=?",
            (record["job_id"],),
        ).fetchone()
        if not manifest or str(manifest["content_sha256"]) != str(record["content_sha256"]):
            raise ArchiveStorageUnavailable("archive manifest mismatch")
    return resolved


def archive_descriptor(archive_path: str | Path) -> dict[str, Any]:
    """Rebuild the catalog descriptor from one completed immutable segment."""
    path = Path(archive_path).resolve(strict=True)
    with readonly_connection(path) as connection:
        row = connection.execute(
            "SELECT * FROM discovery_archive_manifest"
        ).fetchone()
        if not row:
            raise ArchiveIntegrityError("archive manifest missing")
        descriptor = dict(row)
    descriptor.update({
        "archive_path": str(path),
        "archive_file_sha256": _sha256_file(path),
        "archive_file_size": int(path.stat().st_size),
        "archive_mtime_ns": int(path.stat().st_mtime_ns),
        "row_counts": json.loads(descriptor.pop("row_counts_json")),
    })
    return descriptor


def register_archive_segment(
    internal_path: str | Path, archive: dict[str, Any], *,
    mount_path: str | Path = DEFAULT_MOUNT_PATH,
    archive_root: str | Path = DEFAULT_ARCHIVE_ROOT,
    uuid_probe: Callable[[Path], str] | None = None,
) -> None:
    verify_archive_record(
        archive, mount_path=mount_path, archive_root=archive_root,
        uuid_probe=uuid_probe,
    )
    connection = sqlite3.connect(Path(internal_path))
    try:
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("BEGIN IMMEDIATE")
        initialize_archive_catalog(connection)
        existing = connection.execute(
            "SELECT * FROM discovery_archive_segments WHERE job_id=?", (archive["job_id"],),
        ).fetchone()
        values = (
            archive["job_id"], archive["segment_id"], archive["archive_path"],
            archive["volume_uuid"], archive["schema_version"],
            archive["source_db_fingerprint"], archive["content_sha256"],
            archive["archive_file_sha256"],
            int(archive.get("archive_file_size") or Path(archive["archive_path"]).stat().st_size),
            int(archive.get("archive_mtime_ns") or Path(archive["archive_path"]).stat().st_mtime_ns),
            json.dumps(archive["row_counts"], sort_keys=True, separators=(",", ":")),
            "valid", archive["created_at"], _now(),
        )
        if existing:
            if tuple(existing[1:9]) != tuple(values[1:9]):
                raise ArchiveIntegrityError("different archive is already registered")
        else:
            connection.execute(
                """INSERT INTO discovery_archive_segments
                   (job_id,segment_id,archive_path,volume_uuid,schema_version,
                    source_db_fingerprint,content_sha256,archive_file_sha256,
                    archive_file_size,archive_mtime_ns,row_counts_json,status,created_at,verified_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", values,
            )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()


def remove_archive_registration(internal_path: str | Path, job_id: str) -> None:
    connection = sqlite3.connect(Path(internal_path))
    try:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute("DELETE FROM discovery_archive_segments WHERE job_id=?", (job_id,))
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()


def delete_internal_job_rows(
    internal_path: str | Path, job_id: str, *,
    mount_path: str | Path = DEFAULT_MOUNT_PATH,
    archive_root: str | Path = DEFAULT_ARCHIVE_ROOT,
    uuid_probe: Callable[[Path], str] | None = None,
) -> dict[str, int]:
    """Delete exactly one registered job after its immutable archive is authoritative."""
    catalog = archive_catalog_record(internal_path, job_id)
    if not catalog:
        raise ArchiveIntegrityError("valid archive registration required before delete")
    verify_archive_record(
        catalog, mount_path=mount_path, archive_root=archive_root,
        uuid_probe=uuid_probe,
    )
    connection = sqlite3.connect(Path(internal_path))
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("BEGIN IMMEDIATE")
        record = connection.execute(
            "SELECT * FROM discovery_archive_segments WHERE job_id=? AND status='valid'",
            (job_id,),
        ).fetchone()
        if not record:
            raise ArchiveIntegrityError("valid archive registration required before delete")
        counts = job_row_counts(connection, job_id)
        expected = json.loads(record["row_counts_json"])
        if counts != expected:
            raise ArchiveIntegrityError("internal row counts changed after archive verification")
        if logical_job_checksum(connection, job_id) != str(record["content_sha256"]):
            raise ArchiveIntegrityError("internal content changed after archive verification")
        for table in reversed(COMPONENT_TABLES):
            connection.execute(f'DELETE FROM "{table}" WHERE job_id=?', (job_id,))
        connection.execute("DELETE FROM discovery_incremental_jobs WHERE job_id=?", (job_id,))
        connection.commit()
        return counts
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()


def restore_job_from_archive(
    internal_path: str | Path, archive_path: str | Path, job_id: str,
) -> dict[str, int]:
    """Fixture/recovery primitive: atomically re-import one immutable segment."""
    source = readonly_connection(archive_path)
    target = sqlite3.connect(Path(internal_path))
    try:
        target.execute("PRAGMA foreign_keys=OFF")
        target.execute("BEGIN IMMEDIATE")
        existing = sum(job_row_counts(target, job_id).values())
        if existing:
            raise ArchiveIntegrityError("internal job rows already exist")
        counts = {}
        for table in ARCHIVE_TABLES:
            counts[table] = _copy_job_table(
                source, target, table, job_id, batch_size=500,
            )
        target.commit()
        return counts
    except BaseException:
        target.rollback()
        raise
    finally:
        target.close()
        source.close()


@contextmanager
def archived_read_connection(
    internal_path: str | Path, job_id: str, *,
    mount_path: str | Path = DEFAULT_MOUNT_PATH,
    archive_root: str | Path = DEFAULT_ARCHIVE_ROOT,
    expected_volume_uuid: str | None = None,
    uuid_probe: Callable[[Path], str] | None = None,
) -> Iterator[sqlite3.Connection | None]:
    record = archive_catalog_record(internal_path, job_id)
    if not record:
        yield None
        return
    if expected_volume_uuid and str(record["volume_uuid"]).casefold() != str(
        expected_volume_uuid
    ).casefold():
        raise ArchiveStorageUnavailable("archive catalog UUID differs from configuration")
    path = verify_archive_record(
        record, mount_path=mount_path, archive_root=archive_root,
        uuid_probe=uuid_probe, full_checksum=False,
    )
    connection = readonly_connection(path)
    try:
        yield connection
    finally:
        connection.close()
