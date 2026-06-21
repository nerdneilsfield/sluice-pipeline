from __future__ import annotations

import gzip
import os
import shutil
from datetime import datetime
from pathlib import Path

import aiosqlite

from sluice.dag._name import safe_name
from sluice.dag.rotation.manifest import fsync_db_file


async def _checkpoint(db_path: str) -> None:
    """WAL checkpoint (TRUNCATE) with PRAGMA busy_timeout set."""
    conn = await aiosqlite.connect(db_path)
    try:
        await conn.execute("PRAGMA busy_timeout = 5000")
        cur = await conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        row = await cur.fetchone()
        if row is not None and row[0]:
            raise RuntimeError(
                f"could not checkpoint busy database: {db_path} (busy={row[0]})"
            )
        await conn.commit()
    finally:
        await conn.close()


async def integrity_check(db_path: str) -> None:
    """Raise RuntimeError if PRAGMA integrity_check does not return 'ok'."""
    conn = await aiosqlite.connect(db_path)
    try:
        await conn.execute("PRAGMA busy_timeout = 5000")
        cur = await conn.execute("PRAGMA integrity_check")
        row = await cur.fetchone()
        if row is None or row[0] != "ok":
            raise RuntimeError(
                f"integrity_check failed for {db_path}: {row}"
            )
    finally:
        await conn.close()


def _clean_companions(base_path: str) -> None:
    for suffix in ("-wal", "-shm"):
        path = Path(base_path + suffix)
        if path.exists():
            path.unlink()


def _fsync_parent(path: Path | str) -> None:
    """fsync the parent directory so rename operations are durable."""
    parent = Path(path).parent
    fd = os.open(str(parent), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _stamp_from_iso(iso: str) -> str:
    dt = datetime.fromisoformat(iso)
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise ValueError(f"cycle_anchor_iso must be timezone-aware: {iso!r}")
    return dt.strftime("%Y%m%dT%H%M%S")


def archive_old_active(
    *, old_active_path: str, archive_temp: Path
) -> None:
    """Step 1 of swap: rename old active DB to archive_temp.

    On success, the old path no longer exists. The caller is responsible
    for fsyncing the parent directory via _fsync_parent.
    """
    os.replace(old_active_path, archive_temp)
    _fsync_parent(archive_temp)


def install_new_active(
    *, new_active_path: str, old_active_path: str
) -> None:
    """Step 2 of swap: rename new active DB into the live path.

    The caller MUST have verified new_active_path integrity and fsynced
    it before calling. On success, new_active_path no longer exists at
    its old location.
    """
    os.replace(new_active_path, old_active_path)
    _fsync_parent(old_active_path)


def restore_old_active(
    *, archive_temp: Path, old_active_path: str
) -> None:
    """Rollback: restore the old active DB from archive_temp."""
    os.replace(archive_temp, old_active_path)
    _fsync_parent(old_active_path)


def gzip_archive(
    *, archive_temp: Path, archive_gz: Path
) -> None:
    """gzip archive_temp to archive_gz atomically, then unlink archive_temp."""
    archive_gz_tmp = archive_gz.with_name(archive_gz.name + ".tmp")
    try:
        with open(archive_temp, "rb") as src, gzip.open(
            archive_gz_tmp, "wb"
        ) as dst:
            shutil.copyfileobj(src, dst, length=1024 * 1024)
        _fsync_file(archive_gz_tmp)
        os.replace(archive_gz_tmp, archive_gz)
        _fsync_parent(archive_gz)
    except Exception:
        try:
            archive_gz_tmp.unlink(missing_ok=True)
        except OSError as cleanup_exc:
            import logging

            logging.getLogger(__name__).warning(
                "could not clean up gzip tmp %s: %s",
                archive_gz_tmp,
                cleanup_exc,
            )
        raise
    archive_temp.unlink()


def _fsync_file(path: Path) -> None:
    """Open file read-only and fsync it (durable rename semantics)."""
    fd = os.open(str(path), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


async def swap_active_file(
    *,
    old_active_path: str,
    new_active_path: str,
    archive_dir: str,
    task_name: str,
    cycle_anchor_iso: str,
) -> str:
    """Swap the active DB: archive old, install new, gzip archive.

    Sequence (each file-mutating step is followed by parent fsync so
    a crash at any point leaves the filesystem in a recoverable state;
    the orchestrator's manifest tracks phase transitions):

    1. Pre-checks (no stale temps).
    2. Checkpoint both DBs (TRUNCATE); verify wal files absent.
    3. fsync new_active_path (ensure durability before install).
    4. archive_old_active: rename old → archive_temp; parent fsync.
    5. install_new_active: rename new → old; parent fsync.
       (If step 5 fails, restore_old_active rolls back.)
    6. _clean_companions on new active.
    7. gzip_archive: compress archive_temp to .gz; parent fsync.

    Returns the archive_gz path on success.
    """
    if Path(old_active_path).resolve() == Path(new_active_path).resolve():
        raise ValueError("old_active_path and new_active_path must differ")

    Path(archive_dir).mkdir(parents=True, exist_ok=True)
    stamp = _stamp_from_iso(cycle_anchor_iso)
    safe_task_name = safe_name(task_name)
    archive_temp = Path(archive_dir) / f"{safe_task_name}-{stamp}.db.tmp"
    archive_gz = Path(archive_dir) / f"{safe_task_name}-{stamp}.db.gz"

    if archive_temp.exists():
        raise FileExistsError(
            f"stale archive temp from a prior crash: {archive_temp}; "
            "if active DB is already fresh then swap succeeded and only gzip failed; "
            "inspect, gzip-or-remove manually, then retry"
        )
    if archive_gz.exists():
        raise FileExistsError(f"archive already exists: {archive_gz}")

    # Step 2: checkpoint both DBs so -wal files are merged.
    await _checkpoint(old_active_path)
    await _checkpoint(new_active_path)
    _clean_companions(old_active_path)
    _clean_companions(new_active_path)

    # Step 3: fsync new active to make its bytes durable on disk before
    # we install it as the live DB.
    fsync_db_file(new_active_path)

    # Steps 4-5: archive old, install new. Rollback on failure.
    archive_old_active(
        old_active_path=old_active_path, archive_temp=archive_temp
    )
    try:
        install_new_active(
            new_active_path=new_active_path, old_active_path=old_active_path
        )
    except Exception as replace_exc:
        try:
            restore_old_active(
                archive_temp=archive_temp, old_active_path=old_active_path
            )
        except Exception as rollback_exc:
            raise RuntimeError(
                "failed to install new active DB and failed to restore old active DB; "
                f"inspect {archive_temp} and {old_active_path} manually"
            ) from rollback_exc
        raise replace_exc

    # Step 6: clean WAL/SHM companions of the freshly installed active DB.
    _clean_companions(old_active_path)

    # Step 7: gzip archive and finalize.
    gzip_archive(archive_temp=archive_temp, archive_gz=archive_gz)
    return str(archive_gz)
