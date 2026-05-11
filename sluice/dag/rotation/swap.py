from __future__ import annotations

import gzip
import os
import shutil
from datetime import datetime
from pathlib import Path

import aiosqlite

from sluice.dag._name import safe_name


async def _checkpoint(db_path: str) -> None:
    conn = await aiosqlite.connect(db_path)
    try:
        cur = await conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        row = await cur.fetchone()
        if row is not None and row[0]:
            raise RuntimeError(f"could not checkpoint busy database: {db_path}")
        await conn.commit()
    finally:
        await conn.close()


def _clean_companions(base_path: str) -> None:
    for suffix in ("-wal", "-shm"):
        path = Path(base_path + suffix)
        if path.exists():
            path.unlink()


def _stamp_from_iso(iso: str) -> str:
    dt = datetime.fromisoformat(iso)
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise ValueError(f"cycle_anchor_iso must be timezone-aware: {iso!r}")
    return dt.strftime("%Y%m%dT%H%M%S")


async def swap_active_file(
    *,
    old_active_path: str,
    new_active_path: str,
    archive_dir: str,
    task_name: str,
    cycle_anchor_iso: str,
) -> str:
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
    archive_gz_tmp = archive_gz.with_name(archive_gz.name + ".tmp")
    if archive_gz_tmp.exists():
        raise FileExistsError(
            f"stale gzip temp from a prior crash: {archive_gz_tmp}; remove manually, then retry"
        )

    await _checkpoint(old_active_path)
    await _checkpoint(new_active_path)
    _clean_companions(old_active_path)
    _clean_companions(new_active_path)

    os.replace(old_active_path, archive_temp)
    try:
        os.replace(new_active_path, old_active_path)
    except Exception as replace_exc:
        try:
            os.replace(archive_temp, old_active_path)
        except Exception as rollback_exc:
            raise RuntimeError(
                "failed to install new active DB and failed to restore old active DB; "
                f"inspect {archive_temp} and {old_active_path} manually"
            ) from rollback_exc
        raise replace_exc

    _clean_companions(old_active_path)
    try:
        with open(archive_temp, "rb") as src, gzip.open(archive_gz_tmp, "wb") as dst:
            shutil.copyfileobj(src, dst, length=1024 * 1024)
        os.replace(archive_gz_tmp, archive_gz)
    except Exception as exc:
        try:
            archive_gz_tmp.unlink(missing_ok=True)
        except Exception as cleanup_exc:
            exc.add_note(f"also failed to remove partial gzip {archive_gz_tmp}: {cleanup_exc}")
        raise
    archive_temp.unlink()
    return str(archive_gz)
