from __future__ import annotations

import json
from pathlib import Path

import pytest

from sluice.dag import db
from sluice.dag.runtime_lock import (
    RuntimeLock,
    RuntimeLockHeldError,
)
from sluice.dag.upgrade import (
    UpgradePhase,
    UpgradeService,
    read_runtime_marker,
)


async def _make_task_dir(tmp_path: Path) -> tuple[Path, Path, Path]:
    task_dir = tmp_path / "tasks" / "demo"
    task_dir.mkdir(parents=True)
    (task_dir / "task.toml").write_text(
        """
[task]
name = "demo"
db_path = "demo.db"
seen_db = "demo_seen.db"
enabled = true
timezone = "UTC"

[task.window]
anchor = "0 8 * * *"  
length = "24h"

[task.deployments]
pipelines = "all"
"""
    )
    (task_dir / "sources.toml").write_text(
        """
[[sources]]
name = "blog"
type = "rss"
url = "https://x"
"""
    )
    (task_dir / "stages.toml").write_text(
        """
[[stages]]
name = "fetch_blog"
type = "fetch_rss"

[[stages]]
name = "extract"
type = "dedupe"
"""
    )
    (task_dir / "sinks.toml").write_text("")
    (task_dir / "pipelines.toml").write_text(
        """
[[pipelines]]
name = "main"
cron = "0 8 * * *"
sources = ["blog"]
stages = ["fetch_blog", "extract"]
"""
    )

    abs_db = tmp_path / "demo.db"
    abs_seen = tmp_path / "demo_seen.db"
    from dataclasses import replace

    from sluice.dag.admin import migrate_task
    from sluice.dag.loader import load_task

    task_abs = replace(
        load_task(task_dir),
        db_path=str(abs_db),
        seen_db=str(abs_seen),
    )
    await migrate_task(task_abs)
    return task_dir, abs_db, abs_seen


@pytest.mark.asyncio
async def test_dry_run_validates_without_mutating(tmp_path: Path):
    task_dir, db_path, seen_path = await _make_task_dir(tmp_path)
    service = UpgradeService(task_dir=task_dir, db_path=db_path, seen_path=seen_path)
    result = await service.dry_run()
    assert result.ok is True
    async with db.open_active_db(str(db_path)) as conn:
        cur = await conn.execute("SELECT MAX(version) FROM schema_version")
        version = (await cur.fetchone())[0]
    assert version == 2


@pytest.mark.asyncio
async def test_backup_writes_db_copies_and_manifest(tmp_path: Path):
    task_dir, db_path, seen_path = await _make_task_dir(tmp_path)
    backup_dir = tmp_path / "backup"
    service = UpgradeService(task_dir=task_dir, db_path=db_path, seen_path=seen_path)
    result = await service.backup(backup_dir)
    assert result.ok is True
    assert (backup_dir / "active.db").exists()
    assert (backup_dir / "seen.db").exists()
    manifest_path = backup_dir / "backup_manifest.json"
    assert manifest_path.exists()
    manifest = json.loads(manifest_path.read_text())
    assert manifest["active"]["path"].endswith("active.db")
    assert manifest["seen"]["path"].endswith("seen.db")
    assert "hash" in manifest["active"]
    assert "hash" in manifest["seen"]


@pytest.mark.asyncio
async def test_backup_then_upgrade_advances_schema_to_v3(tmp_path: Path):
    task_dir, db_path, seen_path = await _make_task_dir(tmp_path)
    backup_dir = tmp_path / "backup"
    service = UpgradeService(task_dir=task_dir, db_path=db_path, seen_path=seen_path)
    await service.backup(backup_dir)
    await service.apply_fixed_migration()
    await service.apply_dynamic_migration()
    await service.write_runtime_marker(runtime_core="v3")
    async with db.open_active_db(str(db_path)) as conn:
        cur = await conn.execute("SELECT MAX(version) FROM schema_version")
        version = (await cur.fetchone())[0]
    assert version == 3
    marker = read_runtime_marker(task_dir)
    assert marker is not None
    assert marker["runtime_core"] == "v3"


@pytest.mark.asyncio
async def test_restore_validates_hashes_and_overwrites(tmp_path: Path):
    task_dir, db_path, seen_path = await _make_task_dir(tmp_path)
    backup_dir = tmp_path / "backup"
    service = UpgradeService(task_dir=task_dir, db_path=db_path, seen_path=seen_path)
    await service.backup(backup_dir)
    await service.apply_fixed_migration()
    await service.apply_dynamic_migration()
    await service.write_runtime_marker(runtime_core="v3")

    import aiosqlite
    async with aiosqlite.connect(str(db_path)) as conn:
        await conn.execute("DROP TABLE IF EXISTS items")
        await conn.commit()

    restore_service = UpgradeService(
        task_dir=task_dir, db_path=db_path, seen_path=seen_path
    )
    result = await restore_service.restore(backup_dir)
    assert result.ok is True

    async with db.open_active_db(str(db_path)) as conn:
        cur = await conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='items'"
        )
        items_exists = await cur.fetchone()
    assert items_exists is not None


@pytest.mark.asyncio
async def test_restore_rejects_corrupt_backup(tmp_path: Path):
    task_dir, db_path, seen_path = await _make_task_dir(tmp_path)
    backup_dir = tmp_path / "backup"
    backup_dir.mkdir()
    (backup_dir / "active.db").write_bytes(b"not a database")
    (backup_dir / "seen.db").write_bytes(b"not a database")
    (backup_dir / "backup_manifest.json").write_text(
        json.dumps(
            {
                "active": {"path": "active.db", "hash": "deadbeef", "schema": "v2"},
                "seen": {"path": "seen.db", "hash": "deadbeef", "schema": "v2"},
            }
        )
    )
    service = UpgradeService(task_dir=task_dir, db_path=db_path, seen_path=seen_path)
    result = await service.restore(backup_dir)
    assert result.ok is False


@pytest.mark.asyncio
async def test_runtime_lock_shared_then_exclusive_conflicts(tmp_path: Path):
    lock_path = tmp_path / ".sluice" / "runtime-core.lock"
    shared = RuntimeLock(lock_path.parent)
    shared._lock_path = lock_path
    shared.acquire_shared()
    try:
        excl = RuntimeLock(lock_path.parent)
        excl._lock_path = lock_path
        with pytest.raises(RuntimeLockHeldError):
            excl.acquire_exclusive()
    finally:
        shared.release()


@pytest.mark.asyncio
async def test_runtime_lock_exclusive_then_shared_conflicts(tmp_path: Path):
    lock_path = tmp_path / ".sluice" / "runtime-core.lock"
    excl = RuntimeLock(lock_path.parent)
    excl._lock_path = lock_path
    excl.acquire_exclusive()
    try:
        shared = RuntimeLock(lock_path.parent)
        shared._lock_path = lock_path
        with pytest.raises(RuntimeLockHeldError):
            shared.acquire_shared()
    finally:
        excl.release()


@pytest.mark.asyncio
async def test_runtime_marker_round_trip(tmp_path: Path):
    from sluice.dag.upgrade import read_runtime_marker, write_runtime_marker

    write_runtime_marker(
        task_dir=tmp_path,
        runtime_core="v3",
        schema_version=3,
        upgrade_id="uid-1",
        backup_manifest_hash="h",
    )
    marker = read_runtime_marker(tmp_path)
    assert marker is not None
    assert marker["runtime_core"] == "v3"
    assert marker["schema_version"] == 3
    assert marker["upgrade_id"] == "uid-1"
    assert marker["backup_manifest_hash"] == "h"


@pytest.mark.asyncio
async def test_upgrade_phase_manifest_records_progress(tmp_path: Path):
    task_dir, db_path, seen_path = await _make_task_dir(tmp_path)
    backup_dir = tmp_path / "backup"
    service = UpgradeService(task_dir=task_dir, db_path=db_path, seen_path=seen_path)
    await service.backup(backup_dir)
    phase = service.read_phase_manifest()
    assert phase is not None
    assert phase[-1]["phase"] == UpgradePhase.BACKUP_WRITTEN.value
