from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from sluice.dag import db
from sluice.dag.dynamic_tables import migrate_task_dynamic_tables
from sluice.dag.rotation.lock import RotationLock
from sluice.dag.rotation.orchestrator import rotate_task
from sluice.dag.sluice_config import RotationConfig, SluiceConfig
from sluice.dag.writer import WriterThread
from tests.dag.rotation.test_rotation_data import _seed_item, _task


@pytest.mark.asyncio
async def test_rotate_task_lock_busy(tmp_path: Path):
    task = _task(tmp_path)
    lock = RotationLock(task.db_path)
    lock.acquire()
    try:
        result = await rotate_task(task, config=SluiceConfig())
    finally:
        lock.release()

    assert result.status == "lock_busy"


@pytest.mark.asyncio
async def test_rotate_task_lock_busy_while_writer_is_idle(tmp_path: Path):
    task = _task(tmp_path)
    writer = WriterThread(active_path=task.db_path, seen_path=task.seen_db)
    await writer.start()
    try:
        result = await rotate_task(task, config=SluiceConfig())
    finally:
        await writer.stop()

    assert result.status == "lock_busy"


@pytest.mark.asyncio
async def test_rotate_task_no_op_empty_db(tmp_path: Path):
    task = _task(tmp_path)
    async with db.open_active_db(task.db_path) as conn:
        await migrate_task_dynamic_tables(conn, task)

    result = await rotate_task(task, config=SluiceConfig())

    assert result.status == "no_op"


@pytest.mark.asyncio
async def test_rotate_task_archives_terminal_and_carries_pending(tmp_path: Path):
    task = _task(tmp_path)
    archive_dir = tmp_path / "archive"
    async with db.open_active_db(task.db_path) as conn:
        await migrate_task_dynamic_tables(conn, task)
        await _seed_item(conn, "done")
        await _seed_item(conn, "pending")
        cur = await conn.execute(
            "INSERT INTO pipelines (run_key, task, pipeline_name, cycle_anchor, "
            "trigger, started_at, status) VALUES "
            "('rk', 't', 'p', 'c', 'manual', 'now', 'success')"
        )
        await conn.execute(
            "INSERT INTO pipeline_p (item_id, last_run_id, status, processed_at) "
            "VALUES ('done', ?, 'success', 'now')",
            (cur.lastrowid,),
        )
        await conn.commit()

    result = await rotate_task(
        task,
        config=SluiceConfig(
            rotation=RotationConfig(
                drain_timeout_seconds=1, archive_dir=str(archive_dir)
            )
        ),
        now=datetime(2026, 5, 4, 12, 0, 1, tzinfo=timezone.utc),
    )

    async with db.open_active_db(task.db_path) as conn:
        cur = await conn.execute("SELECT item_id FROM items")
        active_items = await cur.fetchall()
        cur = await conn.execute("SELECT closed FROM rotation_gate WHERE id = 1")
        gate = await cur.fetchone()
    async with db.open_seen_db(task.seen_db) as conn:
        cur = await conn.execute("SELECT item_id FROM item_seen")
        seen_items = await cur.fetchall()

    assert result.status == "rotated"
    assert result.archive_count == 1
    assert result.carryover_count == 1
    assert active_items == [("pending",)]
    assert seen_items == [("done",)]
    assert gate == (0,)
    assert result.archive_path is not None
    assert Path(result.archive_path).exists()


@pytest.mark.asyncio
async def test_rotate_task_cleans_new_db_family_on_pre_swap_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    task = _task(tmp_path)
    async with db.open_active_db(task.db_path) as conn:
        await migrate_task_dynamic_tables(conn, task)
        await _seed_item(conn, "done")
        cur = await conn.execute(
            "INSERT INTO pipelines (run_key, task, pipeline_name, cycle_anchor, "
            "trigger, started_at, status) VALUES "
            "('rk', 't', 'p', 'c', 'manual', 'now', 'success')"
        )
        await conn.execute(
            "INSERT INTO pipeline_p (item_id, last_run_id, status, processed_at) "
            "VALUES ('done', ?, 'success', 'now')",
            (cur.lastrowid,),
        )
        await conn.commit()

    async def fail_build_new_active(path, task):
        Path(path).write_bytes(b"partial")
        Path(path + "-wal").write_bytes(b"wal")
        Path(path + "-shm").write_bytes(b"shm")
        raise RuntimeError("build failed")

    monkeypatch.setattr(
        "sluice.dag.rotation.orchestrator.build_new_active",
        fail_build_new_active,
    )

    with pytest.raises(RuntimeError, match="build failed"):
        await rotate_task(task, config=SluiceConfig())

    assert not Path(str(task.db_path) + ".new").exists()
    assert not Path(str(task.db_path) + ".new-wal").exists()
    assert not Path(str(task.db_path) + ".new-shm").exists()
    async with db.open_active_db(task.db_path) as conn:
        cur = await conn.execute("SELECT closed FROM rotation_gate WHERE id = 1")
        assert await cur.fetchone() == (0,)


@pytest.mark.asyncio
async def test_rotate_task_reopens_gate_on_drain_timeout(tmp_path: Path):
    task = _task(tmp_path)
    async with db.open_active_db(task.db_path) as conn:
        await migrate_task_dynamic_tables(conn, task)
        await conn.execute(
            "INSERT INTO pipeline_admissions"
            "(admission_id, task, pipeline_name, registered_at) "
            "VALUES ('a1', 't', 'p', 'now')"
        )
        await conn.commit()

    result = await rotate_task(
        task,
        config=SluiceConfig(rotation=RotationConfig(drain_timeout_seconds=0)),
    )

    assert result.status == "drain_timeout"
    async with db.open_active_db(task.db_path) as conn:
        cur = await conn.execute("SELECT closed FROM rotation_gate WHERE id = 1")
        assert await cur.fetchone() == (0,)


@pytest.mark.asyncio
async def test_rotate_task_leaves_new_active_open_when_gzip_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    task = _task(tmp_path)
    archive_dir = tmp_path / "archive"
    async with db.open_active_db(task.db_path) as conn:
        await migrate_task_dynamic_tables(conn, task)
        await _seed_item(conn, "done")
        cur = await conn.execute(
            "INSERT INTO pipelines (run_key, task, pipeline_name, cycle_anchor, "
            "trigger, started_at, status) VALUES "
            "('rk', 't', 'p', 'c', 'manual', 'now', 'success')"
        )
        await conn.execute(
            "INSERT INTO pipeline_p (item_id, last_run_id, status, processed_at) "
            "VALUES ('done', ?, 'success', 'now')",
            (cur.lastrowid,),
        )
        await conn.commit()

    def fail_copy(src, dst, *, length=0):
        dst.write(b"partial")
        raise OSError("compress failed")

    monkeypatch.setattr("sluice.dag.rotation.swap.shutil.copyfileobj", fail_copy)

    with pytest.raises(OSError, match="compress failed"):
        await rotate_task(
            task,
            config=SluiceConfig(
                rotation=RotationConfig(
                    drain_timeout_seconds=1,
                    archive_dir=str(archive_dir),
                )
            ),
            now=datetime(2026, 5, 4, 12, 0, 1, tzinfo=timezone.utc),
        )

    async with db.open_active_db(task.db_path) as conn:
        cur = await conn.execute("SELECT item_id FROM items")
        assert await cur.fetchall() == []
        cur = await conn.execute("SELECT closed FROM rotation_gate WHERE id = 1")
        assert await cur.fetchone() == (0,)
    assert (archive_dir / "t-20260504T120001.db.tmp").exists()
