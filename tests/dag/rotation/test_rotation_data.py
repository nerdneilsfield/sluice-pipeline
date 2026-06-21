from __future__ import annotations

import gzip
import os
import sqlite3
from pathlib import Path

import pytest

from sluice.dag import db
from sluice.dag.config import (
    DeploymentsDef,
    PipelineDef,
    SinkDef,
    SourceDef,
    StageDef,
    TaskConfig,
    WindowDef,
)
from sluice.dag.dynamic_tables import migrate_task_dynamic_tables
from sluice.dag.rotation.copy import copy_carryover
from sluice.dag.rotation.new_active import build_new_active
from sluice.dag.rotation.partition import Partition, partition_items
from sluice.dag.rotation.seen_reconcile import reconcile_seen_for_archive
from sluice.dag.rotation.swap import _checkpoint, _stamp_from_iso, swap_active_file


def _task(tmp_path: Path) -> TaskConfig:
    return TaskConfig(
        name="t",
        db_path=str(tmp_path / "active.db"),
        seen_db=str(tmp_path / "seen.db"),
        enabled=True,
        timezone="UTC",
        window=WindowDef(anchor="0 8 * * *", length="24h"),
        deployments=DeploymentsDef(pipelines="all"),
        sources=[SourceDef(name="src", type="rss")],
        sinks=[SinkDef(name="sink", type="file_md")],
        stages=[StageDef(name="s1", type="fetch_rss")],
        pipelines=[
            PipelineDef(
                name="p",
                cron="* * * * *",
                stages=["s1"],
                sources=["src"],
                upstream_all=None,
                upstream_any=None,
                sinks=["sink"],
                post_run_cleanup=None,
            )
        ],
        task_dir=str(tmp_path),
    )


async def _seed_item(conn, item_id: str, *, dropped: int = 0) -> None:
    await conn.execute(
        """
        INSERT INTO items (
          item_id, source_name, guid, url, ingested_at, ingested_by, updated_at,
          raw_json, dropped
        ) VALUES (?, 'src', ?, ?, 'now', 'p', 'now', '{}', ?)
        """,
        (item_id, f"g-{item_id}", f"https://e/{item_id}", dropped),
    )


async def _seed_copy_state(conn, task: TaskConfig) -> None:
    await migrate_task_dynamic_tables(conn, task)
    await _seed_item(conn, "carry")
    await _seed_item(conn, "archive")
    cur = await conn.execute(
        "INSERT INTO pipelines (run_key, task, pipeline_name, cycle_anchor, "
        "trigger, started_at, status) VALUES "
        "('rk', 't', 'p', 'c', 'manual', 'now', 'success')"
    )
    pipeline_run_id = cur.lastrowid
    cur = await conn.execute(
        "INSERT INTO stages (pipeline_run_id, stage_name, started_at, status) "
        "VALUES (?, 's1', 'now', 'done')",
        (pipeline_run_id,),
    )
    stage_run_id = cur.lastrowid
    await conn.execute(
        "INSERT INTO pipeline_p (item_id, last_run_id, status, processed_at) "
        "VALUES ('carry', ?, 'success', 'now')",
        (pipeline_run_id,),
    )
    await conn.execute(
        "INSERT INTO stage_s1 (pipeline_name, item_id, status, attempts, "
        "last_stage_run_id, last_pipeline_run_id, last_processed_at) "
        "VALUES ('p', 'carry', 'done', 1, ?, ?, 'now')",
        (stage_run_id, pipeline_run_id),
    )
    await conn.execute(
        "INSERT INTO stage_attempt_log (stage_run_id, pipeline_run_id, "
        "pipeline_name, stage_name, item_id, attempt_no, status, processed_at) "
        "VALUES (?, ?, 'p', 's1', 'carry', 1, 'done', 'now')",
        (stage_run_id, pipeline_run_id),
    )
    await conn.execute(
        "INSERT INTO pipeline_p_sink_pending(item_id, run_key, pending_since) "
        "VALUES ('carry', 'rk', 'now')"
    )
    await conn.execute(
        "INSERT INTO sink_emissions(run_key, sink_id, ordinal, sink_type, emitted_at) "
        "VALUES ('rk', 'sink', 0, 'file_md', 'now')"
    )
    await conn.execute(
        "INSERT INTO sink_delivery_log(run_key, sink_id, sink_type, attempt_at, "
        "ordinal, status) VALUES ('rk', 'sink', 'file_md', 'now', 0, 'success')"
    )
    await conn.execute(
        "INSERT INTO attachment_mirror(url_hash, url, local_path, fetched_at, "
        "last_referenced_at, task) VALUES ('h', 'https://e/a', '/tmp/a', 'now', "
        "'now', 't')"
    )
    await conn.commit()


@pytest.mark.asyncio
async def test_partition_archives_terminal_and_keeps_missing_terminal(tmp_path: Path):
    task = _task(tmp_path)
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
        partition = await partition_items(conn, task)

    assert partition.archive_ids == {"done"}
    assert partition.carryover_ids == {"pending"}


@pytest.mark.asyncio
async def test_copy_carryover_preserves_fk_state_and_attachment_cache(tmp_path: Path):
    task = _task(tmp_path)
    old_path = tmp_path / "old.db"
    new_path = tmp_path / "new.db"
    async with db.open_active_db(old_path) as conn:
        await _seed_copy_state(conn, task)

    await build_new_active(new_path, task)
    partition = Partition(
        carryover_ids={"carry"},
        archive_ids={"archive"},
        item_reach={"carry": frozenset({"p"}), "archive": frozenset({"p"})},
    )
    async with db.open_active_db(new_path) as conn:
        await copy_carryover(conn, old_path=str(old_path), partition=partition, task=task)
        await conn.commit()
        cur = await conn.execute("PRAGMA foreign_key_check")
        fk_errors = await cur.fetchall()
        cur = await conn.execute("SELECT item_id FROM items")
        item_ids = {row[0] for row in await cur.fetchall()}
        cur = await conn.execute("SELECT COUNT(*) FROM attachment_mirror")
        mirror_count = (await cur.fetchone())[0]
        cur = await conn.execute("SELECT COUNT(*) FROM sink_emissions")
        emission_count = (await cur.fetchone())[0]

    assert fk_errors == []
    assert item_ids == {"carry"}
    assert mirror_count == 1
    assert emission_count == 1


@pytest.mark.asyncio
async def test_copy_carryover_copies_attachment_cache_without_carryover(tmp_path: Path):
    task = _task(tmp_path)
    old_path = tmp_path / "old.db"
    new_path = tmp_path / "new.db"
    async with db.open_active_db(old_path) as conn:
        await _seed_copy_state(conn, task)

    await build_new_active(new_path, task)
    partition = Partition(carryover_ids=set(), archive_ids={"archive"}, item_reach={})
    async with db.open_active_db(new_path) as conn:
        await copy_carryover(conn, old_path=str(old_path), partition=partition, task=task)
        await conn.commit()
        cur = await conn.execute("SELECT COUNT(*) FROM attachment_mirror")
        mirror_count = (await cur.fetchone())[0]
        cur = await conn.execute("SELECT COUNT(*) FROM items")
        item_count = (await cur.fetchone())[0]

    assert mirror_count == 1
    assert item_count == 0


@pytest.mark.asyncio
async def test_reconcile_seen_for_archive_inserts_seen_rows(tmp_path: Path):
    active = tmp_path / "active.db"
    seen = tmp_path / "seen.db"
    async with db.open_active_db(active) as conn:
        await _seed_item(conn, "a")
        await conn.commit()

    offered = await reconcile_seen_for_archive(
        old_active_path=str(active),
        seen_path=str(seen),
        archive_ids={"a"},
        archived_at="now",
    )

    async with db.open_seen_db(seen) as conn:
        cur = await conn.execute("SELECT item_id FROM item_seen")
        rows = await cur.fetchall()
    assert offered == 1
    assert rows == [("a",)]


@pytest.mark.asyncio
async def test_swap_active_file_checkpoints_and_archives(tmp_path: Path):
    old = tmp_path / "active.db"
    new = tmp_path / "active.db.new"
    archive_dir = tmp_path / "archive"
    async with db.open_active_db(old) as conn:
        await _seed_item(conn, "old")
        await conn.commit()
    async with db.open_active_db(new) as conn:
        await _seed_item(conn, "new")
        await conn.commit()

    archive = await swap_active_file(
        old_active_path=str(old),
        new_active_path=str(new),
        archive_dir=str(archive_dir),
        task_name="t",
        cycle_anchor_iso="2026-05-04T12:00:01+00:00",
    )

    async with db.open_active_db(old) as conn:
        cur = await conn.execute("SELECT item_id FROM items")
        active_items = await cur.fetchall()

    unzipped = tmp_path / "archive.db"
    with gzip.open(archive, "rb") as src:
        unzipped.write_bytes(src.read())
    with sqlite3.connect(unzipped) as conn:
        archived_items = conn.execute("SELECT item_id FROM items").fetchall()

    assert _stamp_from_iso("2026-05-04T12:00:01+00:00") == "20260504T120001"
    assert active_items == [("new",)]
    assert archived_items == [("old",)]
    assert not Path(str(old) + "-wal").exists()
    assert not Path(str(new) + "-wal").exists()


def test_stamp_from_iso_rejects_naive_timestamp():
    with pytest.raises(ValueError, match="timezone-aware"):
        _stamp_from_iso("2026-05-04T12:00:01")


@pytest.mark.asyncio
async def test_swap_active_file_rejects_task_name_path_components(tmp_path: Path):
    old = tmp_path / "active.db"
    new = tmp_path / "active.db.new"
    archive_dir = tmp_path / "archive"
    async with db.open_active_db(old) as conn:
        await _seed_item(conn, "old")
        await conn.commit()
    async with db.open_active_db(new) as conn:
        await _seed_item(conn, "new")
        await conn.commit()

    with pytest.raises(ValueError, match="unsafe identifier"):
        await swap_active_file(
            old_active_path=str(old),
            new_active_path=str(new),
            archive_dir=str(archive_dir),
            task_name="../outside",
            cycle_anchor_iso="2026-05-04T12:00:01+00:00",
        )

    assert not (tmp_path / "outside-20260504T120001.db.gz").exists()


@pytest.mark.asyncio
async def test_swap_active_file_rejects_same_active_and_new_path(tmp_path: Path):
    active = tmp_path / "active.db"
    async with db.open_active_db(active) as conn:
        await _seed_item(conn, "old")
        await conn.commit()

    with pytest.raises(ValueError, match="must differ"):
        await swap_active_file(
            old_active_path=str(active),
            new_active_path=str(active),
            archive_dir=str(tmp_path / "archive"),
            task_name="t",
            cycle_anchor_iso="2026-05-04T12:00:01+00:00",
        )


@pytest.mark.asyncio
async def test_checkpoint_raises_when_sqlite_reports_busy(monkeypatch: pytest.MonkeyPatch):
    class FakeCursor:
        async def fetchone(self):
            return (1, 0, 0)

    class FakeConn:
        def __init__(self):
            self.calls = []

        async def execute(self, sql):
            self.calls.append(sql)
            return FakeCursor()

        async def commit(self):
            raise AssertionError("busy checkpoint must not commit")

        async def close(self):
            return None

    async def fake_connect(path):
        return FakeConn()

    monkeypatch.setattr("sluice.dag.rotation.swap.aiosqlite.connect", fake_connect)

    with pytest.raises(RuntimeError, match="busy database"):
        await _checkpoint("active.db")


@pytest.mark.asyncio
async def test_swap_active_file_propagates_failed_rollback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    old = tmp_path / "active.db"
    new = tmp_path / "active.db.new"
    archive_dir = tmp_path / "archive"
    async with db.open_active_db(old) as conn:
        await _seed_item(conn, "old")
        await conn.commit()
    async with db.open_active_db(new) as conn:
        await _seed_item(conn, "new")
        await conn.commit()

    real_replace = os.replace

    def fake_replace(src, dst):
        src_path = Path(src)
        dst_path = Path(dst)
        if src_path == new and dst_path == old:
            raise OSError("install failed")
        if src_path.name.endswith(".db.tmp") and dst_path == old:
            raise OSError("rollback failed")
        return real_replace(src, dst)

    monkeypatch.setattr("sluice.dag.rotation.swap.os.replace", fake_replace)

    with pytest.raises(RuntimeError, match="failed to install new active DB"):
        await swap_active_file(
            old_active_path=str(old),
            new_active_path=str(new),
            archive_dir=str(archive_dir),
            task_name="t",
            cycle_anchor_iso="2026-05-04T12:00:01+00:00",
        )


@pytest.mark.asyncio
async def test_swap_active_file_removes_partial_gzip_on_compress_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    old = tmp_path / "active.db"
    new = tmp_path / "active.db.new"
    archive_dir = tmp_path / "archive"
    async with db.open_active_db(old) as conn:
        await _seed_item(conn, "old")
        await conn.commit()
    async with db.open_active_db(new) as conn:
        await _seed_item(conn, "new")
        await conn.commit()

    def fail_copy(src, dst, *, length=0):
        dst.write(b"partial")
        raise OSError("compress failed")

    monkeypatch.setattr("sluice.dag.rotation.swap.shutil.copyfileobj", fail_copy)

    with pytest.raises(OSError, match="compress failed"):
        await swap_active_file(
            old_active_path=str(old),
            new_active_path=str(new),
            archive_dir=str(archive_dir),
            task_name="t",
            cycle_anchor_iso="2026-05-04T12:00:01+00:00",
        )

    assert not (archive_dir / "t-20260504T120001.db.gz").exists()
    assert not (archive_dir / "t-20260504T120001.db.gz.tmp").exists()
    assert (archive_dir / "t-20260504T120001.db.tmp").exists()
    async with db.open_active_db(old) as conn:
        cur = await conn.execute("SELECT item_id FROM items")
        assert await cur.fetchall() == [("new",)]
