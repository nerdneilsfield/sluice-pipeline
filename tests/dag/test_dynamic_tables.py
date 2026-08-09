from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest

from sluice.core.errors import UpgradeError
from sluice.dag import db
from sluice.dag.dynamic_tables import (
    ensure_pipeline_sink_pending_table,
    ensure_pipeline_table,
    ensure_stage_table,
    migrate_task_dynamic_tables,
    upgrade_task_dynamic_tables,
)


@dataclass(frozen=True)
class _Pipeline:
    name: str
    sinks: tuple[str, ...] = ()


@dataclass(frozen=True)
class _Stage:
    name: str


@dataclass(frozen=True)
class _Task:
    pipelines: tuple[_Pipeline, ...]
    stages: tuple[_Stage, ...]


async def _columns(conn, table: str) -> set[str]:
    cur = await conn.execute(f"PRAGMA table_info({table})")
    return {row[1] for row in await cur.fetchall()}


async def _indexes(conn, table: str) -> set[str]:
    cur = await conn.execute(f"PRAGMA index_list({table})")
    return {row[1] for row in await cur.fetchall()}


@pytest.mark.asyncio
async def test_ensure_pipeline_table_shape_and_status_constraint(tmp_path: Path):
    async with db.open_active_db(str(tmp_path / "active.db")) as conn:
        await ensure_pipeline_table(conn, "report_telegram")
        cols = await _columns(conn, "pipeline_report_telegram")
        indexes = await _indexes(conn, "pipeline_report_telegram")
        cur = await conn.execute(
            "INSERT INTO pipelines (run_key, task, pipeline_name, cycle_anchor, "
            "trigger, started_at, status) VALUES ('rk', 't', 'report_telegram', "
            "'c', 'cron', 'n', 'success')"
        )
        pipeline_run_id = cur.lastrowid
        await conn.execute(
            "INSERT INTO pipeline_report_telegram "
            "(item_id, last_run_id, status, processed_at) "
            "VALUES ('ok', ?, 'success', 'n')",
            (pipeline_run_id,),
        )
        with pytest.raises(Exception):
            await conn.execute(
                "INSERT INTO pipeline_report_telegram "
                "(item_id, last_run_id, status, processed_at) "
                "VALUES ('bad', ?, 'done', 'n')",
                (pipeline_run_id,),
            )

    assert {"item_id", "last_run_id", "status", "processed_at", "error_msg"} <= cols
    assert "idx_pipeline_report_telegram_status" in indexes


@pytest.mark.asyncio
async def test_ensure_pipeline_sink_pending_table_is_sibling(tmp_path: Path):
    async with db.open_active_db(str(tmp_path / "active.db")) as conn:
        await ensure_pipeline_sink_pending_table(conn, "report_telegram")
        names_cur = await conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        names = {row[0] for row in await names_cur.fetchall()}
        cols = await _columns(conn, "pipeline_report_telegram_sink_pending")
        indexes = await _indexes(conn, "pipeline_report_telegram_sink_pending")

    assert "pipeline_report_telegram_sink_pending" in names
    assert {"item_id", "run_key", "pending_since"} <= cols
    assert "idx_pipeline_report_telegram_sink_pending_runkey" in indexes


@pytest.mark.asyncio
async def test_ensure_stage_table_shape_pk_status_constraint_and_index(tmp_path: Path):
    async with db.open_active_db(str(tmp_path / "active.db")) as conn:
        await ensure_stage_table(conn, "summarize")
        cols = await _columns(conn, "stage_summarize")
        indexes = await _indexes(conn, "stage_summarize")
        cur = await conn.execute(
            "INSERT INTO pipelines (run_key, task, pipeline_name, cycle_anchor, "
            "trigger, started_at, status) VALUES ('rk', 't', 'p', 'c', 'cron', 'n', 'success')"
        )
        pipeline_run_id = cur.lastrowid
        cur = await conn.execute(
            "INSERT INTO stages (pipeline_run_id, stage_name, started_at, status) "
            "VALUES (?, 'summarize', 'n', 'success')",
            (pipeline_run_id,),
        )
        stage_run_id = cur.lastrowid
        await conn.execute(
            "INSERT INTO stage_summarize "
            "(pipeline_name, item_id, status, last_stage_run_id, "
            "last_pipeline_run_id, last_processed_at) "
            "VALUES ('p', 'item-1', 'done', ?, ?, 'n')",
            (stage_run_id, pipeline_run_id),
        )
        with pytest.raises(Exception):
            await conn.execute(
                "INSERT INTO stage_summarize "
                "(pipeline_name, item_id, status, last_stage_run_id, "
                "last_pipeline_run_id, last_processed_at) "
                "VALUES ('p', 'item-2', 'success', ?, ?, 'n')",
                (stage_run_id, pipeline_run_id),
            )
        with pytest.raises(Exception):
            await conn.execute(
                "INSERT INTO stage_summarize "
                "(pipeline_name, item_id, status, last_stage_run_id, "
                "last_pipeline_run_id, last_processed_at) "
                "VALUES ('p', 'item-1', 'skipped', ?, ?, 'n')",
                (stage_run_id, pipeline_run_id),
            )

    assert {
        "pipeline_name",
        "item_id",
        "status",
        "attempts",
        "last_stage_run_id",
        "last_pipeline_run_id",
        "last_processed_at",
        "last_error_class",
        "last_error_msg",
    } <= cols
    assert "idx_stage_summarize_status" in indexes


@pytest.mark.asyncio
async def test_migrate_task_dynamic_tables_creates_all_and_is_idempotent(
    tmp_path: Path,
):
    task = _Task(
        pipelines=(
            _Pipeline("fetch_blog"),
            _Pipeline("report_telegram", sinks=("telegram",)),
        ),
        stages=(_Stage("extract"), _Stage("summarize")),
    )

    async with db.open_active_db(str(tmp_path / "active.db")) as conn:
        await migrate_task_dynamic_tables(conn, task)
        await migrate_task_dynamic_tables(conn, task)
        cur = await conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        names = {row[0] for row in await cur.fetchall()}

    assert {
        "pipeline_fetch_blog",
        "pipeline_report_telegram",
        "pipeline_report_telegram_sink_pending",
        "stage_extract",
        "stage_summarize",
    } <= names


@pytest.mark.asyncio
async def test_dynamic_table_names_must_be_safe(tmp_path: Path):
    async with db.open_active_db(str(tmp_path / "active.db")) as conn:
        with pytest.raises(ValueError):
            await ensure_pipeline_table(conn, "bad-name")
        with pytest.raises(ValueError):
            await ensure_stage_table(conn, "bad name")


# ---------------------------------------------------------------------------
# v3 dynamic table upgrade
# ---------------------------------------------------------------------------


async def _seed_v2_pipeline_row(
    conn,
    *,
    item_id: str,
    last_run_id: int,
    status: str = "success",
    processed_at: str = "2026-06-21T00:00:00+00:00",
    error_msg: str | None = None,
) -> None:
    await conn.execute(
        "INSERT INTO pipeline_blog (item_id, last_run_id, status, processed_at, error_msg) "
        "VALUES (?, ?, ?, ?, ?)",
        (item_id, last_run_id, status, processed_at, error_msg),
    )


async def _seed_v2_stage_row(
    conn,
    *,
    pipeline_name: str,
    item_id: str,
    last_stage_run_id: int,
    last_pipeline_run_id: int,
    status: str = "done",
    attempts: int = 1,
) -> None:
    await conn.execute(
        "INSERT INTO stage_summarize "
        "(pipeline_name, item_id, status, attempts, last_stage_run_id, "
        "last_pipeline_run_id, last_processed_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            pipeline_name,
            item_id,
            status,
            attempts,
            last_stage_run_id,
            last_pipeline_run_id,
            "2026-06-21T00:00:00+00:00",
        ),
    )


async def _seed_pipeline_run(
    conn,
    *,
    run_key: str,
    cycle_anchor: str,
    run_id_returned: bool = True,
) -> int:
    cur = await conn.execute(
        "INSERT INTO pipelines (run_key, task, pipeline_name, cycle_anchor, trigger, "
        "started_at, status) VALUES (?, 't', 'blog', ?, 'cron', 'n', 'success')",
        (run_key, cycle_anchor),
    )
    if not run_id_returned:
        return 0
    return cur.lastrowid


async def _seed_stage_run(conn, *, pipeline_run_id: int) -> int:
    cur = await conn.execute(
        "INSERT INTO stages (pipeline_run_id, stage_name, started_at, status) "
        "VALUES (?, 'summarize', 'n', 'success')",
        (pipeline_run_id,),
    )
    return cur.lastrowid


def _blog_task() -> _Task:
    return _Task(
        pipelines=(_Pipeline("blog", sinks=("telegram",)),),
        stages=(_Stage("summarize"),),
    )


@pytest.mark.asyncio
async def test_upgrade_rebuilds_pipeline_table_to_run_scoped_pk(tmp_path: Path):
    async with db.open_active_db(str(tmp_path / "active.db")) as conn:
        await migrate_task_dynamic_tables(conn, _blog_task())
        run_id = await _seed_pipeline_run(conn, run_key="rk1", cycle_anchor="2026-06-21")
        await _seed_v2_pipeline_row(conn, item_id="i1", last_run_id=run_id)
        await conn.commit()

        await db.apply_v3_migrations(conn)
        await upgrade_task_dynamic_tables(conn, _blog_task())

        sql = (await (await conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='pipeline_blog'"
        )).fetchone())[0]
        cols = await _columns(conn, "pipeline_blog")
    assert "PRIMARY KEY (run_id, item_id)" in sql
    assert "cycle_id" in cols
    assert "run_id" in cols


@pytest.mark.asyncio
async def test_upgrade_rebuilds_stage_table_to_run_scoped_pk(tmp_path: Path):
    async with db.open_active_db(str(tmp_path / "active.db")) as conn:
        await migrate_task_dynamic_tables(conn, _blog_task())
        run_id = await _seed_pipeline_run(conn, run_key="rk1", cycle_anchor="2026-06-21")
        stage_run_id = await _seed_stage_run(conn, pipeline_run_id=run_id)
        await _seed_v2_stage_row(
            conn,
            pipeline_name="blog",
            item_id="i1",
            last_stage_run_id=stage_run_id,
            last_pipeline_run_id=run_id,
        )
        await conn.commit()

        await db.apply_v3_migrations(conn)
        await upgrade_task_dynamic_tables(conn, _blog_task())

        sql = (await (await conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='stage_summarize'"
        )).fetchone())[0]
        cols = await _columns(conn, "stage_summarize")
    assert "PRIMARY KEY (run_id, pipeline_name, item_id)" in sql
    assert "cycle_id" in cols
    assert "run_id" in cols


@pytest.mark.asyncio
async def test_upgrade_copies_run_id_and_cycle_id(tmp_path: Path):
    async with db.open_active_db(str(tmp_path / "active.db")) as conn:
        await migrate_task_dynamic_tables(conn, _blog_task())
        run_id = await _seed_pipeline_run(conn, run_key="rk1", cycle_anchor="2026-06-21")
        await _seed_v2_pipeline_row(conn, item_id="i1", last_run_id=run_id)
        await conn.commit()

        await db.apply_v3_migrations(conn)
        await upgrade_task_dynamic_tables(conn, _blog_task())

        cur = await conn.execute(
            "SELECT run_id, cycle_id FROM pipeline_blog WHERE item_id = ?", ("i1",)
        )
        row = await cur.fetchone()
    assert row == (run_id, "2026-06-21")


@pytest.mark.asyncio
async def test_upgrade_backfills_stage_attempt_log(tmp_path: Path):
    async with db.open_active_db(str(tmp_path / "active.db")) as conn:
        await migrate_task_dynamic_tables(conn, _blog_task())
        run_id = await _seed_pipeline_run(conn, run_key="rk1", cycle_anchor="2026-06-21")
        stage_run_id = await _seed_stage_run(conn, pipeline_run_id=run_id)
        await conn.execute(
            "INSERT INTO stage_attempt_log "
            "(stage_run_id, pipeline_run_id, pipeline_name, stage_name, item_id, "
            "attempt_no, status, processed_at) "
            "VALUES (?, ?, 'blog', 'summarize', 'i1', 1, 'success', 'n')",
            (stage_run_id, run_id),
        )
        await conn.commit()

        await db.apply_v3_migrations(conn)
        await upgrade_task_dynamic_tables(conn, _blog_task())

        cur = await conn.execute(
            "SELECT run_id, cycle_id FROM stage_attempt_log WHERE item_id = 'i1'"
        )
        row = await cur.fetchone()
    assert row == (run_id, "2026-06-21")


@pytest.mark.asyncio
async def test_upgrade_fails_closed_when_sink_pending_non_empty(tmp_path: Path):
    async with db.open_active_db(str(tmp_path / "active.db")) as conn:
        await migrate_task_dynamic_tables(conn, _blog_task())
        await conn.execute(
            "INSERT INTO pipeline_blog_sink_pending (item_id, run_key, pending_since) "
            "VALUES ('i1', 'rk1', 'now')"
        )
        await conn.commit()
        await db.apply_v3_migrations(conn)

        with pytest.raises(UpgradeError, match="sink_pending"):
            await upgrade_task_dynamic_tables(conn, _blog_task())


@pytest.mark.asyncio
async def test_upgrade_fails_closed_when_run_row_missing(tmp_path: Path):
    async with db.open_active_db(str(tmp_path / "active.db")) as conn:
        await migrate_task_dynamic_tables(conn, _blog_task())
        await conn.execute("PRAGMA foreign_keys = OFF")
        await _seed_v2_pipeline_row(conn, item_id="orphan", last_run_id=99999)
        await conn.execute("PRAGMA foreign_keys = ON")
        await conn.commit()
        await db.apply_v3_migrations(conn)

        with pytest.raises(UpgradeError, match="run row"):
            await upgrade_task_dynamic_tables(conn, _blog_task())


@pytest.mark.asyncio
async def test_upgrade_is_idempotent(tmp_path: Path):
    async with db.open_active_db(str(tmp_path / "active.db")) as conn:
        await migrate_task_dynamic_tables(conn, _blog_task())
        run_id = await _seed_pipeline_run(conn, run_key="rk1", cycle_anchor="2026-06-21")
        await _seed_v2_pipeline_row(conn, item_id="i1", last_run_id=run_id)
        await conn.commit()

        await db.apply_v3_migrations(conn)
        await upgrade_task_dynamic_tables(conn, _blog_task())
        await upgrade_task_dynamic_tables(conn, _blog_task())

        cur = await conn.execute("SELECT COUNT(*) FROM pipeline_blog")
        count = (await cur.fetchone())[0]
        cur = await conn.execute(
            "SELECT version FROM dynamic_schema_version WHERE table_name = 'pipeline_blog'"
        )
        version_row = await cur.fetchone()
    assert count == 1
    assert version_row is not None and version_row[0] >= 3


@pytest.mark.asyncio
async def test_upgrade_is_retryable_after_injected_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    async with db.open_active_db(str(tmp_path / "active.db")) as conn:
        await migrate_task_dynamic_tables(conn, _blog_task())
        run_id = await _seed_pipeline_run(conn, run_key="rk1", cycle_anchor="2026-06-21")
        await _seed_v2_pipeline_row(conn, item_id="i1", last_run_id=run_id)
        await conn.commit()
        await db.apply_v3_migrations(conn)

        original = upgrade_task_dynamic_tables
        call_count = {"n": 0}

        async def _flaky(conn, task):
            call_count["n"] += 1
            if call_count["n"] == 1:
                # corrupt by leaving a __new table around before failing
                await conn.execute(
                    "CREATE TABLE IF NOT EXISTS pipeline_blog__new (x INTEGER)"
                )
                await conn.commit()
                raise RuntimeError("injected")
            return await original(conn, task)

        with pytest.raises(RuntimeError, match="injected"):
            await _flaky(conn, _blog_task())
        # retry — must succeed
        await upgrade_task_dynamic_tables(conn, _blog_task())

        cur = await conn.execute("SELECT COUNT(*) FROM pipeline_blog")
        count = (await cur.fetchone())[0]
        cur = await conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='pipeline_blog__new'"
        )
        leftover = await cur.fetchone()
    assert count == 1
    assert leftover is None
