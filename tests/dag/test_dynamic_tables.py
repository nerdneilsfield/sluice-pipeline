from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest

from sluice.dag import db
from sluice.dag.dynamic_tables import (
    ensure_pipeline_sink_pending_table,
    ensure_pipeline_table,
    ensure_stage_table,
    migrate_task_dynamic_tables,
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
        names_cur = await conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
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
        cur = await conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
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
