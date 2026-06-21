from __future__ import annotations

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
from sluice.dag.dynamic_tables import (
    migrate_task_dynamic_tables,
    upgrade_task_dynamic_tables,
)
from sluice.dag.eligibility import (
    select_eligible_items,
    select_entry_carryover,
)
from sluice.dag.rotation.partition import partition_items


def _v3_task(tmp_path: Path) -> TaskConfig:
    return TaskConfig(
        name="t",
        db_path=str(tmp_path / "active.db"),
        seen_db=str(tmp_path / "seen.db"),
        enabled=True,
        timezone="UTC",
        window=WindowDef(anchor="0 8 * * *", length="24h"),
        deployments=DeploymentsDef(pipelines="all"),
        sources=[SourceDef("feed", "rss", {"url": "x"})],
        sinks=[],
        stages=[StageDef("extract", "dedupe", {})],
        pipelines=[
            PipelineDef(
                name="main", cron="0 8 * * *", stages=["extract"],
                sources=["feed"], upstream_all=None, upstream_any=None,
                sinks=None, post_run_cleanup="none",
            )
        ],
        task_dir=str(tmp_path),
    )


async def _seed_v3(tmp_path: Path):
    task = _v3_task(tmp_path)
    async with db.open_active_db(task.db_path) as conn:
        await migrate_task_dynamic_tables(conn, task)
        await conn.commit()
        await db.apply_v3_migrations(conn)
        await upgrade_task_dynamic_tables(conn, task)
        # Insert one item and one run-scoped pipeline_P row
        await conn.execute(
            "INSERT INTO items(item_id, source_name, guid, url, title, ingested_at, "
            "ingested_by, updated_at, raw_json, dropped) VALUES "
            "('i1', 'feed', 'g1', 'u', 't', 'n', 'main', 'n', '{}', 0)"
        )
        await conn.execute(
            "INSERT INTO pipelines(run_key, task, pipeline_name, cycle_anchor, cycle_id, "
            "trigger, started_at, status) VALUES "
            "('rk1', 't', 'main', 'c', 'c', 'cron', 'n', 'success')"
        )
        run_id = (await (await conn.execute(
            "SELECT id FROM pipelines WHERE run_key='rk1'"
        )).fetchone())[0]
        await conn.execute(
            "INSERT INTO pipeline_main(run_id, item_id, cycle_id, status, processed_at) "
            "VALUES (?, 'i1', 'c', 'success', 'n')",
            (run_id,),
        )
        await conn.commit()
    return task


@pytest.mark.asyncio
async def test_select_entry_carryover_returns_no_rows_when_terminal_in_v3(tmp_path: Path):
    task = await _seed_v3(tmp_path)
    async with db.open_active_db(task.db_path) as conn:
        rows = await select_entry_carryover(conn, task.pipelines[0])
    assert rows == []


@pytest.mark.asyncio
async def test_partition_items_archives_terminal_items_in_v3(tmp_path: Path):
    task = await _seed_v3(tmp_path)
    async with db.open_active_db(task.db_path) as conn:
        partition = await partition_items(conn, task)
    assert "i1" in partition.archive_ids
    assert "i1" not in partition.carryover_ids


@pytest.mark.asyncio
async def test_partition_items_carries_over_non_terminal_items_in_v3(tmp_path: Path):
    task = _v3_task(tmp_path)
    async with db.open_active_db(task.db_path) as conn:
        await migrate_task_dynamic_tables(conn, task)
        await conn.commit()
        await db.apply_v3_migrations(conn)
        await upgrade_task_dynamic_tables(conn, task)
        await conn.execute(
            "INSERT INTO items(item_id, source_name, guid, url, title, ingested_at, "
            "ingested_by, updated_at, raw_json, dropped) VALUES "
            "('i2', 'feed', 'g2', 'u', 't', 'n', 'main', 'n', '{}', 0)"
        )
        await conn.commit()
    async with db.open_active_db(task.db_path) as conn:
        partition = await partition_items(conn, task)
    assert "i2" in partition.carryover_ids
    assert "i2" not in partition.archive_ids
