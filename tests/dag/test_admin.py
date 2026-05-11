from pathlib import Path

import aiosqlite
import pytest

from sluice.dag.admin import (
    kill_item,
    list_items,
    migrate_task,
    retry_pipeline,
    scaffold_from_v1,
    show_item,
)
from sluice.dag.loader import load_task
from tests.dag.test_helpers import (
    make_task_dir,
    seed_item,
    seed_pipeline_state,
    seed_stage_state,
)


@pytest.mark.asyncio
async def test_migrate_task_creates_schema_and_dynamic_tables(tmp_path: Path):
    task_dir = make_task_dir(tmp_path, name="t1", pipeline_names=["p1"], stage_names=["s1"])
    task = load_task(task_dir)

    await migrate_task(task)

    async with aiosqlite.connect(task.db_path) as db:
        rows = await (
            await db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        ).fetchall()
    names = {row[0] for row in rows}
    assert {"items", "pipelines", "pipeline_p1", "stage_s1"} <= names
    assert {"rotation_gate", "pipeline_admissions"} <= names


@pytest.mark.asyncio
async def test_retry_pipeline_clears_terminal_rows_for_single_item(tmp_path: Path):
    task_dir = make_task_dir(tmp_path, name="t1", pipeline_names=["p1"], stage_names=["s1"])
    task = load_task(task_dir)
    await migrate_task(task)
    await seed_pipeline_state(task.db_path, pipeline="p1", item_id="X", status="failed")
    await seed_stage_state(task.db_path, stage="s1", pipeline="p1", item_id="X", status="failed")

    cleared = await retry_pipeline(task, pipeline="p1", item_id="X")
    assert cleared == 1

    async with aiosqlite.connect(task.db_path) as db:
        rows = await (
            await db.execute("SELECT COUNT(*) FROM pipeline_p1 WHERE item_id='X'")
        ).fetchone()
        assert rows[0] == 0
        rows = await (
            await db.execute(
                "SELECT COUNT(*) FROM stage_s1 WHERE item_id='X' AND pipeline_name='p1'"
            )
        ).fetchone()
        assert rows[0] == 0


@pytest.mark.asyncio
async def test_retry_pipeline_without_item_clears_all(tmp_path: Path):
    task_dir = make_task_dir(tmp_path, name="t1", pipeline_names=["p1"], stage_names=["s1"])
    task = load_task(task_dir)
    await migrate_task(task)
    for item_id in ("A", "B"):
        await seed_pipeline_state(task.db_path, pipeline="p1", item_id=item_id, status="failed")

    cleared = await retry_pipeline(task, pipeline="p1", item_id=None)
    assert cleared == 2


@pytest.mark.asyncio
async def test_list_items_joins_pipeline_status(tmp_path: Path):
    task_dir = make_task_dir(tmp_path, name="t1", pipeline_names=["p1", "p2"], stage_names=["s1"])
    task = load_task(task_dir)
    await migrate_task(task)
    await seed_item(task.db_path, item_id="X", source_name="src1", title="hello")
    await seed_pipeline_state(task.db_path, pipeline="p1", item_id="X", status="success")
    await seed_pipeline_state(task.db_path, pipeline="p2", item_id="X", status="failed")

    rows = await list_items(task, limit=10)
    assert len(rows) == 1
    assert rows[0]["item_id"] == "X"
    assert rows[0]["title"] == "hello"
    assert rows[0]["p1"] == "success"
    assert rows[0]["p2"] == "failed"


@pytest.mark.asyncio
async def test_show_item_returns_full_provenance(tmp_path: Path):
    task_dir = make_task_dir(tmp_path, name="t1", pipeline_names=["p1"], stage_names=["s1"])
    task = load_task(task_dir)
    await migrate_task(task)
    await seed_item(task.db_path, item_id="X", source_name="src1", title="hello")
    await seed_pipeline_state(task.db_path, pipeline="p1", item_id="X", status="success")
    await seed_stage_state(task.db_path, stage="s1", pipeline="p1", item_id="X", status="done")

    provenance = await show_item(task, item_id="X")
    assert provenance["item"]["item_id"] == "X"
    assert provenance["pipelines"]["p1"]["status"] == "success"
    assert provenance["stages"]["s1"][0]["status"] == "done"


@pytest.mark.asyncio
async def test_kill_item_sets_global_tombstone(tmp_path: Path):
    task_dir = make_task_dir(tmp_path, name="t1", pipeline_names=["p1"], stage_names=["s1"])
    task = load_task(task_dir)
    await migrate_task(task)
    await seed_item(task.db_path, item_id="X", source_name="src1", title="hello")

    await kill_item(task, item_id="X", reason="operator")

    async with aiosqlite.connect(task.db_path) as db:
        db.row_factory = aiosqlite.Row
        row = await (
            await db.execute(
                "SELECT dropped, dropped_reason, dropped_at FROM items WHERE item_id='X'"
            )
        ).fetchone()
        assert row["dropped"] == 1
        assert row["dropped_reason"] == "operator"
        assert row["dropped_at"] is not None


def test_scaffold_emits_five_files(tmp_path: Path):
    old = tmp_path / "old.toml"
    old.write_text(
        """\
id = "demo"
description = "demo task"
enabled = true
cron = "0 8 * * *"
timezone = "Asia/Shanghai"
window = "24h"

[[sources]]
type = "rss"
url = "https://example.com/rss"
tag = "ai"

[[stages]]
name = "fetch"
type = "fetch_rss"

[[sinks]]
id = "local"
type = "file_md"
out = "./out"
"""
    )
    out_dir = tmp_path / "tasks" / "demo"

    scaffold_from_v1(old_path=old, out_dir=out_dir)

    for filename in ("task.toml", "sources.toml", "sinks.toml", "stages.toml", "pipelines.toml"):
        assert (out_dir / filename).exists()
    assert "https://example.com/rss" in (out_dir / "sources.toml").read_text()
    assert "demo_main" in (out_dir / "pipelines.toml").read_text()
