from __future__ import annotations

import pytest

from sluice.dag import db
from sluice.dag.config import (
    DeploymentsDef,
    PipelineDef,
    StageDef,
    TaskConfig,
    WindowDef,
)
from sluice.dag.dynamic_tables import migrate_task_dynamic_tables
from sluice.dag.item import Item
from sluice.dag.phases.resume import load_resume_done
from sluice.dag.phases.run_key_resolve import resolve_run_key
from sluice.dag.phases.seen_post import build_seen_post_job
from sluice.dag.phases.skip_compute import compute_skip_ids
from sluice.dag.phases.tx1 import build_tx1_job
from sluice.dag.phases.tx2 import build_tx2_job
from sluice.dag.stage_context import ItemDrop
from sluice.dag.stage_driver import DriverResult
from sluice.dag.stores.sink_pending import SinkPendingStore
from sluice.dag.stores.stage_state import StageStateStore


def _row(item_id="i", **over):
    base = {
        "item_id": item_id,
        "source_name": "feed",
        "guid": None,
        "url": f"https://x/{item_id}",
        "title": None,
        "author": None,
        "published_at": None,
        "ingested_at": "2026-05-04T00:00:00+00:00",
        "ingested_by": "P",
        "updated_at": "2026-05-04T00:00:00+00:00",
        "tag": None,
        "raw_summary": None,
        "raw_content": None,
        "raw_json": "{}",
        "fulltext": None,
        "attachments_json": None,
        "summary": None,
        "score": None,
        "tags_json": None,
        "extras_json": None,
        "dropped": 0,
        "dropped_reason": None,
        "dropped_at": None,
    }
    base.update(over)
    return base


def _task(tmp_path, *, sinks=None):
    return TaskConfig(
        name="t",
        db_path=str(tmp_path / "active.db"),
        seen_db=str(tmp_path / "seen.db"),
        enabled=True,
        timezone="UTC",
        window=WindowDef(anchor="0 8 * * *", length="24h"),
        deployments=DeploymentsDef(pipelines="all"),
        sources=[],
        sinks=[],
        stages=[StageDef("S", "dedupe", {})],
        pipelines=[
            PipelineDef(
                name="P",
                cron="0 8 * * *",
                stages=["S"],
                sources=["feed"],
                upstream_all=None,
                upstream_any=None,
                sinks=sinks,
                post_run_cleanup="none",
            )
        ],
    )


@pytest.mark.asyncio
async def test_tx1_persists_new_item_dirty_and_stage_state_then_tx2_success(tmp_path):
    task = _task(tmp_path)
    pipeline = task.pipelines[0]
    stage_defs = {"S": task.stages[0]}
    item = Item.from_row(_row("i"))
    item["fulltext"] = "x"
    driver = DriverResult(
        done_ids_per_stage={"S": {"i"}},
        input_ids_per_stage={"S": {"i"}},
        new_items=[item],
        final_working_set=[item],
    )
    out1 = {}
    out2 = {}
    async with db.open_active_db(task.db_path) as active:
        await migrate_task_dynamic_tables(active, task)
        async with db.open_seen_db(task.seen_db) as seen:
            await build_tx1_job(
                task=task,
                pipeline=pipeline,
                stage_defs=stage_defs,
                run_key="rk",
                cycle_anchor="c",
                started_at="start",
                trigger="manual",
                items=[item],
                driver_result=driver,
                out=out1,
            ).run(active, seen)
            await build_tx2_job(
                pipeline=pipeline,
                stage_defs=stage_defs,
                pipeline_run_id=out1["pipeline_run_id"],
                run_key="rk",
                items=[item],
                driver_result=driver,
                emission_results=[],
                items_in=out1["items_in"],
                run_level_error=None,
                out=out2,
            ).run(active, seen)
        rec = await active.execute("SELECT fulltext FROM items WHERE item_id='i'")
        assert (await rec.fetchone())[0] == "x"
        cur = await active.execute("SELECT status FROM pipeline_P WHERE item_id='i'")
        assert (await cur.fetchone())[0] == "success"
        cur = await active.execute("SELECT attempts FROM stage_S WHERE item_id='i'")
        assert (await cur.fetchone())[0] == 1
    assert out2["status"] == "success"


@pytest.mark.asyncio
async def test_tx2_drop_only_run_is_success(tmp_path):
    task = _task(tmp_path)
    pipeline = task.pipelines[0]
    item = Item.from_row(_row("i"))
    driver = DriverResult(drops_per_stage={"S": [ItemDrop("i", "limit")]})
    async with db.open_active_db(task.db_path) as active:
        await migrate_task_dynamic_tables(active, task)
        async with db.open_seen_db(task.seen_db) as seen:
            run_id = await active.execute(
                "INSERT INTO pipelines(run_key, task, pipeline_name, cycle_anchor, "
                "trigger, started_at, status) VALUES ('rk', 't', 'P', 'c', 'manual', "
                "'start', 'running')"
            )
            out = {}
            await build_tx2_job(
                pipeline=pipeline,
                stage_defs={"S": task.stages[0]},
                pipeline_run_id=run_id.lastrowid,
                run_key="rk",
                items=[item],
                driver_result=driver,
                emission_results=[],
                items_in=1,
                run_level_error=None,
                out=out,
            ).run(active, seen)
    assert out["status"] == "success"


@pytest.mark.asyncio
async def test_seen_post_writes_seen_only(tmp_path):
    item = Item.from_row(_row("i"))
    async with db.open_active_db(str(tmp_path / "active.db")) as active:
        async with db.open_seen_db(str(tmp_path / "seen.db")) as seen:
            await build_seen_post_job([item]).run(active, seen)
            cur = await seen.execute("SELECT item_id FROM item_seen")
            assert (await cur.fetchone())[0] == "i"


@pytest.mark.asyncio
async def test_run_key_skip_and_resume_helpers(tmp_path):
    task = _task(tmp_path, sinks=["out"])
    async with db.open_active_db(task.db_path) as active:
        await migrate_task_dynamic_tables(active, task)
        pending = SinkPendingStore(active, "P")
        await pending.upsert(item_id="a", run_key="rk_old", pending_since="1")
        await pending.upsert(item_id="b", run_key="rk_new", pending_since="2")
        assert await resolve_run_key(active, "P", "fresh") == ("rk_old", ["b"])
        await active.execute(
            "INSERT INTO items(item_id, source_name, ingested_at, ingested_by, "
            "updated_at, raw_json, dropped) VALUES ('x', 'feed', 't', 'P', 't', '{}', 1)"
        )
        async with db.open_seen_db(task.seen_db) as seen:
            await seen.execute(
                "INSERT INTO item_seen(item_id, source_hash, archived_at) VALUES ('y', 'h', 't')"
            )
            assert await compute_skip_ids(active, seen, task.pipelines[0]) >= {"x", "y"}
        cur = await active.execute(
            "INSERT INTO pipelines(run_key, task, pipeline_name, cycle_anchor, "
            "trigger, started_at, status) VALUES ('rk', 't', 'P', 'c', 'manual', "
            "'start', 'running')"
        )
        stage_cur = await active.execute(
            "INSERT INTO stages(pipeline_run_id, stage_name, started_at, status) "
            "VALUES (?, 'S', 'start', 'success')",
            (cur.lastrowid,),
        )
        await StageStateStore(active, "S").upsert(
            pipeline_name="P",
            item_id="done",
            status="done",
            attempts=1,
            last_stage_run_id=stage_cur.lastrowid,
            last_pipeline_run_id=cur.lastrowid,
            last_processed_at="now",
            last_error_class=None,
            last_error_msg=None,
        )
        assert await load_resume_done(
            active,
            pipeline_name="P",
            stage_names=["S"],
            stage_types={"S": "dedupe"},
            item_ids=["done", "other"],
        ) == {"S": {"done"}}
