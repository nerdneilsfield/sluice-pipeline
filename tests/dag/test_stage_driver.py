from __future__ import annotations

import pytest

from sluice.dag.config import PipelineDef, StageDef
from sluice.dag.item import Item
from sluice.dag.stage_context import ItemDrop, ItemError, StageResult
from sluice.dag.stage_driver import drive_stages
from sluice.dag.stage_executor import StageExecutor, bind_executor, reset_registry_for_tests


def _row(item_id):
    return {
        "item_id": item_id,
        "source_name": "s",
        "guid": None,
        "url": None,
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


class _DropExec(StageExecutor):
    async def execute(self, ctx):
        ctx.items[0].mark_dropped("forced")
        return StageResult(drops=[ItemDrop(ctx.items[0]["item_id"], "forced")])


class _MutateExec(StageExecutor):
    async def execute(self, ctx):
        for item in ctx.live_items():
            item["title"] = "MUT"
        return StageResult()


class _ErrExec(StageExecutor):
    async def execute(self, ctx):
        return StageResult(
            errors=[ItemError(item["item_id"], "Boom", "boom") for item in ctx.live_items()]
        )


class _FetchExec(StageExecutor):
    async def execute(self, ctx):
        new = Item.from_row(_row("new"))
        return StageResult(new_items=[new])


class _SortExec(StageExecutor):
    reorders_items = True

    async def execute(self, ctx):
        ctx.items.sort(key=lambda item: item["title"], reverse=True)
        return StageResult()


class _ReorderExec(StageExecutor):
    reorders_items = True

    async def execute(self, ctx):
        ctx.items.reverse()
        return StageResult()


class _LimitExec(StageExecutor):
    async def execute(self, ctx):
        drops = []
        for item in ctx.items[1:]:
            item.mark_dropped("limit")
            drops.append(ItemDrop(item["item_id"], "limit"))
        return StageResult(drops=drops)


@pytest.fixture(autouse=True)
def _registry():
    reset_registry_for_tests()
    bind_executor("test_drop", _DropExec)
    bind_executor("test_mutate", _MutateExec)
    bind_executor("test_err", _ErrExec)
    bind_executor("test_fetch", _FetchExec)
    bind_executor("test_reorder", _ReorderExec)
    bind_executor("sort", _SortExec)
    bind_executor("limit", _LimitExec)
    yield
    reset_registry_for_tests()


def _pipeline(stages):
    return PipelineDef(
        name="P",
        cron="0 8 * * *",
        stages=stages,
        sources=["src"],
        upstream_all=None,
        upstream_any=None,
        sinks=None,
        post_run_cleanup="none",
    )


def _stage(name, type_):
    return StageDef(name=name, type=type_, params={})


@pytest.mark.asyncio
async def test_driver_runs_in_order_and_propagates_drops():
    items = [Item.from_row(_row("a")), Item.from_row(_row("b"))]
    res = await drive_stages(
        task_name="t",
        pipeline=_pipeline(["s_drop", "s_mut"]),
        stage_defs={
            "s_drop": _stage("s_drop", "test_drop"),
            "s_mut": _stage("s_mut", "test_mutate"),
        },
        run_key="rk",
        cycle_anchor="c",
        items=items,
        resume_done={},
    )
    assert {d.item_id for d in res.drops_per_stage["s_drop"]} == {"a"}
    assert items[0].is_dropped()
    assert items[1]["title"] == "MUT"


@pytest.mark.asyncio
async def test_driver_done_ids_exclude_errored_items():
    items = [Item.from_row(_row("a")), Item.from_row(_row("b"))]
    res = await drive_stages(
        task_name="t",
        pipeline=_pipeline(["s_err", "s_mut"]),
        stage_defs={
            "s_err": _stage("s_err", "test_err"),
            "s_mut": _stage("s_mut", "test_mutate"),
        },
        run_key="rk",
        cycle_anchor="c",
        items=items,
        resume_done={},
    )
    assert res.input_ids_per_stage["s_err"] == {"a", "b"}
    assert res.done_ids_per_stage["s_err"] == set()
    assert res.input_ids_per_stage["s_mut"] == set()
    assert "a" not in res.done_ids_per_stage["s_mut"]


@pytest.mark.asyncio
async def test_driver_promotes_new_items_into_working_set():
    items: list[Item] = []
    res = await drive_stages(
        task_name="t",
        pipeline=_pipeline(["s_fetch", "s_mut"]),
        stage_defs={
            "s_fetch": _stage("s_fetch", "test_fetch"),
            "s_mut": _stage("s_mut", "test_mutate"),
        },
        run_key="rk",
        cycle_anchor="c",
        items=items,
        resume_done={},
    )
    assert len(res.new_items) == 1
    assert res.new_items[0]["title"] == "MUT"
    assert "new" in res.input_ids_per_stage["s_mut"]
    assert "new" in res.done_ids_per_stage["s_mut"]
    assert {item["item_id"] for item in res.final_working_set} == {"new"}


@pytest.mark.asyncio
async def test_driver_skips_done_for_persistent_stage_but_not_volatile():
    item = Item.from_row(_row("a"))
    res = await drive_stages(
        task_name="t",
        pipeline=_pipeline(["s_mut", "s_sort"]),
        stage_defs={
            "s_mut": _stage("s_mut", "test_mutate"),
            "s_sort": _stage("s_sort", "sort"),
        },
        run_key="rk",
        cycle_anchor="c",
        items=[item],
        resume_done={"s_mut": {"a"}, "s_sort": {"a"}},
    )
    assert item["title"] is None
    assert res.skipped_per_stage["s_mut"] == {"a"}
    assert res.skipped_per_stage["s_sort"] == set()


@pytest.mark.asyncio
async def test_driver_preserves_sort_order_for_later_stages():
    a = Item.from_row(_row("a"))
    b = Item.from_row(_row("b"))
    a["title"] = "A"
    b["title"] = "B"
    res = await drive_stages(
        task_name="t",
        pipeline=_pipeline(["s_sort", "s_limit"]),
        stage_defs={
            "s_sort": _stage("s_sort", "sort"),
            "s_limit": _stage("s_limit", "limit"),
        },
        run_key="rk",
        cycle_anchor="c",
        items=[a, b],
        resume_done={},
    )
    assert res.done_ids_per_stage["s_limit"] == {"b"}
    assert {drop.item_id for drop in res.drops_per_stage["s_limit"]} == {"a"}
    assert [item["item_id"] for item in res.final_working_set] == ["b", "a"]


@pytest.mark.asyncio
async def test_driver_uses_executor_reorder_marker():
    a = Item.from_row(_row("a"))
    b = Item.from_row(_row("b"))
    res = await drive_stages(
        task_name="t",
        pipeline=_pipeline(["s_reorder", "s_limit"]),
        stage_defs={
            "s_reorder": _stage("s_reorder", "test_reorder"),
            "s_limit": _stage("s_limit", "limit"),
        },
        run_key="rk",
        cycle_anchor="c",
        items=[a, b],
        resume_done={},
    )
    assert res.done_ids_per_stage["s_limit"] == {"b"}
    assert [item["item_id"] for item in res.final_working_set] == ["b", "a"]
