from __future__ import annotations

import pytest

from sluice.dag.item import Item
from sluice.dag.stage_context import ItemDrop, ItemError, StageContext, StageResult
from sluice.dag.stage_executor import (
    StageExecutor,
    bind_executor,
    get_executor_class,
    is_bound,
    reset_registry_for_tests,
)


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


class _Exec(StageExecutor):
    type_name = "x"

    async def execute(self, ctx):
        return StageResult()


def test_live_items_excludes_dropped():
    a = Item.from_row(_row("a"))
    b = Item.from_row(_row("b"))
    a.mark_dropped("x")
    ctx = StageContext("t", "p", "s", "x", "rk", "c", [a, b], {})
    assert ctx.live_items() == [b]


def test_stage_result_ids():
    res = StageResult(
        drops=[ItemDrop("a", "f")],
        errors=[ItemError("b", "Boom", "boom")],
        rendered_output="out",
    )
    assert res.dropped_ids() == {"a"}
    assert res.errored_ids() == {"b"}
    assert res.rendered_output == "out"


def test_executor_registry():
    reset_registry_for_tests()
    bind_executor("x", _Exec)
    assert is_bound("x")
    assert get_executor_class("x") is _Exec
    with pytest.raises(ValueError):
        bind_executor("x", _Exec)
    reset_registry_for_tests()
