import pytest
from datetime import datetime, timezone
from sluice.config import FieldOp
from sluice.context import PipelineContext
from sluice.core.item import Item
from sluice.processors.field_filter import FieldFilterProcessor


def mk(**kw):
    base = dict(
        source_id="s",
        pipeline_id="p",
        guid="g",
        url="https://x",
        title="t",
        published_at=datetime(2026, 4, 28, tzinfo=timezone.utc),
        raw_summary="hello" * 200,
        fulltext="abcdef" * 1000,
    )
    base.update(kw)
    return Item(**base)


@pytest.mark.asyncio
async def test_truncate_dataclass_field():
    p = FieldFilterProcessor(name="ff", ops=[FieldOp(op="truncate", field="fulltext", n=50)])
    ctx = PipelineContext("p", "p/r", "2026-04-28", [mk()], {})
    ctx = await p.process(ctx)
    assert len(ctx.items[0].fulltext) == 50


@pytest.mark.asyncio
async def test_drop_extras_field():
    it = mk()
    it.extras["junk"] = "x"
    p = FieldFilterProcessor(name="ff", ops=[FieldOp(op="drop", field="extras.junk")])
    ctx = await p.process(PipelineContext("p", "p/r", "2026-04-28", [it], {}))
    assert "junk" not in ctx.items[0].extras


@pytest.mark.asyncio
async def test_drop_dataclass_field_sets_none():
    p = FieldFilterProcessor(name="ff", ops=[FieldOp(op="drop", field="raw_summary")])
    ctx = await p.process(PipelineContext("p", "p/r", "2026-04-28", [mk()], {}))
    assert ctx.items[0].raw_summary is None
