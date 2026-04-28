import pytest, re
from datetime import datetime, timezone, timedelta
from sluice.context import PipelineContext
from sluice.core.item import Item
from sluice.config import FilterRule
from sluice.processors.filter import FilterProcessor

def mk(**kw):
    base = dict(source_id="s", pipeline_id="p", guid="g", url="https://x",
                title="t", published_at=datetime(2026, 4, 28,
                tzinfo=timezone.utc), raw_summary=None)
    base.update(kw); return Item(**base)

def ctx_with(items):
    return PipelineContext("p", "p/r", "2026-04-28", items, {})

@pytest.mark.asyncio
async def test_keep_if_all():
    items = [mk(summary="x"*100), mk(summary="short"),
             mk(summary="x"*200)]
    p = FilterProcessor(name="f", mode="keep_if_all",
                        rules=[FilterRule(field="summary",
                                          op="min_length", value=50)])
    out = await p.process(ctx_with(items))
    assert len(out.items) == 2

@pytest.mark.asyncio
async def test_drop_if_any():
    items = [mk(title="ad: free stuff"), mk(title="real article")]
    p = FilterProcessor(name="f", mode="drop_if_any",
                        rules=[FilterRule(field="title",
                                          op="matches",
                                          value=r"(?i)\bad\b")])
    out = await p.process(ctx_with(items))
    assert [it.title for it in out.items] == ["real article"]

@pytest.mark.asyncio
async def test_numeric_gte():
    a = mk(); a.extras["score"] = 8
    b = mk(); b.extras["score"] = 3
    p = FilterProcessor(name="f", mode="keep_if_all",
                        rules=[FilterRule(field="extras.score",
                                          op="gte", value=5)])
    out = await p.process(ctx_with([a, b]))
    assert len(out.items) == 1 and out.items[0].extras["score"] == 8

@pytest.mark.asyncio
async def test_newer_than():
    fresh = mk(published_at=datetime.now(timezone.utc))
    stale = mk(published_at=datetime.now(timezone.utc) - timedelta(days=10))
    p = FilterProcessor(name="f", mode="keep_if_all",
                        rules=[FilterRule(field="published_at",
                                          op="newer_than", value="48h")])
    out = await p.process(ctx_with([fresh, stale]))
    assert len(out.items) == 1


def test_invalid_regex_raises():
    from sluice.processors.filter import _safe_search
    with pytest.raises(ValueError):
        _safe_search("[invalid", "text")


def test_long_pattern_rejected():
    from sluice.processors.filter import _safe_search
    with pytest.raises(ValueError):
        _safe_search("a" * 5001, "text")


@pytest.mark.asyncio
async def test_eq():
    items = [mk(title="a"), mk(title="b")]
    p = FilterProcessor(name="f", mode="keep_if_all",
                        rules=[FilterRule(field="title", op="eq", value="a")])
    out = await p.process(ctx_with(items))
    assert [it.title for it in out.items] == ["a"]


@pytest.mark.asyncio
async def test_not_in():
    items = [mk(title="a"), mk(title="b")]
    p = FilterProcessor(name="f", mode="keep_if_all",
                        rules=[FilterRule(field="title", op="not_in", value=["a"])])
    out = await p.process(ctx_with(items))
    assert [it.title for it in out.items] == ["b"]


@pytest.mark.asyncio
async def test_max_length():
    items = [mk(summary="x"*10), mk(summary="x"*200)]
    p = FilterProcessor(name="f", mode="keep_if_all",
                        rules=[FilterRule(field="summary", op="max_length", value=50)])
    out = await p.process(ctx_with(items))
    assert len(out.items) == 1


@pytest.mark.asyncio
async def test_exists():
    items = [mk(summary="x"), mk(summary=None)]
    p = FilterProcessor(name="f", mode="keep_if_all",
                        rules=[FilterRule(field="summary", op="exists")])
    out = await p.process(ctx_with(items))
    assert len(out.items) == 1


@pytest.mark.asyncio
async def test_contains():
    items = [mk(title="hello world"), mk(title="goodbye")]
    p = FilterProcessor(name="f", mode="keep_if_all",
                        rules=[FilterRule(field="title", op="contains", value="world")])
    out = await p.process(ctx_with(items))
    assert len(out.items) == 1
