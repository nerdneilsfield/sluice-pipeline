from datetime import datetime, timezone

import pytest

from sluice.core.errors import ConfigError
from sluice.processors.sort import SortProcessor
from tests.conftest import make_ctx, make_item


def _proc(**kwargs) -> SortProcessor:
    return SortProcessor(name="test_sort", **kwargs)


def _items_with_scores(*scores):
    return [
        make_item(
            url=f"https://x.com/{i}",
            guid=str(i),
            extras={"score": s} if s is not None else {},
        )
        for i, s in enumerate(scores)
    ]


@pytest.mark.asyncio
async def test_sort_desc_by_score():
    proc = _proc(sort_by="extras.score", sort_order="desc")
    ctx = make_ctx(items=_items_with_scores(3, 7, 1, 9))
    ctx = await proc.process(ctx)
    scores = [it.extras["score"] for it in ctx.items]
    assert scores == [9, 7, 3, 1]


@pytest.mark.asyncio
async def test_sort_asc_by_score():
    proc = _proc(sort_by="extras.score", sort_order="asc")
    ctx = make_ctx(items=_items_with_scores(3, 7, 1, 9))
    ctx = await proc.process(ctx)
    scores = [it.extras["score"] for it in ctx.items]
    assert scores == [1, 3, 7, 9]


@pytest.mark.asyncio
async def test_sort_missing_last_by_default():
    proc = _proc(sort_by="extras.score", sort_order="desc", sort_missing="last")
    ctx = make_ctx(items=_items_with_scores(5, None, 8))
    ctx = await proc.process(ctx)
    scores = [it.extras.get("score") for it in ctx.items]
    assert scores == [8, 5, None]


@pytest.mark.asyncio
async def test_sort_missing_first():
    proc = _proc(sort_by="extras.score", sort_order="desc", sort_missing="first")
    ctx = make_ctx(items=_items_with_scores(5, None, 8))
    ctx = await proc.process(ctx)
    scores = [it.extras.get("score") for it in ctx.items]
    assert scores == [None, 8, 5]


@pytest.mark.asyncio
async def test_sort_missing_drop():
    proc = _proc(sort_by="extras.score", sort_order="desc", sort_missing="drop")
    ctx = make_ctx(items=_items_with_scores(5, None, 8))
    ctx = await proc.process(ctx)
    assert len(ctx.items) == 2
    assert all("score" in it.extras for it in ctx.items)


@pytest.mark.asyncio
async def test_sort_does_not_drop_items():
    proc = _proc(sort_by="extras.score", sort_order="desc")
    ctx = make_ctx(items=_items_with_scores(1, 2, 3))
    ctx = await proc.process(ctx)
    assert len(ctx.items) == 3


@pytest.mark.asyncio
async def test_sort_empty_batch():
    proc = _proc(sort_by="extras.score")
    ctx = make_ctx(items=[])
    ctx = await proc.process(ctx)
    assert ctx.items == []


@pytest.mark.asyncio
async def test_sort_stable_on_equal_scores():
    proc = _proc(sort_by="extras.score", sort_order="desc")
    items = [
        make_item(url=f"https://x.com/{i}", guid=str(i), extras={"score": 5})
        for i in range(3)
    ]
    ctx = make_ctx(items=items)
    ctx = await proc.process(ctx)
    assert len(ctx.items) == 3


@pytest.mark.asyncio
async def test_sort_by_string_field():
    proc = _proc(sort_by="title", sort_order="asc")
    items = [
        make_item(title="Beta", url="https://x.com/b", guid="b"),
        make_item(title="Alpha", url="https://x.com/a", guid="a"),
    ]

    ctx = await proc.process(make_ctx(items=items))

    assert [it.title for it in ctx.items] == ["Alpha", "Beta"]


@pytest.mark.asyncio
async def test_sort_type_string_keeps_numeric_titles_lexical():
    proc = _proc(sort_by="title", sort_type="string", sort_order="asc")
    items = [
        make_item(title="2", url="https://x.com/2", guid="2"),
        make_item(title="10", url="https://x.com/10", guid="10"),
    ]

    ctx = await proc.process(make_ctx(items=items))

    assert [it.title for it in ctx.items] == ["10", "2"]


@pytest.mark.asyncio
async def test_sort_type_number_sorts_numeric_strings():
    proc = _proc(sort_by="extras.score", sort_type="number", sort_order="asc")
    items = [
        make_item(url="https://x.com/10", guid="10", extras={"score": "10"}),
        make_item(url="https://x.com/2", guid="2", extras={"score": "2"}),
    ]

    ctx = await proc.process(make_ctx(items=items))

    assert [it.extras["score"] for it in ctx.items] == ["2", "10"]


@pytest.mark.asyncio
async def test_sort_by_datetime_field_desc():
    proc = _proc(sort_by="published_at", sort_type="datetime", sort_order="desc")
    old = make_item(url="https://x.com/old", guid="old")
    old.published_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    new = make_item(url="https://x.com/new", guid="new")
    new.published_at = datetime(2026, 1, 2, tzinfo=timezone.utc)

    ctx = await proc.process(make_ctx(items=[old, new]))

    assert [it.guid for it in ctx.items] == ["new", "old"]


@pytest.mark.asyncio
async def test_sort_raises_on_unsupported_value_type():
    proc = _proc(sort_by="extras.sort_key")
    item = make_item(extras={"sort_key": {"nested": 1}})

    with pytest.raises(ConfigError, match="sort"):
        await proc.process(make_ctx(items=[item]))


@pytest.mark.asyncio
async def test_sort_raises_on_mixed_value_types():
    proc = _proc(sort_by="extras.sort_key")
    items = [
        make_item(url="https://x.com/n", guid="n", extras={"sort_key": 1}),
        make_item(url="https://x.com/s", guid="s", extras={"sort_key": "alpha"}),
    ]

    with pytest.raises(ConfigError, match="mixed"):
        await proc.process(make_ctx(items=items))
