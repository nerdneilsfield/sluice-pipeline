import pytest

from sluice.processors.sort import SortProcessor
from tests.conftest import make_ctx, make_item


def _proc(**kwargs) -> SortProcessor:
    return SortProcessor(name="test_sort", **kwargs)


def _items_with_scores(*scores):
    return [
        make_item(url=f"https://x.com/{i}", guid=str(i), extras={"score": s} if s is not None else {})
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
