import pytest

from sluice.core.errors import ConfigError
from sluice.core.item import Item
from sluice.processors.limit import LimitProcessor
from tests.conftest import make_ctx


def _items(scores, source_ids=None):
    out = []
    for i, s in enumerate(scores):
        sid = source_ids[i] if source_ids else f"src{i}"
        it = Item(
            source_id=sid,
            pipeline_id="p",
            guid=f"g{i}",
            url=f"u{i}",
            title=f"t{i}",
            published_at=None,
            raw_summary=None,
        )
        if s is not None:
            it.extras["relevance"] = s
        out.append(it)
    return out


def _ctx(items):
    return make_ctx(items=items)


@pytest.mark.asyncio
async def test_basic_top_n_desc():
    p = LimitProcessor(
        name="L",
        top_n=2,
        sort_by="extras.relevance",
        sort_order="desc",
        sort_missing="last",
    )
    out = await p.process(_ctx(_items([0.1, 0.9, 0.5])))
    scores = [it.extras["relevance"] for it in out.items]
    assert scores == [0.9, 0.5]


@pytest.mark.asyncio
async def test_sort_missing_drop():
    p = LimitProcessor(
        name="L",
        top_n=10,
        sort_by="extras.relevance",
        sort_order="desc",
        sort_missing="drop",
    )
    out = await p.process(_ctx(_items([0.5, None, 0.3])))
    assert all("relevance" in it.extras for it in out.items)


@pytest.mark.asyncio
async def test_sort_missing_last_desc_keeps_missing_at_end():
    p = LimitProcessor(
        name="L",
        top_n=10,
        sort_by="extras.relevance",
        sort_order="desc",
        sort_missing="last",
    )
    out = await p.process(_ctx(_items([0.5, None, 0.3])))
    assert out.items[-1].extras.get("relevance") is None


@pytest.mark.asyncio
async def test_sort_missing_first_asc_keeps_missing_at_front():
    p = LimitProcessor(
        name="L",
        top_n=10,
        sort_by="extras.relevance",
        sort_order="asc",
        sort_missing="first",
    )
    out = await p.process(_ctx(_items([0.5, None, 0.3])))
    assert out.items[0].extras.get("relevance") is None


@pytest.mark.asyncio
async def test_group_by_per_group_max_then_top_n():
    items = _items([0.9, 0.8, 0.7, 0.4, 0.3], source_ids=["A", "A", "A", "B", "B"])
    p = LimitProcessor(
        name="L",
        top_n=3,
        sort_by="extras.relevance",
        sort_order="desc",
        sort_missing="last",
        group_by="source_id",
        per_group_max=2,
    )
    out = await p.process(_ctx(items))
    sids = [it.source_id for it in out.items]
    assert sids == ["A", "A", "B"]


@pytest.mark.asyncio
async def test_per_group_max_required_when_group_by_set():
    with pytest.raises(ConfigError):
        LimitProcessor(
            name="L",
            top_n=10,
            sort_by="extras.relevance",
            group_by="source_id",
        )
