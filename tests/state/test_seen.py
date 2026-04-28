import pytest
from datetime import datetime, timezone
from sluice.state.db import open_db
from sluice.state.seen import SeenStore
from sluice.core.item import Item

def make_item(key: str) -> Item:
    return Item(source_id="s", pipeline_id="p", guid=key,
                url=f"https://x/{key}", title=key,
                published_at=datetime(2026, 4, 28, tzinfo=timezone.utc),
                raw_summary=None, summary="sum-" + key)

@pytest.mark.asyncio
async def test_mark_and_check(tmp_path):
    async with open_db(tmp_path / "d.db") as db:
        s = SeenStore(db)
        assert await s.is_seen("p", "k1") is False
        await s.mark_seen_batch("p", [(make_item("k1"), "k1")])
        assert await s.is_seen("p", "k1") is True

@pytest.mark.asyncio
async def test_filter_unseen(tmp_path):
    async with open_db(tmp_path / "d.db") as db:
        s = SeenStore(db)
        await s.mark_seen_batch("p", [(make_item("a"), "a")])
        unseen = await s.filter_unseen("p", ["a", "b", "c"])
        assert unseen == ["b", "c"]

@pytest.mark.asyncio
async def test_filter_unseen_large_batch(tmp_path):
    async with open_db(tmp_path / "d.db") as db:
        s = SeenStore(db)
        batch = [(make_item(f"k{i}"), f"k{i}") for i in range(1000)]
        await s.mark_seen_batch("p", batch)
        keys = [f"k{i}" for i in range(1500)]
        unseen = await s.filter_unseen("p", keys)
        assert len(unseen) == 500
        assert unseen[0] == "k1000"
