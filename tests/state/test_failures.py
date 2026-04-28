import pytest, json
from datetime import datetime, timezone
from sluice.state.db import open_db
from sluice.state.failures import FailureStore
from sluice.core.item import Item

def mk(k="k1"):
    return Item(source_id="s", pipeline_id="p", guid=k, url=f"https://x/{k}",
                title=k, published_at=datetime(2026, 4, 28, tzinfo=timezone.utc),
                raw_summary=None)

@pytest.mark.asyncio
async def test_record_and_increment(tmp_path):
    async with open_db(tmp_path / "d.db") as db:
        f = FailureStore(db)
        await f.record("p", "k1", mk(), stage="summarize",
                       error_class="LLMError", error_msg="boom",
                       max_retries=3)
        rows = await f.list("p")
        assert len(rows) == 1 and rows[0]["attempts"] == 1
        assert rows[0]["status"] == "failed"
        await f.record("p", "k1", mk(), stage="summarize",
                       error_class="LLMError", error_msg="boom2",
                       max_retries=3)
        rows = await f.list("p")
        assert rows[0]["attempts"] == 2

@pytest.mark.asyncio
async def test_dead_letter_after_max(tmp_path):
    async with open_db(tmp_path / "d.db") as db:
        f = FailureStore(db)
        for _ in range(3):
            await f.record("p", "k1", mk(), stage="x",
                           error_class="E", error_msg="m", max_retries=3)
        rows = await f.list("p")
        assert rows[0]["status"] == "dead_letter"

@pytest.mark.asyncio
async def test_resolve_and_requeue(tmp_path):
    async with open_db(tmp_path / "d.db") as db:
        f = FailureStore(db)
        await f.record("p", "k1", mk(), stage="x", error_class="E",
                       error_msg="m", max_retries=3)
        items = await f.requeue("p")
        assert len(items) == 1 and items[0].guid == "k1"
        await f.mark_resolved("p", "k1")
        rows = await f.list("p", status="resolved")
        assert len(rows) == 1
        assert await f.requeue("p") == []
