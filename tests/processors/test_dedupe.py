from datetime import datetime, timezone

import pytest

from sluice.context import PipelineContext
from sluice.core.item import Item
from sluice.processors.dedupe import DedupeProcessor
from sluice.state.db import open_db
from sluice.state.failures import FailureStore
from sluice.state.seen import SeenStore


def mk(guid=None, url="https://x/a", title="t"):
    return Item(
        source_id="s",
        pipeline_id="p",
        guid=guid,
        url=url,
        title=title,
        published_at=datetime(2026, 4, 28, tzinfo=timezone.utc),
        raw_summary=None,
    )


@pytest.mark.asyncio
async def test_dedupe_drops_seen_keeps_unseen(tmp_path):
    async with open_db(tmp_path / "d.db") as db:
        seen = SeenStore(db)
        fail = FailureStore(db)
        await seen.mark_seen_batch("p", [(mk(guid="seen1"), "seen1")])
        proc = DedupeProcessor(name="d", pipeline_id="p", seen=seen, failures=fail)
        ctx = PipelineContext(
            pipeline_id="p",
            run_key="p/r",
            run_date="2026-04-28",
            items=[mk(guid="seen1"), mk(guid="new1")],
            context={},
        )
        ctx = await proc.process(ctx)
    assert [it.guid for it in ctx.items] == ["new1"]


@pytest.mark.asyncio
async def test_dedupe_excludes_failed(tmp_path):
    async with open_db(tmp_path / "d.db") as db:
        seen = SeenStore(db)
        fail = FailureStore(db)
        await fail.record(
            "p", "fkey", mk(guid="fkey"), stage="x", error_class="E", error_msg="m", max_retries=3
        )
        proc = DedupeProcessor(name="d", pipeline_id="p", seen=seen, failures=fail)
        ctx = PipelineContext(
            pipeline_id="p",
            run_key="p/r",
            run_date="2026-04-28",
            items=[mk(guid="fkey"), mk(guid="ok")],
            context={},
        )
        ctx = await proc.process(ctx)
    assert [it.guid for it in ctx.items] == ["ok"]
