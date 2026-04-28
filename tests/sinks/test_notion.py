from datetime import datetime, timezone

import pytest

from sluice.context import PipelineContext
from sluice.core.item import Item
from sluice.sinks.notion import NotionSink, chunk_markdown
from sluice.state.db import open_db
from sluice.state.emissions import EmissionStore


class FakeAdapter:
    def __init__(self):
        self.created = []
        self.updated = []

    async def create_page(self, *, parent_id, parent_type, title, properties, markdown):
        self.created.append({"parent_id": parent_id, "title": title, "markdown": markdown})
        return f"pg-{len(self.created)}"

    async def replace_page_blocks(self, page_id, markdown):
        self.updated.append((page_id, markdown))


def test_chunk_markdown_respects_limit():
    long = "a" * 5000
    chunks = chunk_markdown(long, 1900)
    assert all(len(c) <= 1900 for c in chunks)
    assert "".join(chunks) == long


def test_chunk_markdown_keeps_short():
    assert chunk_markdown("short", 1900) == ["short"]


def mk(s):
    return Item(
        source_id="s",
        pipeline_id="p",
        guid=s,
        url=f"https://x/{s}",
        title=s,
        published_at=datetime(2026, 4, 28, tzinfo=timezone.utc),
        raw_summary=None,
    )


@pytest.mark.asyncio
async def test_create_then_update(tmp_path, monkeypatch):
    monkeypatch.setenv("PARENT", "db-123")
    fake = FakeAdapter()
    s = NotionSink(
        id="notion_main",
        input="context.markdown",
        parent_id="env:PARENT",
        parent_type="database",
        title_template="X · {run_date}",
        properties={"Tag": "AI"},
        mode="upsert",
        max_block_chars=1900,
        adapter=fake,
    )
    ctx = PipelineContext("p", "p/2026-04-28", "2026-04-28", [mk("a")], {"markdown": "hello world"})
    async with open_db(tmp_path / "d.db") as db:
        e = EmissionStore(db)
        await s.emit(ctx, emissions=e)
        assert fake.created[0]["parent_id"] == "db-123"
        assert fake.created[0]["title"] == "X · 2026-04-28"
        assert fake.created[0]["markdown"] == "hello world"
        ctx.context["markdown"] = "v2"
        await s.emit(ctx, emissions=e)
        assert fake.updated and fake.updated[0] == ("pg-1", "v2")
