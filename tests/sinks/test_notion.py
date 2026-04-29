from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from sluice.context import PipelineContext
from sluice.core.errors import SinkError
from sluice.core.item import Item
from sluice.sinks.notion import (
    DefaultNotionifyAdapter,
    NotionSink,
    chunk_markdown,
    normalize_database_properties,
)
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


def test_normalize_database_properties_uses_schema_types():
    schema = {
        "Tag": {"type": "multi_select"},
        "Source": {"type": "select"},
        "Summary": {"type": "rich_text"},
        "URL": {"type": "url"},
        "Published": {"type": "date"},
        "Reviewed": {"type": "checkbox"},
    }
    props = normalize_database_properties(
        {
            "Tag": "AI",
            "Source": "sluice",
            "Summary": "daily digest",
            "URL": "https://example.com",
            "Published": "2026-04-29",
            "Reviewed": True,
        },
        schema,
    )
    assert props == {
        "Tag": {"multi_select": [{"name": "AI"}]},
        "Source": {"select": {"name": "sluice"}},
        "Summary": {"rich_text": [{"text": {"content": "daily digest"}}]},
        "URL": {"url": "https://example.com"},
        "Published": {"date": {"start": "2026-04-29"}},
        "Reviewed": {"checkbox": True},
    }


def test_normalize_database_properties_preserves_explicit_notion_values():
    schema = {"Tag": {"type": "multi_select"}}
    explicit = {"Tag": {"multi_select": [{"name": "AI"}, {"name": "Infra"}]}}
    assert normalize_database_properties(explicit, schema) == explicit


def test_normalize_database_properties_rejects_unknown_raw_values():
    with pytest.raises(SinkError, match="Tag"):
        normalize_database_properties({"Tag": "AI"}, {})


@pytest.mark.asyncio
async def test_default_adapter_creates_database_page_with_typed_properties():
    schema = {
        "Name": {"type": "title"},
        "Tag": {"type": "multi_select"},
        "Source": {"type": "select"},
    }

    class FakeTransport:
        def request(self, method, path):
            assert (method, path) == ("GET", "/databases/db-123")
            return {"properties": schema}

    class FakeClient:
        def __init__(self):
            self._transport = FakeTransport()
            self.created = None

        def create_page_with_markdown(self, **kwargs):
            self.created = kwargs
            return SimpleNamespace(page_id="page-1")

    fake_client = FakeClient()
    adapter = object.__new__(DefaultNotionifyAdapter)
    adapter._client = fake_client

    page_id = await adapter.create_page(
        parent_id="db-123",
        parent_type="database",
        title="AI Daily",
        properties={"Tag": "AI", "Source": "sluice"},
        markdown="hello",
    )

    assert page_id == "page-1"
    assert fake_client.created == {
        "parent_id": "db-123",
        "parent_type": "database",
        "title": "AI Daily",
        "markdown": "hello",
        "title_from_h1": False,
        "properties": {
            "Tag": {"multi_select": [{"name": "AI"}]},
            "Source": {"select": {"name": "sluice"}},
        },
    }


@pytest.mark.asyncio
async def test_default_adapter_uses_data_source_schema_for_new_notion_api():
    schema = {
        "Name": {"type": "title"},
        "Tag": {"type": "multi_select"},
        "Source": {"type": "select"},
    }

    class FakeTransport:
        def request(self, method, path):
            assert method == "GET"
            if path == "/databases/db-123":
                return {"data_sources": [{"id": "ds-456", "name": "default"}]}
            if path == "/data_sources/ds-456":
                return {"properties": schema}
            raise AssertionError(path)

    class FakeClient:
        def __init__(self):
            self._transport = FakeTransport()
            self.created = None

        def create_page_with_markdown(self, **kwargs):
            self.created = kwargs
            return SimpleNamespace(page_id="page-1")

    fake_client = FakeClient()
    adapter = object.__new__(DefaultNotionifyAdapter)
    adapter._client = fake_client

    page_id = await adapter.create_page(
        parent_id="db-123",
        parent_type="database",
        title="AI Daily",
        properties={"Tag": "AI", "Source": "sluice"},
        markdown="hello",
    )

    assert page_id == "page-1"
    assert fake_client.created == {
        "parent_id": "db-123",
        "parent_type": "database",
        "title": "AI Daily",
        "markdown": "hello",
        "title_from_h1": False,
        "properties": {
            "Tag": {"multi_select": [{"name": "AI"}]},
            "Source": {"select": {"name": "sluice"}},
        },
    }


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
