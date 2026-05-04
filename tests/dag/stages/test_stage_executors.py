from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

import sluice.dag.stages  # noqa: F401
from sluice.dag.item import Item
from sluice.dag.stage_context import StageContext
from sluice.dag.stage_executor import get_executor_class, is_bound


def make_ctx(stage_type: str, *, items: list[Item] | None = None, params=None) -> StageContext:
    return StageContext(
        task_name="task",
        pipeline_name="pipe",
        stage_name=stage_type,
        stage_type=stage_type,
        run_key="run",
        cycle_anchor="2026-05-04T00:00:00Z",
        items=items or [],
        params=params or {},
    )


def make_item(item_id: str, **values) -> Item:
    row = {
        "item_id": item_id,
        "source_name": "src",
        "guid": item_id,
        "url": f"https://example.com/{item_id}",
        "title": item_id,
        "ingested_at": "2026-05-04T00:00:00+00:00",
        "ingested_by": "test",
        "updated_at": "2026-05-04T00:00:00+00:00",
        "raw_json": "{}",
        "dropped": 0,
    }
    row.update(values)
    return Item.from_row(row)


@pytest.mark.parametrize(
    "type_name",
    [
        "fetch_rss",
        "dedupe",
        "cross_dedupe",
        "filter",
        "field_filter",
        "html_strip",
        "fetcher_apply",
        "mirror_attachments",
        "enrich",
        "llm_stage",
        "score_tag",
        "summarize_score_tag",
        "sort",
        "limit",
        "render",
    ],
)
def test_stage_executors_are_bound(type_name):
    assert is_bound(type_name)


@pytest.mark.asyncio
async def test_fetch_rss_uses_injected_fetch_and_skips_known_ids():
    rss = """<?xml version="1.0"?><rss><channel>
    <item><guid>g1</guid><link>https://example.com/a</link><title>A</title>
      <pubDate>Mon, 04 May 2026 01:00:00 GMT</pubDate><description>Hello</description></item>
    <item><guid>g2</guid><link>https://example.com/b</link><title>B</title>
      <pubDate>Mon, 04 May 2026 01:30:00 GMT</pubDate></item>
    </channel></rss>"""
    calls = []

    async def fetch_text(url: str) -> str:
        calls.append(url)
        return rss

    cls = get_executor_class("fetch_rss")
    executor = cls(
        _fetch_text=fetch_text,
        _now=lambda: datetime(2026, 5, 4, 2, tzinfo=timezone.utc),
    )
    result = await executor.execute(
        make_ctx(
            "fetch_rss",
            params={
                "feeds": [{"name": "feed", "url": "https://feed.example/rss", "tag": "ai"}],
                "window_start": "2026-05-04T00:00:00+00:00",
                "window_end": "2026-05-04T02:00:00+00:00",
                "skip_item_ids": ["feed:g1"],
            },
        )
    )

    assert calls == ["https://feed.example/rss"]
    assert [item["item_id"] for item in result.new_items] == ["feed:g2"]
    item = result.new_items[0]
    assert item["source_name"] == "feed"
    assert item["guid"] == "g2"
    assert item["ingested_by"] == "pipe"
    assert item["raw_json"]
    assert {"item_id", "source_name", "url", "title", "raw_json"} <= item.dirty_columns()


@pytest.mark.asyncio
async def test_drop_stages_return_drop_records():
    a = make_item("a", url="https://x/a")
    b = make_item("b", url="https://x/a")

    dedupe = await get_executor_class("dedupe")().execute(
        make_ctx("dedupe", items=[a, b], params={"already_seen_ids": ["a"]})
    )
    assert [(drop.item_id, drop.reason) for drop in dedupe.drops] == [("a", "already_seen")]

    c = make_item("c", url="https://x/a")
    d = make_item("d", url="https://x/a")
    cross = await get_executor_class("cross_dedupe")().execute(
        make_ctx("cross_dedupe", items=[c, d], params={"key": "url"})
    )
    assert [(drop.item_id, drop.reason) for drop in cross.drops] == [("d", "duplicate:url")]


@pytest.mark.asyncio
async def test_field_filter_html_strip_sort_limit_and_render():
    a = make_item("a", title="  <b>Alpha</b>  ", score=2)
    b = make_item("b", title="<p>Beta</p>", score=9)
    c = make_item("c", title="Gamma", score=5)

    await get_executor_class("field_filter")().execute(
        make_ctx(
            "field_filter",
            items=[a, b, c],
            params={"ops": [{"op": "strip", "field": "title"}, {"op": "lower", "field": "title"}]},
        )
    )
    assert a["title"] == "<b>alpha</b>"
    assert "title" in a.dirty_columns()

    await get_executor_class("html_strip")().execute(
        make_ctx("html_strip", items=[a, b, c], params={"fields": ["title"]})
    )
    assert [item["title"] for item in [a, b, c]] == ["alpha", "beta", "gamma"]

    items = [a, b, c]
    await get_executor_class("sort")().execute(
        make_ctx("sort", items=items, params={"sort_by": "score", "sort_order": "desc"})
    )
    assert [item["item_id"] for item in items] == ["b", "c", "a"]

    d = make_item("d", score=None)
    sort_drop_items = [a, d]
    sort_drop = await get_executor_class("sort")().execute(
        make_ctx(
            "sort",
            items=sort_drop_items,
            params={"sort_by": "score", "sort_missing": "drop"},
        )
    )
    assert [item["item_id"] for item in sort_drop_items] == ["a"]
    assert [drop.item_id for drop in sort_drop.drops] == ["d"]
    assert d.is_dropped()

    limited = await get_executor_class("limit")().execute(
        make_ctx("limit", items=items, params={"top_n": 2})
    )
    assert [drop.item_id for drop in limited.drops] == ["a"]

    rendered = await get_executor_class("render")().execute(
        make_ctx(
            "render",
            items=items,
            params={"template": "{{ items|map(attribute='title')|join(',') }}"},
        )
    )
    assert rendered.rendered_output == "beta,gamma"


@pytest.mark.asyncio
async def test_mockable_hook_stages_mark_dirty_fields():
    item = make_item("a", raw_summary="short", fulltext="", attachments_json='[{"url":"https://x/a.png"}]')

    async def fetch(item):
        return f"full:{item['url']}"

    await get_executor_class("fetcher_apply")(fetch=fetch).execute(
        make_ctx("fetcher_apply", items=[item], params={"write_field": "fulltext"})
    )
    assert item["fulltext"] == "full:https://example.com/a"

    async def mirror(url, attachment):
        return {"url": "file:///a.png", "source_url": url}

    await get_executor_class("mirror_attachments")(mirror=mirror).execute(
        make_ctx("mirror_attachments", items=[item])
    )
    assert json.loads(item["attachments_json"])[0]["url"] == "file:///a.png"

    async def enrich(item):
        return {"topic": "python"}

    await get_executor_class("enrich")(enrich=enrich).execute(
        make_ctx("enrich", items=[item], params={"output_key": "enrich"})
    )
    assert item.get_extra("enrich") == {"topic": "python"}

    async def llm(prompt, item):
        return {"summary": "S", "score": 8, "tags": ["x"]}

    await get_executor_class("summarize_score_tag")(llm=llm).execute(
        make_ctx("summarize_score_tag", items=[item], params={"input_field": "fulltext"})
    )
    assert item["summary"] == "S"
    assert item["score"] == 8
    assert json.loads(item["tags_json"]) == ["x"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "stage_type",
    [
        "fetcher_apply",
        "mirror_attachments",
        "enrich",
        "llm_stage",
        "score_tag",
        "summarize_score_tag",
    ],
)
async def test_io_stages_fail_fast_without_backend(stage_type):
    item = make_item("a", raw_summary="", fulltext="", attachments_json="[]")
    with pytest.raises(RuntimeError, match=f"{stage_type} requires a backend"):
        await get_executor_class(stage_type)().execute(make_ctx(stage_type, items=[item]))


@pytest.mark.asyncio
async def test_render_template_file_resolves_config_root(tmp_path):
    (tmp_path / "template.md.j2").write_text(
        "{{ pipeline_name }} {{ items[0].title }}",
        encoding="utf-8",
    )
    item = make_item("a", title="Alpha")

    result = await get_executor_class("render")().execute(
        make_ctx(
            "render",
            items=[item],
            params={"template_file": "template.md.j2", "config_root": str(tmp_path)},
        )
    )

    assert result.rendered_output == "pipe Alpha"
