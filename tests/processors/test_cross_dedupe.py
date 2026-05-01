import difflib

import pytest

from sluice.processors.cross_dedupe import CrossDedupeProcessor
from tests.conftest import make_ctx, make_item


def _proc(**kwargs) -> CrossDedupeProcessor:
    return CrossDedupeProcessor(name="cd", **kwargs)


class _FakeLog:
    def __init__(self):
        self.events = []

    def bind(self, **fields):
        bound = _FakeLog()
        bound.events = self.events
        bound.fields = fields
        return bound

    def debug(self, message):
        self.events.append((message, self.fields))


@pytest.mark.asyncio
async def test_url_dedup_keeps_first_when_no_priority():
    a = make_item(url="https://example.com/a", source_id="rss_0")
    b = make_item(url="https://example.com/a", source_id="rss_1")

    ctx = await _proc().process(make_ctx(items=[a, b]))

    assert len(ctx.items) == 1
    assert ctx.items[0].source_id == "rss_0"


@pytest.mark.asyncio
async def test_url_dedup_respects_source_priority():
    a = make_item(url="https://example.com/a", source_id="rss_0")
    b = make_item(url="https://example.com/a", source_id="rss_hn")

    ctx = await _proc(source_priority=["rss_hn", "rss_0"]).process(make_ctx(items=[a, b]))

    assert len(ctx.items) == 1
    assert ctx.items[0].source_id == "rss_hn"


@pytest.mark.asyncio
async def test_url_dedup_uses_first_seen_when_priority_does_not_rank_group():
    a = make_item(url="https://example.com/a", source_id="rss_0")
    b = make_item(url="https://example.com/a", source_id="rss_1")

    ctx = await _proc(source_priority=["rss_other"]).process(make_ctx(items=[a, b]))

    assert len(ctx.items) == 1
    assert ctx.items[0].source_id == "rss_0"


@pytest.mark.asyncio
async def test_url_dedup_merges_tags_case_sensitively_in_order():
    a = make_item(url="https://x.com/a", tags=["AI", "ml"])
    b = make_item(url="https://x.com/a", tags=["ml", "ai", "data"])

    ctx = await _proc(merge_tags=True).process(make_ctx(items=[a, b]))

    assert ctx.items[0].tags == ["AI", "ml", "ai", "data"]


@pytest.mark.asyncio
async def test_url_dedup_no_merge_when_disabled():
    a = make_item(url="https://x.com/a", tags=["ai"])
    b = make_item(url="https://x.com/a", tags=["ml"])

    ctx = await _proc(merge_tags=False).process(make_ctx(items=[a, b]))

    assert ctx.items[0].tags == ["ai"]


@pytest.mark.asyncio
async def test_empty_url_skips_url_round_but_can_match_title_round():
    a = make_item(url="", title="Same Title")
    b = make_item(url="", title="Same Title")

    ctx = await _proc(title_similarity_threshold=0.85).process(make_ctx(items=[a, b]))

    assert len(ctx.items) == 1


@pytest.mark.asyncio
async def test_empty_url_items_not_grouped_by_url():
    a = make_item(url="", title="Python release notes")
    b = make_item(url="", title="Rust ownership guide")

    ctx = await _proc().process(make_ctx(items=[a, b]))

    assert len(ctx.items) == 2


@pytest.mark.asyncio
async def test_title_dedup_above_threshold():
    a = make_item(url="https://x.com/a", title="GCC 16 released")
    b = make_item(url="https://y.com/b", title="GCC 16 released!")
    ratio = difflib.SequenceMatcher(None, a.title.lower(), b.title.lower()).ratio()
    assert ratio >= 0.85

    ctx = await _proc(title_similarity_threshold=0.85).process(make_ctx(items=[a, b]))

    assert len(ctx.items) == 1


@pytest.mark.asyncio
async def test_title_dedup_below_threshold_keeps_both():
    a = make_item(url="https://x.com/a", title="Python 3.13 released")
    b = make_item(url="https://y.com/b", title="Rust 2.0 announced")

    ctx = await _proc(title_similarity_threshold=0.85).process(make_ctx(items=[a, b]))

    assert len(ctx.items) == 2


@pytest.mark.asyncio
async def test_empty_title_skips_title_round():
    a = make_item(url="https://x.com/a", title="")
    b = make_item(url="https://y.com/b", title="")

    ctx = await _proc(title_similarity_threshold=0.85).process(make_ctx(items=[a, b]))

    assert len(ctx.items) == 2


@pytest.mark.asyncio
async def test_title_dedup_greedy_pairwise_not_transitive():
    a = make_item(url="https://x.com/a", title="abcdefghij")
    b = make_item(url="https://y.com/b", title="abcdefxyij")
    c = make_item(url="https://z.com/c", title="abxyefxyij")
    threshold = 0.8
    assert difflib.SequenceMatcher(None, a.title.lower(), b.title.lower()).ratio() >= threshold
    assert difflib.SequenceMatcher(None, b.title.lower(), c.title.lower()).ratio() >= threshold
    assert difflib.SequenceMatcher(None, a.title.lower(), c.title.lower()).ratio() < threshold

    ctx = await _proc(title_similarity_threshold=threshold).process(make_ctx(items=[a, b, c]))

    assert [it.url for it in ctx.items] == ["https://x.com/a", "https://z.com/c"]


@pytest.mark.asyncio
async def test_output_preserves_relative_order_of_kept_items():
    a = make_item(url="https://x.com/a", title="A")
    b = make_item(url="https://x.com/b", title="B")
    c = make_item(url="https://x.com/c", title="C")

    ctx = await _proc().process(make_ctx(items=[a, b, c]))

    assert [it.url for it in ctx.items] == [
        "https://x.com/a",
        "https://x.com/b",
        "https://x.com/c",
    ]


@pytest.mark.asyncio
async def test_url_priority_winner_keeps_own_original_position():
    low = make_item(url="https://x.com/dup", source_id="rss_low", title="Dup")
    unique = make_item(url="https://x.com/unique", source_id="rss_0", title="Unique")
    priority = make_item(url="https://x.com/dup", source_id="rss_hn", title="Dup")

    ctx = await _proc(source_priority=["rss_hn"]).process(make_ctx(items=[low, unique, priority]))

    assert [it.url for it in ctx.items] == ["https://x.com/unique", "https://x.com/dup"]
    assert ctx.items[1].source_id == "rss_hn"


@pytest.mark.asyncio
async def test_title_priority_winner_keeps_own_original_position():
    low = make_item(url="https://x.com/a", source_id="rss_low", title="GCC 16 released")
    unique = make_item(url="https://x.com/b", source_id="rss_0", title="Unrelated article")
    priority = make_item(url="https://x.com/c", source_id="rss_hn", title="GCC 16 released!")

    ctx = await _proc(source_priority=["rss_hn"], title_similarity_threshold=0.85).process(
        make_ctx(items=[low, unique, priority])
    )

    assert [it.url for it in ctx.items] == ["https://x.com/b", "https://x.com/c"]
    assert ctx.items[1].source_id == "rss_hn"


@pytest.mark.asyncio
async def test_debug_logs_each_dropped_item_with_merge_details(monkeypatch):
    fake_log = _FakeLog()
    monkeypatch.setattr("sluice.processors.cross_dedupe.log", fake_log)
    url_kept = make_item(url="https://x.com/url", source_id="rss_a", title="URL kept")
    url_drop_1 = make_item(url="https://x.com/url", source_id="rss_b", title="URL drop 1")
    url_drop_2 = make_item(url="https://x.com/url", source_id="rss_c", title="URL drop 2")
    title_kept = make_item(url="https://x.com/title-a", source_id="rss_d", title="GCC 16 released")
    title_drop = make_item(url="https://x.com/title-b", source_id="rss_e", title="GCC 16 released!")

    await _proc().process(
        make_ctx(items=[url_kept, url_drop_1, url_drop_2, title_kept, title_drop])
    )

    merge_events = [
        fields for message, fields in fake_log.events if message == "cross_dedupe.merged"
    ]
    assert [event["dropped_source"] for event in merge_events] == [
        "rss_b",
        "rss_c",
        "rss_e",
    ]
    assert merge_events[0]["method"] == "url"
    assert merge_events[0]["url"] == "https://x.com/url"
    assert merge_events[2]["method"] == "title"
    assert merge_events[2]["url"] == "https://x.com/title-b"
    assert merge_events[2]["ratio"] >= 0.85


def test_threshold_out_of_range_raises():
    from sluice.config import CrossDedupeConfig
    from sluice.core.errors import ConfigError

    with pytest.raises(ConfigError):
        CrossDedupeConfig(type="cross_dedupe", name="x", title_similarity_threshold=1.5)
