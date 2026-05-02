from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import pytest
import respx

from sluice.sources.rss import RssSource
from sluice.sources._filter import SourceFilter
from sluice.config import FilterRule, SourceFilterConfig

FIXTURE = (Path(__file__).parent / "fixtures" / "example.xml").read_text()


@pytest.mark.asyncio
async def test_rss_fetch_in_window():
    src = RssSource(url="https://feed.example/rss", pipeline_id="p", source_id="s1", tag="ai")
    with respx.mock() as r:
        r.get("https://feed.example/rss").mock(return_value=httpx.Response(200, text=FIXTURE))
        end = datetime(2026, 4, 28, 12, tzinfo=timezone.utc)
        start = end - timedelta(hours=24)
        items = [it async for it in src.fetch(start, end)]
    assert len(items) == 2
    a = items[0]
    assert a.guid == "guid-a"
    assert a.url == "https://x.com/a"
    assert a.tags == ["ai"]
    b = items[1]
    assert b.url == "https://x.com/b"
    assert b.guid is None


@pytest.mark.asyncio
async def test_rss_drops_outside_window():
    src = RssSource(url="https://feed.example/rss", pipeline_id="p", source_id="s1")
    with respx.mock() as r:
        r.get("https://feed.example/rss").mock(return_value=httpx.Response(200, text=FIXTURE))
        end = datetime(2027, 4, 28, tzinfo=timezone.utc)
        start = end - timedelta(hours=24)
        items = [it async for it in src.fetch(start, end)]
    assert items == []


@pytest.mark.asyncio
async def test_rss_extracts_enclosures_as_attachments():
    rss = """<?xml version="1.0"?>
    <rss><channel>
      <item>
        <guid>g</guid>
        <title>T</title>
        <link>https://x.com/a</link>
        <pubDate>Tue, 28 Apr 2026 10:00:00 GMT</pubDate>
        <enclosure url="https://cdn.example/a.jpg" type="image/jpeg" length="123" />
      </item>
    </channel></rss>
    """
    src = RssSource(url="https://feed.example/rss", pipeline_id="p", source_id="s1")
    with respx.mock() as r:
        r.get("https://feed.example/rss").mock(return_value=httpx.Response(200, text=rss))
        end = datetime(2026, 4, 28, 12, tzinfo=timezone.utc)
        start = end - timedelta(hours=24)
        items = [it async for it in src.fetch(start, end)]

    assert len(items) == 1
    assert items[0].attachments[0].url == "https://cdn.example/a.jpg"
    assert items[0].attachments[0].mime_type == "image/jpeg"
    assert items[0].attachments[0].length == 123


@pytest.mark.asyncio
async def test_rss_filter_matches_title_or_content():
    rss = """<?xml version="1.0"?>
    <rss><channel>
      <item>
        <guid>title</guid>
        <title>GPT-5 release notes</title>
        <link>https://x.com/title</link>
        <pubDate>Tue, 28 Apr 2026 10:00:00 GMT</pubDate>
        <description>Other text</description>
      </item>
      <item>
        <guid>content</guid>
        <title>Platform update</title>
        <link>https://x.com/content</link>
        <pubDate>Tue, 28 Apr 2026 10:00:00 GMT</pubDate>
        <description>New agent tooling is available.</description>
      </item>
      <item>
        <guid>drop</guid>
        <title>Company news</title>
        <link>https://x.com/drop</link>
        <pubDate>Tue, 28 Apr 2026 10:00:00 GMT</pubDate>
        <description>Unrelated text.</description>
      </item>
    </channel></rss>
    """
    src = SourceFilter(
        RssSource(
            url="https://feed.example/rss",
            pipeline_id="p",
            source_id="s1",
        ),
        filter=SourceFilterConfig(
            mode="keep_if_any",
            rules=[
                FilterRule(field="title", op="matches", value=r"(?i)\bgpt-5\b"),
                FilterRule(field="content", op="matches", value=r"(?i)\bagent\b"),
            ],
        ),
    )
    with respx.mock() as r:
        r.get("https://feed.example/rss").mock(return_value=httpx.Response(200, text=rss))
        end = datetime(2026, 4, 28, 12, tzinfo=timezone.utc)
        start = end - timedelta(hours=24)
        items = [it async for it in src.fetch(start, end)]

    assert [it.guid for it in items] == ["title", "content"]
