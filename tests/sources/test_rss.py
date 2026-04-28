import pytest, httpx, respx
from datetime import datetime, timezone, timedelta
from pathlib import Path
from sluice.sources.rss import RssSource

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
