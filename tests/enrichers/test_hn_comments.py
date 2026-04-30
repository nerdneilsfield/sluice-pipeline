from pathlib import Path

import pytest
import respx
from httpx import Response

from sluice.core.item import Item
from sluice.enrichers.hn_comments import HnCommentsEnricher

FIXTURE = Path(__file__).parent.parent / "fixtures" / "hn" / "sample_thread.html"


def _it(url):
    return Item(
        source_id="s",
        pipeline_id="p",
        guid="g",
        url=url,
        title="t",
        published_at=None,
        raw_summary=None,
    )


@pytest.mark.asyncio
async def test_skip_non_hn_url():
    e = HnCommentsEnricher()
    out = await e.enrich(_it("https://example.com/story"))
    assert out is None


@pytest.mark.asyncio
@respx.mock
async def test_fetches_and_parses(tmp_path):
    # Primary path: Firebase API
    item_data = {"id": 47908051, "kids": [111, 222]}
    comment_111 = {"id": 111, "by": "alice", "text": "Great article about oil."}
    comment_222 = {"id": 222, "by": "bob", "text": "Really interesting read."}

    respx.get("https://hacker-news.firebaseio.com/v0/item/47908051.json").mock(
        return_value=Response(200, json=item_data)
    )
    respx.get("https://hacker-news.firebaseio.com/v0/item/111.json").mock(
        return_value=Response(200, json=comment_111)
    )
    respx.get("https://hacker-news.firebaseio.com/v0/item/222.json").mock(
        return_value=Response(200, json=comment_222)
    )

    e = HnCommentsEnricher()
    out = await e.enrich(_it("https://news.ycombinator.com/item?id=47908051"))
    assert out is not None and "alice" in out and "bob" in out


@pytest.mark.asyncio
async def test_per_host_token_bucket_serializes(monkeypatch):
    import asyncio
    import time

    from sluice.enrichers.hn_comments import _HostBucket

    bucket = _HostBucket(delay_seconds=0.05)
    times = []

    async def run():
        await bucket.acquire()
        times.append(time.monotonic())

    await asyncio.gather(run(), run(), run())
    assert times[-1] - times[0] >= 0.09
