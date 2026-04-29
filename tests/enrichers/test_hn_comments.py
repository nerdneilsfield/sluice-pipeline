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
    assert FIXTURE.exists(), "fixture must be committed; not optional"
    respx.get("https://www.hckrnws.com/stories/47908051").mock(
        return_value=Response(
            200, content=FIXTURE.read_bytes(), headers={"content-type": "text/html"}
        )
    )
    e = HnCommentsEnricher()
    out = await e.enrich(_it("https://news.ycombinator.com/item?id=47908051"))
    assert out is not None and len(out) > 0


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
