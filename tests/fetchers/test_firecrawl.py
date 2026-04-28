import pytest, httpx, respx
from sluice.fetchers.firecrawl import FirecrawlFetcher
from sluice.fetchers._ssrf import SSRFError


@pytest.mark.asyncio
async def test_firecrawl_returns_markdown():
    f = FirecrawlFetcher(base_url="http://fc:3002", api_key="K", timeout=30)
    with respx.mock() as r:
        r.post("http://fc:3002/v1/scrape").mock(
            return_value=httpx.Response(
                200,
                json={
                    "success": True,
                    "data": {"markdown": "# title\n\nbody"},
                },
            )
        )
        md = await f.extract("https://x/a")
    assert "title" in md and "body" in md


@pytest.mark.asyncio
async def test_firecrawl_failure_raises():
    f = FirecrawlFetcher(base_url="http://fc:3002", api_key="K", timeout=30)
    with respx.mock() as r:
        r.post("http://fc:3002/v1/scrape").mock(
            return_value=httpx.Response(502, text="bad gateway")
        )
        with pytest.raises(Exception):
            await f.extract("https://x/a")


@pytest.mark.asyncio
async def test_blocks_private_ip():
    f = FirecrawlFetcher(base_url="http://fc:3002", api_key="K", timeout=30)
    with pytest.raises(SSRFError):
        await f.extract("http://169.254.169.254/latest/meta-data/")
