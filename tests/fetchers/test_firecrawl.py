import httpx
import pytest
import respx

from sluice.fetchers._ssrf import SSRFError
from sluice.fetchers.firecrawl import FirecrawlFetcher


@pytest.mark.asyncio
async def test_firecrawl_returns_markdown(monkeypatch):
    import socket

    def fake_getaddrinfo(host, port, *args, **kwargs):
        return [(socket.AF_INET, socket.SOCK_STREAM, 0, "", ("93.184.216.34", 0))]

    monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)
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
        md = await f.extract("https://public.example.com/a")
    assert "title" in md and "body" in md


@pytest.mark.asyncio
async def test_firecrawl_failure_raises(monkeypatch):
    import socket

    def fake_getaddrinfo(host, port, *args, **kwargs):
        return [(socket.AF_INET, socket.SOCK_STREAM, 0, "", ("93.184.216.34", 0))]

    monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)
    f = FirecrawlFetcher(base_url="http://fc:3002", api_key="K", timeout=30)
    with respx.mock() as r:
        r.post("http://fc:3002/v1/scrape").mock(
            return_value=httpx.Response(502, text="bad gateway")
        )
        with pytest.raises(Exception):
            await f.extract("https://public.example.com/a")


@pytest.mark.asyncio
async def test_blocks_private_ip():
    f = FirecrawlFetcher(base_url="http://fc:3002", api_key="K", timeout=30)
    with pytest.raises(SSRFError):
        await f.extract("http://169.254.169.254/latest/meta-data/")
