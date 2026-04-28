import httpx
import pytest
import respx

from sluice.fetchers._ssrf import SSRFError
from sluice.fetchers.jina_reader import JinaReaderFetcher


@pytest.mark.asyncio
async def test_jina_prefix_url(monkeypatch):
    import socket

    def fake_getaddrinfo(host, port, *args, **kwargs):
        return [(socket.AF_INET, socket.SOCK_STREAM, 0, "", ("93.184.216.34", 0))]

    monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)
    f = JinaReaderFetcher(base_url="https://r.jina.ai", timeout=30)
    with respx.mock() as r:
        r.get("https://r.jina.ai/https://public.example.com/a").mock(
            return_value=httpx.Response(200, text="# md content")
        )
        md = await f.extract("https://public.example.com/a")
    assert "md content" in md


@pytest.mark.asyncio
async def test_blocks_private_ip():
    f = JinaReaderFetcher(base_url="https://r.jina.ai", timeout=30)
    with pytest.raises(SSRFError):
        await f.extract("http://169.254.169.254/latest/meta-data/")
