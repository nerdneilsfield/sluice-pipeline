import pytest, httpx, respx
from sluice.fetchers.trafilatura_fetcher import TrafilaturaFetcher
from sluice.fetchers._ssrf import SSRFError

HTML = (
    "<html><head><title>T</title></head><body>"
    "<article><h1>Hello</h1><p>This is the body content of an article. "
    "It has enough text to be a real extraction target. " * 5 + "</p></article></body></html>"
)


@pytest.mark.asyncio
async def test_extract_html():
    f = TrafilaturaFetcher(timeout=10)
    with respx.mock() as r:
        r.get("https://x/a").mock(return_value=httpx.Response(200, text=HTML))
        md = await f.extract("https://x/a")
    assert "Hello" in md
    assert "body content" in md


@pytest.mark.asyncio
async def test_blocks_private_ip():
    f = TrafilaturaFetcher(timeout=10)
    with pytest.raises(SSRFError):
        await f.extract("http://169.254.169.254/latest/meta-data/")


def test_blocks_domain_resolving_to_private_ip(monkeypatch):
    """DNS resolution to private IP must be blocked."""
    import socket

    def fake_getaddrinfo(host, port, *args, **kwargs):
        return [(socket.AF_INET, socket.SOCK_STREAM, 0, "", ("192.168.1.1", 0))]

    monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)
    with pytest.raises(SSRFError, match="blocked private IP resolved"):
        from sluice.fetchers._ssrf import guard

        guard("http://evil.example.com/secret")


@pytest.mark.asyncio
async def test_redirect_to_private_ip_blocked(monkeypatch):
    """302 redirect to private IP must be blocked."""
    import socket

    def fake_getaddrinfo(host, port, *args, **kwargs):
        return [(socket.AF_INET, socket.SOCK_STREAM, 0, "", (host, 0))]

    monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)
    f = TrafilaturaFetcher(timeout=10)
    with respx.mock() as r:
        r.get("https://public.example.com/a").mock(
            return_value=httpx.Response(302, headers={"location": "http://192.168.1.1/secret"})
        )
        with pytest.raises(SSRFError):
            await f.extract("https://public.example.com/a")
