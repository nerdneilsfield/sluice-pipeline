import httpx
import pytest
import respx

from sluice.fetchers._ssrf import SSRFError, guard
from sluice.fetchers.trafilatura_fetcher import TrafilaturaFetcher

HTML = (
    "<html><head><title>T</title></head><body>"
    "<article><h1>Hello</h1><p>This is the body content of an article. "
    "It has enough text to be a real extraction target. " * 5 + "</p></article></body></html>"
)


@pytest.mark.asyncio
async def test_extract_html(monkeypatch):
    import socket

    def fake_getaddrinfo(host, port, *args, **kwargs):
        return [(socket.AF_INET, socket.SOCK_STREAM, 0, "", ("93.184.216.34", 0))]

    monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)
    f = TrafilaturaFetcher(timeout=10)
    with respx.mock() as r:
        r.get("https://public.example.com/a").mock(return_value=httpx.Response(200, text=HTML))
        md = await f.extract("https://public.example.com/a")
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


@pytest.mark.asyncio
async def test_relative_redirect_stays_public(monkeypatch):
    """Relative redirects should be resolved against the current public URL."""
    import socket

    def fake_getaddrinfo(host, port, *args, **kwargs):
        return [(socket.AF_INET, socket.SOCK_STREAM, 0, "", ("93.184.216.34", 0))]

    monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)
    f = TrafilaturaFetcher(timeout=10)
    with respx.mock() as r:
        r.get("https://public.example.com/a").mock(
            return_value=httpx.Response(302, headers={"location": "/b"})
        )
        r.get("https://public.example.com/b").mock(return_value=httpx.Response(200, text=HTML))
        md = await f.extract("https://public.example.com/a")

    assert "Hello" in md


def test_blocks_ipv4_mapped_loopback():
    """IPv4-mapped IPv6 literals should inherit the embedded IPv4 block status."""
    with pytest.raises(SSRFError):
        guard("http://[::ffff:127.0.0.1]/")


def test_blocks_non_global_address():
    """Non-global addresses beyond RFC1918/link-local should be blocked."""
    with pytest.raises(SSRFError):
        guard("http://100.64.0.1/")


def test_blocks_tun_fake_ip_resolution_by_default(monkeypatch):
    """TUN fake-ip DNS should stay blocked unless explicitly enabled."""
    import socket

    def fake_getaddrinfo(host, port, *args, **kwargs):
        return [(socket.AF_INET, socket.SOCK_STREAM, 0, "", ("198.18.0.174", 0))]

    monkeypatch.delenv("SLUICE_SSRF_ALLOW_TUN_FAKE_IP", raising=False)
    monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)
    with pytest.raises(SSRFError, match="198.18.0.174"):
        guard("https://public.example.com/article")


def test_allows_tun_fake_ip_resolution_when_enabled(monkeypatch):
    """Proxy/TUN fake-ip DNS can be allowed for domain names via opt-in env."""
    import socket

    def fake_getaddrinfo(host, port, *args, **kwargs):
        return [(socket.AF_INET, socket.SOCK_STREAM, 0, "", ("198.18.0.174", 0))]

    monkeypatch.setenv("SLUICE_SSRF_ALLOW_TUN_FAKE_IP", "1")
    monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)
    guard("https://public.example.com/article")


def test_tun_fake_ip_literal_stays_blocked_when_enabled(monkeypatch):
    monkeypatch.setenv("SLUICE_SSRF_ALLOW_TUN_FAKE_IP", "1")
    with pytest.raises(SSRFError):
        guard("http://198.18.0.174/")
