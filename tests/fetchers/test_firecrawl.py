import socket

import httpx
import pytest
import respx

from sluice.core.errors import ConfigError
from sluice.fetchers._ssrf import SSRFError
from sluice.fetchers.firecrawl import FirecrawlFetcher


@pytest.fixture
def allow_public_dns(monkeypatch):
    def fake_getaddrinfo(host, port, *args, **kwargs):
        return [(socket.AF_INET, socket.SOCK_STREAM, 0, "", ("93.184.216.34", 0))]

    monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)


# Endpoint routing


@pytest.mark.asyncio
@respx.mock
async def test_v2_default_endpoint(allow_public_dns):
    respx.post("https://fc.example/v2/scrape").mock(
        return_value=httpx.Response(200, json={"data": {"markdown": "hello"}})
    )
    f = FirecrawlFetcher(base_url="https://fc.example")
    out = await f.extract("https://example.com/article")
    assert out == "hello"


@pytest.mark.asyncio
@respx.mock
async def test_v1_explicit(allow_public_dns):
    respx.post("https://fc.example/v1/scrape").mock(
        return_value=httpx.Response(200, json={"data": {"markdown": "v1 text"}})
    )
    f = FirecrawlFetcher(base_url="https://fc.example", api_version="v1")
    out = await f.extract("https://example.com/article")
    assert out == "v1 text"


@pytest.mark.asyncio
@respx.mock
async def test_versioned_base_url_uses_scrape_only(allow_public_dns):
    respx.post("https://fc.example/v2/scrape").mock(
        return_value=httpx.Response(200, json={"data": {"markdown": "ok"}})
    )
    f = FirecrawlFetcher(base_url="https://fc.example/v2")
    out = await f.extract("https://example.com/article")
    assert out == "ok"


def test_versioned_base_url_conflicts_with_api_version_raises():
    with pytest.raises(ConfigError, match="conflict"):
        FirecrawlFetcher(base_url="https://fc.example/v1", api_version="v2")


def test_invalid_api_version_raises():
    with pytest.raises(ConfigError, match="api_version"):
        FirecrawlFetcher(base_url="https://fc.example", api_version="v3")


# API headers


@pytest.mark.asyncio
@respx.mock
async def test_api_headers_sent(allow_public_dns):
    route = respx.post("https://fc.example/v2/scrape").mock(
        return_value=httpx.Response(200, json={"data": {"markdown": "x"}})
    )
    f = FirecrawlFetcher(
        base_url="https://fc.example",
        api_headers={"Authorization": "Bearer tok", "X-Custom": "val"},
    )
    await f.extract("https://example.com/article")
    assert route.calls[0].request.headers["Authorization"] == "Bearer tok"
    assert route.calls[0].request.headers["X-Custom"] == "val"


@pytest.mark.asyncio
@respx.mock
async def test_api_key_fallback_when_no_auth_header(allow_public_dns):
    route = respx.post("https://fc.example/v2/scrape").mock(
        return_value=httpx.Response(200, json={"data": {"markdown": "x"}})
    )
    f = FirecrawlFetcher(base_url="https://fc.example", api_key="mykey")
    await f.extract("https://example.com/article")
    assert route.calls[0].request.headers["Authorization"] == "Bearer mykey"


@pytest.mark.asyncio
@respx.mock
async def test_api_key_skipped_when_auth_header_present(allow_public_dns):
    route = respx.post("https://fc.example/v2/scrape").mock(
        return_value=httpx.Response(200, json={"data": {"markdown": "x"}})
    )
    f = FirecrawlFetcher(
        base_url="https://fc.example",
        api_key="ignored",
        api_headers={"Authorization": "Bearer explicit"},
    )
    await f.extract("https://example.com/article")
    assert route.calls[0].request.headers["Authorization"] == "Bearer explicit"


@pytest.mark.asyncio
@respx.mock
async def test_api_key_skipped_when_auth_header_lowercase(allow_public_dns):
    """Case-insensitive check: authorization= should block api_key fallback."""
    route = respx.post("https://fc.example/v2/scrape").mock(
        return_value=httpx.Response(200, json={"data": {"markdown": "x"}})
    )
    f = FirecrawlFetcher(
        base_url="https://fc.example",
        api_key="ignored",
        api_headers={"authorization": "Bearer lower"},
    )
    await f.extract("https://example.com/article")
    assert route.calls[0].request.headers["authorization"] == "Bearer lower"
    assert "Bearer ignored" not in str(route.calls[0].request.headers)


# Legacy api_key config still works


@pytest.mark.asyncio
@respx.mock
async def test_legacy_api_key_config(allow_public_dns):
    respx.post("https://fc.local/v1/scrape").mock(
        return_value=httpx.Response(200, json={"data": {"markdown": "ok"}})
    )
    f = FirecrawlFetcher(base_url="https://fc.local", api_key="fc-key", api_version="v1")
    out = await f.extract("https://example.com/")
    assert out == "ok"


# Existing failure behavior


@pytest.mark.asyncio
@respx.mock
async def test_firecrawl_failure_raises(allow_public_dns):
    respx.post("https://fc.example/v2/scrape").mock(return_value=httpx.Response(500))
    f = FirecrawlFetcher(base_url="https://fc.example")
    with pytest.raises(Exception):
        await f.extract("https://example.com/article")


@pytest.mark.asyncio
async def test_blocks_private_ip():
    f = FirecrawlFetcher(base_url="https://fc.example")
    with pytest.raises(SSRFError):
        await f.extract("http://169.254.169.254/latest/meta-data/")
