import socket
from unittest.mock import AsyncMock

import httpx
import pytest
import respx

from sluice.fetchers.crawl4ai import (
    Crawl4AIFetcher,
    _extract_markdown,
    _get_task_id,
)


@pytest.fixture
def allow_public_dns(monkeypatch):
    def fake_getaddrinfo(host, port, *args, **kwargs):
        return [(socket.AF_INET, socket.SOCK_STREAM, 0, "", ("93.184.216.34", 0))]

    monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)


@pytest.fixture
def no_sleep(monkeypatch):
    monkeypatch.setattr("sluice.fetchers.crawl4ai.asyncio.sleep", AsyncMock())


def test_default_configuration():
    f = Crawl4AIFetcher()

    assert f.base_url == "http://localhost:11235"
    assert f.timeout == 120
    assert f.poll_interval == 2.0
    assert f.poll_timeout == 120
    assert f.poll_paths == ["/crawl/job/{task_id}", "/task/{task_id}", "/job/{task_id}"]


def test_extract_results_list():
    assert _extract_markdown({"results": [{"markdown": "hello"}]}) == "hello"


def test_extract_result_dict():
    assert _extract_markdown({"result": {"markdown": "world"}}) == "world"


def test_extract_data_dict():
    assert _extract_markdown({"data": {"markdown": "data-md"}}) == "data-md"


def test_extract_nested_result_results_list():
    data = {"result": {"success": True, "results": [{"markdown": "nested result"}]}}
    assert _extract_markdown(data) == "nested result"


def test_extract_top_level():
    assert _extract_markdown({"markdown": "top"}) == "top"


def test_extract_nested_dict_raw_markdown():
    assert _extract_markdown({"markdown": {"raw_markdown": "raw", "fit_markdown": "fit"}}) == "raw"


def test_extract_nested_dict_fit_fallback():
    assert _extract_markdown({"markdown": {"fit_markdown": "fit"}}) == "fit"


def test_extract_nested_dict_markdown_fallback():
    assert _extract_markdown({"markdown": {"markdown": "nested"}}) == "nested"


def test_extract_empty_string_skipped_tries_next():
    data = {"results": [{"markdown": ""}], "markdown": "top"}
    assert _extract_markdown(data) == "top"


def test_extract_no_markdown_returns_none():
    assert _extract_markdown({"foo": "bar"}) is None


def test_get_task_id_lookup_order():
    assert _get_task_id({"task_id": "task", "job_id": "job", "id": "id"}) == "task"
    assert _get_task_id({"job_id": "job", "id": "id"}) == "job"
    assert _get_task_id({"id": "id"}) == "id"
    assert _get_task_id({"task_id": "", "id": ""}) is None


@pytest.mark.asyncio
@respx.mock
async def test_sync_results_response(allow_public_dns):
    respx.post("http://localhost:11235/crawl/job").mock(
        return_value=httpx.Response(200, json={"results": [{"markdown": "article text"}]})
    )
    f = Crawl4AIFetcher()

    out = await f.extract("https://example.com/article")

    assert out == "article text"


@pytest.mark.asyncio
@respx.mock
async def test_sync_data_response(allow_public_dns):
    respx.post("http://localhost:11235/crawl/job").mock(
        return_value=httpx.Response(200, json={"data": {"markdown": "data md"}})
    )
    f = Crawl4AIFetcher()

    out = await f.extract("https://example.com/article")

    assert out == "data md"


@pytest.mark.asyncio
@respx.mock
async def test_falls_back_to_sync_crawl_when_job_endpoint_missing(allow_public_dns):
    respx.post("http://localhost:11235/crawl/job").mock(return_value=httpx.Response(404))
    respx.post("http://localhost:11235/crawl").mock(
        return_value=httpx.Response(200, json={"results": [{"markdown": "sync fallback"}]})
    )
    f = Crawl4AIFetcher()

    out = await f.extract("https://example.com/article")

    assert out == "sync fallback"


@pytest.mark.asyncio
@respx.mock
async def test_initial_request_posts_urls_array(allow_public_dns):
    route = respx.post("http://localhost:11235/crawl/job").mock(
        return_value=httpx.Response(200, json={"results": [{"markdown": "x"}]})
    )
    f = Crawl4AIFetcher()

    await f.extract("https://example.com/article")

    assert route.calls[0].request.read() == b'{"urls":["https://example.com/article"]}'


@pytest.mark.asyncio
@respx.mock
async def test_api_headers_on_initial_request(allow_public_dns):
    route = respx.post("http://localhost:11235/crawl/job").mock(
        return_value=httpx.Response(200, json={"results": [{"markdown": "x"}]})
    )
    f = Crawl4AIFetcher(api_headers={"Authorization": "Bearer secret"})

    await f.extract("https://example.com/article")

    assert route.calls[0].request.headers["Authorization"] == "Bearer secret"


@pytest.mark.asyncio
@respx.mock
async def test_api_key_fallback(allow_public_dns):
    route = respx.post("http://localhost:11235/crawl/job").mock(
        return_value=httpx.Response(200, json={"results": [{"markdown": "x"}]})
    )
    f = Crawl4AIFetcher(api_key="mykey")

    await f.extract("https://example.com/article")

    assert route.calls[0].request.headers["Authorization"] == "Bearer mykey"


@pytest.mark.asyncio
@respx.mock
async def test_api_key_skipped_when_lowercase_auth_present(allow_public_dns):
    route = respx.post("http://localhost:11235/crawl/job").mock(
        return_value=httpx.Response(200, json={"results": [{"markdown": "x"}]})
    )
    f = Crawl4AIFetcher(api_key="ignored", api_headers={"authorization": "Bearer lower"})

    await f.extract("https://example.com/article")

    assert route.calls[0].request.headers["authorization"] == "Bearer lower"
    assert "Bearer ignored" not in str(route.calls[0].request.headers)


@pytest.mark.asyncio
@respx.mock
async def test_poll_task_id(allow_public_dns, no_sleep):
    respx.post("http://localhost:11235/crawl/job").mock(
        return_value=httpx.Response(200, json={"task_id": "t1"})
    )
    respx.get("http://localhost:11235/crawl/job/t1").mock(
        return_value=httpx.Response(
            200,
            json={
                "status": "completed",
                "results": [{"markdown": "polled"}],
            },
        )
    )
    f = Crawl4AIFetcher(poll_interval=0.01, poll_timeout=5.0)

    assert await f.extract("https://example.com/article") == "polled"


@pytest.mark.asyncio
@respx.mock
async def test_poll_job_id_field(allow_public_dns, no_sleep):
    respx.post("http://localhost:11235/crawl/job").mock(
        return_value=httpx.Response(200, json={"job_id": "j1"})
    )
    respx.get("http://localhost:11235/crawl/job/j1").mock(
        return_value=httpx.Response(
            200,
            json={
                "status": "success",
                "results": [{"markdown": "job polled"}],
            },
        )
    )
    f = Crawl4AIFetcher(poll_interval=0.01, poll_timeout=5.0)

    assert await f.extract("https://example.com/article") == "job polled"


@pytest.mark.asyncio
@respx.mock
async def test_poll_id_field(allow_public_dns, no_sleep):
    respx.post("http://localhost:11235/crawl/job").mock(
        return_value=httpx.Response(200, json={"id": "i1"})
    )
    respx.get("http://localhost:11235/crawl/job/i1").mock(
        return_value=httpx.Response(
            200,
            json={
                "status": "done",
                "results": [{"markdown": "id polled"}],
            },
        )
    )
    f = Crawl4AIFetcher(poll_interval=0.01, poll_timeout=5.0)

    assert await f.extract("https://example.com/article") == "id polled"


@pytest.mark.asyncio
@respx.mock
async def test_poll_fallback_path_on_404(allow_public_dns, no_sleep):
    respx.post("http://localhost:11235/crawl/job").mock(
        return_value=httpx.Response(200, json={"task_id": "t2"})
    )
    respx.get("http://localhost:11235/crawl/job/t2").mock(return_value=httpx.Response(404))
    respx.get("http://localhost:11235/task/t2").mock(
        return_value=httpx.Response(
            200,
            json={
                "status": "done",
                "results": [{"markdown": "fallback path"}],
            },
        )
    )
    f = Crawl4AIFetcher(poll_interval=0.01, poll_timeout=5.0)

    assert await f.extract("https://example.com/article") == "fallback path"


@pytest.mark.asyncio
@respx.mock
async def test_poll_failure_status_raises(allow_public_dns, no_sleep):
    respx.post("http://localhost:11235/crawl/job").mock(
        return_value=httpx.Response(200, json={"task_id": "t3"})
    )
    respx.get("http://localhost:11235/crawl/job/t3").mock(
        return_value=httpx.Response(200, json={"status": "failed"})
    )
    f = Crawl4AIFetcher(poll_interval=0.01, poll_timeout=5.0)

    with pytest.raises(RuntimeError, match="failed"):
        await f.extract("https://example.com/article")


@pytest.mark.asyncio
@respx.mock
async def test_poll_success_without_markdown_raises(allow_public_dns, no_sleep):
    respx.post("http://localhost:11235/crawl/job").mock(
        return_value=httpx.Response(200, json={"task_id": "t-success-empty"})
    )
    respx.get("http://localhost:11235/crawl/job/t-success-empty").mock(
        return_value=httpx.Response(200, json={"status": "completed"})
    )
    f = Crawl4AIFetcher(poll_interval=0.01, poll_timeout=5.0)

    with pytest.raises(RuntimeError, match="completed but no markdown"):
        await f.extract("https://example.com/article")


@pytest.mark.asyncio
@respx.mock
async def test_poll_timeout_raises(allow_public_dns, no_sleep, monkeypatch):
    tick = [0.0]

    def fake_monotonic():
        tick[0] += 0.1
        return tick[0]

    monkeypatch.setattr("sluice.fetchers.crawl4ai.time.monotonic", fake_monotonic)
    respx.post("http://localhost:11235/crawl/job").mock(
        return_value=httpx.Response(200, json={"task_id": "t4"})
    )
    respx.get("http://localhost:11235/crawl/job/t4").mock(
        return_value=httpx.Response(200, json={"status": "running"})
    )
    f = Crawl4AIFetcher(poll_interval=0.0, poll_timeout=0.05)

    with pytest.raises(RuntimeError, match="timeout"):
        await f.extract("https://example.com/article")


@pytest.mark.asyncio
@respx.mock
async def test_no_markdown_and_no_task_id_raises(allow_public_dns):
    respx.post("http://localhost:11235/crawl/job").mock(
        return_value=httpx.Response(200, json={"status": "queued"})
    )
    f = Crawl4AIFetcher()

    with pytest.raises(RuntimeError, match="no markdown"):
        await f.extract("https://example.com/article")


@pytest.mark.asyncio
@respx.mock
async def test_api_headers_sent_on_poll(allow_public_dns, no_sleep):
    respx.post("http://localhost:11235/crawl/job").mock(
        return_value=httpx.Response(200, json={"task_id": "t5"})
    )
    poll_route = respx.get("http://localhost:11235/crawl/job/t5").mock(
        return_value=httpx.Response(
            200,
            json={
                "status": "completed",
                "results": [{"markdown": "x"}],
            },
        )
    )
    f = Crawl4AIFetcher(
        api_headers={"Authorization": "Bearer tok"},
        poll_interval=0.01,
        poll_timeout=5.0,
    )

    await f.extract("https://example.com/article")

    assert poll_route.calls[0].request.headers["Authorization"] == "Bearer tok"


@pytest.mark.asyncio
@respx.mock
async def test_poll_inline_markdown_in_poll_response_skips_status(allow_public_dns, no_sleep):
    respx.post("http://localhost:11235/crawl/job").mock(
        return_value=httpx.Response(200, json={"task_id": "t6"})
    )
    respx.get("http://localhost:11235/crawl/job/t6").mock(
        return_value=httpx.Response(
            200,
            json={
                "status": "unknown_status",
                "results": [{"markdown": "inline from poll"}],
            },
        )
    )
    f = Crawl4AIFetcher(poll_interval=0.01, poll_timeout=5.0)

    out = await f.extract("https://example.com/article")

    assert out == "inline from poll"
