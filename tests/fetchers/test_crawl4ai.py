import json
import socket
from unittest.mock import AsyncMock

import httpx
import pytest
import respx

from sluice.fetchers._ssrf import SSRFError
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
    assert f.poll_paths == ["/task/{task_id}", "/jobs/{task_id}"]


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
    respx.post("http://localhost:11235/crawl").mock(
        return_value=httpx.Response(200, json={"results": [{"markdown": "article text"}]})
    )
    f = Crawl4AIFetcher()

    out = await f.extract("https://example.com/article")

    assert out == "article text"


@pytest.mark.asyncio
@respx.mock
async def test_sync_data_response(allow_public_dns):
    respx.post("http://localhost:11235/crawl").mock(
        return_value=httpx.Response(200, json={"data": {"markdown": "data md"}})
    )
    f = Crawl4AIFetcher()

    out = await f.extract("https://example.com/article")

    assert out == "data md"


@pytest.mark.asyncio
@respx.mock
async def test_initial_request_does_not_probe_job_endpoint(allow_public_dns):
    job_route = respx.post("http://localhost:11235/crawl/job").mock(
        return_value=httpx.Response(500)
    )
    respx.post("http://localhost:11235/crawl").mock(
        return_value=httpx.Response(200, json={"results": [{"markdown": "sync crawl"}]})
    )
    f = Crawl4AIFetcher()

    out = await f.extract("https://example.com/article")

    assert out == "sync crawl"
    assert job_route.call_count == 0


@pytest.mark.asyncio
@respx.mock
async def test_initial_request_posts_urls_array(allow_public_dns):
    route = respx.post("http://localhost:11235/crawl").mock(
        return_value=httpx.Response(200, json={"results": [{"markdown": "x"}]})
    )
    f = Crawl4AIFetcher()

    await f.extract("https://example.com/article")

    assert route.calls[0].request.read() == b'{"urls":["https://example.com/article"]}'


@pytest.mark.asyncio
@respx.mock
async def test_initial_request_sends_wait_options(allow_public_dns):
    route = respx.post("http://localhost:11235/crawl").mock(
        return_value=httpx.Response(200, json={"results": [{"markdown": "x"}]})
    )
    f = Crawl4AIFetcher(
        wait_for="css:.article",
        wait_for_timeout=10000,
        wait_until="networkidle",
        delay_before_return_html=1.0,
    )

    await f.extract("https://example.com/article")

    assert json.loads(route.calls[0].request.content) == {
        "urls": ["https://example.com/article"],
        "wait_for": "css:.article",
        "wait_for_timeout": 10000,
        "wait_until": "networkidle",
        "delay_before_return_html": 1.0,
    }


@pytest.mark.asyncio
@respx.mock
async def test_api_headers_on_initial_request(allow_public_dns):
    route = respx.post("http://localhost:11235/crawl").mock(
        return_value=httpx.Response(200, json={"results": [{"markdown": "x"}]})
    )
    f = Crawl4AIFetcher(api_headers={"Authorization": "Bearer secret"})

    await f.extract("https://example.com/article")

    assert route.calls[0].request.headers["Authorization"] == "Bearer secret"


@pytest.mark.asyncio
@respx.mock
async def test_api_key_fallback(allow_public_dns):
    route = respx.post("http://localhost:11235/crawl").mock(
        return_value=httpx.Response(200, json={"results": [{"markdown": "x"}]})
    )
    f = Crawl4AIFetcher(api_key="mykey")

    await f.extract("https://example.com/article")

    assert route.calls[0].request.headers["Authorization"] == "Bearer mykey"


@pytest.mark.asyncio
@respx.mock
async def test_api_key_skipped_when_lowercase_auth_present(allow_public_dns):
    route = respx.post("http://localhost:11235/crawl").mock(
        return_value=httpx.Response(200, json={"results": [{"markdown": "x"}]})
    )
    f = Crawl4AIFetcher(api_key="ignored", api_headers={"authorization": "Bearer lower"})

    await f.extract("https://example.com/article")

    assert route.calls[0].request.headers["authorization"] == "Bearer lower"
    assert "Bearer ignored" not in str(route.calls[0].request.headers)


@pytest.mark.asyncio
async def test_blocks_private_target_url():
    f = Crawl4AIFetcher()

    with pytest.raises(SSRFError):
        await f.extract("http://169.254.169.254/latest/meta-data/")


@pytest.mark.asyncio
@respx.mock
async def test_poll_task_id(allow_public_dns, no_sleep):
    respx.post("https://crawl.example/crawl").mock(
        return_value=httpx.Response(200, json={"task_id": "t1"})
    )
    respx.get("https://crawl.example/task/t1").mock(
        return_value=httpx.Response(
            200,
            json={
                "status": "completed",
                "results": [{"markdown": "polled"}],
            },
        )
    )
    f = Crawl4AIFetcher(base_url="https://crawl.example", poll_interval=0.01, poll_timeout=5.0)

    assert await f.extract("https://example.com/article") == "polled"


@pytest.mark.asyncio
@respx.mock
async def test_poll_allows_default_local_service_base_url(allow_public_dns, no_sleep):
    respx.post("http://localhost:11235/crawl").mock(
        return_value=httpx.Response(200, json={"task_id": "local-task"})
    )
    respx.get("http://localhost:11235/task/local-task").mock(
        return_value=httpx.Response(200, json={"status": "completed", "markdown": "local poll"})
    )
    f = Crawl4AIFetcher(poll_interval=0.01, poll_timeout=5.0)

    assert await f.extract("https://example.com/article") == "local poll"


@pytest.mark.asyncio
@respx.mock
async def test_poll_job_id_field(allow_public_dns, no_sleep):
    respx.post("https://crawl.example/crawl").mock(
        return_value=httpx.Response(200, json={"job_id": "j1"})
    )
    respx.get("https://crawl.example/task/j1").mock(
        return_value=httpx.Response(
            200,
            json={
                "status": "success",
                "results": [{"markdown": "job polled"}],
            },
        )
    )
    f = Crawl4AIFetcher(base_url="https://crawl.example", poll_interval=0.01, poll_timeout=5.0)

    assert await f.extract("https://example.com/article") == "job polled"


@pytest.mark.asyncio
@respx.mock
async def test_poll_id_field(allow_public_dns, no_sleep):
    respx.post("https://crawl.example/crawl").mock(
        return_value=httpx.Response(200, json={"id": "i1"})
    )
    respx.get("https://crawl.example/task/i1").mock(
        return_value=httpx.Response(
            200,
            json={
                "status": "done",
                "results": [{"markdown": "id polled"}],
            },
        )
    )
    f = Crawl4AIFetcher(base_url="https://crawl.example", poll_interval=0.01, poll_timeout=5.0)

    assert await f.extract("https://example.com/article") == "id polled"


@pytest.mark.asyncio
@respx.mock
async def test_poll_fallback_path_on_404(allow_public_dns, no_sleep):
    respx.post("https://crawl.example/crawl").mock(
        return_value=httpx.Response(200, json={"task_id": "t2"})
    )
    respx.get("https://crawl.example/task/t2").mock(return_value=httpx.Response(404))
    respx.get("https://crawl.example/jobs/t2").mock(
        return_value=httpx.Response(
            200,
            json={
                "status": "done",
                "results": [{"markdown": "fallback path"}],
            },
        )
    )
    f = Crawl4AIFetcher(base_url="https://crawl.example", poll_interval=0.01, poll_timeout=5.0)

    assert await f.extract("https://example.com/article") == "fallback path"


@pytest.mark.asyncio
@respx.mock
async def test_poll_http_error_does_not_lock_bad_path(allow_public_dns, no_sleep):
    respx.post("https://crawl.example/crawl").mock(
        return_value=httpx.Response(200, json={"task_id": "t-http-error"})
    )
    bad_route = respx.get("https://crawl.example/task/t-http-error").mock(
        side_effect=httpx.ConnectError("temporary network failure")
    )
    fallback_route = respx.get("https://crawl.example/jobs/t-http-error").mock(
        return_value=httpx.Response(200, json={"status": "completed", "markdown": "fallback"})
    )
    f = Crawl4AIFetcher(base_url="https://crawl.example", poll_interval=0.01, poll_timeout=5.0)

    assert await f.extract("https://example.com/article") == "fallback"
    assert bad_route.call_count == 1
    assert fallback_route.call_count == 1


@pytest.mark.asyncio
@respx.mock
async def test_poll_url_is_ssrf_guarded_before_get(allow_public_dns, no_sleep, monkeypatch):
    guarded_urls = []

    def fake_guard(url):
        guarded_urls.append(url)

    monkeypatch.setattr("sluice.fetchers.crawl4ai.guard", fake_guard)
    respx.post("https://crawl.example/crawl").mock(
        return_value=httpx.Response(200, json={"task_id": "t-guard"})
    )
    respx.get("https://crawl.example/task/t-guard").mock(
        return_value=httpx.Response(200, json={"status": "completed", "markdown": "guarded"})
    )
    f = Crawl4AIFetcher(base_url="https://crawl.example", poll_interval=0.01, poll_timeout=5.0)

    assert await f.extract("https://example.com/article") == "guarded"
    assert guarded_urls == [
        "https://example.com/article",
        "https://crawl.example/task/t-guard",
    ]


@pytest.mark.asyncio
@respx.mock
async def test_poll_failure_status_raises(allow_public_dns, no_sleep):
    respx.post("https://crawl.example/crawl").mock(
        return_value=httpx.Response(200, json={"task_id": "t3"})
    )
    respx.get("https://crawl.example/task/t3").mock(
        return_value=httpx.Response(200, json={"status": "failed"})
    )
    f = Crawl4AIFetcher(base_url="https://crawl.example", poll_interval=0.01, poll_timeout=5.0)

    with pytest.raises(RuntimeError, match="failed"):
        await f.extract("https://example.com/article")


@pytest.mark.asyncio
@respx.mock
async def test_poll_success_without_markdown_raises(allow_public_dns, no_sleep):
    respx.post("https://crawl.example/crawl").mock(
        return_value=httpx.Response(200, json={"task_id": "t-success-empty"})
    )
    respx.get("https://crawl.example/task/t-success-empty").mock(
        return_value=httpx.Response(200, json={"status": "completed"})
    )
    f = Crawl4AIFetcher(base_url="https://crawl.example", poll_interval=0.01, poll_timeout=5.0)

    with pytest.raises(RuntimeError, match="completed but no markdown"):
        await f.extract("https://example.com/article")


@pytest.mark.asyncio
@respx.mock
async def test_poll_retries_5xx_until_success(allow_public_dns, no_sleep):
    respx.post("https://crawl.example/crawl").mock(
        return_value=httpx.Response(200, json={"task_id": "t-retry"})
    )
    route = respx.get("https://crawl.example/task/t-retry").mock(
        side_effect=[
            httpx.Response(502),
            httpx.Response(200, json={"status": "completed", "markdown": "recovered"}),
        ]
    )
    f = Crawl4AIFetcher(base_url="https://crawl.example", poll_interval=0.01, poll_timeout=5.0)

    assert await f.extract("https://example.com/article") == "recovered"
    assert route.call_count == 2


@pytest.mark.asyncio
@respx.mock
async def test_poll_timeout_raises(allow_public_dns, no_sleep, monkeypatch):
    tick = [0.0]

    def fake_monotonic():
        tick[0] += 0.1
        return tick[0]

    monkeypatch.setattr("sluice.fetchers.crawl4ai.time.monotonic", fake_monotonic)
    respx.post("https://crawl.example/crawl").mock(
        return_value=httpx.Response(200, json={"task_id": "t4"})
    )
    respx.get("https://crawl.example/task/t4").mock(
        return_value=httpx.Response(200, json={"status": "running"})
    )
    f = Crawl4AIFetcher(base_url="https://crawl.example", poll_interval=0.0, poll_timeout=0.05)

    with pytest.raises(RuntimeError, match="timeout"):
        await f.extract("https://example.com/article")


@pytest.mark.asyncio
@respx.mock
async def test_no_markdown_and_no_task_id_raises(allow_public_dns):
    respx.post("http://localhost:11235/crawl").mock(
        return_value=httpx.Response(200, json={"status": "queued"})
    )
    f = Crawl4AIFetcher()

    with pytest.raises(RuntimeError, match="no markdown"):
        await f.extract("https://example.com/article")


@pytest.mark.asyncio
@respx.mock
async def test_api_headers_sent_on_poll(allow_public_dns, no_sleep):
    respx.post("https://crawl.example/crawl").mock(
        return_value=httpx.Response(200, json={"task_id": "t5"})
    )
    poll_route = respx.get("https://crawl.example/task/t5").mock(
        return_value=httpx.Response(
            200,
            json={
                "status": "completed",
                "results": [{"markdown": "x"}],
            },
        )
    )
    f = Crawl4AIFetcher(
        base_url="https://crawl.example",
        api_headers={"Authorization": "Bearer tok"},
        poll_interval=0.01,
        poll_timeout=5.0,
    )

    await f.extract("https://example.com/article")

    assert poll_route.calls[0].request.headers["Authorization"] == "Bearer tok"


@pytest.mark.asyncio
@respx.mock
async def test_poll_inline_markdown_in_poll_response_skips_status(allow_public_dns, no_sleep):
    respx.post("https://crawl.example/crawl").mock(
        return_value=httpx.Response(200, json={"task_id": "t6"})
    )
    respx.get("https://crawl.example/task/t6").mock(
        return_value=httpx.Response(
            200,
            json={
                "status": "unknown_status",
                "results": [{"markdown": "inline from poll"}],
            },
        )
    )
    f = Crawl4AIFetcher(base_url="https://crawl.example", poll_interval=0.01, poll_timeout=5.0)

    out = await f.extract("https://example.com/article")

    assert out == "inline from poll"
