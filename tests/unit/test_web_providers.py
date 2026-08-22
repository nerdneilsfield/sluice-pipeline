"""Task 6 red tests for fixed provider adapters and runtime catalog."""

from __future__ import annotations

import base64
from dataclasses import dataclass, replace
from datetime import timedelta
import json
import logging
from threading import Event, Thread
from types import SimpleNamespace
from urllib.parse import urlsplit

import pytest

import sluice.web as web_module
from sluice.errors import DefinitionError, WebSafetyError
from sluice.web import extractors as extractor_module
from sluice.web import http as http_module
from sluice.web import providers as provider_module
from sluice.web import registry as registry_module
from sluice.web.contracts import (
    DocumentArtifact,
    ExtractorBinding,
    FetchRequest,
    NO_MATCH,
    ProviderBinding,
    QualityPolicy,
    WebAttemptCancelled,
    WebBinding,
    WebExecutionProbe,
    reset_web_execution_probe,
    set_web_execution_probe,
)
from sluice.web.http import DirectResponse
from sluice.web.providers import (
    Crawl4AIProvider,
    FirecrawlProvider,
    build_provider,
)
from sluice.web.registry import WebRuntimeCatalog, resolve_provider_binding
from sluice.web.url import normalize_target_url


@dataclass
class _FakeTransport:
    body: str

    def get(self, url: str, **kwargs: object) -> DirectResponse:
        return DirectResponse(
            status_code=200,
            headers={"content-type": "text/html"},
            body=self.body.encode("utf-8"),
            url=url,
            final_url=url,
            peer="93.184.216.34",
        )


def _runtime(transport: object) -> object:
    return type("Runtime", (), {"http_client": transport})()


def _request(*, acceptable: frozenset[str] = frozenset({"text", "raw_html", "markdown"})) -> FetchRequest:
    preferred = tuple(item for item in ("markdown", "raw_html", "text") if item in acceptable)
    return FetchRequest(
        url="https://example.test/article",
        mode="article",
        acceptable=acceptable,
        preferred=preferred,
        quality=QualityPolicy(min_chars=1),
    )


@dataclass
class _ServiceRequest:
    path: str
    json: dict[str, object]
    headers: dict[str, str]
    url: str
    follow_redirects: object


class _FakeService:
    def __init__(
        self,
        payload: object,
        *,
        status_code: int = 200,
        response_headers: dict[str, str] | None = None,
    ) -> None:
        self.payload = payload
        self.status_code = status_code
        self.response_headers = response_headers or {
            "content-type": "application/json"
        }
        self.requests: list[_ServiceRequest] = []

    def post(
        self,
        url: str,
        *,
        json: dict[str, object],
        headers: dict[str, str],
        **kwargs: object,
    ) -> DirectResponse:
        path = urlsplit(url).path if "://" in url else url
        self.requests.append(
            _ServiceRequest(path, json, headers, url, kwargs.get("follow_redirects"))
        )
        peer = "10.0.0.8" if ".internal" in url else "93.184.216.34"
        return DirectResponse(
            status_code=self.status_code,
            headers=self.response_headers,
            body=json_module_dumps(self.payload),
            url=url if "://" in url else f"https://service.example{path}",
            final_url=url if "://" in url else f"https://service.example{path}",
            peer=peer,
        )


def json_module_dumps(value: object) -> bytes:
    return json.dumps(value).encode("utf-8")


def test_firecrawl_article_requests_markdown_and_raw_html_once(monkeypatch) -> None:
    monkeypatch.setattr(
        "sluice.web.url.resolve_host", lambda host: ["93.184.216.34"]
    )
    service = _FakeService(
        {
            "success": True,
            "data": {
                "markdown": "article markdown",
                "rawHtml": "<html><body>article</body></html>",
                "metadata": {"sourceURL": "https://example.test/article"},
            },
        }
    )
    binding = ProviderBinding(
        name="fire",
        type="firecrawl",
        base_url="https://api.firecrawl.dev",
        only_main_content=True,
        wait_for_selector="main",
        wait_for=2,
        mobile=True,
        actions=(
            {"wait": {"milliseconds": 10}},
            {"click": {"selector": "#load"}},
            {"write": {"text": "sluice"}},
            {"press": {"key": "Enter"}},
            {"scroll": {"direction": "down"}},
        ),
        target_headers={"X-Binding": "binding"},
        cookies={"session": "cookie-secret"},
        capabilities=frozenset({"raw_html", "markdown", "rendered_js"}),
    )

    request = FetchRequest(
        url="https://example.test/article",
        mode="article",
        acceptable=frozenset({"text", "raw_html", "markdown"}),
        preferred=("markdown", "raw_html", "text"),
        target_headers={"X-Request": "request"},
        quality=QualityPolicy(min_chars=1),
    )

    artifact = FirecrawlProvider(binding, service).fetch(request)

    assert [request.path for request in service.requests] == ["/v2/scrape"]
    payload = service.requests[0].json
    assert payload["url"] == "https://example.test/article"
    assert payload["formats"] == ["markdown", "rawHtml"]
    assert payload["onlyMainContent"] is True
    assert "waitForSelector" not in payload
    assert payload["waitFor"] == 2_000
    assert payload["mobile"] is True
    assert payload["actions"] == [
        {"type": "wait", "selector": "main"},
        {"type": "wait", "milliseconds": 10},
        {"type": "click", "selector": "#load"},
        {"type": "write", "text": "sluice"},
        {"type": "press", "key": "Enter"},
        {"type": "scroll", "direction": "down"},
    ]
    assert payload["maxAge"] == 0
    assert payload["storeInCache"] is False
    assert payload["skipTlsVerification"] is False
    assert payload["headers"] == {
        "X-Binding": "binding",
        "X-Request": "request",
        "Cookie": "session=cookie-secret",
    }
    assert artifact.markdown == "article markdown"
    assert artifact.raw_html == "<html><body>article</body></html>"
    assert service.requests[0].url == "https://api.firecrawl.dev/v2/scrape"
    assert service.requests[0].follow_redirects is False


def test_firecrawl_list_requests_raw_html_only(monkeypatch) -> None:
    monkeypatch.setattr(
        "sluice.web.url.resolve_host", lambda host: ["93.184.216.34"]
    )
    service = _FakeService(
        {"success": True, "data": {"rawHtml": "<html><body>list</body></html>"}}
    )
    binding = ProviderBinding(
        name="fire",
        type="firecrawl",
        base_url="https://api.firecrawl.dev",
        capabilities=frozenset({"raw_html", "markdown", "rendered_js"}),
    )
    request = FetchRequest(
        url="https://example.test/releases",
        mode="list_html",
        acceptable=frozenset({"raw_html"}),
        preferred=("raw_html",),
        list_selector="article",
        quality=QualityPolicy(min_chars=1),
    )

    artifact = FirecrawlProvider(binding, service).fetch(request)

    assert service.requests[0].json["formats"] == ["rawHtml"]
    assert artifact.raw_html == "<html><body>list</body></html>"
    assert artifact.markdown is None


def test_crawl4ai_uses_sync_crawl_never_legacy_poll_paths(monkeypatch) -> None:
    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["10.0.0.8"])
    service = _FakeService(
        {
            "success": True,
            "url": "https://example.test/article",
            "html": "<html><body>article</body></html>",
            "markdown": "article markdown",
            "status_code": 200,
        }
    )
    binding = ProviderBinding(
        name="browser",
        type="crawl4ai",
        base_url="http://crawl4ai.internal:11235",
        options={
            "api_key": "service-secret",
            "browser_type": "firefox",
            "headless": False,
            "wait_until": "networkidle",
            "wait_for_selector": ".ready",
            "delay_before_return_html": 3,
        },
        target_headers={"X-Binding": "binding"},
        cookies={"session": "cookie-secret"},
        capabilities=frozenset({"raw_html", "markdown", "rendered_js"}),
    )

    request = FetchRequest(
        url="https://example.test/article",
        mode="article",
        acceptable=frozenset({"text", "raw_html", "markdown"}),
        preferred=("markdown", "raw_html", "text"),
        target_headers={"X-Request": "request"},
        quality=QualityPolicy(min_chars=1),
    )

    artifact = Crawl4AIProvider(binding, service).fetch(request)

    assert [request.path for request in service.requests] == ["/crawl"]
    payload = service.requests[0].json
    assert payload == {
        "urls": ["https://example.test/article"],
        "browser_config": {
            "type": "BrowserConfig",
            "params": {
                "browser_type": "firefox",
                "headless": False,
                "headers": {
                    "type": "dict",
                    "value": {
                        "X-Binding": "binding",
                        "X-Request": "request",
                    },
                },
                "cookies": [
                    {
                        "type": "dict",
                        "value": {
                            "name": "session",
                            "value": "cookie-secret",
                            "url": "https://example.test/article",
                        },
                    }
                ],
            },
        },
        "crawler_config": {
            "type": "CrawlerRunConfig",
            "params": {
                "cache_mode": "bypass",
                "stream": False,
                "wait_until": "networkidle",
                "wait_for": ".ready",
                "delay_before_return_html": 3,
            },
        },
    }
    assert service.requests[0].headers == {"Authorization": "Bearer service-secret"}
    assert artifact.raw_html == "<html><body>article</body></html>"
    assert artifact.markdown == "article markdown"
    assert service.requests[0].url == "http://crawl4ai.internal:11235/crawl"
    assert service.requests[0].follow_redirects is False


def test_crawl4ai_self_host_can_omit_bearer_authentication(monkeypatch) -> None:
    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["10.0.0.8"])
    service = _FakeService(
        {"success": True, "html": "<article>body</article>", "markdown": "body"}
    )
    binding = ProviderBinding(
        name="browser",
        type="crawl4ai",
        base_url="http://crawl4ai.internal:11235",
        capabilities=frozenset({"raw_html", "markdown", "rendered_js"}),
    )

    Crawl4AIProvider(binding, service).fetch(_request())

    assert service.requests[0].headers == {}


def test_jina_reader_posts_for_json_markdown_with_declared_reader_headers(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "sluice.web.url.resolve_host", lambda host: ["93.184.216.34"]
    )
    service = _FakeService(
        {
            "code": 200,
            "data": {
                "url": "https://example.test/article",
                "title": "Reader title",
                "content": "# Reader markdown\n\nUseful body.",
            },
        }
    )
    binding = ProviderBinding(
        name="reader",
        type="jina_reader",
        base_url="https://r.jina.ai",
        options={
            "api_key": "jina-secret",
            "target_selector": "article",
            "with_generated_alt": True,
            "remove_selectors": ("nav", ".ads"),
        },
        capabilities=frozenset({"markdown"}),
    )

    provider = getattr(provider_module, "JinaReaderProvider")(binding, service)
    artifact = provider.fetch(_request())

    assert [request.path for request in service.requests] == ["/"]
    assert service.requests[0].json == {"url": "https://example.test/article"}
    assert service.requests[0].headers == {
        "Accept": "application/json",
        "Authorization": "Bearer jina-secret",
        "X-Remove-Selector": "nav,.ads",
        "X-Target-Selector": "article",
        "X-With-Generated-Alt": "true",
    }
    assert artifact.markdown == "# Reader markdown\n\nUseful body."
    assert artifact.raw_html is None
    assert artifact.text is None
    assert artifact.title == "Reader title"
    assert artifact.capabilities == frozenset({"markdown"})


def test_zyte_browser_mode_maps_page_content_type_action_and_basic_auth(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "sluice.web.url.resolve_host", lambda host: ["93.184.216.34"]
    )
    service = _FakeService(
        {
            "url": "https://example.test/article",
            "statusCode": 200,
            "browserHtml": "<article>Rendered article</article>",
            "pageContent": {
                "title": "Rendered title",
                "itemMain": "Rendered article",
            },
        }
    )
    binding = ProviderBinding(
        name="zyte-browser",
        type="zyte",
        base_url="https://api.zyte.com",
        options={
            "api_key": "zyte-secret",
            "mode": "browser",
            "automatic_content": True,
            "actions": (
                {"write": {"selector": "#q", "text": "sluice"}},
            ),
        },
        capabilities=frozenset({"raw_html", "text"}),
    )

    provider = getattr(provider_module, "ZyteProvider")(binding, service)
    artifact = provider.fetch(_request())

    assert [request.path for request in service.requests] == ["/v1/extract"]
    payload = service.requests[0].json
    assert payload["url"] == "https://example.test/article"
    assert payload["browserHtml"] is True
    assert payload["pageContent"] is True
    assert payload["pageContentOptions"] == {"extractFrom": "browserHtml"}
    assert payload["actions"] == [
        {
            "action": "type",
            "selector": {"type": "css", "value": "#q"},
            "text": "sluice",
        }
    ]
    encoded = base64.b64encode(b"zyte-secret:").decode("ascii")
    assert service.requests[0].headers == {"Authorization": f"Basic {encoded}"}
    assert artifact.raw_html == "<article>Rendered article</article>"
    assert artifact.text == "Rendered article"
    assert artifact.title == "Rendered title"
    assert artifact.status_code == 200
    assert artifact.capabilities == frozenset({"raw_html", "text"})


@pytest.mark.parametrize(
    ("mode", "expected"),
    [
        (
            "auto",
            {
                "url": "https://example.test/article",
                "browserHtml": True,
                "pageContent": True,
            },
        ),
        (
            "http",
            {
                "url": "https://example.test/article",
                "httpResponseBody": True,
                "pageContent": True,
                "pageContentOptions": {"extractFrom": "httpResponseBody"},
            },
        ),
        (
            "browser",
            {
                "url": "https://example.test/article",
                "browserHtml": True,
                "pageContent": True,
                "pageContentOptions": {"extractFrom": "browserHtml"},
            },
        ),
    ],
)
def test_zyte_mode_mapping_is_exact(
    mode: str,
    expected: dict[str, object],
) -> None:
    binding = ProviderBinding(
        name=f"zyte-{mode}",
        type="zyte",
        options={"mode": mode, "automatic_content": True},
    )

    assert provider_module.zyte_payload(binding, _request()) == expected


def test_zyte_translates_the_complete_validated_action_subset() -> None:
    assert provider_module.zyte_actions(
        [
            {"wait": {"milliseconds": 1500}},
            {"click": {"selector": "#open"}},
            {"write": {"selector": "#q", "text": "sluice"}},
            {"press": {"key": "Enter"}},
            {"scroll": {"x": 12, "y": 34}},
        ]
    ) == [
        {"action": "waitForTimeout", "timeout": 1.5},
        {
            "action": "click",
            "selector": {"type": "css", "value": "#open"},
        },
        {
            "action": "type",
            "selector": {"type": "css", "value": "#q"},
            "text": "sluice",
        },
        {"action": "keyPress", "key": "Enter"},
        {"action": "scrollTo", "x": 12, "y": 34},
    ]


@pytest.mark.parametrize(
    (
        "output",
        "expected_payload",
        "field",
        "response_content_type",
        "artifact_content_type",
    ),
    [
        (
            "markdown",
            {
                "url": "https://example.test/article",
                "zone": "unlocker-zone",
                "format": "raw",
                "data_format": "markdown",
                "country": "US",
            },
            "markdown",
            "text/html",
            "text/markdown",
        ),
        (
            "html",
            {
                "url": "https://example.test/article",
                "zone": "unlocker-zone",
                "format": "raw",
                "country": "US",
            },
            "raw_html",
            "text/html",
            "text/html",
        ),
    ],
)
def test_bright_data_uses_raw_sync_envelope_and_only_requests_markdown_when_configured(
    monkeypatch,
    output: str,
    expected_payload: dict[str, object],
    field: str,
    response_content_type: str,
    artifact_content_type: str,
) -> None:
    monkeypatch.setattr(
        "sluice.web.url.resolve_host", lambda host: ["93.184.216.34"]
    )
    body = "# Bright markdown" if output == "markdown" else "<main>Bright HTML</main>"
    service = _FakeService(
        {
            "status_code": 200,
            "headers": {"content-type": response_content_type},
            "body": body,
        }
    )
    binding = ProviderBinding(
        name=f"bright-{output}",
        type="bright_data",
        base_url="https://api.brightdata.com",
        options={
            "api_key": "bright-secret",
            "zone": "unlocker-zone",
            "output": output,
            "country": "US",
            "async": False,
        },
        capabilities=frozenset({field}),
    )

    provider = getattr(provider_module, "BrightDataProvider")(binding, service)
    artifact = provider.fetch(_request())

    assert [request.path for request in service.requests] == ["/request"]
    assert service.requests[0].json == expected_payload
    assert service.requests[0].headers == {
        "Authorization": "Bearer bright-secret"
    }
    assert getattr(artifact, field) == body
    assert artifact.capabilities == frozenset({field})
    assert artifact.status_code == 200
    assert artifact.content_type == artifact_content_type


def test_provider_deadline_bounds_service_body_stream(monkeypatch) -> None:
    clock = [100.0]
    monkeypatch.setattr(
        http_module,
        "time",
        SimpleNamespace(monotonic=lambda: clock[0]),
        raising=False,
    )
    monkeypatch.setattr(
        "sluice.web.url.resolve_host", lambda host: ["93.184.216.34"]
    )

    class StreamingResponse:
        status_code = 200
        headers = {"content-type": "application/json"}
        url = "https://api.zyte.com/v1/extract"
        peer = "93.184.216.34"

        def iter_bytes(self):
            yield b'{"browserHtml":"partial'
            clock[0] = 106.0
            yield b' body"}'

    class Service:
        calls = 0

        def post(self, url: str, **kwargs: object) -> StreamingResponse:
            del url, kwargs
            self.calls += 1
            return StreamingResponse()

    binding = ProviderBinding(
        name="zyte",
        type="zyte",
        base_url="https://api.zyte.com",
        timeout=timedelta(seconds=60),
        connect_timeout=timedelta(seconds=10),
    )
    service = Service()

    with pytest.raises(WebSafetyError, match="timeout"):
        http_module.bounded_service_post(
            "https://api.zyte.com/v1/extract",
            binding,
            {"url": "https://example.test/article", "browserHtml": True},
            transport=service,
            deadline=105.0,
        )

    assert service.calls == 1


def test_probe_is_checked_after_service_dns_and_before_send(monkeypatch) -> None:
    stale = False

    def resolve(host: str) -> list[str]:
        nonlocal stale
        assert host == "api.zyte.com"
        stale = True
        return ["93.184.216.34"]

    monkeypatch.setattr("sluice.web.url.resolve_host", resolve)

    class Service:
        calls = 0

        def post(self, url: str, **kwargs: object) -> DirectResponse:
            del url, kwargs
            self.calls += 1
            return DirectResponse(
                status_code=200,
                headers={"content-type": "application/json"},
                body=b"{}",
            )

    binding = ProviderBinding(
        name="zyte",
        type="zyte",
        base_url="https://api.zyte.com",
        timeout=timedelta(seconds=60),
        connect_timeout=timedelta(seconds=10),
    )
    service = Service()

    def check() -> None:
        if stale:
            raise WebAttemptCancelled()

    token = set_web_execution_probe(WebExecutionProbe(check))
    try:
        with pytest.raises(WebAttemptCancelled):
            http_module.bounded_service_post(
                "https://api.zyte.com/v1/extract",
                binding,
                {"url": "https://example.test/article"},
                transport=service,
            )
    finally:
        reset_web_execution_probe(token)

    assert service.calls == 0


def test_deadline_is_rechecked_after_service_dns_and_before_send(monkeypatch) -> None:
    clock = [100.0]
    monkeypatch.setattr(
        http_module,
        "time",
        SimpleNamespace(monotonic=lambda: clock[0]),
        raising=False,
    )

    def resolve(host: str) -> list[str]:
        assert host == "api.zyte.com"
        clock[0] = 106.0
        return ["93.184.216.34"]

    monkeypatch.setattr("sluice.web.url.resolve_host", resolve)

    class Service:
        calls = 0

        def post(self, url: str, **kwargs: object) -> DirectResponse:
            del url, kwargs
            self.calls += 1
            return DirectResponse(
                status_code=200,
                headers={"content-type": "application/json"},
                body=b"{}",
            )

    binding = ProviderBinding(
        name="zyte",
        type="zyte",
        base_url="https://api.zyte.com",
        timeout=timedelta(seconds=60),
        connect_timeout=timedelta(seconds=10),
    )
    service = Service()

    with pytest.raises(WebSafetyError, match="timeout"):
        http_module.bounded_service_post(
            "https://api.zyte.com/v1/extract",
            binding,
            {"url": "https://example.test/article"},
            transport=service,
            deadline=105.0,
        )

    assert service.calls == 0


def test_curl_connect_read_and_callback_share_the_absolute_deadline(
    monkeypatch,
) -> None:
    clock = [100.0]
    monkeypatch.setattr(
        registry_module.time,
        "monotonic",
        lambda: clock[0],
    )
    request_kwargs: dict[str, object] = {}

    class CurlOpt:
        RESOLVE = 1
        FRESH_CONNECT = 2
        FORBID_REUSE = 3

    class Session:
        def __init__(self, **kwargs: object) -> None:
            self.curl_options = dict(kwargs.get("curl_options", {}))

        def request(self, method: str, url: str, **kwargs: object) -> object:
            del method, url
            request_kwargs.update(kwargs)
            callback = kwargs["content_callback"]
            assert callable(callback)
            callback(b"first")
            clock[0] = 106.0
            try:
                callback(b"late")
            except Exception:
                raise RuntimeError("curl callback aborted") from None
            raise AssertionError("the deadline must abort callback streaming")

        def close(self) -> None:
            pass

    transport = registry_module._ThreadLocalCurlSession(
        SimpleNamespace(Session=Session),
        SimpleNamespace(CurlOpt=CurlOpt),
        provider_name="curl",
        impersonate="chrome",
        http_version="auto",
    )
    binding = ProviderBinding(
        name="curl",
        type="curl_cffi",
        timeout=timedelta(seconds=60),
        connect_timeout=timedelta(seconds=10),
    )
    request = replace(_request(), stage_deadline=105.0)
    try:
        with pytest.raises(WebSafetyError, match="timeout"):
            transport.get_pinned(
                normalize_target_url("https://example.test/article"),
                ("93.184.216.34",),
                request,
                binding,
            )
    finally:
        transport.close()

    assert request_kwargs["timeout"] == (5.0, 5.0)


def test_pooled_httpx_request_clamps_connect_and_read_to_remaining_budget(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        http_module,
        "time",
        SimpleNamespace(monotonic=lambda: 100.0),
        raising=False,
    )

    class Response:
        status_code = 200
        headers = {"content-type": "application/json"}
        url = "https://api.zyte.com/v1/extract"
        extensions: dict[str, object] = {}

        def iter_bytes(self):
            yield b"{}"

        def close(self) -> None:
            pass

    class Client:
        request_extensions: dict[str, object] | None = None

        def send(self, request, *, stream: bool):
            assert stream is True
            self.request_extensions = dict(request.extensions)
            return Response()

        def close(self) -> None:
            pass

    binding = ProviderBinding(
        name="zyte",
        type="zyte",
        timeout=timedelta(seconds=60),
        connect_timeout=timedelta(seconds=10),
    )
    request = FetchRequest(
        url="https://api.zyte.com/v1/extract",
        mode="article",
        acceptable=frozenset({"text"}),
        stage_deadline=105.0,
    )
    pooled = http_module._PooledPinnedHTTPXClient(
        timeout=None,
    )
    pooled._client.close()
    client = Client()
    pooled._client = client
    try:
        pooled.post_pinned(
            normalize_target_url("https://api.zyte.com/v1/extract"),
            ("93.184.216.34",),
            request,
            binding,
            {"url": "https://example.test/article"},
        )
    finally:
        pooled.close()

    assert client.request_extensions is not None
    assert client.request_extensions["timeout"] == {
        "connect": 5.0,
        "read": 5.0,
        "write": 5.0,
        "pool": 5.0,
    }


def test_service_429_exposes_only_safe_status_and_retry_after(monkeypatch) -> None:
    monkeypatch.setattr(
        "sluice.web.url.resolve_host", lambda host: ["93.184.216.34"]
    )
    service = _FakeService(
        {"detail": "vendor-body-with-zyte-secret"},
        status_code=429,
        response_headers={
            "content-type": "application/json",
            "retry-after": "23",
        },
    )
    binding = ProviderBinding(
        name="zyte",
        type="zyte",
        base_url="https://api.zyte.com",
        options={"api_key": "zyte-secret", "mode": "browser"},
        capabilities=frozenset({"raw_html", "text"}),
    )

    with pytest.raises(WebSafetyError) as raised:
        getattr(provider_module, "ZyteProvider")(binding, service).fetch(_request())

    assert raised.value.args == ("rate_limited",)
    assert raised.value.status_code == 429
    assert raised.value.status_kind == "service"
    assert raised.value.retry_after_seconds == 23.0
    assert "zyte-secret" not in str(raised.value)
    assert "vendor-body" not in str(raised.value)


def test_service_429_ignores_an_unbounded_retry_after_value(monkeypatch) -> None:
    monkeypatch.setattr(
        "sluice.web.url.resolve_host", lambda host: ["93.184.216.34"]
    )
    service = _FakeService(
        {"detail": "vendor error"},
        status_code=429,
        response_headers={
            "content-type": "application/json",
            "retry-after": "9" * 5000,
        },
    )
    binding = ProviderBinding(
        name="zyte",
        type="zyte",
        base_url="https://api.zyte.com",
        options={"api_key": "zyte-secret", "mode": "browser"},
        capabilities=frozenset({"raw_html", "text"}),
    )

    with pytest.raises(WebSafetyError) as raised:
        getattr(provider_module, "ZyteProvider")(binding, service).fetch(_request())

    assert raised.value.args == ("rate_limited",)
    assert raised.value.status_code == 429
    assert raised.value.status_kind == "service"
    assert raised.value.retry_after_seconds is None


def test_runtime_digest_shares_provider_transport_limiter_and_cooldown() -> None:
    binding = ProviderBinding(
        name="zyte-shared",
        type="zyte",
        base_url="https://api.zyte.com",
        options={"api_key": "zyte-secret", "mode": "browser"},
        timeout=timedelta(seconds=60),
        connect_timeout=timedelta(seconds=10),
        concurrency=1,
        cooldown=timedelta(seconds=30),
        capabilities=frozenset({"raw_html", "text"}),
    )
    retained_copy = replace(binding)
    operationally_distinct = replace(
        binding,
        cooldown=timedelta(seconds=31),
    )
    catalog = WebRuntimeCatalog(
        (binding, retained_copy, operationally_distinct)
    )
    try:
        shared_limiter = catalog.limiter(binding)
        shared_limiter.cooldown(5)

        assert catalog.provider(binding) is catalog.provider(retained_copy)
        assert catalog.transport_for(binding) is catalog.transport_for(retained_copy)
        assert shared_limiter is catalog.limiter(retained_copy)
        assert catalog.limiter(retained_copy).in_cooldown() is True
        assert catalog.provider(binding) is not catalog.provider(
            operationally_distinct
        )
        assert catalog.transport_for(binding) is not catalog.transport_for(
            operationally_distinct
        )
        assert shared_limiter is not catalog.limiter(operationally_distinct)
        assert catalog.client is catalog.client
    finally:
        catalog.close()


@pytest.mark.parametrize(
    ("provider_type", "class_name"),
    [
        ("jina_reader", "JinaReaderProvider"),
        ("zyte", "ZyteProvider"),
        ("bright_data", "BrightDataProvider"),
    ],
)
def test_hosted_provider_adapters_are_in_the_public_fixed_map(
    provider_type: str,
    class_name: str,
) -> None:
    binding = ProviderBinding(
        name=provider_type,
        type=provider_type,
        base_url={
            "jina_reader": "https://r.jina.ai",
            "zyte": "https://api.zyte.com",
            "bright_data": "https://api.brightdata.com",
        }[provider_type],
        options={
            "api_key": "service-secret",
            **(
                {"zone": "unlocker", "output": "markdown"}
                if provider_type == "bright_data"
                else {}
            ),
        },
        capabilities=frozenset({"markdown"}),
    )

    provider = build_provider(binding, _FakeService({"data": {}}))

    assert isinstance(provider, getattr(web_module, class_name))


@pytest.mark.parametrize(
    ("provider_type", "base_url", "address"),
    [
        ("firecrawl", "https://api.firecrawl.dev", "93.184.216.34"),
        ("crawl4ai", "http://crawl4ai.internal:11235", "10.0.0.8"),
    ],
)
def test_catalog_hosted_provider_prefers_binding_pinned_transport_over_global_http_client(
    monkeypatch, provider_type: str, base_url: str, address: str
) -> None:
    monkeypatch.setattr(
        "sluice.web.url.resolve_host", lambda host: [address]
    )

    class GlobalCompatibilityHTTPClient:
        def __init__(self) -> None:
            self.calls = 0

        def post(self, url: str, **kwargs: object) -> DirectResponse:
            del kwargs
            self.calls += 1
            return DirectResponse(
                status_code=200,
                headers={"content-type": "application/json"},
                body=json_module_dumps(
                    {
                        "success": True,
                        "data": {
                            "rawHtml": "<html>global</html>",
                            "metadata": {"sourceURL": "https://example.test/article"},
                        },
                    }
                ),
                url=url,
                final_url=url,
                peer=address,
            )

    class BindingPinnedService:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple[str, ...]]] = []

        def post_pinned(
            self,
            target: object,
            addresses: tuple[str, ...],
            request: FetchRequest,
            binding: ProviderBinding,
            payload: dict[str, object],
        ) -> DirectResponse:
            del request, binding, payload
            self.calls.append((str(target), addresses))
            return DirectResponse(
                status_code=200,
                headers={"content-type": "application/json"},
                body=json_module_dumps(
                    {
                        "success": True,
                        "data": {
                            "rawHtml": "<html>binding</html>",
                            "metadata": {"sourceURL": "https://example.test/article"},
                        },
                    }
                ),
                url=str(target),
                final_url=str(target),
                peer=address,
            )

    global_client = GlobalCompatibilityHTTPClient()
    binding_transport = BindingPinnedService()
    binding = ProviderBinding(
        name="hosted",
        type=provider_type,
        base_url=base_url,
        capabilities=frozenset({"raw_html", "markdown", "rendered_js"}),
    )
    runtime = SimpleNamespace(
        http_client=global_client,
        transports={binding.name: binding_transport},
    )
    catalog = WebRuntimeCatalog((binding,), runtime=runtime)
    try:
        artifact = catalog.provider(binding).fetch(_request())
    finally:
        catalog.close()

    assert artifact.raw_html == "<html>binding</html>"
    assert global_client.calls == 0
    assert binding_transport.calls == [
        (f"{base_url}/{'v2/scrape' if provider_type == 'firecrawl' else 'crawl'}", (address,))
    ]


@pytest.mark.parametrize("provider_type", ["firecrawl", "crawl4ai"])
def test_remote_provider_rejects_mixed_service_dns_before_post(
    monkeypatch, provider_type: str
) -> None:
    monkeypatch.setattr(
        "sluice.web.url.resolve_host",
        lambda host: ["93.184.216.34", "10.0.0.8"],
    )
    service = _FakeService(
        {"success": True, "data": {"rawHtml": "<html>body</html>"}}
    )
    binding = ProviderBinding(
        name="remote",
        type=provider_type,
        base_url="https://service.example",
        capabilities=frozenset({"raw_html", "markdown", "rendered_js"}),
    )
    provider = (
        FirecrawlProvider(binding, service)
        if provider_type == "firecrawl"
        else Crawl4AIProvider(binding, service)
    )

    with pytest.raises(WebSafetyError, match="ssrf_blocked"):
        provider.fetch(_request())

    assert service.requests == []


def test_service_type_error_after_send_is_not_retried(monkeypatch) -> None:
    monkeypatch.setattr(
        "sluice.web.url.resolve_host", lambda host: ["93.184.216.34"]
    )

    class TypeErrorAfterSend:
        calls = 0

        def post(self, url: str, **kwargs: object) -> object:
            del url, kwargs
            self.calls += 1
            raise TypeError("response decoding failed after request was sent")

    service = TypeErrorAfterSend()
    binding = ProviderBinding(
        name="fire",
        type="firecrawl",
        base_url="https://api.firecrawl.dev",
    )

    with pytest.raises(WebSafetyError, match="provider_error"):
        FirecrawlProvider(binding, service).fetch(_request())

    assert service.calls == 1


def test_remote_provider_requires_https_for_public_service_before_post(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "sluice.web.url.resolve_host", lambda host: ["93.184.216.34"]
    )
    service = _FakeService(
        {"success": True, "data": {"rawHtml": "<html>body</html>"}}
    )
    binding = ProviderBinding(
        name="fire",
        type="firecrawl",
        base_url="http://api.example.test",
    )

    with pytest.raises(WebSafetyError, match="service_https_required"):
        FirecrawlProvider(binding, service).fetch(_request())

    assert service.requests == []


def test_remote_provider_rejects_service_redirect_without_following(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "sluice.web.url.resolve_host", lambda host: ["93.184.216.34"]
    )

    class RedirectingService:
        calls = 0

        def post(self, url: str, **kwargs: object) -> DirectResponse:
            del kwargs
            self.calls += 1
            return DirectResponse(
                status_code=302,
                headers={"location": "https://attacker.example/steal"},
                url=url,
                final_url=url,
                peer="93.184.216.34",
            )

    service = RedirectingService()
    binding = ProviderBinding(
        name="fire",
        type="firecrawl",
        base_url="https://api.example.test",
    )

    with pytest.raises(WebSafetyError, match="service_redirect"):
        FirecrawlProvider(binding, service).fetch(_request())

    assert service.calls == 1


def test_remote_provider_rejects_unapproved_service_peer(monkeypatch) -> None:
    monkeypatch.setattr(
        "sluice.web.url.resolve_host", lambda host: ["93.184.216.34"]
    )

    class ReboundService:
        calls = 0

        def post(self, url: str, **kwargs: object) -> DirectResponse:
            del kwargs
            self.calls += 1
            return DirectResponse(
                status_code=200,
                headers={"content-type": "application/json"},
                body=json_module_dumps(
                    {"success": True, "data": {"rawHtml": "<html>body</html>"}}
                ),
                url=url,
                final_url=url,
                peer="93.184.216.35",
            )

    service = ReboundService()
    binding = ProviderBinding(
        name="fire",
        type="firecrawl",
        base_url="https://api.example.test",
    )

    with pytest.raises(WebSafetyError, match="peer_mismatch"):
        FirecrawlProvider(binding, service).fetch(_request())

    assert service.calls == 1


def test_remote_provider_rejects_changed_service_response_url(monkeypatch) -> None:
    monkeypatch.setattr(
        "sluice.web.url.resolve_host", lambda host: ["93.184.216.34"]
    )

    class ChangedURLService:
        calls = 0

        def post(self, url: str, **kwargs: object) -> DirectResponse:
            del url, kwargs
            self.calls += 1
            return DirectResponse(
                status_code=200,
                headers={"content-type": "application/json"},
                body=json_module_dumps(
                    {"success": True, "data": {"rawHtml": "<html>body</html>"}}
                ),
                url="https://attacker.example/stolen",
                final_url="https://attacker.example/stolen",
                peer="93.184.216.34",
            )

    service = ChangedURLService()
    binding = ProviderBinding(
        name="fire",
        type="firecrawl",
        base_url="https://api.example.test",
    )

    with pytest.raises(WebSafetyError, match="service_redirect"):
        FirecrawlProvider(binding, service).fetch(_request())

    assert service.calls == 1


@pytest.mark.parametrize("provider_type", ["firecrawl", "crawl4ai"])
def test_remote_provider_rejects_protected_target_headers_before_post(
    monkeypatch, provider_type: str
) -> None:
    monkeypatch.setattr(
        "sluice.web.url.resolve_host", lambda host: ["93.184.216.34"]
    )
    service = _FakeService(
        {"success": True, "data": {"rawHtml": "<html>body</html>"}}
    )
    binding = ProviderBinding(
        name="remote",
        type=provider_type,
        base_url="https://service.example",
        target_headers={"Host": "metadata.internal"},
    )
    provider = (
        FirecrawlProvider(binding, service)
        if provider_type == "firecrawl"
        else Crawl4AIProvider(binding, service)
    )

    with pytest.raises(WebSafetyError, match="forbidden_header"):
        provider.fetch(_request())

    assert service.requests == []


def test_catalog_rejects_ambiguous_typed_and_header_service_auth() -> None:
    binding = ProviderBinding(
        name="fire",
        type="firecrawl",
        options={"api_key": "typed-secret"},
        base_url="https://api.firecrawl.dev",
        service_headers={"AUTHORIZATION": "Bearer header-secret"},
    )

    with pytest.raises(DefinitionError, match="api_key.*Authorization"):
        WebRuntimeCatalog((binding,))


def test_catalog_service_post_uses_pinned_streaming_httpx_client(monkeypatch) -> None:
    monkeypatch.setattr(
        "sluice.web.url.resolve_host", lambda host: ["93.184.216.34"]
    )
    created: list[object] = []
    requests: list[object] = []

    class Stream:
        def get_extra_info(self, name: str):
            return ("93.184.216.34", 443) if name == "server_addr" else None

    class Response:
        status_code = 200
        headers = {"content-type": "application/json"}

        def __init__(self, request: object) -> None:
            self.url = getattr(request, "url")
            self.extensions = {"network_stream": Stream()}

        def iter_bytes(self):
            yield json_module_dumps(
                {"success": True, "data": {"rawHtml": "<html>body</html>"}}
            )

        def close(self) -> None:
            pass

    class Client:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs
            created.append(self)

        def send(self, request: object, *, stream: bool = False):
            assert stream is True
            requests.append(request)
            return Response(request)

        def close(self) -> None:
            pass

    monkeypatch.setattr("httpx.Client", Client)
    binding = ProviderBinding(
        name="fire",
        type="firecrawl",
        base_url="https://api.firecrawl.dev",
        capabilities=frozenset(
            {"raw_html", "raw_source", "markdown", "rendered_js"}
        ),
    )
    catalog = WebRuntimeCatalog((binding,))
    try:
        artifact = catalog.provider(binding).fetch(_request())
    finally:
        catalog.close()

    assert artifact.raw_html == "<html>body</html>"
    assert len(created) == 1
    assert created[0].kwargs["follow_redirects"] is False
    assert created[0].kwargs["trust_env"] is False
    assert len(requests) == 1
    assert getattr(requests[0], "method") == "POST"
    assert str(getattr(requests[0], "url")) == "https://api.firecrawl.dev/v2/scrape"
    assert getattr(requests[0], "extensions") == {
        "sluice.addresses": ("93.184.216.34",)
    }


def test_crawl4ai_raw_source_accepts_exact_body_field(monkeypatch) -> None:
    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["10.0.0.8"])
    feed = '<feed xmlns="http://www.w3.org/2005/Atom"><title>empty</title></feed>'
    service = _FakeService(
        {
            "success": True,
            "results": [
                {
                    "success": True,
                    "url": "https://example.test/article",
                    "body": feed,
                    "cleaned_html": "<html>normalized</html>",
                }
            ],
        }
    )
    binding = ProviderBinding(
        name="browser",
        type="crawl4ai",
        base_url="http://crawl4ai.internal:11235",
        capabilities=frozenset(
            {"raw_html", "raw_source", "markdown", "rendered_js"}
        ),
    )
    request = FetchRequest(
        url="https://example.test/article",
        mode="raw_source",
        acceptable=frozenset({"raw_source"}),
        preferred=("raw_source",),
        quality=QualityPolicy(min_chars=1),
    )

    artifact = Crawl4AIProvider(binding, service).fetch(request)

    assert artifact.raw_source == feed.encode()
    assert artifact.capabilities == frozenset({"raw_source"})


@pytest.mark.parametrize(
    "returned_source",
    [
        {"cleaned_html": '<feed xmlns="http://www.w3.org/2005/Atom" />'},
        {
            "body": {
                "value": '<feed xmlns="http://www.w3.org/2005/Atom" />'
            }
        },
    ],
    ids=("cleaned-html", "normalized-body-object"),
)
def test_crawl4ai_raw_source_never_uses_normalized_html(
    monkeypatch, returned_source: dict[str, object]
) -> None:
    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["10.0.0.8"])
    service = _FakeService(
        {
            "success": True,
            "results": [
                {
                    "success": True,
                    "url": "https://example.test/article",
                    **returned_source,
                }
            ],
        }
    )
    binding = ProviderBinding(
        name="browser",
        type="crawl4ai",
        base_url="http://crawl4ai.internal:11235",
        capabilities=frozenset(
            {"raw_html", "raw_source", "markdown", "rendered_js"}
        ),
    )
    request = FetchRequest(
        url="https://example.test/article",
        mode="raw_source",
        acceptable=frozenset({"raw_source"}),
        preferred=("raw_source",),
        quality=QualityPolicy(min_chars=1),
    )

    artifact = Crawl4AIProvider(binding, service).fetch(request)

    assert artifact.raw_source is None
    assert artifact.capabilities == frozenset()


def test_present_non_string_reported_final_url_fails(monkeypatch) -> None:
    monkeypatch.setattr(
        "sluice.web.url.resolve_host", lambda host: ["93.184.216.34"]
    )
    service = _FakeService(
        {
            "success": True,
            "data": {
                "rawHtml": "<html>body</html>",
                "metadata": {"sourceURL": 42},
            },
        }
    )
    binding = ProviderBinding(
        name="fire",
        type="firecrawl",
        base_url="https://api.firecrawl.dev",
    )

    with pytest.raises(WebSafetyError, match="provider_error"):
        FirecrawlProvider(binding, service).fetch(_request())


def test_present_malformed_crawl4ai_redirected_url_fails(monkeypatch) -> None:
    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["10.0.0.8"])
    service = _FakeService(
        {
            "success": True,
            "results": [
                {
                    "success": True,
                    "url": "https://example.test/article",
                    "redirected_url": "file:///etc/passwd",
                    "html": "<html>body</html>",
                }
            ],
        }
    )
    binding = ProviderBinding(
        name="browser",
        type="crawl4ai",
        base_url="http://crawl4ai.internal:11235",
    )

    with pytest.raises(WebSafetyError):
        Crawl4AIProvider(binding, service).fetch(_request())


def test_firecrawl_rejects_v1_before_sending_a_request() -> None:
    service = _FakeService({"success": True, "data": {}})
    binding = ProviderBinding(
        name="fire",
        type="firecrawl",
        base_url="https://api.firecrawl.dev",
        options={"api_version": "v1"},
    )

    with pytest.raises(DefinitionError, match="v2"):
        FirecrawlProvider(binding, service)
    assert service.requests == []


def test_trafilatura_provider_returns_raw_html_and_provider_original_markdown(monkeypatch) -> None:
    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["93.184.216.34"])
    monkeypatch.setattr(
        "sluice.web.providers.importlib.import_module",
        lambda name: type(
            "TrafilaturaModule",
            (),
            {"extract": staticmethod(lambda *args, **kwargs: "body")},
        )()
        if name == "trafilatura"
        else __import__(name),
    )
    provider = build_provider(
        ProviderBinding(
            name="article",
            type="trafilatura",
            capabilities=frozenset({"raw_html", "text", "markdown"}),
        ),
        _runtime(_FakeTransport("<article>body</article>")),
    )
    artifact = provider.fetch(_request())
    assert artifact.raw_html == "<article>body</article>"
    assert artifact.markdown == "body"
    assert artifact.provider_instance == "article"


def test_trafilatura_provider_missing_dependency_fails_startup_with_instance(
    monkeypatch,
) -> None:
    original_import = __import__("importlib").import_module

    def missing(name: str, *args: object, **kwargs: object):
        if name == "trafilatura":
            raise ImportError("optional trafilatura is unavailable")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr("sluice.web.providers.importlib.import_module", missing)
    with pytest.raises(DefinitionError, match="article"):
        build_provider(
            ProviderBinding(
                name="article",
                type="trafilatura",
                capabilities=frozenset({"raw_html", "text", "markdown"}),
            ),
            _runtime(_FakeTransport("<article>fallback text</article>")),
        )


def test_trafilatura_extraction_failure_returns_no_match(monkeypatch) -> None:
    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["93.184.216.34"])

    class BrokenTrafilatura:
        @staticmethod
        def extract(*args: object, **kwargs: object):
            raise RuntimeError("extractor failed")

    monkeypatch.setattr(
        "sluice.web.providers.importlib.import_module",
        lambda name: BrokenTrafilatura() if name == "trafilatura" else __import__(name),
    )
    provider = build_provider(
        ProviderBinding(
            name="article",
            type="trafilatura",
            capabilities=frozenset({"raw_html", "text", "markdown"}),
        ),
        _runtime(_FakeTransport("<article>fallback text</article>")),
    )
    assert provider.fetch(_request()) is NO_MATCH


def test_catalog_httpx_uses_pinned_stream_and_decompressed_cap(monkeypatch) -> None:
    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["93.184.216.34"])

    class Stream:
        def __init__(self) -> None:
            self.closed = False

        def get_extra_info(self, name: str):
            return ("93.184.216.34", 443) if name == "server_addr" else None

    class Response:
        status_code = 200
        headers = {"content-type": "text/plain"}
        url = "https://example.test/article"

        def __init__(self) -> None:
            self.extensions = {"network_stream": Stream()}
            self.closed = False

        def iter_bytes(self):
            yield b"012"
            yield b"345"

        def close(self) -> None:
            self.closed = True

    created: list[object] = []

    class Client:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs
            self.get_calls = 0
            self.send_calls = 0
            self.request_extensions: dict[str, object] | None = None
            created.append(self)

        def get(self, url: str, **kwargs: object):
            self.get_calls += 1
            raise AssertionError("catalog must not use buffering Client.get")

        def send(self, request: object, *, stream: bool = False):
            self.send_calls += 1
            self.request_extensions = dict(getattr(request, "extensions", {}))
            assert stream is True
            return Response()

        def close(self) -> None:
            pass

    monkeypatch.setattr("httpx.Client", Client)
    binding = ProviderBinding(
        name="direct",
        type="httpx",
        capabilities=frozenset({"text"}),
        max_body_bytes=5,
    )
    catalog = WebRuntimeCatalog((binding,))
    try:
        with pytest.raises(WebSafetyError, match="body_too_large"):
            catalog.provider(binding).fetch(_request(acceptable=frozenset({"text"})))
        client = created[0]
        assert client.get_calls == 0
        assert client.send_calls == 1
        assert client.request_extensions == {"sluice.addresses": ("93.184.216.34",)}
        assert "transport" in client.kwargs
    finally:
        catalog.close()


@pytest.mark.parametrize("provider_type", ["httpx", "trafilatura"])
def test_httpx_profiles_require_remote_dns_proxy_opt_in(
    monkeypatch, provider_type: str
) -> None:
    if provider_type == "trafilatura":
        monkeypatch.setattr(
            "sluice.web.providers.importlib.import_module",
            lambda name: type(
                "TrafilaturaModule",
                (),
                {"extract": staticmethod(lambda *args, **kwargs: "body")},
            )()
            if name == "trafilatura"
            else __import__(name),
        )
    binding = ProviderBinding(
        name=f"{provider_type}-direct",
        type=provider_type,
        capabilities=(
            frozenset({"text"})
            if provider_type == "httpx"
            else frozenset({"raw_html", "text", "markdown"})
        ),
        proxy="http://proxy.example:8080",
    )
    with pytest.raises(DefinitionError, match="allow_remote_dns_proxy"):
        WebRuntimeCatalog((binding,))


@pytest.mark.parametrize("provider_type", ["httpx", "trafilatura"])
def test_httpx_profiles_use_resolved_proxy_but_guard_every_redirect_locally(
    monkeypatch, caplog, provider_type: str
) -> None:
    secret = "credential-that-must-not-be-logged"
    resolved_proxy = f"http://proxy-user:{secret}@proxy.example:8080"
    monkeypatch.setenv("DIRECT_PROXY", resolved_proxy)
    if provider_type == "trafilatura":
        monkeypatch.setattr(
            "sluice.web.providers.importlib.import_module",
            lambda name: type(
                "TrafilaturaModule",
                (),
                {"extract": staticmethod(lambda *args, **kwargs: "body")},
            )()
            if name == "trafilatura"
            else __import__(name),
        )

    requests: list[object] = []
    created: list[object] = []

    class Stream:
        def get_extra_info(self, name: str):
            return ("10.0.0.8", 8080) if name == "server_addr" else None

    class Response:
        status_code = 302
        headers = {"location": "http://127.0.0.1/metadata"}

        def __init__(self, request: object) -> None:
            self.url = getattr(request, "url")
            self.extensions = {"network_stream": Stream()}

        def iter_bytes(self):
            yield b""

        def close(self) -> None:
            pass

    class Client:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs
            created.append(self)

        def send(self, request: object, *, stream: bool = False):
            assert stream is True
            requests.append(request)
            return Response(request)

        def close(self) -> None:
            pass

    monkeypatch.setattr("httpx.Client", Client)
    resolutions: list[str] = []

    def resolve(host: str) -> list[str]:
        resolutions.append(host)
        return ["93.184.216.34"]

    monkeypatch.setattr("sluice.web.url.resolve_host", resolve)
    binding = ProviderBinding(
        name=f"{provider_type}-remote-proxy",
        type=provider_type,
        capabilities=(
            frozenset({"text"})
            if provider_type == "httpx"
            else frozenset({"raw_html", "text", "markdown"})
        ),
        proxy="env:DIRECT_PROXY",
        target_policy={"allow_remote_dns_proxy": True},
    )

    with caplog.at_level(logging.WARNING, logger="sluice.web.registry"):
        catalog = WebRuntimeCatalog((binding,))
    try:
        with pytest.raises(WebSafetyError, match="ssrf_blocked"):
            catalog.provider(binding).fetch(
                _request(acceptable=frozenset({"text"}))
            )

        assert resolutions == ["example.test"]
        assert len(requests) == 1
        assert str(getattr(requests[0], "url")) == "https://example.test/article"
        assert "sluice.addresses" not in getattr(requests[0], "extensions")
        assert created[0].kwargs["proxy"] == resolved_proxy
        assert any(
            provider_type in warning
            and "pinning cannot be verified" in warning
            for warning in catalog.warnings
        )
        persisted = str(WebBinding(fetch_chain=(binding,)).semantic_projection())
        assert "env:DIRECT_PROXY" in persisted
        assert secret not in persisted
        assert secret not in str(catalog.warnings)
        assert secret not in caplog.text
    finally:
        catalog.close()


def test_httpx_socks5_proxy_keeps_approved_target_address_and_original_identity(
    monkeypatch,
) -> None:
    requests: list[object] = []
    created: list[object] = []

    class Stream:
        def get_extra_info(self, name: str):
            return ("10.0.0.8", 1080) if name == "server_addr" else None

    class Response:
        status_code = 200
        headers = {"content-type": "text/plain"}

        def __init__(self, request: object) -> None:
            self.url = getattr(request, "url")
            self.extensions = {"network_stream": Stream()}

        def iter_bytes(self):
            yield b"ok"

        def close(self) -> None:
            pass

    class Client:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs
            created.append(self)

        def send(self, request: object, *, stream: bool = False):
            assert stream is True
            requests.append(request)
            if len(requests) == 1:
                raise OSError("first approved target address is unavailable")
            return Response(request)

        def close(self) -> None:
            pass

    monkeypatch.setattr("httpx.Client", Client)
    monkeypatch.setattr(
        "sluice.web.url.resolve_host",
        lambda host: ["93.184.216.34", "93.184.216.35"],
    )
    binding = ProviderBinding(
        name="httpx-local-proxy",
        type="httpx",
        capabilities=frozenset({"text"}),
        proxy="socks5://proxy.example:1080",
    )

    catalog = WebRuntimeCatalog((binding,))
    try:
        artifact = catalog.provider(binding).fetch(
            _request(acceptable=frozenset({"text"}))
        )

        assert artifact.text == "ok"
        assert artifact.final_url == "https://example.test/article"
        assert created[0].kwargs["proxy"] == "socks5://proxy.example:1080"
        assert [str(getattr(sent, "url")) for sent in requests] == [
            "https://93.184.216.34/article",
            "https://93.184.216.35/article",
        ]
        sent = requests[-1]
        assert getattr(sent, "headers")["host"] == "example.test"
        assert getattr(sent, "extensions")["sni_hostname"] == "example.test"
        assert not catalog.warnings
    finally:
        catalog.close()


def test_startup_http_requires_trust_and_has_no_peer_claim() -> None:
    with pytest.raises(DefinitionError, match="trusted"):
        build_provider(
            ProviderBinding(name="compat", type="startup_http"),
            _runtime(_FakeTransport("ok")),
        )
    provider = build_provider(
        ProviderBinding(
            name="compat",
            type="startup_http",
            options={"trusted": True},
            capabilities=frozenset({"raw_html", "text", "raw_source"}),
        ),
        _runtime(_FakeTransport("ok")),
    )
    artifact = provider.fetch(_request(acceptable=frozenset({"text"})))
    assert artifact.status_code == 200
    assert artifact.final_url == "https://example.test/article"
    assert artifact.metadata.get("peer") is None


def test_unknown_provider_type_is_rejected_by_fixed_map() -> None:
    with pytest.raises(DefinitionError, match="unsupported"):
        build_provider(ProviderBinding(name="bad", type="not_a_provider"), _runtime(None))


def test_catalog_creates_one_direct_httpx_client_and_closes_it(monkeypatch) -> None:
    created: list[object] = []

    class Client:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs
            self.close_calls = 0
            self.get_calls = 0
            self.send_calls = 0
            created.append(self)

        def get(self, url: str, **kwargs: object) -> DirectResponse:
            self.get_calls += 1
            raise AssertionError("catalog must use streamed send")

        class Response:
            status_code = 200
            headers = {"content-type": "text/plain"}
            url = "https://example.test/article"
            extensions: dict[str, object] = {}

            def iter_bytes(self):
                yield b"ok"

            def close(self) -> None:
                pass

        def send(self, request: object, *, stream: bool = False):
            self.send_calls += 1
            assert stream is True
            return self.Response()

        def close(self) -> None:
            self.close_calls += 1

    monkeypatch.setattr("httpx.Client", Client)
    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["93.184.216.34"])
    binding = ProviderBinding(
        name="direct",
        type="httpx",
        capabilities=frozenset({"text"}),
    )
    catalog = WebRuntimeCatalog((binding,))
    try:
        assert len(created) == 1
        provider = catalog.provider(binding)
        provider.fetch(_request(acceptable=frozenset({"text"})))
        provider.fetch(_request(acceptable=frozenset({"text"})))
        assert created[0].send_calls == 2
    finally:
        catalog.close()
    assert created[0].close_calls == 1


def test_catalog_closes_new_owned_transport_when_close_wins_creation_race(
    monkeypatch,
) -> None:
    constructing = Event()
    release = Event()
    created: list[object] = []

    class BlockingTransport:
        def __init__(self, **kwargs: object) -> None:
            self.close_calls = 0
            created.append(self)
            constructing.set()
            assert release.wait(2)

        def close(self) -> None:
            self.close_calls += 1

    monkeypatch.setattr(
        registry_module, "_PooledPinnedHTTPXClient", BlockingTransport
    )
    catalog = WebRuntimeCatalog()
    binding = ProviderBinding(name="direct", type="httpx")
    failures: list[BaseException] = []

    def create_transport() -> None:
        try:
            catalog.transport_for(binding)
        except BaseException as error:
            failures.append(error)

    worker = Thread(target=create_transport)
    worker.start()
    assert constructing.wait(2)
    catalog.close()
    release.set()
    worker.join(2)

    assert not worker.is_alive()
    assert len(failures) == 1
    assert isinstance(failures[0], DefinitionError)
    assert str(failures[0]) == "web runtime catalog is closed"
    assert len(created) == 1
    assert created[0].close_calls == 1


def test_catalog_close_logs_only_a_safe_fixed_code(monkeypatch) -> None:
    class RaisingTransport:
        def close(self) -> None:
            raise RuntimeError("Authorization bearer private-close-secret")

    class RecordingLogger:
        def __init__(self) -> None:
            self.debug_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

        def debug(self, *args: object, **kwargs: object) -> None:
            self.debug_calls.append((args, kwargs))

    monkeypatch.setattr(
        registry_module,
        "_PooledPinnedHTTPXClient",
        lambda **kwargs: RaisingTransport(),
    )
    logger = RecordingLogger()
    catalog = WebRuntimeCatalog(
        (ProviderBinding(name="direct", type="httpx"),),
        logger=logger,
    )

    catalog.close()

    assert logger.debug_calls == [(("web_runtime_close_failed",), {})]


def test_resolve_provider_binding_only_expands_secret_capable_schema_fields(
    monkeypatch,
) -> None:
    monkeypatch.setenv("API_SECRET", "resolved-api-secret")
    monkeypatch.setenv("HEADER_SECRET", "resolved-header-secret")
    monkeypatch.setenv("ACTION_LITERAL", "must-not-be-expanded")
    monkeypatch.delenv("ORDINARY_LITERAL", raising=False)
    action = {"write": {"text": "env:ACTION_LITERAL"}}
    binding = ProviderBinding(
        name="cloud",
        type="firecrawl",
        options={
            "api_key": "env:API_SECRET",
            "ordinary_text": "env:ORDINARY_LITERAL",
            "actions": (action,),
        },
        service_headers={"X-Token": "env:HEADER_SECRET"},
        wait_for="env:ORDINARY_LITERAL",
        actions=(action,),
    )

    resolved = resolve_provider_binding(binding)

    assert resolved.options["api_key"] == "resolved-api-secret"
    assert resolved.service_headers["X-Token"] == "resolved-header-secret"
    assert resolved.options["ordinary_text"] == "env:ORDINARY_LITERAL"
    assert resolved.options["actions"][0]["write"]["text"] == "env:ACTION_LITERAL"
    assert resolved.wait_for == "env:ORDINARY_LITERAL"
    assert resolved.actions[0]["write"]["text"] == "env:ACTION_LITERAL"


def test_catalog_built_extractor_options_are_deeply_immutable() -> None:
    authored_options = {
        "mode": "article",
        "nested": {"selectors": ["article", "main"]},
    }
    binding = ExtractorBinding(
        name="local-article",
        type="beautifulsoup",
        options=authored_options,
    )
    catalog = WebRuntimeCatalog()
    try:
        extractor = catalog.extractor(binding)
        with pytest.raises(TypeError):
            extractor.options["mode"] = "full"
        with pytest.raises(TypeError):
            extractor.options["nested"]["selectors"] = ("body",)
        assert extractor.options["nested"]["selectors"] == ("article", "main")
        assert authored_options == {
            "mode": "article",
            "nested": {"selectors": ["article", "main"]},
        }
    finally:
        catalog.close()


def test_catalog_curl_retains_per_worker_sessions_and_uses_pinned_callback_stream(
    monkeypatch,
) -> None:
    created: list[object] = []

    class CurlOpt:
        RESOLVE = 1
        FRESH_CONNECT = 2
        FORBID_REUSE = 3

    class Response:
        status_code = 200
        headers = {"content-type": "text/plain"}
        primary_ip = "93.184.216.34"

        def __init__(self, url: str) -> None:
            self.url = url

    class Session:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs
            self.curl_options = dict(kwargs.get("curl_options", {}))
            self.close_calls = 0
            self.get_calls = 0
            self.request_calls = 0
            created.append(self)

        def get(self, url: str, **kwargs: object) -> DirectResponse:
            self.get_calls += 1
            raise AssertionError("curl provider must not use eager Session.get")

        def request(self, method: str, url: str, **kwargs: object) -> Response:
            self.request_calls += 1
            assert method == "GET"
            assert kwargs["allow_redirects"] is False
            assert kwargs["accept_encoding"] == "gzip, deflate, br"
            assert kwargs.get("stream") is not True
            callback = kwargs["content_callback"]
            assert callable(callback)
            callback(b"ok")
            assert self.curl_options == {
                CurlOpt.RESOLVE: [
                    "example.test:443:93.184.216.34,[2606:2800:220:1:248:1893:25c8:1946]"
                ],
                CurlOpt.FRESH_CONNECT: 1,
                CurlOpt.FORBID_REUSE: 1,
            }
            return Response(url)

        def close(self) -> None:
            self.close_calls += 1

    requests_module = SimpleNamespace(Session=Session)
    curl_module = SimpleNamespace(CurlOpt=CurlOpt)
    original_import = __import__("importlib").import_module

    def fake_import(name: str, *args: object, **kwargs: object):
        if name == "curl_cffi.requests":
            return requests_module
        if name == "curl_cffi":
            return curl_module
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr("importlib.import_module", fake_import)
    monkeypatch.setattr(
        "sluice.web.url.resolve_host",
        lambda host: [
            "93.184.216.34",
            "2606:2800:220:1:248:1893:25c8:1946",
        ],
    )
    binding = ProviderBinding(
        name="browser",
        type="curl_cffi",
        capabilities=frozenset({"text"}),
    )
    catalog = WebRuntimeCatalog((binding,))
    try:
        assert len(created) == 1
        provider = catalog.provider(binding)
        provider.fetch(_request(acceptable=frozenset({"text"})))
        provider.fetch(_request(acceptable=frozenset({"text"})))
        assert created[0].request_calls == 2
        failures: list[BaseException] = []

        def fetch_twice() -> None:
            try:
                provider.fetch(_request(acceptable=frozenset({"text"})))
                provider.fetch(_request(acceptable=frozenset({"text"})))
            except BaseException as error:
                failures.append(error)

        worker = Thread(target=fetch_twice)
        worker.start()
        worker.join(2)
        assert not worker.is_alive()
        assert failures == []
        assert len(created) == 2
        assert created[1].request_calls == 2
        assert all(session.get_calls == 0 for session in created)
        assert all(session.kwargs["trust_env"] is False for session in created)
    finally:
        catalog.close()
    assert [session.close_calls for session in created] == [1, 1]


def test_catalog_curl_caps_decompressed_callback_body(monkeypatch) -> None:
    class CurlOpt:
        RESOLVE = 1
        FRESH_CONNECT = 2
        FORBID_REUSE = 3

    class Session:
        def __init__(self, **kwargs: object) -> None:
            self.curl_options = dict(kwargs.get("curl_options", {}))

        def request(self, method: str, url: str, **kwargs: object) -> object:
            callback = kwargs["content_callback"]
            callback(b"012")
            try:
                callback(b"345")
            except Exception:
                # curl_cffi may surface a callback abort as its own write
                # error instead of propagating the callback exception.
                raise RuntimeError("curl write callback aborted") from None
            raise AssertionError("body cap must abort curl before completion")

        def close(self) -> None:
            pass

    original_import = __import__("importlib").import_module

    def fake_import(name: str, *args: object, **kwargs: object):
        if name == "curl_cffi.requests":
            return SimpleNamespace(Session=Session)
        if name == "curl_cffi":
            return SimpleNamespace(CurlOpt=CurlOpt)
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr("importlib.import_module", fake_import)
    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["93.184.216.34"])
    binding = ProviderBinding(
        name="browser",
        type="curl_cffi",
        capabilities=frozenset({"text"}),
        max_body_bytes=5,
    )
    catalog = WebRuntimeCatalog((binding,))
    try:
        with pytest.raises(WebSafetyError, match="body_too_large"):
            catalog.provider(binding).fetch(
                _request(acceptable=frozenset({"text"}))
            )
    finally:
        catalog.close()


def test_catalog_rejects_curl_runtime_without_safe_pinned_callback_api(
    monkeypatch,
) -> None:
    class CurlOpt:
        RESOLVE = 1
        FRESH_CONNECT = 2
        FORBID_REUSE = 3

    class UnsafeSession:
        def __init__(self, **kwargs: object) -> None:
            pass

        def get(self, url: str, **kwargs: object) -> object:
            return object()

        def close(self) -> None:
            pass

    original_import = __import__("importlib").import_module

    def fake_import(name: str, *args: object, **kwargs: object):
        if name == "curl_cffi.requests":
            return SimpleNamespace(Session=UnsafeSession)
        if name == "curl_cffi":
            return SimpleNamespace(CurlOpt=CurlOpt)
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr("importlib.import_module", fake_import)
    binding = ProviderBinding(
        name="unsafe-browser",
        type="curl_cffi",
        capabilities=frozenset({"text"}),
    )
    with pytest.raises(DefinitionError, match="unsafe-browser"):
        WebRuntimeCatalog((binding,))


def test_curl_remote_dns_proxy_requires_opt_in_and_records_unpinned_warning(
    monkeypatch,
) -> None:
    class CurlOpt:
        RESOLVE = 1
        FRESH_CONNECT = 2
        FORBID_REUSE = 3

    requests: list[dict[str, object]] = []
    sessions: list[object] = []

    class Session:
        def __init__(self, **kwargs: object) -> None:
            self.curl_options = dict(kwargs.get("curl_options", {}))
            sessions.append(self)

        def request(self, method: str, url: str, **kwargs: object) -> object:
            requests.append(dict(kwargs))
            callback = kwargs["content_callback"]
            callback(b"ok")
            return SimpleNamespace(
                status_code=200,
                headers={"content-type": "text/plain"},
                url=url,
                primary_ip="10.0.0.8",
            )

        def close(self) -> None:
            pass

    original_import = __import__("importlib").import_module

    def fake_import(name: str, *args: object, **kwargs: object):
        if name == "curl_cffi.requests":
            return SimpleNamespace(Session=Session)
        if name == "curl_cffi":
            return SimpleNamespace(CurlOpt=CurlOpt)
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr("importlib.import_module", fake_import)
    resolutions: list[str] = []

    def resolve(host: str) -> list[str]:
        resolutions.append(host)
        return ["93.184.216.34"]

    monkeypatch.setattr("sluice.web.url.resolve_host", resolve)
    denied = ProviderBinding(
        name="remote-proxy-denied",
        type="curl_cffi",
        capabilities=frozenset({"text"}),
        proxy="http://proxy.example:8080",
    )
    with pytest.raises(DefinitionError, match="allow_remote_dns_proxy"):
        WebRuntimeCatalog((denied,))

    allowed = ProviderBinding(
        name="remote-proxy",
        type="curl_cffi",
        capabilities=frozenset({"text"}),
        proxy="http://proxy.example:8080",
        target_policy={"allow_remote_dns_proxy": True},
    )
    catalog = WebRuntimeCatalog((allowed,))
    try:
        artifact = catalog.provider(allowed).fetch(
            _request(acceptable=frozenset({"text"}))
        )
        assert artifact.text == "ok"
        assert resolutions == ["example.test"]
        assert requests[0]["proxy"] == "http://proxy.example:8080"
        assert requests[0]["allow_redirects"] is False
        assert CurlOpt.RESOLVE not in sessions[0].curl_options
        assert any(
            "remote-proxy" in warning and "pinning cannot be verified" in warning
            for warning in catalog.warnings
        )
    finally:
        catalog.close()


@pytest.mark.parametrize(
    ("extractor_type", "module_attribute", "instance_name"),
    [
        ("trafilatura", "trafilatura", "article-extractor"),
        ("newspaper4k", "newspaper", "news-extractor"),
        ("resiliparse", "resiliparse", "fast-extractor"),
    ],
)
def test_catalog_rejects_missing_configured_optional_extractor_by_instance(
    monkeypatch,
    extractor_type: str,
    module_attribute: str,
    instance_name: str,
) -> None:
    monkeypatch.setattr(extractor_module, module_attribute, None)
    catalog = WebRuntimeCatalog()
    try:
        with pytest.raises(DefinitionError, match=instance_name):
            catalog.extractor(
                ExtractorBinding(name=instance_name, type=extractor_type)
            )
    finally:
        catalog.close()


def _raw_html_artifact() -> DocumentArtifact:
    return DocumentArtifact(
        raw_html="<article>body</article>",
        source_url="https://example.test/article",
        final_url="https://example.test/article",
        content_type="text/html",
        capabilities=frozenset({"raw_html"}),
    )


def test_optional_extractors_return_no_match_after_validated_runtime_failure(
    monkeypatch,
) -> None:
    class BrokenTrafilatura:
        @staticmethod
        def extract(*args: object, **kwargs: object) -> object:
            raise RuntimeError("must remain private")

    class BrokenArticle:
        def __init__(self, *args: object, **kwargs: object) -> None:
            self.config = SimpleNamespace()

        def download(self, **kwargs: object) -> None:
            pass

        def parse(self) -> None:
            raise RuntimeError("must remain private")

    class BrokenResiliparse:
        extract = SimpleNamespace(
            html2text=SimpleNamespace(
                extract_plain_text=staticmethod(
                    lambda *args, **kwargs: (_ for _ in ()).throw(
                        RuntimeError("must remain private")
                    )
                )
            )
        )

    monkeypatch.setattr(extractor_module, "trafilatura", BrokenTrafilatura())
    monkeypatch.setattr(
        extractor_module,
        "newspaper",
        SimpleNamespace(article=SimpleNamespace(Article=BrokenArticle)),
    )
    monkeypatch.setattr(extractor_module, "resiliparse", BrokenResiliparse())
    catalog = WebRuntimeCatalog()
    try:
        for name, extractor_type in (
            ("article", "trafilatura"),
            ("news", "newspaper4k"),
            ("fast", "resiliparse"),
        ):
            extractor = catalog.extractor(
                ExtractorBinding(name=name, type=extractor_type)
            )
            assert extractor.extract(_raw_html_artifact()) is NO_MATCH
    finally:
        catalog.close()


def test_invalid_reported_final_url_fails_instead_of_using_request_url(monkeypatch) -> None:
    monkeypatch.setattr(
        "sluice.web.providers.bounded_manual_get",
        lambda *args, **kwargs: DirectResponse(
            status_code=200,
            headers={"content-type": "text/plain"},
            body=b"ok",
            url="https://example.test/article",
            final_url="file:///etc/passwd",
        ),
    )
    provider = build_provider(
        ProviderBinding(name="direct", type="httpx", capabilities=frozenset({"text"})),
        _runtime(_FakeTransport("unused")),
    )
    with pytest.raises(WebSafetyError) as error:
        provider.fetch(_request(acceptable=frozenset({"text"})))
    assert getattr(error.value, "reason", None) in {"bad_url", "ssrf_blocked", "provider_error"}


def test_startup_http_does_not_hide_invalid_reported_final_url() -> None:
    class InvalidFinalURL:
        def get(self, url: str, **kwargs: object) -> DirectResponse:
            return DirectResponse(
                status_code=200,
                headers={"content-type": "text/plain"},
                body=b"ok",
                url=url,
                final_url="file:///etc/passwd",
            )

    provider = build_provider(
        ProviderBinding(
            name="compat",
            type="startup_http",
            options={"trusted": True},
            capabilities=frozenset({"text"}),
        ),
        _runtime(InvalidFinalURL()),
    )
    with pytest.raises(WebSafetyError):
        provider.fetch(_request(acceptable=frozenset({"text"})))
