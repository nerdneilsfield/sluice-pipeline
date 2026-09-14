"""Sequential WebContentClient fallback coordination tests."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

import pytest

from sluice.app import open_app
from sluice.contracts import SourceRecord, SourceResult
from sluice.errors import ExecutionError, WebSafetyError
from sluice.reconcile import tick
from sluice.source_runtime import collect_source
from sluice.web.client import WebContentClient
from sluice.web.contracts import (
    DocumentArtifact,
    ExtractorBinding,
    FetchChainExhausted,
    FetchRequest,
    ProviderBinding,
    QualityPolicy,
    WebAttemptCancelled,
    WebExecutionProbe,
    WebStageDeadlineExpired,
    reset_web_execution_probe,
    set_web_execution_probe,
)
from sluice.web.providers import FirecrawlProvider
from sluice.web.registry import ProviderLimiter


def _pipeline_source(config, checkpoint):
    del config, checkpoint
    return SourceResult(
        (SourceRecord("article", {"url": "https://example.test/article"}),), "done"
    )


def _pipeline_sink(config, payload, delivery_key):
    del config, payload, delivery_key


def test_fetch_extract_project_chain_materializes_json_not_artifact(
    tmp_path: Path,
) -> None:
    definition = tmp_path / "definition.yml"
    definition.write_text(
        """version: 1
web:
  providers: {direct: {type: startup_http, trusted: true}}
  fetch_chains: {robust: {providers: [direct]}}
  extractors: {identity: {type: identity}}
  extract_chains: {article: {extractors: [identity]}}
sources:
  feed: {use: tests.integration.test_web_pipeline._pipeline_source, with: {}}
stages:
  article:
    input: each
    tasks:
      - use: sluice.tasks.fetch_webpage
        with: {fetch_chain: robust, extract_chain: article, min_chars: 1}
      - use: sluice.tasks.to_text
pipelines:
  p:
    nodes:
      article: {stage: article, from: source.feed}
      deliver:
        from: article
        deliver: {use: tests.integration.test_web_pipeline._pipeline_sink, with: {}}
""",
        encoding="utf-8",
    )

    class Client:
        def resolve(self, request, fetch_chain, extract_chain):
            assert request.url == "https://example.test/article"
            assert fetch_chain[0].name == "direct"
            assert extract_chain[0].type == "identity"
            return DocumentArtifact(
                source_url=request.url,
                final_url=request.url,
                content_type="text/markdown",
                markdown="# Article\n\nBody",
                selected_format="markdown",
                capabilities=frozenset({"markdown"}),
                provider_instance="direct",
                provider_type="startup_http",
                extraction="identity",
            )

        def close(self):
            pass

    app = open_app(
        definition,
        tmp_path / "state.db",
        http_client=type("HTTP", (), {"get": lambda self, url: "unused"})(),
        web_content_client=Client(),
    )
    try:
        collect_source(app, "feed", datetime(2026, 1, 1, tzinfo=UTC))
        tick(app, datetime(2026, 1, 1, tzinfo=UTC))
        payload, metadata = app.conn.execute(
            "SELECT payload_json,metadata_json FROM items WHERE producer_kind='stage'"
        ).fetchone()
    finally:
        app.close()

    assert "DocumentArtifact" not in payload
    assert '"web"' in metadata


def _provider(name: str, *capabilities: str) -> ProviderBinding:
    return ProviderBinding(
        name=name,
        type="startup_http",
        options={"trusted": True},
        capabilities=frozenset(capabilities),
    )


def _article_request() -> FetchRequest:
    return FetchRequest(
        url="https://example.test/article",
        mode="article",
        acceptable=frozenset({"raw_html", "markdown", "text"}),
        preferred=("markdown", "raw_html", "text"),
        quality=QualityPolicy(min_chars=1),
    )


def _list_request(selector: str) -> FetchRequest:
    return FetchRequest(
        url="https://example.test/releases",
        mode="list_html",
        acceptable=frozenset({"raw_html"}),
        preferred=("raw_html",),
        list_selector=selector,
        quality=QualityPolicy(min_chars=1),
    )


def _raw_source_request() -> FetchRequest:
    return FetchRequest(
        url="https://example.test/feed.xml",
        mode="raw_source",
        acceptable=frozenset({"raw_source"}),
        preferred=("raw_source",),
        quality=QualityPolicy(min_chars=1),
    )


class _RemoteService:
    def __init__(self, payload: object) -> None:
        self.payload = payload
        self.paths: list[str] = []

    def post(self, path: str, **kwargs: object) -> object:
        del kwargs
        self.paths.append(urlsplit(path).path if "://" in path else path)
        return self.payload


class _ProviderCatalog:
    def __init__(self, provider: object) -> None:
        self._provider = provider

    def provider(self, binding: object) -> object:
        del binding
        return self._provider


@dataclass
class _FakeLimiter:
    acquire_calls: int = 0
    release_calls: int = 0

    def acquire(self, timeout: float | None = None) -> bool:
        del timeout
        self.acquire_calls += 1
        return True

    def release(self) -> None:
        self.release_calls += 1


class _FakeCatalog:
    def __init__(self, artifacts: dict[str, DocumentArtifact]) -> None:
        self.artifacts = artifacts
        self.calls: list[str] = []
        self.extractor_calls: list[str] = []
        self.limiters = {name: _FakeLimiter() for name in artifacts}

    def provider(self, binding: ProviderBinding):
        catalog = self

        class Provider:
            def fetch(self, request: FetchRequest) -> DocumentArtifact:
                del request
                catalog.calls.append(binding.name)
                artifact = catalog.artifacts[binding.name]
                if isinstance(artifact, BaseException):
                    raise artifact
                return artifact

        return Provider()

    def limiter(self, binding: ProviderBinding) -> _FakeLimiter:
        return self.limiters[binding.name]

    def extractor(self, binding: ExtractorBinding):
        from sluice.web.extractors import build_extractor

        catalog = self
        extractor = build_extractor(binding)

        class TrackingExtractor:
            def extract(self, artifact: DocumentArtifact):
                catalog.extractor_calls.append(binding.name)
                return extractor.extract(artifact)

        return TrackingExtractor()


def test_article_falls_through_challenge_to_second_provider_markdown() -> None:
    first = _provider("first", "raw_html", "markdown", "text")
    second = _provider("second", "raw_html", "markdown", "text")
    catalog = _FakeCatalog(
        {
            "first": DocumentArtifact(
                source_url="https://example.test/article",
                final_url="https://example.test/article",
                content_type="text/html",
                raw_html='<div id="challenge-running">verify you are human</div>',
                capabilities=frozenset({"raw_html"}),
                provider_instance="first",
                provider_type="startup_http",
            ),
            "second": DocumentArtifact(
                source_url="https://example.test/article",
                final_url="https://example.test/article",
                content_type="text/markdown",
                markdown="A sufficiently useful provider markdown body.",
                capabilities=frozenset({"markdown"}),
                provider_instance="second",
                provider_type="startup_http",
            ),
        }
    )
    client = WebContentClient(catalog)

    artifact = client.resolve(
        _article_request(),
        (first, second),
        (ExtractorBinding(name="identity", type="identity"),),
    )

    assert artifact.provider_instance == "second"
    assert artifact.selected_format == "markdown"
    assert catalog.calls == ["first", "second"]


def test_list_mode_uses_selector_inside_provider_loop() -> None:
    first = _provider("static", "raw_html")
    second = _provider("browser", "raw_html")
    catalog = _FakeCatalog(
        {
            "static": DocumentArtifact(
                source_url="https://example.test/releases",
                final_url="https://example.test/releases",
                content_type="text/html",
                raw_html=(
                    "<html><body><p>"
                    + ("no matching release " * 12)
                    + "</p></body></html>"
                ),
                capabilities=frozenset({"raw_html"}),
                provider_instance="static",
                provider_type="startup_http",
            ),
            "browser": DocumentArtifact(
                source_url="https://example.test/releases",
                final_url="https://example.test/releases",
                content_type="text/html",
                raw_html=(
                    '<article class="release"><a href="/new">'
                    + ("new release " * 12)
                    + "</a></article>"
                ),
                capabilities=frozenset({"raw_html"}),
                provider_instance="browser",
                provider_type="startup_http",
            ),
        }
    )
    client = WebContentClient(catalog)

    artifact = client.resolve(_list_request("article.release"), (first, second), None)

    assert artifact.provider_instance == "browser"
    assert catalog.calls == ["static", "browser"]


def test_raw_source_rejects_markdown_and_accepts_zero_entry_atom() -> None:
    first = _provider("reader", "markdown")
    second = _provider("feed", "raw_source")
    catalog = _FakeCatalog(
        {
            "reader": DocumentArtifact(
                source_url="https://example.test/feed.xml",
                final_url="https://example.test/feed.xml",
                content_type="text/markdown",
                markdown="# feed",
                capabilities=frozenset({"markdown"}),
                provider_instance="reader",
                provider_type="startup_http",
            ),
            "feed": DocumentArtifact(
                source_url="https://example.test/feed.xml",
                final_url="https://example.test/feed.xml",
                content_type="application/atom+xml",
                raw_source=b'<feed xmlns="http://www.w3.org/2005/Atom"><title>empty</title></feed>',
                capabilities=frozenset({"raw_source"}),
                provider_instance="feed",
                provider_type="startup_http",
            ),
        }
    )
    client = WebContentClient(catalog)

    artifact = client.resolve(_raw_source_request(), (first, second), None)

    assert artifact.provider_instance == "feed"
    assert artifact.raw_source is not None
    assert artifact.raw_source.startswith(b"<feed")
    assert catalog.calls == ["feed"]


def test_raw_source_rejects_non_generic_binary_mime_and_falls_back() -> None:
    first = _provider("image", "raw_source")
    second = _provider("feed", "raw_source")
    feed = b'<feed xmlns="http://www.w3.org/2005/Atom" />'
    catalog = _FakeCatalog(
        {
            "image": DocumentArtifact(
                final_url="https://example.test/feed.xml",
                content_type="image/png",
                raw_source=feed,
                capabilities=frozenset({"raw_source"}),
            ),
            "feed": DocumentArtifact(
                final_url="https://example.test/feed.xml",
                content_type="application/atom+xml",
                raw_source=feed,
                capabilities=frozenset({"raw_source"}),
            ),
        }
    )

    result = WebContentClient(catalog).resolve(
        _raw_source_request(), (first, second), None
    )

    assert result.provider_instance == "feed"
    assert catalog.calls == ["image", "feed"]
    assert [attempt.reason for attempt in result.attempts] == ["bad_content_type"]


def test_firecrawl_raw_source_is_conditional_on_feed_validation(monkeypatch) -> None:
    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["93.184.216.34"])
    binding = ProviderBinding(
        name="fire",
        type="firecrawl",
        base_url="https://api.firecrawl.dev",
        capabilities=frozenset({"raw_html", "raw_source", "markdown", "rendered_js"}),
    )
    service = _RemoteService(
        {
            "success": True,
            "data": {
                "rawHtml": '<feed xmlns="http://www.w3.org/2005/Atom"><title>empty</title></feed>'
            },
        }
    )
    provider = FirecrawlProvider(binding, service)

    result = WebContentClient(_ProviderCatalog(provider)).resolve(
        _raw_source_request(), (binding,), None
    )

    assert result.raw_source is not None
    assert result.capabilities == frozenset({"raw_source"})
    assert service.paths == ["/v2/scrape"]


def test_firecrawl_raw_source_rejects_article_html_and_exhausts_chain(
    monkeypatch,
) -> None:
    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["93.184.216.34"])
    binding = ProviderBinding(
        name="fire",
        type="firecrawl",
        base_url="https://api.firecrawl.dev",
        capabilities=frozenset({"raw_html", "raw_source", "markdown", "rendered_js"}),
    )
    service = _RemoteService(
        {
            "success": True,
            "data": {"rawHtml": "<html><body>not a feed</body></html>"},
        }
    )
    provider = FirecrawlProvider(binding, service)

    with pytest.raises(ExecutionError, match="provider chain exhausted"):
        WebContentClient(_ProviderCatalog(provider)).resolve(
            _raw_source_request(), (binding,), None
        )


@pytest.mark.parametrize(
    "content_type",
    [
        "application/rss+xml",
        "text/plain; charset=utf-8",
        "application/octet-stream",
        None,
        "not a mime",
    ],
)
def test_raw_source_parses_supported_advisory_mime(
    content_type: str | None,
) -> None:
    binding = _provider("feed", "raw_source")
    catalog = _FakeCatalog(
        {
            "feed": DocumentArtifact(
                final_url="https://example.test/feed.xml",
                content_type=content_type,
                raw_source=b"<rss><channel /></rss>",
                capabilities=frozenset({"raw_source"}),
            )
        }
    )

    result = WebContentClient(catalog).resolve(_raw_source_request(), (binding,), None)

    assert result.provider_instance == "feed"


def test_article_exhaustion_keeps_redacted_ordered_attempts() -> None:
    first = _provider("first", "markdown")
    second = _provider("second", "markdown")
    catalog = _FakeCatalog(
        {
            "first": DocumentArtifact(
                source_url="https://example.test/article",
                final_url="https://example.test/article",
                content_type="text/markdown",
                markdown="",
                capabilities=frozenset({"markdown"}),
                provider_instance="first",
                provider_type="startup_http",
            ),
            "second": DocumentArtifact(
                source_url="https://example.test/article",
                final_url="https://example.test/article",
                content_type="text/markdown",
                markdown="",
                capabilities=frozenset({"markdown"}),
                provider_instance="second",
                provider_type="startup_http",
            ),
        }
    )

    with pytest.raises(FetchChainExhausted) as raised:
        WebContentClient(catalog).resolve(
            _article_request(),
            (first, second),
            (ExtractorBinding(name="identity", type="identity"),),
        )

    assert [failure.provider_instance for failure in raised.value.attempts] == [
        "first",
        "second",
    ]
    assert all(
        failure.reason in {"empty", "too_short", "extract_failed"}
        for failure in raised.value.attempts
    )


def test_shared_list_selector_returns_ordered_matches() -> None:
    from sluice.web.select import select_list_records

    matches = select_list_records(
        '<article class="release"><a href="/new">New release</a></article>',
        "https://example.test/releases",
        "article.release",
    )

    assert [match.href for match in matches] == ["/new"]
    assert [match.url for match in matches] == ["https://example.test/new"]


def test_no_href_match_does_not_hide_later_distinct_href_identity() -> None:
    from sluice.web.select import select_list_records

    matches = select_list_records(
        "<article>No link</article>"
        '<article><a href="/releases">Releases link</a></article>',
        "https://example.test/releases",
        "article",
    )

    assert [match.logical_id for match in matches] == [
        "https://example.test/releases#match-0",
        "https://example.test/releases",
    ]


def test_raw_feed_validator_accepts_zero_entry_atom() -> None:
    from sluice.web.feed import validate_raw_feed

    root = validate_raw_feed(
        b'<feed xmlns="http://www.w3.org/2005/Atom"><title>empty</title></feed>'
    )

    assert root is not None
    assert root.kind == "atom"


def test_resolve_rejects_mode_incompatible_extractor_chains_before_fetch() -> None:
    provider = _provider("only", "raw_html", "raw_source", "markdown")
    catalog = _FakeCatalog(
        {
            "only": DocumentArtifact(
                source_url="https://example.test/article",
                final_url="https://example.test/article",
                content_type="text/markdown",
                markdown="provider markdown",
                raw_source=b"<feed />",
                capabilities=frozenset({"markdown", "raw_source"}),
                provider_instance="only",
                provider_type="startup_http",
            )
        }
    )
    identity = (ExtractorBinding(name="identity", type="identity"),)
    non_identity = (ExtractorBinding(name="plain", type="beautifulsoup"),)
    client = WebContentClient(catalog)

    for request, extract_chain in (
        (_article_request(), None),
        (_article_request(), non_identity),
        (_list_request("article.release"), identity),
        (_raw_source_request(), identity),
    ):
        with pytest.raises(ExecutionError, match="extractor chain"):
            client.resolve(request, (provider,), extract_chain)

    assert catalog.calls == []


def test_article_stops_after_first_winner_without_prefetching_later_provider() -> None:
    first = _provider("first", "markdown")
    second = _provider("second", "markdown")
    catalog = _FakeCatalog(
        {
            "first": DocumentArtifact(
                final_url="https://example.test/article",
                content_type="text/markdown",
                markdown="first provider wins",
                capabilities=frozenset({"markdown"}),
            ),
            "second": DocumentArtifact(
                final_url="https://example.test/article",
                content_type="text/markdown",
                markdown="must never be fetched",
                capabilities=frozenset({"markdown"}),
            ),
        }
    )

    result = WebContentClient(catalog).resolve(
        _article_request(),
        (first, second),
        (ExtractorBinding(name="identity", type="identity"),),
    )

    assert result.provider_instance == "first"
    assert catalog.calls == ["first"]
    assert catalog.limiters["second"].acquire_calls == 0


def test_provider_without_an_acceptable_static_capability_is_never_called() -> None:
    ineligible = _provider("ineligible")
    eligible = _provider("eligible", "markdown")
    catalog = _FakeCatalog(
        {
            "ineligible": DocumentArtifact(
                final_url="https://example.test/article",
                content_type="text/markdown",
                markdown="must not bypass static eligibility",
                capabilities=frozenset({"markdown"}),
            ),
            "eligible": DocumentArtifact(
                final_url="https://example.test/article",
                content_type="text/markdown",
                markdown="eligible provider wins",
                capabilities=frozenset({"markdown"}),
            ),
        }
    )

    result = WebContentClient(catalog).resolve(
        _article_request(),
        (ineligible, eligible),
        (ExtractorBinding(name="identity", type="identity"),),
    )

    assert result.provider_instance == "eligible"
    assert catalog.calls == ["eligible"]
    assert catalog.limiters["ineligible"].acquire_calls == 0


def test_busy_limiter_is_not_released_without_a_permit() -> None:
    busy = _provider("busy", "markdown")
    available = _provider("available", "markdown")
    catalog = _FakeCatalog(
        {
            "busy": DocumentArtifact(),
            "available": DocumentArtifact(
                final_url="https://example.test/article",
                content_type="text/markdown",
                markdown="available provider wins",
                capabilities=frozenset({"markdown"}),
            ),
        }
    )

    class BusyLimiter(_FakeLimiter):
        def acquire(self, timeout: float | None = None) -> bool:
            del timeout
            self.acquire_calls += 1
            return False

    busy_limiter = BusyLimiter()
    catalog.limiters["busy"] = busy_limiter

    result = WebContentClient(catalog).resolve(
        _article_request(),
        (busy, available),
        (ExtractorBinding(name="identity", type="identity"),),
    )

    assert result.provider_instance == "available"
    assert busy_limiter.acquire_calls == 1
    assert busy_limiter.release_calls == 0
    assert catalog.calls == ["available"]
    assert [attempt.reason for attempt in result.attempts] == ["provider_busy"]


def test_source_attempt_without_stage_deadline_uses_provider_deadline_only(
    monkeypatch,
) -> None:
    now = 100.0
    monkeypatch.setattr("sluice.web.client.time.monotonic", lambda: now)
    binding = ProviderBinding(
        name="reader",
        type="startup_http",
        options={"trusted": True},
        timeout_seconds=15,
        capabilities=frozenset({"markdown"}),
    )

    class RecordingLimiter:
        timeout: float | None = None

        def acquire(self, timeout: float | None = None) -> bool:
            self.timeout = timeout
            return True

        def release(self) -> None:
            pass

    class RecordingProvider:
        received: FetchRequest | None = None

        def fetch(self, request: FetchRequest) -> DocumentArtifact:
            self.received = request
            return DocumentArtifact(
                final_url=request.url,
                content_type="text/markdown",
                markdown="provider deadline body",
                capabilities=frozenset({"markdown"}),
            )

    limiter = RecordingLimiter()
    provider = RecordingProvider()

    class Catalog:
        def limiter(self, received: ProviderBinding) -> RecordingLimiter:
            assert received is binding
            return limiter

        def provider(self, received: ProviderBinding) -> RecordingProvider:
            assert received is binding
            return provider

    result = WebContentClient(Catalog()).resolve(
        _article_request(),
        (binding,),
        (ExtractorBinding(name="identity", type="identity"),),
    )

    assert result.provider_instance == "reader"
    assert limiter.timeout == pytest.approx(15.0)
    assert provider.received is not None
    assert provider.received.stage_deadline == pytest.approx(115.0)


def test_active_provider_cooldown_advances_without_a_network_call(
    monkeypatch,
) -> None:
    monkeypatch.setattr("sluice.web.registry.time.monotonic", lambda: 100.0)
    cooling = _provider("cooling", "markdown")
    available = _provider("available", "markdown")
    catalog = _FakeCatalog(
        {
            "cooling": DocumentArtifact(),
            "available": DocumentArtifact(
                final_url="https://example.test/article",
                content_type="text/markdown",
                markdown="available after cooldown",
                capabilities=frozenset({"markdown"}),
            ),
        }
    )
    cooling_limiter = ProviderLimiter(concurrency=1, cooldown_seconds=30)
    cooling_limiter.cooldown()
    catalog.limiters["cooling"] = cooling_limiter

    result = WebContentClient(catalog).resolve(
        _article_request(),
        (cooling, available),
        (ExtractorBinding(name="identity", type="identity"),),
    )

    assert result.provider_instance == "available"
    assert catalog.calls == ["available"]
    assert [attempt.reason for attempt in result.attempts] == ["rate_limited"]


def test_retry_after_failure_cools_provider_and_retains_only_safe_status() -> None:
    limited = _provider("limited", "markdown")
    available = _provider("available", "markdown")
    rate_limit = WebSafetyError("rate_limited")
    rate_limit.reason = "rate_limited"
    rate_limit.status_code = 429
    rate_limit.status_kind = "service"
    rate_limit.retry_after_seconds = 17.0

    @dataclass
    class CooldownLimiter(_FakeLimiter):
        cooldown_calls: list[float | None] = None  # type: ignore[assignment]

        def __post_init__(self) -> None:
            self.cooldown_calls = []

        def cooldown(self, seconds: float | None = None) -> None:
            self.cooldown_calls.append(seconds)

    catalog = _FakeCatalog(
        {
            "limited": rate_limit,  # type: ignore[dict-item]
            "available": DocumentArtifact(
                final_url="https://example.test/article",
                content_type="text/markdown",
                markdown="fallback after rate limit",
                capabilities=frozenset({"markdown"}),
            ),
        }
    )
    limiter = CooldownLimiter()
    catalog.limiters["limited"] = limiter

    result = WebContentClient(catalog).resolve(
        _article_request(),
        (limited, available),
        (ExtractorBinding(name="identity", type="identity"),),
    )

    assert result.provider_instance == "available"
    assert limiter.cooldown_calls == [17.0]
    assert len(result.attempts) == 1
    assert result.attempts[0].reason == "rate_limited"
    assert result.attempts[0].status_code == 429
    assert result.attempts[0].status_kind == "service"


def test_provider_deadline_expiry_advances_to_next_source_provider(
    monkeypatch,
) -> None:
    clock = [100.0]
    monkeypatch.setattr("sluice.web.client.time.monotonic", lambda: clock[0])
    first = ProviderBinding(
        name="first",
        type="startup_http",
        options={"trusted": True},
        timeout_seconds=15,
        capabilities=frozenset({"markdown"}),
    )
    second = ProviderBinding(
        name="second",
        type="startup_http",
        options={"trusted": True},
        timeout_seconds=15,
        capabilities=frozenset({"markdown"}),
    )
    calls: list[str] = []

    class Catalog:
        limiters = {"first": _FakeLimiter(), "second": _FakeLimiter()}

        def limiter(self, binding: ProviderBinding) -> _FakeLimiter:
            return self.limiters[binding.name]

        def provider(self, binding: ProviderBinding):
            class Provider:
                def fetch(self, request: FetchRequest) -> DocumentArtifact:
                    calls.append(binding.name)
                    if binding.name == "first":
                        assert request.stage_deadline == 115.0
                        clock[0] = 116.0
                    return DocumentArtifact(
                        final_url=request.url,
                        content_type="text/markdown",
                        markdown=f"{binding.name} provider body",
                        capabilities=frozenset({"markdown"}),
                    )

            return Provider()

    result = WebContentClient(Catalog()).resolve(
        _article_request(),
        (first, second),
        (ExtractorBinding(name="identity", type="identity"),),
    )

    assert result.provider_instance == "second"
    assert calls == ["first", "second"]
    assert [attempt.reason for attempt in result.attempts] == ["timeout"]


def test_stage_deadline_expiry_never_starts_another_provider(monkeypatch) -> None:
    clock = [100.0]
    monkeypatch.setattr("sluice.web.client.time.monotonic", lambda: clock[0])
    first = ProviderBinding(
        name="first",
        type="startup_http",
        options={"trusted": True},
        timeout_seconds=15,
        capabilities=frozenset({"markdown"}),
    )
    second = ProviderBinding(
        name="second",
        type="startup_http",
        options={"trusted": True},
        timeout_seconds=15,
        capabilities=frozenset({"markdown"}),
    )
    calls: list[str] = []

    class Catalog:
        def limiter(self, binding: ProviderBinding) -> _FakeLimiter:
            return _FakeLimiter()

        def provider(self, binding: ProviderBinding):
            class Provider:
                def fetch(self, request: FetchRequest) -> DocumentArtifact:
                    calls.append(binding.name)
                    clock[0] = 111.0
                    return DocumentArtifact(
                        final_url=request.url,
                        content_type="text/markdown",
                        markdown="late stage body",
                        capabilities=frozenset({"markdown"}),
                    )

            return Provider()

    request = FetchRequest(
        url="https://example.test/article",
        mode="article",
        acceptable=frozenset({"markdown", "raw_html", "text"}),
        preferred=("markdown", "raw_html", "text"),
        quality=QualityPolicy(min_chars=1),
        stage_deadline=110.0,
    )

    expired = False
    try:
        WebContentClient(Catalog()).resolve(
            request,
            (first, second),
            (ExtractorBinding(name="identity", type="identity"),),
        )
    except WebStageDeadlineExpired:
        expired = True

    assert expired is True
    assert calls == ["first"]


def test_stage_deadline_dominates_a_final_provider_timeout(monkeypatch) -> None:
    clock = [100.0]
    monkeypatch.setattr("sluice.web.client.time.monotonic", lambda: clock[0])
    binding = ProviderBinding(
        name="only",
        type="startup_http",
        options={"trusted": True},
        timeout_seconds=30,
        capabilities=frozenset({"markdown"}),
    )

    class Catalog:
        def limiter(self, received: ProviderBinding) -> _FakeLimiter:
            assert received is binding
            return _FakeLimiter()

        def provider(self, received: ProviderBinding):
            assert received is binding

            class Provider:
                def fetch(self, request: FetchRequest) -> DocumentArtifact:
                    assert request.stage_deadline == 110.0
                    clock[0] = 111.0
                    raise WebSafetyError("timeout")

            return Provider()

    request = FetchRequest(
        url="https://example.test/article",
        mode="article",
        acceptable=frozenset({"markdown"}),
        preferred=("markdown",),
        quality=QualityPolicy(min_chars=1),
        stage_deadline=110.0,
    )

    with pytest.raises(WebStageDeadlineExpired):
        WebContentClient(Catalog()).resolve(
            request,
            (binding,),
            (ExtractorBinding(name="identity", type="identity"),),
        )


def test_probe_cancellation_during_limiter_acquire_beats_busy_exhaustion() -> None:
    binding = _provider("only", "markdown")
    stale = False
    network_calls = 0

    class Limiter:
        def acquire(self, timeout: float | None = None) -> bool:
            del timeout
            nonlocal stale
            stale = True
            return False

    class Catalog:
        def limiter(self, received: ProviderBinding) -> Limiter:
            assert received is binding
            return Limiter()

        def provider(self, received: ProviderBinding):
            assert received is binding

            class Provider:
                def fetch(self, request: FetchRequest) -> DocumentArtifact:
                    del request
                    nonlocal network_calls
                    network_calls += 1
                    raise AssertionError("a busy provider must not run")

            return Provider()

    def check() -> None:
        if stale:
            raise WebAttemptCancelled()

    token = set_web_execution_probe(WebExecutionProbe(check))
    try:
        with pytest.raises(WebAttemptCancelled):
            WebContentClient(Catalog()).resolve(
                _article_request(),
                (binding,),
                (ExtractorBinding(name="identity", type="identity"),),
            )
    finally:
        reset_web_execution_probe(token)

    assert network_calls == 0


def test_probe_cancellation_after_extractor_blocks_publication() -> None:
    binding = _provider("html", "raw_html")
    checks = 0
    extractor_calls = 0

    class Catalog:
        def limiter(self, received: ProviderBinding) -> _FakeLimiter:
            assert received is binding
            return _FakeLimiter()

        def provider(self, received: ProviderBinding):
            assert received is binding

            class Provider:
                def fetch(self, request: FetchRequest) -> DocumentArtifact:
                    return DocumentArtifact(
                        final_url=request.url,
                        content_type="text/html",
                        raw_html=(
                            "<article><p>"
                            + ("provider html body " * 12)
                            + "</p></article>"
                        ),
                        capabilities=frozenset({"raw_html"}),
                    )

            return Provider()

        def extractor(self, received: ExtractorBinding):
            assert received.type == "beautifulsoup"

            class Extractor:
                def extract(self, artifact: DocumentArtifact) -> DocumentArtifact:
                    nonlocal extractor_calls
                    extractor_calls += 1
                    return DocumentArtifact(
                        source_url=artifact.source_url,
                        final_url=artifact.final_url,
                        content_type="text/plain",
                        text="extracted provider body",
                        selected_format="text",
                        capabilities=frozenset({"text"}),
                    )

            return Extractor()

    def check() -> None:
        nonlocal checks
        checks += 1
        if checks == 6:
            raise WebAttemptCancelled()

    token = set_web_execution_probe(WebExecutionProbe(check))
    cancelled = False
    try:
        WebContentClient(Catalog()).resolve(
            _article_request(),
            (binding,),
            (
                ExtractorBinding(name="identity", type="identity"),
                ExtractorBinding(name="local", type="beautifulsoup"),
            ),
        )
    except WebAttemptCancelled:
        cancelled = True
    finally:
        reset_web_execution_probe(token)

    assert extractor_calls == 1
    assert checks == 6
    assert cancelled is True


def test_list_exhaustion_returns_earliest_valid_empty_candidate() -> None:
    first = _provider("first", "raw_html")
    second = _provider("second", "raw_html")
    catalog = _FakeCatalog(
        {
            name: DocumentArtifact(
                final_url=f"https://example.test/{name}",
                content_type="text/html",
                raw_html="<main><p>" + (f"valid empty {name} " * 12) + "</p></main>",
                capabilities=frozenset({"raw_html"}),
            )
            for name in ("first", "second")
        }
    )

    result = WebContentClient(catalog).resolve(
        _list_request("article.release"), (first, second), None
    )

    assert result.provider_instance == "first"
    assert result.final_url == "https://example.test/first"
    assert catalog.calls == ["first", "second"]


def test_list_missing_mime_requires_deterministic_html_sniff() -> None:
    plain = _provider("plain", "raw_html")
    html = _provider("html", "raw_html")
    catalog = _FakeCatalog(
        {
            "plain": DocumentArtifact(
                final_url="https://example.test/releases",
                content_type=None,
                raw_html="plain response without an HTML signature " * 8,
                capabilities=frozenset({"raw_html"}),
            ),
            "html": DocumentArtifact(
                final_url="https://example.test/releases",
                content_type="text/html",
                raw_html=(
                    '<article class="release"><a href="/new">'
                    + ("new release " * 12)
                    + "</a></article>"
                ),
                capabilities=frozenset({"raw_html"}),
            ),
        }
    )

    result = WebContentClient(catalog).resolve(
        _list_request("article.release"), (plain, html), None
    )

    assert result.provider_instance == "html"
    assert [attempt.reason for attempt in result.attempts] == ["bad_content_type"]


def test_article_rejects_challenge_html_even_with_provider_markdown() -> None:
    first = _provider("first", "raw_html", "markdown")
    second = _provider("second", "markdown")
    catalog = _FakeCatalog(
        {
            "first": DocumentArtifact(
                final_url="https://example.test/article",
                content_type="text/html",
                raw_html='<div id="challenge-running">verify you are human</div>',
                markdown="apparently useful markdown",
                selected_format="markdown",
                capabilities=frozenset({"raw_html", "markdown"}),
            ),
            "second": DocumentArtifact(
                final_url="https://example.test/article",
                content_type="text/markdown",
                markdown="safe fallback markdown",
                capabilities=frozenset({"markdown"}),
            ),
        }
    )

    result = WebContentClient(catalog).resolve(
        _article_request(),
        (first, second),
        (ExtractorBinding(name="identity", type="identity"),),
    )

    assert result.provider_instance == "second"
    assert catalog.calls == ["first", "second"]
    assert [attempt.reason for attempt in result.attempts] == ["challenge"]


@pytest.mark.parametrize(
    "artifact",
    [
        DocumentArtifact(
            final_url="https://example.test/article",
            content_type="application/json",
            markdown="provider markdown that must not bypass its invalid MIME",
            selected_format="markdown",
            capabilities=frozenset({"markdown"}),
        ),
        DocumentArtifact(
            final_url="https://example.test/article",
            content_type="text/plain",
            raw_html="<article>" + ("raw HTML with the wrong MIME " * 8) + "</article>",
            capabilities=frozenset({"raw_html"}),
        ),
    ],
)
def test_article_rejects_representation_with_incompatible_mime(
    artifact: DocumentArtifact,
) -> None:
    binding = _provider("bad", *artifact.capabilities)
    chain = [ExtractorBinding(name="identity", type="identity")]
    if "raw_html" in artifact.capabilities:
        chain.append(ExtractorBinding(name="plain", type="beautifulsoup"))
    catalog = _FakeCatalog({"bad": artifact})

    with pytest.raises(FetchChainExhausted) as raised:
        WebContentClient(catalog).resolve(_article_request(), (binding,), chain)

    assert [attempt.reason for attempt in raised.value.attempts] == ["bad_content_type"]


def test_article_rejects_unadvertised_raw_html_without_running_extractors() -> None:
    binding = _provider("bad", "raw_html")
    catalog = _FakeCatalog(
        {
            "bad": DocumentArtifact(
                final_url="https://example.test/article",
                content_type="text/html",
                raw_html="<article>" + ("unadvertised content " * 8) + "</article>",
                capabilities=frozenset(),
            )
        }
    )

    with pytest.raises(FetchChainExhausted) as raised:
        WebContentClient(catalog).resolve(
            _article_request(),
            (binding,),
            (
                ExtractorBinding(name="identity", type="identity"),
                ExtractorBinding(name="plain", type="beautifulsoup"),
            ),
        )

    assert [attempt.reason for attempt in raised.value.attempts] == [
        "missing_capability"
    ]
    assert catalog.extractor_calls == []


def test_article_recomputes_stale_provider_quality_for_the_winner() -> None:
    binding = _provider("reader", "markdown")
    catalog = _FakeCatalog(
        {
            "reader": DocumentArtifact(
                final_url="https://example.test/article",
                content_type="text/markdown",
                markdown="quality is evaluated under this request",
                selected_format="markdown",
                quality={"passed": False, "reason": "too_short"},
                capabilities=frozenset({"markdown"}),
            )
        }
    )

    result = WebContentClient(catalog).resolve(
        _article_request(),
        (binding,),
        (ExtractorBinding(name="identity", type="identity"),),
    )

    assert result.quality.passed is True
    assert result.quality.reason is None


def test_identity_ignores_unacceptable_markdown_and_uses_provider_text() -> None:
    request = FetchRequest(
        url="https://example.test/article",
        mode="article",
        acceptable=frozenset({"text"}),
        preferred=("text",),
        quality=QualityPolicy(min_chars=1),
    )
    binding = _provider("reader", "markdown", "text")
    catalog = _FakeCatalog(
        {
            "reader": DocumentArtifact(
                final_url=request.url,
                content_type="text/plain",
                markdown="unacceptable markdown",
                text="acceptable provider text",
                selected_format="markdown",
                capabilities=frozenset({"markdown", "text"}),
            )
        }
    )

    result = WebContentClient(catalog).resolve(
        request,
        (binding,),
        (ExtractorBinding(name="identity", type="identity"),),
    )

    assert result.selected_format == "text"
    assert result.text == "acceptable provider text"
    assert result.extraction == "identity"


def test_identity_tries_provider_originals_in_preferred_order() -> None:
    request = FetchRequest(
        url="https://example.test/article",
        mode="article",
        acceptable=frozenset({"markdown", "text"}),
        preferred=("markdown", "text"),
        quality=QualityPolicy(min_chars=20),
    )
    binding = _provider("reader", "markdown", "text")
    catalog = _FakeCatalog(
        {
            "reader": DocumentArtifact(
                final_url=request.url,
                content_type="text/plain",
                markdown="too short",
                text="The provider text representation passes quality.",
                selected_format="markdown",
                capabilities=frozenset({"markdown", "text"}),
            )
        }
    )

    result = WebContentClient(catalog).resolve(
        request,
        (binding,),
        (ExtractorBinding(name="identity", type="identity"),),
    )

    assert result.selected_format == "text"
    assert result.text == "The provider text representation passes quality."
    assert result.extraction == "identity"
    assert catalog.extractor_calls == []


def test_extract_binding_digest_is_canonical_for_equivalent_options() -> None:
    binding = _provider("reader", "markdown")
    artifact = DocumentArtifact(
        final_url="https://example.test/article",
        content_type="text/markdown",
        markdown="provider markdown",
        capabilities=frozenset({"markdown"}),
    )

    def resolve(options: dict[str, object]) -> str | None:
        result = WebContentClient(_FakeCatalog({"reader": artifact})).resolve(
            _article_request(),
            (binding,),
            (
                ExtractorBinding(name="identity", type="identity"),
                ExtractorBinding(name="plain", type="beautifulsoup", options=options),
            ),
        )
        return result.extract_binding_digest

    first = resolve({"mode": "article", "remove_selectors": ["nav", "aside"]})
    second = resolve({"remove_selectors": ["nav", "aside"], "mode": "article"})

    assert first is not None
    assert first == second


def test_provider_cannot_return_capability_outside_its_static_declaration() -> None:
    binding = _provider("bad", "raw_html")
    catalog = _FakeCatalog(
        {
            "bad": DocumentArtifact(
                final_url="https://example.test/article",
                content_type="text/markdown",
                markdown="undeclared markdown",
                capabilities=frozenset({"markdown"}),
            )
        }
    )

    with pytest.raises(FetchChainExhausted) as raised:
        WebContentClient(catalog).resolve(
            _article_request(),
            (binding,),
            (ExtractorBinding(name="identity", type="identity"),),
        )

    assert [attempt.reason for attempt in raised.value.attempts] == [
        "missing_capability"
    ]


def test_raw_source_winner_retains_only_the_validated_raw_representation() -> None:
    binding = _provider("feed", "raw_source", "raw_html", "text", "markdown")
    catalog = _FakeCatalog(
        {
            "feed": DocumentArtifact(
                final_url="https://example.test/feed.xml",
                content_type="application/atom+xml",
                raw_source=b'<feed xmlns="http://www.w3.org/2005/Atom" />',
                raw_html="<p>rewritten</p>",
                text="rewritten",
                markdown="rewritten",
                selected_format="markdown",
                capabilities=frozenset({"raw_source", "raw_html", "text", "markdown"}),
            )
        }
    )

    result = WebContentClient(catalog).resolve(_raw_source_request(), (binding,), None)

    assert result.raw_source == b'<feed xmlns="http://www.w3.org/2005/Atom" />'
    assert result.raw_html is None
    assert result.text is None
    assert result.markdown is None
    assert result.selected_format is None
    assert result.capabilities == frozenset({"raw_source"})


def test_raw_feed_validator_rejects_dtd_entities_and_xinclude() -> None:
    from sluice.web.feed import validate_raw_feed

    assert (
        validate_raw_feed(
            b'<!DOCTYPE feed [<!ENTITY xxe SYSTEM "file:///etc/passwd">]>'
            b'<feed xmlns="http://www.w3.org/2005/Atom"><title>&xxe;</title></feed>'
        )
        is None
    )
    assert (
        validate_raw_feed(
            b'<feed xmlns="http://www.w3.org/2005/Atom" '
            b'xmlns:x="http://www.w3.org/2001/XInclude"><x:include href="file:///etc/passwd"/></feed>'
        )
        is None
    )


def test_raw_feed_validator_rejects_utf16_dtd_entity_expansion() -> None:
    from sluice.web.feed import validate_raw_feed

    unsafe = (
        '<!DOCTYPE rss [<!ENTITY x "private">]>'
        "<rss><channel><title>&x;</title></channel></rss>"
    ).encode("utf-16")

    assert validate_raw_feed(unsafe) is None


def test_raw_feed_validator_does_not_treat_plain_include_text_as_xinclude() -> None:
    from sluice.web.feed import validate_raw_feed

    root = validate_raw_feed(
        b'<feed xmlns="http://www.w3.org/2005/Atom">'
        b"<title>release:include notes</title><include>local metadata</include></feed>"
    )

    assert root is not None
    assert root.kind == "atom"


@pytest.mark.parametrize("root", [b"<RSS><channel /></RSS>", b"<Feed />"])
def test_raw_feed_validator_requires_exact_root_case(root: bytes) -> None:
    from sluice.web.feed import validate_raw_feed

    assert validate_raw_feed(root) is None
