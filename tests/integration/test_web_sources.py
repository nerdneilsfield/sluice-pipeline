"""Integration coverage for private web-backed built-in Sources."""

from __future__ import annotations

import logging
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from types import MappingProxyType, SimpleNamespace

import pytest

from sluice import source_runtime
from sluice.app import open_app
from sluice.config import load_definition
from sluice.errors import ExecutionError
from sluice.read_model import pipeline_list
from sluice.source_runtime import collect_source
from sluice.sources.rss import parse_feed_bytes
from sluice.web.client import WebContentClient
from sluice.web.contracts import DocumentArtifact, WebBinding
from sluice.web.providers import FirecrawlProvider
from sluice.web.registry import resolve_provider_binding


class _HTTP:
    def get(self, url: str) -> str:
        raise AssertionError(f"unexpected startup HTTP request: {url}")


class ScriptedClient:
    def __init__(self, results: list[object]) -> None:
        self.results = iter(results)
        self.calls: list[tuple[str, ...]] = []

    def resolve(self, request, fetch_chain, extract_chain):
        del request, extract_chain
        self.calls.append(tuple(provider.name for provider in fetch_chain))
        result = next(self.results)
        if isinstance(result, BaseException):
            raise result
        return result


def _open(tmp_path: Path, definition: str, client: ScriptedClient):
    path = tmp_path / "definition.yml"
    path.write_text(definition, encoding="utf-8")
    return open_app(
        path, tmp_path / "state.db", http_client=_HTTP(), web_content_client=client
    )


def _raw_artifact(raw: bytes, provider: str = "fallback") -> DocumentArtifact:
    return DocumentArtifact(
        source_url="https://example.test/feed.xml",
        final_url="https://example.test/feed.xml",
        content_type="application/atom+xml",
        raw_source=raw,
        capabilities=frozenset({"raw_source"}),
        provider_instance=provider,
        provider_type="startup_http",
    )


def _rss_definition() -> str:
    return """version: 1
web:
  providers:
    fallback: {type: startup_http, trusted: true}
  fetch_chains: {raw: {providers: [fallback]}}
sources:
  feed:
    use: sluice.sources.rss
    with: {url: https://example.test/feed.xml, raw_fallback_chain: raw}
"""


def test_list_source_projects_selected_records_without_persisting_raw_html(
    tmp_path: Path,
) -> None:
    client = ScriptedClient(
        [
            DocumentArtifact(
                source_url="https://example.test/releases",
                final_url="https://example.test/releases",
                content_type="text/html",
                raw_html='<article class="release"><a href="/new">New release</a></article>',
                capabilities=frozenset({"raw_html"}),
                provider_instance="browser",
                provider_type="startup_http",
            )
        ]
    )
    app = _open(
        tmp_path,
        """version: 1
web:
  providers:
    static: {type: startup_http, trusted: true}
    browser: {type: startup_http, trusted: true}
  fetch_chains: {robust: {providers: [static, browser]}}
sources:
  releases:
    use: sluice.sources.webpage
    with: {url: https://example.test/releases, list_selector: article.release, fetch_chain: robust}
""",
        client,
    )
    try:
        collect_source(app, "releases", datetime(2026, 1, 1, tzinfo=UTC))
        row = app.conn.execute(
            "SELECT logical_id,payload_json,metadata_json FROM items"
        ).fetchone()
    finally:
        app.close()

    assert client.calls == [("static", "browser")]
    assert row[0] == "https://example.test/new"
    assert (
        row[1]
        == '{"content":"New release","title":"New release","url":"https://example.test/new"}'
    )
    assert '"extractor":"list_selector"' in row[2]
    assert "raw_html" not in row[1]
    assert "<article" not in row[2]


@pytest.mark.parametrize(
    "invalid_direct",
    [
        b"not XML",
        b'<!DOCTYPE feed [<!ENTITY xxe "blocked">]><feed />',
        b'<feed xmlns="http://www.w3.org/2005/Atom"><xi:include xmlns:xi="http://www.w3.org/2001/XInclude" /></feed>',
        b"<document><entry><id>nested</id></entry></document>",
    ],
)
def test_rss_invalid_direct_artifact_falls_back_to_valid_zero_entry_atom(
    tmp_path: Path, invalid_direct: bytes
) -> None:
    client = ScriptedClient(
        [
            _raw_artifact(invalid_direct, provider="__rss_direct__"),
            _raw_artifact(
                b'<feed xmlns="http://www.w3.org/2005/Atom"><title>empty</title></feed>'
            ),
        ]
    )
    app = _open(tmp_path, _rss_definition(), client)
    try:
        collect_source(app, "feed", datetime(2026, 1, 1, tzinfo=UTC))
        count = app.conn.execute("SELECT COUNT(*) FROM items").fetchone()[0]
    finally:
        app.close()

    assert client.calls == [("__rss_direct__",), ("fallback",)]
    assert count == 0


def test_rss_invalid_direct_artifact_without_fallback_fails(tmp_path: Path) -> None:
    client = ScriptedClient([_raw_artifact(b"not XML", provider="__rss_direct__")])
    app = _open(
        tmp_path,
        """version: 1
sources:
  feed: {use: sluice.sources.rss, with: {url: https://example.test/feed.xml}}
""",
        client,
    )
    try:
        with pytest.raises(ExecutionError, match="provider_error"):
            collect_source(app, "feed", datetime(2026, 1, 1, tzinfo=UTC))
    finally:
        app.close()


def test_web_source_failures_hide_resolved_provider_secrets_for_direct_and_queued_collection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    secret = "actual-secret-value"
    vendor_body = "vendor-body-private"
    monkeypatch.setenv("WEB_SECRET", secret)
    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["93.184.216.34"])
    definition_path = tmp_path / "definition.yml"
    definition_path.write_text(
        """version: 1
web:
  providers:
    vendor: {type: firecrawl, base_url: https://vendor.test, api_key: env:WEB_SECRET}
  fetch_chains: {robust: {providers: [vendor]}}
sources:
  page:
    use: sluice.sources.webpage
    retries: 1
    with: {url: https://example.test/page, list_selector: article, fetch_chain: robust}
pipelines:
  p:
    nodes:
      documents: {from: source.page, deliver: {use: tests.integration.test_web_sources.sink, with: {}}}
""",
        encoding="utf-8",
    )

    class Transport:
        def post(self, url, json=None, headers=None, **kwargs):
            del url, json, headers, kwargs
            raise RuntimeError(f"{vendor_body} contains {secret}")

    binding = load_definition(definition_path).sources["page"].web_binding
    assert binding is not None
    resolved = resolve_provider_binding(binding.fetch_chain[0])
    assert resolved.options["api_key"] == secret
    provider = FirecrawlProvider(resolved, SimpleNamespace(service_client=Transport()))

    class Catalog:
        def provider(self, received):
            assert received.name == "vendor"
            return provider

        def limiter(self, received):
            del received

    app = open_app(
        definition_path,
        tmp_path / "state.db",
        http_client=_HTTP(),
        web_content_client=WebContentClient(Catalog()),
    )
    caplog.set_level(logging.INFO, logger="sluice.source_runtime")
    try:
        with pytest.raises(ExecutionError, match="provider_error"):
            collect_source(app, "page", datetime(2026, 1, 1, tzinfo=UTC))
        queued_id = app.conn.execute(
            "SELECT id FROM executions WHERE kind='source' AND state='queued'"
        ).fetchone()[0]
        source_runtime._execute_queued_source(
            app, queued_id, datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC)
        )
        errors = [
            str(row[0] or "")
            for row in app.conn.execute(
                "SELECT error FROM executions WHERE kind='source' ORDER BY id"
            )
        ]
        summary = next(item for item in pipeline_list(app) if item.name == "p")
    finally:
        app.close()

    assert errors == ["provider_error", "provider_error"]
    assert summary.latest_error == "provider_error"
    assert secret not in caplog.text and vendor_body not in caplog.text
    assert secret not in str(summary) and vendor_body not in str(summary)
    assert all(secret not in error and vendor_body not in error for error in errors)


class _CustomSource:
    def collect(self, config, checkpoint):
        del config, checkpoint
        raise ExecutionError("custom source diagnostic")


def test_custom_source_with_web_binding_retains_its_diagnostic(tmp_path: Path) -> None:
    app = _open(
        tmp_path,
        """version: 1
sources:
  custom: {use: tests.integration.test_web_sources.custom_source, with: {}}
""",
        ScriptedClient([]),
    )
    source = app.definition.sources["custom"]
    replacement = replace(source, web_binding=WebBinding())
    object.__setattr__(
        app,
        "definition",
        replace(app.definition, sources=MappingProxyType({"custom": replacement})),
    )
    try:
        with pytest.raises(ExecutionError, match="custom source diagnostic"):
            collect_source(app, "custom", datetime(2026, 1, 1, tzinfo=UTC))
        assert (
            app.conn.execute("SELECT error FROM executions").fetchone()[0]
            == "custom source diagnostic"
        )
    finally:
        app.close()


custom_source = _CustomSource()


def test_builtin_source_without_web_binding_retains_compatibility_diagnostic(
    tmp_path: Path,
) -> None:
    app = _open(
        tmp_path,
        """version: 1
web:
  providers: {direct: {type: startup_http, trusted: true}}
  fetch_chains: {robust: {providers: [direct]}}
sources:
  page:
    use: sluice.sources.webpage
    with: {url: https://example.test/page, list_selector: article, fetch_chain: robust}
""",
        ScriptedClient([]),
    )
    source = app.definition.sources["page"]
    replacement = replace(source, web_binding=None)
    object.__setattr__(
        app,
        "definition",
        replace(app.definition, sources=MappingProxyType({"page": replacement})),
    )
    try:
        with pytest.raises(
            ExecutionError, match="webpage collect requires literal HTML"
        ):
            collect_source(app, "page", datetime(2026, 1, 1, tzinfo=UTC))
        assert (
            app.conn.execute("SELECT error FROM executions").fetchone()[0]
            == "webpage collect requires literal HTML"
        )
    finally:
        app.close()


def sink(config, payload, delivery_key):
    del config, payload, delivery_key


def test_feed_projection_uses_only_kind_specific_direct_entry_children() -> None:
    atom = b"""<feed xmlns="http://www.w3.org/2005/Atom">
      <entry><id>root-entry</id><title>Root</title></entry>
      <extension><entry><id>nested-entry</id><title>Nested</title></entry></extension>
      <item><guid>wrong-atom-item</guid></item>
    </feed>"""
    rss2 = b"""<rss version="2.0"><channel>
      <item><guid>channel-item</guid><title>Channel</title></item>
      <extension><item><guid>nested-item</guid><title>Nested</title></item></extension>
      <entry><id>wrong-rss-entry</id></entry>
    </channel><item><guid>root-item</guid></item></rss>"""
    rss1 = b"""<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
      <item><title>rdf-item</title></item>
      <extension><item><title>nested-rdf-item</title></item></extension>
      <entry><id>wrong-rdf-entry</id></entry>
    </rdf:RDF>"""

    assert [
        record.logical_id
        for record in parse_feed_bytes(atom, "https://example.test/feed")
    ] == ["root-entry"]
    assert [
        record.logical_id
        for record in parse_feed_bytes(rss2, "https://example.test/feed")
    ] == ["channel-item"]
    assert [
        record.logical_id
        for record in parse_feed_bytes(rss1, "https://example.test/feed")
    ] == ["rdf-item"]
    assert (
        parse_feed_bytes(
            b'<feed xmlns="http://www.w3.org/2005/Atom"><extension><entry><id>nested</id></entry></extension></feed>',
            "https://example.test/feed",
        )
        == ()
    )
