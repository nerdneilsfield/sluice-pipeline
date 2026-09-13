"""Integration coverage for private web-backed built-in Sources."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from sluice.app import open_app
from sluice.errors import ExecutionError
from sluice.source_runtime import collect_source
from sluice.sources.rss import parse_feed_bytes
from sluice.web.contracts import DocumentArtifact


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
    return open_app(path, tmp_path / "state.db", http_client=_HTTP(), web_content_client=client)


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


def test_list_source_projects_selected_records_without_persisting_raw_html(tmp_path: Path) -> None:
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
        collect_source(app, "releases", datetime(2026, 1, 1, tzinfo=timezone.utc))
        row = app.conn.execute("SELECT logical_id,payload_json,metadata_json FROM items").fetchone()
    finally:
        app.close()

    assert client.calls == [("static", "browser")]
    assert row[0] == "https://example.test/new"
    assert row[1] == '{"content":"New release","title":"New release","url":"https://example.test/new"}'
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
        collect_source(app, "feed", datetime(2026, 1, 1, tzinfo=timezone.utc))
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
        with pytest.raises(ExecutionError, match="invalid RSS raw source"):
            collect_source(app, "feed", datetime(2026, 1, 1, tzinfo=timezone.utc))
    finally:
        app.close()


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

    assert [record.logical_id for record in parse_feed_bytes(atom, "https://example.test/feed")] == ["root-entry"]
    assert [record.logical_id for record in parse_feed_bytes(rss2, "https://example.test/feed")] == ["channel-item"]
    assert [record.logical_id for record in parse_feed_bytes(rss1, "https://example.test/feed")] == ["rdf-item"]
    assert parse_feed_bytes(
        b'<feed xmlns="http://www.w3.org/2005/Atom"><extension><entry><id>nested</id></entry></extension></feed>',
        "https://example.test/feed",
    ) == ()
