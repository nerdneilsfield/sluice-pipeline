"""Integration coverage for private web-backed built-in Sources."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from sluice.app import open_app
from sluice.errors import ExecutionError
from sluice.source_runtime import collect_source
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


def test_rss_uses_synthetic_direct_provider_then_raw_fallback_for_zero_entry_atom(tmp_path: Path) -> None:
    client = ScriptedClient(
        [
            ExecutionError("web source provider chain exhausted"),
            DocumentArtifact(
                source_url="https://example.test/feed.xml",
                final_url="https://example.test/feed.xml",
                content_type="application/atom+xml",
                raw_source=b'<feed xmlns="http://www.w3.org/2005/Atom"><title>empty</title></feed>',
                capabilities=frozenset({"raw_source"}),
                provider_instance="fallback",
                provider_type="startup_http",
            ),
        ]
    )
    app = _open(
        tmp_path,
        """version: 1
web:
  providers:
    fallback: {type: startup_http, trusted: true}
  fetch_chains: {raw: {providers: [fallback]}}
sources:
  feed:
    use: sluice.sources.rss
    with: {url: https://example.test/feed.xml, raw_fallback_chain: raw}
""",
        client,
    )
    try:
        collect_source(app, "feed", datetime(2026, 1, 1, tzinfo=timezone.utc))
        count = app.conn.execute("SELECT COUNT(*) FROM items").fetchone()[0]
    finally:
        app.close()

    assert client.calls == [("__rss_direct__",), ("fallback",)]
    assert count == 0
