"""Content extraction behavior inspired by the legacy article fetcher."""

from __future__ import annotations

import pytest

from sluice.contracts import NEW
from sluice.errors import ExecutionError
from sluice.tasks.content import extract_content
from sluice.web.contracts import DocumentArtifact
from sluice.web.extractors import EXTRACTOR_FORMATS, beautifulsoup
from sluice.web.projectors import project_markdown, project_text


def test_extract_content_prefers_article_and_drops_boilerplate() -> None:
    html = """
    <html><body>
      <nav>Home Login Subscribe</nav>
      <main class="page-shell">
        <article class="post-content">
          <h1>Useful headline</h1>
          <p>The first paragraph has the information readers need.</p>
          <p>The second paragraph contains the actual article body.</p>
        </article>
      </main>
      <aside>Related links and advertisements</aside>
      <footer>Copyright and cookie settings</footer>
    </body></html>
    """

    result = extract_content.run({}, html)

    assert isinstance(result, NEW)
    assert result.value == (
        "Useful headline\n"
        "The first paragraph has the information readers need.\n"
        "The second paragraph contains the actual article body."
    )
    assert "Subscribe" not in result.value
    assert "advertisements" not in result.value


def test_extract_content_scores_content_container_without_article_tag() -> None:
    html = """
    <div class="cookie-banner">Accept all cookies now</div>
    <div class="post-body">
      <h2>Title</h2>
      <p>This is the long enough body that should win the content score.</p>
      <p>It has a second paragraph so it is clearly the article container.</p>
    </div>
    <div class="site-menu">Home Topics Archive</div>
    """

    result = extract_content.run({}, html)

    assert isinstance(result, NEW)
    assert "This is the long enough body" in result.value
    assert "Accept all cookies" not in result.value
    assert "site-menu" not in result.value


def test_extract_content_supports_explicit_plain_mode() -> None:
    html = "<nav>Menu</nav><article><p>Body</p></article>"

    result = extract_content.run({"mode": "plain"}, html)

    assert isinstance(result, NEW)
    assert result.value == "Menu\nBody"


def test_extract_content_keeps_article_table_text_and_decodes_entities() -> None:
    html = """
    <article>
      <p>R&amp;D&nbsp;results</p>
      <table><tr><th>Quarter</th><th>Score</th></tr>
        <tr><td>Q1</td><td>42</td></tr>
      </table>
    </article>
    """

    result = extract_content.run({}, html)

    assert isinstance(result, NEW)
    assert "R&D results" in result.value
    assert "Quarter" in result.value
    assert "Q1" in result.value
    assert "42" in result.value


def test_extract_content_rejects_non_string_mode_with_execution_error() -> None:
    with pytest.raises(ExecutionError, match="mode must be article or plain"):
        extract_content.run({"mode": []}, "<article>Body</article>")


def test_offline_extractor_uses_fixed_output_format() -> None:
    artifact = DocumentArtifact(
        raw_html="<nav>Menu</nav><article><h1>Headline</h1><p>Body</p></article>",
        content_type="text/html",
        capabilities=frozenset({"raw_html"}),
    )

    result = beautifulsoup.extract(artifact)

    assert result.selected_format == "text"
    assert result.text == "Headline\nBody"
    assert EXTRACTOR_FORMATS == {
        "trafilatura": "markdown",
        "newspaper4k": "text",
        "resiliparse": "text",
        "beautifulsoup": "text",
    }


def test_projectors_require_selected_artifact_and_never_persist_visible_text() -> None:
    artifact = DocumentArtifact(
        raw_html="<p>Body</p>",
        content_type="text/html",
        capabilities=frozenset({"raw_html"}),
    )

    with pytest.raises(ExecutionError, match="selected_format"):
        project_markdown(artifact)

    selected = DocumentArtifact(
        markdown="# Title\n\nBody",
        selected_format="markdown",
        content_type="text/markdown",
        capabilities=frozenset({"markdown"}),
        quality={"quality_chars": 10, "visible_text": "must stay transient"},
    )
    result = project_markdown(selected)

    assert result.value == "# Title\n\nBody"
    assert "visible_text" not in result.metadata["web"]
    assert result.metadata["web"]["quality_chars"] == 10


def test_projectors_accept_item_and_string_with_metadata_merge() -> None:
    from sluice.contracts import Item

    item = Item(
        id="item-1",
        producer_scope=("source", "feed"),
        logical_id="logical-1",
        payload={"content": "plain body", "url": "https://example.test/item"},
        metadata={"existing": "kept"},
        content_hash="hash",
        observed_at="2024-01-01T00:00:00Z",
        produced_by=1,
    )

    item_result = project_text(item)
    html_result = project_markdown("<p>body</p>", input_format="html")

    assert item_result.metadata["existing"] == "kept"
    assert item_result.metadata["web"]["source_url"] == "https://example.test/item"
    assert html_result.value == "body"
