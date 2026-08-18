"""Content extraction behavior inspired by the legacy article fetcher."""

from __future__ import annotations

import pytest

from sluice.contracts import NEW
from sluice.errors import ExecutionError
from sluice.tasks.content import extract_content


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
