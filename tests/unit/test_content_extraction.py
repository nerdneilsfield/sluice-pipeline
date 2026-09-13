"""Content extraction behavior inspired by the legacy article fetcher."""

from __future__ import annotations

import pytest

from sluice.contracts import NEW
from sluice.errors import ExecutionError
from sluice.tasks.content import extract_content, fetch_webpage, to_text
from sluice.web.contracts import DocumentArtifact
from sluice.web.extractors import (
    EXTRACTOR_FORMATS,
    NO_MATCH,
    beautifulsoup,
    extract_beautifulsoup,
    extract_identity,
    extract_newspaper4k,
)
from sluice.web.projectors import coerce_projector_input, project_markdown, project_text


def test_content_task_bindings_project_artifacts_and_use_private_client() -> None:
    from sluice.web.contracts import PrivateWebInvocation, WebBinding

    artifact = DocumentArtifact(
        source_url="https://example.test/article",
        final_url="https://example.test/article",
        content_type="text/markdown",
        markdown="# Article\n\nBody",
        selected_format="markdown",
        capabilities=frozenset({"markdown"}),
    )

    assert to_text.run({}, artifact).value == "Article\nBody"

    class Client:
        invocation: PrivateWebInvocation | None = None

        def run_web(self, invocation: PrivateWebInvocation, input: object) -> object:
            self.invocation = invocation
            assert input == "https://example.test/article"
            return NEW(artifact)

    task = fetch_webpage.bind(
        type("Adapters", (), {"http_client": None, "web_content_client": Client()})()
    )
    result = task.run_web(
        PrivateWebInvocation(
            {"fetch_chain": "robust", "extract_chain": "article"},
            WebBinding(output="artifact"),
        ),
        "https://example.test/article",
    )

    assert result == NEW(artifact)
    assert task.web_content_client.invocation is not None


def test_extract_content_clears_identity_selection_before_a_new_digest(monkeypatch) -> None:
    from dataclasses import replace

    from sluice.web.contracts import ExtractorBinding, PrivateWebInvocation, QualityPolicy, WebBinding

    artifact = DocumentArtifact(
        markdown="Provider-origin markdown",
        selected_format="markdown",
        extraction="identity",
        extract_binding_digest="old-digest",
        quality={"passed": True},
        content_type="text/markdown",
        capabilities=frozenset({"markdown"}),
    )
    binding = WebBinding(
        extract_chain=(ExtractorBinding(name="identity", type="identity"),),
        extract_chain_name="replacement",
        quality=QualityPolicy(min_chars=1),
        output="artifact",
    )
    received: list[DocumentArtifact] = []

    class Identity:
        def extract(self, candidate: DocumentArtifact) -> DocumentArtifact:
            received.append(candidate)
            return replace(candidate, selected_format="markdown")

    monkeypatch.setattr("sluice.tasks.content.build_extractor", lambda _: Identity())

    result = extract_content.run_web(
        PrivateWebInvocation({"extract_chain": "replacement"}, binding), artifact
    )

    assert isinstance(result, NEW)
    assert received[0].markdown == "Provider-origin markdown"
    assert received[0].selected_format is None
    assert received[0].quality is None
    assert received[0].extract_binding_digest is None


def test_extract_content_reruns_a_different_artifact_digest() -> None:
    from sluice.web.contracts import ExtractorBinding, PrivateWebInvocation, QualityPolicy, WebBinding

    artifact = DocumentArtifact(
        raw_html="<article><p>Fresh body</p></article>",
        text="stale extraction",
        selected_format="text",
        extraction="beautifulsoup",
        extract_binding_digest="old-digest",
        content_type="text/html",
        capabilities=frozenset({"raw_html", "text"}),
    )
    binding = WebBinding(
        extract_chain=(
            ExtractorBinding(name="identity", type="identity"),
            ExtractorBinding(name="fallback", type="beautifulsoup"),
        ),
        extract_chain_name="article",
        quality=QualityPolicy(min_chars=1),
        output="artifact",
    )

    result = extract_content.run_web(
        PrivateWebInvocation({"extract_chain": "article"}, binding), artifact
    )

    assert isinstance(result, NEW)
    assert isinstance(result.value, DocumentArtifact)
    assert result.value.text == "Fresh body"
    assert result.value.extract_binding_digest != "old-digest"


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


def test_beautifulsoup_plain_mode_excludes_head_and_title_text() -> None:
    artifact = DocumentArtifact(
        raw_html="<head><title>Hidden title</title><meta name='description' content='hidden'></head>"
        "<body><article>Visible body</article></body>",
        content_type="text/html",
        capabilities=frozenset({"raw_html"}),
    )

    result = extract_beautifulsoup(artifact, {"mode": "plain"})

    assert result.text == "Visible body"
    assert result.title == "Hidden title"


def test_newspaper4k_parses_supplied_html_without_network_or_image_fetch(monkeypatch) -> None:
    import types

    from sluice.web import extractors as extractor_module

    image_fetches: list[str] = []

    class FakeArticle:
        def __init__(self, url: str, **kwargs) -> None:
            self.config = types.SimpleNamespace(fetch_images=kwargs.get("fetch_images", True))
            self.fetch_images = lambda: image_fetches.append("fetch")
            self.text = ""
            self.title = ""
            self.authors = []
            self.publish_date = None

        def download(self, *, input_html: str | None = None, ignore_read_more: bool = False):
            assert input_html is not None
            assert ignore_read_more is True
            self.html = input_html
            return self

        def parse(self):
            # newspaper4k 0.9.3 invokes this method even for cached HTML.
            self.fetch_images()
            self.text = "Offline body"
            self.title = "Offline title"
            return self

    fake_module = types.SimpleNamespace(
        article=types.SimpleNamespace(Article=FakeArticle),
    )
    monkeypatch.setattr(extractor_module, "newspaper", fake_module)

    artifact = DocumentArtifact(
        raw_html="<p>Offline body</p><img src='https://example.test/image.jpg'>",
        content_type="text/html",
        capabilities=frozenset({"raw_html"}),
    )

    result = extract_newspaper4k(artifact)

    assert result.text == "Offline body"
    assert result.title == "Offline title"
    assert image_fetches == []


def test_auto_projector_keeps_markdown_autolink_out_of_html_converter() -> None:
    source = "Read <https://example.test/article> or <user@example.com>"

    artifact = coerce_projector_input(source)
    result = project_markdown(source)

    assert artifact.selected_format == "text"
    assert result.value == source
    assert result.metadata["web"]["extractor"] == "text_normalizer"


def test_auto_projector_preserves_markdown_syntax_for_non_html_input() -> None:
    source = "# Title\n\n*bold*"

    markdown = project_markdown(source)
    text = project_text(source)

    assert markdown.value == source
    assert text.value == "# Title\n*bold*"
    assert text.metadata["web"]["extractor"] == "text_normalizer"

    explicit_markdown = project_text(source, input_format="markdown")
    assert explicit_markdown.value == "Title\nbold"


def test_markdown_metadata_ignores_links_inside_code_fences() -> None:
    source = "```md\n[example](https://code.example/)\n```\n[real](https://real.example/)"

    result = project_markdown(source, input_format="markdown")

    assert result.metadata["web"]["links"] == (
        {"text": "real", "url": "https://real.example/"},
    )


def test_markdown_metadata_ignores_inline_code_and_raw_html_links() -> None:
    source = (
        "`[code](https://example.test/secret)`\n\n"
        "<script>[script](https://example.test/script)</script>\n\n"
        "[real](https://example.test/real) [mail](mailto:user@example.com) [frag](#part)"
    )

    result = project_markdown(source, input_format="markdown")

    assert result.metadata["web"]["links"] == (
        {"text": "real", "url": "https://example.test/real"},
        {"text": "mail", "url": "mailto:user@example.com"},
        {"text": "frag", "url": "#part"},
    )


def test_markdown_metadata_drops_links_from_embedded_raw_html() -> None:
    result = project_markdown(
        "<object>[secret](https://secret.example)</object>\n\nvisible",
        input_format="markdown",
    )

    assert result.metadata["web"]["links"] == ()


def test_markdown_metadata_keeps_mailto_and_fragment_destinations() -> None:
    artifact = DocumentArtifact(
        markdown="[mail](mailto:user@example.com) [frag](#part)",
        selected_format="markdown",
        content_type="text/markdown",
        capabilities=frozenset({"markdown"}),
        source_url="https://example.test/page",
        final_url="https://example.test/page",
    )

    links = project_markdown(artifact).metadata["web"]["links"]

    assert links == (
        {"text": "mail", "url": "mailto:user@example.com"},
        {"text": "frag", "url": "#part"},
    )


def test_markdown_autolinks_remain_markdown_and_metadata_links() -> None:
    source = "<https://example.test/article> <user@example.com>"

    result = project_markdown(source, input_format="markdown")

    assert result.value == source
    assert result.metadata["web"]["links"] == (
        {"text": "https://example.test/article", "url": "https://example.test/article"},
        {"text": "user@example.com", "url": "mailto:user@example.com"},
    )


def test_auto_text_projection_normalizes_whitespace_without_rendering_markup() -> None:
    source = "  **bold**  \n x "

    result = project_text(source)

    assert result.value == "**bold**\nx"
    assert project_markdown(source).value == source


def test_html_projector_drops_non_visible_embedded_content() -> None:
    source = (
        "<p>Visible</p><iframe>iframe secret</iframe>"
        "<object>object secret</object><embed src='movie.swf'>"
    )

    assert project_markdown(source, input_format="html").value == "Visible"
    assert project_text(source, input_format="html").value == "Visible"


def test_html_metadata_ignores_links_inside_embedded_content() -> None:
    source = "<p>Visible</p><object><a href='https://secret.example'>secret</a></object>"

    result = project_markdown(source, input_format="html")

    assert result.value == "Visible"
    assert result.metadata["web"]["links"] == ()


def test_html_projector_unwraps_nested_parenthesis_unsafe_destinations() -> None:
    result = project_markdown(
        '<p><a href="javascript:alert(1)">Text</a>'
        '<img src="javascript:alert(1)" alt="Alt"></p>',
        input_format="html",
    )

    assert result.value == "TextAlt"
    assert not result.value.endswith(")")


def test_html_fallback_escapes_literal_text_nodes_for_markdown() -> None:
    assert project_markdown("<p>&lt;tag&gt; &amp; text</p>", input_format="html").value == (
        "&lt;tag&gt; & text"
    )
    assert project_markdown("<p>*literal*</p>", input_format="html").value == r"\*literal\*"


def test_identity_requires_provider_capability_but_accepts_explicit_local_input() -> None:
    provider = DocumentArtifact(
        markdown="# Provider body",
        content_type="text/markdown",
        capabilities=frozenset(),
        provider_instance="reader",
        provider_type="jina_reader",
        extraction="provider",
    )
    advertised = DocumentArtifact(
        markdown="# Advertised body",
        content_type="text/markdown",
        capabilities=frozenset({"markdown"}),
        provider_instance="reader",
        provider_type="jina_reader",
        extraction="provider",
    )
    local = DocumentArtifact(
        markdown="# Local body",
        content_type="text/markdown",
        capabilities=frozenset(),
        extraction="input",
    )
    stale = DocumentArtifact(
        markdown="# Stale body",
        content_type="text/markdown",
        capabilities=frozenset(),
        extraction=None,
    )
    local_raw_html = DocumentArtifact(
        raw_html="<p>Original HTML</p>",
        markdown="# Stale Markdown",
        selected_format="raw_html",
        content_type="text/html",
        capabilities=frozenset(),
        extraction="input",
    )

    assert extract_identity(provider) is NO_MATCH
    assert extract_identity(advertised).markdown == "# Advertised body"
    assert extract_identity(local).markdown == "# Local body"
    assert extract_identity(stale) is NO_MATCH
    assert extract_identity(local_raw_html) is NO_MATCH


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
