"""Pure Artifact/Item/string projectors for web content.

Projectors are the only boundary at which a transient ``DocumentArtifact``
becomes a JSON payload.  They deliberately read exactly ``selected_format``;
populated but unselected representations are never guessed or persisted.
"""

from __future__ import annotations

import importlib
import json
import re
import unicodedata
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Comment, NavigableString, Tag
from markdown_it import MarkdownIt

from ..contracts import NEW, Item
from ..errors import ExecutionError
from .contracts import DocumentArtifact
from .sanitize import sanitize_markdown, sanitize_metadata

INPUT_FORMATS = frozenset({"auto", "html", "text", "markdown"})
_HIDDEN_TAGS = (
    "script",
    "style",
    "template",
    "noscript",
    "svg",
    "canvas",
    "head",
    "title",
)
_HIDDEN_TAGS = _HIDDEN_TAGS + ("iframe", "object", "embed")
_HTML_START_TAGS = frozenset(
    {
        "a",
        "abbr",
        "address",
        "article",
        "aside",
        "audio",
        "b",
        "base",
        "bdi",
        "bdo",
        "blockquote",
        "body",
        "br",
        "button",
        "canvas",
        "caption",
        "cite",
        "code",
        "col",
        "colgroup",
        "data",
        "datalist",
        "dd",
        "del",
        "details",
        "dfn",
        "dialog",
        "div",
        "dl",
        "dt",
        "em",
        "embed",
        "fieldset",
        "figcaption",
        "figure",
        "footer",
        "form",
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
        "head",
        "header",
        "hgroup",
        "hr",
        "html",
        "i",
        "iframe",
        "img",
        "input",
        "ins",
        "kbd",
        "label",
        "legend",
        "li",
        "link",
        "main",
        "map",
        "mark",
        "menu",
        "meta",
        "meter",
        "nav",
        "noscript",
        "object",
        "ol",
        "optgroup",
        "option",
        "output",
        "p",
        "picture",
        "pre",
        "progress",
        "q",
        "rp",
        "rt",
        "ruby",
        "s",
        "samp",
        "script",
        "search",
        "section",
        "select",
        "slot",
        "small",
        "source",
        "span",
        "strong",
        "style",
        "sub",
        "summary",
        "sup",
        "table",
        "tbody",
        "td",
        "template",
        "textarea",
        "tfoot",
        "th",
        "thead",
        "time",
        "title",
        "tr",
        "track",
        "u",
        "ul",
        "var",
        "video",
        "wbr",
    }
)
_BLOCK_TAGS = frozenset(
    {
        "address",
        "article",
        "aside",
        "blockquote",
        "body",
        "br",
        "dd",
        "div",
        "dl",
        "dt",
        "figcaption",
        "figure",
        "footer",
        "form",
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
        "header",
        "hr",
        "li",
        "main",
        "nav",
        "ol",
        "p",
        "pre",
        "section",
        "table",
        "td",
        "th",
        "tr",
        "ul",
    }
)
_WORD = re.compile(r"[^\W_]+", re.UNICODE)
_WHITESPACE = re.compile(r"\s+")


@dataclass(frozen=True, slots=True)
class _CoercedInput:
    artifact: DocumentArtifact
    item_metadata: dict[str, Any]
    input_kind: str
    preserve_markup: bool = False


def _normalise_text(value: str) -> str:
    value = unicodedata.normalize("NFC", value.replace("\u00a0", " "))
    lines = [_WHITESPACE.sub(" ", line).strip() for line in value.splitlines()]
    return "\n".join(line for line in lines if line).strip()


def _is_html(value: str) -> bool:
    if not value.strip():
        return False
    # Markdown code spans/fences are literal source, not HTML input.  Remove
    # them before sniffing so an example such as `````html\n<div>...````` stays
    # in the Markdown path.
    candidate = re.sub(
        r"(?P<fence>`{3,}|~{3,})[^\n]*\n.*?(?P=fence)\s*",
        "",
        value,
        flags=re.DOTALL,
    )
    candidate = re.sub(r"`+[^`\n]+`+", "", candidate)
    if not candidate.strip():
        return False
    try:
        soup = BeautifulSoup(candidate, "html.parser")
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        return False
    # BeautifulSoup intentionally accepts arbitrary names as tags.  That is
    # useful for malformed HTML, but it also turns Markdown autolinks such as
    # ``<https://example.test>`` and email/angle syntax into fake tags.  A
    # deterministic HTML sniff therefore requires a standard HTML start-tag
    # name; unknown angle-bracket text remains Markdown.
    if any(
        isinstance(tag, Tag) and tag.name.casefold() in _HTML_START_TAGS
        for tag in soup.find_all(True)
    ):
        return True
    # Custom elements are valid HTML when they have an explicit closing tag,
    # while a lone ``<angle>`` is ordinary Markdown angle syntax.  Requiring
    # the paired close keeps this extension deterministic without reintroducing
    # URI/email autolink false positives.
    for tag in soup.find_all(True):
        if not isinstance(tag, Tag) or not re.fullmatch(
            r"[a-z][a-z0-9-]*", tag.name.casefold()
        ):
            continue
        if re.search(
            rf"</\s*{re.escape(tag.name)}\s*>", candidate, flags=re.IGNORECASE
        ):
            return True
    return False


def _item_content(item: Item) -> tuple[str, Mapping[str, Any]]:
    payload = item.payload
    if isinstance(payload, str):
        return payload, {}
    if isinstance(payload, Mapping):
        content = payload.get("content")
        if isinstance(content, str):
            return content, payload
    raise ExecutionError(
        "web projector Item payload must be a string or object with string content"
    )


def _input_url(payload: Mapping[str, Any]) -> str | None:
    for key in ("url", "link"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return None


def _format_for_input(value: str, input_format: str) -> str:
    if input_format == "auto":
        return "html" if _is_html(value) else "text"
    return input_format


def _coerce(value: DocumentArtifact | Item | str, input_format: str) -> _CoercedInput:
    if input_format not in INPUT_FORMATS:
        raise ExecutionError("input_format must be auto, html, text, or markdown")
    if isinstance(value, DocumentArtifact):
        if input_format != "auto":
            raise ExecutionError("input_format must be auto for a web Artifact")
        if value.selected_format is None:
            raise ExecutionError("web Artifact has no selected_format")
        selected = value.selected_format
        field = {
            "raw_html": value.raw_html,
            "text": value.text,
            "markdown": value.markdown,
        }[selected]
        if not isinstance(field, str):
            raise ExecutionError(
                "web Artifact selected_format has no corresponding value"
            )
        return _CoercedInput(value, {}, selected)

    item_metadata: dict[str, Any] = {}
    payload: Mapping[str, Any] = {}
    if isinstance(value, Item):
        content, payload = _item_content(value)
        item_metadata = dict(value.metadata)
    elif isinstance(value, str):
        content = value
    else:
        raise ExecutionError("web projector input must be an Artifact, Item, or string")

    kind = _format_for_input(content, input_format)
    source_url = _input_url(payload)
    title = payload.get("title") if isinstance(payload.get("title"), str) else None
    fields: dict[str, Any] = {
        "source_url": source_url,
        "final_url": source_url,
        "title": title,
        "content_type": {
            "html": "text/html",
            "text": "text/plain",
            "markdown": "text/markdown",
        }[kind],
        "capabilities": frozenset({"raw_html" if kind == "html" else kind}),
        "selected_format": "raw_html" if kind == "html" else kind,
    }
    if kind == "html":
        fields["raw_html"] = content
    elif kind == "markdown":
        fields["markdown"] = content
    else:
        fields["text"] = content
    artifact = DocumentArtifact(**fields)
    return _CoercedInput(
        artifact,
        item_metadata,
        kind,
        preserve_markup=input_format == "auto" and kind == "text",
    )


def coerce_projector_input(
    value: DocumentArtifact | Item | str, *, input_format: str = "auto"
) -> DocumentArtifact:
    """Coerce supported projector input into a transient Artifact.

    Item metadata is intentionally not returned here; callers that need to
    preserve it use the internal coercion result.  The public helper remains
    useful to Task/runtime adapters that only need the artifact value.
    """

    return _coerce(value, input_format).artifact


def _soup_html(value: str) -> BeautifulSoup:
    try:
        soup = BeautifulSoup(value, "html.parser")
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        raise ExecutionError("web projector received invalid HTML") from None
    for node in list(soup.find_all(_HIDDEN_TAGS)):
        node.decompose()
    for comment in list(soup.find_all(string=lambda item: isinstance(item, Comment))):
        comment.extract()
    return soup


def _visible_html(value: str) -> str:
    soup = _soup_html(value)
    return _normalise_text(soup.get_text("\n", strip=True))


def _safe_markup_url(value: str, base_url: str | None) -> str:
    value = value.strip()
    if base_url and value and not re.match(r"^[A-Za-z][A-Za-z0-9+.-]*:", value):
        value = urljoin(base_url, value)
    # sanitize_markdown performs the final normalized URL policy; keeping the
    # source here lets it remove unsafe destinations without HTML interpolation.
    return value


def _escape_html_text(value: str) -> str:
    """Keep HTML text nodes literal when emitted into Markdown."""

    value = value.replace("\\", "\\\\")
    value = re.sub(r"([`*_{}\[\]()#+.!|~\-])", r"\\\1", value)
    return value.replace("<", "&lt;").replace(">", "&gt;")


def _own_html_to_markdown(value: str, base_url: str | None) -> str:
    soup = _soup_html(value)
    root = soup.body if soup.body is not None else soup
    parts: list[str] = []

    def append(text: str) -> None:
        if text:
            parts.append(text)

    def newline() -> None:
        if parts and not parts[-1].endswith("\n"):
            parts.append("\n")

    def walk(node: Any, *, in_pre: bool = False) -> None:
        if isinstance(node, NavigableString):
            text = str(node)
            if not in_pre:
                text = _WHITESPACE.sub(" ", text)
                text = _escape_html_text(text)
            append(text)
            return
        if not isinstance(node, Tag):
            return
        tag = node.name.casefold()
        if tag in _HIDDEN_TAGS:
            return
        if tag in _BLOCK_TAGS:
            newline()
        if tag in {"h1", "h2", "h3", "h4", "h5", "h6"}:
            append("#" * int(tag[1]) + " ")
        elif tag == "li":
            append("- ")
        elif tag in {"strong", "b"}:
            append("**")
        elif tag in {"em", "i"}:
            append("_")
        elif tag == "code" and node.parent is not None and node.parent.name != "pre":
            append("`")
        elif tag == "a":
            append("[")
        elif tag == "img":
            alt = str(node.get("alt", ""))
            src = _safe_markup_url(str(node.get("src", "")), base_url)
            append(f"![{alt}]({src})" if src else alt)
            return
        elif tag == "br":
            newline()
        elif tag == "hr":
            append("---")
        elif tag == "pre":
            newline()
            append("```")
            newline()
        for child in node.children:
            walk(child, in_pre=in_pre or tag == "pre")
        if tag == "a":
            href = _safe_markup_url(str(node.get("href", "")), base_url)
            append(f"]({href})" if href else "]")
        elif tag in {"strong", "b"}:
            append("**")
        elif tag in {"em", "i"}:
            append("_")
        elif tag == "code" and node.parent is not None and node.parent.name != "pre":
            append("`")
        elif tag == "pre":
            newline()
            append("```")
        if tag in _BLOCK_TAGS:
            newline()

    walk(root)
    result = "".join(parts)
    lines = [line.rstrip() for line in result.splitlines()]
    compact: list[str] = []
    blank = False
    for line in lines:
        if not line.strip():
            if compact and not blank:
                compact.append("")
            blank = True
            continue
        compact.append(line.strip())
        blank = False
    while compact and compact[-1] == "":
        compact.pop()
    return "\n\n".join(compact)


def _converter_html(value: str) -> str:
    """Serialize cleaned HTML with literal text safe for Markdown conversion."""

    soup = _soup_html(value)
    for node in list(soup.find_all(string=True)):
        parent = node.parent
        if isinstance(parent, Tag) and parent.name.casefold() in {"pre", "code"}:
            continue
        node.replace_with(_escape_html_text(str(node)))
    return str(soup)


def _html_to_markdown(value: str, base_url: str | None) -> str:
    # html-to-markdown is a declared core dependency in normal installations.
    # Keep a deterministic local implementation for minimal/offline installs
    # and for tests that intentionally omit optional packaging extras.
    try:
        module = importlib.import_module("html_to_markdown")
    except (ImportError, ModuleNotFoundError):
        module = None
    converter = getattr(module, "convert", None) if module is not None else None
    # Normalize through the same hidden-node policy used by visible-text
    # projection before handing HTML to an optional converter.  Otherwise a
    # converter may retain fallback text nested in iframe/object/embed nodes
    # even though those nodes are non-visible content for this boundary.
    cleaned_value = _converter_html(value)
    if callable(converter):
        try:
            try:
                converted = converter(cleaned_value, allow_raw_html=False)
            except TypeError:
                converted = converter(cleaned_value)
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            raise ExecutionError("web Markdown conversion failed") from None
        if not isinstance(converted, str):
            raise ExecutionError("web Markdown conversion failed")
        result = converted
    else:
        result = _own_html_to_markdown(value, base_url)
    try:
        return sanitize_markdown(result, base_url).strip()
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        raise ExecutionError("web Markdown sanitization failed") from None


def _escape_plain_markdown(value: str) -> str:
    # Keep authored plain text literal in a Markdown channel.  Backslash is
    # escaped first so the operation is idempotent for already-safe values.
    value = value.replace("\\", "\\\\")
    return re.sub(r"([`*_{}\[\]()#+.!|<>~\-])", r"\\\1", value).strip()


def _markdown_to_text(value: str, base_url: str | None) -> str:
    try:
        value = sanitize_markdown(value, base_url)
        tokens = MarkdownIt("commonmark", {"html": True, "linkify": False}).parse(value)
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        raise ExecutionError("web Markdown rendering failed") from None
    parts: list[str] = []

    def newline() -> None:
        if parts and not parts[-1].endswith("\n"):
            parts.append("\n")

    for token in tokens:
        if token.type == "inline":
            for child in token.children or ():
                if child.type in {"text", "code_inline", "html_inline", "image"}:
                    parts.append(child.content)
                elif child.type in {"softbreak", "hardbreak"}:
                    newline()
            newline()
        elif token.type in {"fence", "code_block"}:
            parts.append(token.content)
            newline()
    return _normalise_text("".join(parts))


def _selected_value(artifact: DocumentArtifact) -> str:
    selected = artifact.selected_format
    if selected == "raw_html":
        value = artifact.raw_html
    elif selected == "text":
        value = artifact.text
    elif selected == "markdown":
        value = artifact.markdown
    else:
        raise ExecutionError("web Artifact has no selected_format")
    if not isinstance(value, str):
        raise ExecutionError("web Artifact selected_format has no corresponding value")
    return value


def _meta_content(tag: Tag) -> str | None:
    value = tag.get("content")
    return value if isinstance(value, str) else None


def _html_metadata(value: str, base_url: str | None) -> dict[str, Any]:
    try:
        # Keep head/title/JSON-LD nodes long enough to read metadata.  The
        # visible-content helper removes those nodes before rendering text.
        soup = BeautifulSoup(value, "html.parser")
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        raise ExecutionError("web projector received invalid HTML") from None
    for comment in list(soup.find_all(string=lambda item: isinstance(item, Comment))):
        comment.extract()
    result: dict[str, Any] = {}
    title = soup.find("title")
    if isinstance(title, Tag):
        text = _normalise_text(title.get_text(" ", strip=True))
        if text:
            result["title"] = text
    open_graph: dict[str, str] = {}
    twitter: dict[str, str] = {}
    authors: list[str] = []
    published: str | None = None
    for tag in soup.find_all("meta"):
        key = tag.get("property") or tag.get("name")
        content = _meta_content(tag)
        if not isinstance(key, str) or content is None:
            continue
        folded = key.casefold().strip()
        if folded.startswith("og:"):
            open_graph[folded[3:]] = content
        elif folded.startswith("twitter:"):
            twitter[folded[8:]] = content
        elif folded in {"author", "article:author", "byline"}:
            authors.append(content)
        elif folded in {"article:published_time", "datepublished", "date"}:
            published = published or content
    if open_graph:
        result["open_graph"] = open_graph
    if twitter:
        result["twitter_card"] = twitter

    links: list[dict[str, str]] = []
    visible_soup = _soup_html(value)
    for anchor in visible_soup.find_all("a"):
        href = anchor.get("href")
        if not isinstance(href, str) or not href.strip():
            continue
        text = _normalise_text(anchor.get_text(" ", strip=True))
        links.append(
            {"text": text, "url": urljoin(base_url, href) if base_url else href}
        )
    if links:
        result["links"] = links

    json_ld: list[Any] = []
    for script in soup.find_all("script"):
        typ = str(script.get("type", "")).casefold()
        if typ != "application/ld+json":
            continue
        try:
            parsed = json.loads(script.string or script.get_text())
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        values = parsed if isinstance(parsed, list) else [parsed]
        json_ld.extend(values)
        if isinstance(parsed, Mapping):
            author = parsed.get("author")
            author_values = author if isinstance(author, list) else [author]
            for item in author_values:
                if isinstance(item, Mapping):
                    item = item.get("name")
                if isinstance(item, str):
                    authors.append(item)
            candidate_date = parsed.get("datePublished")
            if isinstance(candidate_date, str):
                published = published or candidate_date
    if json_ld:
        result["json_ld"] = json_ld
    if authors:
        result["authors"] = authors
    if published:
        result["published_at"] = published
    return result


def _markdown_metadata(value: str, base_url: str | None) -> dict[str, Any]:
    try:
        tokens = MarkdownIt("commonmark", {"html": True, "linkify": False}).parse(value)
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        return {}
    links: list[dict[str, str]] = []
    for token in tokens:
        if token.type != "inline":
            continue
        open_link: dict[str, str] | None = None
        for child in token.children or ():
            if child.type == "link_open":
                destination = child.attrGet("href")
                open_link = {
                    "text": "",
                    "url": destination if isinstance(destination, str) else "",
                }
            elif child.type == "link_close":
                if open_link is not None and open_link["url"]:
                    open_link["text"] = _normalise_text(open_link["text"])
                    if open_link["text"]:
                        destination = open_link["url"]
                        open_link["url"] = (
                            destination
                            if destination.startswith("#")
                            else urljoin(base_url, destination)
                            if base_url
                            else destination
                        )
                        links.append(open_link)
                open_link = None
            elif open_link is not None and child.type in {
                "text",
                "code_inline",
                "softbreak",
                "hardbreak",
            }:
                open_link["text"] += (
                    " " if child.type in {"softbreak", "hardbreak"} else child.content
                )
    if not links:
        return {}
    return {"links": links}


def _date_iso(value: Any) -> Any:
    if isinstance(value, datetime):
        parsed = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
        return (
            parsed.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
        )
    return value


def _word_count(value: str) -> int:
    return len(_WORD.findall(value))


def _extractor_label(artifact: DocumentArtifact, kind: str, output: str) -> str:
    if artifact.extraction not in {None, "", "input"}:
        return str(artifact.extraction)
    if kind in {"html", "raw_html"}:
        return "html_to_markdown" if output == "markdown" else "text_normalizer"
    if kind == "markdown":
        return "identity"
    return "text_normalizer"


def _metadata_web(
    artifact: DocumentArtifact,
    *,
    kind: str,
    output: str,
    rendered: str,
    source_metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    base_url = artifact.final_url or artifact.source_url
    raw: dict[str, Any] = dict(artifact.metadata)
    if isinstance(raw.get("web"), Mapping):
        nested = dict(raw["web"])
        nested.update({key: value for key, value in raw.items() if key != "web"})
        raw = nested

    # Metadata parsed from the selected representation fills absent fields;
    # explicit provider fields and Artifact provenance win deterministically.
    if source_metadata:
        for key, value in source_metadata.items():
            if key not in raw or raw[key] in (None, "", {}, []):
                raw[key] = value
    if artifact.source_url is not None:
        raw["source_url"] = artifact.source_url
    if artifact.final_url is not None:
        raw["final_url"] = artifact.final_url
    if artifact.title is not None:
        raw["title"] = artifact.title
    if artifact.provider_instance is not None:
        raw["provider_instance"] = artifact.provider_instance
    if artifact.provider_type is not None:
        raw["provider_type"] = artifact.provider_type
    raw["extractor"] = _extractor_label(artifact, kind, output)
    raw["output_format"] = output
    raw["quality_chars"] = len(rendered)
    raw["quality_words"] = _word_count(rendered)
    if "published_at" in raw:
        raw["published_at"] = _date_iso(raw["published_at"])
    # ``quality`` can carry useful scalar diagnostics, but visible_text is
    # intentionally excluded from this durable projection.
    quality = artifact.quality
    if isinstance(quality, Mapping):
        for key in ("quality_chars", "quality_words"):
            if key not in raw and isinstance(quality.get(key), int):
                raw[key] = quality[key]
    elif quality is not None:
        for key in ("quality_chars", "quality_words"):
            if key not in raw:
                candidate = getattr(quality, key, None)
                if isinstance(candidate, int):
                    raw[key] = candidate

    return sanitize_metadata(raw, base_url)


def _project(
    input_value: DocumentArtifact | Item | str, *, input_format: str, output: str
) -> NEW:
    coerced = _coerce(input_value, input_format)
    artifact = coerced.artifact
    selected = artifact.selected_format
    value = _selected_value(artifact)
    base_url = artifact.final_url or artifact.source_url
    source_metadata: dict[str, Any] = {}

    if selected == "raw_html":
        source_metadata = _html_metadata(value, base_url)
        rendered = (
            _html_to_markdown(value, base_url)
            if output == "markdown"
            else _visible_html(value)
        )
    elif selected == "markdown":
        sanitized = sanitize_markdown(value, base_url)
        source_metadata = _markdown_metadata(sanitized, base_url)
        rendered = (
            sanitized.strip()
            if output == "markdown"
            else _markdown_to_text(sanitized, base_url)
        )
    else:
        if output == "markdown":
            rendered = (
                value if coerced.preserve_markup else _escape_plain_markdown(value)
            )
        else:
            rendered = _normalise_text(value)

    web_metadata = _metadata_web(
        artifact,
        kind=coerced.input_kind,
        output=output,
        rendered=_markdown_to_text(rendered, base_url)
        if output == "markdown"
        else rendered,
        source_metadata=source_metadata,
    )
    metadata = dict(coerced.item_metadata)
    metadata["web"] = web_metadata
    return NEW(rendered, metadata=metadata)


def project_markdown(
    input: DocumentArtifact | Item | str, *, input_format: str = "auto"
) -> NEW:
    """Project an Artifact, Item, or string into sanitized Markdown."""

    return _project(input, input_format=input_format, output="markdown")


def project_text(
    input: DocumentArtifact | Item | str, *, input_format: str = "auto"
) -> NEW:
    """Project an Artifact, Item, or string into normalized visible text."""

    return _project(input, input_format=input_format, output="text")


__all__ = [
    "INPUT_FORMATS",
    "coerce_projector_input",
    "project_markdown",
    "project_text",
]
