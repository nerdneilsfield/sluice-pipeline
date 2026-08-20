"""Deterministic, local-only web content extractors.

The web coordinator owns provider fallback and quality selection.  This module
only transforms representations already present on a :class:`DocumentArtifact`.
In particular, no extractor fetches a URL; newspaper4k's compatibility
``download(input_html=...)`` call only loads the supplied body in memory.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import datetime, timezone
import importlib
import json
import re
import unicodedata
from typing import Any, Protocol

from bs4 import BeautifulSoup, Comment, NavigableString, Tag

from ..errors import ExecutionError, WebSafetyError
from .contracts import DocumentArtifact, ExtractorBinding, NO_MATCH
from .sanitize import sanitize_markdown


EXTRACTOR_FORMATS: dict[str, str] = {
    "trafilatura": "markdown",
    "newspaper4k": "text",
    "resiliparse": "text",
    "beautifulsoup": "text",
}

_HIDDEN_TAGS = (
    "script",
    "style",
    "template",
    "noscript",
    "svg",
    "canvas",
    "head",
    "title",
    "iframe",
    "object",
    "embed",
)
_BLOCK_TAGS = frozenset(
    {
        "address",
        "article",
        "aside",
        "blockquote",
        "body",
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
_BOILERPLATE_HINTS = frozenset(
    {
        "advert",
        "breadcrumb",
        "comment",
        "cookie",
        "footer",
        "header",
        "login",
        "menu",
        "nav",
        "newsletter",
        "popup",
        "related",
        "share",
        "sidebar",
        "social",
        "subscribe",
    }
)
_CONTENT_HINTS = frozenset({"article", "content", "entry", "main", "post", "primary"})
_WHITESPACE = re.compile(r"\s+")


class WebExtractor(Protocol):
    """Protocol consumed by the web coordinator's local extractor chain."""

    name: str

    def extract(self, artifact: DocumentArtifact) -> DocumentArtifact | object:
        raise NotImplementedError


def _normalise_text(value: str) -> str:
    value = unicodedata.normalize("NFC", value.replace("\u00a0", " "))
    lines = [_WHITESPACE.sub(" ", line).strip() for line in value.splitlines()]
    return "\n".join(line for line in lines if line).strip()


def _failure() -> WebSafetyError:
    # Keep extraction failures in the stable web vocabulary.  The coordinator
    # and sanitizers never need to expose an optional library's exception text.
    return WebSafetyError("extract_failed")


def _with_result(
    artifact: DocumentArtifact,
    *,
    selected_format: str,
    extraction: str,
    value: str,
    title: str | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> DocumentArtifact:
    fields: dict[str, Any] = {
        "selected_format": selected_format,
        "extraction": extraction,
    }
    if selected_format == "markdown":
        fields["markdown"] = value
    elif selected_format == "text":
        fields["text"] = value
    if title is not None and title.strip():
        fields["title"] = _normalise_text(title)
    if metadata is not None:
        merged = dict(artifact.metadata)
        merged.update(dict(metadata))
        fields["metadata"] = merged
    capabilities = set(artifact.capabilities)
    capabilities.add(selected_format)
    fields["capabilities"] = frozenset(capabilities)
    return replace(artifact, **fields)


def _provider_original(artifact: DocumentArtifact) -> bool:
    """Whether the candidate still carries provider-original data.

    The coordinator clears non-identity output before a chain with a new
    digest.  The extraction marker makes this helper defensive when an
    extractor is called directly in a unit test.
    """

    return artifact.extraction in {
        None,
        "",
        "input",
        "provider",
        "provider_original",
        "identity",
    }


def _identity_representation_allowed(artifact: DocumentArtifact, candidate: str) -> bool:
    """Check provenance before identity reuses a representation.

    Provider artifacts must explicitly advertise every representation they
    expose.  Direct/local candidates have no provider identity, so the
    explicit ``input`` extraction marker is the provenance that permits an
    empty capability set; if capabilities are present they still constrain
    the field so stale values cannot leak across formats.
    """

    if (
        artifact.extraction == "input"
        and artifact.provider_instance is None
        and artifact.provider_type is None
    ):
        if artifact.selected_format == "raw_html":
            return False
        if artifact.selected_format in {"markdown", "text"}:
            return candidate == artifact.selected_format
        return not artifact.capabilities or candidate in artifact.capabilities
    return candidate in artifact.capabilities


def _identity_extract(artifact: DocumentArtifact) -> DocumentArtifact | object:
    if not isinstance(artifact, DocumentArtifact) or not _provider_original(artifact):
        return NO_MATCH

    # Preserve an already selected provider-original representation when one
    # is present; otherwise Markdown is preferred over text.  Callers that
    # need text-only identity should omit Markdown from the artifact.
    selected = artifact.selected_format
    candidates = ("markdown", "text")
    if selected in {"markdown", "text"}:
        candidates = (selected, "text" if selected == "markdown" else "markdown")
    for candidate in candidates:
        if not _identity_representation_allowed(artifact, candidate):
            continue
        if candidate == "markdown" and isinstance(artifact.markdown, str) and artifact.markdown.strip():
            try:
                value = sanitize_markdown(artifact.markdown, artifact.final_url or artifact.source_url)
            except Exception:
                raise _failure() from None
            if not value.strip():
                return NO_MATCH
            return _with_result(
                artifact,
                selected_format="markdown",
                extraction="identity",
                value=value,
            )
        if candidate == "text" and isinstance(artifact.text, str) and artifact.text.strip():
            return _with_result(
                artifact,
                selected_format="text",
                extraction="identity",
                value=_normalise_text(artifact.text),
            )
    return NO_MATCH


def _soup_for_html(html: str) -> BeautifulSoup:
    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception:
        raise _failure() from None
    for node in list(soup.find_all(_HIDDEN_TAGS)):
        node.decompose()
    for comment in list(soup.find_all(string=lambda value: isinstance(value, Comment))):
        comment.extract()
    return soup


def _remove_selectors(soup: BeautifulSoup, selectors: tuple[str, ...]) -> None:
    for selector in selectors:
        try:
            matches = soup.select(selector)
        except Exception:
            raise _failure() from None
        for node in matches:
            node.decompose()


def _hint(node: Tag) -> str:
    values: list[str] = [node.name.casefold()]
    for key in ("id", "class", "role"):
        value = node.get(key)
        if isinstance(value, (list, tuple)):
            values.extend(str(item).casefold() for item in value)
        elif isinstance(value, str):
            values.append(value.casefold())
    return " ".join(values)


def _node_text(node: Tag) -> str:
    # A newline separator keeps block boundaries while BeautifulSoup decodes
    # entities and normalizes malformed HTML deterministically.
    return _normalise_text(node.get_text("\n", strip=True))


def _article_root(soup: BeautifulSoup, selector: str | None) -> Tag | BeautifulSoup | None:
    if selector:
        try:
            selected = soup.select_one(selector)
        except Exception:
            raise _failure() from None
        if selected is None:
            return None
        return selected

    for tag in ("article", "main"):
        candidate = soup.find(tag)
        if isinstance(candidate, Tag) and _node_text(candidate):
            return candidate

    candidates: list[tuple[float, Tag]] = []
    for candidate in soup.find_all(("section", "div", "body")):
        if not isinstance(candidate, Tag):
            continue
        text = _node_text(candidate)
        if not text:
            continue
        hint = _hint(candidate)
        if any(word in hint for word in _BOILERPLATE_HINTS):
            penalty = len(text) * 2.0
        else:
            penalty = 0.0
        bonus = 0.0
        if candidate.name == "body":
            bonus += 100.0
        if any(word in hint for word in _CONTENT_HINTS):
            bonus += 1_500.0
        if candidate.name == "section":
            bonus += 200.0
        candidates.append((len(text) + bonus - penalty, candidate))
    if candidates:
        return max(candidates, key=lambda pair: pair[0])[1]
    return soup


def _beautifulsoup_text(
    html: str,
    *,
    mode: str = "article",
    content_selector: str | None = None,
    remove_selectors: tuple[str, ...] = (),
) -> tuple[str, str | None]:
    # Keep the document title as extractor provenance, but never let head or
    # title text enter the visible plain/article representation.  Selector
    # removal applies to metadata as well as body content, matching the
    # previous behavior where a caller could explicitly remove ``title``.
    try:
        title_soup = BeautifulSoup(html, "html.parser")
    except Exception:
        raise _failure() from None
    _remove_selectors(title_soup, remove_selectors)
    title_node = title_soup.find("title")
    title = _normalise_text(title_node.get_text(" ", strip=True)) if isinstance(title_node, Tag) else None

    soup = _soup_for_html(html)
    _remove_selectors(soup, remove_selectors)
    root: Tag | BeautifulSoup | None
    if mode == "plain":
        root = soup
    else:
        root = _article_root(soup, content_selector)
    if root is None:
        text = ""
    else:
        text = _node_text(root) if isinstance(root, Tag) else _normalise_text(root.get_text("\n", strip=True))
    return text, title


def _extract_beautifulsoup(artifact: DocumentArtifact, options: Mapping[str, Any]) -> DocumentArtifact | object:
    if not isinstance(artifact.raw_html, str) or not artifact.raw_html.strip():
        return NO_MATCH
    mode = options.get("mode", "article")
    if not isinstance(mode, str) or mode not in {"article", "plain"}:
        raise ExecutionError("beautifulsoup.mode must be article or plain")
    selector = options.get("content_selector")
    if selector is not None and (not isinstance(selector, str) or not selector.strip()):
        raise ExecutionError("beautifulsoup.content_selector must be a non-empty string")
    raw_remove = options.get("remove_selectors", ())
    if not isinstance(raw_remove, (list, tuple)) or any(
        not isinstance(item, str) or not item.strip() for item in raw_remove
    ):
        raise ExecutionError("beautifulsoup.remove_selectors must be strings")
    text, title = _beautifulsoup_text(
        artifact.raw_html,
        mode=mode,
        content_selector=selector,
        remove_selectors=tuple(raw_remove),
    )
    if not text:
        return NO_MATCH
    return _with_result(artifact, selected_format="text", extraction="beautifulsoup", value=text, title=title)


def _load_optional(module_name: str) -> Any | None:
    try:
        return importlib.import_module(module_name)
    except (ImportError, ModuleNotFoundError):
        return None


# Exposed module references make optional integrations easy to inject in tests
# while keeping imports lazy for the core installation.
trafilatura = _load_optional("trafilatura")
newspaper = _load_optional("newspaper")
resiliparse = _load_optional("resiliparse")


def _extract_trafilatura(artifact: DocumentArtifact, options: Mapping[str, Any]) -> DocumentArtifact | object:
    if not isinstance(artifact.raw_html, str) or not artifact.raw_html.strip():
        return NO_MATCH
    module = trafilatura
    if module is None or not callable(getattr(module, "extract", None)):
        return NO_MATCH
    kwargs = {
        "include_tables": bool(options.get("include_tables", True)),
        "include_comments": bool(options.get("include_comments", False)),
        "favor_precision": bool(options.get("favor_precision", False)),
        "favor_recall": bool(options.get("favor_recall", False)),
        "output_format": "markdown",
    }
    try:
        # Crucially this is the supplied body, never a URL/fetch_url call.
        value = module.extract(artifact.raw_html, **kwargs)
    except Exception:
        raise _failure() from None
    if not isinstance(value, str) or not value.strip():
        return NO_MATCH
    try:
        value = sanitize_markdown(value, artifact.final_url or artifact.source_url)
    except Exception:
        raise _failure() from None
    if not value.strip():
        return NO_MATCH
    return _with_result(artifact, selected_format="markdown", extraction="trafilatura", value=value)


def _newspaper_article(module: Any, language: str | None) -> Any | None:
    article_cls = getattr(getattr(module, "article", None), "Article", None)
    if article_cls is None:
        article_cls = getattr(module, "Article", None)
    if article_cls is None:
        return None
    # Both options are important when parsing caller-supplied HTML: older
    # newspaper releases may follow a read-more/meta-refresh URL from
    # ``download`` even though no network fetch was requested explicitly.
    kwargs: dict[str, Any] = {"fetch_images": False, "follow_meta_refresh": False}
    if language:
        kwargs["language"] = language
    try:
        return article_cls(url="", **kwargs)
    except TypeError:
        try:
            # Older releases may reject follow_meta_refresh while still
            # accepting the image flag.  Keep that compatibility path before
            # falling back to the minimal constructor.
            fallback_kwargs = {key: value for key, value in kwargs.items() if key != "follow_meta_refresh"}
            article = article_cls("", **fallback_kwargs)
        except Exception:
            try:
                article = article_cls("")
            except Exception:
                return None
        config = getattr(article, "config", None)
        if config is not None:
            for key, value in kwargs.items():
                try:
                    setattr(config, key, value)
                except Exception:
                    pass
        if language:
            try:
                article.config.language = language
            except Exception:
                pass
    return article


def _disable_newspaper_image_fetch(article: Any) -> None:
    """Prevent Article.parse from fetching image URLs from supplied HTML.

    newspaper4k 0.9.3 calls ``Article.fetch_images`` unconditionally from
    ``parse``; that helper probes every image URL even when the constructor's
    ``fetch_images`` configuration flag is false.  Shadowing it with a local
    no-op keeps parsing deterministic and offline while preserving the normal
    text/title extraction path.  The configuration flag is also set for older
    releases that consult it directly.
    """

    config = getattr(article, "config", None)
    if config is not None:
        try:
            setattr(config, "fetch_images", False)
        except Exception:
            pass

    # Keep compatibility shims and test doubles from following a configured
    # read-more selector when they implement ``download`` themselves.
    try:
        setattr(article, "read_more_link", "")
    except Exception:
        pass

    def no_fetch_images(*_args: Any, **_kwargs: Any) -> None:
        return None

    try:
        article.fetch_images = no_fetch_images
    except Exception:
        # If an old implementation exposes a read-only method, its config
        # flag above is still the safest available guard.
        pass


def _date_value(value: Any) -> str | None:
    if isinstance(value, datetime):
        parsed = value
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    if isinstance(value, str):
        return value.strip() or None
    return None


def _extract_newspaper(artifact: DocumentArtifact, options: Mapping[str, Any]) -> DocumentArtifact | object:
    if not isinstance(artifact.raw_html, str) or not artifact.raw_html.strip():
        return NO_MATCH
    module = newspaper
    if module is None:
        return NO_MATCH
    language = options.get("language")
    if language is not None and not isinstance(language, str):
        raise ExecutionError("newspaper4k.language must be a string")
    article = _newspaper_article(module, language)
    if article is None:
        return NO_MATCH
    try:
        _disable_newspaper_image_fetch(article)
        # newspaper4k 0.9.3 exposes ``download(input_html=...)`` rather than
        # set_html.  Passing the fetched body explicitly keeps this path
        # offline; ignore_read_more prevents a supplied page from triggering
        # another target request.  Retain set_html for older compatible
        # versions and test doubles.
        download = getattr(article, "download", None)
        if callable(download):
            try:
                download(input_html=artifact.raw_html, ignore_read_more=True)
            except TypeError:
                try:
                    download(input_html=artifact.raw_html)
                except TypeError:
                    download = None
        if not callable(download):
            set_html = getattr(article, "set_html", None)
            if not callable(set_html):
                return NO_MATCH
            set_html(artifact.raw_html)
        # A compatibility download implementation may rebuild its parser and
        # restore the class image method; reapply the no-op immediately before
        # parse so this boundary remains offline regardless of that detail.
        _disable_newspaper_image_fetch(article)
        parse = getattr(article, "parse", None)
        if callable(parse):
            parse()
    except Exception:
        raise _failure() from None
    value = getattr(article, "text", None)
    if not isinstance(value, str) or not value.strip():
        return NO_MATCH
    metadata: dict[str, Any] = {}
    authors = getattr(article, "authors", None)
    if isinstance(authors, (list, tuple)):
        metadata["authors"] = [item for item in authors if isinstance(item, str)]
    published = _date_value(getattr(article, "publish_date", None))
    if published is not None:
        metadata["published_at"] = published
    title = getattr(article, "title", None)
    return _with_result(
        artifact,
        selected_format="text",
        extraction="newspaper4k",
        value=_normalise_text(value),
        title=title if isinstance(title, str) else None,
        metadata=metadata,
    )


def _resiliparse_extract_function(module: Any) -> Any | None:
    extract_pkg = getattr(module, "extract", None)
    html2text = getattr(extract_pkg, "html2text", None)
    fn = getattr(html2text, "extract_plain_text", None)
    if callable(fn):
        return fn
    try:
        imported = importlib.import_module("resiliparse.extract.html2text")
    except (ImportError, ModuleNotFoundError):
        return None
    fn = getattr(imported, "extract_plain_text", None)
    return fn if callable(fn) else None


def _extract_resiliparse(artifact: DocumentArtifact, options: Mapping[str, Any]) -> DocumentArtifact | object:
    if not isinstance(artifact.raw_html, str) or not artifact.raw_html.strip():
        return NO_MATCH
    module = resiliparse
    if module is None:
        return NO_MATCH
    fn = _resiliparse_extract_function(module)
    if fn is None:
        return NO_MATCH
    main_content = bool(options.get("main_content", True))
    preserve_formatting = bool(options.get("preserve_formatting", False))
    try:
        value = fn(
            artifact.raw_html,
            main_content=main_content,
            preserve_formatting=preserve_formatting,
        )
    except Exception:
        raise _failure() from None
    if not isinstance(value, str) or not value.strip():
        return NO_MATCH
    value = _normalise_text(value)
    if not value:
        return NO_MATCH
    return _with_result(artifact, selected_format="text", extraction="resiliparse", value=value)


@dataclass(frozen=True, slots=True)
class _ConfiguredExtractor:
    name: str
    type: str
    options: Mapping[str, Any]

    def extract(self, artifact: DocumentArtifact) -> DocumentArtifact | object:
        if self.type == "identity":
            return _identity_extract(artifact)
        if self.type == "trafilatura":
            return _extract_trafilatura(artifact, self.options)
        if self.type == "newspaper4k":
            return _extract_newspaper(artifact, self.options)
        if self.type == "resiliparse":
            return _extract_resiliparse(artifact, self.options)
        if self.type == "beautifulsoup":
            return _extract_beautifulsoup(artifact, self.options)
        raise ExecutionError(f"unsupported web extractor: {self.type}")

    def __call__(self, artifact: DocumentArtifact) -> DocumentArtifact | object:
        return self.extract(artifact)


def _configured(name: str, extractor_type: str, options: Mapping[str, Any] | None = None) -> _ConfiguredExtractor:
    return _ConfiguredExtractor(name, extractor_type, dict(options or {}))


identity = _configured("identity", "identity")
trafilatura_extractor = _configured("trafilatura", "trafilatura")
newspaper4k = _configured("newspaper4k", "newspaper4k")
resiliparse_extractor = _configured("resiliparse", "resiliparse")
beautifulsoup = _configured("beautifulsoup", "beautifulsoup")

# Fixed instances are convenient for direct offline use.  ``build_extractor``
# still creates an option-aware instance for expanded bindings.
EXTRACTORS: dict[str, WebExtractor] = {
    "identity": identity,
    "trafilatura": trafilatura_extractor,
    "newspaper4k": newspaper4k,
    "resiliparse": resiliparse_extractor,
    "beautifulsoup": beautifulsoup,
}


def extract_identity(artifact: DocumentArtifact) -> DocumentArtifact | object:
    return identity.extract(artifact)


def extract_trafilatura(artifact: DocumentArtifact, options: Mapping[str, Any] | None = None) -> DocumentArtifact | object:
    return _configured("trafilatura", "trafilatura", options).extract(artifact)


def extract_newspaper4k(artifact: DocumentArtifact, options: Mapping[str, Any] | None = None) -> DocumentArtifact | object:
    return _configured("newspaper4k", "newspaper4k", options).extract(artifact)


def extract_resiliparse(artifact: DocumentArtifact, options: Mapping[str, Any] | None = None) -> DocumentArtifact | object:
    return _configured("resiliparse", "resiliparse", options).extract(artifact)


def extract_beautifulsoup(artifact: DocumentArtifact, options: Mapping[str, Any] | None = None) -> DocumentArtifact | object:
    return _configured("beautifulsoup", "beautifulsoup", options).extract(artifact)


def build_extractor(binding: ExtractorBinding | str, options: Mapping[str, Any] | None = None) -> WebExtractor:
    """Build one fixed extractor from an expanded binding or type name."""

    if isinstance(binding, ExtractorBinding):
        return _configured(binding.name, binding.type, binding.options)
    if not isinstance(binding, str):
        raise TypeError("extractor binding must be an ExtractorBinding or type name")
    if binding not in {"identity", *EXTRACTOR_FORMATS}:
        raise ExecutionError(f"unsupported web extractor: {binding}")
    return _configured(binding, binding, options)


__all__ = [
    "EXTRACTOR_FORMATS",
    "EXTRACTORS",
    "WebExtractor",
    "NO_MATCH",
    "identity",
    "trafilatura_extractor",
    "newspaper4k",
    "resiliparse_extractor",
    "beautifulsoup",
    "build_extractor",
    "extract_identity",
    "extract_trafilatura",
    "extract_newspaper4k",
    "extract_resiliparse",
    "extract_beautifulsoup",
    "trafilatura",
    "newspaper",
    "resiliparse",
]
