"""Deterministic visible-content quality evaluation."""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, replace
from typing import Any

from bs4 import BeautifulSoup, Comment, NavigableString, Tag

from .contracts import DocumentArtifact, QualityPolicy

_HIDDEN_TAGS = frozenset(
    {
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
    }
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
_CHALLENGE_SELECTORS = (
    "#challenge-running",
    "#challenge-stage",
    ".cf-challenge-running",
    ".cf-turnstile",
    ".g-recaptcha",
    ".h-captcha",
    'iframe[src*="challenges.cloudflare.com"]',
)
_CHALLENGE_PHRASES = frozenset(
    {
        "verify you are human",
        "checking your browser",
        "access denied",
        "enable javascript and cookies to continue",
    }
)
_WORD = re.compile(r"[^\W_]+", re.UNICODE)
_WHITESPACE = re.compile(r"\s+")


def _normalise_text(value: str) -> str:
    value = value.replace("\u00a0", " ")
    value = unicodedata.normalize("NFC", value)
    # Keep block newlines while collapsing all other Unicode whitespace.
    lines = [_WHITESPACE.sub(" ", line).strip() for line in value.splitlines()]
    lines = [line for line in lines if line]
    return "\n".join(lines).strip()


def _visible_html(html: str) -> tuple[str, float, BeautifulSoup]:
    soup = BeautifulSoup(html, "html.parser")
    for node in list(soup.find_all(_HIDDEN_TAGS)):
        node.decompose()

    def append_visible(
        node: Any, parts: list[str], anchors: list[str], in_anchor: bool = False
    ) -> None:
        if isinstance(node, Comment):
            return
        if isinstance(node, NavigableString):
            text = str(node)
            if text:
                parts.append(text)
                if in_anchor:
                    anchors.append(text)
            return
        if not isinstance(node, Tag):
            return
        anchor = in_anchor or node.name.casefold() == "a"
        if node.name.casefold() in _BLOCK_TAGS:
            parts.append("\n")
        for child in node.children:
            append_visible(child, parts, anchors, anchor)
        if node.name.casefold() in _BLOCK_TAGS:
            parts.append("\n")

    parts: list[str] = []
    anchors: list[str] = []
    append_visible(soup, parts, anchors)
    visible = _normalise_text("".join(parts))
    anchor_chars = len(_normalise_text("".join(anchors)))
    ratio = anchor_chars / len(visible) if visible else 0.0
    return visible, ratio, soup


def _visible_markdown(value: str) -> str:
    # Quality is intentionally conservative for provider Markdown.  Remove
    # fenced/raw HTML and formatting syntax, leaving visible labels/text.
    value = re.sub(r"```(?:[^\n]*)\n(.*?)```", r"\1", value, flags=re.DOTALL)
    value = re.sub(
        r"<script\b[^>]*>.*?</script\s*>", "", value, flags=re.IGNORECASE | re.DOTALL
    )
    value = re.sub(
        r"<style\b[^>]*>.*?</style\s*>", "", value, flags=re.IGNORECASE | re.DOTALL
    )
    value = re.sub(r"<[^>]+>", "", value)
    value = re.sub(r"!\[([^\]]*)\]\([^)]*\)", r"\1", value)
    value = re.sub(r"\[([^\]]+)\]\([^)]*\)", r"\1", value)
    value = re.sub(r"(^|\s)[#>*_`~-]+", r"\1", value)
    return _normalise_text(value)


def _sniff_kind(artifact: DocumentArtifact) -> str | None:
    content_type = (artifact.content_type or "").split(";", 1)[0].strip().lower()
    if content_type in {"text/html", "application/xhtml+xml"}:
        return "html"
    if content_type in {"text/plain", "text/markdown"}:
        return "text"
    if content_type:
        return None
    raw = artifact.raw_html
    if raw is None and artifact.text is None and artifact.markdown is not None:
        raw = artifact.markdown
    if raw is not None:
        stripped = raw.lstrip().casefold()
        if stripped.startswith(("<?xml", "<rss", "<feed")):
            return "xml"
        if stripped.startswith("<") and (
            "<html" in stripped[:512]
            or "<article" in stripped[:512]
            or "<!doctype" in stripped[:512]
            or re.match(r"<\s*[a-z][^>]*>", stripped[:512]) is not None
        ):
            return "html"
        # An absent MIME is not permission to treat arbitrary bytes/text as a
        # valid article.  Only deterministic HTML/XML signatures may proceed.
        return None
    if artifact.raw_source is not None:
        prefix = artifact.raw_source.lstrip().lower()
        if prefix.startswith((b"<rss", b"<feed", b"<?xml")):
            return "xml"
    return None


def _challenge_reason(soup: BeautifulSoup, visible: str) -> str | None:
    for selector in _CHALLENGE_SELECTORS:
        try:
            if soup.select_one(selector) is not None:
                return "challenge"
        except Exception:
            continue
    for script in soup.find_all("script"):
        src = str(script.get("src", "")).casefold()
        if "/cdn-cgi/challenge-platform/" in src:
            return "challenge"
    folded = _WHITESPACE.sub(" ", visible).casefold()
    if any(phrase in folded for phrase in _CHALLENGE_PHRASES):
        return "challenge"
    password = soup.select_one('form input[type="password"]')
    if password is not None:
        article = soup.find(["article", "main"])
        article_text = (
            _normalise_text(article.get_text(" ", strip=True)) if article else ""
        )
        if len(article_text) < 80:
            return "challenge"
    return None


def _empty_shell(soup: BeautifulSoup, visible: str) -> bool:
    if len(visible) >= 100:
        return False
    app_root = soup.find(id=re.compile(r"^(app|root|__next|content)$", re.IGNORECASE))
    has_script = bool(soup.find("script", src=True))
    if app_root is not None and has_script:
        return True
    content_nodes = soup.find_all(["article", "main", "section", "p"])
    return not any(
        len(_normalise_text(node.get_text(" ", strip=True))) >= 80
        for node in content_nodes
    )


@dataclass(frozen=True, slots=True)
class QualityMetrics:
    quality_chars: int = 0
    quality_words: int = 0
    link_ratio: float = 0.0
    passed: bool = False
    reason: str | None = None
    visible_text: str = ""

    @property
    def chars(self) -> int:
        return self.quality_chars

    @property
    def words(self) -> int:
        return self.quality_words

    @property
    def ok(self) -> bool:
        return self.passed


@dataclass(frozen=True, slots=True)
class QualityEvaluation:
    quality: QualityMetrics
    artifact: DocumentArtifact | None = None

    @property
    def passed(self) -> bool:
        return self.quality.passed

    @property
    def reason(self) -> str | None:
        return self.quality.reason

    @property
    def quality_chars(self) -> int:
        return self.quality.quality_chars

    @property
    def quality_words(self) -> int:
        return self.quality.quality_words


def evaluate_quality(
    artifact: DocumentArtifact | str,
    policy: QualityPolicy | None = None,
) -> QualityEvaluation:
    """Evaluate normalized visible content and return stable quality metrics."""

    policy = policy or QualityPolicy()
    if isinstance(artifact, str):
        artifact = DocumentArtifact(
            text=artifact, content_type="text/plain", capabilities=frozenset({"text"})
        )
    if not isinstance(artifact, DocumentArtifact):
        metrics = QualityMetrics(reason="provider_error")
        return QualityEvaluation(metrics, None)

    status = artifact.status_code
    if status is not None and (status < 200 or status >= 300 or status == 204):
        return QualityEvaluation(QualityMetrics(reason="http_status"), artifact)
    byte_sizes = [
        len(value)
        for value in (artifact.raw_source,)
        if isinstance(value, (bytes, bytearray, memoryview))
    ]
    byte_sizes.extend(
        len(value.encode("utf-8"))
        for value in (artifact.raw_html, artifact.text, artifact.markdown)
        if isinstance(value, str)
    )
    if byte_sizes and max(byte_sizes) > policy.max_body_bytes:
        return QualityEvaluation(QualityMetrics(reason="body_too_large"), artifact)

    try:
        kind = _sniff_kind(artifact)
    except Exception:
        return QualityEvaluation(QualityMetrics(reason="provider_error"), artifact)
    if kind not in {"html", "text"}:
        return QualityEvaluation(QualityMetrics(reason="bad_content_type"), artifact)
    if kind == "html":
        source = artifact.raw_html or artifact.text or artifact.markdown or ""
        try:
            # Inspect challenge selectors and script URLs before removing
            # hidden script/style nodes from visible-text normalization.
            raw_soup = BeautifulSoup(source, "html.parser")
            raw_challenge = _challenge_reason(raw_soup, "")
            for node in list(raw_soup.find_all(_HIDDEN_TAGS)):
                node.decompose()
            for comment in list(
                raw_soup.find_all(string=lambda value: isinstance(value, Comment))
            ):
                comment.extract()
            raw_visible = _normalise_text(raw_soup.get_text(" ", strip=True))
            raw_challenge = raw_challenge or _challenge_reason(raw_soup, raw_visible)
            visible, link_ratio, soup = _visible_html(source)
            challenge = raw_challenge or _challenge_reason(soup, visible)
        except Exception:
            return QualityEvaluation(QualityMetrics(reason="provider_error"), artifact)
        if challenge is not None:
            return QualityEvaluation(
                QualityMetrics(reason=challenge, visible_text=visible), artifact
            )
        if _empty_shell(soup, visible):
            return QualityEvaluation(
                QualityMetrics(reason="empty_shell", visible_text=visible), artifact
            )
    else:
        source = artifact.text or artifact.markdown or artifact.raw_html or ""
        visible = (
            _visible_markdown(source)
            if artifact.markdown is not None
            else _normalise_text(source)
        )
        link_ratio = 0.0
        folded = _WHITESPACE.sub(" ", visible).casefold()
        if any(phrase in folded for phrase in _CHALLENGE_PHRASES):
            return QualityEvaluation(
                QualityMetrics(reason="challenge", visible_text=visible), artifact
            )

    chars = len(visible)
    words = len(_WORD.findall(visible))
    metrics = QualityMetrics(
        quality_chars=chars,
        quality_words=words,
        link_ratio=link_ratio,
        passed=False,
        visible_text=visible,
    )
    if chars == 0:
        return QualityEvaluation(replace(metrics, reason="empty"), artifact)
    if policy.min_words is not None and words < policy.min_words:
        return QualityEvaluation(replace(metrics, reason="too_short"), artifact)
    if policy.max_link_ratio is not None and link_ratio > policy.max_link_ratio:
        # The persisted failure vocabulary is intentionally closed; link-ratio
        # rejection is represented by the existing ``too_short`` candidate
        # failure while the metric itself remains available for diagnostics.
        return QualityEvaluation(replace(metrics, reason="too_short"), artifact)
    if chars < policy.min_chars:
        return QualityEvaluation(replace(metrics, reason="too_short"), artifact)
    return QualityEvaluation(replace(metrics, passed=True, reason=None), artifact)


__all__ = ["QualityEvaluation", "QualityMetrics", "evaluate_quality"]
