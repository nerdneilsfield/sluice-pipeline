"""Markdown, metadata, and exception sanitizers for the web boundary."""

from __future__ import annotations

import json
import re
import warnings
from collections.abc import Mapping
from datetime import UTC, datetime
from itertools import islice
from typing import Any
from urllib.parse import urljoin, urlsplit

from bs4 import BeautifulSoup, Comment, MarkupResemblesLocatorWarning
from markdown_it import MarkdownIt

from ..errors import WebSafetyError
from .contracts import FetchChainExhausted, WebAttemptFailure
from .url import normalize_target_url

MAX_METADATA_BYTES = 65_536
MAX_FIELD_STRING = 8_192
MAX_FIELD_LIST = 100
MAX_WARNINGS = 20
_SAFE_WARNINGS = frozenset(
    {
        "metadata_truncated",
        "sanitizer_removed_html",
        "sanitizer_rejected_link",
        "sanitizer_rejected_image",
        "provider_metadata_ignored",
        "publication_date_invalid",
    }
)
_RE_HTML = re.compile(r"<!--.*?-->|<[^>]*>", re.DOTALL)
_RE_SCRIPT = re.compile(
    r"<(script|style|template|noscript|svg|canvas)\b[^>]*>.*?</\1\s*>",
    re.IGNORECASE | re.DOTALL,
)
_RE_RAW_BLOCK = re.compile(
    r"<(?P<tag>address|article|aside|blockquote|body|canvas|div|embed|fieldset|footer|form|h[1-6]|head|header|hr|iframe|main|nav|object|ol|p|pre|section|table|title|ul)\b[^>]*>.*?</(?P=tag)\s*>",
    re.IGNORECASE | re.DOTALL,
)
_RE_EMAIL = re.compile(
    r"[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+"
)
_RE_AUTOLINK = re.compile(
    r"<((?:[A-Za-z][A-Za-z0-9+.-]*:)[^>\s]+|"
    r"[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@"
    r"[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+)>",
    re.IGNORECASE,
)
_RE_REFERENCE = re.compile(r"^(\s*\[[^\]]+\]:\s*)(\S+)(.*)$", re.MULTILINE)
_RE_DATE = re.compile(
    r"^\d{4}-\d{2}-\d{2}(?:[Tt ][0-9]{2}:[0-9]{2}(?::[0-9]{2}(?:\.[0-9]+)?)?(?:[Zz]|[+-][0-9]{2}:?[0-9]{2})?)?$"
)
_RE_RFC3339 = re.compile(
    r"^\d{4}-\d{2}-\d{2}[Tt]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|z|[+-]\d{2}:?\d{2})$"
)
_RE_FENCED_CODE = re.compile(
    r"(?P<fence>`{3,}|~{3,})[^\n]*\n.*?(?P=fence)\s*", re.DOTALL
)
_RE_INLINE_CODE = re.compile(r"(?<!`)`+[^`\n]+`+(?!`)")
_RE_IMAGE_START = re.compile(r"!\[([^\]\n]*)\]\(")
_RE_LINK_START = re.compile(r"(?<!!)\[([^\]\n]+)\]\(")


def _visible_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", MarkupResemblesLocatorWarning)
            soup = BeautifulSoup(text, "html.parser")
        for node in soup.find_all(
            [
                "script",
                "style",
                "template",
                "noscript",
                "svg",
                "head",
                "title",
                "iframe",
                "object",
                "embed",
            ]
        ):
            node.decompose()
        for comment in list(
            soup.find_all(string=lambda item: isinstance(item, Comment))
        ):
            comment.extract()
        return " ".join(soup.get_text(" ", strip=True).replace("\u00a0", " ").split())
    except Exception:
        return ""


def _normalise_destination(
    destination: str,
    base_url: str | None,
    *,
    image: bool = False,
    allow_email: bool = False,
) -> str | None:
    destination = destination.strip()
    if not destination:
        return None
    # Same-document fragments do not need network validation.
    if destination.startswith("#"):
        return None if image else destination
    try:
        parsed = urlsplit(destination)
    except ValueError:
        return None
    scheme = parsed.scheme.casefold()
    if allow_email and not scheme and not image and _RE_EMAIL.fullmatch(destination):
        return destination
    if scheme in {"javascript", "data", "file", "vbscript", "about"}:
        return None
    if not parsed.scheme and base_url:
        try:
            destination = urljoin(base_url, destination)
            parsed = urlsplit(destination)
            scheme = parsed.scheme.casefold()
        except ValueError:
            return None
    if image and scheme not in {"http", "https"}:
        return None
    if scheme in {"http", "https"}:
        try:
            return str(normalize_target_url(destination))
        except WebSafetyError:
            return None
    if scheme == "mailto" and not image:
        # Keep a conservative mailto grammar and discard control characters.
        if "@" not in parsed.path or any(ord(char) < 0x20 for char in destination):
            return None
        return destination
    return None


def _markdown_parse(value: str) -> list[Any]:
    # Parse with HTML tokenization enabled as the design requires.  Rendering
    # is intentionally not used; this call validates the token stream before
    # our deterministic sanitizer strips raw HTML.
    return MarkdownIt("commonmark", {"html": True, "linkify": False}).parse(value)


def _strip_html_blocks(value: str) -> str:
    """Drop complete raw HTML block token ranges while preserving code fences."""

    try:
        tokens = _markdown_parse(value)
    except Exception:
        return value
    lines = value.splitlines(keepends=True)
    ranges = [
        (token.map[0], token.map[1])
        for token in tokens
        if getattr(token, "type", None) == "html_block"
        and getattr(token, "map", None)
        and len(token.map) == 2
    ]
    if not ranges:
        return value
    return "".join(
        line
        for index, line in enumerate(lines)
        if not any(start <= index < end for start, end in ranges)
    )


def _scan_markdown_destination(value: str, start: int) -> tuple[str, int] | None:
    """Read one Markdown destination and return it plus the consumed end.

    A regular expression cannot safely distinguish the closing delimiter from
    parentheses nested inside a destination (for example
    ``javascript:alert(1)``).  This bounded scanner follows the inline-link
    shape used by the sanitizer and consumes the complete token before URL
    validation, so an unsafe destination cannot leave a trailing ``)``.
    """

    if start >= len(value) or value[start].isspace():
        return None
    index = start
    if value[index] == "<":
        close = value.find(">", index + 1)
        if close < 0:
            return None
        destination = value[index + 1 : close]
        index = close + 1
    else:
        chars: list[str] = []
        depth = 0
        while index < len(value):
            char = value[index]
            if char == "\\" and index + 1 < len(value):
                chars.append(value[index : index + 2])
                index += 2
                continue
            if char == "(":
                depth += 1
                chars.append(char)
                index += 1
                continue
            if char == ")":
                if depth == 0:
                    break
                depth -= 1
                chars.append(char)
                index += 1
                continue
            if char.isspace() and depth == 0:
                break
            chars.append(char)
            index += 1
        if not chars or depth:
            return None
        destination = "".join(chars)

    # Optional Markdown link titles are deliberately discarded by the
    # canonical sanitizer, but must still be consumed as part of this token.
    if index < len(value) and value[index] != ")":
        title_index = index
        while title_index < len(value) and value[title_index].isspace():
            title_index += 1
        if title_index >= len(value) or value[title_index] not in "'\"":
            return None
        quote = value[title_index]
        title_index += 1
        while title_index < len(value):
            if value[title_index] == "\\":
                title_index += 2
                continue
            if value[title_index] == quote:
                break
            title_index += 1
        if title_index >= len(value) or value[title_index] != quote:
            return None
        index = title_index + 1
        while index < len(value) and value[index].isspace():
            index += 1

    if index >= len(value) or value[index] != ")":
        return None
    return destination, index + 1


def _rewrite_markdown_destinations(
    value: str,
    base_url: str | None,
    *,
    image: bool,
    warnings: list[str] | None = None,
) -> str:
    pattern = _RE_IMAGE_START if image else _RE_LINK_START
    output: list[str] = []
    cursor = 0
    search = 0
    while True:
        match = pattern.search(value, search)
        if match is None:
            output.append(value[cursor:])
            return "".join(output)
        output.append(value[cursor : match.start()])
        parsed = _scan_markdown_destination(value, match.end())
        if parsed is None:
            # Preserve malformed syntax for the later raw-markup pass rather
            # than dropping text or looping on the same opener.
            output.append(value[match.start() : match.end()])
            cursor = match.end()
            search = match.end()
            continue
        destination, end = parsed
        safe = _normalise_destination(destination, base_url, image=image)
        if safe is None:
            if warnings is not None:
                warning = (
                    "sanitizer_rejected_image" if image else "sanitizer_rejected_link"
                )
                if warning not in warnings:
                    warnings.append(warning)
            replacement = match.group(1)
        elif image:
            replacement = f"![{match.group(1)}]({safe})"
        else:
            replacement = f"[{match.group(1)}]({safe})"
        output.append(replacement)
        cursor = end
        search = end


def sanitize_markdown(markdown: str, base_url: str | None = None) -> str:
    """Remove raw HTML and unsafe links/images from provider Markdown."""

    if not isinstance(markdown, str):
        raise WebSafetyError("provider_error")
    try:
        _markdown_parse(markdown)
    except Exception:
        # Parser failures are not rendered through an untrusted fallback; map
        # them to the same stable provider boundary as other sanitizer errors.
        raise WebSafetyError("provider_error") from None
    # Markdown-It identifies multiline raw blocks without confusing HTML-like
    # text inside fenced code.  Regexes below remain the fallback for inline
    # tags and malformed fragments.
    value = _strip_html_blocks(markdown)
    protected: list[str] = []

    def protect(match: re.Match[str]) -> str:
        token = f"\x00SLUICE_CODE_{len(protected)}\x00"
        protected.append(match.group(0))
        return token

    value = _RE_FENCED_CODE.sub(protect, value)
    value = _RE_INLINE_CODE.sub(protect, value)
    value = _RE_SCRIPT.sub("", value)
    # Markdown-It represents a raw HTML block as one token, including its
    # textual contents.  Remove the complete block rather than leaving page
    # markup text behind in the sanitized representation.
    value = _RE_RAW_BLOCK.sub("", value)

    value = _rewrite_markdown_destinations(
        value,
        base_url,
        image=True,
        warnings=None,
    )
    value = _rewrite_markdown_destinations(value, base_url, image=False)

    def reference_replacement(match: re.Match[str]) -> str:
        destination = match.group(2)
        safe = _normalise_destination(destination, base_url)
        if safe is None:
            return match.group(1)
        return f"{match.group(1)}{safe}{match.group(3)}"

    value = _RE_REFERENCE.sub(reference_replacement, value)

    autolinks: list[str] = []

    def autolink_replacement(match: re.Match[str]) -> str:
        safe = _normalise_destination(match.group(1), base_url, allow_email=True)
        # Keep approved autolinks as Markdown autolinks so projection and
        # metadata extraction retain their link semantics.  Unsafe values are
        # unwrapped to visible text without their angle delimiters.
        rendered = f"<{safe}>" if safe is not None else match.group(1)
        token = f"\x00SLUICE_AUTOLINK_{len(autolinks)}\x00"
        autolinks.append(rendered)
        return token

    value = _RE_AUTOLINK.sub(autolink_replacement, value)
    # Remove ordinary raw HTML tags after link/image handling.  Script content
    # was removed above; stripping comments and tags leaves no executable DOM.
    value = _RE_HTML.sub("", value)
    for index, autolink in enumerate(autolinks):
        value = value.replace(f"\x00SLUICE_AUTOLINK_{index}\x00", autolink)
    for index, code in enumerate(protected):
        value = value.replace(f"\x00SLUICE_CODE_{index}\x00", code)
    return value


def _bounded_string(value: Any) -> str:
    # Descriptive metadata is scalar text.  Do not stringify arbitrary
    # mappings/sequences: their representation can embed credentials or
    # other provider-controlled secrets into persisted metadata.
    if not isinstance(value, str):
        return ""
    return _visible_text(value)[:MAX_FIELD_STRING]


def _safe_url(
    value: Any,
    base_url: str | None,
    warnings: list[str],
    warning: str,
    *,
    allow_mailto_fragment: bool = False,
) -> str | None:
    if not isinstance(value, str):
        return None
    destination = _normalise_destination(value, base_url)
    if destination is None:
        if warning in _SAFE_WARNINGS and warning not in warnings:
            warnings.append(warning)
        return None
    lowered = destination.lower()
    if lowered.startswith(("http://", "https://")):
        return destination
    if allow_mailto_fragment and (
        lowered.startswith("mailto:") or destination.startswith("#")
    ):
        return destination
    if warning in _SAFE_WARNINGS and warning not in warnings:
        warnings.append(warning)
    return None


def _first(mapping: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in mapping:
            return mapping[key]
    return None


def _sanitize_open_graph(
    raw: Any, base_url: str | None, warnings: list[str]
) -> dict[str, Any]:
    if not isinstance(raw, Mapping):
        return {}
    aliases = {
        "title": ("title", "og:title"),
        "type": ("type", "og:type"),
        "url": ("url", "og:url"),
        "description": ("description", "og:description"),
        "site_name": ("site_name", "site-name", "og:site_name"),
        "locale": ("locale", "og:locale"),
        "image": ("image", "image:url", "og:image"),
    }
    result: dict[str, Any] = {}
    for output_key, candidates in aliases.items():
        value = _first(raw, *candidates)
        if value is None:
            continue
        if output_key in {"url", "image"}:
            if isinstance(value, (list, tuple)):
                values = []
                for item in value[:MAX_FIELD_LIST]:
                    safe = _safe_url(
                        item,
                        base_url,
                        warnings,
                        "sanitizer_rejected_image"
                        if output_key == "image"
                        else "sanitizer_rejected_link",
                    )
                    if safe is not None and safe not in values:
                        values.append(safe)
                if values:
                    result[output_key] = values if output_key == "image" else values[0]
            else:
                safe = _safe_url(
                    value,
                    base_url,
                    warnings,
                    "sanitizer_rejected_image"
                    if output_key == "image"
                    else "sanitizer_rejected_link",
                )
                if safe is not None:
                    result[output_key] = safe
        else:
            text = _bounded_string(value)
            if text:
                result[output_key] = text
    return result


def _sanitize_twitter(
    raw: Any, base_url: str | None, warnings: list[str]
) -> dict[str, Any]:
    if not isinstance(raw, Mapping):
        return {}
    aliases = {
        "card": ("card", "twitter:card"),
        "title": ("title", "twitter:title"),
        "description": ("description", "twitter:description"),
        "site": ("site", "twitter:site"),
        "creator": ("creator", "twitter:creator"),
        "image": ("image", "twitter:image", "image:src"),
    }
    result: dict[str, Any] = {}
    for output_key, candidates in aliases.items():
        value = _first(raw, *candidates)
        if value is None:
            continue
        if output_key == "image":
            safe = _safe_url(value, base_url, warnings, "sanitizer_rejected_image")
            if safe is not None:
                result[output_key] = safe
        else:
            text = _bounded_string(value)
            if text:
                result[output_key] = text
    return result


def _normalise_rfc3339(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if _RE_RFC3339.fullmatch(text) is None:
        return None
    try:
        parsed = datetime.fromisoformat(
            text[:-1] + "+00:00" if text[-1] in "Zz" else text
        )
    except (TypeError, ValueError, OverflowError):
        return None
    if parsed.tzinfo is None:
        return None
    utc = parsed.astimezone(UTC)
    if utc.microsecond:
        value = utc.isoformat(timespec="microseconds").replace("+00:00", "Z")
        head, fraction = value[:-1].split(".", 1)
        return f"{head}.{fraction.rstrip('0')}Z"
    return utc.isoformat(timespec="seconds").replace("+00:00", "Z")


def _contains_html_marker(value: Any, budget: list[int], depth: int = 0) -> bool:
    """Inspect only a bounded prefix of metadata collections for raw markup."""

    if budget[0] <= 0 or depth > 4:
        return False
    budget[0] -= 1
    if isinstance(value, str):
        return (
            re.search(
                r"<\s*(?:script|style|template|noscript|svg|[A-Za-z][^>]{0,80})",
                value[:MAX_FIELD_STRING],
                re.IGNORECASE,
            )
            is not None
        )
    if isinstance(value, Mapping):
        return any(
            _contains_html_marker(item, budget, depth + 1)
            for _key, item in islice(value.items(), MAX_FIELD_LIST)
        )
    if isinstance(value, (list, tuple)):
        return any(
            _contains_html_marker(item, budget, depth + 1)
            for item in value[:MAX_FIELD_LIST]
        )
    return False


def _sanitize_json_ld_object(
    raw: Mapping[str, Any], base_url: str | None, warnings: list[str]
) -> dict[str, Any]:
    allowed = {
        "@type",
        "headline",
        "name",
        "description",
        "author",
        "datePublished",
        "dateModified",
        "url",
        "image",
    }
    result: dict[str, Any] = {}
    for key, value in islice(raw.items(), MAX_FIELD_LIST):
        if key not in allowed:
            continue
        if key in {"url", "image"}:
            if isinstance(value, (list, tuple)):
                urls: list[str] = []
                for item in value[:MAX_FIELD_LIST]:
                    item_value = item.get("url") if isinstance(item, Mapping) else item
                    safe = _safe_url(
                        item_value,
                        base_url,
                        warnings,
                        "sanitizer_rejected_image"
                        if key == "image"
                        else "sanitizer_rejected_link",
                    )
                    if safe is not None and safe not in urls:
                        urls.append(safe)
                if urls:
                    result[key] = urls
            elif isinstance(value, Mapping):
                safe = _safe_url(
                    value.get("url"),
                    base_url,
                    warnings,
                    "sanitizer_rejected_image"
                    if key == "image"
                    else "sanitizer_rejected_link",
                )
                if safe is not None:
                    result[key] = safe
            else:
                safe = _safe_url(
                    value,
                    base_url,
                    warnings,
                    "sanitizer_rejected_image"
                    if key == "image"
                    else "sanitizer_rejected_link",
                )
                if safe is not None:
                    result[key] = safe
        elif key == "author":
            authors: list[str] = []
            values = value if isinstance(value, (list, tuple)) else (value,)
            for item in values[:MAX_FIELD_LIST]:
                text = item.get("name") if isinstance(item, Mapping) else item
                text = _bounded_string(text)
                if text and text not in authors:
                    authors.append(text)
            if authors:
                result[key] = authors[:MAX_FIELD_LIST]
        elif key in {"datePublished", "dateModified"}:
            text = _bounded_string(value)
            normalized = _normalise_rfc3339(value)
            if normalized is not None:
                result[key] = normalized
            elif text and _RE_DATE.fullmatch(text):
                result[key] = text
            elif text and "publication_date_invalid" not in warnings:
                warnings.append("publication_date_invalid")
        elif key == "@type":
            if isinstance(value, str):
                result[key] = _bounded_string(value)
            elif isinstance(value, (list, tuple)):
                result[key] = [
                    _bounded_string(item)
                    for item in value[:MAX_FIELD_LIST]
                    if _bounded_string(item)
                ]
        else:
            text = _bounded_string(value)
            if text:
                result[key] = text
    return result


def _sanitize_json_ld(
    raw: Any, base_url: str | None, warnings: list[str]
) -> list[dict[str, Any]]:
    """Return ordered, bounded JSON-LD objects without unknown fields."""

    values = raw if isinstance(raw, (list, tuple)) else (raw,)
    result: list[dict[str, Any]] = []
    for item in values[:MAX_FIELD_LIST]:
        if not isinstance(item, Mapping):
            continue
        sanitized = _sanitize_json_ld_object(item, base_url, warnings)
        if sanitized:
            result.append(sanitized)
    return result


def _sanitize_links(
    raw: Any, base_url: str | None, warnings: list[str]
) -> list[dict[str, str]]:
    if not isinstance(raw, (list, tuple)):
        return []
    result: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in raw[:MAX_FIELD_LIST]:
        if not isinstance(item, Mapping):
            continue
        destination = _safe_url(
            item.get("url", item.get("href")),
            base_url,
            warnings,
            "sanitizer_rejected_link",
            allow_mailto_fragment=True,
        )
        if destination is None or destination in seen:
            continue
        text = _bounded_string(item.get("text", item.get("title", "")))
        if not text:
            text = destination
        result.append({"text": text, "url": destination})
        seen.add(destination)
    return result


def _truncate_fields(value: Any) -> Any:
    if isinstance(value, str):
        return value[:MAX_FIELD_STRING]
    if isinstance(value, list):
        return [_truncate_fields(item) for item in value[:MAX_FIELD_LIST]]
    if isinstance(value, tuple):
        return [_truncate_fields(item) for item in value[:MAX_FIELD_LIST]]
    if isinstance(value, Mapping):
        return {
            str(key): _truncate_fields(item)
            for key, item in islice(value.items(), MAX_FIELD_LIST)
        }
    return value


def _encoded_size(value: Mapping[str, Any]) -> int:
    return len(
        json.dumps(
            value, ensure_ascii=False, separators=(",", ":"), sort_keys=True
        ).encode("utf-8")
    )


def sanitize_metadata(
    metadata: Mapping[str, Any] | None, base_url: str | None = None
) -> dict[str, Any]:
    """Structurally allowlist and deterministically bound web metadata."""

    if not isinstance(metadata, Mapping):
        return {}
    source: Mapping[str, Any] = (
        metadata.get("web", metadata)
        if isinstance(metadata.get("web", metadata), Mapping)
        else {}
    )
    warnings: list[str] = []
    raw_warnings = source.get("warnings", ())
    if isinstance(raw_warnings, (list, tuple)):
        for warning in raw_warnings[:MAX_WARNINGS]:
            if (
                isinstance(warning, str)
                and warning in _SAFE_WARNINGS
                and warning not in warnings
            ):
                warnings.append(warning)

    known_top_level = {
        "source_url",
        "final_url",
        "title",
        "provider_instance",
        "provider_type",
        "extractor",
        "output_format",
        "quality_chars",
        "quality_words",
        "published_at",
        "open_graph",
        "twitter_card",
        "json_ld",
        "links",
        "authors",
        "warnings",
    }
    if (
        any(key not in known_top_level for key in islice(source, MAX_FIELD_LIST))
        and "provider_metadata_ignored" not in warnings
    ):
        warnings.append("provider_metadata_ignored")
    if (
        _contains_html_marker(source, [MAX_FIELD_LIST * 4])
        and "sanitizer_removed_html" not in warnings
    ):
        warnings.append("sanitizer_removed_html")

    scalar_fields = (
        "source_url",
        "final_url",
        "title",
        "provider_instance",
        "provider_type",
        "extractor",
        "output_format",
        "quality_chars",
        "quality_words",
        "published_at",
    )
    result: dict[str, Any] = {key: None for key in scalar_fields}
    for key in scalar_fields:
        if key not in source:
            continue
        value = source[key]
        if key in {"source_url", "final_url"}:
            safe = _safe_url(value, base_url, warnings, "sanitizer_rejected_link")
            if safe is not None:
                result[key] = safe
            else:
                result[key] = None
        elif key in {"quality_chars", "quality_words"}:
            if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
                result[key] = value
        elif key == "published_at":
            result[key] = _normalise_rfc3339(value)
            if (
                value is not None
                and result[key] is None
                and _bounded_string(value)
                and "publication_date_invalid" not in warnings
            ):
                warnings.append("publication_date_invalid")
        elif key == "output_format":
            result[key] = (
                value
                if isinstance(value, str) and value in {"text", "markdown"}
                else None
            )
        else:
            result[key] = _bounded_string(value) if isinstance(value, str) else None

    result["open_graph"] = _sanitize_open_graph(
        source.get("open_graph", {}), base_url, warnings
    )
    result["twitter_card"] = _sanitize_twitter(
        source.get("twitter_card", {}), base_url, warnings
    )
    result["json_ld"] = _sanitize_json_ld(source.get("json_ld", {}), base_url, warnings)
    result["links"] = _sanitize_links(source.get("links", ()), base_url, warnings)

    authors: list[str] = []
    raw_authors = source.get("authors", ())
    if isinstance(raw_authors, (list, tuple)):
        for author in raw_authors[:MAX_FIELD_LIST]:
            text = _bounded_string(author)
            if text and text not in authors:
                authors.append(text)
    result["authors"] = authors
    result["warnings"] = warnings[:MAX_WARNINGS]
    result = _truncate_fields(result)

    omitted = False
    for field_name in ("json_ld", "open_graph", "twitter_card", "links", "authors"):
        if _encoded_size(result) <= MAX_METADATA_BYTES:
            break
        if result.get(field_name):
            result[field_name] = {} if isinstance(result[field_name], Mapping) else []
            omitted = True
    if omitted and "metadata_truncated" not in result["warnings"]:
        result["warnings"] = (result["warnings"] + ["metadata_truncated"])[
            :MAX_WARNINGS
        ]
    # Required scalar provenance fields may not be omitted.  As a final guard,
    # trim optional strings until the bounded JSON representation fits.
    if _encoded_size(result) > MAX_METADATA_BYTES:
        for key in sorted(result):
            if key in {
                "source_url",
                "final_url",
                "provider_instance",
                "provider_type",
                "extractor",
                "output_format",
                "quality_chars",
                "quality_words",
                "warnings",
            }:
                continue
            if isinstance(result[key], str):
                result[key] = result[key][:256]
            elif isinstance(result[key], (dict, list)):
                result[key] = {} if isinstance(result[key], dict) else []
            if _encoded_size(result) <= MAX_METADATA_BYTES:
                break
    if _encoded_size(result) > MAX_METADATA_BYTES:
        # Required scalar provenance values stay present, but may be shortened
        # further than the per-field ceiling to honor the global byte bound.
        required_strings = [
            key for key in scalar_fields if isinstance(result.get(key), str)
        ]
        while required_strings and _encoded_size(result) > MAX_METADATA_BYTES:
            changed = False
            for key in required_strings:
                value = result[key]
                if isinstance(value, str) and len(value) > 64:
                    result[key] = value[: max(64, len(value) // 2)]
                    changed = True
                    if _encoded_size(result) <= MAX_METADATA_BYTES:
                        break
            if not changed:
                break
    return result


def sanitize_web_error(error: BaseException) -> str:
    """Map arbitrary provider exceptions to a stable safe reason/summary."""

    stable = {
        "bad_url",
        "ssrf_blocked",
        "timeout",
        "network",
        "rate_limited",
        "provider_busy",
        "http_status",
        "bad_content_type",
        "body_too_large",
        "unsafe_xml",
        "challenge",
        "empty_shell",
        "empty",
        "too_short",
        "extract_failed",
        "missing_capability",
        "provider_error",
    }
    aliases = {
        "peer_mismatch": "ssrf_blocked",
        "forbidden_header": "bad_url",
        "service_redirect": "provider_error",
        "too_many_redirects": "ssrf_blocked",
    }
    if isinstance(error, WebSafetyError):
        reason = getattr(error, "reason", None)
        if reason is None and error.args and isinstance(error.args[0], str):
            reason = (
                error.args[0]
                if error.args[0] in stable or error.args[0] in aliases
                else None
            )
        return aliases.get(reason, reason if reason in stable else "provider_error")
    if isinstance(error, FetchChainExhausted):
        attempts: list[dict[str, Any]] = []
        for attempt in error.attempts[:100]:
            if isinstance(attempt, WebAttemptFailure):
                item: dict[str, Any] = {
                    "provider": attempt.provider_instance,
                    "reason": attempt.reason,
                }
                if attempt.status_code is not None:
                    item["status"] = attempt.status_code
                attempts.append(item)
        # Canonical JSON contains only bounded identifiers/reasons supplied by
        # the frozen contract, never exception text or response bodies.
        return json.dumps(
            {"reason": "provider_error", "attempts": attempts},
            separators=(",", ":"),
            sort_keys=True,
        )
    try:
        import httpx

        if isinstance(error, httpx.TimeoutException):
            return "timeout"
        if isinstance(error, httpx.HTTPStatusError):
            status = getattr(error.response, "status_code", 0)
            return "rate_limited" if status == 429 else "http_status"
        if isinstance(error, httpx.HTTPError):
            return "network"
    except Exception:
        pass
    if isinstance(error, TimeoutError):
        return "timeout"
    if isinstance(error, OSError):
        return "network"
    return "provider_error"


__all__ = [
    "MAX_METADATA_BYTES",
    "sanitize_markdown",
    "sanitize_metadata",
    "sanitize_web_error",
]
