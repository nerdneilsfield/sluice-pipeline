"""Shared SoupSieve CSS selection for webpage list Sources."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag

from ..errors import ExecutionError, WebSafetyError
from .url import normalize_target_url_string


@dataclass(frozen=True, slots=True)
class ListRecord:
    """One selector match in document order.

    ``href`` preserves the authored attribute (which is useful provenance and
    keeps direct helper callers compatible), while ``url`` is the normalized
    absolute HTTP(S) destination used for identity and Source payloads.
    """

    href: str | None
    url: str
    logical_id: str
    text: str
    title: str | None = None


# A descriptive alias makes the helper's result useful to both the coordinator
# and the later Source projector without exposing BeautifulSoup Tag objects.
ListMatch = ListRecord

_HIDDEN_TAGS = ("script", "style", "template", "noscript", "svg")


def _normalise_text(value: str) -> str:
    return " ".join(value.replace("\u00a0", " ").split())


def _href_for(node: Tag) -> str | None:
    value = node.get("href")
    if isinstance(value, str) and value.strip():
        return value.strip()
    # Selectors commonly target cards/articles while the destination lives on
    # their first nested link.  Keep the original attribute value in ``href``
    # and expose the resolved/normalized value separately as ``url``.
    nested = node.find("a", href=True)
    if isinstance(nested, Tag):
        value = nested.get("href")
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _text_for(node: Tag) -> str:
    # Work on a detached parse so selecting a parent does not decompose the
    # hidden descendants of a later nested selector match.
    detached = BeautifulSoup(str(node), "html.parser")
    for hidden in list(detached.find_all(_HIDDEN_TAGS)):
        hidden.decompose()
    return _normalise_text(detached.get_text(" ", strip=True))


def _matches(soup: BeautifulSoup, selector: str) -> Iterable[Tag]:
    try:
        selected = soup.select(selector)
    except Exception as error:
        # SoupSieve's exception text may include selector/configuration input;
        # callers only need a stable execution classification.
        raise ExecutionError("invalid list selector") from error
    return (node for node in selected if isinstance(node, Tag))


def select_list_records(
    raw_html: str,
    final_url: str,
    selector: str,
) -> tuple[ListRecord, ...]:
    """Select ordered, deduplicated HTTP(S) list matches from HTML.

    Invalid destinations are skipped (they can never become Source records),
    while malformed selectors and page URLs are execution errors so a caller
    cannot mistake a selector failure for a valid empty page.
    """

    if not isinstance(raw_html, str) or not raw_html.strip():
        raise ExecutionError("list response must contain HTML")
    if not isinstance(final_url, str) or not final_url.strip():
        raise ExecutionError("list response has no final URL")
    if not isinstance(selector, str) or not selector.strip():
        raise ExecutionError("list selector must be a non-empty string")
    try:
        normalized_final = normalize_target_url_string(final_url)
    except WebSafetyError as error:
        raise ExecutionError("list response has an invalid final URL") from error
    try:
        soup = BeautifulSoup(raw_html, "html.parser")
    except Exception as error:
        raise ExecutionError("invalid webpage HTML") from error

    records: list[ListRecord] = []
    seen_urls: set[str] = set()
    for index, node in enumerate(_matches(soup, selector)):
        raw_href = _href_for(node)
        text = _text_for(node)
        if raw_href is None:
            url = normalized_final
            logical_id = f"{normalized_final}#match-{index}"
        else:
            try:
                url = normalize_target_url_string(urljoin(normalized_final, raw_href))
            except WebSafetyError:
                continue
            logical_id = url
            if url in seen_urls:
                continue
            seen_urls.add(url)
        records.append(
            ListRecord(
                href=raw_href,
                url=url,
                logical_id=logical_id,
                text=text,
                title=text or None,
            )
        )
    return tuple(records)


__all__ = ["ListMatch", "ListRecord", "select_list_records"]
