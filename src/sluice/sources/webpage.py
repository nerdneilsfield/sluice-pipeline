"""Dependency-free webpage Source using the startup HTTP adapter."""

from __future__ import annotations

from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Mapping
from urllib.parse import urljoin

from sluice.contracts import (
    HTTPClient,
    JSONValue,
    KEEP_CHECKPOINT,
    SourceRecord,
    SourceResult,
)
from sluice.errors import ExecutionError


class _SelectorParser(HTMLParser):
    def __init__(self, selector: str):
        super().__init__(convert_charrefs=True)
        self.selector = selector.strip()
        self.records: list[dict[str, str | None]] = []
        self._active: list[dict[str, str | None]] = []
        self._depth = 0

    def _matches(self, tag: str, attrs: dict[str, str]) -> bool:
        selector = self.selector
        if not selector:
            return False
        if selector.startswith("#"):
            return attrs.get("id") == selector[1:]
        if selector.startswith("."):
            return selector[1:] in (attrs.get("class") or "").split()
        if "." in selector:
            wanted_tag, wanted_class = selector.split(".", 1)
            return tag == wanted_tag and wanted_class in (attrs.get("class") or "").split()
        return tag == selector

    def handle_starttag(self, tag: str, attrs_list):
        attrs = {key: value or "" for key, value in attrs_list}
        if self._matches(tag, attrs):
            self._active.append(
                {"tag": tag, "href": attrs.get("href"), "text": "", "depth": str(self._depth)}
            )
        self._depth += 1

    def handle_startendtag(self, tag: str, attrs_list):
        self.handle_starttag(tag, attrs_list)
        self.handle_endtag(tag)

    def handle_data(self, data: str):
        text = data.strip()
        if text:
            for record in self._active:
                record["text"] = (record.get("text") or "") + text

    def handle_endtag(self, tag: str):
        self._depth = max(0, self._depth - 1)
        if not self._active:
            return
        # The most recently opened matching element closes first.  Nested
        # matching elements remain ordered and independent.
        for index in range(len(self._active) - 1, -1, -1):
            if self._active[index].get("tag") == tag:
                self.records.append(self._active.pop(index))
                break


def _page_record(url: str, html: str) -> SourceRecord:
    parser = _SelectorParser("title")
    try:
        parser.feed(html)
        parser.close()
    except (TypeError, ValueError) as error:
        raise ExecutionError("invalid webpage HTML") from error
    title = parser.records[0].get("text") if parser.records else None
    if not title:
        heading = _SelectorParser("h1")
        try:
            heading.feed(html)
            heading.close()
        except (TypeError, ValueError) as error:
            raise ExecutionError("invalid webpage HTML") from error
        title = heading.records[0].get("text") if heading.records else None
    payload: dict[str, JSONValue] = {"url": url, "content": html}
    if title:
        payload["title"] = title
    return SourceRecord(url, payload)


def _list_records(url: str, html: str, selector: str) -> tuple[SourceRecord, ...]:
    parser = _SelectorParser(selector)
    try:
        parser.feed(html)
        parser.close()
    except (TypeError, ValueError) as error:
        raise ExecutionError("invalid webpage HTML") from error
    records: list[SourceRecord] = []
    for index, found in enumerate(parser.records):
        text = (found.get("text") or "").strip()
        href = found.get("href")
        record_url = urljoin(url, href) if href else url
        logical_id = href or f"{url}#{index}"
        payload: dict[str, JSONValue] = {"url": record_url, "content": text}
        if text:
            payload["title"] = text
        records.append(SourceRecord(logical_id, payload))
    return tuple(records)


def _collect_html(config: Mapping[str, JSONValue], html: str) -> SourceResult:
    if not isinstance(html, str):
        raise ExecutionError("webpage response must be text")
    url = config.get("url")
    if not isinstance(url, str) or not url:
        raise ExecutionError("webpage Source requires a non-empty url")
    selector = config.get("list_selector", config.get("selector"))
    if selector is not None and (not isinstance(selector, str) or not selector.strip()):
        raise ExecutionError("webpage selector must be a non-empty string")
    records = (
        _list_records(url, html, selector)
        if isinstance(selector, str)
        else (_page_record(url, html),)
    )
    next_checkpoint = config["next_checkpoint"] if "next_checkpoint" in config else KEEP_CHECKPOINT
    return SourceResult(records=records, next_checkpoint=next_checkpoint)


def collect(config: Mapping[str, JSONValue], checkpoint: JSONValue) -> SourceResult:
    """Collect HTML supplied explicitly in ``config`` (no network fallback)."""

    html = config.get("html", config.get("content"))
    if not isinstance(html, str):
        raise ExecutionError("webpage collect requires a startup HTTP adapter")
    return _collect_html(config, html)


@dataclass(frozen=True, slots=True)
class _WebpageSource:
    http_client: HTTPClient | None

    def collect(
        self, config: Mapping[str, JSONValue], checkpoint: JSONValue
    ) -> SourceResult:
        if self.http_client is None:
            raise ExecutionError("webpage Source has no startup HTTP adapter")
        url = config.get("url")
        if not isinstance(url, str) or not url:
            raise ExecutionError("webpage Source requires a non-empty url")
        try:
            html = self.http_client.get(url)
        except BaseException as error:
            raise ExecutionError("webpage HTTP request failed") from error
        return _collect_html(config, html)


def bind(http_client: HTTPClient | None) -> _WebpageSource:
    return _WebpageSource(http_client)
