"""Small, dependency-free RSS 2.0 and Atom Source adapter."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping
from urllib.parse import urlencode, urlsplit, urlunsplit, parse_qsl
import xml.etree.ElementTree as ET

from sluice.contracts import (
    HTTPClient,
    JSONValue,
    KEEP_CHECKPOINT,
    SourceRecord,
    SourceResult,
)
from sluice.errors import ExecutionError


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1].lower()


def _text(element: ET.Element | None) -> str | None:
    if element is None:
        return None
    value = "".join(element.itertext()).strip()
    return value or None


def _child(element: ET.Element, *names: str) -> ET.Element | None:
    wanted = {name.lower() for name in names}
    for candidate in list(element):
        if _local_name(candidate.tag) in wanted:
            return candidate
    return None


def _field(element: ET.Element, *names: str) -> str | None:
    return _text(_child(element, *names))


def _link(element: ET.Element) -> str | None:
    for candidate in list(element):
        if _local_name(candidate.tag) != "link":
            continue
        href = candidate.attrib.get("href")
        if href and href.strip():
            return href.strip()
        value = _text(candidate)
        if value:
            return value
    return None


def _request_url(config: Mapping[str, JSONValue], checkpoint: JSONValue) -> str:
    url = config.get("url")
    if not isinstance(url, str) or not url:
        raise ExecutionError("RSS Source requires a non-empty url")

    # A checkpoint is a cursor only when the definition explicitly names the
    # query parameter.  Merely having a prior checkpoint must not silently
    # alter a feed request.
    parameter = config.get("cursor_param", config.get("checkpoint_param"))
    if parameter is None or checkpoint is None:
        return url
    if not isinstance(parameter, str) or not parameter:
        raise ExecutionError("RSS cursor_param must be a non-empty string")
    if checkpoint is KEEP_CHECKPOINT:
        return url
    split = urlsplit(url)
    query = list(parse_qsl(split.query, keep_blank_values=True))
    query = [(key, value) for key, value in query if key != parameter]
    if isinstance(checkpoint, str):
        cursor_text = checkpoint
    else:
        from sluice.canonical import canonical_json

        cursor_text = canonical_json(checkpoint)
    query.append((parameter, cursor_text))
    return urlunsplit((split.scheme, split.netloc, split.path, urlencode(query), split.fragment))


def _parse(xml: str) -> tuple[SourceRecord, ...]:
    if not isinstance(xml, str):
        raise ExecutionError("RSS response must be text")
    try:
        root = ET.fromstring(xml)
    except (ET.ParseError, TypeError) as error:
        raise ExecutionError("invalid RSS or Atom XML") from error

    records: list[SourceRecord] = []
    for index, entry in enumerate(
        element
        for element in root.iter()
        if _local_name(element.tag) in {"item", "entry"}
    ):
        entry_id = _field(entry, "guid", "id")
        link = _link(entry)
        title = _field(entry, "title")
        content = _field(entry, "content", "encoded", "description", "summary")
        published = _field(entry, "published", "pubdate")
        updated = _field(entry, "updated", "modified")
        logical_id = entry_id or link or title or f"entry-{index}"
        if not isinstance(logical_id, str) or not logical_id.strip():
            raise ExecutionError("RSS record logical_id must be a non-empty string")

        payload: dict[str, JSONValue] = {}
        if title is not None:
            payload["title"] = title
        if link is not None:
            payload["link"] = link
        if content is not None:
            payload["content"] = content
        if published is not None:
            payload["published"] = published
        if updated is not None:
            payload["updated"] = updated
        records.append(SourceRecord(logical_id.strip(), payload))
    return tuple(records)


def collect(config: Mapping[str, JSONValue], checkpoint: JSONValue) -> SourceResult:
    """Parse XML supplied explicitly in ``config``.

    The startup-bound adapter normally uses :class:`_RSSSource`; this public
    function is also useful for callers that already have a response body.
    """

    xml = config.get("xml", config.get("content"))
    if not isinstance(xml, str):
        raise ExecutionError("RSS collect requires a startup HTTP adapter")
    next_checkpoint = config["next_checkpoint"] if "next_checkpoint" in config else KEEP_CHECKPOINT
    return SourceResult(records=_parse(xml), next_checkpoint=next_checkpoint)


@dataclass(frozen=True, slots=True)
class _RSSSource:
    http_client: HTTPClient | None

    def collect(
        self, config: Mapping[str, JSONValue], checkpoint: JSONValue
    ) -> SourceResult:
        if self.http_client is None:
            raise ExecutionError("RSS Source has no startup HTTP adapter")
        url = _request_url(config, checkpoint)
        try:
            xml = self.http_client.get(url)
        except BaseException as error:
            raise ExecutionError("RSS HTTP request failed") from error
        records = _parse(xml)
        next_checkpoint = config["next_checkpoint"] if "next_checkpoint" in config else KEEP_CHECKPOINT
        return SourceResult(records=records, next_checkpoint=next_checkpoint)


def bind(http_client: HTTPClient | None) -> _RSSSource:
    """Bind the process-start HTTP adapter into an immutable Source object."""

    return _RSSSource(http_client)
