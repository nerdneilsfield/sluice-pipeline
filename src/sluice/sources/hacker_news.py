"""Hacker News Source (official Firebase API, fixed host)."""

from __future__ import annotations

import json
from collections.abc import Mapping

from sluice.contracts import KEEP_CHECKPOINT, JSONValue, SourceRecord, SourceResult
from sluice.errors import ExecutionError
from sluice.sources._http import (
    check_keys,
    fetch,
    int_option,
    timeout_seconds,
)

_LABEL = "hacker_news"
_KEYS = {"feed", "limit", "include_stats", "timeout_seconds"}
_FEEDS = {"top", "new", "best", "ask", "show", "job"}
_BASE = "https://hacker-news.firebaseio.com/v0"
_MAX_BYTES = 256 * 1024


def _get_json(path: str, timeout: float) -> object:
    body = fetch(
        f"{_BASE}/{path}.json", timeout=timeout, max_bytes=_MAX_BYTES, label=_LABEL
    )
    try:
        return json.loads(body)
    except (ValueError, RecursionError):
        raise ExecutionError("hacker_news response is not valid JSON") from None


def _payload(item_id: int, item: dict, include_stats: bool) -> dict[str, JSONValue]:
    hn_url = f"https://news.ycombinator.com/item?id={item_id}"
    payload: dict[str, JSONValue] = {"id": item_id}
    if isinstance(item.get("title"), str):
        payload["title"] = item["title"]
    url = item.get("url")
    payload["url"] = url if isinstance(url, str) and url else hn_url
    payload["hn_url"] = hn_url
    for key in ("by", "type", "text"):
        if isinstance(item.get(key), str) and item[key]:
            payload[key] = item[key]
    if isinstance(item.get("time"), int):
        payload["time"] = item["time"]
    if include_stats:
        for key in ("score", "descendants"):
            if isinstance(item.get(key), int):
                payload[key] = item[key]
    return payload


def collect(config: Mapping[str, JSONValue], checkpoint: JSONValue) -> SourceResult:
    del checkpoint
    check_keys(config, _KEYS, _LABEL)
    feed = config.get("feed", "top")
    if feed not in _FEEDS:
        raise ExecutionError(f"hacker_news feed must be one of {sorted(_FEEDS)}")
    limit = int_option(config, "limit", 30, 1, 100, _LABEL)
    include_stats = config.get("include_stats", False)
    if not isinstance(include_stats, bool):
        raise ExecutionError("hacker_news include_stats must be a boolean")
    timeout = timeout_seconds(config, _LABEL)
    ids = _get_json(f"{feed}stories", timeout)
    if not isinstance(ids, list) or any(
        isinstance(i, bool) or not isinstance(i, int) for i in ids
    ):
        raise ExecutionError("hacker_news feed is not a list of ids")
    records: list[SourceRecord] = []
    seen: set[int] = set()
    for item_id in ids[:limit]:
        if item_id in seen:
            continue
        seen.add(item_id)
        item = _get_json(f"item/{item_id}", timeout)
        if not isinstance(item, dict) or item.get("deleted") or item.get("dead"):
            continue
        records.append(
            SourceRecord(f"hn:{item_id}", _payload(item_id, item, include_stats))
        )
    return SourceResult(tuple(records), KEEP_CHECKPOINT)


class _HackerNewsSource:
    def collect(
        self, config: Mapping[str, JSONValue], checkpoint: JSONValue
    ) -> SourceResult:
        return collect(config, checkpoint)


def bind(adapters: object | None) -> _HackerNewsSource:
    del adapters
    return _HackerNewsSource()
