"""GitHub Trending Source: parses the public HTML page (not an API)."""

from __future__ import annotations

import re
from collections.abc import Mapping
from urllib.parse import quote

from bs4 import BeautifulSoup

from sluice.contracts import KEEP_CHECKPOINT, JSONValue, SourceRecord, SourceResult
from sluice.errors import ExecutionError
from sluice.sources._http import check_keys, fetch, int_option, timeout_seconds

_LABEL = "github_trending"
_KEYS = {"since", "language", "limit", "include_stats", "timeout_seconds"}
_SINCE = ("daily", "weekly", "monthly")
_LANGUAGE = re.compile(r"^[A-Za-z0-9+#._-]{1,40}$")
_REPO = re.compile(r"^[\w.-]+/[\w.-]+$")
_PERIOD = re.compile(r"([\d,]+)\s+stars\s+(?:today|this week|this month)")
_MAX_BYTES = 2 * 1024 * 1024


def _count(text: str) -> int | None:
    digits = text.strip().replace(",", "")
    return int(digits) if digits.isdigit() else None


def _text(row, selector: str) -> str:
    node = row.select_one(selector)
    return node.get_text(" ", strip=True) if node else ""


def _parse(row, include_stats: bool) -> SourceRecord | None:
    link = row.select_one("h2 a[href]")
    repo = str(link["href"]).strip("/") if link else ""
    if not _REPO.fullmatch(repo):
        return None
    payload: dict[str, JSONValue] = {"repo": repo, "url": f"https://github.com/{repo}"}
    for key, selector in (
        ("description", "p"),
        ("language", "[itemprop=programmingLanguage]"),
    ):
        if text := _text(row, selector):
            payload[key] = text
    if include_stats:
        period = _PERIOD.search(row.get_text(" ", strip=True))
        for key, value in (
            ("stars_total", _text(row, 'a[href$="/stargazers"]')),
            ("forks", _text(row, 'a[href$="/forks"]')),
            ("stars_period", period.group(1) if period else ""),
        ):
            if (number := _count(value)) is not None:
                payload[key] = number
    return SourceRecord(repo, payload)


def collect(config: Mapping[str, JSONValue], checkpoint: JSONValue) -> SourceResult:
    del checkpoint
    check_keys(config, _KEYS, _LABEL)
    since = config.get("since", "daily")
    if since not in _SINCE:
        raise ExecutionError(f"github_trending since must be one of {list(_SINCE)}")
    language = config.get("language")
    if language is not None and not (
        isinstance(language, str) and _LANGUAGE.fullmatch(language)
    ):
        raise ExecutionError("github_trending language is not valid")
    limit = int_option(config, "limit", 25, 1, 50, _LABEL)
    include_stats = config.get("include_stats", False)
    if not isinstance(include_stats, bool):
        raise ExecutionError("github_trending include_stats must be a boolean")
    timeout = timeout_seconds(config, _LABEL)
    path = f"/{quote(language, safe='')}" if language else ""
    body = fetch(
        f"https://github.com/trending{path}?since={since}",
        timeout=timeout,
        max_bytes=_MAX_BYTES,
        label=_LABEL,
        content_type="text/html",
    )
    soup = BeautifulSoup(body, "html.parser")
    records: dict[str, SourceRecord] = {}
    for row in soup.select("article.Box-row"):
        record = _parse(row, include_stats)
        if record is not None:
            records.setdefault(record.logical_id, record)
    if not records:
        raise ExecutionError("GitHub Trending page layout not recognized")
    return SourceResult(tuple(list(records.values())[:limit]), KEEP_CHECKPOINT)


class _GitHubTrendingSource:
    def collect(
        self, config: Mapping[str, JSONValue], checkpoint: JSONValue
    ) -> SourceResult:
        return collect(config, checkpoint)


def bind(adapters: object | None) -> _GitHubTrendingSource:
    del adapters
    return _GitHubTrendingSource()
