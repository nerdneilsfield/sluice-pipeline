"""GitHub activity Source: releases and commits from the public REST API."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from urllib.parse import urlencode

from sluice.contracts import KEEP_CHECKPOINT, JSONValue, SourceRecord, SourceResult
from sluice.errors import ExecutionError
from sluice.sources._http import (
    check_keys,
    fetch,
    int_option,
    resolve_env,
    timeout_seconds,
)

_LABEL = "github_activity"
_KEYS = {"repo", "events", "branch", "limit", "token", "timeout_seconds"}
_EVENTS = ("releases", "commits")
_REPO = re.compile(r"^[\w.-]+/[\w.-]+$")
_TOKEN = re.compile(r"^env:[A-Za-z_][A-Za-z0-9_]*$")
_MAX_BYTES = 5 * 1024 * 1024


def _str(obj: object, *path: str) -> str | None:
    for key in path:
        obj = obj.get(key) if isinstance(obj, dict) else None
    return obj if isinstance(obj, str) else None


def _get_list(
    repo: str,
    kind: str,
    query: dict[str, str | int],
    headers: dict[str, str],
    timeout: float,
) -> list[dict]:
    body = fetch(
        f"https://api.github.com/repos/{repo}/{kind}?{urlencode(query)}",
        headers=headers,
        timeout=timeout,
        max_bytes=_MAX_BYTES,
        label="GitHub API",
    )
    try:
        data = json.loads(body)
    except (ValueError, RecursionError):
        raise ExecutionError("github_activity response is not valid JSON") from None
    if not isinstance(data, list) or any(not isinstance(i, dict) for i in data):
        raise ExecutionError(f"github_activity {kind} response is not a list")
    return data


def _release(repo: str, item: dict) -> SourceRecord | None:
    release_id = item.get("id")
    if (
        item.get("draft")
        or isinstance(release_id, bool)
        or not isinstance(release_id, int)
    ):
        return None
    fields = {
        "tag": _str(item, "tag_name"),
        "name": _str(item, "name"),
        "url": _str(item, "html_url"),
        "body": _str(item, "body"),
        "published_at": _str(item, "published_at"),
        "author": _str(item, "author", "login"),
    }
    payload: dict[str, JSONValue] = {"kind": "release", "repo": repo}
    payload.update({k: v for k, v in fields.items() if v is not None})
    payload["prerelease"] = item.get("prerelease") is True
    return SourceRecord(f"github:{repo}:release:{release_id}", payload)


def _commit(repo: str, item: dict) -> SourceRecord | None:
    sha = _str(item, "sha")
    if not sha:
        return None
    message = _str(item, "commit", "message") or ""
    fields = {
        "title": message.split("\n", 1)[0],
        "message": message,
        "author": _str(item, "commit", "author", "name"),
        "date": _str(item, "commit", "author", "date"),
        "url": _str(item, "html_url"),
    }
    payload: dict[str, JSONValue] = {"kind": "commit", "repo": repo, "sha": sha}
    payload.update({k: v for k, v in fields.items() if v is not None})
    return SourceRecord(f"github:{repo}:commit:{sha}", payload)


def collect(config: Mapping[str, JSONValue], checkpoint: JSONValue) -> SourceResult:
    del checkpoint
    check_keys(config, _KEYS, _LABEL)
    repo = config.get("repo")
    if (
        not isinstance(repo, str)
        or not _REPO.fullmatch(repo)
        or {".", ".."} & set(repo.split("/"))
    ):
        raise ExecutionError("github_activity repo must be owner/name")
    events = config.get("events", list(_EVENTS))
    if (
        not isinstance(events, list)
        or not events
        or len(set(map(str, events))) != len(events)
        or any(event not in _EVENTS for event in events)
    ):
        raise ExecutionError(
            f"github_activity events must be a subset of {list(_EVENTS)}"
        )
    branch = config.get("branch")
    if branch is not None and not (isinstance(branch, str) and branch):
        raise ExecutionError("github_activity branch must be a non-empty string")
    limit = int_option(config, "limit", 30, 1, 100, _LABEL)
    token = config.get("token")
    if token is not None and not (isinstance(token, str) and _TOKEN.fullmatch(token)):
        raise ExecutionError("github_activity token must be env:NAME")
    timeout = timeout_seconds(config, _LABEL)
    headers = {"Accept": "application/vnd.github+json", "User-Agent": "sluice"}
    if token is not None:
        headers["Authorization"] = f"Bearer {resolve_env(token, _LABEL)}"
    records: dict[str, SourceRecord] = {}
    if "releases" in events:
        for item in _get_list(repo, "releases", {"per_page": limit}, headers, timeout):
            if record := _release(repo, item):
                records.setdefault(record.logical_id, record)
    if "commits" in events:
        query: dict[str, str | int] = {"per_page": limit}
        if branch:
            query["sha"] = branch
        for item in _get_list(repo, "commits", query, headers, timeout):
            if record := _commit(repo, item):
                records.setdefault(record.logical_id, record)
    return SourceResult(tuple(records.values()), KEEP_CHECKPOINT)


class _GitHubActivitySource:
    def collect(
        self, config: Mapping[str, JSONValue], checkpoint: JSONValue
    ) -> SourceResult:
        return collect(config, checkpoint)


def bind(adapters: object | None) -> _GitHubActivitySource:
    del adapters
    return _GitHubActivitySource()
