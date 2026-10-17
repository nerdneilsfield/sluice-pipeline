"""Generic JSON HTTP API Source."""

from __future__ import annotations

import json
from collections.abc import Mapping

from sluice.contracts import KEEP_CHECKPOINT, JSONValue, SourceRecord, SourceResult
from sluice.errors import ExecutionError
from sluice.sources._http import (
    check_keys,
    fetch,
    int_option,
    resolve_env,
    target_policy,
    timeout_seconds,
)

_LABEL = "json_api"
_KEYS = {
    "url",
    "headers",
    "items_path",
    "id_field",
    "limit",
    "timeout_seconds",
    "target_allow_hosts",
    "target_allow_cidrs",
}
_MAX_BYTES = 5 * 1024 * 1024


def _lookup(value: object, path: str) -> object:
    for key in path.split(".") if path else ():
        if not isinstance(value, dict) or key not in value:
            raise LookupError(path)
        value = value[key]
    return value


def _reject_constant(name: str) -> object:
    raise ValueError(name)


def _headers(value: object) -> dict[str, str]:
    if value is None:
        return {}
    if not isinstance(value, Mapping) or any(
        not isinstance(k, str) or not isinstance(v, str) for k, v in value.items()
    ):
        raise ExecutionError("json_api headers must be a map of strings")
    return {name: resolve_env(item, _LABEL) for name, item in value.items()}


def collect(config: Mapping[str, JSONValue], checkpoint: JSONValue) -> SourceResult:
    del checkpoint
    check_keys(config, _KEYS, _LABEL)
    url = config.get("url")
    id_field = config.get("id_field")
    items_path = config.get("items_path", "")
    if not isinstance(url, str) or not url:
        raise ExecutionError("json_api requires a non-empty url")
    if not isinstance(id_field, str) or not id_field:
        raise ExecutionError("json_api requires a non-empty id_field")
    if not isinstance(items_path, str):
        raise ExecutionError("json_api items_path must be a string")
    limit = int_option(config, "limit", 100, 1, 1000, _LABEL)
    policy = target_policy(config, _LABEL)
    timeout = timeout_seconds(config, _LABEL)
    body = fetch(
        url,
        headers=_headers(config.get("headers")),
        timeout=timeout,
        max_bytes=_MAX_BYTES,
        label=_LABEL,
        policy=policy,
    )
    try:
        document = json.loads(body, parse_constant=_reject_constant)
    except (ValueError, RecursionError):
        raise ExecutionError("json_api response is not valid JSON") from None
    try:
        items = _lookup(document, items_path)
    except LookupError:
        raise ExecutionError("json_api items_path not found in response") from None
    if not isinstance(items, list):
        raise ExecutionError("json_api items_path does not point to a list")
    records: list[SourceRecord] = []
    seen: set[str] = set()
    for item in items[:limit]:
        if not isinstance(item, dict):
            raise ExecutionError("json_api item is not an object")
        try:
            raw_id = _lookup(item, id_field)
        except LookupError:
            raise ExecutionError("json_api item has no id_field value") from None
        if (
            isinstance(raw_id, bool)
            or not isinstance(raw_id, (str, int))
            or raw_id == ""
        ):
            raise ExecutionError("json_api id_field must be a non-empty string or int")
        logical_id = str(raw_id)
        if logical_id not in seen:
            seen.add(logical_id)
            records.append(SourceRecord(logical_id, item))
    return SourceResult(tuple(records), KEEP_CHECKPOINT)


class _JsonApiSource:
    def collect(
        self, config: Mapping[str, JSONValue], checkpoint: JSONValue
    ) -> SourceResult:
        return collect(config, checkpoint)


def bind(adapters: object | None) -> _JsonApiSource:
    del adapters
    return _JsonApiSource()
