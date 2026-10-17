"""Generic JSON webhook: one DNS-pinned, redirect-free HTTPS POST per Delivery."""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping

from ..canonical import canonical_json
from ..contracts import JSONValue
from ..errors import ExecutionError
from ..sources._http import check_keys, resolve_env, target_policy, timeout_seconds
from ..web.contracts import FetchRequest, ProviderBinding
from ..web.http import send_pinned_httpx_post
from ..web.url import guard_resolved_addresses, normalize_target_url
from ._common import setting

_LABEL = "Webhook"
_KEYS = {
    "url",
    "headers",
    "timeout_seconds",
    "target_allow_hosts",
    "target_allow_cidrs",
}
_MAX_REQUEST_BYTES = 1024 * 1024
_MAX_RESPONSE_BYTES = 64 * 1024


def send(
    config: Mapping[str, JSONValue], payload: JSONValue, delivery_key: str
) -> None:
    check_keys(config, _KEYS, _LABEL)
    url = setting(config, "url")
    raw_headers = config.get("headers") or {}
    if not isinstance(raw_headers, Mapping) or any(
        not isinstance(k, str) or not isinstance(v, str) for k, v in raw_headers.items()
    ):
        raise ExecutionError("Webhook headers must be a map of strings")
    headers = {
        name: resolve_env(value, _LABEL)
        for name, value in raw_headers.items()
        if name.casefold() != "idempotency-key"
    }
    headers["Idempotency-Key"] = delivery_key
    policy = target_policy(config, _LABEL)
    timeout = timeout_seconds(config, _LABEL)

    body = {"delivery_key": delivery_key, "payload": payload}
    encoded = canonical_json(body)
    if len(encoded.encode()) > _MAX_REQUEST_BYTES:
        raise ExecutionError("Webhook body exceeds 1 MiB")

    target = normalize_target_url(url, policy)
    if target.scheme != "https":
        raise ExecutionError("Webhook url must use https")
    addresses = guard_resolved_addresses(target.host, policy)
    request = FetchRequest(
        url=str(target),
        mode="article",
        acceptable=frozenset({"text"}),
        target_headers=headers,
        stage_deadline=time.monotonic() + timeout,
    )
    binding = ProviderBinding("direct", "httpx", max_body_bytes=_MAX_RESPONSE_BYTES)
    response = send_pinned_httpx_post(
        target, addresses, json.loads(encoded), request, binding
    )
    if not 200 <= response.status_code < 300:
        raise ExecutionError(f"Webhook HTTP {response.status_code}")


class _WebhookSink:
    def __init__(self, client):
        self.client = client

    def send(self, config, rendered_payload, delivery_key):
        return self.client(config, rendered_payload, delivery_key)


def bind(
    client: Callable[[Mapping[str, JSONValue], JSONValue, str], str | None]
    | None = None,
) -> _WebhookSink:
    return _WebhookSink(send if client is None else client)
