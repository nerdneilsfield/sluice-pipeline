"""Feishu custom bot webhook, with optional timestamp signing."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from collections.abc import Callable, Mapping
from urllib.parse import urlsplit

from ..contracts import JSONValue
from ..errors import ExecutionError
from ._common import message_text, post_json, setting, timeout_seconds


def send(
    config: Mapping[str, JSONValue], payload: JSONValue, delivery_key: str
) -> None:
    del delivery_key
    webhook = setting(config, "webhook")
    try:
        url = urlsplit(webhook)
        valid = (
            url.scheme == "https"
            and url.hostname == "open.feishu.cn"
            and url.port in {None, 443}
            and url.username is None
            and url.password is None
            and url.path.startswith("/open-apis/bot/v2/hook/")
            and bool(url.path.removeprefix("/open-apis/bot/v2/hook/"))
            and not url.query
            and not url.fragment
        )
    except ValueError:
        valid = False
    if not valid:
        raise ExecutionError("Feishu webhook must be an HTTPS custom bot URL")
    body = {"msg_type": "text", "content": {"text": message_text(payload)}}
    if "secret" in config:
        timestamp = str(int(time.time()))
        key = f"{timestamp}\n{setting(config, 'secret')}".encode()
        body["timestamp"] = timestamp
        body["sign"] = base64.b64encode(
            hmac.new(key, b"", hashlib.sha256).digest()
        ).decode()
    if len(json.dumps(body, ensure_ascii=False).encode()) > 20_000:
        raise ExecutionError("Feishu message exceeds 20000 bytes")
    result = post_json(webhook, body, timeout_seconds(config))
    code = result.get("code")
    if isinstance(code, bool) or not isinstance(code, int) or code != 0:
        raise ExecutionError("Feishu rejected the message")


class _FeishuSink:
    def __init__(self, client):
        self.client = client

    def send(self, config, rendered_payload, delivery_key):
        return self.client(config, rendered_payload, delivery_key)


def bind(
    client: Callable[[Mapping[str, JSONValue], JSONValue, str], str | None]
    | None = None,
) -> _FeishuSink:
    return _FeishuSink(send if client is None else client)
