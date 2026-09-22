"""Telegram Sink startup adapter."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass

from ..contracts import JSONValue


@dataclass(frozen=True, slots=True)
class _TelegramSink:
    client: Callable[[Mapping[str, JSONValue], JSONValue, str], str | None]

    def send(
        self,
        config: Mapping[str, JSONValue],
        rendered_payload: JSONValue,
        delivery_key: str,
    ) -> str | None:
        return self.client(config, rendered_payload, delivery_key)


def bind(
    client: Callable[[Mapping[str, JSONValue], JSONValue, str], str | None]
    | None = None,
) -> _TelegramSink:
    return _TelegramSink(send if client is None else client)


def send(config: Mapping[str, JSONValue], payload: JSONValue, delivery_key: str) -> str:
    """Send one plain-text message; durable retries belong to Delivery."""
    from ..errors import ExecutionError
    from ._common import message_text, post_json, setting, timeout_seconds

    del delivery_key
    token = setting(config, "bot_token")
    if any(
        character
        not in "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ:_-"
        for character in token
    ):
        raise ExecutionError("Telegram bot_token is invalid")
    text = message_text(payload)
    if len(text.encode("utf-16-le")) // 2 > 4096:
        raise ExecutionError("Telegram text exceeds 4096 UTF-16 units")
    chat_id = setting(config, "chat_id", config.get("target"))
    result = post_json(
        f"https://api.telegram.org/bot{token}/sendMessage",
        {"chat_id": chat_id, "text": text},
        timeout_seconds(config),
    )
    message = result.get("result")
    if result.get("ok") is not True or not isinstance(message, Mapping):
        raise ExecutionError("Telegram rejected the message")
    message_id = message.get("message_id")
    if isinstance(message_id, bool) or not isinstance(message_id, int):
        raise ExecutionError("Telegram returned no message_id")
    return str(message_id)
