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
    client: Callable[[Mapping[str, JSONValue], JSONValue, str], str | None],
) -> _TelegramSink:
    return _TelegramSink(client)
