"""Configuration and text handling shared by the built-in senders."""

from __future__ import annotations

import os
from collections.abc import Mapping

import httpx

from ..canonical import canonical_json
from ..errors import ExecutionError


def setting(config: Mapping, name: str, default: str | None = None) -> str:
    value = config.get(name, default)
    if isinstance(value, str) and value.startswith("env:"):
        value = os.environ.get(value[4:])
    if not isinstance(value, str) or not value.strip():
        raise ExecutionError(f"Sink requires {name}")
    return value


def message_text(payload: object) -> str:
    if isinstance(payload, str):
        text = payload
    elif isinstance(payload, Mapping) and isinstance(payload.get("content"), str):
        text = payload["content"]
    else:
        text = canonical_json(payload)
    if not text.strip():
        raise ExecutionError("Sink message is empty")
    return text


def timeout_seconds(config: Mapping) -> float:
    value = config.get("timeout_seconds", 30)
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not 0 < value <= 300
    ):
        raise ExecutionError("Sink timeout_seconds must be in (0, 300]")
    return float(value)


def post_json(url: str, body: dict, timeout: float) -> dict:
    # No redirects: tokens are embedded in Telegram and webhook URLs. Do not
    # persist response bodies or transport exceptions containing those URLs.
    try:
        with httpx.Client(
            timeout=timeout, follow_redirects=False, trust_env=False
        ) as client:
            response = client.post(url, json=body)
            response.raise_for_status()
            result = response.json()
        if not isinstance(result, dict):
            raise ExecutionError("Sink returned an invalid response")
        return result
    except (httpx.HTTPError, ValueError):
        raise ExecutionError("Sink HTTP request failed") from None
