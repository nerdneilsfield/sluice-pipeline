"""Token counting and truncation helpers.

Uses tiktoken with a cl100k_base encoder by default. The encoder choice is
approximate for non-OpenAI models — we only need a stable token estimate for
routing and overflow trimming, not exact billing accuracy.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Iterable

import tiktoken

_DEFAULT_ENCODER = "cl100k_base"


@lru_cache(maxsize=8)
def _get_encoder(name: str = _DEFAULT_ENCODER):
    try:
        return tiktoken.get_encoding(name)
    except Exception:
        return tiktoken.get_encoding(_DEFAULT_ENCODER)


def count_tokens(text: str, encoder: str = _DEFAULT_ENCODER) -> int:
    if not text:
        return 0
    return len(_get_encoder(encoder).encode(text))


def count_message_tokens(messages: Iterable[dict], encoder: str = _DEFAULT_ENCODER) -> int:
    enc = _get_encoder(encoder)
    total = 0
    for m in messages:
        content = m.get("content")
        if isinstance(content, str):
            total += len(enc.encode(content))
        elif isinstance(content, list):
            for part in content:
                if isinstance(part, dict):
                    text = part.get("text") or ""
                    if isinstance(text, str):
                        total += len(enc.encode(text))
        # +4 per message for role/separator overhead, OpenAI-style estimate
        total += 4
    return total


def truncate_to_tokens(text: str, max_tokens: int, encoder: str = _DEFAULT_ENCODER) -> str:
    if max_tokens <= 0:
        return ""
    enc = _get_encoder(encoder)
    tokens = enc.encode(text)
    if len(tokens) <= max_tokens:
        return text
    return enc.decode(tokens[:max_tokens])


def truncate_messages_to_tokens(
    messages: list[dict],
    max_tokens: int,
    encoder: str = _DEFAULT_ENCODER,
) -> list[dict]:
    """Trim message content from newest to oldest until the list fits."""
    if not messages:
        return messages

    out = [dict(message) for message in messages]
    if count_message_tokens(out, encoder) <= max_tokens:
        return out

    for idx in range(len(out) - 1, -1, -1):
        if count_message_tokens(out, encoder) <= max_tokens:
            break
        content = out[idx].get("content")
        if not isinstance(content, str):
            continue
        other = [message for j, message in enumerate(out) if j != idx]
        remaining = max(0, max_tokens - count_message_tokens(other, encoder) - 4)
        out[idx]["content"] = truncate_to_tokens(content, remaining, encoder)

    while len(out) > 1 and count_message_tokens(out, encoder) > max_tokens:
        out.pop(0)
    if count_message_tokens(out, encoder) > max_tokens:
        return []
    return out
