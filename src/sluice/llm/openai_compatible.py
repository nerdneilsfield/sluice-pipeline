"""Synchronous client for OpenAI-compatible ``/chat/completions`` endpoints.

The v4 Task contract is synchronous and durable Stage retries are owned by the
runtime.  This client therefore only retries transient transport/endpoint
failures within one call; it does not implement provider fallback or Stage
retry policy.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from time import sleep
from types import MappingProxyType
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from ..canonical import canonical_json
from ..contracts import JSONValue
from ..errors import ExecutionError, InvalidStructuredOutput

_RETRYABLE_HTTP_STATUS = frozenset({408, 429}) | frozenset(range(500, 600))


def _strip_json_fence(value: str) -> str:
    text = value.strip()
    if not text.startswith("```"):
        return text
    lines = text.splitlines()
    if lines and lines[0].strip().lower() in {"```", "```json"}:
        lines = lines[1:]
    if lines and lines[-1].strip() == "```":
        lines = lines[:-1]
    return "\n".join(lines).strip()


def _decode_completion(body: bytes) -> JSONValue:
    try:
        response = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise ExecutionError("OpenAI-compatible response is not valid JSON") from error
    if not isinstance(response, Mapping):
        raise ExecutionError("OpenAI-compatible response must be an object")
    choices = response.get("choices")
    if not isinstance(choices, list) or not choices:
        raise ExecutionError("OpenAI-compatible response has no choices")
    choice = choices[0]
    if not isinstance(choice, Mapping):
        raise ExecutionError("OpenAI-compatible response choice must be an object")
    message = choice.get("message")
    if not isinstance(message, Mapping):
        raise ExecutionError("OpenAI-compatible response has no message")
    content = message.get("content")
    if isinstance(content, str):
        content = _strip_json_fence(content)
        try:
            content = json.loads(content)
        except (TypeError, ValueError, json.JSONDecodeError) as error:
            raise InvalidStructuredOutput(
                "OpenAI-compatible response content is not valid JSON"
            ) from error
    try:
        canonical_json(content)
    except ExecutionError as error:
        raise InvalidStructuredOutput(
            "OpenAI-compatible response content must be JSON-compatible"
        ) from error
    return content  # type: ignore[return-value]


@dataclass(frozen=True, slots=True)
class OpenAICompatibleClient:
    """Call one OpenAI-compatible endpoint with bounded transient retries.

    ``max_attempts`` is the total number of HTTP attempts, including the first
    call.  A Task's ``prompt`` is sent as a system message and its immutable
    input payload is sent as canonical JSON in a separate user message.
    """

    base_url: str
    api_key: str | None = field(default=None, repr=False)
    timeout: float = 60.0
    max_attempts: int = 3
    backoff_seconds: float = 1.0
    max_backoff_seconds: float = 30.0
    extra_headers: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.base_url, str) or not self.base_url.strip():
            raise ValueError("base_url must be a non-empty string")
        if self.api_key is not None and not isinstance(self.api_key, str):
            raise ValueError("api_key must be a string or None")
        if not isinstance(self.timeout, (int, float)) or self.timeout <= 0:
            raise ValueError("timeout must be positive")
        if type(self.max_attempts) is not int or self.max_attempts < 1:
            raise ValueError("max_attempts must be a positive integer")
        if (
            not isinstance(self.backoff_seconds, (int, float))
            or self.backoff_seconds < 0
        ):
            raise ValueError("backoff_seconds must be non-negative")
        if (
            not isinstance(self.max_backoff_seconds, (int, float))
            or self.max_backoff_seconds < 0
        ):
            raise ValueError("max_backoff_seconds must be non-negative")
        if not isinstance(self.extra_headers, Mapping):
            raise TypeError("extra_headers must be a mapping")
        if any(not isinstance(key, str) for key in self.extra_headers):
            raise TypeError("extra_headers keys must be strings")
        if any(not isinstance(value, str) for value in self.extra_headers.values()):
            raise TypeError("extra_headers values must be strings")
        object.__setattr__(self, "base_url", self.base_url.strip().rstrip("/"))
        object.__setattr__(
            self, "extra_headers", MappingProxyType(dict(self.extra_headers))
        )

    def complete(
        self,
        model: str,
        prompt: str,
        input_payload: JSONValue,
    ) -> JSONValue:
        """Return the first JSON value in a successful chat completion."""

        if not isinstance(model, str) or not model:
            raise ExecutionError("LLM model must be a non-empty string")
        if not isinstance(prompt, str) or not prompt:
            raise ExecutionError("LLM prompt must be a non-empty string")
        request_payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": prompt},
                {"role": "user", "content": canonical_json(input_payload)},
            ],
        }
        body = json.dumps(
            request_payload,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        headers.update(self.extra_headers)
        request = Request(
            f"{self.base_url}/chat/completions",
            data=body,
            headers=headers,
            method="POST",
        )
        return _decode_completion(self._post(request))

    def _post(self, request: Request) -> bytes:
        last_error: BaseException | None = None
        for attempt in range(self.max_attempts):
            try:
                with urlopen(request, timeout=self.timeout) as response:
                    status = response.getcode() or 200
                    body = response.read()
                if status >= 400:
                    description = f"HTTP {status}"
                    if status not in _RETRYABLE_HTTP_STATUS:
                        raise ExecutionError(
                            f"OpenAI-compatible endpoint returned {description}"
                        )
                    last_error = ExecutionError(
                        f"OpenAI-compatible endpoint returned {description}"
                    )
                else:
                    return body
            except HTTPError as error:
                if error.code not in _RETRYABLE_HTTP_STATUS:
                    raise ExecutionError(
                        f"OpenAI-compatible endpoint returned HTTP {error.code}"
                    ) from error
                last_error = error
            except (URLError, TimeoutError, OSError) as error:
                last_error = error

            if attempt + 1 >= self.max_attempts:
                break
            delay = min(
                float(self.max_backoff_seconds),
                float(self.backoff_seconds) * (2**attempt),
            )
            if delay > 0:
                sleep(delay)

        raise ExecutionError(
            f"OpenAI-compatible endpoint failed after {self.max_attempts} attempts"
        ) from last_error
