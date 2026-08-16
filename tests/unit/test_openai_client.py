"""Behavioral tests for the OpenAI-compatible v4 LLM boundary."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from urllib.error import HTTPError

import pytest

from sluice.contracts import Item
from sluice.llm import OpenAICompatibleClient
from sluice.tasks.llm import bind_llm


class _Response:
    def __init__(self, payload: dict[str, Any], status: int = 200):
        self._body = json.dumps(payload).encode("utf-8")
        self.status = status

    def __enter__(self) -> "_Response":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def read(self) -> bytes:
        return self._body

    def getcode(self) -> int:
        return self.status


def _item() -> Item:
    return Item(
        id="item-1",
        producer_scope=("source", "feed"),
        logical_id="logical-1",
        payload={"title": "hello"},
        metadata={},
        content_hash="hash",
        observed_at="2026-08-14T00:00:00Z",
        produced_by=1,
    )


def test_openai_client_posts_prompt_and_payload_and_parses_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests: list[tuple[str, dict[str, str], dict[str, Any], float]] = []

    def fake_urlopen(request: Any, timeout: float) -> _Response:
        body = json.loads(request.data.decode("utf-8"))
        requests.append((request.full_url, dict(request.headers), body, timeout))
        return _Response(
            {"choices": [{"message": {"content": '{"summary":"ok"}'}}]}
        )

    monkeypatch.setattr("sluice.llm.openai_compatible.urlopen", fake_urlopen)
    client = OpenAICompatibleClient(
        "https://llm.example/v1",
        api_key="secret",
        timeout=7.5,
        max_attempts=1,
    )

    result = client.complete("model-a", "Return JSON.", {"title": "hello"})

    assert result == {"summary": "ok"}
    assert requests == [
        (
            "https://llm.example/v1/chat/completions",
            {
                "Accept": "application/json",
                "Authorization": "Bearer secret",
                "Content-type": "application/json",
            },
            {
                "model": "model-a",
                "messages": [
                    {"role": "system", "content": "Return JSON."},
                    {"role": "user", "content": '{"title":"hello"}'},
                ],
            },
            7.5,
        )
    ]


def test_openai_client_retries_transient_http_errors_with_exponential_backoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts = 0
    delays: list[float] = []

    def fake_urlopen(request: Any, timeout: float) -> _Response:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise HTTPError(
                request.full_url,
                429,
                "rate limited",
                {"Retry-After": "0"},
                None,
            )
        return _Response({"choices": [{"message": {"content": '{"ok":true}'}}]})

    monkeypatch.setattr("sluice.llm.openai_compatible.urlopen", fake_urlopen)
    monkeypatch.setattr("sluice.llm.openai_compatible.sleep", delays.append)
    client = OpenAICompatibleClient(
        "https://llm.example/v1",
        max_attempts=3,
        backoff_seconds=0.25,
    )

    assert client.complete("model-a", "Return JSON.", {"x": 1}) == {"ok": True}
    assert attempts == 3
    assert delays == [0.25, 0.5]


def test_openai_client_does_not_retry_non_transient_http_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts = 0

    def fake_urlopen(request: Any, timeout: float) -> _Response:
        nonlocal attempts
        attempts += 1
        raise HTTPError(request.full_url, 401, "unauthorized", {}, None)

    monkeypatch.setattr("sluice.llm.openai_compatible.urlopen", fake_urlopen)
    client = OpenAICompatibleClient("https://llm.example/v1", max_attempts=4)

    with pytest.raises(Exception, match="401"):
        client.complete("model-a", "Return JSON.", {"x": 1})
    assert attempts == 1


def test_llm_task_uses_each_task_prompt_file(tmp_path: Path) -> None:
    prompt = tmp_path / "custom.md"
    prompt.write_text("Summarize only the title.", encoding="utf-8")

    class RecordingClient:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str, object]] = []

        def complete(self, model: str, text: str, payload: object) -> object:
            self.calls.append((model, text, payload))
            return {"summary": "ok"}

    client = RecordingClient()
    task = bind_llm(client)
    task.run(
        {"model": "model-a", "prompt": str(prompt), "output": {"summary": "string"}},
        _item(),
    )

    assert client.calls == [("model-a", "Summarize only the title.", {"title": "hello"})]
