"""Raw single-shot client tests. Retry/fallback semantics live in test_middleware.py."""

import httpx
import pytest
import respx

from sluice.config import BaseEndpoint, KeyConfig, ModelEntry, Provider, ProvidersConfig
from sluice.core.errors import QuotaExhausted, RateLimitError
from sluice.llm.client import raw_chat
from sluice.llm.pool import ProviderPool


def make_pool(monkeypatch, *, quota_tokens: list[str] | None = None) -> ProviderPool:
    monkeypatch.setenv("K", "v")
    return ProviderPool(
        ProvidersConfig(
            providers=[
                Provider(
                    name="p1",
                    type="openai_compatible",
                    base=[
                        BaseEndpoint(
                            url="https://main",
                            weight=1,
                            key=[
                                KeyConfig(
                                    value="env:K",
                                    weight=1,
                                    quota_error_tokens=quota_tokens or [],
                                )
                            ],
                        )
                    ],
                    models=[ModelEntry(model_name="m1")],
                ),
            ]
        )
    )


@pytest.mark.asyncio
async def test_raw_chat_success(monkeypatch):
    pool = make_pool(monkeypatch)
    with respx.mock(base_url="https://main") as r:
        r.post("/chat/completions").mock(
            return_value=httpx.Response(
                200,
                json={
                    "choices": [{"message": {"content": "hi"}}],
                    "usage": {"prompt_tokens": 10, "completion_tokens": 2},
                },
            )
        )
        res = await raw_chat(pool, "p1/m1", [{"role": "user", "content": "x"}], timeout=5)
        assert res.text == "hi"
        assert res.cost.prompt_tokens == 10


@pytest.mark.asyncio
async def test_raw_chat_429_rate_limit(monkeypatch):
    pool = make_pool(monkeypatch)
    with respx.mock(base_url="https://main") as r:
        r.post("/chat/completions").mock(return_value=httpx.Response(429, text="slow down"))
        with pytest.raises(RateLimitError):
            await raw_chat(pool, "p1/m1", [{"role": "user", "content": "x"}], timeout=5)


@pytest.mark.asyncio
async def test_raw_chat_429_quota_exhausted(monkeypatch):
    pool = make_pool(monkeypatch, quota_tokens=["quota_exceeded"])
    with respx.mock(base_url="https://main") as r:
        r.post("/chat/completions").mock(
            return_value=httpx.Response(429, text="quota_exceeded for today")
        )
        with pytest.raises(QuotaExhausted):
            await raw_chat(pool, "p1/m1", [{"role": "user", "content": "x"}], timeout=5)


@pytest.mark.asyncio
async def test_raw_chat_4xx_raises_status(monkeypatch):
    pool = make_pool(monkeypatch)
    with respx.mock(base_url="https://main") as r:
        r.post("/chat/completions").mock(return_value=httpx.Response(401, text="unauthorized"))
        with pytest.raises(httpx.HTTPStatusError):
            await raw_chat(pool, "p1/m1", [{"role": "user", "content": "x"}], timeout=5)
