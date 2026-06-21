"""Raw single-shot client tests. Retry/fallback semantics live in test_middleware.py."""

import json

import httpx
import pytest
import respx

from sluice.config import BaseEndpoint, KeyConfig, ModelEntry, Provider, ProvidersConfig
from sluice.core.errors import BudgetExceeded, QuotaExhausted, RateLimitError
from sluice.llm.budget import RunBudget
from sluice.llm.client import raw_chat
from sluice.llm.pool import ProviderPool


def make_pool(
    monkeypatch,
    *,
    quota_tokens: list[str] | None = None,
    input_price_per_1k: float = 0.0,
    output_price_per_1k: float = 0.0,
) -> ProviderPool:
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
                    models=[
                        ModelEntry(
                            model_name="m1",
                            input_price_per_1k=input_price_per_1k,
                            output_price_per_1k=output_price_per_1k,
                        )
                    ],
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
        assert json.loads(r.calls[0].request.content)["max_tokens"] == 4096


@pytest.mark.asyncio
async def test_raw_chat_rejects_projected_cost_before_request(monkeypatch):
    pool = make_pool(monkeypatch, input_price_per_1k=1.0, output_price_per_1k=1.0)
    budget = RunBudget(max_calls=10, max_usd=0.001)
    with respx.mock(base_url="https://main", assert_all_called=False) as r:
        route = r.post("/chat/completions").mock(return_value=httpx.Response(500, text="nope"))

        with pytest.raises(BudgetExceeded):
            await raw_chat(
                pool,
                "p1/m1",
                [{"role": "user", "content": "hello"}],
                timeout=5,
                budget=budget,
            )

        assert route.call_count == 0
        assert budget.calls == 0


@pytest.mark.asyncio
async def test_raw_chat_rejects_actual_cost_over_budget(monkeypatch):
    pool = make_pool(monkeypatch, input_price_per_1k=0.0, output_price_per_1k=1.0)
    budget = RunBudget(max_calls=10, max_usd=5.0)
    with respx.mock(base_url="https://main") as r:
        r.post("/chat/completions").mock(
            return_value=httpx.Response(
                200,
                json={
                    "choices": [{"message": {"content": "huge"}}],
                    "usage": {"prompt_tokens": 1, "completion_tokens": 6000},
                },
            )
        )

        with pytest.raises(BudgetExceeded):
            await raw_chat(
                pool,
                "p1/m1",
                [{"role": "user", "content": "hello"}],
                timeout=5,
                budget=budget,
            )

        assert budget.calls == 0


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
