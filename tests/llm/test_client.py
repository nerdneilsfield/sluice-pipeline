import httpx
import pytest
import respx

from sluice.config import BaseEndpoint, KeyConfig, ModelEntry, Provider, ProvidersConfig
from sluice.core.errors import AllProvidersExhausted
from sluice.llm.client import LLMClient, StageLLMConfig
from sluice.llm.pool import ProviderPool


def make_pool(monkeypatch):
    monkeypatch.setenv("K", "v")
    return ProviderPool(
        ProvidersConfig(
            providers=[
                Provider(
                    name="p1",
                    type="openai_compatible",
                    base=[
                        BaseEndpoint(
                            url="https://main", weight=1, key=[KeyConfig(value="env:K", weight=1)]
                        )
                    ],
                    models=[ModelEntry(model_name="m1")],
                ),
                Provider(
                    name="p2",
                    type="openai_compatible",
                    base=[
                        BaseEndpoint(
                            url="https://fb", weight=1, key=[KeyConfig(value="env:K", weight=1)]
                        )
                    ],
                    models=[ModelEntry(model_name="m2")],
                ),
            ]
        )
    )


@pytest.mark.asyncio
async def test_chat_success(monkeypatch):
    pool = make_pool(monkeypatch)
    cfg = StageLLMConfig(model="p1/m1")
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
        client = LLMClient(pool, cfg)
        out = await client.chat([{"role": "user", "content": "hello"}])
        assert out == "hi"


@pytest.mark.asyncio
async def test_falls_through_to_fallback(monkeypatch):
    pool = make_pool(monkeypatch)
    cfg = StageLLMConfig(model="p1/m1", fallback_model="p2/m2")
    with respx.mock() as r:
        r.post("https://main/chat/completions").mock(return_value=httpx.Response(429))
        r.post("https://fb/chat/completions").mock(
            return_value=httpx.Response(
                200,
                json={
                    "choices": [{"message": {"content": "fb"}}],
                    "usage": {"prompt_tokens": 1, "completion_tokens": 1},
                },
            )
        )
        out = await LLMClient(pool, cfg).chat([{"role": "user", "content": "x"}])
        assert out == "fb"


@pytest.mark.asyncio
async def test_500_errors_trigger_fallback(monkeypatch):
    pool = make_pool(monkeypatch)
    cfg = StageLLMConfig(model="p1/m1", fallback_model="p2/m2")
    with respx.mock() as r:
        r.post("https://main/chat/completions").mock(
            return_value=httpx.Response(500, text="internal error")
        )
        r.post("https://fb/chat/completions").mock(
            return_value=httpx.Response(
                200,
                json={
                    "choices": [{"message": {"content": "fb-recovery"}}],
                    "usage": {"prompt_tokens": 1, "completion_tokens": 1},
                },
            )
        )
        out = await LLMClient(pool, cfg).chat([{"role": "user", "content": "x"}])
    assert out == "fb-recovery"


@pytest.mark.asyncio
async def test_all_exhausted_raises(monkeypatch):
    pool = make_pool(monkeypatch)
    cfg = StageLLMConfig(model="p1/m1", fallback_model="p2/m2")
    with respx.mock() as r:
        r.post("https://main/chat/completions").mock(return_value=httpx.Response(429))
        r.post("https://fb/chat/completions").mock(return_value=httpx.Response(429))
        with pytest.raises(AllProvidersExhausted):
            await LLMClient(pool, cfg).chat([{"role": "user", "content": "x"}])


@pytest.mark.asyncio
async def test_401_403_fail_fast_no_fallback(monkeypatch):
    """4xx client errors (except 429) should fail immediately, not burn fallback quota."""
    pool = make_pool(monkeypatch)
    cfg = StageLLMConfig(model="p1/m1", fallback_model="p2/m2")
    with respx.mock(assert_all_mocked=False) as r:
        r.post("https://main/chat/completions").mock(
            return_value=httpx.Response(401, text="unauthorized")
        )
        # fallback should NOT be called — don't register it in respx
        with pytest.raises(httpx.HTTPStatusError):
            await LLMClient(pool, cfg).chat([{"role": "user", "content": "x"}])
