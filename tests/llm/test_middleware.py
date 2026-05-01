import json

import httpx
import pytest
import respx

from sluice.config import BaseEndpoint, KeyConfig, ModelEntry, Provider, ProvidersConfig
from sluice.core.errors import AllProvidersExhausted, ConfigError
from sluice.llm.client import StageLLMConfig
from sluice.llm.middleware import LLMMiddleware
from sluice.llm.pool import ProviderPool
from sluice.llm.tokens import count_message_tokens


def _ok(text: str = "ok") -> httpx.Response:
    return httpx.Response(
        200,
        json={
            "choices": [{"message": {"content": text}}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1},
        },
    )


def make_pool(
    monkeypatch, *, primary_input: int = 10_000, long_ctx_input: int = 1_000_000
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
                            key=[KeyConfig(value="env:K", weight=1)],
                        )
                    ],
                    models=[ModelEntry(model_name="m1", max_input_tokens=primary_input)],
                ),
                Provider(
                    name="p2",
                    type="openai_compatible",
                    base=[
                        BaseEndpoint(
                            url="https://fb",
                            weight=1,
                            key=[KeyConfig(value="env:K", weight=1)],
                        )
                    ],
                    models=[ModelEntry(model_name="m2", max_input_tokens=10_000)],
                ),
                Provider(
                    name="p3",
                    type="openai_compatible",
                    base=[
                        BaseEndpoint(
                            url="https://lc",
                            weight=1,
                            key=[KeyConfig(value="env:K", weight=1)],
                        )
                    ],
                    models=[ModelEntry(model_name="lc", max_input_tokens=long_ctx_input)],
                ),
            ]
        )
    )


@pytest.mark.asyncio
async def test_chat_success(monkeypatch):
    pool = make_pool(monkeypatch)
    cfg = StageLLMConfig(model="p1/m1", same_model_retries=0)
    with respx.mock(base_url="https://main") as r:
        r.post("/chat/completions").mock(return_value=_ok("hi"))
        out = await LLMMiddleware(pool, cfg).chat([{"role": "user", "content": "hello"}])
        assert out == "hi"


@pytest.mark.asyncio
async def test_falls_through_to_fallback_on_quota(monkeypatch):
    pool = make_pool(monkeypatch)
    # configure quota tokens so 429 → QuotaExhausted → advances chain
    pool.runtimes["p1"].provider.base[0].key[0].quota_error_tokens = ["quota"]
    cfg = StageLLMConfig(model="p1/m1", fallback_model="p2/m2", same_model_retries=0)
    with respx.mock(assert_all_called=False) as r:
        r.post("https://main/chat/completions").mock(
            return_value=httpx.Response(429, text="quota exceeded")
        )
        r.post("https://fb/chat/completions").mock(return_value=_ok("fb"))
        out = await LLMMiddleware(pool, cfg).chat([{"role": "user", "content": "x"}])
        assert out == "fb"


@pytest.mark.asyncio
async def test_500_retried_then_fallback(monkeypatch):
    pool = make_pool(monkeypatch)
    cfg = StageLLMConfig(
        model="p1/m1",
        fallback_model="p2/m2",
        same_model_retries=1,
        same_model_retry_backoff=0.0,
    )
    with respx.mock() as r:
        r.post("https://main/chat/completions").mock(return_value=httpx.Response(500, text="boom"))
        r.post("https://fb/chat/completions").mock(return_value=_ok("fb-ok"))
        out = await LLMMiddleware(pool, cfg).chat([{"role": "user", "content": "x"}])
        assert out == "fb-ok"


@pytest.mark.asyncio
async def test_401_advances_chain(monkeypatch):
    """Auth error now switches to next model instead of failing fast."""
    pool = make_pool(monkeypatch)
    cfg = StageLLMConfig(model="p1/m1", fallback_model="p2/m2", same_model_retries=0)
    with respx.mock() as r:
        r.post("https://main/chat/completions").mock(
            return_value=httpx.Response(401, text="unauthorized")
        )
        r.post("https://fb/chat/completions").mock(return_value=_ok("after-auth"))
        out = await LLMMiddleware(pool, cfg).chat([{"role": "user", "content": "x"}])
        assert out == "after-auth"


@pytest.mark.asyncio
async def test_all_exhausted_raises(monkeypatch):
    pool = make_pool(monkeypatch)
    pool.runtimes["p1"].provider.base[0].key[0].quota_error_tokens = ["quota"]
    pool.runtimes["p2"].provider.base[0].key[0].quota_error_tokens = ["quota"]
    cfg = StageLLMConfig(model="p1/m1", fallback_model="p2/m2", same_model_retries=0)
    with respx.mock() as r:
        r.post("https://main/chat/completions").mock(return_value=httpx.Response(429, text="quota"))
        r.post("https://fb/chat/completions").mock(return_value=httpx.Response(429, text="quota"))
        with pytest.raises(AllProvidersExhausted):
            await LLMMiddleware(pool, cfg).chat([{"role": "user", "content": "x"}])


@pytest.mark.asyncio
async def test_config_error_fails_fast_without_fallback(monkeypatch):
    pool = make_pool(monkeypatch)
    cfg = StageLLMConfig(model="missing/m", fallback_model="p2/m2", same_model_retries=0)
    with respx.mock(assert_all_called=False) as r:
        fb_route = r.post("https://fb/chat/completions").mock(return_value=_ok("fb"))
        with pytest.raises(ConfigError):
            await LLMMiddleware(pool, cfg).chat([{"role": "user", "content": "x"}])
        assert fb_route.call_count == 0


@pytest.mark.asyncio
async def test_overflow_400_switches_to_long_context(monkeypatch):
    pool = make_pool(monkeypatch)
    cfg = StageLLMConfig(
        model="p1/m1",
        long_context_model="p3/lc",
        same_model_retries=0,
    )
    with respx.mock(assert_all_called=False) as r:
        r.post("https://main/chat/completions").mock(
            return_value=httpx.Response(400, text="context_length exceeded")
        )
        r.post("https://lc/chat/completions").mock(return_value=_ok("from-lc"))
        out = await LLMMiddleware(pool, cfg).chat([{"role": "user", "content": "tiny"}])
        assert out == "from-lc"


@pytest.mark.asyncio
async def test_overflow_skips_intermediate_fallback(monkeypatch):
    pool = make_pool(monkeypatch)
    cfg = StageLLMConfig(
        model="p1/m1",
        fallback_model="p2/m2",
        long_context_model="p3/lc",
        same_model_retries=0,
    )
    with respx.mock(assert_all_called=False) as r:
        r.post("https://main/chat/completions").mock(
            return_value=httpx.Response(400, text="context_length exceeded")
        )
        fb_route = r.post("https://fb/chat/completions").mock(return_value=_ok("fb"))
        r.post("https://lc/chat/completions").mock(return_value=_ok("from-lc"))
        out = await LLMMiddleware(pool, cfg).chat([{"role": "user", "content": "tiny"}])
        assert out == "from-lc"
        assert fb_route.call_count == 0


@pytest.mark.asyncio
async def test_token_routing_uses_long_context_directly(monkeypatch):
    pool = make_pool(monkeypatch)
    cfg = StageLLMConfig(
        model="p1/m1",  # cap 10k → threshold 8k
        long_context_model="p3/lc",
        same_model_retries=0,
    )
    big = "word " * 20_000  # > 8k tokens
    with respx.mock(assert_all_called=False) as r:
        # main must NOT be called
        main_route = r.post("https://main/chat/completions").mock(return_value=_ok("main"))
        r.post("https://lc/chat/completions").mock(return_value=_ok("routed"))
        out = await LLMMiddleware(pool, cfg).chat([{"role": "user", "content": big}])
        assert out == "routed"
        assert main_route.call_count == 0


@pytest.mark.asyncio
async def test_overflow_on_long_context_trims_and_retries(monkeypatch):
    pool = make_pool(monkeypatch, long_ctx_input=1_000_000)
    cfg = StageLLMConfig(
        model="p3/lc",  # already on long-context
        same_model_retries=2,
        same_model_retry_backoff=0.0,
        overflow_trim_step_tokens=50,
    )
    calls = {"n": 0}
    bodies = []

    def handler(request):
        calls["n"] += 1
        bodies.append(json.loads(request.content))
        if calls["n"] == 1:
            return httpx.Response(400, text="input_tokens_limit reached")
        return _ok("trimmed-ok")

    with respx.mock() as r:
        r.post("https://lc/chat/completions").mock(side_effect=handler)
        out = await LLMMiddleware(pool, cfg).chat(
            [{"role": "user", "content": "hello world " * 200}]
        )
    assert out == "trimmed-ok"
    assert calls["n"] == 2
    first = bodies[0]["messages"][0]["content"]
    second = bodies[1]["messages"][0]["content"]
    assert len(second) < len(first)


@pytest.mark.asyncio
async def test_overflow_trim_retries_once_when_same_model_retries_zero(monkeypatch):
    pool = make_pool(monkeypatch, long_ctx_input=1_000_000)
    cfg = StageLLMConfig(
        model="p3/lc",
        same_model_retries=0,
        same_model_retry_backoff=0.0,
        overflow_trim_step_tokens=50,
    )
    bodies = []

    def handler(request):
        bodies.append(json.loads(request.content))
        if len(bodies) == 1:
            return httpx.Response(400, text="input_tokens_limit reached")
        return _ok("trimmed-ok")

    with respx.mock() as r:
        r.post("https://lc/chat/completions").mock(side_effect=handler)
        out = await LLMMiddleware(pool, cfg).chat(
            [{"role": "user", "content": "hello world " * 200}]
        )
    assert out == "trimmed-ok"
    assert len(bodies) == 2
    assert len(bodies[1]["messages"][0]["content"]) < len(bodies[0]["messages"][0]["content"])


@pytest.mark.asyncio
async def test_overflow_trims_when_prompt_below_naive_floor(monkeypatch):
    pool = make_pool(monkeypatch, long_ctx_input=1_000_000)
    cfg = StageLLMConfig(
        model="p3/lc",
        same_model_retries=2,
        same_model_retry_backoff=0.0,
        overflow_trim_step_tokens=50,
    )
    bodies = []

    def handler(request):
        bodies.append(json.loads(request.content))
        if len(bodies) == 1:
            return httpx.Response(400, text="input_tokens_limit reached")
        return _ok("trimmed-ok")

    with respx.mock() as r:
        r.post("https://lc/chat/completions").mock(side_effect=handler)
        out = await LLMMiddleware(pool, cfg).chat(
            [{"role": "user", "content": "small prompt that is nowhere near ten thousand tokens"}]
        )
    assert out == "trimmed-ok"
    first = bodies[0]["messages"][0]["content"]
    second = bodies[1]["messages"][0]["content"]
    assert len(second) < len(first)


@pytest.mark.asyncio
async def test_preemptive_fit_respects_small_model_cap(monkeypatch):
    pool = make_pool(monkeypatch, primary_input=100)
    cfg = StageLLMConfig(model="p1/m1", same_model_retries=0)
    bodies = []

    def handler(request):
        bodies.append(json.loads(request.content))
        return _ok("fit-ok")

    with respx.mock() as r:
        r.post("https://main/chat/completions").mock(side_effect=handler)
        out = await LLMMiddleware(pool, cfg).chat(
            [
                {"role": "system", "content": "system " * 5000},
                {"role": "user", "content": "user " * 5000},
            ]
        )
    assert out == "fit-ok"
    sent_tokens = count_message_tokens(bodies[0]["messages"])
    assert sent_tokens <= pool.model_entry("p1/m1").max_input_tokens
    assert sent_tokens < count_message_tokens(
        [
            {"role": "system", "content": "system " * 5000},
            {"role": "user", "content": "user " * 5000},
        ]
    )


@pytest.mark.asyncio
async def test_400_non_overflow_fails_fast(monkeypatch):
    pool = make_pool(monkeypatch)
    cfg = StageLLMConfig(model="p1/m1", fallback_model="p2/m2", same_model_retries=0)
    with respx.mock(assert_all_mocked=False) as r:
        r.post("https://main/chat/completions").mock(
            return_value=httpx.Response(400, text="invalid request: bad role")
        )
        # fallback must NOT be called
        with pytest.raises(httpx.HTTPStatusError):
            await LLMMiddleware(pool, cfg).chat([{"role": "user", "content": "x"}])
