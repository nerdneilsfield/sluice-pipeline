"""Raw single-shot OpenAI-compatible chat call.

This module is the lowest layer: one model spec, one HTTP call, one provider key
chosen by ProviderPool. It owns NO retry, NO fallback, NO routing — those live
in :mod:`sluice.llm.middleware`. Stages should not use this directly.
"""

from dataclasses import dataclass

from sluice.core.errors import QuotaExhausted, RateLimitError
from sluice.llm.budget import CallCost, RunBudget, compute_cost
from sluice.llm.pool import ProviderPool
from sluice.llm.provider import Endpoint
from sluice.logging_setup import get_logger

log = get_logger(__name__)


@dataclass
class StageLLMConfig:
    """Per-stage LLM configuration consumed by the middleware.

    The retry/fallback chain lives here. The middleware also synthesises a
    long-context model into the chain when set.
    """

    model: str
    retry_model: str | None = None
    fallback_model: str | None = None
    fallback_model_2: str | None = None
    long_context_model: str | None = None
    timeout: float = 120.0
    same_model_retries: int = 2
    same_model_retry_backoff: float = 1.0
    overflow_trim_step_tokens: int = 100_000
    long_context_threshold_ratio: float = 0.8


@dataclass
class RawCallResult:
    text: str
    cost: CallCost
    model_spec: str
    endpoint: Endpoint


async def raw_chat(
    pool: ProviderPool,
    model_spec: str,
    messages: list[dict],
    *,
    timeout: float,
    budget: RunBudget | None = None,
) -> RawCallResult:
    """Single HTTP call to one model spec via the pool.

    Raises:
        RateLimitError: 429 without quota-error tokens
        QuotaExhausted: 429 with provider-configured quota tokens
        httpx.HTTPStatusError: for other 4xx/5xx
        httpx.NetworkError: connection-level failures
    """
    ep = pool.acquire(model_spec)
    log.bind(model=model_spec, base_url=ep.base_url, timeout=timeout).debug("llm.call_started")
    url = ep.base_url.rstrip("/") + "/chat/completions"
    headers = {
        "Authorization": f"Bearer {ep.api_key}",
        "Content-Type": "application/json",
        **ep.extra_headers,
    }
    payload = {"model": ep.model_entry.model_name, "messages": messages}
    r = await pool.client.post(url, headers=headers, json=payload, timeout=timeout)
    if r.status_code == 429:
        text = r.text.lower()
        if any(t in text for t in ep._key_ref.quota_error_tokens):
            pool.cool_down(ep)
            raise QuotaExhausted(text)
        raise RateLimitError(text)
    r.raise_for_status()
    data = r.json()
    usage = data.get("usage", {})
    cost = compute_cost(ep, usage.get("prompt_tokens", 0), usage.get("completion_tokens", 0))
    if budget:
        budget.record(cost)
    text_out = data["choices"][0]["message"]["content"]
    log.bind(model=model_spec, cost=cost).debug("llm.call_succeeded")
    return RawCallResult(text=text_out, cost=cost, model_spec=model_spec, endpoint=ep)
