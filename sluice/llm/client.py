from dataclasses import dataclass

import httpx

from sluice.core.errors import (
    AllProvidersExhausted,
    QuotaExhausted,
    RateLimitError,
)
from sluice.llm.budget import CallCost, RunBudget, compute_cost
from sluice.llm.pool import ProviderPool
from sluice.logging_setup import get_logger

log = get_logger(__name__)


@dataclass
class StageLLMConfig:
    model: str
    retry_model: str | None = None
    fallback_model: str | None = None
    fallback_model_2: str | None = None
    timeout: float = 120.0


class LLMClient:
    def __init__(self, pool: ProviderPool, cfg: StageLLMConfig, budget: RunBudget | None = None):
        self.pool = pool
        self.cfg = cfg
        self.budget = budget
        self.last_cost: CallCost | None = None

    def _chain(self):
        return [
            m
            for m in (
                self.cfg.model,
                self.cfg.retry_model,
                self.cfg.fallback_model,
                self.cfg.fallback_model_2,
            )
            if m
        ]

    async def chat(self, messages: list[dict]) -> str:
        chain = self._chain()
        for spec in chain:
            ep = self.pool.acquire(spec)
            log.bind(model=spec, base_url=ep.base_url, timeout=self.cfg.timeout).debug(
                "llm.call_started"
            )
            try:
                out = await self._call(ep, messages)
                log.bind(model=spec, cost=self.last_cost).debug("llm.call_succeeded")
                return out
            except (RateLimitError, QuotaExhausted, httpx.NetworkError, httpx.HTTPStatusError) as e:
                if isinstance(e, httpx.HTTPStatusError):
                    # 4xx client errors (401/403) = bad key/config → fail fast
                    if 400 <= e.response.status_code < 500 and e.response.status_code != 429:
                        raise
                log.bind(
                    model=spec,
                    error_class=type(e).__name__,
                    error=str(e),
                ).info("llm.call_retryable_failure")
                if isinstance(e, QuotaExhausted):
                    self.pool.cool_down(ep)
                continue
        log.bind(model_chain=chain).error("llm.providers_exhausted")
        raise AllProvidersExhausted(chain)

    async def _call(self, ep, messages: list[dict]) -> str:
        url = ep.base_url.rstrip("/") + "/chat/completions"
        headers = {
            "Authorization": f"Bearer {ep.api_key}",
            "Content-Type": "application/json",
            **ep.extra_headers,
        }
        payload = {"model": ep.model_entry.model_name, "messages": messages}
        r = await self.pool.client.post(
            url,
            headers=headers,
            json=payload,
            timeout=self.cfg.timeout,
        )
        if r.status_code == 429:
            text = r.text.lower()
            if any(t in text for t in ep._key_ref.quota_error_tokens):
                raise QuotaExhausted(text)
            raise RateLimitError(text)
        r.raise_for_status()
        data = r.json()
        usage = data.get("usage", {})
        self.last_cost = compute_cost(
            ep, usage.get("prompt_tokens", 0), usage.get("completion_tokens", 0)
        )
        if self.budget:
            self.budget.record(self.last_cost)
        return data["choices"][0]["message"]["content"]
