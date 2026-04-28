from dataclasses import dataclass
import httpx
from sluice.llm.pool import ProviderPool
from sluice.llm.budget import compute_cost, CallCost, RunBudget
from sluice.core.errors import (
    RateLimitError, QuotaExhausted, AllProvidersExhausted,
)

@dataclass
class StageLLMConfig:
    model: str
    retry_model: str | None = None
    fallback_model: str | None = None
    fallback_model_2: str | None = None
    timeout: float = 60.0

class LLMClient:
    def __init__(self, pool: ProviderPool, cfg: StageLLMConfig,
                 budget: RunBudget | None = None):
        self.pool = pool
        self.cfg = cfg
        self.budget = budget
        self.last_cost: CallCost | None = None

    def _chain(self):
        return [m for m in (
            self.cfg.model, self.cfg.retry_model,
            self.cfg.fallback_model, self.cfg.fallback_model_2) if m]

    async def chat(self, messages: list[dict]) -> str:
        chain = self._chain()
        for spec in chain:
            ep = self.pool.acquire(spec)
            try:
                return await self._call(ep, messages)
            except (RateLimitError, QuotaExhausted, httpx.NetworkError, httpx.HTTPStatusError) as e:
                if isinstance(e, httpx.HTTPStatusError):
                    # 4xx client errors (401/403) = bad key/config → fail fast
                    if 400 <= e.response.status_code < 500 and e.response.status_code != 429:
                        raise
                if isinstance(e, QuotaExhausted):
                    self.pool.cool_down(ep)
                continue
        raise AllProvidersExhausted(chain)

    async def _call(self, ep, messages: list[dict]) -> str:
        url = ep.base_url.rstrip("/") + "/chat/completions"
        headers = {"Authorization": f"Bearer {ep.api_key}",
                   "Content-Type": "application/json", **ep.extra_headers}
        payload = {"model": ep.model_entry.model_name, "messages": messages}
        r = await self.pool.client.post(url, headers=headers, json=payload)
        if r.status_code == 429:
            text = r.text.lower()
            if any(t in text for t in ep._key_ref.quota_error_tokens):
                raise QuotaExhausted(text)
            raise RateLimitError(text)
        r.raise_for_status()
        data = r.json()
        usage = data.get("usage", {})
        self.last_cost = compute_cost(ep,
                                      usage.get("prompt_tokens", 0),
                                      usage.get("completion_tokens", 0))
        if self.budget:
            self.budget.record(self.last_cost)
        return data["choices"][0]["message"]["content"]
