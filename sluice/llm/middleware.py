"""LLM middleware: routing, retry, fallback, and overflow handling.

This is the only LLM call path stages should use. Responsibilities:

* **Routing** — if the input exceeds ``long_context_threshold_ratio`` of the
  primary model's input cap and ``long_context_model`` is set, route the call
  there directly.
* **Same-model retry** — transient errors (network, rate limit, 5xx) are
  retried in place ``same_model_retries`` times with exponential backoff.
* **Model fallback** — on auth failure (401/403), provider-quota exhaustion,
  or after same-model retries are exhausted, advance to the next model in the
  chain. The chain is: primary → retry → fallback → fallback_2 → long_context.
* **Overflow recovery** — if a 4xx looks like context-too-long, switch to the
  long-context model first; if already there or no long-context configured,
  trim the prompt by ``overflow_trim_step_tokens`` and retry.

The class exposes a ``chat(messages) -> str`` interface so it is a drop-in
replacement for the old ``LLMClient`` consumed by stage processors. ``last_cost``
is set after each successful call.
"""

from __future__ import annotations

import asyncio

import httpx

from sluice.core.errors import (
    AllProvidersExhausted,
    QuotaExhausted,
    RateLimitError,
)
from sluice.llm.budget import CallCost, RunBudget
from sluice.llm.client import StageLLMConfig, raw_chat
from sluice.llm.pool import ProviderPool
from sluice.llm.tokens import (
    count_message_tokens,
    truncate_messages_to_tokens,
)
from sluice.logging_setup import get_logger

log = get_logger(__name__)


_OVERFLOW_KEYWORDS = (
    "context_length",
    "context length",
    "too long",
    "maximum context",
    "input length",
    "range of input",
    "token limit",
    "max_tokens",
    "input_tokens_limit",
    "request_tokens_limit",
)

_EXPECTED_CALL_ERRORS = (
    QuotaExhausted,
    RateLimitError,
    httpx.NetworkError,
    httpx.TimeoutException,
    httpx.HTTPStatusError,
)


def _is_context_overflow(exc: Exception) -> bool:
    if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code == 400:
        msg = (exc.response.text or "").lower()
        return any(k in msg for k in _OVERFLOW_KEYWORDS)
    msg = str(exc).lower()
    return any(k in msg for k in _OVERFLOW_KEYWORDS)


def _is_auth_error(exc: Exception) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in (401, 403)
    return False


def _is_retryable_same_model(exc: Exception) -> bool:
    if isinstance(exc, (RateLimitError, httpx.NetworkError, httpx.TimeoutException)):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code >= 500
    return False


class LLMMiddleware:
    """Routing + retry + fallback + overflow handling around :func:`raw_chat`."""

    def __init__(
        self,
        pool: ProviderPool,
        cfg: StageLLMConfig,
        budget: RunBudget | None = None,
    ):
        self.pool = pool
        self.cfg = cfg
        self.budget = budget
        self.last_cost: CallCost | None = None

    # ---------- public API ----------
    async def chat(self, messages: list[dict]) -> str:
        chain = self._fallback_chain()
        primary = chain[0]
        # Token-based routing: switch primary to long-context if input is large
        prompt_tokens = count_message_tokens(messages)
        primary_cap = self._input_cap(primary)
        threshold = int(primary_cap * self.cfg.long_context_threshold_ratio)
        if (
            self.cfg.long_context_model
            and prompt_tokens > threshold
            and self.cfg.long_context_model != primary
        ):
            log.bind(
                tokens=prompt_tokens,
                threshold=threshold,
                primary=primary,
                long_context=self.cfg.long_context_model,
            ).info("llm.routing_to_long_context")
            chain = [self.cfg.long_context_model] + [
                m for m in chain if m != self.cfg.long_context_model
            ]

        current_messages = list(messages)
        last_exc: Exception | None = None

        chain_idx = 0
        while chain_idx < len(chain):
            model_spec = chain[chain_idx]
            cap = self._input_cap(model_spec)
            current_messages = self._fit_to_budget(current_messages, cap)
            jump_to_idx: int | None = None

            attempt = 0
            overflow_trims = 0
            max_overflow_trims = max(1, self.cfg.same_model_retries)
            while attempt <= self.cfg.same_model_retries:
                try:
                    res = await raw_chat(
                        self.pool,
                        model_spec,
                        current_messages,
                        timeout=self.cfg.timeout,
                        budget=self.budget,
                    )
                    self.last_cost = res.cost
                    return res.text
                except Exception as exc:
                    last_exc = exc
                    if not isinstance(exc, _EXPECTED_CALL_ERRORS):
                        raise

                    # context overflow → switch to long-context first; else trim
                    if _is_context_overflow(exc):
                        long_ctx = self.cfg.long_context_model
                        if (
                            long_ctx
                            and model_spec != long_ctx
                            and long_ctx in chain[chain_idx + 1 :]
                        ):
                            jump_to_idx = chain.index(long_ctx, chain_idx + 1)
                            log.bind(model=model_spec, next=long_ctx).warning(
                                "llm.overflow_switching_to_long_context"
                            )
                            break
                        # already on long-context (or none configured): trim and retry
                        if overflow_trims >= max_overflow_trims:
                            break
                        current_tokens = count_message_tokens(current_messages)
                        trimmed_budget = max(
                            current_tokens - self.cfg.overflow_trim_step_tokens,
                            max(1, current_tokens // 2),
                        )
                        if trimmed_budget >= current_tokens:
                            trimmed_budget = max(1, current_tokens // 2)
                        current_messages = truncate_messages_to_tokens(
                            current_messages, trimmed_budget
                        )
                        overflow_trims += 1
                        log.bind(
                            model=model_spec,
                            new_budget=trimmed_budget,
                            attempt=attempt + 1,
                        ).warning("llm.overflow_trimming")
                        continue

                    # auth / quota → drop this model, try next in chain
                    if _is_auth_error(exc) or isinstance(exc, QuotaExhausted):
                        log.bind(
                            model=model_spec,
                            error_class=type(exc).__name__,
                        ).warning("llm.advance_chain")
                        break

                    # transient → retry same model with backoff
                    if _is_retryable_same_model(exc) and attempt < self.cfg.same_model_retries:
                        delay = self.cfg.same_model_retry_backoff * (2**attempt)
                        log.bind(
                            model=model_spec,
                            error_class=type(exc).__name__,
                            attempt=attempt + 1,
                            delay=delay,
                        ).info("llm.same_model_retry")
                        attempt += 1
                        await asyncio.sleep(delay)
                        continue

                    # other 4xx (bad request, etc.) — non-retryable, fail fast
                    if (
                        isinstance(exc, httpx.HTTPStatusError)
                        and 400 <= exc.response.status_code < 500
                    ):
                        raise

                    # exhausted same-model retries on a retryable error → next model
                    log.bind(
                        model=model_spec,
                        error_class=type(exc).__name__,
                    ).info("llm.same_model_exhausted_advancing")
                    break

            if jump_to_idx is not None:
                chain_idx = jump_to_idx
            else:
                chain_idx += 1

        log.bind(model_chain=chain).error("llm.providers_exhausted")
        if last_exc is not None:
            raise AllProvidersExhausted(chain) from last_exc
        raise AllProvidersExhausted(chain)

    # ---------- internals ----------
    def _fallback_chain(self) -> list[str]:
        seen: set[str] = set()
        chain: list[str] = []
        for m in (
            self.cfg.model,
            self.cfg.retry_model,
            self.cfg.fallback_model,
            self.cfg.fallback_model_2,
            self.cfg.long_context_model,
        ):
            if m and m not in seen:
                chain.append(m)
                seen.add(m)
        return chain

    def _input_cap(self, model_spec: str) -> int:
        try:
            entry = self.pool.model_entry(model_spec)
            if entry is None:
                return 128_000
            return entry.max_input_tokens
        except Exception:
            return 128_000

    def _fit_to_budget(self, messages: list[dict], cap_tokens: int) -> list[dict]:
        # leave headroom for completion
        headroom = max(1, cap_tokens // 16)
        budget = max(1, cap_tokens - headroom)
        if count_message_tokens(messages) <= budget:
            return messages
        log.bind(cap=cap_tokens, budget=budget).info("llm.preemptive_trim")
        return truncate_messages_to_tokens(messages, budget)
