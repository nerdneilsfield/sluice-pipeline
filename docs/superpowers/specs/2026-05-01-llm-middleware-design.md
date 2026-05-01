# Sluice V1.3 — LLM Middleware Layer

**Status:** Design draft
**Date:** 2026-05-01
**Owner:** dengqi

## Summary

Introduce a thin **LLM middleware** between stage processors and the raw HTTP layer. All LLM calls in sluice (current and future) flow through it. The middleware owns:

1. **Token-based routing** — when input tokens exceed a configurable share of the primary model's input cap, the call is routed directly to a long-context model.
2. **Same-model retry with backoff** — transient errors (network, 5xx, plain rate limits) are retried in place a small number of times before advancing the chain.
3. **Model fallback chain** — auth errors (401/403) and provider quota exhaustion (configured `quota_error_tokens` 429) advance to the next model in the chain. Chain order: `model → retry_model → fallback_model → fallback_model_2 → long_context_model` (deduplicated, order preserved).
4. **Context overflow recovery** — 4xx responses whose body matches known overflow keywords trigger one of two recovery actions: switch to the long-context model if not already there, otherwise trim the prompt by `overflow_trim_step_tokens` and retry.

The current `LLMClient` (one class doing both raw HTTP and chain fallback) is split into two layers:

- `sluice/llm/client.py` — raw single-shot HTTP call (`raw_chat`). One model spec, one provider key, one HTTP request. No retry, no fallback. Stages must not call this directly.
- `sluice/llm/middleware.py` — `LLMMiddleware` class with the same `chat(messages) -> str` interface processors already use. Drop-in replacement.

## Why

Current `LLMClient.chat()` does fallback through the model chain (`model → retry_model → fallback_model → fallback_model_2`) but is missing:

- **No long-context routing or switching.** Big inputs send to the small-context primary and just fail.
- **No overflow detection or prompt trimming.** A `400 context_length_exceeded` raises straight up to the stage.
- **No same-model retry with backoff.** A single 5xx blip burns one position in the fallback chain.
- **Auth (401/403) fail-fast.** Today this raises immediately; the natural behaviour for a misconfigured provider is to advance to the next model.

CodeWiki has a battle-tested middleware layer (`codewiki/src/be/llm_middleware.py`) that handles all four. We port the design — async, no pydantic-ai (sluice uses raw OpenAI-compatible HTTP), no `MiddlewareModel` agent adapter.

## Scope

In:

- New `sluice/llm/middleware.py` (`LLMMiddleware` class).
- New `sluice/llm/tokens.py` (tiktoken-based counting + truncation).
- Refactor `sluice/llm/client.py` to a raw `raw_chat()` function plus the existing `StageLLMConfig` dataclass.
- Add `max_input_tokens` / `max_output_tokens` to `ModelEntry` (providers config).
- Add `long_context_model: str | None` field to `LLMStageConfig`, `ScoreTagConfig`, `SummarizeScoreTagConfig`.
- Wire all three stages in `builders.py` to construct `LLMMiddleware` instead of the old `LLMClient`.
- New `tiktoken` dependency in `pyproject.toml`.

Out:

- No pydantic-ai integration. Sluice does not use pydantic-ai.
- No streaming. The current pipeline does not consume token-by-token output.
- No global LLM config block. Settings stay per-stage to preserve flexibility.
- No metrics or observability changes (existing `loguru` log lines suffice).
- No change to budget accounting or provider pool / cooldown semantics.

## Non-goals

- We do not replace the provider pool's key rotation. Same-model retry inside the middleware does **not** rotate keys; key rotation already happens inside `pool.acquire()` via weighted random + cooldown.
- We do not do exact billing-grade token counting. `tiktoken cl100k_base` is approximate but stable across calls.
- We do not auto-tune the long-context threshold. The default `0.8 * primary_model.max_input_tokens` is a sane starting point and can be overridden per stage later if needed.

## Behaviour Changes (User-Visible)

1. **401/403 no longer fail fast.** They now advance to the next model in the chain. This is the requested change.
2. **`ModelEntry` gains two fields** (`max_input_tokens=128_000`, `max_output_tokens=4_096`). Existing `providers.toml` files keep working with these defaults; users wanting accurate routing should set them per-model.
3. **Three stages gain a `long_context_model` field.** When unset, behaviour is unchanged (no routing, no overflow switch). When set, the model is appended to the fallback chain and used for token-overflow recovery.
4. **`tiktoken` becomes a hard dependency.** Adds ~15 MB on install; no runtime cost when not calling LLMs.

## Architecture

```
┌──────────────────────────┐
│  Stage processor         │  llm_factory() -> LLMMiddleware
│  (llm_stage / score_tag  │
│   / summarize_score_tag) │
└──────────┬───────────────┘
           │ chat(messages) -> str
           ▼
┌────────────────────────────────────────────────┐
│  LLMMiddleware                                 │
│  - build fallback chain                        │
│  - token-route to long-context if input > 80%  │
│  - per-model: same-model retries with backoff  │
│  - on auth/quota → advance chain               │
│  - on overflow → switch to long-ctx then trim  │
└──────────┬─────────────────────────────────────┘
           │ raw_chat(pool, spec, messages, ...)
           ▼
┌────────────────────────────────────────────────┐
│  raw_chat()  (sluice/llm/client.py)            │
│  - pool.acquire(spec) → Endpoint               │
│  - one POST /chat/completions                  │
│  - raises QuotaExhausted / RateLimitError /    │
│           HTTPStatusError / NetworkError       │
└──────────┬─────────────────────────────────────┘
           │
           ▼
┌────────────────────────────────────────────────┐
│  ProviderPool                                  │
│  - weighted base + key selection               │
│  - cooldowns on quota exhaustion               │
└────────────────────────────────────────────────┘
```

## Decisions

| # | Decision | Rationale |
|---|----------|-----------|
| 1 | tiktoken hard dep | Approximate-token routing is a load-bearing feature; falling back to `len(s)//4` would mis-route on CJK input where ratio is closer to 1:1. |
| 2 | `max_input_tokens` on `ModelEntry` | Per-model — accurate and lives next to other model facts (price). Per-stage override would duplicate model knowledge across pipelines. |
| 3 | 401/403 → advance chain | Matches CodeWiki. A misconfigured key on one provider should not kill the run when a working fallback exists. |
| 4 | No pydantic-ai adapter | Sluice does not use pydantic-ai; nothing to adapt. |
| 5 | Long-context routing threshold = `0.8 * cap` | Leaves headroom for output. Configurable via `long_context_threshold_ratio` on `StageLLMConfig`. |
| 6 | Auto pre-trim before each call | If we already know a message exceeds a model's cap, we trim before sending rather than waiting for the 400. Saves one round-trip and one log-line. |
| 7 | Same-model retries default = 2 | Two retries with exponential backoff (1s, 2s) absorbs typical provider blips without paying multi-second penalties on hard failures. |
| 8 | Overflow **jumps** to `long_context_model`, not just "advances chain" | A naive `break` would advance to the next chain entry, which may be a small-context `fallback_model`. Calling that on an overflowing prompt is wasteful and can mis-attribute the failure. The middleware indexes into the chain so it can skip intermediates. |
| 9 | Processor `max_input_chars` is left untouched | Each LLM-using processor already truncates input by character count before constructing messages. We deliberately do **not** remove that — it serves as a coarse pre-filter that bounds the worst case. **However**, when a stage sets `long_context_model`, the operator must also raise `max_input_chars` so the prompt the middleware actually sees is large enough to trigger token-based routing. Documented as a config caveat. |
| 10 | Overflow trim has no fixed floor | An earlier draft clamped the trim budget at `max(current - step, 10_000)`. That keeps a small (<10k) prompt frozen at its current size when the server flags it as overflow — same body resent, infinite-loop risk. The middleware instead clamps at `max(1, current_tokens // 2)`, guaranteeing strict shrinkage on every retry. |
| 11 | Pipeline ↔ providers cross-validation lives in the example-verification script, not in pydantic | `PipelineConfig.model_validate` cannot see `ProvidersConfig`; cross-file integrity (every `model` / `retry_model` / `fallback_model` / `long_context_model` resolves to a known `(provider, model_name)`) is checked in `Task 8 Step 3`'s verification snippet. Promoting this to runtime validation is out of scope. |

## Error Taxonomy

| Condition | Detection | Action |
|-----------|-----------|--------|
| Network error | `httpx.NetworkError`, `httpx.TimeoutException` | Same-model retry; advance chain after exhaustion |
| Plain rate limit (429) | `RateLimitError` from raw layer | Same-model retry; advance chain after exhaustion |
| Provider quota (429 with quota tokens) | `QuotaExhausted` | Advance chain immediately. Pool cools down the key. |
| Auth (401/403) | `HTTPStatusError` w/ status 401/403 | Advance chain immediately |
| Server error (5xx) | `HTTPStatusError` w/ status >= 500 | Same-model retry; advance chain after exhaustion |
| Other 4xx (e.g. 400 bad request) | `HTTPStatusError` w/ status in 400-499 | Fail fast (raise) |
| Context overflow (400 + overflow keywords) | Status 400, body matches known token-limit substrings | **Jump directly** to `long_context_model` if available and not yet tried (skipping any intermediate `fallback_model` / `fallback_model_2`); otherwise trim by `overflow_trim_step_tokens` and retry on the same model |

Overflow keywords (case-insensitive substring match): `context_length`, `context length`, `too long`, `maximum context`, `input length`, `range of input`, `token limit`, `max_tokens`, `input_tokens_limit`, `request_tokens_limit`.

## Config Surface

`ModelEntry` (providers.toml):

```toml
[[providers.models]]
model_name = "deepseek-chat"
input_price_per_1k = 0.00027
output_price_per_1k = 0.00110
max_input_tokens = 64_000          # NEW
max_output_tokens = 8_192          # NEW
```

Stage config (any of `llm_stage`, `score_tag`, `summarize_score_tag`):

```toml
[[stages]]
type = "llm_stage"
name = "summarize"
model = "deepseek/deepseek-chat"
retry_model = "deepseek/deepseek-chat"
fallback_model = "glm/glm-4-flash"
long_context_model = "glm/glm-4-plus"   # NEW
# … existing fields unchanged
```

`StageLLMConfig` (internal, constructed in `builders.py`) gains:

```
long_context_model: str | None
same_model_retries: int = 2
same_model_retry_backoff: float = 1.0
overflow_trim_step_tokens: int = 100_000
long_context_threshold_ratio: float = 0.8
```

The latter four are not exposed in TOML for now — defaults are good enough; we can promote them to stage config when a real need arises.

## Interaction with Processor-Side `max_input_chars`

`LLMStageProcessor`, `ScoreTagProcessor`, and `SummarizeScoreTagProcessor` each call `_truncate(str(raw), self.max_input_chars, ...)` **before** rendering the prompt template that the middleware ultimately sees (`sluice/processors/llm_stage.py:114`, `sluice/processors/score_tag.py:150`, `sluice/processors/summarize_score_tag.py:123`). The middleware has no way to recover content the processor has already discarded.

Implications:

- **Token-based routing is gated by `max_input_chars`.** If a stage sets `long_context_model = "glm/glm-4-plus"` but leaves `max_input_chars` at e.g. 20k characters (~5k tokens), the prompt may never exceed the primary model's threshold and routing will silently never fire.
- **Recommended config rule.** When a stage sets `long_context_model`, also raise `max_input_chars` to roughly `4 × max_input_tokens` of the long-context model (English averages ~4 chars/token; CJK closer to 1.5–2). The example pipeline TOML demonstrates this.
- **No automatic coupling.** We deliberately do not change processor truncation to defer to the middleware. Doing so would (a) silently bypass long-standing per-stage budgets and (b) entangle two layers that today have a clean boundary. The cost is a documented config rule; the benefit is no implicit behaviour change for stages that don't opt in.

## Failure Modes & Mitigations

- **All models in chain fail.** `AllProvidersExhausted(chain)` is raised, identical to today. Stages already handle this via `failures` store.
- **tiktoken misestimates CJK or code-heavy text.** Acceptable: routing decisions are heuristic. The middleware always re-checks against the actual model's cap before sending.
- **Long-context model itself overflows.** After switch, we fall into the trim path with `overflow_trim_step_tokens` decrements until a request fits or the per-model retry budget is exhausted, at which point we advance the chain. The trim is **strictly** decreasing — no fixed floor (decision #10) — so a small prompt that the server still calls overflow does not get stuck retrying the same body.
- **`tiktoken.get_encoding` network call on first use.** tiktoken caches encoders to `~/.cache/tiktoken`. First-run pipelines may hit a small extra latency; subsequent runs are local.
- **Provider returns 200 with empty content.** Out of scope — same as today.

## Testing Strategy

Unit tests under `tests/llm/`:

- `test_client.py` — focused on `raw_chat`: success, 429 → `RateLimitError`, 429-with-quota-token → `QuotaExhausted`, 4xx → `HTTPStatusError`. (Replaces the old monolithic test file.)
- `test_middleware.py` — covers the middleware layer:
  - Single-call success.
  - Quota exhaustion advances chain.
  - 5xx is retried, then advances chain on second failure.
  - 401 advances chain (behaviour change).
  - All-exhausted raises `AllProvidersExhausted`.
  - 400 with `context_length` body switches to long-context model.
  - **400 with `context_length` body AND a `fallback_model` set** — the middleware must JUMP to `long_context_model`, not call `fallback_model` first. Asserts `fallback_route.call_count == 0`.
  - Token-routing: prompt > 80% of primary cap goes straight to long-context (primary endpoint must NOT be hit).
  - Overflow on long-context model trims and retries successfully — assertion records both request bodies and confirms `len(second.content) < len(first.content)`.
  - **Overflow on a small prompt** (well under any plausible floor) still strictly shrinks the prompt — guards against the `max(count - step, 10_000)` regression.
  - 400 with a non-overflow body fails fast (no chain advance).

Builder-level test (`tests/test_builders_long_context.py`):

- Each of `LLMStageConfig`, `ScoreTagConfig`, `SummarizeScoreTagConfig` builders threads `long_context_model` from stage config into `StageLLMConfig`, and `proc.llm_factory()` returns an `LLMMiddleware` (not the old `LLMClient`).

We do not add integration tests against real providers in this spec; the existing E2E paths through `LLMStageProcessor` continue to exercise the middleware end-to-end.

## Migration

- Existing `providers.toml` and `pipelines/*.toml` continue to work without edits.
- Users adopting `long_context_model` add it per-stage as needed, **and must also raise `max_input_chars` on the same stage** — see "Interaction with Processor-Side `max_input_chars`" above.
- Anyone holding a reference to the old `LLMClient` class breaks at import time. There is one such site (`builders.py`), updated in this change. No external API.
