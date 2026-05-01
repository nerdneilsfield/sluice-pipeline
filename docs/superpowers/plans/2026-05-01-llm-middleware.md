# LLM Middleware Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Insert a thin middleware layer between sluice stage processors and the raw HTTP LLM call, owning routing (token-based long-context switch), retry (same-model + chain), fallback (auth/quota → next model), and overflow recovery (switch + trim).

**Architecture:** Split the existing monolithic `LLMClient` into two layers. The bottom layer (`raw_chat` in `sluice/llm/client.py`) does one HTTP call to one model spec, no retry, no fallback. The new `LLMMiddleware` (in `sluice/llm/middleware.py`) builds a fallback chain, applies same-model retries with exponential backoff, detects context overflow via 4xx body keywords, and uses a tiktoken-based token counter (`sluice/llm/tokens.py`) for routing decisions and prompt trimming. Stages call `LLMMiddleware.chat(messages) -> str`, the same shape they used to call `LLMClient.chat`. `ModelEntry` in `providers.toml` gains `max_input_tokens` / `max_output_tokens` so the middleware knows each model's cap. Three stage configs gain `long_context_model` to opt into routing/recovery.

**Tech Stack:** Python 3.11+, httpx (async), pydantic, tiktoken, pytest + pytest-asyncio + respx.

**Spec:** `docs/superpowers/specs/2026-05-01-llm-middleware-design.md`

---

## File Structure

```
sluice/llm/
  client.py        # CHANGED: now exposes raw_chat() + StageLLMConfig only
  middleware.py    # NEW: LLMMiddleware class
  tokens.py        # NEW: tiktoken count + truncate helpers
  budget.py        # unchanged
  pool.py          # unchanged
  provider.py      # unchanged
sluice/
  config.py        # CHANGED: ModelEntry +2 fields; 3 stage configs +long_context_model
  builders.py      # CHANGED: instantiate LLMMiddleware instead of LLMClient
configs/
  providers.toml.example  # CHANGED: show max_input_tokens/max_output_tokens
  pipelines/ai_news.toml.example  # CHANGED: show long_context_model usage
pyproject.toml     # CHANGED: add tiktoken dep
tests/llm/
  test_tokens.py       # NEW
  test_client.py       # REWRITTEN: only raw_chat tests
  test_middleware.py   # NEW: routing/retry/fallback/overflow
```

**Existing-code note:** Some of the file edits above were already applied in a previous (improperly-sequenced) attempt. The plan still describes them step-by-step so the engineer can verify each one independently — diffing against `git status` is part of Task 0.

---

### Task 0: Verify starting state, install full deps, and align with prior partial edits

**Files:**
- Read: `git status -s`, `git diff --stat`

- [ ] **Step 1: Inspect the working tree**

```bash
cd /home/dengqi/Source/langs/python/sluice
git status -s
git diff --stat
```

Expected (from a prior partial run): modifications to `pyproject.toml`, `sluice/builders.py`, `sluice/config.py`, `sluice/llm/client.py`, `tests/llm/test_client.py`, plus untracked `sluice/llm/middleware.py`, `sluice/llm/tokens.py`, `tests/llm/test_middleware.py`. If the tree is clean, this plan starts from scratch — proceed normally. If the partial edits are present, treat each subsequent task as "verify the existing diff matches the spec, then move on" rather than re-writing from zero.

- [ ] **Step 2: Install ALL extras so the full test suite collects**

The `dev` extra alone is insufficient — `python3 -m pytest -q` collection imports `email`, `metrics`, etc., which require `aiosqlite`, `aiofiles`, `trafilatura`, `prefect`, `aiosmtplib`, `prometheus_client`. Install the full set:

```bash
python3 -m pip install -e '.[dev,all]'
```

If `[all]` is not defined in `pyproject.toml`, install explicitly:

```bash
python3 -m pip install -e '.[dev]' aiosqlite aiofiles trafilatura prefect aiosmtplib prometheus_client
```

- [ ] **Step 3: Run the existing test suite to capture baseline**

```bash
python3 -m pytest -q
```

Expected: collection succeeds; all current tests pass (the partial edits, if present, may have broken `tests/llm/test_client.py` — that's expected and Task 5 fixes it).

- [ ] **Step 4: Per-task commit hygiene given the existing partial diff**

Several tracked files (`sluice/config.py`, `sluice/builders.py`, `sluice/llm/client.py`, `tests/llm/test_client.py`, `pyproject.toml`) already contain edits spanning **multiple** future tasks. To preserve the per-task commit boundary the plan promises, commit steps that touch those pre-dirty shared files use **`git add -p`** and inspect the staged diff with `git diff --cached <file>` before committing. New files, or tracked files first touched by a single later task (for example the example TOML files in Task 8), may be staged normally. If selecting hunks proves too painful for a given task, fall back to a single consolidation commit at the end with message `refactor(llm): consolidate middleware tasks 1-7` — and note this in the PR description. Do **not** silently bundle unrelated tasks under one task's commit message.

---

### Task 1: Add tiktoken dependency

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1: Add `tiktoken>=0.7` to `[project].dependencies`**

In `pyproject.toml`, inside the `dependencies = [...]` list under `[project]`, add the line `"tiktoken>=0.7",` after the existing `"croniter>=2.0",`. Final block:

```toml
dependencies = [
  "pydantic>=2.6",
  "aiosqlite>=0.20",
  "httpx>=0.27",
  "feedparser>=6.0",
  "trafilatura>=1.12",
  "jinja2>=3.1",
  "typer>=0.12",
  "prefect>=3.0",
  "python-dateutil>=2.9",
  "regex>=2024.0",
  "notionify>=0.1.3",
  "fake-useragent>=2.2",
  "rich>=13.0",
  "tqdm>=4.66",
  "loguru>=0.7",
  "aiofiles>=23.0",
  "croniter>=2.0",
  "tiktoken>=0.7",
]
```

- [ ] **Step 2: Install (already done in Task 0; this re-syncs after the dep edit)**

```bash
python3 -m pip install -e '.[dev,all]'
```

Expected: tiktoken downloads and installs.

- [ ] **Step 3: Smoke-test tiktoken import**

```bash
python3 -c "import tiktoken; print(tiktoken.get_encoding('cl100k_base').encode('hello world'))"
```

Expected: a list of 2 integers printed (no exception).

- [ ] **Step 4: Commit (only the tiktoken hunk — `pyproject.toml` may also have other edits)**

```bash
git add -p pyproject.toml      # stage ONLY the +"tiktoken>=0.7", line
git diff --cached pyproject.toml
git commit -m "chore(llm): add tiktoken dep for middleware token counting"
```

---

### Task 2: Token counting helper (`sluice/llm/tokens.py`)

**Files:**
- Create: `sluice/llm/tokens.py`
- Test: `tests/llm/test_tokens.py`

- [ ] **Step 1: Write the failing test**

Create `tests/llm/test_tokens.py`:

```python
from sluice.llm.tokens import (
    count_tokens,
    count_message_tokens,
    truncate_to_tokens,
    truncate_messages_to_tokens,
)


def test_count_tokens_empty_is_zero():
    assert count_tokens("") == 0


def test_count_tokens_short_string_positive():
    assert count_tokens("hello world") > 0


def test_count_tokens_grows_with_length():
    short = count_tokens("hello")
    long = count_tokens("hello " * 100)
    assert long > short * 50


def test_count_message_tokens_includes_overhead():
    """Per-message overhead means N empty messages count > 0."""
    msgs = [{"role": "user", "content": ""} for _ in range(3)]
    assert count_message_tokens(msgs) >= 3 * 4


def test_count_message_tokens_handles_list_content():
    msgs = [
        {"role": "user", "content": [{"type": "text", "text": "hello"}]},
    ]
    assert count_message_tokens(msgs) > 4


def test_truncate_to_tokens_short_unchanged():
    s = "hello"
    assert truncate_to_tokens(s, 1000) == s


def test_truncate_to_tokens_long_shrinks():
    s = "word " * 1000
    out = truncate_to_tokens(s, 50)
    assert count_tokens(out) <= 50
    assert len(out) < len(s)


def test_truncate_to_tokens_zero_returns_empty():
    assert truncate_to_tokens("anything", 0) == ""


def test_truncate_messages_keeps_earlier_intact():
    msgs = [
        {"role": "system", "content": "stay"},
        {"role": "user", "content": "word " * 5000},
    ]
    out = truncate_messages_to_tokens(msgs, 100)
    assert out[0]["content"] == "stay"
    assert count_message_tokens(out) <= 100 + 8  # +overhead slack
```

- [ ] **Step 2: Run test, verify it fails**

```bash
python3 -m pytest tests/llm/test_tokens.py -v
```

Expected: ImportError or ModuleNotFoundError on `sluice.llm.tokens`.

- [ ] **Step 3: Implement `sluice/llm/tokens.py`**

```python
"""Token counting and truncation helpers.

Uses tiktoken's ``cl100k_base`` encoder by default. The encoder choice is
approximate for non-OpenAI models — we only need a stable token estimate for
routing and overflow trimming, not exact billing accuracy.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Iterable

import tiktoken

_DEFAULT_ENCODER = "cl100k_base"


@lru_cache(maxsize=8)
def _get_encoder(name: str = _DEFAULT_ENCODER):
    try:
        return tiktoken.get_encoding(name)
    except Exception:
        return tiktoken.get_encoding(_DEFAULT_ENCODER)


def count_tokens(text: str, encoder: str = _DEFAULT_ENCODER) -> int:
    if not text:
        return 0
    return len(_get_encoder(encoder).encode(text))


def count_message_tokens(messages: Iterable[dict], encoder: str = _DEFAULT_ENCODER) -> int:
    enc = _get_encoder(encoder)
    total = 0
    for m in messages:
        content = m.get("content")
        if isinstance(content, str):
            total += len(enc.encode(content))
        elif isinstance(content, list):
            for part in content:
                if isinstance(part, dict):
                    text = part.get("text") or ""
                    if isinstance(text, str):
                        total += len(enc.encode(text))
        total += 4  # role/separator overhead per message (OpenAI-style estimate)
    return total


def truncate_to_tokens(text: str, max_tokens: int, encoder: str = _DEFAULT_ENCODER) -> str:
    if max_tokens <= 0:
        return ""
    enc = _get_encoder(encoder)
    tokens = enc.encode(text)
    if len(tokens) <= max_tokens:
        return text
    return enc.decode(tokens[:max_tokens])


def truncate_messages_to_tokens(
    messages: list[dict],
    max_tokens: int,
    encoder: str = _DEFAULT_ENCODER,
) -> list[dict]:
    """Trim the last user message so the whole list fits in budget.

    Keeps system / earlier turns intact. If even after trimming the last
    message we are still over budget, returns the messages with that last
    message aggressively truncated to whatever budget remains (>=0).
    """
    if not messages:
        return messages
    other_tokens = count_message_tokens(messages[:-1], encoder)
    last = dict(messages[-1])
    overhead = 4
    remaining = max(0, max_tokens - other_tokens - overhead)
    content = last.get("content")
    if isinstance(content, str):
        last["content"] = truncate_to_tokens(content, remaining, encoder)
    return [*messages[:-1], last]
```

- [ ] **Step 4: Run test, verify it passes**

```bash
python3 -m pytest tests/llm/test_tokens.py -v
```

Expected: 9 PASS.

- [ ] **Step 5: Commit**

```bash
git add sluice/llm/tokens.py tests/llm/test_tokens.py
git commit -m "feat(llm): add token counting and truncation helpers"
```

---

### Task 3: Extend `ModelEntry` with token caps

**Files:**
- Modify: `sluice/config.py` (lines around `class ModelEntry`)
- Test: `tests/config/test_model_entry.py` (new file — the project already has a `tests/config/` directory with `test_provider_config.py` and `test_pipeline_config.py`; we follow that convention)

- [ ] **Step 1: Write the failing test**

Create `tests/config/test_model_entry.py`:

```python
from sluice.config import ModelEntry


def test_model_entry_default_token_caps():
    m = ModelEntry(model_name="x")
    assert m.max_input_tokens == 128_000
    assert m.max_output_tokens == 4_096


def test_model_entry_overridden_token_caps():
    m = ModelEntry(model_name="big", max_input_tokens=1_000_000, max_output_tokens=8_192)
    assert m.max_input_tokens == 1_000_000
    assert m.max_output_tokens == 8_192
```

- [ ] **Step 2: Run test, verify it fails**

```bash
python3 -m pytest tests/config/test_model_entry.py -v
```

Expected: AttributeError or pydantic ValidationError on the new fields.

- [ ] **Step 3: Add the fields**

In `sluice/config.py`, change `class ModelEntry` to:

```python
class ModelEntry(BaseModel):
    model_name: str
    is_stream: bool = True
    is_support_json_schema: bool = False
    is_support_json_object: bool = False
    input_price_per_1k: float = 0.0
    output_price_per_1k: float = 0.0
    max_input_tokens: int = 128_000
    max_output_tokens: int = 4_096
```

- [ ] **Step 4: Run test, verify it passes**

```bash
python3 -m pytest tests/config/test_model_entry.py -v
```

Expected: 2 PASS.

- [ ] **Step 5: Commit**

```bash
git add -p sluice/config.py    # stage ONLY the two new ModelEntry fields
git add tests/config/test_model_entry.py
git diff --cached sluice/config.py
git commit -m "feat(config): add max_input_tokens/max_output_tokens to ModelEntry"
```

---

### Task 4: Add `long_context_model` to three stage configs

**Files:**
- Modify: `sluice/config.py` — `LLMStageConfig`, `ScoreTagConfig`, `SummarizeScoreTagConfig`
- Test: `tests/test_config_long_context.py` (new)

- [ ] **Step 1: Write the failing test**

Create `tests/test_config_long_context.py`:

```python
from sluice.config import (
    LLMStageConfig,
    ScoreTagConfig,
    SummarizeScoreTagConfig,
)


def _llm_stage(**extra):
    return LLMStageConfig(
        type="llm_stage",
        name="t",
        mode="aggregate",
        input_field="x",
        output_target="y",
        prompt_file="p.md",
        model="p/m",
        **extra,
    )


def test_llm_stage_long_context_default_none():
    assert _llm_stage().long_context_model is None


def test_llm_stage_long_context_set():
    assert _llm_stage(long_context_model="prov/lc").long_context_model == "prov/lc"


def test_score_tag_long_context_field():
    cfg = ScoreTagConfig(
        type="score_tag",
        name="s",
        input_field="x",
        prompt_file="p.md",
        model="p/m",
        long_context_model="prov/lc",
    )
    assert cfg.long_context_model == "prov/lc"


def test_summarize_score_tag_long_context_field():
    cfg = SummarizeScoreTagConfig(
        type="summarize_score_tag",
        name="s",
        input_field="x",
        prompt_file="p.md",
        model="p/m",
        long_context_model="prov/lc",
    )
    assert cfg.long_context_model == "prov/lc"
```

- [ ] **Step 2: Run test, verify it fails**

```bash
python3 -m pytest tests/test_config_long_context.py -v
```

Expected: AttributeError when the test accesses `.long_context_model` (or an assertion failure caused by the missing field). Pydantic's default extra-field policy may ignore unknown constructor kwargs rather than raising `ValidationError`.

- [ ] **Step 3: Add the field to each stage config**

In `sluice/config.py`, add `long_context_model: str | None = None` immediately after `fallback_model_2: str | None = None` in three places:

1. `class LLMStageConfig` (already has retry/fallback chain).
2. `class ScoreTagConfig`.
3. `class SummarizeScoreTagConfig`.

Each new line goes between the existing `fallback_model_2` and the `workers` field.

- [ ] **Step 4: Run test, verify it passes**

```bash
python3 -m pytest tests/test_config_long_context.py -v
```

Expected: 4 PASS.

- [ ] **Step 5: Commit**

```bash
git add -p sluice/config.py    # stage ONLY the three long_context_model lines
git add tests/test_config_long_context.py
git diff --cached sluice/config.py
git commit -m "feat(config): add long_context_model to LLM stage configs"
```

---

### Task 5: Refactor `client.py` into a raw single-shot call

**Files:**
- Rewrite: `sluice/llm/client.py`
- Rewrite: `tests/llm/test_client.py`

- [ ] **Step 1: Write the failing test for the new API**

Replace `tests/llm/test_client.py` entirely:

```python
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
```

- [ ] **Step 2: Run test, verify it fails**

```bash
python3 -m pytest tests/llm/test_client.py -v
```

Expected: ImportError on `raw_chat` (or, if Task 0 found prior partial edits, the existing rewritten file may already pass — verify and move on).

- [ ] **Step 3: Rewrite `sluice/llm/client.py`**

```python
"""Raw single-shot OpenAI-compatible chat call.

This module is the lowest layer: one model spec, one HTTP call, one provider key
chosen by ProviderPool. It owns NO retry, NO fallback, NO routing — those live
in :mod:`sluice.llm.middleware`. Stages must not use this directly.
"""

from dataclasses import dataclass

import httpx

from sluice.core.errors import QuotaExhausted, RateLimitError
from sluice.llm.budget import CallCost, RunBudget, compute_cost
from sluice.llm.pool import ProviderPool
from sluice.llm.provider import Endpoint
from sluice.logging_setup import get_logger

log = get_logger(__name__)


@dataclass
class StageLLMConfig:
    """Per-stage LLM configuration consumed by the middleware.

    The retry/fallback chain lives here. The middleware also synthesises
    ``long_context_model`` into the chain when set.
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
```

- [ ] **Step 4: Run test, verify it passes**

```bash
python3 -m pytest tests/llm/test_client.py -v
```

Expected: 4 PASS.

- [ ] **Step 5: Run the full test suite — `builders.py` and processors still import the old `LLMClient`; that import will now fail. Confirm.**

```bash
python3 -m pytest -q
```

Expected: failures with `ImportError: cannot import name 'LLMClient'` from `sluice/builders.py`. This is intentional — Task 7 fixes builders.

- [ ] **Step 6: Commit (plan continues; broken state is local to this commit)**

```bash
git add -p sluice/llm/client.py tests/llm/test_client.py
git diff --cached sluice/llm/client.py tests/llm/test_client.py
git commit -m "refactor(llm): split LLMClient into raw_chat single-shot helper"
```

---

### Task 6: Implement `LLMMiddleware`

**Files:**
- Create: `sluice/llm/middleware.py`
- Test: `tests/llm/test_middleware.py`

- [ ] **Step 1: Write the failing test**

Create `tests/llm/test_middleware.py`:

```python
import httpx
import pytest
import respx

from sluice.config import BaseEndpoint, KeyConfig, ModelEntry, Provider, ProvidersConfig
from sluice.core.errors import AllProvidersExhausted
from sluice.llm.client import StageLLMConfig
from sluice.llm.middleware import LLMMiddleware
from sluice.llm.pool import ProviderPool


def _ok(text: str = "ok") -> httpx.Response:
    return httpx.Response(
        200,
        json={
            "choices": [{"message": {"content": text}}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1},
        },
    )


def make_pool(monkeypatch, *, long_ctx_input: int = 1_000_000) -> ProviderPool:
    monkeypatch.setenv("K", "v")
    return ProviderPool(
        ProvidersConfig(
            providers=[
                Provider(
                    name="p1",
                    type="openai_compatible",
                    base=[BaseEndpoint(url="https://main", weight=1,
                                       key=[KeyConfig(value="env:K", weight=1)])],
                    models=[ModelEntry(model_name="m1", max_input_tokens=10_000)],
                ),
                Provider(
                    name="p2",
                    type="openai_compatible",
                    base=[BaseEndpoint(url="https://fb", weight=1,
                                       key=[KeyConfig(value="env:K", weight=1)])],
                    models=[ModelEntry(model_name="m2", max_input_tokens=10_000)],
                ),
                Provider(
                    name="p3",
                    type="openai_compatible",
                    base=[BaseEndpoint(url="https://lc", weight=1,
                                       key=[KeyConfig(value="env:K", weight=1)])],
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
    pool.runtimes["p1"].provider.base[0].key[0].quota_error_tokens = ["quota"]
    cfg = StageLLMConfig(model="p1/m1", fallback_model="p2/m2", same_model_retries=0)
    with respx.mock() as r:
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
        model="p1/m1", fallback_model="p2/m2",
        same_model_retries=1, same_model_retry_backoff=0.0,
    )
    with respx.mock() as r:
        r.post("https://main/chat/completions").mock(
            return_value=httpx.Response(500, text="boom")
        )
        r.post("https://fb/chat/completions").mock(return_value=_ok("fb-ok"))
        out = await LLMMiddleware(pool, cfg).chat([{"role": "user", "content": "x"}])
        assert out == "fb-ok"


@pytest.mark.asyncio
async def test_401_advances_chain(monkeypatch):
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
        r.post("https://main/chat/completions").mock(
            return_value=httpx.Response(429, text="quota")
        )
        r.post("https://fb/chat/completions").mock(
            return_value=httpx.Response(429, text="quota")
        )
        with pytest.raises(AllProvidersExhausted):
            await LLMMiddleware(pool, cfg).chat([{"role": "user", "content": "x"}])


@pytest.mark.asyncio
async def test_overflow_400_switches_to_long_context(monkeypatch):
    pool = make_pool(monkeypatch)
    cfg = StageLLMConfig(
        model="p1/m1", long_context_model="p3/lc", same_model_retries=0,
    )
    with respx.mock() as r:
        r.post("https://main/chat/completions").mock(
            return_value=httpx.Response(400, text="context_length exceeded")
        )
        r.post("https://lc/chat/completions").mock(return_value=_ok("from-lc"))
        out = await LLMMiddleware(pool, cfg).chat([{"role": "user", "content": "tiny"}])
        assert out == "from-lc"


@pytest.mark.asyncio
async def test_overflow_skips_intermediate_fallback(monkeypatch):
    """Critical: when overflow is detected on primary AND a fallback_model exists,
    the middleware must jump DIRECTLY to long_context_model, NOT call fallback first.

    Previously the inner-loop `break` would advance to the next chain entry, which
    is `fallback_model` (also small-context) — defeating the overflow recovery.
    """
    pool = make_pool(monkeypatch)
    cfg = StageLLMConfig(
        model="p1/m1",
        fallback_model="p2/m2",       # small-context fallback — must be skipped
        long_context_model="p3/lc",
        same_model_retries=0,
    )
    with respx.mock(assert_all_mocked=False) as r:
        r.post("https://main/chat/completions").mock(
            return_value=httpx.Response(400, text="context_length exceeded")
        )
        # fallback (p2) MUST NOT be called — assert_all_mocked=False would still
        # let an unmocked call through, so we explicitly mock it to fail loudly:
        fb_route = r.post("https://fb/chat/completions").mock(
            return_value=httpx.Response(500, text="should not be called")
        )
        r.post("https://lc/chat/completions").mock(return_value=_ok("from-lc"))
        out = await LLMMiddleware(pool, cfg).chat([{"role": "user", "content": "x"}])
        assert out == "from-lc"
        assert fb_route.call_count == 0, "fallback was called on overflow — chain skip is broken"


@pytest.mark.asyncio
async def test_token_routing_uses_long_context_directly(monkeypatch):
    pool = make_pool(monkeypatch)
    cfg = StageLLMConfig(
        model="p1/m1",  # cap 10k → threshold 8k
        long_context_model="p3/lc",
        same_model_retries=0,
    )
    big = "word " * 20_000
    with respx.mock(assert_all_mocked=False) as r:
        # main must NOT be called
        r.post("https://lc/chat/completions").mock(return_value=_ok("routed"))
        out = await LLMMiddleware(pool, cfg).chat([{"role": "user", "content": big}])
        assert out == "routed"


@pytest.mark.asyncio
async def test_overflow_on_long_context_trims_and_retries(monkeypatch):
    """Trim path must ACTUALLY shrink the request body on retry, not just
    record an attempt. Asserts the second request's prompt is strictly shorter
    than the first."""
    import json as _json

    pool = make_pool(monkeypatch, long_ctx_input=1_000_000)
    cfg = StageLLMConfig(
        model="p3/lc",
        same_model_retries=2, same_model_retry_backoff=0.0,
        overflow_trim_step_tokens=50,
    )
    bodies: list[dict] = []

    def handler(request):
        bodies.append(_json.loads(request.content))
        if len(bodies) == 1:
            return httpx.Response(400, text="input_tokens_limit reached")
        return _ok("trimmed-ok")

    # Use a non-tiny prompt so trimming has somewhere to go even with small
    # inputs; the implementation must NOT clamp the trim budget at a floor
    # (e.g. 10_000) that prevents shrinking when the input is below that floor.
    long_prompt = "alpha beta gamma " * 200
    with respx.mock() as r:
        r.post("https://lc/chat/completions").mock(side_effect=handler)
        out = await LLMMiddleware(pool, cfg).chat(
            [{"role": "user", "content": long_prompt}]
        )
    assert out == "trimmed-ok"
    assert len(bodies) == 2
    first_content = bodies[0]["messages"][-1]["content"]
    second_content = bodies[1]["messages"][-1]["content"]
    assert len(second_content) < len(first_content), (
        f"trim retry did not shrink prompt: "
        f"first={len(first_content)} second={len(second_content)}"
    )


@pytest.mark.asyncio
async def test_overflow_trims_when_prompt_below_naive_floor(monkeypatch):
    """Regression: if the prompt is already small (< any hardcoded floor), an
    overflow signal must STILL trim further, not stick at the floor and retry
    the same body. This catches the `max(count - step, 10_000)` bug where a
    small prompt would never shrink."""
    import json as _json

    pool = make_pool(monkeypatch, long_ctx_input=1_000_000)
    cfg = StageLLMConfig(
        model="p3/lc",
        same_model_retries=2, same_model_retry_backoff=0.0,
        overflow_trim_step_tokens=20,
    )
    bodies: list[dict] = []

    def handler(request):
        bodies.append(_json.loads(request.content))
        if len(bodies) == 1:
            return httpx.Response(400, text="input_tokens_limit reached")
        return _ok("ok")

    short_prompt = "hello world " * 30  # well under any sane floor
    with respx.mock() as r:
        r.post("https://lc/chat/completions").mock(side_effect=handler)
        await LLMMiddleware(pool, cfg).chat(
            [{"role": "user", "content": short_prompt}]
        )

    first = bodies[0]["messages"][-1]["content"]
    second = bodies[1]["messages"][-1]["content"]
    assert len(second) < len(first), (
        "trim retry on a small prompt did not shrink — likely a hardcoded floor"
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
```

- [ ] **Step 2: Run test, verify it fails**

```bash
python3 -m pytest tests/llm/test_middleware.py -v
```

Expected: ImportError on `sluice.llm.middleware`.

- [ ] **Step 3: Implement `sluice/llm/middleware.py`**

```python
"""LLM middleware: routing, retry, fallback, and overflow handling.

Drop-in replacement for the old ``LLMClient`` — exposes ``chat(messages) -> str``.

Responsibilities:

* **Routing** — if the input exceeds ``long_context_threshold_ratio`` of the
  primary model's input cap and ``long_context_model`` is set, route there
  directly.
* **Same-model retry** — transient errors (network, plain rate limit, 5xx)
  retry in place ``same_model_retries`` times with exponential backoff.
* **Model fallback** — auth (401/403) and provider-quota exhaustion advance
  the chain immediately. Same-model retry exhaustion advances after retries.
* **Overflow recovery** — 4xx with overflow keywords switches to long-context
  if available; otherwise trims by ``overflow_trim_step_tokens`` and retries.
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
from sluice.llm.provider import parse_model_spec
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

    async def chat(self, messages: list[dict]) -> str:
        chain = self._fallback_chain()
        primary = chain[0]

        # Token-based routing: switch primary to long-context if input is large.
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

        # Index-based iteration so overflow can JUMP to long-context, not just
        # fall through to the next chain entry (which may be a small-context
        # fallback model — calling it on an overflowing prompt is wasteful).
        chain_idx = 0
        while chain_idx < len(chain):
            model_spec = chain[chain_idx]
            cap = self._input_cap(model_spec)
            current_messages = self._fit_to_budget(current_messages, cap)
            jump_to_idx: int | None = None

            for attempt in range(self.cfg.same_model_retries + 1):
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

                    # Context overflow: switch DIRECTLY to long-context if it
                    # exists later in the chain; otherwise trim and retry.
                    if _is_context_overflow(exc):
                        long_ctx = self.cfg.long_context_model
                        if long_ctx and model_spec != long_ctx and long_ctx in chain:
                            lc_idx = chain.index(long_ctx)
                            if lc_idx > chain_idx:
                                log.bind(
                                    model=model_spec,
                                    next=long_ctx,
                                    from_idx=chain_idx,
                                    to_idx=lc_idx,
                                ).warning("llm.overflow_jumping_to_long_context")
                                jump_to_idx = lc_idx
                                break
                        # Trim must STRICTLY shrink the prompt on each retry —
                        # no fixed floor (e.g. 10_000) that would freeze a
                        # small prompt at its current size and re-send the
                        # same body. We compute a budget below the current
                        # token count and clamp only at >= 1 token.
                        current_tokens = count_message_tokens(current_messages)
                        trimmed_budget = max(
                            current_tokens - self.cfg.overflow_trim_step_tokens,
                            max(1, current_tokens // 2),
                        )
                        if trimmed_budget >= current_tokens:
                            # Final guard: if step is huge or count is tiny,
                            # at least halve.
                            trimmed_budget = max(1, current_tokens // 2)
                        current_messages = truncate_messages_to_tokens(
                            current_messages, trimmed_budget
                        )
                        log.bind(
                            model=model_spec,
                            old_tokens=current_tokens,
                            new_budget=trimmed_budget,
                            attempt=attempt + 1,
                        ).warning("llm.overflow_trimming")
                        continue

                    # Auth / provider-quota → next model immediately.
                    if _is_auth_error(exc) or isinstance(exc, QuotaExhausted):
                        log.bind(
                            model=model_spec,
                            error_class=type(exc).__name__,
                        ).warning("llm.advance_chain")
                        break

                    # Transient → retry same model with exponential backoff.
                    if (
                        _is_retryable_same_model(exc)
                        and attempt < self.cfg.same_model_retries
                    ):
                        delay = self.cfg.same_model_retry_backoff * (2**attempt)
                        log.bind(
                            model=model_spec,
                            error_class=type(exc).__name__,
                            attempt=attempt + 1,
                            delay=delay,
                        ).info("llm.same_model_retry")
                        await asyncio.sleep(delay)
                        continue

                    # Other 4xx → non-retryable, fail fast.
                    if (
                        isinstance(exc, httpx.HTTPStatusError)
                        and 400 <= exc.response.status_code < 500
                    ):
                        raise

                    # Same-model retries exhausted on a retryable error → advance.
                    log.bind(
                        model=model_spec,
                        error_class=type(exc).__name__,
                    ).info("llm.same_model_exhausted_advancing")
                    break

            # Either jump to long-context, or advance one step in the chain.
            chain_idx = jump_to_idx if jump_to_idx is not None else chain_idx + 1

        log.bind(model_chain=chain).error("llm.providers_exhausted")
        if last_exc is not None:
            raise AllProvidersExhausted(chain) from last_exc
        raise AllProvidersExhausted(chain)

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
            prov, name = parse_model_spec(model_spec)
            runtime = self.pool.runtimes.get(prov)
            if runtime is None:
                return 128_000
            entry = runtime._models.get(name)
            if entry is None:
                return 128_000
            return entry.max_input_tokens
        except Exception:
            return 128_000

    def _fit_to_budget(self, messages: list[dict], cap_tokens: int) -> list[dict]:
        headroom = max(1024, cap_tokens // 16)
        budget = max(1024, cap_tokens - headroom)
        if count_message_tokens(messages) <= budget:
            return messages
        log.bind(cap=cap_tokens, budget=budget).info("llm.preemptive_trim")
        return truncate_messages_to_tokens(messages, budget)
```

- [ ] **Step 4: Run test, verify it passes**

```bash
python3 -m pytest tests/llm/test_middleware.py -v
```

Expected: 11 PASS.

- [ ] **Step 5: Commit**

```bash
git add sluice/llm/middleware.py tests/llm/test_middleware.py
git diff --cached sluice/llm/middleware.py tests/llm/test_middleware.py
git commit -m "feat(llm): add LLMMiddleware (routing, retry, fallback, overflow)"
```

---

### Task 7: Wire `builders.py` to use the middleware

**Files:**
- Modify: `sluice/builders.py` (3 stage-construction blocks: LLMStageConfig, ScoreTagConfig, SummarizeScoreTagConfig)
- Test: `tests/test_builders_long_context.py` (new — TDD assertion that the builder threads `long_context_model` into `StageLLMConfig` and constructs `LLMMiddleware`)

- [ ] **Step 1: Write the failing builder test**

The existing `tests/test_builders.py` only inspects processor names, not the LLM factory. Add a new file `tests/test_builders_long_context.py` that asserts each of the three stage builders constructs an `LLMMiddleware` whose `cfg.long_context_model` matches the stage config.

The real builder API is `build_processors(*, pipe, global_cfg, seen, failures, fetcher_chain, llm_pool, budget, ...)` (see `sluice/builders.py:158`), and `PipelineConfig` requires `id`, `sources`, `stages`, `sinks`. The processor stores the lambda as `self.llm_factory` (see `sluice/processors/llm_stage.py:74`). Test:

```python
"""Builder must thread long_context_model from stage config into StageLLMConfig
and use LLMMiddleware (not the old LLMClient) as the llm_factory."""

import pytest

from sluice.builders import build_processors
from sluice.config import (
    BaseEndpoint,
    GlobalConfig,
    KeyConfig,
    LLMStageConfig,
    ModelEntry,
    PipelineConfig,
    Provider,
    ProvidersConfig,
    ScoreTagConfig,
    SummarizeScoreTagConfig,
)
from sluice.llm.middleware import LLMMiddleware
from sluice.llm.pool import ProviderPool


def _providers_cfg(monkeypatch):
    monkeypatch.setenv("K", "v")
    return ProvidersConfig(
        providers=[
            Provider(
                name="p",
                type="openai_compatible",
                base=[BaseEndpoint(url="https://x", weight=1,
                                   key=[KeyConfig(value="env:K", weight=1)])],
                models=[ModelEntry(model_name="m"), ModelEntry(model_name="lc")],
            )
        ]
    )


def _pipeline_with(stage):
    """Minimal valid PipelineConfig — id/sources/stages/sinks all required."""
    return PipelineConfig(
        id="t",
        sources=[],
        stages=[stage],
        sinks=[],
    )


@pytest.mark.parametrize("stage_factory", [
    lambda prompt_path: LLMStageConfig(
        type="llm_stage", name="s", mode="aggregate",
        input_field="x", output_target="y", prompt_file=prompt_path,
        model="p/m", long_context_model="p/lc",
    ),
    lambda prompt_path: ScoreTagConfig(
        type="score_tag", name="s",
        input_field="x", prompt_file=prompt_path,
        model="p/m", long_context_model="p/lc",
    ),
    lambda prompt_path: SummarizeScoreTagConfig(
        type="summarize_score_tag", name="s",
        input_field="x", prompt_file=prompt_path,
        model="p/m", long_context_model="p/lc",
    ),
])
def test_builder_threads_long_context_model(
    monkeypatch, tmp_path, stage_factory,
):
    prompt = tmp_path / "p.md"
    prompt.write_text("hello {{ item.x }}")
    stage = stage_factory(str(prompt))

    pool = ProviderPool(_providers_cfg(monkeypatch))
    pipe = _pipeline_with(stage)
    procs = build_processors(
        pipe=pipe,
        global_cfg=GlobalConfig(),
        seen=None,
        failures=None,
        fetcher_chain=None,
        llm_pool=pool,
        budget=None,
        dry_run=True,
    )
    proc = procs[0]
    mw = proc.llm_factory()
    assert isinstance(mw, LLMMiddleware), (
        f"expected LLMMiddleware, got {type(mw).__name__}"
    )
    assert mw.cfg.long_context_model == "p/lc"
    assert mw.cfg.model == "p/m"
```

> **Verification before running:** if any of the kwargs above (`seen=None`, `failures=None`, `dry_run=True`) trip a non-null assertion inside `build_processors`, supply minimal in-memory stubs rather than reshaping the test. The shape of the assertions (factory returns `LLMMiddleware`, `long_context_model` threaded) is fixed — the construction prelude may need small tweaks per current code.

- [ ] **Step 2: Run test, verify it fails**

```bash
python3 -m pytest tests/test_builders_long_context.py -v
```

Expected: ImportError on `LLMClient`, OR an `AssertionError` that the factory returns `LLMClient` instead of `LLMMiddleware`.

- [ ] **Step 3: Verify the existing tests for builders fail with the current import**

```bash
python3 -m pytest -q tests/test_builders.py 2>&1 | tail -20
```

Expected: ImportError on `LLMClient` from `sluice.llm.client`.

- [ ] **Step 4: Update the import line**

In `sluice/builders.py`, replace:

```python
from sluice.llm.client import LLMClient, StageLLMConfig
```

with:

```python
from sluice.llm.client import StageLLMConfig
from sluice.llm.middleware import LLMMiddleware
```

- [ ] **Step 5: Replace the three `llm_factory` constructions and add `long_context_model=`**

In each of the three `isinstance(st, …)` branches (`LLMStageConfig`, `ScoreTagConfig`, `SummarizeScoreTagConfig`), do two edits:

- Add `long_context_model=st.long_context_model,` to the `StageLLMConfig(...)` constructor (right after `fallback_model_2=...`).
- Replace `lambda cfg=stage_llm: LLMClient(llm_pool, cfg, budget)` with `lambda cfg=stage_llm: LLMMiddleware(llm_pool, cfg, budget)`.

There are exactly 3 occurrences of `LLMClient(` and 3 occurrences of `StageLLMConfig(` to update.

- [ ] **Step 6: Run the full suite**

```bash
python3 -m pytest -q
```

Expected: all tests pass (token + config + raw client + middleware + new builder long-context test + existing builder/processor tests).

- [ ] **Step 7: Commit**

```bash
git add -p sluice/builders.py
git add tests/test_builders_long_context.py
git diff --cached sluice/builders.py
git commit -m "feat(llm): route stage processors through LLMMiddleware"
```

---

### Task 8: Update example configs to demonstrate new fields

**Files:**
- Modify: `configs/providers.toml.example`
- Modify: `configs/pipelines/ai_news.toml.example`

- [ ] **Step 1: Add `max_input_tokens` / `max_output_tokens` to a couple of model entries**

In `configs/providers.toml.example`, append two new fields to **one** representative model entry per provider (e.g. `glm-4-plus`, `deepseek-chat`). Example:

```toml
[[providers.models]]
model_name          = "glm-4-plus"
input_price_per_1k  = 0.007
output_price_per_1k = 0.007
max_input_tokens    = 128_000
max_output_tokens   = 4_096
```

- [ ] **Step 2: Add `long_context_model = "..."` and bump `max_input_chars` on the same stage**

**Caveat — processor-side char truncation:** `LLMStageProcessor`, `ScoreTagProcessor`, and `SummarizeScoreTagProcessor` apply a per-item `max_input_chars` cut **before** handing messages to the middleware (see `sluice/processors/llm_stage.py:114`, `sluice/processors/score_tag.py:150`, `sluice/processors/summarize_score_tag.py:123`). If `max_input_chars` is left at its small default, the middleware will never see a prompt large enough to trigger long-context routing — defeating the feature.

**Pick a model that actually exists in the example providers.** The current `configs/providers.toml.example` defines providers `glm`, `deepseek`, `openai`, and `ollama`. There is no `google` provider, so do **not** use `google/gemini-1.5-pro` in the pipeline example. Instead, use one of the existing models that has the largest `max_input_tokens` you set in Step 1 — e.g. `glm/glm-4-plus` (set its `max_input_tokens = 128_000` in step 1).

In the active `summarize` `llm_stage` block (the uncommented block with `name = "summarize"`), add:

```toml
# Optional: when prompt > 80% of model input cap (or on overflow), route here.
# Must reference a (provider, model) pair that exists in providers.toml.
long_context_model = "glm/glm-4-plus"
# IMPORTANT: bump max_input_chars when long_context_model is set; the processor
# truncates BEFORE the middleware sees the prompt, so a low cap prevents routing.
max_input_chars = 400_000
```

- [ ] **Step 3: Verify the examples parse, validate, AND cross-reference correctly**

Three layers of validation are required, because pydantic does **not** cross-validate between two separate config files:

1. TOML syntax (`tomllib.loads`).
2. Single-file schema (`ProvidersConfig.model_validate`, `PipelineConfig.model_validate`).
3. **Cross-file integrity** — every `model` / `retry_model` / `fallback_model` / `fallback_model_2` / `long_context_model` referenced by a stage must resolve to a `(provider, model_name)` pair that actually exists in `providers.toml.example`. This is what catches the `google/gemini-1.5-pro` mistake.

Run:

```bash
python3 - <<'PY'
import tomllib, pathlib, sys
from sluice.config import ProvidersConfig, PipelineConfig

prov_raw = tomllib.loads(pathlib.Path('configs/providers.toml.example').read_text())
prov = ProvidersConfig.model_validate(prov_raw)
print('OK providers')

pipe_raw = tomllib.loads(pathlib.Path('configs/pipelines/ai_news.toml.example').read_text())
pipe = PipelineConfig.model_validate(pipe_raw)
print('OK pipeline')

# Cross-check: collect all known (provider, model_name) specs.
known = {f"{p.name}/{m.model_name}" for p in prov.providers for m in p.models}

ref_fields = ('model', 'retry_model', 'fallback_model', 'fallback_model_2', 'long_context_model')
errors = []
for st in pipe.stages:
    for f in ref_fields:
        spec = getattr(st, f, None)
        if spec and spec not in known:
            errors.append(f"stage {st.name!r}.{f} -> {spec!r} not in providers.toml")

# Ensure the active example, not only a commented sample block, demonstrates the feature.
summarize = next((st for st in pipe.stages if getattr(st, 'name', None) == 'summarize'), None)
if summarize is None:
    errors.append("active stage 'summarize' not found")
else:
    if getattr(summarize, 'long_context_model', None) != 'glm/glm-4-plus':
        errors.append("active stage 'summarize' must set long_context_model = 'glm/glm-4-plus'")
    if getattr(summarize, 'max_input_chars', 0) < 400_000:
        errors.append("active stage 'summarize' must set max_input_chars >= 400_000")

if errors:
    print('CROSS-REF ERRORS:')
    for e in errors:
        print(' -', e)
    sys.exit(1)
print('OK cross-ref')
print('OK active long-context example')
PY
```

Expected: `OK providers`, `OK pipeline`, `OK cross-ref`, and `OK active long-context example`. Any dangling reference or missing active example setting fails the script with exit 1.

- [ ] **Step 4: Commit**

```bash
git add configs/providers.toml.example configs/pipelines/ai_news.toml.example
git commit -m "docs(config): demonstrate max_input_tokens and long_context_model"
```

---

### Task 9: End-to-end verification

**Files:**
- None (verification only)

- [ ] **Step 1: Full test suite + coverage**

```bash
python3 -m pytest -q
```

Expected: all green. No skipped LLM tests except those gated on real keys.

- [ ] **Step 2: Lint**

```bash
python3 -m ruff check sluice tests
python3 -m ruff format --check sluice tests
```

Expected: no errors.

- [ ] **Step 3: Manual smoke (optional, requires real keys)**

Run a small pipeline with a stage configured to use `long_context_model`:

```bash
python3 -m sluice.cli run --config configs/sluice.toml.example --pipeline configs/pipelines/ai_news.toml.example --dry-run
```

Expected: pipeline boots without error. (Dry-run still calls LLMs per existing semantics; skip if no keys available.)

- [ ] **Step 4: Commit any small fixups discovered**

```bash
git status
# If anything dirty:
git add -p <shared-tracked-files>
git add <new-files>
git diff --cached
git commit -m "chore: post-merge fixups for llm middleware"
```

---

## Self-Review

**Spec coverage** — checked each spec section against tasks:

| Spec section | Implemented in |
|---|---|
| `tokens.py` (count + truncate) | Task 2 |
| `ModelEntry.max_input_tokens` | Task 3 |
| Stage `long_context_model` field | Task 4 |
| `client.py` raw_chat refactor | Task 5 |
| Middleware routing + retry + fallback + overflow | Task 6 |
| `builders.py` wiring | Task 7 |
| Example config updates | Task 8 |
| `tiktoken` dep | Task 1 |
| 401/403 advances chain (behaviour change) | Task 6 (tested in `test_401_advances_chain`) |
| Overflow keyword detection | Task 6 (`_OVERFLOW_KEYWORDS`) |
| Pre-emptive trim | Task 6 (`_fit_to_budget`) |

**Placeholder scan** — no TODOs / "implement later" / generic "add tests" left. Every code step ships actual code; every test step ships a concrete assertion.

**Type consistency** — `RawCallResult.text`, `LLMMiddleware.chat() -> str`, `StageLLMConfig.long_context_model: str | None` are consistent across Tasks 5/6/7. `count_message_tokens` signature stable across Task 2 + Task 6 use sites. `last_cost` typed `CallCost | None` matches `RunBudget.record(cost: CallCost)` in `budget.py`.

---

**Plan complete and saved to `docs/superpowers/plans/2026-05-01-llm-middleware.md`.** Two execution options:

1. **Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.
2. **Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

Which approach?
