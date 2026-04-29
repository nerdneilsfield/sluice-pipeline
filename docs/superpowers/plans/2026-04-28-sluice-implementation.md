# Sluice Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a code-native, configurable RSS-to-Notion daily pipeline ("sluice") that replaces n8n, with pluggable Source/Fetcher/Processor/Sink protocols, weighted LLM provider pool with fallback chains, SQLite state for dedupe + idempotent sinks, and Prefect for scheduling.

**Architecture:** Linear DAG per pipeline (TOML-declared). Plugins register via decorators into typed registries. Pydantic v2 parses the TOML with discriminated unions. Async throughout (`asyncio` + `aiosqlite` + `httpx`). Prefect wraps the flow for scheduling/observability; the pipeline-level commit-after-success contract guarantees external sinks are idempotent across retries.

**Tech Stack:** Python 3.11+, Pydantic v2, aiosqlite, httpx, feedparser, trafilatura, jinja2, prefect 3, typer, pytest + pytest-asyncio, notionify (existing local package).

**Spec:** `docs/superpowers/specs/2026-04-28-sluice-design.md`

**Deferred from v1 (review-driven scope cuts):**
- Native Anthropic Messages API client. v1 supports `openai_compatible`
  providers only; access Claude via OpenRouter / Cloudflare AI Gateway /
  any aggregator that exposes an OpenAI-shape endpoint.
- Per-tier worker counts on the LLM chain (`retry_workers`,
  `fallback_workers`, …). Pydantic accepts the fields (forward-compat),
  but v1 honors only the stage-level `workers`.
- Provider `active_windows` time-window routing.
- `sluice gc` for cleanup of resolved/dead-lettered failed_items.
- **Notion page cover image**. notionify v3's `create_page_with_markdown`
  has no `cover` slot; setting one needs an extra `pages.update` Notion
  API call. Removed from v1 sink config so we don't ship a knob that
  silently no-ops. Re-add after notionify exposes covers or via a small
  post-create patch in DefaultNotionifyAdapter.

These are tracked in spec Open Questions and will be picked up in v1.1.

---

## File Structure

```
sluice/
├── pyproject.toml
├── sluice/
│   ├── __init__.py
│   ├── core/
│   │   ├── item.py              # Item, Attachment dataclasses
│   │   ├── protocols.py         # Source/Fetcher/Processor/Sink/LLMProvider Protocols
│   │   └── errors.py            # Typed exception hierarchy
│   ├── context.py               # PipelineContext dataclass
│   ├── config.py                # Pydantic v2 models (all TOML schemas)
│   ├── registry.py              # Plugin decorators + lookup
│   ├── state/
│   │   ├── db.py                # aiosqlite + PRAGMA user_version migrations
│   │   ├── migrations/0001_init.sql
│   │   ├── seen.py              # SeenStore
│   │   ├── failures.py          # FailureStore (lifecycle: failed→dead_letter→resolved)
│   │   ├── emissions.py         # EmissionStore (sink_emissions)
│   │   ├── cache.py             # UrlCacheStore (TTL eviction)
│   │   └── run_log.py           # RunLog
│   ├── llm/
│   │   ├── provider.py          # Provider/Base/Key dataclasses, weighted selection
│   │   ├── pool.py              # ProviderPool (singleton), endpoint acquire+semaphore
│   │   ├── client.py            # LLMClient: chat() with retry→fallback→fb2 chain
│   │   └── budget.py            # Cost projector + RunBudget tracker
│   ├── sources/
│   │   ├── base.py              # @register_source decorator
│   │   └── rss.py               # RssSource
│   ├── fetchers/
│   │   ├── base.py              # @register_fetcher
│   │   ├── chain.py             # FetcherChain (min_chars, on_all_failed, cache integration)
│   │   ├── trafilatura_fetcher.py
│   │   ├── firecrawl.py
│   │   └── jina_reader.py
│   ├── processors/
│   │   ├── base.py              # @register_processor
│   │   ├── dedupe.py
│   │   ├── filter.py            # rule engine
│   │   ├── field_filter.py      # truncate/drop ops
│   │   ├── fetcher_apply.py     # invokes FetcherChain
│   │   ├── llm_stage.py         # per_item / aggregate modes
│   │   └── render.py            # jinja2 with fixed context dict
│   ├── sinks/
│   │   ├── base.py              # Sink ABC: emit() dispatches to create()/update() via emissions lookup
│   │   ├── file_md.py
│   │   └── notion.py            # wraps notionify; chunks to max_block_chars
│   ├── url_canon.py             # URL canonicalization (lowercase host, strip trackers)
│   ├── window.py                # window/lookback computation
│   ├── flow.py                  # Prefect flow assembler: TOML → DAG; commit-after-success
│   └── cli.py                   # typer: run/list/deploy/validate/failures
├── tests/
│   └── (mirrors sluice/ layout)
├── configs/
│   ├── sluice.toml.example
│   ├── providers.toml.example
│   └── pipelines/ai_news.toml.example
├── prompts/
│   ├── summarize_zh.md
│   └── daily_brief_zh.md
├── data/                        # gitignored, runtime
└── out/                         # gitignored, runtime
```

---

## Phase 0 — Project Bootstrap

### Task 0.1: Scaffold pyproject + layout

**Files:**
- Create: `pyproject.toml`, `sluice/__init__.py`, `tests/__init__.py`, `.gitignore`, `README.md`

- [ ] **Step 1: Create `pyproject.toml`**

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "sluice"
version = "0.1.0"
description = "Code-native, configurable RSS-to-Notion pipeline"
requires-python = ">=3.11"
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
]

[project.optional-dependencies]
dev = ["pytest>=8", "pytest-asyncio>=0.23", "pytest-cov>=5",
       "respx>=0.21", "freezegun>=1.5", "ruff>=0.5"]

[project.scripts]
sluice = "sluice.cli:app"

[tool.pytest.ini_options]
asyncio_mode = "auto"
addopts = "-ra --cov=sluice --cov-report=term-missing"

[tool.ruff]
line-length = 100
target-version = "py311"
```

- [ ] **Step 2: Create `.gitignore`**

```
__pycache__/
*.egg-info/
.venv/
.pytest_cache/
.coverage
htmlcov/
data/
out/
configs/sluice.toml
configs/providers.toml
configs/pipelines/*.toml
!configs/**/*.example
```

- [ ] **Step 3: Initialize git, install dev deps**

Run:
```bash
git init
python -m venv .venv && . .venv/bin/activate
pip install -e '.[dev]'
```
Expected: `pip install` completes; `pytest --version` works.

- [ ] **Step 4: Commit**

```bash
git add pyproject.toml .gitignore sluice/__init__.py tests/__init__.py README.md
git commit -m "chore: scaffold project layout"
```

---

## Phase 1 — Core Types

### Task 1.1: Attachment + Item dataclasses

**Files:**
- Create: `sluice/core/__init__.py`, `sluice/core/item.py`, `tests/core/test_item.py`

- [ ] **Step 1: Write failing test**

```python
# tests/core/test_item.py
from datetime import datetime, timezone
import pytest
from sluice.core.item import Item, Attachment

def make_item(**kw):
    base = dict(
        source_id="src1", pipeline_id="p1",
        guid="g1", url="https://example.com/a",
        title="hi", published_at=datetime(2026, 4, 28, tzinfo=timezone.utc),
        raw_summary=None,
    )
    base.update(kw)
    return Item(**base)

def test_get_dataclass_field():
    it = make_item(summary="hello")
    assert it.get("summary") == "hello"

def test_get_extras_field():
    it = make_item()
    it.extras["relevance"] = 7.5
    assert it.get("extras.relevance") == 7.5

def test_get_nested_attachment_index():
    it = make_item()
    it.attachments.append(Attachment(url="https://x/y.mp3", mime_type="audio/mpeg"))
    assert it.get("attachments.0.url") == "https://x/y.mp3"

def test_get_missing_returns_none():
    it = make_item()
    assert it.get("extras.nope") is None
    assert it.get("nonexistent") is None

def test_get_default_value():
    it = make_item()
    assert it.get("extras.score", default=0) == 0
```

- [ ] **Step 2: Run — expect ImportError**

`pytest tests/core/test_item.py -v` → FAIL.

- [ ] **Step 3: Implement**

```python
# sluice/core/item.py
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

_SENTINEL = object()

@dataclass
class Attachment:
    url: str
    mime_type: str | None = None
    length: int | None = None

@dataclass
class Item:
    source_id: str
    pipeline_id: str
    guid: str | None
    url: str
    title: str
    published_at: datetime | None
    raw_summary: str | None
    fulltext: str | None = None
    attachments: list[Attachment] = field(default_factory=list)
    summary: str | None = None
    extras: dict[str, Any] = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)

    def get(self, path: str, default: Any = None) -> Any:
        cur: Any = self
        for part in path.split("."):
            if isinstance(cur, dict):
                cur = cur.get(part, _SENTINEL)
            elif isinstance(cur, list):
                try:
                    cur = cur[int(part)]
                except (ValueError, IndexError):
                    return default
            else:
                cur = getattr(cur, part, _SENTINEL)
            if cur is _SENTINEL:
                return default
        return cur


def compute_item_key(item: "Item") -> str:
    """Cascading key: guid → sha256(canonical_url) → sha256(title+published_iso)."""
    import hashlib
    if item.guid and item.guid.strip():
        return item.guid.strip()
    if item.url:
        return hashlib.sha256(item.url.encode()).hexdigest()
    pub = item.published_at.isoformat() if item.published_at else ""
    return hashlib.sha256((item.title + pub).encode()).hexdigest()
```

Add to the same test file (Task 1.1):

```python
# tests/core/test_item.py — add these tests
import hashlib
from sluice.core.item import compute_item_key

def test_key_uses_guid():
    assert compute_item_key(make_item(guid="g-1")) == "g-1"

def test_key_falls_back_to_url():
    it = make_item(); it.guid = None
    expected = hashlib.sha256(it.url.encode()).hexdigest()
    assert compute_item_key(it) == expected

def test_key_falls_back_to_title_plus_date():
    it = make_item(); it.guid = None; it.url = ""
    expected = hashlib.sha256(
        (it.title + it.published_at.isoformat()).encode()
    ).hexdigest()
    assert compute_item_key(it) == expected
```

- [ ] **Step 4: Run — expect PASS**

`pytest tests/core/test_item.py -v` → PASS.

- [ ] **Step 5: Commit**

```bash
git add sluice/core/__init__.py sluice/core/item.py tests/core/__init__.py tests/core/test_item.py
git commit -m "feat(core): add Item and Attachment dataclasses with dot-path get"
```

### Task 1.2: Error hierarchy

**Files:**
- Create: `sluice/core/errors.py`, `tests/core/test_errors.py`

- [ ] **Step 1: Write failing test**

```python
# tests/core/test_errors.py
import pytest
from sluice.core.errors import (
    SluiceError, ConfigError, FetchError, AllFetchersFailed,
    LLMError, RateLimitError, QuotaExhausted, AllProvidersExhausted,
    StageError, SinkError, BudgetExceeded,
)

def test_hierarchy():
    assert issubclass(ConfigError, SluiceError)
    assert issubclass(AllFetchersFailed, FetchError)
    assert issubclass(RateLimitError, LLMError)
    assert issubclass(QuotaExhausted, LLMError)
    assert issubclass(AllProvidersExhausted, LLMError)
    assert issubclass(BudgetExceeded, SluiceError)

def test_carries_context():
    e = AllFetchersFailed("https://x/y", attempts=["traf", "firecrawl"])
    assert e.url == "https://x/y"
    assert e.attempts == ["traf", "firecrawl"]
```

- [ ] **Step 2: Run — expect FAIL**

- [ ] **Step 3: Implement**

```python
# sluice/core/errors.py
class SluiceError(Exception): pass
class ConfigError(SluiceError): pass
class StageError(SluiceError): pass
class SinkError(SluiceError): pass
class BudgetExceeded(SluiceError): pass

class FetchError(SluiceError): pass
class AllFetchersFailed(FetchError):
    def __init__(self, url: str, attempts: list[str]):
        super().__init__(f"all fetchers failed for {url}: tried {attempts}")
        self.url = url
        self.attempts = attempts

class LLMError(SluiceError): pass
class RateLimitError(LLMError): pass
class QuotaExhausted(LLMError): pass
class AllProvidersExhausted(LLMError):
    def __init__(self, model_chain: list[str]):
        super().__init__(f"all tiers exhausted: {model_chain}")
        self.model_chain = model_chain
```

- [ ] **Step 4: Run + Commit**

`pytest tests/core/test_errors.py -v` → PASS.

```bash
git add sluice/core/errors.py tests/core/test_errors.py
git commit -m "feat(core): add typed exception hierarchy"
```

### Task 1.3: Protocols + PipelineContext

**Files:**
- Create: `sluice/core/protocols.py`, `sluice/context.py`, `tests/test_context.py`

- [ ] **Step 1: Write failing test**

```python
# tests/test_context.py
from sluice.context import PipelineContext
from sluice.core.item import Item
from datetime import datetime, timezone

def test_context_holds_items_and_dict():
    ctx = PipelineContext(pipeline_id="p", run_key="p/2026-04-28",
                          run_date="2026-04-28", items=[], context={})
    ctx.context["daily_brief"] = "..."
    assert ctx.context["daily_brief"] == "..."
    assert ctx.items == []

def test_context_stats_init():
    ctx = PipelineContext(pipeline_id="p", run_key="p/2026-04-28",
                          run_date="2026-04-28", items=[], context={})
    assert ctx.stats.items_in == 0
    assert ctx.stats.llm_calls == 0
    assert ctx.stats.est_cost_usd == 0.0
```

- [ ] **Step 2: Run — expect FAIL**

- [ ] **Step 3: Implement**

```python
# sluice/core/protocols.py
from typing import Protocol, AsyncIterator
from datetime import timedelta
from sluice.core.item import Item

class Source(Protocol):
    name: str
    async def fetch(self, window_start, window_end) -> AsyncIterator[Item]: ...

class Fetcher(Protocol):
    name: str
    async def extract(self, url: str) -> str: ...

class Processor(Protocol):
    name: str
    async def process(self, ctx) -> "PipelineContext": ...

class Sink(Protocol):
    id: str
    type: str
    async def emit(self, ctx) -> "SinkResult": ...

class LLMProvider(Protocol):
    """Single endpoint chat call. v1 implementations live in sluice.llm.
    Documented as a Protocol for symmetry with the other plugin kinds, even
    though v1 wires the LLM stack directly through ProviderPool/LLMClient
    rather than through the registry."""
    name: str
    async def chat(self, messages: list[dict], model: str) -> str: ...
```

```python
# sluice/context.py
from dataclasses import dataclass, field
from typing import Any
from sluice.core.item import Item

@dataclass
class RunStats:
    items_in: int = 0
    items_out: int = 0
    llm_calls: int = 0
    est_cost_usd: float = 0.0

@dataclass
class PipelineContext:
    pipeline_id: str
    run_key: str
    run_date: str
    items: list[Item]
    context: dict[str, Any]
    stats: RunStats = field(default_factory=RunStats)
    items_resolved_from_failures: list[Item] = field(default_factory=list)

@dataclass
class SinkResult:
    sink_id: str
    sink_type: str
    external_id: str
    created: bool   # True = created new, False = updated existing
```

Move `SinkResult` to `protocols.py` import to keep things tidy:

```python
# add to sluice/core/protocols.py
from dataclasses import dataclass

@dataclass
class SinkResult:
    sink_id: str
    sink_type: str
    external_id: str
    created: bool
```
(Remove from `context.py`.)

- [ ] **Step 4: Run + Commit**

```bash
git add sluice/core/protocols.py sluice/context.py tests/test_context.py
git commit -m "feat(core): add Protocols and PipelineContext"
```

### Task 1.4: URL canonicalization + window math

**Files:**
- Create: `sluice/url_canon.py`, `sluice/window.py`, `tests/test_url_canon.py`, `tests/test_window.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_url_canon.py
from sluice.url_canon import canonical_url

def test_lowercase_scheme_host():
    assert canonical_url("HTTP://Example.COM/Path") == "http://example.com/Path"

def test_strip_fragment():
    assert canonical_url("https://x.com/a#frag") == "https://x.com/a"

def test_strip_tracking_params():
    assert canonical_url("https://x.com/a?utm_source=foo&id=42&fbclid=z") \
        == "https://x.com/a?id=42"

def test_strip_all_known_trackers():
    u = "https://x.com/a?utm_source=a&utm_medium=b&utm_campaign=c&gclid=d&fbclid=e&ref=f&ref_src=g&id=42"
    assert canonical_url(u) == "https://x.com/a?id=42"

def test_keep_path_case():
    assert canonical_url("https://X.com/Foo/Bar") == "https://x.com/Foo/Bar"
```

```python
# tests/test_window.py
from datetime import datetime, timezone, timedelta
from sluice.window import compute_window, parse_duration

def test_parse_duration():
    assert parse_duration("24h") == timedelta(hours=24)
    assert parse_duration("3d") == timedelta(days=3)
    assert parse_duration("90m") == timedelta(minutes=90)
    assert parse_duration("7d") == timedelta(days=7)

def test_compute_window_default_overlap():
    now = datetime(2026, 4, 28, 8, 0, 0, tzinfo=timezone.utc)
    start, end = compute_window(now=now, window="24h", lookback_overlap=None)
    # default = max(1h, window*0.2) = max(1h, 4.8h) = 4.8h
    assert end == now
    assert end - start == timedelta(hours=24) + timedelta(hours=24*0.2)

def test_compute_window_explicit_overlap():
    now = datetime(2026, 4, 28, 8, 0, 0, tzinfo=timezone.utc)
    start, end = compute_window(now=now, window="24h", lookback_overlap="2h")
    assert end - start == timedelta(hours=26)

def test_min_overlap_one_hour():
    now = datetime(2026, 4, 28, tzinfo=timezone.utc)
    start, end = compute_window(now=now, window="1h", lookback_overlap=None)
    # window*0.2 = 12min < 1h floor
    assert end - start == timedelta(hours=2)
```

- [ ] **Step 2: Run — expect FAIL**

- [ ] **Step 3: Implement**

```python
# sluice/url_canon.py
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

TRACKING_PARAMS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "fbclid", "gclid", "ref", "ref_src", "mc_cid", "mc_eid",
}

def canonical_url(url: str) -> str:
    parts = urlsplit(url)
    scheme = parts.scheme.lower()
    netloc = parts.netloc.lower()
    query = "&".join(
        f"{k}={v}" for k, v in parse_qsl(parts.query, keep_blank_values=True)
        if k not in TRACKING_PARAMS
    )
    return urlunsplit((scheme, netloc, parts.path, query, ""))
```

```python
# sluice/window.py
from datetime import datetime, timedelta
import re

_DUR_RE = re.compile(r"^(\d+)([smhd])$")
_UNITS = {"s": "seconds", "m": "minutes", "h": "hours", "d": "days"}

def parse_duration(s: str) -> timedelta:
    m = _DUR_RE.match(s.strip())
    if not m:
        raise ValueError(f"bad duration: {s}")
    return timedelta(**{_UNITS[m.group(2)]: int(m.group(1))})

def compute_window(*, now: datetime, window: str,
                   lookback_overlap: str | None) -> tuple[datetime, datetime]:
    w = parse_duration(window)
    if lookback_overlap is None:
        overlap = max(timedelta(hours=1), w * 0.2)
    else:
        overlap = parse_duration(lookback_overlap)
    return now - (w + overlap), now
```

- [ ] **Step 4: Run + Commit**

`pytest tests/test_url_canon.py tests/test_window.py -v` → PASS.

```bash
git add sluice/url_canon.py sluice/window.py tests/test_url_canon.py tests/test_window.py
git commit -m "feat: URL canonicalization and window/lookback math"
```

---

## Phase 2 — Plugin Registry

### Task 2.1: Generic registry with decorators

**Files:**
- Create: `sluice/registry.py`, `tests/test_registry.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_registry.py
import pytest
from sluice.registry import (
    register_source, register_fetcher, register_processor, register_sink,
    get_source, get_fetcher, get_processor, get_sink, all_processors,
)
from sluice.core.errors import ConfigError

def test_register_and_lookup():
    @register_processor("widget")
    class W: pass
    assert get_processor("widget") is W

def test_unknown_raises():
    with pytest.raises(ConfigError, match="unknown processor"):
        get_processor("nonexistent")

def test_duplicate_raises():
    @register_fetcher("dup_one")
    class A: pass
    with pytest.raises(ConfigError, match="already registered"):
        @register_fetcher("dup_one")
        class B: pass

def test_separate_namespaces():
    @register_source("xyz")
    class S: pass
    @register_processor("xyz")
    class P: pass
    assert get_source("xyz") is S
    assert get_processor("xyz") is P
```

- [ ] **Step 2: Run — expect FAIL**

- [ ] **Step 3: Implement**

```python
# sluice/registry.py
from sluice.core.errors import ConfigError

_SOURCES: dict[str, type] = {}
_FETCHERS: dict[str, type] = {}
_PROCESSORS: dict[str, type] = {}
_SINKS: dict[str, type] = {}

def _make_register(reg: dict[str, type], kind: str):
    def deco(name: str):
        def inner(cls):
            if name in reg:
                raise ConfigError(f"{kind} {name!r} already registered")
            reg[name] = cls
            return cls
        return inner
    return deco

def _make_get(reg: dict[str, type], kind: str):
    def get(name: str):
        if name not in reg:
            raise ConfigError(f"unknown {kind}: {name!r}")
        return reg[name]
    return get

register_source    = _make_register(_SOURCES,    "source")
register_fetcher   = _make_register(_FETCHERS,   "fetcher")
register_processor = _make_register(_PROCESSORS, "processor")
register_sink      = _make_register(_SINKS,      "sink")

get_source    = _make_get(_SOURCES,    "source")
get_fetcher   = _make_get(_FETCHERS,   "fetcher")
get_processor = _make_get(_PROCESSORS, "processor")
get_sink      = _make_get(_SINKS,      "sink")

def all_processors() -> dict[str, type]: return dict(_PROCESSORS)
def all_fetchers()   -> dict[str, type]: return dict(_FETCHERS)
def all_sources()    -> dict[str, type]: return dict(_SOURCES)
def all_sinks()      -> dict[str, type]: return dict(_SINKS)
```

- [ ] **Step 4: Run + Commit**

```bash
git add sluice/registry.py tests/test_registry.py
git commit -m "feat: plugin registry with decorators per kind"
```

---

## Phase 3 — Configuration (Pydantic v2)

### Task 3.1: Provider/key/base config models

**Files:**
- Create: `sluice/config.py`, `tests/config/test_provider_config.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/config/test_provider_config.py
import pytest
import tomllib
from sluice.config import ProvidersConfig

SAMPLE = """
[[providers]]
name = "glm"
type = "openai_compatible"

[[providers.base]]
url = "https://open.bigmodel.cn/api/paas/v4"
weight = 1
key = [
  { value = "env:GLM_API_KEY",   weight = 3 },
  { value = "env:GLM_API_KEY_2", weight = 1, quota_duration = 18000 },
]

[[providers.models]]
model_name = "glm-4-flash"
input_price_per_1k  = 0.0001
output_price_per_1k = 0.0001
"""

def test_parse_provider():
    data = tomllib.loads(SAMPLE)
    cfg = ProvidersConfig.model_validate(data)
    p = cfg.providers[0]
    assert p.name == "glm"
    assert p.type == "openai_compatible"
    assert p.base[0].url.startswith("https://")
    assert p.base[0].key[0].value == "env:GLM_API_KEY"
    assert p.base[0].key[1].quota_duration == 18000
    assert p.models[0].input_price_per_1k == 0.0001

def test_unknown_provider_type_rejected():
    bad = SAMPLE.replace("openai_compatible", "made_up")
    with pytest.raises(Exception):
        ProvidersConfig.model_validate(tomllib.loads(bad))
```

- [ ] **Step 2: Run — expect FAIL**

- [ ] **Step 3: Implement** (ProvidersConfig only — pipeline schema in next task)

```python
# sluice/config.py  (initial version; will grow in 3.2/3.3)
from typing import Literal
from pydantic import BaseModel, Field

class KeyConfig(BaseModel):
    value: str
    weight: int = 1
    quota_duration: int | None = None
    reset_time: str | None = None
    quota_error_tokens: list[str] = Field(default_factory=list)

class BaseEndpoint(BaseModel):
    url: str
    weight: int = 1
    key: list[KeyConfig]
    extra_headers: dict[str, str] = Field(default_factory=dict)
    active_windows: list[str] = Field(default_factory=list)
    active_timezone: str | None = None

class ModelEntry(BaseModel):
    model_name: str
    is_stream: bool = True
    is_support_json_schema: bool = False
    is_support_json_object: bool = False
    input_price_per_1k: float = 0.0
    output_price_per_1k: float = 0.0

class Provider(BaseModel):
    name: str
    # v1: only openai_compatible. Anthropic-native (Messages API) deferred
    # to v1.1 because httpx call shape differs. Use Anthropic via an
    # OpenAI-compatible aggregator (OpenRouter, Cloudflare AI Gateway, etc.)
    # in the meantime — the user's existing keys still work.
    type: Literal["openai_compatible"]
    base: list[BaseEndpoint]
    models: list[ModelEntry]
    extra_headers: dict[str, str] = Field(default_factory=dict)

class ProvidersConfig(BaseModel):
    providers: list[Provider]
```

- [ ] **Step 4: Run + Commit**

```bash
git add sluice/config.py tests/config/__init__.py tests/config/test_provider_config.py
git commit -m "feat(config): provider pool Pydantic models"
```

### Task 3.2: Pipeline config with discriminated stage union

**Files:**
- Modify: `sluice/config.py`
- Create: `tests/config/test_pipeline_config.py`

- [ ] **Step 1: Write failing test**

```python
# tests/config/test_pipeline_config.py
import tomllib, pytest
from sluice.config import PipelineConfig
from sluice.core.errors import ConfigError

PIPE = """
id = "ai_news"
cron = "0 8 * * *"
window = "24h"

[[sources]]
type = "rss"
url  = "https://example.com/feed"
tag  = "ai"

[[stages]]
name = "dedupe"
type = "dedupe"

[[stages]]
name = "fetch"
type = "fetcher_apply"
write_field = "fulltext"

[[stages]]
name = "summarize"
type = "llm_stage"
mode = "per_item"
input_field = "fulltext"
output_field = "summary"
prompt_file = "prompts/summarize_zh.md"
model = "glm/glm-4-flash"

[[stages]]
name = "drop_short"
type = "filter"
mode = "keep_if_all"
rules = [{ field = "summary", op = "min_length", value = 50 }]

[[stages]]
name = "render"
type = "render"
template = "templates/daily.md.j2"
output_field = "context.markdown"

[[sinks]]
id = "local"
type = "file_md"
input = "context.markdown"
path = "./out/{run_date}.md"

[[sinks]]
id = "notion_main"
type = "notion"
input = "context.markdown"
parent_id = "env:N"
parent_type = "database"
token = "env:NOTION_TOKEN"
title_template = "X · {run_date}"
mode = "upsert"
"""

def test_parse_pipeline():
    cfg = PipelineConfig.model_validate(tomllib.loads(PIPE))
    assert cfg.id == "ai_news"
    assert cfg.window == "24h"
    assert cfg.stages[0].type == "dedupe"
    assert cfg.stages[2].type == "llm_stage"
    assert cfg.stages[2].model == "glm/glm-4-flash"
    assert cfg.sinks[0].id == "local"

def test_sink_id_required():
    bad = PIPE.replace('id = "local"\n', "")
    with pytest.raises(Exception):
        PipelineConfig.model_validate(tomllib.loads(bad))

def test_unknown_stage_type_rejected():
    bad = PIPE.replace('type = "dedupe"', 'type = "made_up"')
    with pytest.raises(Exception):
        PipelineConfig.model_validate(tomllib.loads(bad))

def test_filter_op_validated():
    bad = PIPE.replace('"min_length"', '"weird_op"')
    with pytest.raises(Exception):
        PipelineConfig.model_validate(tomllib.loads(bad))
```

- [ ] **Step 2: Run — expect FAIL**

- [ ] **Step 3: Extend `sluice/config.py`**

```python
# Append to sluice/config.py
from typing import Annotated, Any, Union, Literal
from pydantic import BaseModel, Field, model_validator

# ---------- Sources ----------
class RssSourceConfig(BaseModel):
    type: Literal["rss"]
    url: str
    tag: str | None = None
    name: str | None = None

SourceConfig = Annotated[
    Union[RssSourceConfig],
    Field(discriminator="type"),
]

# ---------- Filter rules ----------
FilterOp = Literal[
    "gt", "gte", "lt", "lte", "eq",
    "matches", "not_matches", "contains", "not_contains",
    "in", "not_in", "min_length", "max_length",
    "newer_than", "older_than", "exists", "not_exists",
]

class FilterRule(BaseModel):
    field: str
    op: FilterOp
    value: Any = None

# ---------- Stages ----------
class DedupeConfig(BaseModel):
    type: Literal["dedupe"]
    name: str
    key_strategy: Literal["auto", "guid_only", "url_only"] = "auto"

class FetcherApplyConfig(BaseModel):
    type: Literal["fetcher_apply"]
    name: str
    write_field: str = "fulltext"
    skip_if_field_longer_than: int | None = None

class FilterConfig(BaseModel):
    type: Literal["filter"]
    name: str
    mode: Literal["keep_if_all", "keep_if_any", "drop_if_all", "drop_if_any"]
    rules: list[FilterRule]

class FieldOp(BaseModel):
    op: Literal["truncate", "drop"]
    field: str
    n: int | None = None

class FieldFilterConfig(BaseModel):
    type: Literal["field_filter"]
    name: str
    ops: list[FieldOp]

class LLMStageConfig(BaseModel):
    type: Literal["llm_stage"]
    name: str
    mode: Literal["per_item", "aggregate"]
    input_field: str
    output_field: str | None = None
    output_target: str | None = None  # for aggregate mode
    prompt_file: str
    model: str
    retry_model: str | None = None
    fallback_model: str | None = None
    fallback_model_2: str | None = None
    workers: int = 4
    concurrency: int = 4
    retry_workers: int | None = None
    retry_concurrency: int | None = None
    fallback_workers: int | None = None
    fallback_concurrency: int | None = None
    fallback_2_workers: int | None = None
    fallback_2_concurrency: int | None = None
    output_parser: Literal["text", "json"] = "text"
    output_schema: str | None = None
    on_parse_error: Literal["fail", "skip", "default"] = "fail"
    on_parse_error_default: dict[str, Any] = Field(default_factory=dict)
    max_input_chars: int = 20000
    truncate_strategy: Literal["head_tail", "head", "error"] = "head_tail"

    @model_validator(mode="after")
    def _check_output_target(self):
        if self.mode == "per_item" and not self.output_field:
            raise ValueError("per_item mode requires output_field")
        if self.mode == "aggregate" and not self.output_target:
            raise ValueError("aggregate mode requires output_target")
        return self

class RenderConfig(BaseModel):
    type: Literal["render"]
    name: str
    template: str
    output_field: str  # always "context.<key>"

StageConfig = Annotated[
    Union[
        DedupeConfig, FetcherApplyConfig, FilterConfig,
        FieldFilterConfig, LLMStageConfig, RenderConfig,
    ],
    Field(discriminator="type"),
]

# ---------- Sinks ----------
class CommonSinkFields(BaseModel):
    id: str  # REQUIRED
    emit_on_empty: bool = False

class FileMdSinkConfig(CommonSinkFields):
    type: Literal["file_md"]
    input: str
    path: str

class NotionSinkConfig(CommonSinkFields):
    type: Literal["notion"]
    input: str
    parent_id: str
    parent_type: Literal["database", "page"]
    title_template: str
    token: str = "env:NOTION_TOKEN"  # notionify integration token
    properties: dict[str, Any] = Field(default_factory=dict)
    mode: Literal["upsert", "create_once", "create_new"] = "upsert"
    max_block_chars: int = 1900
    # cover_image_from removed — Notion page cover deferred to v1.1
    # (see "Deferred from v1" at top of plan)

SinkConfig = Annotated[
    Union[FileMdSinkConfig, NotionSinkConfig],
    Field(discriminator="type"),
]

# ---------- Pipeline ----------
class FetcherChainOverride(BaseModel):
    chain: list[str] | None = None
    min_chars: int | None = None
    on_all_failed: Literal["skip", "continue_empty"] | None = None

class CacheOverride(BaseModel):
    enabled: bool = True
    ttl: str = "7d"

class PipelineLimits(BaseModel):
    max_items_per_run: int = 50
    item_overflow_policy: Literal["drop_oldest", "drop_newest"] = "drop_oldest"
    max_llm_calls_per_run: int = 500
    max_estimated_cost_usd: float = 5.0

class PipelineFailures(BaseModel):
    retry_failed: bool = True
    max_retries: int = 3
    retry_backoff: Literal["next_run"] = "next_run"

class StateOverride(BaseModel):
    db_path: str | None = None

class PipelineConfig(BaseModel):
    id: str
    description: str = ""
    enabled: bool = True
    cron: str | None = None
    timezone: str | None = None
    window: str = "24h"
    lookback_overlap: str | None = None
    sources: list[SourceConfig]
    stages: list[StageConfig]
    sinks: list[SinkConfig]
    fetcher: FetcherChainOverride = Field(default_factory=FetcherChainOverride)
    cache:   CacheOverride         = Field(default_factory=CacheOverride)
    limits:  PipelineLimits        = Field(default_factory=PipelineLimits)
    failures: PipelineFailures     = Field(default_factory=PipelineFailures)
    state:   StateOverride         = Field(default_factory=StateOverride)

    @model_validator(mode="after")
    def _unique_sink_ids(self):
        ids = [s.id for s in self.sinks]
        if len(ids) != len(set(ids)):
            raise ValueError(f"sink ids must be unique within pipeline: {ids}")
        return self
```

- [ ] **Step 4: Run + Commit**

```bash
git add sluice/config.py tests/config/test_pipeline_config.py
git commit -m "feat(config): pipeline schema with discriminated stage/sink unions"
```

### Task 3.3: Global sluice.toml + loader

**Files:**
- Modify: `sluice/config.py`
- Create: `sluice/loader.py`, `tests/config/test_loader.py`, `configs/sluice.toml.example`, `configs/providers.toml.example`, `configs/pipelines/ai_news.toml.example`

- [ ] **Step 1: Write failing tests**

```python
# tests/config/test_loader.py
from pathlib import Path
import textwrap
from sluice.loader import load_all

def test_load_all(tmp_path):
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "pipelines").mkdir()
    (tmp_path / "configs" / "sluice.toml").write_text(textwrap.dedent("""
        [state]
        db_path = "./data/sluice.db"
        [runtime]
        timezone = "Asia/Shanghai"
        default_cron = "0 8 * * *"
        prefect_api_url = "http://localhost:4200/api"
        [fetcher]
        chain = ["trafilatura", "firecrawl"]
        min_chars = 500
        on_all_failed = "skip"
        [fetchers.trafilatura]
        type = "trafilatura"
        timeout = 10
        [fetchers.firecrawl]
        type = "firecrawl"
        base_url = "http://localhost:3002"
        api_key = "env:FC_KEY"
        timeout = 60
        [fetcher.cache]
        enabled = true
        ttl = "7d"
    """))
    (tmp_path / "configs" / "providers.toml").write_text(textwrap.dedent("""
        [[providers]]
        name = "glm"
        type = "openai_compatible"
        [[providers.base]]
        url = "https://x"
        weight = 1
        key = [{ value = "env:K", weight = 1 }]
        [[providers.models]]
        model_name = "glm-4-flash"
    """))
    (tmp_path / "configs" / "pipelines" / "p1.toml").write_text(textwrap.dedent("""
        id = "p1"
        window = "24h"
        [[sources]]
        type = "rss"
        url  = "https://x/feed"
        [[stages]]
        name = "d"
        type = "dedupe"
        [[sinks]]
        id = "local"
        type = "file_md"
        input = "context.markdown"
        path  = "./out/{run_date}.md"
    """))
    bundle = load_all(tmp_path / "configs")
    assert bundle.global_cfg.state.db_path == "./data/sluice.db"
    assert bundle.providers.providers[0].name == "glm"
    assert bundle.pipelines["p1"].id == "p1"
```

- [ ] **Step 2: Run — expect FAIL**

- [ ] **Step 3: Add `GlobalConfig` + loader**

```python
# Append to sluice/config.py
class StateConfig(BaseModel):
    db_path: str = "./data/sluice.db"

class RuntimeConfig(BaseModel):
    timezone: str = "Asia/Shanghai"
    default_cron: str = "0 8 * * *"
    prefect_api_url: str = "http://localhost:4200/api"

class FetcherImplConfig(BaseModel):
    type: str
    base_url: str | None = None
    api_key: str | None = None
    timeout: float = 30.0
    extra: dict[str, Any] = Field(default_factory=dict)
    model_config = {"extra": "allow"}

class GlobalFetcherConfig(BaseModel):
    chain: list[str] = ["trafilatura", "firecrawl", "jina_reader"]
    min_chars: int = 500
    on_all_failed: Literal["skip", "continue_empty"] = "skip"
    cache: CacheOverride = Field(default_factory=CacheOverride)

class GlobalConfig(BaseModel):
    state: StateConfig = Field(default_factory=StateConfig)
    runtime: RuntimeConfig = Field(default_factory=RuntimeConfig)
    fetcher: GlobalFetcherConfig = Field(default_factory=GlobalFetcherConfig)
    fetchers: dict[str, FetcherImplConfig] = Field(default_factory=dict)
```

```python
# sluice/loader.py
from dataclasses import dataclass
from pathlib import Path
import tomllib
from sluice.config import GlobalConfig, ProvidersConfig, PipelineConfig
from sluice.core.errors import ConfigError

@dataclass
class ConfigBundle:
    global_cfg: GlobalConfig
    providers: ProvidersConfig
    pipelines: dict[str, PipelineConfig]
    root: Path

def _load(path: Path) -> dict:
    if not path.exists():
        raise ConfigError(f"missing config: {path}")
    return tomllib.loads(path.read_text())

def load_all(root: Path) -> ConfigBundle:
    root = Path(root)
    global_cfg = GlobalConfig.model_validate(_load(root / "sluice.toml"))
    providers  = ProvidersConfig.model_validate(_load(root / "providers.toml"))
    pipes_dir = root / "pipelines"
    pipelines: dict[str, PipelineConfig] = {}
    if pipes_dir.exists():
        for f in sorted(pipes_dir.glob("*.toml")):
            cfg = PipelineConfig.model_validate(_load(f))
            if cfg.id in pipelines:
                raise ConfigError(f"duplicate pipeline id {cfg.id} in {f}")
            pipelines[cfg.id] = cfg
    return ConfigBundle(global_cfg, providers, pipelines, root)

def resolve_env(value: str) -> str:
    """Resolve 'env:NAME' to the env var; pass through if no prefix."""
    import os
    if value.startswith("env:"):
        v = os.environ.get(value[4:])
        if v is None:
            raise ConfigError(f"env var {value[4:]} not set")
        return v
    return value
```

- [ ] **Step 4: Add example configs**

Write `configs/sluice.toml.example`, `configs/providers.toml.example`, `configs/pipelines/ai_news.toml.example` mirroring the spec's TOML samples (full content per spec §Configuration).

- [ ] **Step 5: Run + Commit**

```bash
git add sluice/loader.py tests/config/test_loader.py configs/
git commit -m "feat(config): GlobalConfig + bundle loader + example TOMLs"
```

---

## Phase 4 — State Layer

### Task 4.1: DB module + initial migration

**Files:**
- Create: `sluice/state/__init__.py`, `sluice/state/db.py`, `sluice/state/migrations/0001_init.sql`, `tests/state/test_db.py`

- [ ] **Step 1: Write failing test**

```python
# tests/state/test_db.py
import pytest, aiosqlite
from sluice.state.db import open_db, current_version

@pytest.mark.asyncio
async def test_initial_migration_creates_tables(tmp_path):
    db_path = tmp_path / "x.db"
    async with open_db(db_path) as db:
        assert await current_version(db) == 1
        names = {row[0] async for row in await db.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"seen_items", "failed_items", "sink_emissions",
            "url_cache", "run_log"} <= names

@pytest.mark.asyncio
async def test_idempotent_open(tmp_path):
    db_path = tmp_path / "y.db"
    async with open_db(db_path): pass
    async with open_db(db_path) as db:
        assert await current_version(db) == 1
```

- [ ] **Step 2: Run — expect FAIL**

- [ ] **Step 3: Implement migration SQL** (full schema from spec)

```sql
-- sluice/state/migrations/0001_init.sql
CREATE TABLE seen_items (
  pipeline_id TEXT NOT NULL,
  item_key    TEXT NOT NULL,
  url         TEXT,
  title       TEXT,
  published_at TEXT,
  summary     TEXT,
  seen_at     TEXT NOT NULL,
  PRIMARY KEY (pipeline_id, item_key)
);
CREATE INDEX idx_seen_published ON seen_items(pipeline_id, published_at);

CREATE TABLE failed_items (
  pipeline_id TEXT NOT NULL,
  item_key    TEXT NOT NULL,
  url         TEXT,
  stage       TEXT NOT NULL,
  error_class TEXT NOT NULL,
  error_msg   TEXT NOT NULL,
  attempts    INTEGER NOT NULL DEFAULT 1,
  status      TEXT NOT NULL,
  item_json   TEXT NOT NULL,
  first_failed_at TEXT NOT NULL,
  last_failed_at  TEXT NOT NULL,
  PRIMARY KEY (pipeline_id, item_key)
);
CREATE INDEX idx_failed_status ON failed_items(pipeline_id, status);

CREATE TABLE sink_emissions (
  pipeline_id TEXT NOT NULL,
  run_key     TEXT NOT NULL,
  sink_id     TEXT NOT NULL,
  sink_type   TEXT NOT NULL,
  external_id TEXT,
  emitted_at  TEXT NOT NULL,
  PRIMARY KEY (pipeline_id, run_key, sink_id)
);

CREATE TABLE url_cache (
  url_hash    TEXT PRIMARY KEY,
  url         TEXT NOT NULL,
  fetcher     TEXT NOT NULL,
  markdown    TEXT NOT NULL,
  fetched_at  TEXT NOT NULL,
  expires_at  TEXT NOT NULL
);

CREATE TABLE run_log (
  pipeline_id TEXT NOT NULL,
  run_key     TEXT NOT NULL,
  started_at  TEXT NOT NULL,
  finished_at TEXT,
  status      TEXT NOT NULL,
  items_in    INTEGER,
  items_out   INTEGER,
  llm_calls   INTEGER,
  est_cost_usd REAL,
  error_msg   TEXT,
  PRIMARY KEY (pipeline_id, run_key)
);
```

- [ ] **Step 4: Implement db.py**

```python
# sluice/state/db.py
from contextlib import asynccontextmanager
from pathlib import Path
import aiosqlite

MIGRATIONS = Path(__file__).parent / "migrations"

async def current_version(db: aiosqlite.Connection) -> int:
    async with db.execute("PRAGMA user_version") as cur:
        row = await cur.fetchone()
    return row[0] if row else 0

async def _migrate(db: aiosqlite.Connection) -> None:
    files = sorted(MIGRATIONS.glob("*.sql"))
    have = await current_version(db)
    for f in files:
        v = int(f.stem.split("_", 1)[0])
        if v <= have:
            continue
        await db.executescript(f.read_text())
        await db.execute(f"PRAGMA user_version = {v}")
        await db.commit()

@asynccontextmanager
async def open_db(path):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    async with aiosqlite.connect(str(path)) as db:
        await db.execute("PRAGMA foreign_keys = ON")
        await db.execute("PRAGMA journal_mode = WAL")
        await _migrate(db)
        yield db
```

- [ ] **Step 5: Run + Commit**

```bash
git add sluice/state/__init__.py sluice/state/db.py sluice/state/migrations/0001_init.sql tests/state/__init__.py tests/state/test_db.py
git commit -m "feat(state): aiosqlite DB module with PRAGMA user_version migrations"
```

### Task 4.2: SeenStore

**Files:**
- Create: `sluice/state/seen.py`, `tests/state/test_seen.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/state/test_seen.py
import pytest
from datetime import datetime, timezone
from sluice.state.db import open_db
from sluice.state.seen import SeenStore
from sluice.core.item import Item

def make_item(key: str) -> Item:
    return Item(source_id="s", pipeline_id="p", guid=key,
                url=f"https://x/{key}", title=key,
                published_at=datetime(2026, 4, 28, tzinfo=timezone.utc),
                raw_summary=None, summary="sum-" + key)

@pytest.mark.asyncio
async def test_mark_and_check(tmp_path):
    async with open_db(tmp_path / "d.db") as db:
        s = SeenStore(db)
        assert await s.is_seen("p", "k1") is False
        await s.mark_seen_batch("p", [(make_item("k1"), "k1")])
        assert await s.is_seen("p", "k1") is True

@pytest.mark.asyncio
async def test_filter_unseen(tmp_path):
    async with open_db(tmp_path / "d.db") as db:
        s = SeenStore(db)
        await s.mark_seen_batch("p", [(make_item("a"), "a")])
        unseen = await s.filter_unseen("p", ["a", "b", "c"])
        assert unseen == ["b", "c"]
```

- [ ] **Step 2: Run — expect FAIL**

- [ ] **Step 3: Implement**

```python
# sluice/state/seen.py
from datetime import datetime, timezone
import aiosqlite
from sluice.core.item import Item

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _iso(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt else None

class SeenStore:
    def __init__(self, db: aiosqlite.Connection):
        self.db = db

    async def is_seen(self, pipeline_id: str, item_key: str) -> bool:
        async with self.db.execute(
            "SELECT 1 FROM seen_items WHERE pipeline_id=? AND item_key=?",
            (pipeline_id, item_key),
        ) as cur:
            return await cur.fetchone() is not None

    async def filter_unseen(self, pipeline_id: str,
                            keys: list[str]) -> list[str]:
        if not keys:
            return []
        placeholders = ",".join("?" * len(keys))
        async with self.db.execute(
            f"SELECT item_key FROM seen_items "
            f"WHERE pipeline_id=? AND item_key IN ({placeholders})",
            (pipeline_id, *keys),
        ) as cur:
            seen = {row[0] for row in await cur.fetchall()}
        return [k for k in keys if k not in seen]

    async def mark_seen_batch(self, pipeline_id: str,
                              items_with_keys: list[tuple[Item, str]]) -> None:
        now = _now_iso()
        rows = [
            (pipeline_id, key, it.url, it.title, _iso(it.published_at),
             it.summary, now)
            for it, key in items_with_keys
        ]
        await self.db.executemany(
            "INSERT OR REPLACE INTO seen_items "
            "(pipeline_id, item_key, url, title, published_at, summary, seen_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        await self.db.commit()
```

- [ ] **Step 4: Run + Commit**

```bash
git add sluice/state/seen.py tests/state/test_seen.py
git commit -m "feat(state): SeenStore with batch mark and unseen filter"
```

### Task 4.3: FailureStore (lifecycle)

**Files:**
- Create: `sluice/state/failures.py`, `tests/state/test_failures.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/state/test_failures.py
import pytest, json
from datetime import datetime, timezone
from sluice.state.db import open_db
from sluice.state.failures import FailureStore
from sluice.core.item import Item

def mk(k="k1"):
    return Item(source_id="s", pipeline_id="p", guid=k, url=f"https://x/{k}",
                title=k, published_at=datetime(2026, 4, 28, tzinfo=timezone.utc),
                raw_summary=None)

@pytest.mark.asyncio
async def test_record_and_increment(tmp_path):
    async with open_db(tmp_path / "d.db") as db:
        f = FailureStore(db)
        await f.record("p", "k1", mk(), stage="summarize",
                       error_class="LLMError", error_msg="boom",
                       max_retries=3)
        rows = await f.list("p")
        assert len(rows) == 1 and rows[0]["attempts"] == 1
        assert rows[0]["status"] == "failed"
        await f.record("p", "k1", mk(), stage="summarize",
                       error_class="LLMError", error_msg="boom2",
                       max_retries=3)
        rows = await f.list("p")
        assert rows[0]["attempts"] == 2

@pytest.mark.asyncio
async def test_dead_letter_after_max(tmp_path):
    async with open_db(tmp_path / "d.db") as db:
        f = FailureStore(db)
        for _ in range(3):
            await f.record("p", "k1", mk(), stage="x",
                           error_class="E", error_msg="m", max_retries=3)
        rows = await f.list("p")
        assert rows[0]["status"] == "dead_letter"

@pytest.mark.asyncio
async def test_resolve_and_requeue(tmp_path):
    async with open_db(tmp_path / "d.db") as db:
        f = FailureStore(db)
        await f.record("p", "k1", mk(), stage="x", error_class="E",
                       error_msg="m", max_retries=3)
        items = await f.requeue("p")
        assert len(items) == 1 and items[0].guid == "k1"
        await f.mark_resolved("p", "k1")
        rows = await f.list("p", status="resolved")
        assert len(rows) == 1
        # resolved no longer requeued
        assert await f.requeue("p") == []
```

- [ ] **Step 2: Run — expect FAIL**

- [ ] **Step 3: Implement**

```python
# sluice/state/failures.py
import json
from datetime import datetime, timezone
from dataclasses import asdict
import aiosqlite
from sluice.core.item import Item, Attachment

def _now() -> str: return datetime.now(timezone.utc).isoformat()

def _to_json(it: Item) -> str:
    d = asdict(it)
    d["published_at"] = it.published_at.isoformat() if it.published_at else None
    return json.dumps(d, ensure_ascii=False)

def _from_json(s: str) -> Item:
    d = json.loads(s)
    if d.get("published_at"):
        d["published_at"] = datetime.fromisoformat(d["published_at"])
    d["attachments"] = [Attachment(**a) for a in d.get("attachments", [])]
    return Item(**d)

class FailureStore:
    def __init__(self, db: aiosqlite.Connection):
        self.db = db

    async def record(self, pipeline_id: str, item_key: str, item: Item, *,
                     stage: str, error_class: str, error_msg: str,
                     max_retries: int) -> None:
        async with self.db.execute(
            "SELECT attempts FROM failed_items "
            "WHERE pipeline_id=? AND item_key=?",
            (pipeline_id, item_key),
        ) as cur:
            row = await cur.fetchone()
        now = _now()
        if row is None:
            await self.db.execute(
                "INSERT INTO failed_items (pipeline_id, item_key, url, stage, "
                "error_class, error_msg, attempts, status, item_json, "
                "first_failed_at, last_failed_at) "
                "VALUES (?, ?, ?, ?, ?, ?, 1, 'failed', ?, ?, ?)",
                (pipeline_id, item_key, item.url, stage, error_class,
                 error_msg, _to_json(item), now, now),
            )
        else:
            new_attempts = row[0] + 1
            new_status = "dead_letter" if new_attempts >= max_retries else "failed"
            await self.db.execute(
                "UPDATE failed_items SET attempts=?, status=?, "
                "stage=?, error_class=?, error_msg=?, last_failed_at=? "
                "WHERE pipeline_id=? AND item_key=?",
                (new_attempts, new_status, stage, error_class, error_msg,
                 now, pipeline_id, item_key),
            )
        await self.db.commit()

    async def requeue(self, pipeline_id: str) -> list[Item]:
        async with self.db.execute(
            "SELECT item_json FROM failed_items "
            "WHERE pipeline_id=? AND status='failed'",
            (pipeline_id,),
        ) as cur:
            rows = await cur.fetchall()
        return [_from_json(r[0]) for r in rows]

    async def mark_resolved(self, pipeline_id: str, item_key: str) -> None:
        await self.db.execute(
            "UPDATE failed_items SET status='resolved' "
            "WHERE pipeline_id=? AND item_key=?",
            (pipeline_id, item_key),
        )
        await self.db.commit()

    async def list(self, pipeline_id: str, status: str | None = None) -> list[dict]:
        sql = "SELECT * FROM failed_items WHERE pipeline_id=?"
        args = [pipeline_id]
        if status:
            sql += " AND status=?"
            args.append(status)
        # Set row_factory locally on the cursor instead of mutating the
        # connection (avoid global side-effect on subsequent queries).
        async with self.db.execute(sql, args) as cur:
            cur.row_factory = aiosqlite.Row
            return [dict(r) for r in await cur.fetchall()]

    async def excluded_keys(self, pipeline_id: str) -> set[str]:
        """Keys that dedupe should treat as already-seen (failed/dead_letter)."""
        async with self.db.execute(
            "SELECT item_key FROM failed_items WHERE pipeline_id=? "
            "AND status IN ('failed','dead_letter')",
            (pipeline_id,),
        ) as cur:
            return {r[0] for r in await cur.fetchall()}
```

- [ ] **Step 4: Run + Commit**

```bash
git add sluice/state/failures.py tests/state/test_failures.py
git commit -m "feat(state): FailureStore with status lifecycle and requeue"
```

### Task 4.4: EmissionStore + UrlCacheStore + RunLog

**Files:**
- Create: `sluice/state/emissions.py`, `sluice/state/cache.py`, `sluice/state/run_log.py`,
  `tests/state/test_emissions.py`, `tests/state/test_cache.py`, `tests/state/test_run_log.py`

- [ ] **Step 1: Write failing tests** (one per store)

```python
# tests/state/test_emissions.py
import pytest
from sluice.state.db import open_db
from sluice.state.emissions import EmissionStore

@pytest.mark.asyncio
async def test_lookup_insert(tmp_path):
    async with open_db(tmp_path / "d.db") as db:
        e = EmissionStore(db)
        assert await e.lookup("p", "p/2026-04-28", "notion_main") is None
        await e.insert("p", "p/2026-04-28", "notion_main", "notion", "page-abc")
        rec = await e.lookup("p", "p/2026-04-28", "notion_main")
        assert rec.external_id == "page-abc"
        assert rec.sink_type == "notion"

@pytest.mark.asyncio
async def test_two_sinks_same_type_no_collision(tmp_path):
    async with open_db(tmp_path / "d.db") as db:
        e = EmissionStore(db)
        await e.insert("p", "rk", "fm_a", "file_md", "/a.md")
        await e.insert("p", "rk", "fm_b", "file_md", "/b.md")
        a = await e.lookup("p", "rk", "fm_a")
        b = await e.lookup("p", "rk", "fm_b")
        assert a.external_id == "/a.md" and b.external_id == "/b.md"
```

```python
# tests/state/test_cache.py
import pytest
from datetime import datetime, timezone, timedelta
from sluice.state.db import open_db
from sluice.state.cache import UrlCacheStore

@pytest.mark.asyncio
async def test_get_set_expire(tmp_path):
    async with open_db(tmp_path / "d.db") as db:
        c = UrlCacheStore(db)
        await c.set("https://x/a", "trafilatura", "# md", ttl_seconds=3600)
        hit = await c.get("https://x/a")
        assert hit and hit.markdown == "# md"
        # Force expiry
        past = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        await db.execute("UPDATE url_cache SET expires_at=?", (past,))
        await db.commit()
        assert await c.get("https://x/a") is None
```

```python
# tests/state/test_run_log.py
import pytest
from sluice.state.db import open_db
from sluice.state.run_log import RunLog

@pytest.mark.asyncio
async def test_lifecycle(tmp_path):
    async with open_db(tmp_path / "d.db") as db:
        r = RunLog(db)
        await r.start("p", "p/2026-04-28")
        await r.update_stats("p", "p/2026-04-28",
                             items_in=10, items_out=8,
                             llm_calls=12, est_cost_usd=0.04)
        await r.finish("p", "p/2026-04-28", status="success")
        rows = await r.list("p")
        assert rows[0]["status"] == "success"
        assert rows[0]["items_out"] == 8
```

- [ ] **Step 2: Implement** (after tests fail)

```python
# sluice/state/emissions.py
from dataclasses import dataclass
from datetime import datetime, timezone
import aiosqlite

@dataclass
class Emission:
    sink_id: str
    sink_type: str
    external_id: str
    emitted_at: str

class EmissionStore:
    def __init__(self, db: aiosqlite.Connection): self.db = db

    async def lookup(self, pipeline_id, run_key, sink_id) -> Emission | None:
        async with self.db.execute(
            "SELECT sink_id, sink_type, external_id, emitted_at "
            "FROM sink_emissions "
            "WHERE pipeline_id=? AND run_key=? AND sink_id=?",
            (pipeline_id, run_key, sink_id),
        ) as cur:
            row = await cur.fetchone()
        return Emission(*row) if row else None

    async def insert(self, pipeline_id, run_key, sink_id, sink_type, external_id):
        await self.db.execute(
            "INSERT OR REPLACE INTO sink_emissions "
            "(pipeline_id, run_key, sink_id, sink_type, external_id, emitted_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (pipeline_id, run_key, sink_id, sink_type, external_id,
             datetime.now(timezone.utc).isoformat()),
        )
        await self.db.commit()
```

```python
# sluice/state/cache.py
from dataclasses import dataclass
import hashlib
from datetime import datetime, timezone, timedelta
import aiosqlite

def _hash(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()

@dataclass
class CachedExtraction:
    url: str
    fetcher: str
    markdown: str

class UrlCacheStore:
    def __init__(self, db: aiosqlite.Connection): self.db = db

    async def get(self, url: str) -> CachedExtraction | None:
        now = datetime.now(timezone.utc).isoformat()
        async with self.db.execute(
            "SELECT url, fetcher, markdown FROM url_cache "
            "WHERE url_hash=? AND expires_at > ?", (_hash(url), now),
        ) as cur:
            row = await cur.fetchone()
        return CachedExtraction(*row) if row else None

    async def set(self, url: str, fetcher: str, markdown: str, ttl_seconds: int):
        now = datetime.now(timezone.utc)
        await self.db.execute(
            "INSERT OR REPLACE INTO url_cache "
            "(url_hash, url, fetcher, markdown, fetched_at, expires_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (_hash(url), url, fetcher, markdown, now.isoformat(),
             (now + timedelta(seconds=ttl_seconds)).isoformat()),
        )
        await self.db.commit()
```

```python
# sluice/state/run_log.py
from datetime import datetime, timezone
import aiosqlite

def _now() -> str: return datetime.now(timezone.utc).isoformat()

class RunLog:
    def __init__(self, db: aiosqlite.Connection): self.db = db

    async def start(self, pipeline_id: str, run_key: str):
        await self.db.execute(
            "INSERT OR REPLACE INTO run_log "
            "(pipeline_id, run_key, started_at, status) "
            "VALUES (?, ?, ?, 'running')",
            (pipeline_id, run_key, _now()),
        )
        await self.db.commit()

    async def update_stats(self, pipeline_id, run_key, *, items_in,
                           items_out, llm_calls, est_cost_usd):
        await self.db.execute(
            "UPDATE run_log SET items_in=?, items_out=?, llm_calls=?, "
            "est_cost_usd=? WHERE pipeline_id=? AND run_key=?",
            (items_in, items_out, llm_calls, est_cost_usd,
             pipeline_id, run_key),
        )
        await self.db.commit()

    async def finish(self, pipeline_id, run_key, *, status: str,
                     error_msg: str | None = None):
        await self.db.execute(
            "UPDATE run_log SET finished_at=?, status=?, error_msg=? "
            "WHERE pipeline_id=? AND run_key=?",
            (_now(), status, error_msg, pipeline_id, run_key),
        )
        await self.db.commit()

    async def list(self, pipeline_id, limit=20):
        async with self.db.execute(
            "SELECT * FROM run_log WHERE pipeline_id=? "
            "ORDER BY started_at DESC LIMIT ?",
            (pipeline_id, limit),
        ) as cur:
            cur.row_factory = aiosqlite.Row
            return [dict(r) for r in await cur.fetchall()]
```

- [ ] **Step 3: Run all + Commit**

```bash
pytest tests/state -v
git add sluice/state/{emissions.py,cache.py,run_log.py} tests/state/test_emissions.py tests/state/test_cache.py tests/state/test_run_log.py
git commit -m "feat(state): EmissionStore, UrlCacheStore, RunLog"
```

---

## Phase 5 — LLM Provider Pool

### Task 5.1: Provider parsing + weighted selection

**Files:**
- Create: `sluice/llm/__init__.py`, `sluice/llm/provider.py`, `tests/llm/test_provider.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/llm/test_provider.py
import pytest, random
from sluice.config import Provider, BaseEndpoint, KeyConfig, ModelEntry
from sluice.llm.provider import ProviderRuntime, parse_model_spec

def make_provider():
    return Provider(
        name="glm", type="openai_compatible",
        base=[BaseEndpoint(url="https://a", weight=3,
                           key=[KeyConfig(value="env:K1", weight=2),
                                KeyConfig(value="env:K2", weight=1)]),
              BaseEndpoint(url="https://b", weight=1,
                           key=[KeyConfig(value="env:K3", weight=1)])],
        models=[ModelEntry(model_name="glm-4-flash",
                           input_price_per_1k=0.0001,
                           output_price_per_1k=0.0001)],
    )

def test_parse_model_spec():
    assert parse_model_spec("glm/glm-4-flash") == ("glm", "glm-4-flash")
    with pytest.raises(ValueError):
        parse_model_spec("noslash")

def test_weighted_distribution(monkeypatch):
    monkeypatch.setenv("K1", "v1"); monkeypatch.setenv("K2", "v2")
    monkeypatch.setenv("K3", "v3")
    rt = ProviderRuntime(make_provider())
    rng = random.Random(0)
    counts = {"https://a": 0, "https://b": 0}
    for _ in range(4000):
        ep = rt.pick_endpoint("glm-4-flash", rng=rng)
        counts[ep.base_url] += 1
    # 3:1 split → roughly 3000:1000
    ratio = counts["https://a"] / counts["https://b"]
    assert 2.5 <= ratio <= 3.5

def test_quota_cooldown(monkeypatch):
    monkeypatch.setenv("K1", "v1"); monkeypatch.setenv("K2", "v2")
    monkeypatch.setenv("K3", "v3")
    p = make_provider()
    p.base[0].key[0].quota_duration = 60
    rt = ProviderRuntime(p)
    rt.cool_down_key(p.base[0], p.base[0].key[0])
    rng = random.Random(0)
    for _ in range(50):
        ep = rt.pick_endpoint("glm-4-flash", rng=rng)
        assert ep.api_key != "v1"  # K1 is cooling down
```

- [ ] **Step 2: Run — expect FAIL**

- [ ] **Step 3: Implement**

```python
# sluice/llm/provider.py
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import os, random
from sluice.config import Provider, BaseEndpoint, KeyConfig, ModelEntry
from sluice.core.errors import ConfigError

def parse_model_spec(spec: str) -> tuple[str, str]:
    if "/" not in spec:
        raise ValueError(f"model spec must be 'provider/model': {spec}")
    p, m = spec.split("/", 1)
    return p, m

@dataclass
class Endpoint:
    provider_name: str
    provider_type: str
    base_url: str
    api_key: str
    model_entry: ModelEntry
    extra_headers: dict[str, str]
    _base_ref: BaseEndpoint
    _key_ref: KeyConfig

class ProviderRuntime:
    def __init__(self, provider: Provider):
        self.provider = provider
        self._cooldowns: dict[int, datetime] = {}
        self._models = {m.model_name: m for m in provider.models}

    def cool_down_key(self, base: BaseEndpoint, key: KeyConfig):
        if key.quota_duration:
            self._cooldowns[id(key)] = (
                datetime.now(timezone.utc)
                + timedelta(seconds=key.quota_duration)
            )

    def _key_active(self, key: KeyConfig) -> bool:
        deadline = self._cooldowns.get(id(key))
        if deadline is None:
            return True
        if datetime.now(timezone.utc) >= deadline:
            self._cooldowns.pop(id(key), None)
            return True
        return False

    def pick_endpoint(self, model: str, *, rng: random.Random) -> Endpoint:
        if model not in self._models:
            raise ConfigError(f"model {model} not in provider {self.provider.name}")
        bases = list(self.provider.base)
        rng.shuffle(bases)
        bw = [b.weight for b in bases]
        chosen_base = rng.choices(bases, weights=bw, k=1)[0]
        active_keys = [k for k in chosen_base.key if self._key_active(k)]
        if not active_keys:
            raise ConfigError(f"no active keys on {chosen_base.url}")
        kw = [k.weight for k in active_keys]
        chosen_key = rng.choices(active_keys, weights=kw, k=1)[0]
        api_key = self._resolve(chosen_key.value)
        return Endpoint(
            provider_name=self.provider.name,
            provider_type=self.provider.type,
            base_url=chosen_base.url,
            api_key=api_key,
            model_entry=self._models[model],
            extra_headers={**self.provider.extra_headers,
                           **chosen_base.extra_headers},
            _base_ref=chosen_base,
            _key_ref=chosen_key,
        )

    @staticmethod
    def _resolve(value: str) -> str:
        if value.startswith("env:"):
            v = os.environ.get(value[4:])
            if v is None:
                raise ConfigError(f"env var {value[4:]} not set")
            return v
        return value
```

- [ ] **Step 4: Commit**

```bash
git add sluice/llm/__init__.py sluice/llm/provider.py tests/llm/__init__.py tests/llm/test_provider.py
git commit -m "feat(llm): ProviderRuntime with weighted base/key selection and cooldown"
```

### Task 5.2: ProviderPool + LLMClient with fallback chain

**Files:**
- Create: `sluice/llm/pool.py`, `sluice/llm/client.py`, `sluice/llm/budget.py`,
  `tests/llm/test_pool.py`, `tests/llm/test_client.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/llm/test_pool.py
import pytest
from sluice.config import ProvidersConfig, Provider, BaseEndpoint, KeyConfig, ModelEntry
from sluice.llm.pool import ProviderPool

def make_cfg():
    return ProvidersConfig(providers=[
        Provider(name="glm", type="openai_compatible",
                 base=[BaseEndpoint(url="https://x", weight=1,
                                    key=[KeyConfig(value="env:K", weight=1)])],
                 models=[ModelEntry(model_name="m1")]),
    ])

def test_pool_resolves(monkeypatch):
    monkeypatch.setenv("K", "v")
    pool = ProviderPool(make_cfg())
    ep = pool.acquire("glm/m1")
    assert ep.base_url == "https://x" and ep.api_key == "v"
```

```python
# tests/llm/test_client.py
import pytest, httpx, respx
from sluice.config import ProvidersConfig, Provider, BaseEndpoint, KeyConfig, ModelEntry
from sluice.llm.pool import ProviderPool
from sluice.llm.client import LLMClient, StageLLMConfig
from sluice.core.errors import RateLimitError, AllProvidersExhausted

def make_pool(monkeypatch):
    monkeypatch.setenv("K", "v")
    return ProviderPool(ProvidersConfig(providers=[
        Provider(name="p1", type="openai_compatible",
                 base=[BaseEndpoint(url="https://main", weight=1,
                                    key=[KeyConfig(value="env:K", weight=1)])],
                 models=[ModelEntry(model_name="m1")]),
        Provider(name="p2", type="openai_compatible",
                 base=[BaseEndpoint(url="https://fb", weight=1,
                                    key=[KeyConfig(value="env:K", weight=1)])],
                 models=[ModelEntry(model_name="m2")]),
    ]))

@pytest.mark.asyncio
async def test_chat_success(monkeypatch):
    pool = make_pool(monkeypatch)
    cfg = StageLLMConfig(model="p1/m1")
    with respx.mock(base_url="https://main") as r:
        r.post("/chat/completions").mock(return_value=httpx.Response(200, json={
            "choices": [{"message": {"content": "hi"}}],
            "usage": {"prompt_tokens": 10, "completion_tokens": 2},
        }))
        client = LLMClient(pool, cfg)
        out = await client.chat([{"role": "user", "content": "hello"}])
        assert out == "hi"

@pytest.mark.asyncio
async def test_falls_through_to_fallback(monkeypatch):
    pool = make_pool(monkeypatch)
    cfg = StageLLMConfig(model="p1/m1", fallback_model="p2/m2")
    with respx.mock() as r:
        r.post("https://main/chat/completions").mock(return_value=httpx.Response(429))
        r.post("https://fb/chat/completions").mock(return_value=httpx.Response(200, json={
            "choices": [{"message": {"content": "fb"}}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1},
        }))
        out = await LLMClient(pool, cfg).chat([{"role": "user", "content": "x"}])
        assert out == "fb"

@pytest.mark.asyncio
async def test_all_exhausted_raises(monkeypatch):
    pool = make_pool(monkeypatch)
    cfg = StageLLMConfig(model="p1/m1", fallback_model="p2/m2")
    with respx.mock() as r:
        r.post("https://main/chat/completions").mock(return_value=httpx.Response(429))
        r.post("https://fb/chat/completions").mock(return_value=httpx.Response(429))
        with pytest.raises(AllProvidersExhausted):
            await LLMClient(pool, cfg).chat([{"role": "user", "content": "x"}])
```

- [ ] **Step 2: Run — expect FAIL**

- [ ] **Step 3: Implement** pool/budget/client (compact)

```python
# sluice/llm/pool.py
import random
from sluice.config import ProvidersConfig
from sluice.llm.provider import ProviderRuntime, Endpoint, parse_model_spec
from sluice.core.errors import ConfigError

class ProviderPool:
    def __init__(self, cfg: ProvidersConfig, *, seed: int | None = None):
        self.runtimes = {p.name: ProviderRuntime(p) for p in cfg.providers}
        self._rng = random.Random(seed)

    def acquire(self, model_spec: str) -> Endpoint:
        prov, model = parse_model_spec(model_spec)
        if prov not in self.runtimes:
            raise ConfigError(f"unknown provider: {prov}")
        return self.runtimes[prov].pick_endpoint(model, rng=self._rng)

    def cool_down(self, ep: Endpoint):
        self.runtimes[ep.provider_name].cool_down_key(ep._base_ref, ep._key_ref)
```

```python
# sluice/llm/budget.py
from dataclasses import dataclass, field
from sluice.llm.provider import Endpoint

@dataclass
class CallCost:
    prompt_tokens: int
    completion_tokens: int
    usd: float

@dataclass
class RunBudget:
    max_calls: int
    max_usd: float
    calls: int = 0
    spent_usd: float = 0.0

    def project(self, projected_calls: int, projected_usd: float) -> bool:
        usd_ok = (self.max_usd <= 0
                  or self.spent_usd + projected_usd <= self.max_usd)
        return (self.calls + projected_calls <= self.max_calls
                and usd_ok)

    def record(self, c: CallCost):
        self.calls += 1
        self.spent_usd += c.usd

def compute_cost(ep: Endpoint, prompt_tokens: int, completion_tokens: int) -> CallCost:
    m = ep.model_entry
    usd = (prompt_tokens / 1000.0) * m.input_price_per_1k \
        + (completion_tokens / 1000.0) * m.output_price_per_1k
    return CallCost(prompt_tokens, completion_tokens, usd)
```

```python
# sluice/llm/client.py
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
            except (RateLimitError, QuotaExhausted, httpx.NetworkError) as e:
                if isinstance(e, QuotaExhausted):
                    self.pool.cool_down(ep)
                continue
        raise AllProvidersExhausted(chain)

    async def _call(self, ep, messages: list[dict]) -> str:
        url = ep.base_url.rstrip("/") + "/chat/completions"
        headers = {"Authorization": f"Bearer {ep.api_key}",
                   "Content-Type": "application/json", **ep.extra_headers}
        payload = {"model": ep.model_entry.model_name, "messages": messages}
        async with httpx.AsyncClient(timeout=self.cfg.timeout) as c:
            r = await c.post(url, headers=headers, json=payload)
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
```

- [ ] **Step 4: Run + Commit**

```bash
pytest tests/llm -v
git add sluice/llm/{pool,client,budget}.py tests/llm/test_pool.py tests/llm/test_client.py
git commit -m "feat(llm): ProviderPool + LLMClient with retry/fallback chain"
```

---

## Phase 6 — Sources

### Task 6.1: RSS source

**Files:**
- Create: `sluice/sources/__init__.py`, `sluice/sources/base.py`, `sluice/sources/rss.py`,
  `tests/sources/test_rss.py`, `tests/sources/fixtures/example.xml`

- [ ] **Step 1: Add fixture**

```xml
<!-- tests/sources/fixtures/example.xml -->
<?xml version="1.0"?>
<rss version="2.0"><channel>
  <title>Ex</title><link>https://x.com</link><description>d</description>
  <item>
    <title>Article A</title>
    <link>https://x.com/a</link>
    <guid>guid-a</guid>
    <pubDate>Mon, 28 Apr 2026 06:00:00 +0000</pubDate>
    <description>Summary A</description>
  </item>
  <item>
    <title>Article B (no guid)</title>
    <link>https://x.com/b?utm_source=feed</link>
    <pubDate>Mon, 28 Apr 2026 04:00:00 +0000</pubDate>
    <description>Summary B</description>
  </item>
</channel></rss>
```

- [ ] **Step 2: Write failing test**

```python
# tests/sources/test_rss.py
import pytest, httpx, respx
from datetime import datetime, timezone, timedelta
from pathlib import Path
from sluice.sources.rss import RssSource

FIXTURE = (Path(__file__).parent / "fixtures" / "example.xml").read_text()

@pytest.mark.asyncio
async def test_rss_fetch_in_window():
    src = RssSource(url="https://feed.example/rss",
                    pipeline_id="p", source_id="s1", tag="ai")
    with respx.mock() as r:
        r.get("https://feed.example/rss").mock(
            return_value=httpx.Response(200, text=FIXTURE))
        end = datetime(2026, 4, 28, 12, tzinfo=timezone.utc)
        start = end - timedelta(hours=24)
        items = [it async for it in src.fetch(start, end)]
    assert len(items) == 2
    a = items[0]
    assert a.guid == "guid-a"
    assert a.url == "https://x.com/a"
    assert a.tags == ["ai"]
    # B's URL is canonicalised (utm_source stripped)
    b = items[1]
    assert b.url == "https://x.com/b"
    assert b.guid is None

@pytest.mark.asyncio
async def test_rss_drops_outside_window():
    src = RssSource(url="https://feed.example/rss",
                    pipeline_id="p", source_id="s1")
    with respx.mock() as r:
        r.get("https://feed.example/rss").mock(
            return_value=httpx.Response(200, text=FIXTURE))
        # Window in 2027 — both items out
        end = datetime(2027, 4, 28, tzinfo=timezone.utc)
        start = end - timedelta(hours=24)
        items = [it async for it in src.fetch(start, end)]
    assert items == []
```

- [ ] **Step 3: Implement**

```python
# sluice/sources/base.py
from sluice.registry import register_source
__all__ = ["register_source"]
```

```python
# sluice/sources/rss.py
from datetime import datetime, timezone, timedelta
from typing import AsyncIterator
import feedparser
import httpx
from sluice.core.item import Item
from sluice.url_canon import canonical_url
from sluice.sources.base import register_source

@register_source("rss")
class RssSource:
    def __init__(self, *, url: str, pipeline_id: str, source_id: str,
                 tag: str | None = None, name: str | None = None,
                 timeout: float = 30.0):
        self.url = url
        self.pipeline_id = pipeline_id
        self.source_id = source_id
        self.tags = [tag] if tag else []
        self.name = name or source_id
        self.timeout = timeout

    async def fetch(self, window_start: datetime, window_end: datetime
                    ) -> AsyncIterator[Item]:
        async with httpx.AsyncClient(timeout=self.timeout) as c:
            r = await c.get(self.url)
            r.raise_for_status()
            text = r.text
        feed = feedparser.parse(text)
        future_cap = window_end + timedelta(hours=1)
        for e in feed.entries:
            published = self._parse_date(e)
            # include None published items (window over-include)
            if published is not None:
                if published > future_cap:
                    continue            # drop future-dated junk
                if published < window_start or published > window_end:
                    continue
            yield Item(
                source_id=self.source_id,
                pipeline_id=self.pipeline_id,
                guid=getattr(e, "id", None) or getattr(e, "guid", None) or None,
                url=canonical_url(getattr(e, "link", "")),
                title=getattr(e, "title", "") or "",
                published_at=published,
                raw_summary=getattr(e, "summary", None)
                            or getattr(e, "description", None),
                tags=list(self.tags),
            )

    @staticmethod
    def _parse_date(e) -> datetime | None:
        for attr in ("published_parsed", "updated_parsed"):
            t = getattr(e, attr, None)
            if t:
                return datetime(*t[:6], tzinfo=timezone.utc)
        return None
```

- [ ] **Step 4: Run + Commit**

```bash
pytest tests/sources -v
git add sluice/sources/ tests/sources/
git commit -m "feat(sources): RSS source with window filter and URL canonicalization"
```

---

## Phase 7 — Fetchers

### Task 7.1: Trafilatura fetcher

**Files:**
- Create: `sluice/fetchers/__init__.py`, `sluice/fetchers/base.py`,
  `sluice/fetchers/trafilatura_fetcher.py`, `tests/fetchers/test_trafilatura.py`

- [ ] **Step 1: Write failing test**

```python
# tests/fetchers/test_trafilatura.py
import pytest, httpx, respx
from sluice.fetchers.trafilatura_fetcher import TrafilaturaFetcher

HTML = "<html><head><title>T</title></head><body>" \
       "<article><h1>Hello</h1><p>This is the body content of an article. " \
       "It has enough text to be a real extraction target. " * 5 + \
       "</p></article></body></html>"

@pytest.mark.asyncio
async def test_extract_html():
    f = TrafilaturaFetcher(timeout=10)
    with respx.mock() as r:
        r.get("https://x/a").mock(return_value=httpx.Response(200, text=HTML))
        md = await f.extract("https://x/a")
    assert "Hello" in md
    assert "body content" in md
```

- [ ] **Step 2: Implement**

```python
# sluice/fetchers/base.py
from sluice.registry import register_fetcher
__all__ = ["register_fetcher"]
```

```python
# sluice/fetchers/trafilatura_fetcher.py
import asyncio
import httpx, trafilatura
from sluice.fetchers.base import register_fetcher

@register_fetcher("trafilatura")
class TrafilaturaFetcher:
    name = "trafilatura"

    def __init__(self, *, timeout: float = 10.0):
        self.timeout = timeout

    async def extract(self, url: str) -> str:
        async with httpx.AsyncClient(timeout=self.timeout,
                                      follow_redirects=True) as c:
            r = await c.get(url)
            r.raise_for_status()
            html = r.text
        # trafilatura.extract is sync + CPU-bound (parsing). Offload so we
        # don't block the event loop when many items are fetched in parallel.
        md = await asyncio.to_thread(
            trafilatura.extract, html,
            output_format="markdown",
            include_comments=False,
            include_tables=True,
        )
        return md or ""
```

- [ ] **Step 3: Run + Commit**

```bash
pytest tests/fetchers/test_trafilatura.py -v
git add sluice/fetchers/__init__.py sluice/fetchers/base.py sluice/fetchers/trafilatura_fetcher.py tests/fetchers/__init__.py tests/fetchers/test_trafilatura.py
git commit -m "feat(fetchers): trafilatura local fetcher"
```

### Task 7.2: Firecrawl + Jina Reader fetchers

**Files:**
- Create: `sluice/fetchers/firecrawl.py`, `sluice/fetchers/jina_reader.py`,
  `tests/fetchers/test_firecrawl.py`, `tests/fetchers/test_jina.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/fetchers/test_firecrawl.py
import pytest, httpx, respx
from sluice.fetchers.firecrawl import FirecrawlFetcher

@pytest.mark.asyncio
async def test_firecrawl_returns_markdown():
    f = FirecrawlFetcher(base_url="http://fc:3002", api_key="K", timeout=30)
    with respx.mock() as r:
        r.post("http://fc:3002/v1/scrape").mock(
            return_value=httpx.Response(200, json={
                "success": True,
                "data": {"markdown": "# title\n\nbody"},
            }))
        md = await f.extract("https://x/a")
    assert "title" in md and "body" in md

@pytest.mark.asyncio
async def test_firecrawl_failure_raises():
    f = FirecrawlFetcher(base_url="http://fc:3002", api_key="K", timeout=30)
    with respx.mock() as r:
        r.post("http://fc:3002/v1/scrape").mock(
            return_value=httpx.Response(502, text="bad gateway"))
        with pytest.raises(Exception):
            await f.extract("https://x/a")
```

```python
# tests/fetchers/test_jina.py
import pytest, httpx, respx
from sluice.fetchers.jina_reader import JinaReaderFetcher

@pytest.mark.asyncio
async def test_jina_prefix_url():
    f = JinaReaderFetcher(base_url="https://r.jina.ai", timeout=30)
    with respx.mock() as r:
        r.get("https://r.jina.ai/https://x/a").mock(
            return_value=httpx.Response(200, text="# md content"))
        md = await f.extract("https://x/a")
    assert "md content" in md
```

- [ ] **Step 2: Implement**

```python
# sluice/fetchers/firecrawl.py
import httpx
from sluice.fetchers.base import register_fetcher

@register_fetcher("firecrawl")
class FirecrawlFetcher:
    name = "firecrawl"

    def __init__(self, *, base_url: str, api_key: str | None = None,
                 timeout: float = 60.0):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout

    async def extract(self, url: str) -> str:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        async with httpx.AsyncClient(timeout=self.timeout) as c:
            r = await c.post(f"{self.base_url}/v1/scrape",
                             headers=headers,
                             json={"url": url, "formats": ["markdown"]})
            r.raise_for_status()
            data = r.json()
        return data.get("data", {}).get("markdown", "") or ""
```

```python
# sluice/fetchers/jina_reader.py
import httpx
from sluice.fetchers.base import register_fetcher

@register_fetcher("jina_reader")
class JinaReaderFetcher:
    name = "jina_reader"

    def __init__(self, *, base_url: str = "https://r.jina.ai",
                 api_key: str | None = None, timeout: float = 30.0):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout

    async def extract(self, url: str) -> str:
        headers = {}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        async with httpx.AsyncClient(timeout=self.timeout) as c:
            r = await c.get(f"{self.base_url}/{url}", headers=headers)
            r.raise_for_status()
            return r.text
```

- [ ] **Step 3: Run + Commit**

```bash
pytest tests/fetchers -v
git add sluice/fetchers/firecrawl.py sluice/fetchers/jina_reader.py tests/fetchers/test_firecrawl.py tests/fetchers/test_jina.py
git commit -m "feat(fetchers): firecrawl and jina_reader fetchers"
```

### Task 7.3: FetcherChain with cache integration

**Files:**
- Create: `sluice/fetchers/chain.py`, `tests/fetchers/test_chain.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/fetchers/test_chain.py
import pytest
from sluice.fetchers.chain import FetcherChain
from sluice.core.errors import AllFetchersFailed
from sluice.state.db import open_db
from sluice.state.cache import UrlCacheStore

class FakeFetcher:
    def __init__(self, name, result=None, raise_exc=None):
        self.name = name
        self._result = result
        self._exc = raise_exc
        self.calls = 0

    async def extract(self, url):
        self.calls += 1
        if self._exc:
            raise self._exc
        return self._result

@pytest.mark.asyncio
async def test_first_succeeds(tmp_path):
    async with open_db(tmp_path / "d.db") as db:
        cache = UrlCacheStore(db)
        a = FakeFetcher("a", result="hello world this is long enough text " * 30)
        b = FakeFetcher("b", result="never used")
        chain = FetcherChain([a, b], min_chars=100, on_all_failed="skip",
                             cache=cache, ttl_seconds=3600)
        out = await chain.fetch("https://x/y")
    assert "hello" in out
    assert b.calls == 0  # short-circuited

@pytest.mark.asyncio
async def test_falls_through_on_short(tmp_path):
    async with open_db(tmp_path / "d.db") as db:
        cache = UrlCacheStore(db)
        a = FakeFetcher("a", result="too short")
        b = FakeFetcher("b", result="x" * 1000)
        chain = FetcherChain([a, b], min_chars=500, on_all_failed="skip",
                             cache=cache, ttl_seconds=3600)
        out = await chain.fetch("https://x/y")
    assert len(out) >= 1000
    assert b.calls == 1

@pytest.mark.asyncio
async def test_falls_through_on_exception(tmp_path):
    async with open_db(tmp_path / "d.db") as db:
        cache = UrlCacheStore(db)
        a = FakeFetcher("a", raise_exc=RuntimeError("nope"))
        b = FakeFetcher("b", result="x" * 1000)
        chain = FetcherChain([a, b], min_chars=500, on_all_failed="skip",
                             cache=cache, ttl_seconds=3600)
        out = await chain.fetch("https://x/y")
    assert len(out) >= 1000

@pytest.mark.asyncio
async def test_all_failed_skip(tmp_path):
    async with open_db(tmp_path / "d.db") as db:
        cache = UrlCacheStore(db)
        a = FakeFetcher("a", result="x")
        b = FakeFetcher("b", result="x")
        chain = FetcherChain([a, b], min_chars=500, on_all_failed="skip",
                             cache=cache, ttl_seconds=3600)
        with pytest.raises(AllFetchersFailed):
            await chain.fetch("https://x/y")

@pytest.mark.asyncio
async def test_all_failed_continue_empty(tmp_path):
    async with open_db(tmp_path / "d.db") as db:
        cache = UrlCacheStore(db)
        a = FakeFetcher("a", result="x")
        chain = FetcherChain([a], min_chars=500, on_all_failed="continue_empty",
                             cache=cache, ttl_seconds=3600)
        out = await chain.fetch("https://x/y")
    assert out == ""

@pytest.mark.asyncio
async def test_cache_hit_skips_fetchers(tmp_path):
    async with open_db(tmp_path / "d.db") as db:
        cache = UrlCacheStore(db)
        await cache.set("https://x/y", "trafilatura", "cached md " * 100,
                        ttl_seconds=3600)
        a = FakeFetcher("a", result="x")
        chain = FetcherChain([a], min_chars=100, on_all_failed="skip",
                             cache=cache, ttl_seconds=3600)
        out = await chain.fetch("https://x/y")
    assert "cached md" in out
    assert a.calls == 0  # cache short-circuited

@pytest.mark.asyncio
async def test_cache_disabled(tmp_path):
    a = FakeFetcher("a", result="x" * 1000)
    chain = FetcherChain([a], min_chars=100, on_all_failed="skip",
                         cache=None, ttl_seconds=0)
    out = await chain.fetch("https://x/y")
    assert len(out) == 1000 and a.calls == 1
```

- [ ] **Step 2: Implement**

```python
# sluice/fetchers/chain.py
from sluice.core.errors import AllFetchersFailed
from sluice.state.cache import UrlCacheStore

class FetcherChain:
    def __init__(self, fetchers: list, *, min_chars: int,
                 on_all_failed: str, cache: UrlCacheStore | None,
                 ttl_seconds: int):
        self.fetchers = fetchers
        self.min_chars = min_chars
        self.on_all_failed = on_all_failed
        self.cache = cache
        self.ttl_seconds = ttl_seconds

    async def fetch(self, url: str) -> str:
        if self.cache:
            hit = await self.cache.get(url)
            if hit:
                return hit.markdown
        attempts: list[str] = []
        for f in self.fetchers:
            attempts.append(f.name)
            try:
                md = await f.extract(url)
            except Exception:
                continue
            if md and len(md) >= self.min_chars:
                if self.cache:
                    await self.cache.set(url, f.name, md, self.ttl_seconds)
                return md
        if self.on_all_failed == "continue_empty":
            return ""
        raise AllFetchersFailed(url, attempts)
```

- [ ] **Step 3: Run + Commit**

```bash
pytest tests/fetchers/test_chain.py -v
git add sluice/fetchers/chain.py tests/fetchers/test_chain.py
git commit -m "feat(fetchers): FetcherChain with min_chars + cache + on_all_failed"
```

---

## Phase 8 — Processors

### Task 8.1: Dedupe processor

**Files:**
- Create: `sluice/processors/__init__.py`, `sluice/processors/base.py`,
  `sluice/processors/dedupe.py`, `tests/processors/test_dedupe.py`

- [ ] **Step 1: Write failing test**

```python
# tests/processors/test_dedupe.py
import pytest, hashlib
from datetime import datetime, timezone
from sluice.context import PipelineContext
from sluice.core.item import Item
from sluice.state.db import open_db
from sluice.state.seen import SeenStore
from sluice.state.failures import FailureStore
from sluice.processors.dedupe import DedupeProcessor
from sluice.core.item import compute_item_key as item_key

def mk(guid=None, url="https://x/a", title="t"):
    return Item(source_id="s", pipeline_id="p", guid=guid, url=url,
                title=title, published_at=datetime(2026, 4, 28,
                tzinfo=timezone.utc), raw_summary=None)
# (item_key cascade tests live in tests/core/test_item.py — see Task 1.1)

@pytest.mark.asyncio
async def test_dedupe_drops_seen_keeps_unseen(tmp_path):
    async with open_db(tmp_path / "d.db") as db:
        seen = SeenStore(db); fail = FailureStore(db)
        await seen.mark_seen_batch("p", [(mk(guid="seen1"), "seen1")])
        proc = DedupeProcessor(name="d", pipeline_id="p",
                               seen=seen, failures=fail)
        ctx = PipelineContext(pipeline_id="p", run_key="p/r",
                              run_date="2026-04-28",
                              items=[mk(guid="seen1"), mk(guid="new1")],
                              context={})
        ctx = await proc.process(ctx)
    assert [it.guid for it in ctx.items] == ["new1"]

@pytest.mark.asyncio
async def test_dedupe_excludes_failed(tmp_path):
    async with open_db(tmp_path / "d.db") as db:
        seen = SeenStore(db); fail = FailureStore(db)
        await fail.record("p", "fkey", mk(guid="fkey"), stage="x",
                          error_class="E", error_msg="m", max_retries=3)
        proc = DedupeProcessor(name="d", pipeline_id="p",
                               seen=seen, failures=fail)
        ctx = PipelineContext(pipeline_id="p", run_key="p/r",
                              run_date="2026-04-28",
                              items=[mk(guid="fkey"), mk(guid="ok")],
                              context={})
        ctx = await proc.process(ctx)
    assert [it.guid for it in ctx.items] == ["ok"]
```

- [ ] **Step 2: Implement**

```python
# sluice/processors/base.py
from sluice.registry import register_processor
__all__ = ["register_processor"]
```

```python
# sluice/processors/dedupe.py
from sluice.context import PipelineContext
from sluice.core.item import Item, compute_item_key as item_key  # re-export
from sluice.state.seen import SeenStore
from sluice.state.failures import FailureStore

class DedupeProcessor:
    name = "dedupe"

    def __init__(self, *, name: str, pipeline_id: str,
                 seen: SeenStore, failures: FailureStore):
        self.name = name
        self.pipeline_id = pipeline_id
        self.seen = seen
        self.failures = failures

    async def process(self, ctx: PipelineContext) -> PipelineContext:
        keys = [item_key(it) for it in ctx.items]
        seen_keys = set(keys) - set(await self.seen.filter_unseen(
            self.pipeline_id, keys))
        excluded = await self.failures.excluded_keys(self.pipeline_id)
        kept = [it for it, k in zip(ctx.items, keys)
                if k not in seen_keys and k not in excluded]
        ctx.items = kept
        return ctx
```

- [ ] **Step 3: Run + Commit**

```bash
pytest tests/processors/test_dedupe.py -v
git add sluice/processors/__init__.py sluice/processors/base.py sluice/processors/dedupe.py tests/processors/__init__.py tests/processors/test_dedupe.py
git commit -m "feat(processors): dedupe with seen+failure exclusion and key cascade"
```

### Task 8.2: Filter processor (rule engine)

**Files:**
- Create: `sluice/processors/filter.py`, `tests/processors/test_filter.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/processors/test_filter.py
import pytest, re
from datetime import datetime, timezone, timedelta
from sluice.context import PipelineContext
from sluice.core.item import Item
from sluice.config import FilterRule
from sluice.processors.filter import FilterProcessor

def mk(**kw):
    base = dict(source_id="s", pipeline_id="p", guid="g", url="https://x",
                title="t", published_at=datetime(2026, 4, 28,
                tzinfo=timezone.utc), raw_summary=None)
    base.update(kw); return Item(**base)

def ctx_with(items):
    return PipelineContext("p", "p/r", "2026-04-28", items, {})

@pytest.mark.asyncio
async def test_keep_if_all():
    items = [mk(summary="x"*100), mk(summary="short"),
             mk(summary="x"*200)]
    p = FilterProcessor(name="f", mode="keep_if_all",
                        rules=[FilterRule(field="summary",
                                          op="min_length", value=50)])
    out = await p.process(ctx_with(items))
    assert len(out.items) == 2

@pytest.mark.asyncio
async def test_drop_if_any():
    items = [mk(title="ad: free stuff"), mk(title="real article")]
    p = FilterProcessor(name="f", mode="drop_if_any",
                        rules=[FilterRule(field="title",
                                          op="matches",
                                          value=r"(?i)\bad\b")])
    out = await p.process(ctx_with(items))
    assert [it.title for it in out.items] == ["real article"]

@pytest.mark.asyncio
async def test_numeric_gte():
    a = mk(); a.extras["score"] = 8
    b = mk(); b.extras["score"] = 3
    p = FilterProcessor(name="f", mode="keep_if_all",
                        rules=[FilterRule(field="extras.score",
                                          op="gte", value=5)])
    out = await p.process(ctx_with([a, b]))
    assert len(out.items) == 1 and out.items[0].extras["score"] == 8

@pytest.mark.asyncio
async def test_newer_than():
    fresh = mk(published_at=datetime.now(timezone.utc))
    stale = mk(published_at=datetime.now(timezone.utc) - timedelta(days=10))
    p = FilterProcessor(name="f", mode="keep_if_all",
                        rules=[FilterRule(field="published_at",
                                          op="newer_than", value="48h")])
    out = await p.process(ctx_with([fresh, stale]))
    assert len(out.items) == 1
```

- [ ] **Step 2: Implement**

```python
# sluice/processors/filter.py
import re
from datetime import datetime, timezone
from sluice.context import PipelineContext
from sluice.core.item import Item
from sluice.config import FilterRule
from sluice.window import parse_duration

def _coerce_dt(v):
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    return None

def _eval(rule: FilterRule, item: Item) -> bool:
    v = item.get(rule.field)
    op, target = rule.op, rule.value
    if op == "exists":     return v is not None
    if op == "not_exists": return v is None
    if v is None:          return False
    if op == "gt":  return v >  target
    if op == "gte": return v >= target
    if op == "lt":  return v <  target
    if op == "lte": return v <= target
    if op == "eq":  return v == target
    if op == "matches":      return bool(re.search(target, str(v)))
    if op == "not_matches":  return not re.search(target, str(v))
    if op == "contains":     return target in v
    if op == "not_contains": return target not in v
    if op == "in":           return v in target
    if op == "not_in":       return v not in target
    if op == "min_length":   return len(v) >= target
    if op == "max_length":   return len(v) <= target
    if op in ("newer_than", "older_than"):
        dt = _coerce_dt(v);
        if dt is None: return False
        cutoff = datetime.now(timezone.utc) - parse_duration(target)
        return dt > cutoff if op == "newer_than" else dt < cutoff
    raise ValueError(f"unknown op {op}")

class FilterProcessor:
    name = "filter"

    def __init__(self, *, name: str, mode: str, rules: list[FilterRule]):
        self.name = name
        self.mode = mode
        self.rules = rules

    def _matches(self, item: Item) -> bool:
        results = [_eval(r, item) for r in self.rules]
        if self.mode == "keep_if_all":  return all(results)
        if self.mode == "keep_if_any":  return any(results)
        if self.mode == "drop_if_all":  return not all(results)
        if self.mode == "drop_if_any":  return not any(results)
        raise ValueError(self.mode)

    async def process(self, ctx: PipelineContext) -> PipelineContext:
        ctx.items = [it for it in ctx.items if self._matches(it)]
        return ctx
```

- [ ] **Step 3: Run + Commit**

```bash
pytest tests/processors/test_filter.py -v
git add sluice/processors/filter.py tests/processors/test_filter.py
git commit -m "feat(processors): filter rule engine with all 14 operators"
```

### Task 8.3: FieldFilter processor

**Files:**
- Create: `sluice/processors/field_filter.py`, `tests/processors/test_field_filter.py`

- [ ] **Step 1: Write failing test**

```python
# tests/processors/test_field_filter.py
import pytest
from datetime import datetime, timezone
from sluice.config import FieldOp
from sluice.context import PipelineContext
from sluice.core.item import Item
from sluice.processors.field_filter import FieldFilterProcessor

def mk(**kw):
    base = dict(source_id="s", pipeline_id="p", guid="g", url="https://x",
                title="t", published_at=datetime(2026,4,28,tzinfo=timezone.utc),
                raw_summary="hello"*200, fulltext="abcdef"*1000)
    base.update(kw); return Item(**base)

@pytest.mark.asyncio
async def test_truncate_dataclass_field():
    p = FieldFilterProcessor(name="ff",
                             ops=[FieldOp(op="truncate", field="fulltext", n=50)])
    ctx = PipelineContext("p","p/r","2026-04-28",[mk()],{})
    ctx = await p.process(ctx)
    assert len(ctx.items[0].fulltext) == 50

@pytest.mark.asyncio
async def test_drop_extras_field():
    it = mk(); it.extras["junk"] = "x"
    p = FieldFilterProcessor(name="ff",
                             ops=[FieldOp(op="drop", field="extras.junk")])
    ctx = await p.process(PipelineContext("p","p/r","2026-04-28",[it],{}))
    assert "junk" not in ctx.items[0].extras

@pytest.mark.asyncio
async def test_drop_dataclass_field_sets_none():
    p = FieldFilterProcessor(name="ff",
                             ops=[FieldOp(op="drop", field="raw_summary")])
    ctx = await p.process(PipelineContext("p","p/r","2026-04-28",[mk()],{}))
    assert ctx.items[0].raw_summary is None
```

- [ ] **Step 2: Implement**

```python
# sluice/processors/field_filter.py
from dataclasses import fields as dc_fields
from sluice.context import PipelineContext
from sluice.core.item import Item
from sluice.config import FieldOp

_DC_NAMES = {f.name for f in dc_fields(Item)}

def _truncate(it: Item, field: str, n: int):
    if "." in field:
        head, _, tail = field.partition(".")
        bucket = getattr(it, head, None)
        if isinstance(bucket, dict) and tail in bucket and isinstance(bucket[tail], str):
            bucket[tail] = bucket[tail][:n]
        return
    if field in _DC_NAMES:
        v = getattr(it, field)
        if isinstance(v, str):
            setattr(it, field, v[:n])

def _drop(it: Item, field: str):
    if "." in field:
        head, _, tail = field.partition(".")
        bucket = getattr(it, head, None)
        if isinstance(bucket, dict):
            bucket.pop(tail, None)
        return
    if field in _DC_NAMES:
        setattr(it, field, None)

class FieldFilterProcessor:
    name = "field_filter"
    def __init__(self, *, name: str, ops: list[FieldOp]):
        self.name = name; self.ops = ops

    async def process(self, ctx: PipelineContext) -> PipelineContext:
        for it in ctx.items:
            for op in self.ops:
                if op.op == "truncate":
                    _truncate(it, op.field, op.n or 0)
                elif op.op == "drop":
                    _drop(it, op.field)
        return ctx
```

- [ ] **Step 3: Run + Commit**

```bash
pytest tests/processors/test_field_filter.py -v
git add sluice/processors/field_filter.py tests/processors/test_field_filter.py
git commit -m "feat(processors): field_filter truncate/drop ops"
```

### Task 8.4: FetcherApply processor

**Files:**
- Create: `sluice/processors/fetcher_apply.py`, `tests/processors/test_fetcher_apply.py`

- [ ] **Step 1: Write failing test**

```python
# tests/processors/test_fetcher_apply.py
import pytest
from datetime import datetime, timezone
from sluice.context import PipelineContext
from sluice.core.item import Item
from sluice.processors.fetcher_apply import FetcherApplyProcessor
from sluice.core.errors import AllFetchersFailed

class StubChain:
    def __init__(self, by_url): self.by_url = by_url
    async def fetch(self, url):
        v = self.by_url.get(url)
        if isinstance(v, Exception): raise v
        return v

def mk(url, **kw):
    return Item(source_id="s", pipeline_id="p", guid=None, url=url,
                title="t", published_at=datetime(2026,4,28,tzinfo=timezone.utc),
                raw_summary=kw.get("raw_summary"))

class FailRecorder:
    def __init__(self): self.records = []
    async def record(self, pid, key, item, *, stage, error_class,
                     error_msg, max_retries):
        self.records.append((key, error_class))

@pytest.mark.asyncio
async def test_writes_fulltext():
    chain = StubChain({"https://a": "x" * 1000})
    fr = FailRecorder()
    p = FetcherApplyProcessor(name="fa", chain=chain, write_field="fulltext",
                              skip_if_field_longer_than=None,
                              failures=fr, max_retries=3)
    ctx = PipelineContext("p","p/r","2026-04-28",[mk("https://a")],{})
    ctx = await p.process(ctx)
    assert ctx.items[0].fulltext.startswith("x")

@pytest.mark.asyncio
async def test_skips_if_raw_summary_long_enough():
    chain = StubChain({"https://a": "fetched"})
    p = FetcherApplyProcessor(name="fa", chain=chain, write_field="fulltext",
                              skip_if_field_longer_than=10,
                              failures=FailRecorder(), max_retries=3)
    item = mk("https://a", raw_summary="long enough raw summary here")
    ctx = PipelineContext("p","p/r","2026-04-28",[item],{})
    ctx = await p.process(ctx)
    # raw_summary copied to fulltext, no fetch
    assert ctx.items[0].fulltext == "long enough raw summary here"

@pytest.mark.asyncio
async def test_records_failure_drops_item():
    chain = StubChain({"https://a": AllFetchersFailed("https://a", ["x"])})
    fr = FailRecorder()
    p = FetcherApplyProcessor(name="fa", chain=chain, write_field="fulltext",
                              skip_if_field_longer_than=None,
                              failures=fr, max_retries=3)
    ctx = PipelineContext("p","p/r","2026-04-28",[mk("https://a")],{})
    ctx = await p.process(ctx)
    assert ctx.items == []
    assert fr.records and fr.records[0][1] == "AllFetchersFailed"
```

- [ ] **Step 2: Implement**

```python
# sluice/processors/fetcher_apply.py
from sluice.context import PipelineContext
from sluice.processors.dedupe import item_key

class FetcherApplyProcessor:
    name = "fetcher_apply"

    def __init__(self, *, name, chain, write_field, skip_if_field_longer_than,
                 failures, max_retries):
        self.name = name
        self.chain = chain
        self.write_field = write_field
        self.skip_if_field_longer_than = skip_if_field_longer_than
        self.failures = failures
        self.max_retries = max_retries

    async def process(self, ctx: PipelineContext) -> PipelineContext:
        survivors = []
        for it in ctx.items:
            existing = it.raw_summary or ""
            if (self.skip_if_field_longer_than
                    and len(existing) >= self.skip_if_field_longer_than):
                setattr(it, self.write_field, existing)
                survivors.append(it)
                continue
            try:
                md = await self.chain.fetch(it.url)
            except Exception as e:
                await self.failures.record(
                    it.pipeline_id, item_key(it), it,
                    stage=self.name, error_class=type(e).__name__,
                    error_msg=str(e), max_retries=self.max_retries)
                continue
            setattr(it, self.write_field, md)
            survivors.append(it)
        ctx.items = survivors
        return ctx
```

- [ ] **Step 3: Run + Commit**

```bash
pytest tests/processors/test_fetcher_apply.py -v
git add sluice/processors/fetcher_apply.py tests/processors/test_fetcher_apply.py
git commit -m "feat(processors): fetcher_apply integrating chain + failure recording"
```

### Task 8.5: LLMStage processor (per_item + aggregate)

**Files:**
- Create: `sluice/processors/llm_stage.py`, `tests/processors/test_llm_stage.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/processors/test_llm_stage.py
import pytest, json
from datetime import datetime, timezone
from sluice.context import PipelineContext
from sluice.core.item import Item
from sluice.processors.llm_stage import LLMStageProcessor

class FakeLLM:
    def __init__(self, replies): self.replies = list(replies); self.calls = 0
    async def chat(self, messages):
        self.calls += 1
        return self.replies.pop(0)

def mk(i):
    return Item(source_id="s", pipeline_id="p", guid=f"g{i}",
                url=f"https://x/{i}", title=f"t{i}",
                published_at=datetime(2026,4,28,tzinfo=timezone.utc),
                raw_summary=None, fulltext=f"body {i}")

@pytest.mark.asyncio
async def test_per_item_writes_summary(tmp_path):
    prompt = tmp_path / "p.md"
    prompt.write_text("Summarize: {{ item.fulltext }}")
    llm = FakeLLM(replies=["sum0", "sum1"])
    p = LLMStageProcessor(
        name="summarize", mode="per_item",
        input_field="fulltext", output_field="summary",
        prompt_file=str(prompt), llm_factory=lambda: llm,
        output_parser="text", max_input_chars=1000,
        truncate_strategy="head_tail", workers=2,
    )
    ctx = PipelineContext("p","p/r","2026-04-28",[mk(0), mk(1)],{})
    ctx = await p.process(ctx)
    assert [it.summary for it in ctx.items] == ["sum0", "sum1"]
    assert llm.calls == 2

@pytest.mark.asyncio
async def test_aggregate_writes_context(tmp_path):
    prompt = tmp_path / "p.md"
    prompt.write_text("Items: {{ items|length }}")
    llm = FakeLLM(replies=["BRIEF"])
    p = LLMStageProcessor(
        name="daily", mode="aggregate",
        input_field="summary", output_target="context.daily_brief",
        prompt_file=str(prompt), llm_factory=lambda: llm,
        output_parser="text", max_input_chars=10000,
        truncate_strategy="head_tail", workers=1,
    )
    ctx = PipelineContext("p","p/r","2026-04-28",[mk(0), mk(1)],{})
    ctx = await p.process(ctx)
    assert ctx.context["daily_brief"] == "BRIEF"

@pytest.mark.asyncio
async def test_json_parser_writes_dict(tmp_path):
    prompt = tmp_path / "p.md"; prompt.write_text("rate: {{ item.summary }}")
    llm = FakeLLM(replies=['{"score": 7}'])
    p = LLMStageProcessor(
        name="rate", mode="per_item",
        input_field="summary", output_field="extras.relevance",
        prompt_file=str(prompt), llm_factory=lambda: llm,
        output_parser="json", max_input_chars=1000,
        truncate_strategy="head_tail", workers=1,
    )
    it = mk(0); it.summary = "abc"
    ctx = await p.process(PipelineContext("p","p/r","2026-04-28",[it],{}))
    assert ctx.items[0].extras["relevance"] == {"score": 7}

@pytest.mark.asyncio
async def test_truncate_head_tail(tmp_path):
    prompt = tmp_path / "p.md"
    prompt.write_text("{{ item.fulltext }}")
    seen = []
    class CaptureLLM:
        async def chat(self, messages):
            seen.append(messages[-1]["content"]); return "ok"
    p = LLMStageProcessor(
        name="x", mode="per_item",
        input_field="fulltext", output_field="summary",
        prompt_file=str(prompt), llm_factory=lambda: CaptureLLM(),
        output_parser="text", max_input_chars=20,
        truncate_strategy="head_tail", workers=1,
    )
    it = mk(0); it.fulltext = "A" * 50 + "B" * 50
    await p.process(PipelineContext("p","p/r","2026-04-28",[it],{}))
    # rendered length capped roughly at 20
    assert len(seen[0]) <= 22  # head/tail boundary tolerance

@pytest.mark.asyncio
async def test_per_item_llm_failure_records_and_drops(tmp_path):
    prompt = tmp_path / "p.md"
    prompt.write_text("S: {{ item.fulltext }}")
    class FlakyLLM:
        def __init__(self): self.n = 0
        async def chat(self, messages):
            self.n += 1
            if self.n == 2: raise RuntimeError("transient")
            return f"ok-{self.n}"
    llm = FlakyLLM()
    class FakeFailures:
        def __init__(self): self.records = []
        async def record(self, pid, key, item, *, stage, error_class,
                         error_msg, max_retries):
            self.records.append((key, error_class))
    fr = FakeFailures()
    p = LLMStageProcessor(
        name="summarize", mode="per_item",
        input_field="fulltext", output_field="summary",
        prompt_file=str(prompt), llm_factory=lambda: llm,
        output_parser="text", max_input_chars=1000,
        truncate_strategy="head_tail", workers=1,
        failures=fr, pipeline_id="p",
    )
    ctx = PipelineContext("p","p/r","2026-04-28",[mk(0), mk(1), mk(2)],{})
    ctx = await p.process(ctx)
    # Item 1 dropped (raised); items 0 and 2 survived
    assert len(ctx.items) == 2
    assert len(fr.records) == 1 and fr.records[0][1] == "RuntimeError"

@pytest.mark.asyncio
async def test_budget_preflight_blocks_on_call_count(tmp_path):
    from sluice.llm.budget import RunBudget
    from sluice.core.errors import BudgetExceeded
    prompt = tmp_path / "p.md"; prompt.write_text("x")
    class CountLLM:
        def __init__(self): self.calls = 0
        async def chat(self, m):
            self.calls += 1; return "ok"
    llm = CountLLM()
    budget = RunBudget(max_calls=1, max_usd=10.0)
    p = LLMStageProcessor(
        name="s", mode="per_item",
        input_field="fulltext", output_field="summary",
        prompt_file=str(prompt), llm_factory=lambda: llm,
        output_parser="text", max_input_chars=1000,
        truncate_strategy="head_tail", workers=1,
        budget=budget,
    )
    # 3 items, budget allows only 1 → preflight raises before any call
    with pytest.raises(BudgetExceeded):
        await p.process(PipelineContext("p","p/r","r",[mk(0), mk(1), mk(2)],{}))
    assert llm.calls == 0

@pytest.mark.asyncio
async def test_budget_preflight_blocks_on_usd(tmp_path):
    """max_calls is generous; max_usd is the binding constraint."""
    from sluice.llm.budget import RunBudget
    from sluice.core.errors import BudgetExceeded
    prompt = tmp_path / "p.md"
    prompt.write_text("{{ item.fulltext }}")
    class CountLLM:
        def __init__(self): self.calls = 0
        async def chat(self, m):
            self.calls += 1; return "ok"
    llm = CountLLM()
    # Per-item: each rendered prompt ~ 1000 chars ≈ 333 tokens at 3 char/tok
    # plus ESTIMATED_OUTPUT_TOKENS=1024. At $0.01/1k each, ≈ $0.0136/call.
    # 3 items → ≈ $0.04. Budget cap = $0.01 should block.
    budget = RunBudget(max_calls=999, max_usd=0.01)
    items = [mk(i) for i in range(3)]
    for it in items:
        it.fulltext = "X" * 1000
    p = LLMStageProcessor(
        name="s", mode="per_item",
        input_field="fulltext", output_field="summary",
        prompt_file=str(prompt), llm_factory=lambda: llm,
        output_parser="text", max_input_chars=10000,
        truncate_strategy="head_tail", workers=1,
        budget=budget, model_spec="n/m",
        price_lookup=lambda spec: (0.01, 0.01),  # $0.01/1k both ways
    )
    with pytest.raises(BudgetExceeded):
        await p.process(PipelineContext("p","p/r","r", items, {}))
    assert llm.calls == 0
```

- [ ] **Step 2: Implement**

```python
# sluice/processors/llm_stage.py
import asyncio, json
from pathlib import Path
from typing import Callable
from jinja2 import Template
from sluice.context import PipelineContext
from sluice.core.errors import StageError, BudgetExceeded
from sluice.core.item import compute_item_key

def _truncate(text: str, n: int, strategy: str) -> str:
    if len(text) <= n: return text
    if strategy == "error":
        raise StageError(f"input exceeds {n} chars")
    if strategy == "head":
        return text[:n]
    half = n // 2
    return text[:half] + "\n…\n" + text[-half:]

def _set_path(target, path: str, value):
    head, _, rest = path.partition(".")
    if not rest:
        if isinstance(target, dict): target[head] = value
        else: setattr(target, head, value)
        return
    if head == "extras":
        target.extras[rest] = value
    else:
        setattr(target, head, value)

class LLMStageProcessor:
    name = "llm_stage"

    def __init__(self, *, name, mode, input_field, prompt_file,
                 llm_factory: Callable, output_field=None, output_target=None,
                 output_parser="text", on_parse_error="fail",
                 on_parse_error_default=None, max_input_chars=20000,
                 truncate_strategy="head_tail", workers=4,
                 failures=None, budget=None, pipeline_id: str | None = None,
                 max_retries: int = 3, model_spec: str = "",
                 price_lookup=None):
        """
        failures: FailureStore — per-item LLM errors are recorded here and
            the item is dropped from the run (rest of items continue).
        budget: RunBudget — preflight check before each LLM call. If the
            *next* call would push past max_calls/max_usd, raises
            BudgetExceeded so the runner fails the whole run instead of
            silently skipping a downstream-required stage.
        pipeline_id: needed for failure recording.
        """
        self.name = name; self.mode = mode
        self.input_field = input_field
        self.output_field = output_field
        self.output_target = output_target
        self.template = Template(Path(prompt_file).read_text())
        self.llm_factory = llm_factory
        self.output_parser = output_parser
        self.on_parse_error = on_parse_error
        self.on_parse_error_default = on_parse_error_default or {}
        self.max_input_chars = max_input_chars
        self.truncate_strategy = truncate_strategy
        self.workers = workers
        self.failures = failures
        self.budget = budget
        self.pipeline_id = pipeline_id
        self.max_retries = max_retries
        self.model_spec = model_spec
        # price_lookup(model_spec) -> (input_per_1k, output_per_1k); used
        # for budget projection. Production builder always injects this
        # from ProviderPool after runner validation; the zero-price default
        # exists only for isolated unit tests where USD is not the assertion.
        self._price_lookup = price_lookup or (lambda _: (0.0, 0.0))

    # Char→token heuristic: GPT/Claude tokenizers average ~4 chars/token
    # for English, ~2.5 for Chinese. We use 3.0 as a midpoint that
    # over-projects mildly (safer for caps).
    _CHARS_PER_TOKEN = 3.0
    # Default output budget per call when we cannot inspect (LLM stage).
    # 1024 tokens covers most summary/analysis prompts.
    _ESTIMATED_OUTPUT_TOKENS = 1024

    def _project_usd(self, prompt_chars: int, model_spec: str) -> float:
        """Estimate USD cost for one call given the prompt char count.
        Looks up the model price in the pool's provider runtime."""
        if self.budget is None:
            return 0.0
        # The llm_factory closes over the pool; reach back through it. To
        # keep this decoupled we inject a 'price_lookup' callable below.
        prompt_tokens = prompt_chars / self._CHARS_PER_TOKEN
        in_price, out_price = self._price_lookup(model_spec)
        return ((prompt_tokens / 1000.0) * in_price
                + (self._ESTIMATED_OUTPUT_TOKENS / 1000.0) * out_price)

    def _preflight(self, projected_calls: int, projected_usd: float) -> None:
        if self.budget is None:
            return
        if not self.budget.project(projected_calls=projected_calls,
                                    projected_usd=projected_usd):
            raise BudgetExceeded(
                f"stage {self.name}: would exceed run budget "
                f"(calls={self.budget.calls}+{projected_calls}/"
                f"{self.budget.max_calls}, "
                f"usd=${self.budget.spent_usd:.4f}+${projected_usd:.4f}/"
                f"${self.budget.max_usd})"
            )

    def _render_one(self, item):
        raw = item.get(self.input_field, default="") or ""
        truncated = _truncate(str(raw), self.max_input_chars, self.truncate_strategy)
        item_view = type("It", (), {**item.__dict__,
                                    self.input_field: truncated})()
        return self.template.render(item=item_view)

    def _parse(self, text):
        if self.output_parser == "text":
            return text
        try:
            return json.loads(text)
        except json.JSONDecodeError as e:
            if self.on_parse_error == "fail":
                raise StageError(f"json parse failed: {e}")
            if self.on_parse_error == "default":
                return self.on_parse_error_default
            return None  # skip

    async def _run_per_item(self, ctx: PipelineContext):
        # Preflight: project total chars across all items (post-truncation),
        # convert to tokens, multiply by per-1k price, sum.
        rendered = [self._render_one(it) for it in ctx.items]
        total_chars = sum(len(s) for s in rendered)
        projected_usd = self._project_usd(total_chars, self.model_spec)
        self._preflight(projected_calls=len(rendered),
                         projected_usd=projected_usd)
        sem = asyncio.Semaphore(self.workers)

        async def one(it, content):
            async with sem:
                llm = self.llm_factory()
                msgs = [{"role": "user", "content": content}]
                try:
                    out = await llm.chat(msgs)
                    parsed = self._parse(out)
                except Exception as e:
                    # Per-item LLM error: record + drop, do NOT crash the
                    # gather. The rest of the items continue.
                    if self.failures is not None and self.pipeline_id:
                        await self.failures.record(
                            self.pipeline_id, compute_item_key(it), it,
                            stage=self.name,
                            error_class=type(e).__name__,
                            error_msg=str(e),
                            max_retries=self.max_retries,
                        )
                    return None
                if parsed is None and self.on_parse_error == "skip":
                    return None
                _set_path(it, self.output_field, parsed)
                return it

        results = await asyncio.gather(*[one(it, c)
                                          for it, c in zip(ctx.items, rendered)])
        ctx.items = [it for it in results if it is not None]
        return ctx

    async def _run_aggregate(self, ctx: PipelineContext):
        rendered = self.template.render(items=ctx.items, context=ctx.context)
        rendered = _truncate(rendered, self.max_input_chars, self.truncate_strategy)
        self._preflight(projected_calls=1,
                         projected_usd=self._project_usd(len(rendered), self.model_spec))
        llm = self.llm_factory()
        # Aggregate failures DO crash the stage — losing the daily brief
        # would silently corrupt the render output. The runner catches at
        # pipeline level and marks run_log status=failed (no commit).
        out = await llm.chat([{"role": "user", "content": rendered}])
        parsed = self._parse(out)
        # output_target = "context.<key>"
        _, _, key = self.output_target.partition(".")
        ctx.context[key] = parsed
        return ctx

    async def process(self, ctx: PipelineContext) -> PipelineContext:
        if self.mode == "per_item":
            return await self._run_per_item(ctx)
        return await self._run_aggregate(ctx)
```

- [ ] **Step 3: Run + Commit**

```bash
pytest tests/processors/test_llm_stage.py -v
git add sluice/processors/llm_stage.py tests/processors/test_llm_stage.py
git commit -m "feat(processors): llm_stage with per_item/aggregate modes + json parsing"
```

### Task 8.6: Render processor (Jinja2)

**Files:**
- Create: `sluice/processors/render.py`, `tests/processors/test_render.py`

- [ ] **Step 1: Write failing test**

```python
# tests/processors/test_render.py
import pytest
from datetime import datetime, timezone
from sluice.context import PipelineContext, RunStats
from sluice.core.item import Item
from sluice.processors.render import RenderProcessor

def mk(s):
    return Item(source_id="s", pipeline_id="p", guid=s, url=f"https://x/{s}",
                title=f"T-{s}", published_at=datetime(2026,4,28,tzinfo=timezone.utc),
                raw_summary=None, summary=f"sum-{s}")

@pytest.mark.asyncio
async def test_render_full_context(tmp_path):
    tpl = tmp_path / "t.j2"
    tpl.write_text(
        "# {{ pipeline_id }} {{ run_date }}\n\n"
        "{{ context.daily_brief }}\n\n"
        "{% for it in items %}- {{ it.summary }}\n{% endfor %}\n"
        "stats: {{ stats.items_out }}/{{ stats.llm_calls }}"
    )
    items = [mk("a"), mk("b")]
    ctx = PipelineContext("p1", "p1/2026-04-28", "2026-04-28", items,
                          {"daily_brief": "BRIEF"},
                          stats=RunStats(items_in=2, items_out=2,
                                         llm_calls=3, est_cost_usd=0.01))
    p = RenderProcessor(name="r", template=str(tpl),
                        output_field="context.markdown")
    ctx = await p.process(ctx)
    md = ctx.context["markdown"]
    assert "# p1 2026-04-28" in md
    assert "BRIEF" in md
    assert "- sum-a" in md
    assert "stats: 2/3" in md
```

- [ ] **Step 2: Implement**

```python
# sluice/processors/render.py
from pathlib import Path
from jinja2 import Template
from sluice.context import PipelineContext

class RenderProcessor:
    name = "render"

    def __init__(self, *, name: str, template: str, output_field: str):
        self.name = name
        self.template = Template(Path(template).read_text())
        self.output_field = output_field

    async def process(self, ctx: PipelineContext) -> PipelineContext:
        rendered = self.template.render(
            items=ctx.items,
            context=ctx.context,
            pipeline_id=ctx.pipeline_id,
            run_key=ctx.run_key,
            run_date=ctx.run_date,
            stats=ctx.stats,
        )
        head, _, key = self.output_field.partition(".")
        if head != "context":
            raise ValueError("render output_field must be 'context.<key>'")
        ctx.context[key] = rendered
        return ctx
```

- [ ] **Step 3: Run + Commit**

```bash
pytest tests/processors/test_render.py -v
git add sluice/processors/render.py tests/processors/test_render.py
git commit -m "feat(processors): render with fixed Jinja2 context dict"
```

---

## Phase 9 — Sinks

### Task 9.1: Sink ABC with idempotency dispatch

**Files:**
- Create: `sluice/sinks/__init__.py`, `sluice/sinks/base.py`, `tests/sinks/test_base.py`

- [ ] **Step 1: Write failing test**

```python
# tests/sinks/test_base.py
import pytest
from sluice.context import PipelineContext
from sluice.sinks.base import Sink
from sluice.core.protocols import SinkResult
from sluice.state.db import open_db
from sluice.state.emissions import EmissionStore

class StubSink(Sink):
    type = "stub"
    def __init__(self, sid, mode="upsert"):
        super().__init__(id=sid, mode=mode, emit_on_empty=False)
        self.created = []; self.updated = []
    async def create(self, ctx) -> str:
        self.created.append(ctx.run_key); return f"ext-{len(self.created)}"
    async def update(self, ext_id: str, ctx) -> str:
        self.updated.append((ext_id, ctx.run_key)); return ext_id

def make_ctx(items=None):
    return PipelineContext("p", "p/2026-04-28", "2026-04-28",
                           items or [object()], {"markdown": "x"})

@pytest.mark.asyncio
async def test_first_run_creates(tmp_path):
    async with open_db(tmp_path / "d.db") as db:
        e = EmissionStore(db)
        s = StubSink("sid1")
        res = await s.emit(make_ctx(), emissions=e)
        assert res.created and res.external_id == "ext-1"

@pytest.mark.asyncio
async def test_second_run_upserts(tmp_path):
    async with open_db(tmp_path / "d.db") as db:
        e = EmissionStore(db)
        s = StubSink("sid1", mode="upsert")
        await s.emit(make_ctx(), emissions=e)
        res = await s.emit(make_ctx(), emissions=e)
        assert not res.created and s.updated == [("ext-1", "p/2026-04-28")]

@pytest.mark.asyncio
async def test_create_once_noop_on_retry(tmp_path):
    async with open_db(tmp_path / "d.db") as db:
        e = EmissionStore(db)
        s = StubSink("sid1", mode="create_once")
        await s.emit(make_ctx(), emissions=e)
        res = await s.emit(make_ctx(), emissions=e)
        assert not res.created and s.updated == []  # no-op

@pytest.mark.asyncio
async def test_create_new_always_creates(tmp_path):
    async with open_db(tmp_path / "d.db") as db:
        e = EmissionStore(db)
        s = StubSink("sid1", mode="create_new")
        r1 = await s.emit(make_ctx(), emissions=e)
        r2 = await s.emit(make_ctx(), emissions=e)
        assert r1.external_id != r2.external_id
        assert r1.created and r2.created

@pytest.mark.asyncio
async def test_emit_on_empty_skips(tmp_path):
    async with open_db(tmp_path / "d.db") as db:
        e = EmissionStore(db)
        s = StubSink("sid1"); s.emit_on_empty = False
        res = await s.emit(PipelineContext("p","p/r","r",[],{}), emissions=e)
        assert res is None
```

- [ ] **Step 2: Implement**

```python
# sluice/sinks/base.py
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from sluice.context import PipelineContext
from sluice.core.protocols import SinkResult
from sluice.state.emissions import EmissionStore
from sluice.registry import register_sink as _reg

register_sink = _reg

class Sink(ABC):
    type: str = "abstract"

    def __init__(self, *, id: str, mode: str = "upsert",
                 emit_on_empty: bool = False):
        self.id = id
        self.mode = mode
        self.emit_on_empty = emit_on_empty

    @abstractmethod
    async def create(self, ctx: PipelineContext) -> str: ...

    async def update(self, external_id: str, ctx: PipelineContext) -> str:
        # default: re-create. Subclasses override for true update semantics.
        return await self.create(ctx)

    async def emit(self, ctx: PipelineContext, *,
                   emissions: EmissionStore) -> SinkResult | None:
        if not ctx.items and not self.emit_on_empty:
            return None
        if self.mode == "create_new":
            ext = await self.create(ctx)
            await emissions.insert(ctx.pipeline_id, ctx.run_key,
                                    self.id, self.type, ext)
            return SinkResult(self.id, self.type, ext, created=True)
        prior = await emissions.lookup(ctx.pipeline_id, ctx.run_key, self.id)
        if prior:
            if self.mode == "create_once":
                return SinkResult(self.id, self.type, prior.external_id,
                                  created=False)
            ext = await self.update(prior.external_id, ctx)
            return SinkResult(self.id, self.type, ext, created=False)
        ext = await self.create(ctx)
        await emissions.insert(ctx.pipeline_id, ctx.run_key,
                                self.id, self.type, ext)
        return SinkResult(self.id, self.type, ext, created=True)
```

- [ ] **Step 3: Run + Commit**

```bash
pytest tests/sinks/test_base.py -v
git add sluice/sinks/__init__.py sluice/sinks/base.py tests/sinks/__init__.py tests/sinks/test_base.py
git commit -m "feat(sinks): Sink ABC with idempotent emit dispatch (4 modes)"
```

### Task 9.2: file_md sink

**Files:**
- Create: `sluice/sinks/file_md.py`, `tests/sinks/test_file_md.py`

- [ ] **Step 1: Write failing test**

```python
# tests/sinks/test_file_md.py
import pytest
from pathlib import Path
from sluice.context import PipelineContext
from sluice.sinks.file_md import FileMdSink
from sluice.state.db import open_db
from sluice.state.emissions import EmissionStore

@pytest.mark.asyncio
async def test_writes_file(tmp_path):
    out = tmp_path / "out" / "{run_date}.md"
    s = FileMdSink(id="local", input="context.markdown", path=str(out))
    ctx = PipelineContext("p","p/2026-04-28","2026-04-28",
                          [object()], {"markdown": "# hi"})
    async with open_db(tmp_path / "d.db") as db:
        e = EmissionStore(db)
        res = await s.emit(ctx, emissions=e)
    written = tmp_path / "out" / "2026-04-28.md"
    assert written.read_text() == "# hi"
    assert res.external_id == str(written)

@pytest.mark.asyncio
async def test_overwrite_on_retry(tmp_path):
    out = tmp_path / "{run_date}.md"
    s = FileMdSink(id="local", input="context.markdown", path=str(out),
                   mode="upsert")
    async with open_db(tmp_path / "d.db") as db:
        e = EmissionStore(db)
        await s.emit(PipelineContext("p","p/r","r",[1],{"markdown":"v1"}),
                     emissions=e)
        await s.emit(PipelineContext("p","p/r","r",[1],{"markdown":"v2"}),
                     emissions=e)
    assert (tmp_path / "r.md").read_text() == "v2"
```

- [ ] **Step 2: Implement**

```python
# sluice/sinks/file_md.py
from pathlib import Path
from sluice.sinks.base import Sink, register_sink
from sluice.context import PipelineContext

@register_sink("file_md")
class FileMdSink(Sink):
    type = "file_md"

    def __init__(self, *, id: str, input: str, path: str,
                 mode: str = "upsert", emit_on_empty: bool = False):
        super().__init__(id=id, mode=mode, emit_on_empty=emit_on_empty)
        self.input = input
        self.path_template = path

    def _resolve_path(self, ctx: PipelineContext) -> Path:
        return Path(self.path_template.format(
            run_date=ctx.run_date,
            pipeline_id=ctx.pipeline_id,
            run_key=ctx.run_key.replace("/", "_"),
        ))

    def _content(self, ctx: PipelineContext) -> str:
        head, _, key = self.input.partition(".")
        if head != "context":
            raise ValueError("file_md input must be 'context.<key>'")
        return ctx.context[key]

    async def create(self, ctx: PipelineContext) -> str:
        p = self._resolve_path(ctx)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(self._content(ctx))
        return str(p)

    async def update(self, external_id: str, ctx: PipelineContext) -> str:
        # path is deterministic; just overwrite at the resolved path
        return await self.create(ctx)
```

- [ ] **Step 3: Run + Commit**

```bash
pytest tests/sinks/test_file_md.py -v
git add sluice/sinks/file_md.py tests/sinks/test_file_md.py
git commit -m "feat(sinks): file_md sink with deterministic overwrite"
```

### Task 9.3: notion sink (notionify wrapper)

> **notionify v3 API confirmed** (verified against
> `/home/dengqi/Source/langs/python/notionify/src/notionify/client.py`):
> - `NotionifyClient(token=...).create_page_with_markdown(parent_id, title,
>   markdown, parent_type, properties, title_from_h1) -> PageCreateResult`
>   (`.page_id`)
> - `NotionifyClient(token=...).update_page_from_markdown(page_id,
>   markdown, strategy="diff"|"overwrite", on_conflict="raise"|"overwrite")
>   -> UpdateResult`
> - The local notionify v3 client is synchronous. Sluice exposes an async
>   adapter Protocol and wraps these calls with `asyncio.to_thread` so Notion
>   network I/O does not block the event loop.
>
> notionify takes **raw markdown** and does its own block conversion
> (handles the 2000-char Notion block limit internally), so the
> `max_block_chars`/`chunk_markdown` we plumbed through the spec collapses
> into a single passthrough — we just call notionify with the full string.
> `chunk_markdown` is kept (tested + exposed) so future sinks (Slack
> message split, etc.) reuse it.

**Files:**
- Create: `sluice/sinks/notion.py`, `tests/sinks/test_notion.py`

- [ ] **Step 1: Write failing test**

```python
# tests/sinks/test_notion.py
import pytest
from datetime import datetime, timezone
from sluice.context import PipelineContext
from sluice.core.item import Item
from sluice.sinks.notion import NotionSink, chunk_markdown
from sluice.state.db import open_db
from sluice.state.emissions import EmissionStore

class FakeAdapter:
    def __init__(self): self.created = []; self.updated = []
    async def create_page(self, *, parent_id, parent_type, title, properties,
                          markdown):
        self.created.append({"parent_id": parent_id, "title": title,
                             "markdown": markdown})
        return f"pg-{len(self.created)}"
    async def replace_page_blocks(self, page_id, markdown):
        self.updated.append((page_id, markdown))

def test_chunk_markdown_respects_limit():
    long = "a" * 5000
    chunks = chunk_markdown(long, 1900)
    assert all(len(c) <= 1900 for c in chunks)
    assert "".join(chunks) == long

def test_chunk_markdown_keeps_short():
    assert chunk_markdown("short", 1900) == ["short"]

def mk(s):
    return Item(source_id="s", pipeline_id="p", guid=s, url=f"https://x/{s}",
                title=s, published_at=datetime(2026,4,28,tzinfo=timezone.utc),
                raw_summary=None)

@pytest.mark.asyncio
async def test_create_then_update(tmp_path, monkeypatch):
    monkeypatch.setenv("PARENT", "db-123")
    fake = FakeAdapter()
    s = NotionSink(id="notion_main", input="context.markdown",
                   parent_id="env:PARENT", parent_type="database",
                   title_template="X · {run_date}",
                   properties={"Tag": "AI"}, mode="upsert",
                   max_block_chars=1900,
                   adapter=fake)
    ctx = PipelineContext("p","p/2026-04-28","2026-04-28",
                          [mk("a")],
                          {"markdown": "hello world"})
    async with open_db(tmp_path / "d.db") as db:
        e = EmissionStore(db)
        await s.emit(ctx, emissions=e)
        assert fake.created[0]["parent_id"] == "db-123"
        assert fake.created[0]["title"] == "X · 2026-04-28"
        assert fake.created[0]["markdown"] == "hello world"
        # second emit: update path
        ctx.context["markdown"] = "v2"
        await s.emit(ctx, emissions=e)
        assert fake.updated and fake.updated[0] == ("pg-1", "v2")
```

- [ ] **Step 2: Implement**

```python
# sluice/sinks/notion.py
import asyncio
from typing import Protocol
from sluice.sinks.base import Sink, register_sink
from sluice.context import PipelineContext
from sluice.loader import resolve_env

class NotionifyAdapter(Protocol):
    """Thin Protocol over the notionify package. Tests substitute a fake;
    production wires DefaultNotionifyAdapter, whose methods are async and
    wrap notionify's synchronous client in asyncio.to_thread."""
    async def create_page(self, *, parent_id: str, parent_type: str,
                          title: str, properties: dict, markdown: str
                          ) -> str:
        """Create a Notion page from full markdown; return page_id."""
        ...

    async def replace_page_blocks(self, page_id: str, markdown: str) -> None:
        """Replace page content with new markdown (notionify's
        update_page_from_markdown with strategy='overwrite')."""
        ...

class DefaultNotionifyAdapter:
    """Wires sluice's contract to notionify v3's synchronous NotionifyClient.
    Notion v1 cover image support is INTENTIONALLY OMITTED — see Deferred
    from v1 at top of plan."""

    def __init__(self, token: str):
        from notionify import NotionifyClient
        self._client = NotionifyClient(token=token)

    async def create_page(self, *, parent_id, parent_type, title,
                          properties, markdown):
        result = await asyncio.to_thread(
            self._client.create_page_with_markdown,
            parent_id=parent_id,
            title=title,
            markdown=markdown,
            parent_type=parent_type,
            properties=properties or None,
            title_from_h1=False,
        )
        return result.page_id

    async def replace_page_blocks(self, page_id, markdown):
        # strategy="overwrite": replace page content wholesale, matching
        # our "upsert mode = replace blocks" contract from the spec.
        # on_conflict="overwrite" because sluice owns these pages;
        # external edits between runs are intentionally clobbered.
        await asyncio.to_thread(
            self._client.update_page_from_markdown,
            page_id=page_id, markdown=markdown,
            strategy="overwrite", on_conflict="overwrite",
        )

def chunk_markdown(text: str, max_chars: int) -> list[str]:
    if len(text) <= max_chars:
        return [text] if text else []
    chunks = []
    i = 0
    while i < len(text):
        chunks.append(text[i:i + max_chars])
        i += max_chars
    return chunks

@register_sink("notion")
class NotionSink(Sink):
    type = "notion"

    def __init__(self, *, id: str, input: str, parent_id: str,
                 parent_type: str, title_template: str,
                 token: str = "env:NOTION_TOKEN",
                 properties: dict | None = None, mode: str = "upsert",
                 max_block_chars: int = 1900,
                 emit_on_empty: bool = False,
                 adapter: NotionifyAdapter | None = None):
        super().__init__(id=id, mode=mode, emit_on_empty=emit_on_empty)
        self.input = input
        self.parent_id_raw = parent_id
        self.parent_type = parent_type
        self.title_template = title_template
        self.token_raw = token
        self.properties = properties or {}
        self.max_block_chars = max_block_chars  # passthrough for forward-compat
        # Tests pass a fake adapter; production constructs DefaultNotionifyAdapter
        # lazily on first emit() so a missing NOTION_TOKEN env var fails the
        # specific run rather than module import.
        self._adapter_override = adapter

    def _get_adapter(self) -> NotionifyAdapter:
        if self._adapter_override is not None:
            return self._adapter_override
        return DefaultNotionifyAdapter(token=resolve_env(self.token_raw))

    def _markdown(self, ctx: PipelineContext) -> str:
        head, _, key = self.input.partition(".")
        return ctx.context[key]

    def _title(self, ctx: PipelineContext) -> str:
        return self.title_template.format(
            run_date=ctx.run_date, pipeline_id=ctx.pipeline_id)

    async def create(self, ctx: PipelineContext) -> str:
        parent = resolve_env(self.parent_id_raw)
        adapter = self._get_adapter()
        # notionify handles its own block chunking; we pass full markdown.
        page_id = await adapter.create_page(
            parent_id=parent, parent_type=self.parent_type,
            title=self._title(ctx), properties=self.properties,
            markdown=self._markdown(ctx),
        )
        return page_id

    async def update(self, external_id: str, ctx: PipelineContext) -> str:
        adapter = self._get_adapter()
        await adapter.replace_page_blocks(external_id, self._markdown(ctx))
        return external_id
```

- [ ] **Step 3: Run + Commit**

```bash
pytest tests/sinks/test_notion.py -v
git add sluice/sinks/notion.py tests/sinks/test_notion.py
git commit -m "feat(sinks): notion sink wrapping notionify with chunking + cover"
```

---

## Phase 10 — Flow Assembly

### Task 10.1: Plugin builder (TOML → instances)

**Files:**
- Create: `sluice/builders.py`, `tests/test_builders.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_builders.py
import pytest
from sluice.config import (
    GlobalConfig, FetcherImplConfig, GlobalFetcherConfig,
    PipelineConfig, RssSourceConfig, DedupeConfig, FetcherApplyConfig,
    FilterConfig, FilterRule, FieldFilterConfig, FieldOp, LLMStageConfig,
    RenderConfig, FileMdSinkConfig,
)
from sluice.builders import build_sources, build_fetcher_chain, build_processors

# Force plugin imports
import sluice.sources.rss     # noqa
import sluice.fetchers.trafilatura_fetcher  # noqa
import sluice.fetchers.firecrawl  # noqa
import sluice.processors.dedupe  # noqa
import sluice.processors.filter  # noqa

def test_build_rss_source():
    cfg = PipelineConfig(
        id="p", window="24h",
        sources=[RssSourceConfig(type="rss", url="https://x/feed", tag="ai")],
        stages=[DedupeConfig(type="dedupe", name="d")],
        sinks=[FileMdSinkConfig(id="x", type="file_md",
                                input="context.markdown", path="./{run_date}.md")],
    )
    sources = build_sources(cfg)
    assert len(sources) == 1 and sources[0].url == "https://x/feed"
    assert sources[0].tags == ["ai"]
```

- [ ] **Step 2: Implement**

```python
# sluice/builders.py
from sluice.registry import (
    get_source, get_fetcher, get_processor, get_sink,
)
from sluice.config import (
    GlobalConfig, PipelineConfig, RssSourceConfig,
    DedupeConfig, FetcherApplyConfig, FilterConfig, FieldFilterConfig,
    LLMStageConfig, RenderConfig, FileMdSinkConfig, NotionSinkConfig,
    FetcherImplConfig, GlobalFetcherConfig,
)
from sluice.fetchers.chain import FetcherChain
from sluice.state.cache import UrlCacheStore
from sluice.state.seen import SeenStore
from sluice.state.failures import FailureStore
from sluice.state.emissions import EmissionStore
from sluice.llm.pool import ProviderPool
from sluice.llm.client import LLMClient, StageLLMConfig
from sluice.llm.provider import parse_model_spec
from sluice.core.errors import ConfigError

def _model_price(pool: ProviderPool, model_spec: str) -> tuple[float, float]:
    """Lookup (input_per_1k, output_per_1k) for a 'provider/model' spec."""
    prov, model = parse_model_spec(model_spec)
    rt = pool.runtimes.get(prov)
    if rt is None:
        raise ConfigError(f"unknown provider in model spec: {model_spec}")
    m = rt._models.get(model)
    if m is None:
        raise ConfigError(f"unknown model in model spec: {model_spec}")
    return (m.input_price_per_1k, m.output_price_per_1k)

def build_sources(pipe: PipelineConfig):
    out = []
    for i, s in enumerate(pipe.sources):
        if isinstance(s, RssSourceConfig):
            cls = get_source("rss")
            out.append(cls(url=s.url, pipeline_id=pipe.id,
                           source_id=s.name or f"rss_{i}",
                           tag=s.tag, name=s.name))
    return out

def _resolve_fetcher_chain_cfg(global_cfg: GlobalConfig,
                               pipe: PipelineConfig) -> tuple[list[str], int, str, bool, int]:
    g = global_cfg.fetcher
    chain = pipe.fetcher.chain or g.chain
    min_chars = pipe.fetcher.min_chars or g.min_chars
    on_all_failed = pipe.fetcher.on_all_failed or g.on_all_failed
    # Cache: pipeline overrides global ONLY if the pipeline TOML explicitly
    # set [cache]; otherwise inherit global. We detect "explicit" by checking
    # whether the pipeline-level CacheOverride is the field default instance
    # (Pydantic exposes set_fields via .model_fields_set).
    if "cache" in pipe.model_fields_set:
        cache_enabled = pipe.cache.enabled
        ttl_str = pipe.cache.ttl
    else:
        cache_enabled = g.cache.enabled
        ttl_str = g.cache.ttl
    from sluice.window import parse_duration
    ttl = int(parse_duration(ttl_str).total_seconds())
    return chain, min_chars, on_all_failed, cache_enabled, ttl

def build_fetcher_chain(global_cfg: GlobalConfig, pipe: PipelineConfig,
                        cache: UrlCacheStore | None) -> FetcherChain:
    chain_names, min_chars, on_all_failed, cache_enabled, ttl = \
        _resolve_fetcher_chain_cfg(global_cfg, pipe)
    fetchers = []
    for name in chain_names:
        impl = global_cfg.fetchers.get(name)
        if impl is None:
            raise ValueError(f"fetcher {name!r} declared in chain "
                             f"but missing [fetchers.{name}]")
        cls = get_fetcher(impl.type)
        kwargs = impl.model_dump(exclude={"type"}, exclude_none=True)
        # api_key may be 'env:NAME'
        if kwargs.get("api_key") and kwargs["api_key"].startswith("env:"):
            from sluice.loader import resolve_env
            kwargs["api_key"] = resolve_env(kwargs["api_key"])
        fetchers.append(cls(**kwargs))
    return FetcherChain(
        fetchers, min_chars=min_chars, on_all_failed=on_all_failed,
        cache=cache if cache_enabled else None, ttl_seconds=ttl,
    )

def build_processors(*, pipe: PipelineConfig, global_cfg: GlobalConfig,
                     seen: SeenStore, failures: FailureStore,
                     fetcher_chain: FetcherChain, llm_pool: ProviderPool,
                     budget, dry_run: bool = False):
    # In dry-run we pass failures=None so per-item LLM/fetcher errors are
    # logged-and-dropped without persisting into failed_items.
    eff_failures = None if dry_run else failures
    procs = []
    for st in pipe.stages:
        if isinstance(st, DedupeConfig):
            from sluice.processors.dedupe import DedupeProcessor
            procs.append(DedupeProcessor(name=st.name, pipeline_id=pipe.id,
                                          seen=seen, failures=failures))
        elif isinstance(st, FetcherApplyConfig):
            from sluice.processors.fetcher_apply import FetcherApplyProcessor
            procs.append(FetcherApplyProcessor(
                name=st.name, chain=fetcher_chain,
                write_field=st.write_field,
                skip_if_field_longer_than=st.skip_if_field_longer_than,
                failures=eff_failures, max_retries=pipe.failures.max_retries))
        elif isinstance(st, FilterConfig):
            from sluice.processors.filter import FilterProcessor
            procs.append(FilterProcessor(name=st.name, mode=st.mode,
                                          rules=st.rules))
        elif isinstance(st, FieldFilterConfig):
            from sluice.processors.field_filter import FieldFilterProcessor
            procs.append(FieldFilterProcessor(name=st.name, ops=st.ops))
        elif isinstance(st, LLMStageConfig):
            from sluice.processors.llm_stage import LLMStageProcessor
            stage_llm = StageLLMConfig(
                model=st.model, retry_model=st.retry_model,
                fallback_model=st.fallback_model,
                fallback_model_2=st.fallback_model_2)
            procs.append(LLMStageProcessor(
                name=st.name, mode=st.mode,
                input_field=st.input_field,
                output_field=st.output_field,
                output_target=st.output_target,
                prompt_file=st.prompt_file,
                llm_factory=lambda cfg=stage_llm: LLMClient(llm_pool, cfg, budget),
                output_parser=st.output_parser,
                on_parse_error=st.on_parse_error,
                on_parse_error_default=st.on_parse_error_default,
                max_input_chars=st.max_input_chars,
                truncate_strategy=st.truncate_strategy,
                workers=st.workers,
                failures=eff_failures, budget=budget,
                pipeline_id=pipe.id,
                max_retries=pipe.failures.max_retries,
                model_spec=st.model,
                price_lookup=lambda spec, pool=llm_pool: _model_price(pool, spec)))
        elif isinstance(st, RenderConfig):
            from sluice.processors.render import RenderProcessor
            procs.append(RenderProcessor(name=st.name, template=st.template,
                                          output_field=st.output_field))
        else:
            raise ValueError(f"unknown stage type: {type(st).__name__}")
    return procs

def build_sinks(pipe: PipelineConfig):
    out = []
    for s in pipe.sinks:
        if isinstance(s, FileMdSinkConfig):
            from sluice.sinks.file_md import FileMdSink
            out.append(FileMdSink(id=s.id, input=s.input, path=s.path,
                                   emit_on_empty=s.emit_on_empty))
        elif isinstance(s, NotionSinkConfig):
            from sluice.sinks.notion import NotionSink
            out.append(NotionSink(
                id=s.id, input=s.input,
                parent_id=s.parent_id, parent_type=s.parent_type,
                title_template=s.title_template, token=s.token,
                properties=s.properties,
                mode=s.mode, max_block_chars=s.max_block_chars,
                emit_on_empty=s.emit_on_empty))
        else:
            raise ValueError(f"unknown sink type: {type(s).__name__}")
    return out
```

- [ ] **Step 3: Run + Commit**

```bash
pytest tests/test_builders.py -v
git add sluice/builders.py tests/test_builders.py
git commit -m "feat: TOML→instance builders for sources/fetchers/processors/sinks"
```

### Task 10.2: Pipeline runner (commit-after-success contract)

**Files:**
- Create: `sluice/runner.py`, `tests/test_runner.py`

- [ ] **Step 1: Write failing integration test**

```python
# tests/test_runner.py
import pytest, textwrap
from pathlib import Path
import respx, httpx
from sluice.loader import load_all
from sluice.runner import run_pipeline
from sluice.state.db import open_db

# Force registry imports
import sluice.sources.rss  # noqa
import sluice.fetchers.trafilatura_fetcher  # noqa
import sluice.processors.dedupe  # noqa
import sluice.processors.fetcher_apply  # noqa
import sluice.processors.render  # noqa
import sluice.sinks.file_md  # noqa

@pytest.mark.asyncio
async def test_end_to_end_no_llm(tmp_path, monkeypatch):
    cfgdir = tmp_path / "configs"
    (cfgdir / "pipelines").mkdir(parents=True)
    (cfgdir / "sluice.toml").write_text(textwrap.dedent(f"""
        [state]
        db_path = "{tmp_path}/d.db"
        [runtime]
        timezone = "UTC"
        default_cron = "0 8 * * *"
        prefect_api_url = "http://x"
        [fetcher]
        chain = ["trafilatura"]
        min_chars = 5
        on_all_failed = "skip"
        [fetchers.trafilatura]
        type = "trafilatura"
        timeout = 5
        [fetcher.cache]
        enabled = true
        ttl = "1d"
    """))
    (cfgdir / "providers.toml").write_text(textwrap.dedent("""
        [[providers]]
        name = "n"
        type = "openai_compatible"
        [[providers.base]]
        url = "https://x"
        weight = 1
        key = [{ value = "env:K", weight = 1 }]
        [[providers.models]]
        model_name = "m"
    """))
    tpl = tmp_path / "tpl.j2"
    tpl.write_text("count={{ items|length }}")
    (cfgdir / "pipelines" / "p.toml").write_text(textwrap.dedent(f"""
        id = "p"
        window = "24h"
        timezone = "UTC"
        [[sources]]
        type = "rss"
        url  = "https://feed.example/rss"
        [[stages]]
        name = "d"
        type = "dedupe"
        [[stages]]
        name = "fa"
        type = "fetcher_apply"
        write_field = "fulltext"
        [[stages]]
        name = "r"
        type = "render"
        template = "{tpl}"
        output_field = "context.markdown"
        [[sinks]]
        id = "local"
        type = "file_md"
        input = "context.markdown"
        path  = "{tmp_path}/out_{{{{ run_date }}}}.md"
    """))
    monkeypatch.setenv("K", "x")
    bundle = load_all(cfgdir)

    rss = """<?xml version="1.0"?><rss><channel>
      <item><title>A</title><link>https://art.example/a</link>
        <guid>g-a</guid>
        <pubDate>Mon, 28 Apr 2026 00:00:00 +0000</pubDate>
      </item></channel></rss>"""
    article_html = "<html><body><article>" + ("article body text " * 30) + \
                   "</article></body></html>"

    from datetime import datetime, timezone
    fake_now = datetime(2026, 4, 28, 12, 0, tzinfo=timezone.utc)
    with respx.mock() as r:
        r.get("https://feed.example/rss").mock(
            return_value=httpx.Response(200, text=rss))
        r.get("https://art.example/a").mock(
            return_value=httpx.Response(200, text=article_html))
        result = await run_pipeline(bundle, pipeline_id="p", now=fake_now)

    assert result.status == "success"
    assert result.items_out == 1
    out_files = list(tmp_path.glob("out_*.md"))
    assert len(out_files) == 1
    assert "count=1" in out_files[0].read_text()

    # Run again — dedupe should drop, sink should not re-create
    with respx.mock() as r:
        r.get("https://feed.example/rss").mock(
            return_value=httpx.Response(200, text=rss))
        r.get("https://art.example/a").mock(
            return_value=httpx.Response(200, text=article_html))
        result2 = await run_pipeline(bundle, pipeline_id="p", now=fake_now)
    assert result2.items_out == 0
```

- [ ] **Step 2: Implement runner**

```python
# sluice/runner.py
from dataclasses import dataclass
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import logging
from sluice.config import PipelineConfig, GlobalConfig
from sluice.context import PipelineContext, RunStats
from sluice.loader import ConfigBundle
from sluice.state.db import open_db
from sluice.state.seen import SeenStore
from sluice.state.failures import FailureStore
from sluice.state.emissions import EmissionStore
from sluice.state.cache import UrlCacheStore
from sluice.state.run_log import RunLog
from sluice.llm.pool import ProviderPool
from sluice.llm.budget import RunBudget
from sluice.window import compute_window
from sluice.processors.dedupe import item_key
from sluice.builders import (
    build_sources, build_fetcher_chain, build_processors, build_sinks,
)

log = logging.getLogger(__name__)

@dataclass
class RunResult:
    pipeline_id: str
    run_key: str
    status: str
    items_in: int = 0
    items_out: int = 0
    error: str | None = None

def _resolve_db_path(global_cfg: GlobalConfig, pipe: PipelineConfig) -> str:
    return pipe.state.db_path or global_cfg.state.db_path

def _resolve_tz(global_cfg: GlobalConfig, pipe: PipelineConfig) -> ZoneInfo:
    return ZoneInfo(pipe.timezone or global_cfg.runtime.timezone)

def _run_key(pipe_id: str, run_date: str) -> str:
    return f"{pipe_id}/{run_date}"

async def run_pipeline(bundle: ConfigBundle, *, pipeline_id: str,
                       now: datetime | None = None,
                       dry_run: bool = False) -> RunResult:
    """Run one pipeline once.

    dry_run semantics (intentional carve-out):
      - SKIPPED: sink emit, seen_items insert, failed_items writes from
        per-item LLM/fetcher failures (observed-but-not-persisted),
        sink_emissions insert, mark_resolved on requeued items.
      - STILL WRITTEN: run_log start/update/finish so the dry-run shows
        up in observability with its projected cost. run_log is local
        bookkeeping with no external side effect; suppressing it would
        make `--dry-run` invisible to the operator.
      - Per-item LLM failures during dry-run are logged to stdout but
        NOT recorded in failed_items (would otherwise force retry next
        real run for items the operator was just probing).
    """
    pipe = bundle.pipelines[pipeline_id]
    global_cfg = bundle.global_cfg
    tz = _resolve_tz(global_cfg, pipe)
    now = now or datetime.now(timezone.utc)
    local_now = now.astimezone(tz)
    run_date = local_now.date().isoformat()
    run_key = _run_key(pipe.id, run_date)

    db_path = _resolve_db_path(global_cfg, pipe)
    async with open_db(db_path) as db:
        seen = SeenStore(db); failures = FailureStore(db)
        emissions = EmissionStore(db); cache = UrlCacheStore(db)
        runlog = RunLog(db)
        await runlog.start(pipe.id, run_key)

        budget = RunBudget(max_calls=pipe.limits.max_llm_calls_per_run,
                           max_usd=pipe.limits.max_estimated_cost_usd)
        pool = ProviderPool(bundle.providers)
        chain = build_fetcher_chain(global_cfg, pipe, cache)

        # Validate that every llm_stage's model has non-zero pricing
        # configured if max_estimated_cost_usd > 0; otherwise the cap
        # silently no-ops (price → $0 → never trips). Fail fast.
        from sluice.config import LLMStageConfig as _LLMStageConfig
        from sluice.builders import _model_price as _mp
        from sluice.core.errors import ConfigError as _ConfigError
        if pipe.limits.max_estimated_cost_usd > 0:
            for st in pipe.stages:
                if not isinstance(st, _LLMStageConfig):
                    continue
                for spec in (st.model, st.retry_model,
                             st.fallback_model, st.fallback_model_2):
                    if spec is None:
                        continue
                    ip, op = _mp(pool, spec)
                    if ip == 0.0 and op == 0.0:
                        raise _ConfigError(
                            f"pipeline {pipe.id!r}: max_estimated_cost_usd "
                            f"is set but model {spec!r} has no "
                            f"input/output_price_per_1k in providers.toml "
                            f"— cost cap would silently never trip. "
                            f"Set prices or set max_estimated_cost_usd = 0 "
                            f"to disable the USD cap explicitly."
                        )

        try:
            window_start, window_end = compute_window(
                now=now, window=pipe.window,
                lookback_overlap=pipe.lookback_overlap)

            # 1. Sources (concurrent)
            sources = build_sources(pipe)
            collected = []
            for src in sources:
                async for it in src.fetch(window_start, window_end):
                    collected.append(it)

            # 2. Re-queue failed items (bypass dedupe)
            requeue = await failures.requeue(pipe.id)

            # Snapshot the requeue set BEFORE merging so we know which
            # items came from failures (used at commit time to mark them
            # resolved, regardless of whether they survived later stages).
            requeued_keys = {item_key(it) for it in requeue}

            ctx = PipelineContext(
                pipeline_id=pipe.id, run_key=run_key, run_date=run_date,
                items=collected, context={},
                stats=RunStats(items_in=len(collected) + len(requeue)),
                items_resolved_from_failures=list(requeue),
            )

            # 3. Build stages.
            procs = build_processors(
                pipe=pipe, global_cfg=global_cfg, seen=seen,
                failures=failures, fetcher_chain=chain, llm_pool=pool,
                budget=budget, dry_run=dry_run,
            )

            # 4. Run stages in order. Spec: backpressure trim happens
            # AFTER Source.fetch + dedupe (so dedupe can drop seen items
            # first, freeing up cap for fresh items). We apply the cap
            # immediately after the DedupeProcessor runs. Re-queued failed
            # items are then merged in (bypassing dedupe).
            from sluice.processors.dedupe import DedupeProcessor
            cap = pipe.limits.max_items_per_run
            policy = pipe.limits.item_overflow_policy
            min_dt = datetime.min.replace(tzinfo=timezone.utc)
            requeue_pending = list(requeue)
            for p in procs:
                ctx = await p.process(ctx)
                if isinstance(p, DedupeProcessor):
                    if len(ctx.items) > cap:
                        ctx.items.sort(
                            key=lambda i: i.published_at or min_dt,
                            reverse=(policy == "drop_oldest"))
                        ctx.items = ctx.items[:cap]
                    if requeue_pending:
                        ctx.items.extend(requeue_pending)
                        requeue_pending = []  # only merge once

            ctx.stats.items_out = len(ctx.items)
            ctx.stats.llm_calls = budget.calls
            ctx.stats.est_cost_usd = budget.spent_usd

            # 5. Sinks (skip on dry-run)
            if not dry_run:
                for sink in build_sinks(pipe):
                    await sink.emit(ctx, emissions=emissions)

            # 6. Commit (skip on dry-run)
            if not dry_run:
                kept = [(it, item_key(it)) for it in ctx.items]
                await seen.mark_seen_batch(pipe.id, kept)
                # Mark resolved using the snapshot of original requeued keys,
                # NOT the cleared requeue list. An item is "resolved" if it
                # was in the requeue set AND is now in ctx.items (survived).
                surviving_keys = {item_key(it) for it in ctx.items}
                for k in requeued_keys & surviving_keys:
                    await failures.mark_resolved(pipe.id, k)

            await runlog.update_stats(
                pipe.id, run_key,
                items_in=ctx.stats.items_in, items_out=ctx.stats.items_out,
                llm_calls=ctx.stats.llm_calls,
                est_cost_usd=ctx.stats.est_cost_usd)
            await runlog.finish(pipe.id, run_key, status="success")
            return RunResult(pipeline_id=pipe.id, run_key=run_key,
                             status="success",
                             items_in=ctx.stats.items_in,
                             items_out=ctx.stats.items_out)
        except Exception as e:
            log.exception("pipeline failed")
            await runlog.finish(pipe.id, run_key, status="failed",
                                error_msg=str(e))
            return RunResult(pipeline_id=pipe.id, run_key=run_key,
                             status="failed", error=str(e))
```

- [ ] **Step 3: Run + Commit**

```bash
pytest tests/test_runner.py -v
git add sluice/runner.py tests/test_runner.py
git commit -m "feat: pipeline runner with commit-after-success and requeue merge"
```

### Task 10.2.5: Runner regression tests (review-driven)

**Files:**
- Modify: `tests/test_runner.py` (append three tests)

These cover the three runner blockers caught in spec review: requeued
items must actually be resolved, backpressure must fire after dedupe, and
`--dry-run` must skip external side effects while still writing run_log for
operator visibility.

- [ ] **Step 1: Add the tests**

```python
# Append to tests/test_runner.py

def _bootstrap_minimal(tmp_path, *, rss_text: str, cap: int = 30,
                       extra_pipe_toml: str = ""):
    """Helper used by all three regression tests. Builds a configs/ tree
    with: state DB at tmp_path/d.db, one openai-compat provider (env K),
    one RSS source (mocked), dedupe + render + file_md sink. No LLM stages
    so we don't need to mock the chat endpoint."""
    import textwrap
    cfg = tmp_path / "configs"; (cfg / "pipelines").mkdir(parents=True)
    (cfg / "sluice.toml").write_text(textwrap.dedent(f"""
        [state]
        db_path = "{tmp_path}/d.db"
        [runtime]
        timezone="UTC"
        default_cron="0 8 * * *"
        prefect_api_url="http://x"
        [fetcher]
        chain = ["trafilatura"]
        min_chars = 50
        on_all_failed = "skip"
        [fetchers.trafilatura]
        type = "trafilatura"
        timeout = 5
        [fetcher.cache]
        enabled = false
        ttl = "1d"
    """))
    (cfg / "providers.toml").write_text(textwrap.dedent("""
        [[providers]]
        name = "n"
        type = "openai_compatible"
        [[providers.base]]
        url = "https://llm.example"
        weight = 1
        key = [{ value = "env:K", weight = 1 }]
        [[providers.models]]
        model_name = "m"
    """))
    tpl = tmp_path / "tpl.j2"
    tpl.write_text("count={{ items|length }}")
    (cfg / "pipelines" / "p.toml").write_text(textwrap.dedent(f"""
        id = "p"
        window = "24h"
        timezone = "UTC"
        [limits]
        max_items_per_run = {cap}
        item_overflow_policy = "drop_oldest"
        [[sources]]
        type = "rss"
        url = "https://feed.example/rss"
        [[stages]]
        name = "d"
        type = "dedupe"
        [[stages]]
        name = "r"
        type = "render"
        template = "{tpl}"
        output_field = "context.markdown"
        [[sinks]]
        id = "out"
        type = "file_md"
        input = "context.markdown"
        path  = "{tmp_path}/out_{{{{ run_date }}}}.md"
        {extra_pipe_toml}
    """))
    return cfg

@pytest.mark.asyncio
async def test_requeue_resolved_after_success(tmp_path, monkeypatch):
    """Failed item → next run requeues it → it survives all stages →
    marked resolved at commit time."""
    import respx, httpx
    from datetime import datetime, timezone
    from sluice.state.db import open_db
    from sluice.state.failures import FailureStore
    from sluice.core.item import Item, compute_item_key

    monkeypatch.setenv("K", "v")
    cfg = _bootstrap_minimal(tmp_path, rss_text="")

    failed_item = Item(source_id="s", pipeline_id="p", guid="reseed",
                       url="https://x/r", title="t",
                       published_at=datetime(2026, 4, 28, tzinfo=timezone.utc),
                       raw_summary=None, fulltext="x" * 200)
    async with open_db(tmp_path / "d.db") as conn:
        await FailureStore(conn).record(
            "p", compute_item_key(failed_item), failed_item,
            stage="summarize", error_class="X", error_msg="m",
            max_retries=3,
        )

    bundle = load_all(cfg)
    fake_now = datetime(2026, 4, 28, 12, tzinfo=timezone.utc)
    empty_rss = '<?xml version="1.0"?><rss><channel></channel></rss>'
    with respx.mock() as r:
        r.get("https://feed.example/rss").mock(
            return_value=httpx.Response(200, text=empty_rss))
        result = await run_pipeline(bundle, pipeline_id="p", now=fake_now)

    assert result.status == "success"
    assert result.items_out == 1  # the requeued item
    async with open_db(tmp_path / "d.db") as conn:
        resolved = await FailureStore(conn).list("p", status="resolved")
        still_failed = await FailureStore(conn).list("p", status="failed")
    assert len(resolved) == 1
    assert resolved[0]["item_key"] == compute_item_key(failed_item)
    assert still_failed == []

@pytest.mark.asyncio
async def test_backpressure_fires_after_dedupe(tmp_path, monkeypatch):
    """100 RSS items, 60 already seen, cap=30 → exactly 30 fresh items kept.
    If trim ran pre-dedupe, we'd see ≤30 items mostly comprising "old" seen
    ones that get dedupe'd out, ending below 30. Cap-after-dedupe keeps
    exactly 30 fresh ones."""
    import respx, httpx
    from datetime import datetime, timezone
    from sluice.state.db import open_db
    from sluice.state.seen import SeenStore
    from sluice.core.item import Item, compute_item_key

    monkeypatch.setenv("K", "v")
    cfg = _bootstrap_minimal(tmp_path, rss_text="", cap=30)

    items_xml = "".join(
        f"<item><title>T{i}</title><link>https://x/{i}</link>"
        f"<guid>g-{i}</guid>"
        f"<pubDate>Mon, 28 Apr 2026 0{i % 10}:00:00 +0000</pubDate>"
        f"</item>"
        for i in range(100)
    )
    rss = f'<?xml version="1.0"?><rss><channel>{items_xml}</channel></rss>'

    # Pre-seed 60 of the 100 guids as already-seen
    async with open_db(tmp_path / "d.db") as conn:
        seen = SeenStore(conn)
        seeded = []
        for i in range(60):
            it = Item(source_id="s", pipeline_id="p", guid=f"g-{i}",
                      url=f"https://x/{i}", title=f"T{i}",
                      published_at=datetime(2026, 4, 28, tzinfo=timezone.utc),
                      raw_summary=None)
            seeded.append((it, f"g-{i}"))
        await seen.mark_seen_batch("p", seeded)

    bundle = load_all(cfg)
    fake_now = datetime(2026, 4, 28, 12, tzinfo=timezone.utc)
    with respx.mock() as r:
        r.get("https://feed.example/rss").mock(
            return_value=httpx.Response(200, text=rss))
        result = await run_pipeline(bundle, pipeline_id="p", now=fake_now)

    # Fresh = 40 items (100 - 60 seen). Cap=30 < 40, so trim AFTER dedupe
    # leaves exactly 30. (Pre-dedupe trim would leave 30 of the original
    # 100, of which ~18 would then be dedupe'd out → ~12 items_out.)
    assert result.status == "success"
    assert result.items_out == 30

@pytest.mark.asyncio
async def test_dry_run_writes_nothing_external(tmp_path, monkeypatch):
    """dry_run=True must skip sink emit AND seen_items insert; run_log
    is still written (intentional carve-out for observability)."""
    import respx, httpx
    from datetime import datetime, timezone
    from sluice.state.db import open_db
    from sluice.state.seen import SeenStore
    from sluice.state.run_log import RunLog

    monkeypatch.setenv("K", "v")
    cfg = _bootstrap_minimal(tmp_path, rss_text="")
    rss = ('<?xml version="1.0"?><rss><channel>'
           '<item><title>A</title><link>https://x/a</link>'
           '<guid>g-a</guid>'
           '<pubDate>Mon, 28 Apr 2026 06:00:00 +0000</pubDate>'
           '</item></channel></rss>')

    bundle = load_all(cfg)
    fake_now = datetime(2026, 4, 28, 12, tzinfo=timezone.utc)
    with respx.mock() as r:
        r.get("https://feed.example/rss").mock(
            return_value=httpx.Response(200, text=rss))
        result = await run_pipeline(bundle, pipeline_id="p",
                                     now=fake_now, dry_run=True)

    assert result.status == "success"
    # No sink output file written
    assert list(tmp_path.glob("out_*.md")) == []
    # seen_items remains empty
    async with open_db(tmp_path / "d.db") as conn:
        unseen = await SeenStore(conn).filter_unseen("p", ["g-a"])
        runs = await RunLog(conn).list("p")
    assert unseen == ["g-a"]               # NOT marked seen
    assert len(runs) == 1                  # but run_log DID record it
    assert runs[0]["status"] == "success"
```

- [ ] **Step 2: Run all three**

```bash
pytest tests/test_runner.py::test_requeue_resolved_after_success \
       tests/test_runner.py::test_backpressure_fires_after_dedupe \
       tests/test_runner.py::test_dry_run_writes_nothing_external -v
```
Expected: 3 PASS.

- [ ] **Step 3: Commit**

```bash
pytest tests/test_runner.py -v
git add tests/test_runner.py
git commit -m "test(runner): cover requeue→resolve, post-dedupe backpressure, dry-run"
```

### Task 10.3: Prefect flow wrapper

**Files:**
- Create: `sluice/flow.py`, `tests/test_flow.py`

- [ ] **Step 1: Write failing test**

```python
# tests/test_flow.py
import pytest
from unittest.mock import AsyncMock, patch
from sluice.flow import build_flow

@pytest.mark.asyncio
async def test_flow_calls_runner():
    fake_run = AsyncMock(return_value=type("R", (), {"status": "success",
                                                      "items_out": 5})())
    with patch("sluice.flow.run_pipeline", fake_run):
        flow = build_flow("ai_news")
        result = await flow.fn(config_dir="/tmp/x")  # invoke flow function
    fake_run.assert_called_once()
```

- [ ] **Step 2: Implement**

```python
# sluice/flow.py
from prefect import flow, task
from pathlib import Path
from sluice.loader import load_all
from sluice.runner import run_pipeline

def build_flow(pipeline_id: str):
    @flow(name=f"sluice-{pipeline_id}")
    async def _flow(config_dir: str):
        bundle = load_all(Path(config_dir))
        return await run_pipeline(bundle, pipeline_id=pipeline_id)
    return _flow
```

- [ ] **Step 3: Run + Commit**

```bash
pytest tests/test_flow.py -v
git add sluice/flow.py tests/test_flow.py
git commit -m "feat: prefect flow wrapper around runner"
```

---

## Phase 11 — CLI

### Task 11.1: typer CLI

**Files:**
- Create: `sluice/cli.py`, `tests/test_cli.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_cli.py
import pytest
from typer.testing import CliRunner
from unittest.mock import AsyncMock, patch
from sluice.cli import app

runner = CliRunner()

def test_validate_ok(tmp_path, monkeypatch):
    cfg = tmp_path / "configs"; (cfg / "pipelines").mkdir(parents=True)
    (cfg / "sluice.toml").write_text("""
[state]
db_path="x.db"
[runtime]
timezone="UTC"
default_cron="0 8 * * *"
prefect_api_url="http://x"
[fetcher]
chain=["trafilatura"]
min_chars=10
on_all_failed="skip"
[fetchers.trafilatura]
type="trafilatura"
timeout=10
[fetcher.cache]
enabled=false
ttl="1d"
""")
    (cfg / "providers.toml").write_text("""
[[providers]]
name="x"
type="openai_compatible"
[[providers.base]]
url="https://x"
weight=1
key=[{value="env:K", weight=1}]
[[providers.models]]
model_name="m"
""")
    res = runner.invoke(app, ["validate", "--config-dir", str(cfg)])
    assert res.exit_code == 0
    assert "OK" in res.output

def test_run_invokes_runner(tmp_path):
    fake = AsyncMock(return_value=type("R", (), {"status": "success",
                                                  "run_key": "p/r",
                                                  "items_out": 3,
                                                  "error": None})())
    # Real invocation requires valid configs; just unit test the wiring path
    with patch("sluice.cli.run_pipeline", fake), \
         patch("sluice.cli.load_all") as load:
        load.return_value = type("B", (), {"pipelines": {"p": object()}})()
        res = runner.invoke(app, ["run", "p", "--config-dir", str(tmp_path)])
    assert res.exit_code == 0
    assert "success" in res.output
    # default dry_run=False
    assert fake.call_args.kwargs["dry_run"] is False

def test_run_passes_dry_run_flag(tmp_path):
    fake = AsyncMock(return_value=type("R", (), {"status": "success",
                                                  "run_key": "p/r",
                                                  "items_out": 0,
                                                  "error": None})())
    with patch("sluice.cli.run_pipeline", fake), \
         patch("sluice.cli.load_all") as load:
        load.return_value = type("B", (), {"pipelines": {"p": object()}})()
        res = runner.invoke(app, ["run", "p", "--config-dir", str(tmp_path),
                                   "--dry-run"])
    assert res.exit_code == 0
    assert fake.call_args.kwargs["dry_run"] is True
```

- [ ] **Step 2: Implement**

```python
# sluice/cli.py
import asyncio
from pathlib import Path
import typer
from sluice.loader import load_all
from sluice.runner import run_pipeline

app = typer.Typer(no_args_is_help=True)

# Force plugin imports
def _import_all():
    from sluice.sources import rss  # noqa
    from sluice.fetchers import trafilatura_fetcher, firecrawl, jina_reader  # noqa
    from sluice.processors import (
        dedupe, filter as _f, field_filter, fetcher_apply, llm_stage, render,
    )  # noqa
    from sluice.sinks import file_md, notion  # noqa

@app.command()
def validate(config_dir: Path = typer.Option("./configs",
                                              "--config-dir", "-c")):
    """Validate all TOML config files."""
    _import_all()
    bundle = load_all(config_dir)
    typer.echo(f"OK: {len(bundle.pipelines)} pipeline(s) loaded")

@app.command()
def list(config_dir: Path = typer.Option("./configs",
                                          "--config-dir", "-c")):
    """List configured pipelines."""
    _import_all()
    bundle = load_all(config_dir)
    for pid, pipe in bundle.pipelines.items():
        cron = pipe.cron or bundle.global_cfg.runtime.default_cron
        typer.echo(f"{pid}\t{cron}\tenabled={pipe.enabled}")

@app.command()
def run(pipeline_id: str,
        config_dir: Path = typer.Option("./configs", "--config-dir", "-c"),
        dry_run: bool = typer.Option(False, "--dry-run")):
    """Run a pipeline once."""
    _import_all()
    bundle = load_all(config_dir)
    if pipeline_id not in bundle.pipelines:
        typer.echo(f"unknown pipeline: {pipeline_id}", err=True)
        raise typer.Exit(2)
    res = asyncio.run(run_pipeline(bundle, pipeline_id=pipeline_id,
                                   dry_run=dry_run))
    typer.echo(f"{res.status}\t{res.run_key}\titems_out={res.items_out}"
               + (f"\terror={res.error}" if res.error else ""))
    raise typer.Exit(0 if res.status == "success" else 1)

@app.command()
def failures(pipeline_id: str,
             config_dir: Path = typer.Option("./configs", "--config-dir", "-c"),
             retry: str | None = typer.Option(None, "--retry",
                 help="item_key of a dead-letter to re-queue")):
    """List failed items, optionally re-queue one."""
    _import_all()
    bundle = load_all(config_dir)
    async def _run():
        from sluice.state.db import open_db
        from sluice.state.failures import FailureStore
        db_path = bundle.pipelines[pipeline_id].state.db_path \
            or bundle.global_cfg.state.db_path
        async with open_db(db_path) as db:
            f = FailureStore(db)
            if retry:
                await db.execute(
                    "UPDATE failed_items SET status='failed', attempts=0 "
                    "WHERE pipeline_id=? AND item_key=?",
                    (pipeline_id, retry))
                await db.commit()
                typer.echo(f"requeued {retry}")
                return
            rows = await f.list(pipeline_id)
            for r in rows:
                typer.echo(f"{r['item_key']}\t{r['status']}\t"
                           f"attempts={r['attempts']}\t"
                           f"{r['error_class']}: {r['error_msg'][:80]}")
    asyncio.run(_run())

@app.command()
def deploy(config_dir: Path = typer.Option("./configs", "--config-dir", "-c")):
    """Register all enabled pipelines as Prefect deployments."""
    _import_all()
    bundle = load_all(config_dir)
    from prefect.client.schemas.schedules import CronSchedule
    from sluice.flow import build_flow
    for pid, pipe in bundle.pipelines.items():
        if not pipe.enabled:
            continue
        cron = pipe.cron or bundle.global_cfg.runtime.default_cron
        tz = pipe.timezone or bundle.global_cfg.runtime.timezone
        flow_obj = build_flow(pid)
        flow_obj.serve(
            name=f"sluice-{pid}",
            schedule=CronSchedule(cron=cron, timezone=tz),
            parameters={"config_dir": str(config_dir.resolve())},
        )

if __name__ == "__main__":
    app()
```

- [ ] **Step 3: Run + Commit**

```bash
pytest tests/test_cli.py -v
git add sluice/cli.py tests/test_cli.py
git commit -m "feat(cli): typer commands run/list/validate/deploy/failures"
```

---

## Phase 12 — Sample Pipeline + Smoke Test

### Task 12.1: Author example prompts and pipeline TOML

**Files:**
- Create: `prompts/summarize_zh.md`, `prompts/daily_brief_zh.md`,
  `templates/daily.md.j2`, `configs/pipelines/ai_news.toml.example`

- [ ] **Step 1: Write prompts**

```markdown
<!-- prompts/summarize_zh.md -->
你是一个技术新闻摘要助手。请用 3-4 句中文概括下面这篇文章的核心内容，
聚焦事实和结论，不要客套话。

标题：{{ item.title }}
正文：
{{ item.fulltext }}
```

```markdown
<!-- prompts/daily_brief_zh.md -->
你是技术日报主编。基于今天的 {{ items|length }} 条新闻摘要，写一段 200
字以内的中文综述，归纳今日趋势、值得关注的事件和潜在影响。

新闻列表：
{% for it in items %}
- 【{{ it.title }}】{{ it.summary }}
{% endfor %}
```

```jinja
<!-- templates/daily.md.j2 -->
# {{ pipeline_id }} · {{ run_date }}

> **今日综述**
>
> {{ context.daily_brief }}

---

{% for it in items %}
## {{ it.title }}

{{ it.summary }}

[原文]({{ it.url }}){% if it.published_at %} · {{ it.published_at.strftime("%Y-%m-%d %H:%M") }}{% endif %}

{% endfor %}

---

*Generated by sluice. {{ stats.items_out }} items, {{ stats.llm_calls }} LLM calls, ${{ "%.4f"|format(stats.est_cost_usd) }}.*
```

- [ ] **Step 2: Author full example pipeline**

```toml
# configs/pipelines/ai_news.toml.example
id = "ai_news"
description = "Daily AI news digest"
enabled = true
cron = "0 8 * * *"
timezone = "Asia/Shanghai"
window = "24h"

[[sources]]
type = "rss"
url  = "https://openai.com/blog/rss"
tag  = "ai"

[[sources]]
type = "rss"
url  = "https://www.anthropic.com/news/rss.xml"
tag  = "ai"

[[stages]]
name = "dedupe"
type = "dedupe"

[[stages]]
name = "fetch_fulltext"
type = "fetcher_apply"
write_field = "fulltext"
skip_if_field_longer_than = 2000

[[stages]]
name = "prefilter"
type = "filter"
mode = "keep_if_all"
rules = [
  { field = "fulltext", op = "min_length", value = 300 },
]

[[stages]]
name = "summarize"
type = "llm_stage"
mode = "per_item"
input_field  = "fulltext"
output_field = "summary"
prompt_file  = "prompts/summarize_zh.md"
model          = "glm/glm-4-flash"
fallback_model = "deepseek/deepseek-v3"   # any openai-compatible
workers     = 8
concurrency = 4
max_input_chars = 20000

[[stages]]
name = "daily_analysis"
type = "llm_stage"
mode = "aggregate"
input_field   = "summary"
output_target = "context.daily_brief"
prompt_file   = "prompts/daily_brief_zh.md"
model          = "deepseek/deepseek-v3"
fallback_model = "openai/gpt-4o"

[[stages]]
name = "render"
type = "render"
template = "templates/daily.md.j2"
output_field = "context.markdown"

[[sinks]]
id = "local"
type = "file_md"
input = "context.markdown"
path  = "./out/ai_news/{run_date}.md"

[[sinks]]
id = "notion_main"
type = "notion"
input = "context.markdown"
parent_id = "env:NOTION_DB_AI_NEWS"
parent_type = "database"
token = "env:NOTION_TOKEN"
title_template = "AI 日报 · {run_date}"
properties = { Tag = "AI", Source = "sluice" }
mode = "upsert"
max_block_chars = 1900
# cover_image deferred to v1.1 (notionify v3 has no cover slot)
```

- [ ] **Step 3: Commit**

```bash
git add prompts/ templates/ configs/pipelines/ai_news.toml.example
git commit -m "docs: example prompts, render template, full pipeline TOML"
```

### Task 12.2: End-to-end smoke test with mocked LLM

**Files:**
- Create: `tests/integration/test_smoke.py`

- [ ] **Step 1: Write the test**

```python
# tests/integration/test_smoke.py
import pytest, textwrap, httpx, respx
from datetime import datetime, timezone
from pathlib import Path
from sluice.loader import load_all
from sluice.runner import run_pipeline

import sluice.sources.rss  # noqa
import sluice.fetchers.trafilatura_fetcher  # noqa
import sluice.processors.dedupe  # noqa
import sluice.processors.filter  # noqa
import sluice.processors.fetcher_apply  # noqa
import sluice.processors.llm_stage  # noqa
import sluice.processors.render  # noqa
import sluice.sinks.file_md  # noqa

RSS = """<?xml version='1.0'?><rss><channel>
<item><title>OpenAI GPT-X</title><link>https://o.example/x</link>
<guid>g-x</guid><pubDate>Mon, 28 Apr 2026 06:00:00 +0000</pubDate></item>
<item><title>Anthropic Claude-Y</title><link>https://a.example/y</link>
<guid>g-y</guid><pubDate>Mon, 28 Apr 2026 04:00:00 +0000</pubDate></item>
</channel></rss>"""

ART = "<html><body><article>" + ("This is a long article about AI. " * 40) + "</article></body></html>"

@pytest.mark.asyncio
async def test_full_pipeline_with_mocked_llm(tmp_path, monkeypatch):
    monkeypatch.setenv("K", "v")

    cfg = tmp_path / "configs"; (cfg / "pipelines").mkdir(parents=True)
    (cfg / "sluice.toml").write_text(textwrap.dedent(f"""
        [state]
        db_path = "{tmp_path}/d.db"
        [runtime]
        timezone="UTC"
        default_cron="0 8 * * *"
        prefect_api_url="http://x"
        [fetcher]
        chain = ["trafilatura"]
        min_chars = 50
        on_all_failed = "skip"
        [fetchers.trafilatura]
        type = "trafilatura"
        timeout = 10
        [fetcher.cache]
        enabled = false
        ttl = "1d"
    """))
    (cfg / "providers.toml").write_text(textwrap.dedent("""
        [[providers]]
        name = "n"
        type = "openai_compatible"
        [[providers.base]]
        url = "https://llm.example"
        weight = 1
        key = [{ value = "env:K", weight = 1 }]
        [[providers.models]]
        model_name = "m"
        input_price_per_1k = 0.0001
        output_price_per_1k = 0.0001
    """))
    sp = tmp_path / "sum.md"; sp.write_text("S: {{ item.fulltext }}")
    dp = tmp_path / "daily.md"; dp.write_text("Brief({{ items|length }})")
    tp = tmp_path / "tpl.j2"; tp.write_text(
        "# {{ run_date }}\n{{ context.daily_brief }}\n"
        "{% for it in items %}- {{ it.summary }}\n{% endfor %}")
    (cfg / "pipelines" / "p.toml").write_text(textwrap.dedent(f"""
        id = "p"
        window = "24h"
        timezone = "UTC"
        [[sources]]
        type = "rss"
        url = "https://feed.example/rss"
        [[stages]]
        name = "d"; type = "dedupe"
        [[stages]]
        name = "f"; type = "fetcher_apply"; write_field = "fulltext"
        [[stages]]
        name = "s"; type = "llm_stage"; mode = "per_item"
        input_field = "fulltext"; output_field = "summary"
        prompt_file = "{sp}"
        model = "n/m"; workers = 2
        [[stages]]
        name = "a"; type = "llm_stage"; mode = "aggregate"
        input_field = "summary"; output_target = "context.daily_brief"
        prompt_file = "{dp}"
        model = "n/m"
        [[stages]]
        name = "r"; type = "render"
        template = "{tp}"; output_field = "context.markdown"
        [[sinks]]
        id = "out"; type = "file_md"
        input = "context.markdown"; path = "{tmp_path}/out_{{{{ run_date }}}}.md"
    """))

    bundle = load_all(cfg)
    fake_now = datetime(2026, 4, 28, 12, tzinfo=timezone.utc)
    with respx.mock() as r:
        r.get("https://feed.example/rss").mock(
            return_value=httpx.Response(200, text=RSS))
        r.get("https://o.example/x").mock(return_value=httpx.Response(200, text=ART))
        r.get("https://a.example/y").mock(return_value=httpx.Response(200, text=ART))
        # 2 per_item summarize calls + 1 aggregate brief
        r.post("https://llm.example/chat/completions").mock(side_effect=[
            httpx.Response(200, json={
                "choices": [{"message": {"content": "Summary X"}}],
                "usage": {"prompt_tokens": 100, "completion_tokens": 10}}),
            httpx.Response(200, json={
                "choices": [{"message": {"content": "Summary Y"}}],
                "usage": {"prompt_tokens": 100, "completion_tokens": 10}}),
            httpx.Response(200, json={
                "choices": [{"message": {"content": "TODAY'S BRIEF"}}],
                "usage": {"prompt_tokens": 200, "completion_tokens": 20}}),
        ])
        result = await run_pipeline(bundle, pipeline_id="p", now=fake_now)

    assert result.status == "success" and result.items_out == 2
    out = (tmp_path / "out_2026-04-28.md").read_text()
    assert "TODAY'S BRIEF" in out
    assert "Summary X" in out and "Summary Y" in out
```

- [ ] **Step 2: Run + Commit**

```bash
pytest tests/integration/test_smoke.py -v
git add tests/integration/__init__.py tests/integration/test_smoke.py
git commit -m "test: end-to-end smoke test with mocked RSS + LLM + sinks"
```

### Task 12.3: Coverage check

- [ ] **Step 1: Run full suite with coverage gate**

```bash
pytest --cov=sluice --cov-fail-under=80
```

If under 80%, write additional unit tests for any uncovered branches before proceeding.

- [ ] **Step 2: Commit any added tests**

```bash
git add tests/
git commit -m "test: raise coverage to >=80%"
```

---

## Self-Review Checklist (run after writing code)

- [ ] Every spec section has at least one task implementing it
- [ ] All six Processor types are implemented and tested
- [ ] All four Fetcher types implemented; chain logic tested for cache hit / fall-through / on_all_failed both modes
- [ ] LLM provider chain tested for success, fallthrough, exhaustion
- [ ] Idempotency tests for `upsert` and `create_once`; "two distinct external_ids" test for `create_new`
- [ ] failed_items lifecycle: record → requeue → resolved AND dead-letter after max_retries
- [ ] dedupe excludes failed/dead_letter rows; requeued items merged AFTER dedupe
- [ ] Window math: lookback overlap default and min-1h floor tested
- [ ] item_key cascade: guid → url → title+date tested
- [ ] Backpressure: max_items_per_run trim with both overflow policies (add tests if missed)
- [ ] CLI: validate, run, list, failures, deploy
- [ ] **(review-driven)** per-item LLM failure → recorded to failed_items, item dropped, rest of stage continues
- [ ] **(review-driven)** budget preflight blocks LLM call before exceeding cap (BudgetExceeded raised, llm.calls == 0)
- [ ] **(review-driven)** requeued failed item that succeeds in current run is marked `resolved`
- [ ] **(review-driven)** backpressure trim happens after dedupe, not before
- [ ] **(review-driven)** `--dry-run` skips external side effects: no sink emit, no seen_items insert; run_log still records the dry run
- [ ] **(review-driven)** Notion sink uses adapter Protocol; FakeAdapter in tests, DefaultNotionifyAdapter wired to actual notionify API after inspection
