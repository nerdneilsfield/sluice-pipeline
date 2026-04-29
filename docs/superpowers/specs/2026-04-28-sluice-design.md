# Sluice — Code-native, configurable RSS-to-Notion pipeline

**Status:** Approved design (brainstorm complete, revised after review)
**Date:** 2026-04-28
**Owner:** dengqi

## Name

**Sluice** = a water gate that controls flow. The project controls the flow of
information: choose which sources let water in, what cadence to open the gate,
which processing channels the water passes through, and which downstream
reservoirs it ends up in. The CLI reads naturally: `sluice run daily_digest`.

## Goal

Replace n8n with a code-native, fully configurable agent that:

1. Pulls RSS (and later other sources) on a daily schedule.
2. Fetches full article text when feeds only ship summaries.
3. Per-item LLM summarization, then a single aggregate LLM "daily analysis".
4. Renders a Markdown digest and uploads it to Notion via the existing
   `notionify` package.
5. Designed as the "code version of n8n": every input, processor, and sink is
   a registered plugin; pipelines are declared in TOML.

## Scope

**v1 (this design):**
- RSS source.
- Fetcher chain: trafilatura → firecrawl → jina_reader (crawl4ai available
  but not in default chain).
- Six Processor types: `dedupe`, `filter`, `llm_stage`, `render`,
  `field_filter`, `fetcher_apply`.
- Sinks: `file_md`, `notion` (via `notionify`).
- Local SQLite: `seen_items`, `failed_items`, `sink_emissions`, `run_log`, `url_cache`.
- Self-hosted Prefect for scheduling and observability.
- Multi-pipeline (each TOML file = one pipeline with its own cron).
- Backpressure / cost caps per run.
- Idempotency for external sinks (Notion upsert via stored page_id).

**v2 (out of scope):**
- IMAP / email source.
- Email HTML sink (subscribers).
- GitHub repo sink (push markdown → trigger build).
- RAG over historical summaries.

## Architecture

```
                ┌─────────────────────────────────────────────┐
                │           Prefect server (self-hosted)      │
                │  cron deployment per pipeline → flow run    │
                └────────────────┬────────────────────────────┘
                                 │
                  ┌──────────────▼──────────────┐
                  │     pipeline flow (DAG)     │
                  └──────────────┬──────────────┘
                                 │
   ┌───────────┐  ┌───────────┐  │  ┌────────────┐  ┌────────────┐  ┌──────┐
   │ Source(s) │─▶│ Dedupe    │─▶│─▶│ Fetcher    │─▶│ Summarize  │─▶│ Agg  │
   │ rss       │  │ (sqlite)  │  │  │ chain      │  │ LLM (fan)  │  │      │
   │ (imap→v2) │  │           │  │  │            │  │            │  │      │
   └───────────┘  └───────────┘  │  └────────────┘  └────────────┘  └──┬───┘
                                 │                                      │
                                 │            ┌─────────────────────────▼───┐
                                 │            │   DailyAnalysis LLM (1×)    │
                                 │            └─────────────┬───────────────┘
                                 │                          │
                                 │                  ┌───────▼────────┐
                                 │                  │  Render MD     │
                                 │                  └───────┬────────┘
                                 │                          │
                                 │            ┌─────────────┴───────────┐
                                 │            ▼                         ▼
                                 │      ┌──────────┐              ┌──────────┐
                                 │      │ Notion   │              │ file_md  │
                                 │      │(notionify)│             │          │
                                 │      └────┬─────┘              └──────────┘
                                 │           │
                                 │           ▼
                                 └─▶ commit_run() — sink_emissions + seen_items
```

## Plugin Model — five Protocols

| Plugin | Protocol | v1 implementations |
|---|---|---|
| `Source` | `fetch(window) -> AsyncIterator[Item]` | `rss` |
| `Fetcher` | `extract(url) -> str` (markdown) | `trafilatura`, `firecrawl`, `jina_reader`, `crawl4ai` |
| `Processor` | `process(ctx) -> ctx` | see below |
| `Sink` | `emit(ctx) -> SinkResult` | `file_md`, `notion` |
| `LLMProvider` | `chat(messages, model) -> str` | OpenAI-compatible client + Claude client |

Plugins register themselves via decorators (`@register_source("rss")`).
Adding a new plugin = one file + one decorator. The framework does not
need to be modified.

### The six Processor types

| Type | Purpose |
|---|---|
| `dedupe` | Drop items already in `seen_items` for this pipeline. |
| `fetcher_apply` | Walk the fetcher chain to populate `item.fulltext`. |
| `filter` | Rule-based keep/drop over any item field (cheap, no LLM). |
| `field_filter` | Trim or drop fields on items (e.g. truncate `fulltext` to N chars before passing to an LLM, drop `raw_content` after `fulltext` is populated). Different from `filter`: this mutates fields, never drops items. |
| `llm_stage` | Configurable LLM call, per-item or aggregate. |
| `render` | Jinja2 template → markdown into `context.<key>`. Template receives a fixed context object (see Render Context below). |

## Item Contract

```python
@dataclass
class Item:
    # Provenance
    source_id: str                  # which [[sources]] block produced it
    pipeline_id: str
    # Identity
    guid: str | None                # RSS <guid> if present
    url: str                        # link to article (canonicalised: lowercased
                                    #   scheme/host, fragment stripped, common
                                    #   tracking params removed: utm_*, fbclid,
                                    #   gclid, ref, ref_src, mc_cid, mc_eid)
    # Content
    title: str
    published_at: datetime | None   # tz-aware; None if feed gave nothing
    raw_summary: str | None         # RSS <description> / <content:encoded>
    fulltext: str | None = None     # populated by fetcher_apply
    # Attachments / media
    attachments: list[Attachment] = field(default_factory=list)  # RSS enclosures
    # Stage outputs (well-known by convention; stages may write any name)
    summary: str | None = None
    extras: dict[str, Any] = field(default_factory=dict)
    # Conventions for extras (not enforced, but Sinks read them):
    #   extras['cover_image']    str URL — Notion sets as page cover
    #   extras['author']         str
    #   extras['relevance']      float (e.g. from a rate llm_stage)
    # Tags from source config
    tags: list[str] = field(default_factory=list)

    def get(self, path: str) -> Any:
        """Dot path: 'fulltext', 'extras.relevance', 'tags.0'."""

@dataclass
class Attachment:
    url: str
    mime_type: str | None = None
    length: int | None = None       # bytes, if known from RSS enclosure
```

**Field naming rule:** stage `output_field` may name any of the dataclass
fields above (`summary`, `fulltext`) **or** a key under `extras.*`. Names
without a dot go to `extras` if not a dataclass field. `filter`/`render` use
the same dot-path resolver via `Item.get()`.

## Window Semantics (Source-level time bounds)

```toml
window = "24h"            # nominal window — what the user thinks of as "today"
lookback_overlap = "4h"   # extra lookback to catch delayed RSS publishing
```

- Pipeline `timezone` (default `Asia/Shanghai`) determines "today" boundaries.
- `Source.fetch(window)` is given `window_start = now - (window + lookback_overlap)`,
  `window_end = now`. Default overlap = `max(1h, window * 0.2)`.
- Items with `published_at = None` are **included** in the window (RSS feeds
  occasionally omit it; better to over-include and let dedupe handle it).
- If item-cap trimming needs to order `published_at = None`, those entries
  sort as oldest so timestamped feed items remain stable under caps.
- The `dedupe` processor absorbs the overlap noise: anything seen in a prior
  run is dropped.
- "Future-dated" items (`published_at > now + 1h`) are **dropped** as feed
  errors and logged.
- RSS feed URLs themselves are operator-controlled config and are not SSRF
  guarded in v1. Article/fetcher URLs are guarded because they originate from
  feed content.

## Stage Abstraction

Stages are an **ordered list** declared per pipeline. Each stage is a
Processor. The two key processors:

### `llm_stage` — fully configurable LLM steps

Two modes:
- `per_item`: fan out, one LLM call per item; result written to
  `item.<output_field>`.
- `aggregate`: collect all items into one prompt; result written to
  `context.<output_target>`.

Both `summarize` and `daily_analysis` are just `llm_stage` instances. Users
can add their own stages (`classify`, `rate`, `translate`, ...) by writing
a prompt file and adding a TOML block. **No Python changes required for
new stages.**

Each `llm_stage` carries the four-tier model chain from
`ai-deepresearch-flow`: `model / retry_model / fallback_model /
fallback_model_2`, each with its own `*_workers` and `*_concurrency`.

**Output parsing (NEW)**:
```toml
output_parser = "text"                  # default: raw string into output field
# or:
output_parser = "json"
output_schema = "schemas/relevance.json"  # optional JSON schema for validation
on_parse_error = "skip"                 # "fail" | "skip" | "default"
on_parse_error_default = { relevance = 0 }
```
`filter` operators like `gte` require the upstream `llm_stage` to emit a
parsed numeric field. Without an explicit `output_parser = "json"`, the
output is a string and `filter` numeric ops will reject it at validation
time.

**Input length control (NEW)**:
```toml
max_input_chars = 20000
truncate_strategy = "head_tail"   # "head_tail" | "head" | "error"
```
Names follow `ai-deepresearch-flow`. `error` makes the stage fail-fast
rather than silently truncate.

### `render` — Jinja2 → markdown

Template receives this exact context dict:
```python
{
    "items":       list[Item],          # items that survived all prior stages
    "context":     dict[str, Any],      # everything written by aggregate
                                        #   llm_stages (e.g. context.daily_brief)
    "pipeline_id": str,
    "run_key":     str,                 # "ai_news/2026-04-28"
    "run_date":    str,                 # ISO date in pipeline tz
    "stats": {
        "items_in":     int,            # after Source.fetch
        "items_out":    int,            # after all stages
        "llm_calls":    int,
        "est_cost_usd": float,
    },
}
```
Templates use `{{ pipeline_id }}`, `{{ run_date }}`, `{{ context.daily_brief }}`,
`{% for it in items %}{{ it.summary }}{% endfor %}`, etc.

### `filter` — rule-based, cheap, deterministic

Predicates over any item field, including fields written by upstream LLM
stages. "LLM-based filtering" = upstream `llm_stage` writes a score, then
`filter` drops below threshold.

Operators (kept lean): `gt/gte/lt/lte/eq`, `matches/not_matches`,
`contains/not_contains`, `in/not_in`, `min_length/max_length`,
`newer_than/older_than`, `exists/not_exists`.

`mode`: `keep_if_all | keep_if_any | drop_if_all | drop_if_any`.

## Fetcher Chain

```toml
[fetcher]
chain = ["trafilatura", "firecrawl", "jina_reader"]
min_chars = 500            # under this = treated as failure, fall through
on_all_failed = "skip"     # "skip" | "continue_empty"
                           # skip: drop item, record in failed_items
                           # continue_empty: keep item with fulltext=None,
                           #   downstream stages decide (e.g. filter on
                           #   exists)

[fetcher.cache]
enabled = true             # cache successful extractions to avoid re-hitting
                           # firecrawl/jina on Prefect retries
ttl = "7d"                 # entries past this age are re-fetched
```

Cache is a `url_cache` table keyed by `sha256(canonical_url)`; a successful
fetch writes `(url_hash, fetcher_name, markdown, fetched_at)`. `fetcher_apply`
checks the cache before walking the chain. Failed extractions are **not**
cached (they belong in `failed_items`).

Rationale: ~90% of RSS articles are static HTML — `trafilatura` succeeds
locally with zero network cost. `firecrawl` (self-hosted) handles
JS-rendered / paywalled content. `jina_reader` is a public-network safety
net.

`ScrapeGraphAI` is **not** included: it solves structured-JSON extraction,
which we do not need; using it would mean an extra LLM call per page on top
of our `summarize` stage (5× cost, no benefit for prose articles).

## LLM Provider Pool

Direct port of `ai-deepresearch-flow`'s pattern:

- `[[providers]]` blocks: `name`, `type` (`openai_compatible` | `claude`),
  weighted `base[]` URLs, weighted `key[]` per base, `models[]`.
- Per-key `quota_duration`, `reset_time`, `quota_error_tokens` for
  automatic cool-down on quota errors.
- Optional `active_windows` per base for time-window routing.
- Stages reference models as `"provider_name/model_name"`.
- **Cost fields per `models[]` entry** — used by the cost-cap projector:
  ```toml
  models = [
    { model_name = "glm-4-flash",
      input_price_per_1k  = 0.0001,   # USD per 1k input tokens
      output_price_per_1k = 0.0001 },
  ]
  ```
  Manually configured next to the model: prices vary by aggregator
  (OpenRouter vs vendor direct), automatic fetch is intentionally not done.

`LLMClient.chat()` walks the four-tier chain, falling through on
`RateLimitError | QuotaExhausted | NetworkError`. `ProviderPool.acquire()`
selects URL+key by weight, skips cooled-down keys, returns an endpoint with
a semaphore for concurrency control.

## Backpressure & Cost Caps (per pipeline)

Two semantically distinct cap families:

```toml
[pipeline.limits]
# Item-count cap — enforced before any LLM work: after dedupe when present,
# otherwise immediately after Source.fetch/requeue merge.
# Excess items are dropped per item_overflow_policy.
max_items_per_run = 50            # default tuned for Notion upsert speed
                                  # (~50 items keeps a daily upsert under
                                  # ~30s in practice)
item_overflow_policy = "drop_oldest"   # "drop_oldest" | "drop_newest"

# Cost / call caps — enforced before each llm_stage. Projected = estimated
# tokens × per-model price (configured in providers.toml). These are HARD
# stops: the pipeline FAILS rather than silently skipping a required stage.
max_llm_calls_per_run = 500
max_estimated_cost_usd = 5.0
```

Order of enforcement:
1. Count remaining items after `Source.fetch()` and requeue merge. If a
   `dedupe` stage exists, enforce after dedupe; otherwise enforce before the
   first configured processor. If above `max_items_per_run`, apply
   `item_overflow_policy`.
2. Before each `llm_stage`, check projected calls + cost against
   `max_llm_calls_per_run` / `max_estimated_cost_usd`. If projected total
   would exceed either cap, the run **fails** (no silent skip — skipping
   `summarize` would corrupt downstream `daily_analysis`).
3. The pipeline `run_log` records actual calls + estimated cost.

`LLMClient` itself does no enforcement; caps are policy at pipeline level.

## State (SQLite)

Path configurable in global `[state] db_path`; per-pipeline override
allowed. Driver: `aiosqlite`, no ORM. Schema migrations via
`PRAGMA user_version` + ordered SQL files.

Timestamps stored as **ISO 8601 strings with timezone**
(`'2026-04-28T08:00:00+08:00'`) in `TEXT` columns. SQLite has no native
TIMESTAMP type; we choose readability over `INTEGER` epoch.

```sql
-- v1 schema

CREATE TABLE seen_items (
  pipeline_id TEXT NOT NULL,
  item_key    TEXT NOT NULL,
  url         TEXT,
  title       TEXT,
  published_at TEXT,                   -- ISO 8601 with tz
  summary     TEXT,                    -- kept for future RAG corpus
  seen_at     TEXT NOT NULL,
  PRIMARY KEY (pipeline_id, item_key)
);
CREATE INDEX idx_seen_published ON seen_items(pipeline_id, published_at);

CREATE TABLE failed_items (
  pipeline_id TEXT NOT NULL,
  item_key    TEXT NOT NULL,
  url         TEXT,
  stage       TEXT NOT NULL,           -- which stage failed
  error_class TEXT NOT NULL,
  error_msg   TEXT NOT NULL,
  attempts    INTEGER NOT NULL DEFAULT 1,
  status      TEXT NOT NULL,           -- "failed" | "dead_letter" | "resolved"
  item_json   TEXT NOT NULL,           -- full Item payload (JSON) so retry
                                       --   does not depend on RSS still
                                       --   surfacing the item next window
  first_failed_at TEXT NOT NULL,
  last_failed_at  TEXT NOT NULL,
  PRIMARY KEY (pipeline_id, item_key)
);
CREATE INDEX idx_failed_status ON failed_items(pipeline_id, status);

CREATE TABLE sink_emissions (
  pipeline_id TEXT NOT NULL,
  run_key     TEXT NOT NULL,           -- e.g. "ai_news/2026-04-28"
  sink_id     TEXT NOT NULL,           -- stable user-assigned id from TOML
                                       --   (not type), so two file_md sinks
                                       --   in one pipeline don't collide
  sink_type   TEXT NOT NULL,           -- "notion" / "file_md"
  external_id TEXT,                    -- Notion page_id, file path, etc.
  emitted_at  TEXT NOT NULL,
  PRIMARY KEY (pipeline_id, run_key, sink_id)
);

CREATE TABLE url_cache (
  url_hash    TEXT PRIMARY KEY,        -- sha256(canonical_url)
  url         TEXT NOT NULL,
  fetcher     TEXT NOT NULL,           -- which fetcher succeeded
  markdown    TEXT NOT NULL,
  fetched_at  TEXT NOT NULL,
  expires_at  TEXT NOT NULL
);

CREATE TABLE run_log (
  pipeline_id TEXT NOT NULL,
  run_key     TEXT NOT NULL,
  started_at  TEXT NOT NULL,
  finished_at TEXT,
  status      TEXT NOT NULL,           -- running / success / failed / partial
  items_in    INTEGER,
  items_out   INTEGER,
  llm_calls   INTEGER,
  est_cost_usd REAL,
  error_msg   TEXT,
  PRIMARY KEY (pipeline_id, run_key)
);
```

### `item_key` resolution

Cascading fallback (not configurable in v1):
1. If `item.guid` is present and non-empty after strip, use `guid`.
2. Else use `sha256(canonical_url)`.
3. Else use `sha256(title + published_at_iso)` (`published_at_iso` = empty string if missing).

A `key_strategy = "auto" | "guid_only" | "url_only"` setting on the
`dedupe` stage will be added when a real need surfaces.

### `run_key`

`run_key = "{pipeline_id}/{run_date_iso}"` where `run_date_iso` is the date
in the pipeline's timezone (e.g. `ai_news/2026-04-28`). This is what makes
sink emissions idempotent across Prefect retries within the same logical
day.

A `run_key_template` setting may be exposed in v2.

### Commit-after-success

```
1. Run all stages, collect ctx.items (survivors) and ctx.context.
2. For each sink in pipeline.sinks:
     if sink.mode == "create_new":
         external_id = sink.create(ctx)              # intentionally non-idempotent
         sink_emissions.insert(pipeline_id, run_key,
                               sink.id, sink.type, external_id)
     else:
         prior = sink_emissions.lookup(pipeline_id, run_key, sink.id)
         if prior:
             sink.update(prior.external_id, ctx)     # upsert mode
             # create_once mode: no-op when prior exists
         else:
             external_id = sink.create(ctx)
             sink_emissions.insert(pipeline_id, run_key,
                                   sink.id, sink.type, external_id)
3. After ALL sinks succeed:
     seen_items.insert_batch(ctx.items)
     for item in ctx.items_resolved_from_failures:
         failed_items.update(status="resolved", item_key=item.key)
     run_log.mark_success()
```

`sink.id` is the user-assigned id from TOML (required). `sink.create()` /
`sink.update()` are base-class methods on `Sink`; the public protocol entry
point remains `emit(ctx) -> SinkResult`, which the base class dispatches
to create/update based on `sink_emissions` lookup. This is the contract
that makes external side effects safe under retry.

## Sinks

### Common sink config
```toml
[[sinks]]
id    = "notion_main"     # REQUIRED. Stable, unique within the pipeline.
                          # Used as sink_emissions.sink_id so two sinks
                          # of the same type don't collide on retry.
type  = "..."
emit_on_empty = false     # default: skip only when both items is empty and
                          # the sink input payload is missing/empty.
                          # Aggregate outputs may still emit with no items.
```

### `file_md`
```toml
[[sinks]]
id    = "local_archive"
type  = "file_md"
input = "context.markdown"
path  = "./out/ai_news/{run_date}.md"  # deterministic; overwrite-safe
```

### `notion` (wraps `notionify`)
```toml
[[sinks]]
id          = "notion_main"
type        = "notion"
input       = "context.markdown"
parent_id   = "env:NOTION_DB_AI_NEWS"
parent_type = "database"               # "database" | "page"
token       = "env:NOTION_TOKEN"
title_template = "AI 新闻日报 · {run_date}"
properties = { Tag = "AI", Source = "sluice" }
mode = "upsert"                        # "upsert" | "create_once" | "create_new"
                                       # upsert: lookup sink_emissions →
                                       #   missing = create; present = replace
                                       #   page content (delete + write blocks)
                                       # create_once: create only if no prior
                                       #   sink_emission row exists; no-op
                                       #   on retry. Useful for "publish once"
                                       #   semantics.
                                       # create_new: ALWAYS create a new page,
                                       #   ignore sink_emissions on lookup.
                                       #   This is the one INTENTIONALLY
                                       #   non-idempotent mode; choose only
                                       #   when "one Notion page per run, even
                                       #   on manual re-run" is the desired
                                       #   behavior. Excluded from the
                                       #   no-duplicates idempotency tests.
                                       #   sink_emissions keeps only the latest
                                       #   external_id for this run/sink; audit
                                       #   of every manual re-run page is v2.
max_block_chars = 1900                 # Notion API caps blocks at 2000 chars.
                                       # The sink chunks long markdown lines
                                       # into multiple blocks at this size.
                                       # The notionify package may already do
                                       # this; sluice still passes the cap so
                                       # behavior is explicit.
cover_image_from = "extras.cover_image"  # optional: dot-path on items[0] (or
                                         # an aggregate ctx key) used as the
                                         # Notion page cover URL. Skipped if
                                         # missing/None.
```

## Configuration

TOML throughout (matches `ai-deepresearch-flow`).

```
sluice/
├── configs/
│   ├── sluice.toml             # global: state, runtime, default fetcher chain
│   ├── providers.toml          # LLM provider pool
│   └── pipelines/
│       ├── ai_news.toml
│       └── tech_blogs.toml
├── prompts/
│   ├── summarize_zh.md         # Jinja2; {{ item.title }}, {{ item.fulltext }}
│   └── daily_brief_zh.md       # Jinja2; {{ items }} list
├── data/
│   └── sluice.db
└── out/                        # file_md sink default destination
```

Discriminated unions (`type = "llm_stage" | "filter" | ...`) parsed by
Pydantic v2 via `Annotated[Union[...], Field(discriminator="type")]`.

## Code Layout

```
sluice/
├── cli.py                    # typer CLI: run / list / deploy / validate
├── config.py                 # Pydantic v2 models for all TOML
├── context.py                # PipelineContext (dataclass)
├── flow.py                   # Prefect flow assembler: TOML → DAG
├── registry.py               # plugin registries (type str → class)
├── core/
│   ├── item.py               # Item dataclass
│   ├── protocols.py          # five Protocols
│   └── errors.py
├── sources/{rss.py, ...}
├── fetchers/
│   ├── chain.py              # FetcherChain logic + on_all_failed
│   ├── trafilatura_fetcher.py, firecrawl.py, jina_reader.py
├── processors/
│   ├── dedupe.py, filter.py, llm_stage.py, render.py,
│   ├── field_filter.py, fetcher_apply.py
├── sinks/
│   ├── base.py               # SinkResult dataclass + idempotency helper
│   ├── file_md.py, notion.py (notionify wrapper)
├── llm/
│   ├── provider.py, client.py, pool.py, budget.py
└── state/
    ├── db.py                 # aiosqlite + migrations
    ├── seen.py               # SeenStore (seen_items)
    ├── failures.py           # FailureStore (failed_items, lifecycle)
    ├── emissions.py          # EmissionStore (sink_emissions)
    ├── cache.py              # UrlCacheStore (url_cache, TTL eviction)
    └── run_log.py
```

**Pydantic vs dataclass:**
- Pydantic v2 → all `config.py` schemas (validation + discriminated unions).
- `@dataclass` → `Item`, `PipelineContext` (hot path, mutated each stage).
- **No ORM** — small fixed schema, fixed query patterns; ORM is over-engineering.

## CLI

```
sluice run <pipeline_id>            # run once
sluice run <pipeline_id> --dry-run  # no DB writes, no sink emits
sluice list                         # list pipelines and their next runs
sluice deploy                       # register all enabled pipelines as
                                    #   prefect deployments
sluice validate                     # validate all TOML files
sluice failures <pipeline_id>       # list failed_items, with --retry to re-queue
```

## Error Handling

- **Single item failure** in any stage: logged + recorded to `failed_items`,
  pipeline continues with remaining items.
- **Single fetcher failure**: fall through to next in chain. All-failed
  → `on_all_failed` policy.
- **Single LLM call failure**: tier-fallthrough (retry → fallback →
  fallback_2). All tiers exhausted → item goes to `failed_items`.
- **Sink failure**: pipeline run marked `failed`. `seen_items` not
  committed. Already-emitted sinks recorded in `sink_emissions` so the
  retry uses upsert/update path instead of duplicating.
- **Pipeline-level retry**: Prefect retry (exponential backoff, 3 attempts).
  Idempotency comes from `run_key + sink_emissions`.

### Failed item retry policy
```toml
[pipeline.failures]
retry_failed = true
max_retries = 3                       # after this, item is dead-lettered
retry_backoff = "next_run"            # only retried on next scheduled run,
                                      # not within current run
```

Lifecycle:
- A `status = "failed"` item is re-queued at the start of the next run,
  reconstructed from `item_json`, merged into `ctx.items` before processors,
  and tracked internally as a retry key. It passes through ordinary processors
  before `dedupe`; `dedupe` preserves retry keys even though the `failed_items`
  row still exists.
- `sluice failures --retry ITEM_KEY` moves a dead-letter item back to
  `status = "failed"` and resets `attempts = 0`, allowing an intentional
  manual retry cycle.
- The `failed`/`dead_letter` gate inside `dedupe` only suppresses items
  freshly produced by `Source.fetch` whose key matches an existing failed
  row — preventing the RSS source from continually re-introducing them.
- After `max_retries` exhausted → `status = "dead_letter"`. `dedupe`
  treats these as already-seen so the next RSS poll does not re-introduce
  them.
- Successful re-processing → row updated to `status = "resolved"` and the
  normal `seen_items` row is written. Resolved rows are kept for audit
  (cleaned by a `sluice gc --older-than 90d` command, deferred to v1.1).
- `sluice failures <pipeline_id> --retry <item_key>` forces a single
  dead-letter back to `failed` for manual re-attempt.

## Testing

- **Unit:** each Protocol implementation tested with `Fake*` doubles for
  upstream/downstream.
- **Integration:** in-memory SQLite + httpx mock → end-to-end pipeline run.
- **Contract:** every plugin registered in the registry gets a smoke test
  driven by a sample TOML fixture (dry-run).
- **Idempotency tests:** explicit "run twice in same `run_key`" tests for
  every sink (mode `upsert` and `create_once`), asserting no duplicates.
  `create_new` mode is exempted — it is intentionally non-idempotent and
  is tested instead with "run twice → exactly two distinct external_ids".
- **Coverage target:** 80%+ (project-wide rule from
  `~/.claude/rules/common/testing.md`).

## Non-goals (explicit)

- **Not** a general-purpose workflow engine — Prefect already is one.
  Sluice is the domain-specific assembly + plugin protocols on top.
- **Not** a scraping framework — Sluice delegates to firecrawl/crawl4ai for
  hard cases.
- **Not** an LLM framework — no LangChain, no LlamaIndex, no LangGraph.
  The pipeline is a linear DAG; cycles/agents are out of scope until
  explicitly needed.
- **Not** trying to replace n8n's UI — Prefect's UI provides run history,
  per-task observability, and re-run controls, which is what we actually
  use n8n for.

## Decisions Locked (was Open Questions)

These were open in earlier drafts; locked here so the implementation plan
does not have to re-debate them:

- `run_key` is hard-coded `{pipeline_id}/{run_date}`. Hourly / sub-daily
  pipelines are out of scope for v1.
- `failed_items.max_retries` default = **3**, then `dead_letter`.
- `lookback_overlap` default = `max(1h, window * 0.2)`.
- `llm_stage.output_parser` default = `"text"`. Stages needing numeric/bool
  fields must explicitly set `"json"` and supply `output_schema`.
- `field_filter` v1 ops = `truncate(field, n)` and `drop(field)` only.
- Per-model `input_price_per_1k` / `output_price_per_1k` configured
  manually in `providers.toml`; no automatic price fetching.
- `max_items_per_run` default = **50** (Notion upsert latency budget).

## Open Questions (still to decide)

### v1 (during implementation)
- Does `notionify` already chunk markdown to ≤2000-char Notion blocks?
  If yes, `max_block_chars` becomes a passthrough; if no, the `notion`
  sink in sluice has to chunk before calling notionify.
- `url_cache.ttl` default — proposal `7d`. Longer means more savings but
  staler content if a feed updates a URL in place. Decide after first week
  of real use.
- `sluice gc` for `failed_items` resolved/dead-lettered rows — needed in
  v1 or can wait?

### v2
- Email subscriber list management (CSV? DB table? mailing-list service?)
- RAG: how to index `seen_items.summary` (sqlite-vss? chroma? pgvector?)
- Notion property mapping when one pipeline writes to multiple databases.
- Hourly / sub-daily pipelines (changes `run_key` semantics).
- Local attachment mirroring (download images / enclosures, re-host to
  bypass hotlink protection).
