<div align="center">

# 🚰 sluice

**Code-native pipelines for information. Wire up RSS, LLMs,
and Notion in plain TOML — think of it as n8n in code form.**

[![PyPI version](https://img.shields.io/pypi/v/sluice-pipeline.svg?color=blue)](https://pypi.org/project/sluice-pipeline/)
[![Python](https://img.shields.io/pypi/pyversions/sluice-pipeline.svg?color=blue)](https://pypi.org/project/sluice-pipeline/)
[![License](https://img.shields.io/github/license/nerdneilsfield/sluice-pipeline.svg)](https://github.com/nerdneilsfield/sluice-pipeline/blob/master/LICENSE)
[![CI](https://img.shields.io/github/actions/workflow/status/nerdneilsfield/sluice-pipeline/ci.yml?branch=master&label=CI)](https://github.com/nerdneilsfield/sluice-pipeline/actions)
[![Coverage](https://img.shields.io/badge/coverage-80%25-brightgreen.svg)](https://github.com/nerdneilsfield/sluice-pipeline)
[![Tests](https://img.shields.io/badge/tests-408%20passing-brightgreen.svg)](https://github.com/nerdneilsfield/sluice-pipeline/actions)
[![Stars](https://img.shields.io/github/stars/nerdneilsfield/sluice-pipeline.svg?style=social)](https://github.com/nerdneilsfield/sluice-pipeline)

[**English**](./README.md) · [**简体中文**](./README_ZH.md) · [PyPI](https://pypi.org/project/sluice-pipeline/) · [GitHub](https://github.com/nerdneilsfield/sluice-pipeline)

</div>

> **slu·ice** /sluːs/ — *a water gate. You decide what flows through.*

That's the metaphor. Pick your sources, set when the gate opens, route through
processing channels, and land everything in downstream reservoirs — except the
water is **information**, and every gate, channel, and reservoir is code you
can read.

```text
            ┌──────────┐    ┌─────────┐    ┌──────────┐    ┌──────────┐
RSS ───▶── │  Source  │──▶│ Stages  │──▶│  Render  │──▶│  Sinks   │──▶ Notion
            └──────────┘    └─────────┘    └──────────┘    └──────────┘
                          dedupe / fetch /             file_md / email
                          summarize / filter /         (pluggable)
                          analyze (LLM)
```

---

## Table of contents

- [Why sluice?](#why-sluice)
- [Quickstart](#quickstart)
- [Concepts](#concepts)
- [Configuration](#configuration)
- [Built-in plugins](#built-in-plugins)
- [Operations & observability](#operations--observability)
- [Docker deployment](#docker-deployment)
- [CI/CD](#cicd)
- [Roadmap](#roadmap)
- [Development](#development)
- [License](#license)

---

## What's new in V1.1

After months of running sluice daily on real feeds, here's what got better:

- **Swappable output channels**: Telegram (MarkdownV2), Feishu (post/text/interactive), Email (fail_fast/best_effort) — all with `sink_delivery_log` audit trail.
- **Attachment mirroring**: `mirror_attachments` downloads images/files to local disk with `file://`, `https://`, or relative URL prefix.
- **Enricher protocol + hn_comments**: pluggable enrichers that augment items with external data (HN comment threads via the HN Firebase API, with official HN site as fallback). Run this stage **before** `summarize` so the LLM can incorporate community discussion.
- **Sub-daily pipelines**: `run_key_template` with `{run_hour}`, `{run_minute}`, `{run_iso}`, `{run_epoch}` for cron intervals under 24h.
- **Ranking stages**: `sort` orders by numeric/string/datetime fields; `limit` can sort, group, and cap with `sort_by` / `group_by` / `per_group_max`.
- **`field_filter` ops**: `lower`, `strip`, `regex_replace` in addition to existing `truncate` / `drop`.
- **New cleanup + scoring stages**: `cross_dedupe`, `html_strip`, `score_tag`, and `summarize_score_tag` help deduplicate across feeds, normalize RSS fields, and combine scoring/tagging/summarization before downstream filtering.
- **Smart fetcher fallback**: `on_all_failed = "use_raw_summary"` gracefully falls back to RSS summary text when all fetchers fail.
- **URL cache size cap**: configurable `max_rows` with LRU eviction — keeps the DB lean.
- **GC command**: `sluice gc` reclaims storage from `failed_items`, `url_cache`, `attachment_mirror` + orphan file cleanup.
- **Observability**: custom Prometheus collector, `sluice stats`, `sluice metrics-server`, `sluice deliveries` audit viewer.
- **Lazy registry**: plugins register via stubs — `pip install sluice-pipeline` (no extras) stays lightweight.

---

## Why sluice?

You track a bunch of RSS feeds. You want **a daily digest in Notion** — each
article auto-fetched, LLM-summarized, bundled into a brief, then pushed to a
Notion database. You want it cheap, observable, and **fully under your control**.

Three approaches you'll look at:

|                            | n8n / Zapier                       | A 200-line Python script               | **sluice**                                                     |
| -------------------------- | ---------------------------------- | -------------------------------------- | -------------------------------------------------------------- |
| Add a new feed             | Click 12 buttons                   | Edit code                              | Add 3 lines to TOML                                            |
| Swap LLM provider          | Hope the integration exists        | Hope you wrote it that way             | Edit `providers.toml`, restart                                 |
| Cost cap per run           | Hard                               | Hand-rolled                            | One-line `max_estimated_cost_usd`                              |
| Failure handling           | "Retry the whole workflow"         | `try: ... except: pass`                | Per-item failed_items lifecycle, dead-letter, `--retry`        |
| Self-hosted observability  | n8n web UI (resource-heavy)         | `print` + grep                         | Rich progress bar, loguru diagnostics, Prefect run history    |
| LLM fallback chain         | Manual branching                   | None                                   | model fallback + long-context routing with key cooldown        |
| Idempotency                | "Did it already run?"              | "Did the script crash midway?"         | `sink_emissions` table, upsert on retry                        |

sluice is **n8n in code**: business logic lives in plain Python and TOML —
no SaaS lock-in, no GUI walls, no opaque webhooks.

---

## Quickstart

### 1. Install

```bash
# The PyPI package is called sluice-pipeline (sluice was taken).
# Your imports and CLI commands stay "sluice" — no rename needed.
pip install sluice-pipeline

# With push-channel sinks (Telegram / Feishu / Email)
pip install "sluice-pipeline[channels]"

# With Prometheus metrics
pip install "sluice-pipeline[metrics]"

# With HN Comments enricher
pip install "sluice-pipeline[enrich-hn]"

# Everything
pip install "sluice-pipeline[all]"
```

> **Heads-up:** Python 3.11+. You'll need a
> [Notion integration token](https://developers.notion.com/docs/create-a-notion-integration)
> and at least one OpenAI-compatible API key (DeepSeek, GLM, OpenAI, OpenRouter — anything
> that speaks the `/v1/chat/completions` protocol works). If you're only using the RSS
> source + file_md sink, no LLM key is needed.

### 2. Project layout

```
my-digest/
├── configs/
│   ├── sluice.toml           # global: state, fetcher chain, runtime
│   ├── providers.toml        # LLM provider pool
│   └── pipelines/
│       └── ai_news.toml      # one pipeline per file
├── prompts/
│   ├── summarize_zh.md       # Jinja2 prompt
│   └── daily_brief_zh.md
└── prompts/ (also contains render templates)
    └── daily.md.j2           # render template
```

### 3. A minimal pipeline

```toml
# configs/pipelines/ai_news.toml
id = "ai_news"
window = "24h"
timezone = "Asia/Shanghai"

[[sources]]
type = "rss"
url  = "https://openai.com/blog/rss"

# Optional on any source: run the same mode/rules filter before stages.
# `content` means the source-time article text, e.g. RSS summary/content.
[sources.filter]
mode = "keep_if_any"
rules = [
  { field = "title",   op = "matches", value = "(?i)gpt|agent|model" },
  { field = "content", op = "matches", value = "(?i)gpt|agent|model" },
]

[[stages]]
name = "dedupe"
type = "dedupe"

[[stages]]
name = "summarize"
type = "llm_stage"
mode = "per_item"
input_field  = "raw_summary"
output_field = "summary"
prompt_file  = "prompts/summarize_zh.md"
model        = "openai/gpt-4o-mini"

[[stages]]
name = "render"
type = "render"
template = "prompts/daily.md.j2"
output_field = "context.markdown"

[[sinks]]
id   = "notion_main"
type = "notion"
input          = "context.markdown"
parent_id      = "env:NOTION_DB_AI_NEWS"
parent_type    = "database"
token          = "env:NOTION_TOKEN"
title_template = "AI Daily · {run_date}"
```

### 4. Run it

```bash
# Dry-run: no DB writes, no sink emits — see what *would* happen
sluice run ai_news --dry-run

# For real
sluice run ai_news

# With detailed diagnostics
sluice run ai_news --verbose --log-file logs/ai_news.jsonl

# Schedule it (registers a Prefect deployment with cron)
sluice deploy
```

That's it — **a complete daily digest pipeline in 30 lines of TOML**.

---

## Concepts

Here's the mental model. The water-gate metaphor maps directly into code —
five plugin **Protocols**, and every pipeline is just a composition of them:

| Protocol      | What it does                            | Built-in implementations                                                    |
| ------------- | --------------------------------------- | --------------------------------------------------------------------------- |
| `Source`      | Bring items into the stream             | `rss`                                                                       |
| `Fetcher`     | Hydrate an article URL → markdown       | `trafilatura`, `crawl4ai`, `firecrawl`, `jina_reader`                       |
| `Processor`   | Transform the stream                    | `dedupe`, `cross_dedupe`, `fetcher_apply`, `html_strip`, `filter`, `field_filter`, `score_tag`, `summarize_score_tag`, `sort`, `llm_stage`, `render`, `limit`, `enrich`, `mirror_attachments` |
| `Sink`        | Push items downstream                   | `file_md`, `notion`, `telegram`, `feishu`, `email`                                                         |
| `LLMProvider` | Talk to an OpenAI-compatible endpoint   | weighted base/key pool with 4-tier fallback chain                           |

> Every plugin registers itself with a single decorator. Adding a new source
> (e.g. IMAP, Telegram, Reddit) is **one Python file plus one line of TOML**.

### The Item model

Items flow through the stages. Each carries provenance, content, and arbitrary
extras written by upstream stages:

```python
@dataclass
class Item:
    source_id: str
    pipeline_id: str
    guid: str | None
    url: str                            # canonicalized (utm_*/fbclid/... stripped)
    title: str
    published_at: datetime | None
    raw_summary: str | None
    fulltext: str | None                # populated by fetcher_apply
    attachments: list[Attachment]       # RSS enclosures
    summary: str | None                 # populated by summarize llm_stage
    extras: dict[str, Any]              # anything else stages write
    tags: list[str]

    def get(self, path: str, default=None):
        """Dot path: 'fulltext', 'extras.relevance', 'tags.0'"""
```

### LLM provider pool — the unfair advantage

This is where sluice earns its keep. A 200-line script can call an LLM, sure.
But can it juggle a pool of weighted providers, cool down quota-exhausted keys
automatically, retry transient failures in place, walk a fallback chain, and
jump oversized prompts to a long-context model — without you writing a single
retry loop?

That's what the provider pool does.

<details>
<summary><b>Show full provider config</b></summary>

```toml
# configs/providers.toml
[[providers]]
name = "openrouter"
type = "openai_compatible"

[[providers.base]]
url    = "https://openrouter.ai/api/v1"
weight = 3

key = [
  { value = "env:OR_KEY_1", weight = 2 },
  { value = "env:OR_KEY_2", weight = 1, quota_duration = 18000, quota_error_tokens = ["exceed", "quota"] },
]
active_windows  = ["00:00-08:00"]   # only use this base off-peak
active_timezone = "Asia/Shanghai"

[[providers.models]]
model_name          = "openai/gpt-4o-mini"
input_price_per_1k  = 0.00015
output_price_per_1k = 0.0006
max_input_tokens    = 32000
max_output_tokens   = 4096

[[providers.models]]
model_name          = "openai/gpt-4o"
input_price_per_1k  = 0.0025
output_price_per_1k = 0.01
max_input_tokens    = 128000
max_output_tokens   = 16384

[[providers]]
name = "ollama"
type = "openai_compatible"
[[providers.base]]
url = "http://localhost:11434/v1"
key = [{ value = "local" }]
[[providers.models]]
model_name = "llama3"
max_input_tokens  = 8192
max_output_tokens = 2048
# free local — no price needed
```

Then in any `llm_stage`:

```toml
model            = "openrouter/openai/gpt-4o-mini"
retry_model      = "openrouter/openai/gpt-4o-mini"   # same tier, retry on transient failures
fallback_model   = "openrouter/google/gemini-flash"   # cheaper backup
fallback_model_2 = "ollama/llama3"                    # last-resort local
long_context_model = "openrouter/openai/gpt-4o"        # large prompt / overflow recovery

same_model_retries = 2
overflow_trim_step_tokens = 100000
long_context_threshold_ratio = 0.8
```

The chain walks main → retry → fallback → fallback_2. Large prompts route
directly to `long_context_model`, and context-overflow errors jump straight
there instead of trying intermediate fallbacks. Each tier has independent
worker/concurrency caps. Quota-exhausted keys are cooled down automatically.
Time-window routing lets you push expensive traffic to off-peak hours.

</details>

---

## Configuration

sluice has three TOML layers:

1. **`sluice.toml`** — state DB, runtime, default fetcher chain
2. **`providers.toml`** — LLM provider pool
3. **`pipelines/<id>.toml`** — one file per pipeline

<details>
<summary><b>Show a full production-grade pipeline TOML</b></summary>

```toml
id = "ai_news"
description = "Daily AI/infra news digest"
enabled = true
cron = "0 8 * * *"
timezone = "Asia/Shanghai"
window = "24h"
lookback_overlap = "4h"

# ─── Backpressure / cost caps ───────────────────────────
[limits]
max_items_per_run    = 50          # tuned for Notion upsert latency
item_overflow_policy = "drop_oldest"
max_llm_calls_per_run     = 500
max_estimated_cost_usd    = 5.0    # hard fail before blowing budget

# ─── Failed-item lifecycle ──────────────────────────────
[failures]
retry_failed  = true
max_retries   = 3                  # then dead-letter
retry_backoff = "next_run"

# ─── Sources (multiple allowed) ─────────────────────────
[[sources]]
type = "rss"
url  = "https://openai.com/blog/rss"
tag  = "ai"

[[sources]]
type = "rss"
url  = "https://www.anthropic.com/news/rss.xml"
tag  = "ai"

# ─── Stages (executed in order) ─────────────────────────
[[stages]]
name = "dedupe"
type = "dedupe"

[[stages]]
name = "fetch_fulltext"
type = "fetcher_apply"
write_field = "fulltext"
skip_if_field_longer_than = 2000   # trust the feed if content is already long

[[stages]]
name = "prefilter"
type = "filter"
mode = "keep_if_all"
rules = [
  { field = "fulltext", op = "min_length", value = 300 },
  { field = "title",    op = "not_matches", value = "(?i)sponsored|advertisement" },
]

[[stages]]
name = "summarize"
type = "llm_stage"
mode = "per_item"
input_field    = "fulltext"
output_field   = "summary"
prompt_file    = "prompts/summarize_zh.md"
model          = "openrouter/openai/gpt-4o-mini"
fallback_model = "ollama/llama3"
long_context_model = "openrouter/openai/gpt-4o"
workers        = 8
max_input_chars   = 400000
truncate_strategy = "head_tail"

[[stages]]
name = "daily_analysis"
type = "llm_stage"
mode = "aggregate"
input_field   = "summary"
output_target = "context.daily_brief"
prompt_file   = "prompts/daily_brief_zh.md"
model         = "openrouter/openai/gpt-4o"

[[stages]]
name = "render"
type = "render"
template     = "prompts/daily.md.j2"
output_field = "context.markdown"

# ─── Sinks (multiple allowed, ID-keyed for idempotency) ─
[[sinks]]
id    = "local_archive"
type  = "file_md"
input = "context.markdown"
path  = "./out/ai_news/{run_date}.md"

[[sinks]]
id             = "notion_main"
type           = "notion"
input          = "context.markdown"
parent_id      = "env:NOTION_DB_AI_NEWS"
parent_type    = "database"
token          = "env:NOTION_TOKEN"
title_template = "AI Daily · {run_date}"
properties     = { Tag = "AI", Source = "sluice" }
mode           = "upsert"          # upsert | create_once | create_new
```

</details>

---

## Built-in plugins

<details>
<summary><b>📥 Sources</b></summary>

| `type` | Description                       |
| ------ | --------------------------------- |
| `rss`  | Standard RSS/Atom via feedparser. URL canonicalization (UTM/fbclid/… stripped), enclosures extracted as `Item.attachments`, future-dated items dropped. |

> **Coming in v2:** IMAP, Telegram, Reddit, custom webhook.

</details>

<details>
<summary><b>🌐 Fetchers (full-text extraction chain)</b></summary>

| `type`         | Use when                                                |
| -------------- | ------------------------------------------------------- |
| `trafilatura`  | Pure Python, no extra service. Fast. Default first try. |
| `crawl4ai`     | Self-hosted Crawl4AI. Uses `POST /crawl`, then polls `/task/{task_id}` or `/jobs/{task_id}` when the service returns a task id. |
| `firecrawl`    | Self-hosted Firecrawl for JS-rendered pages.            |
| `jina_reader`  | Hosted Jina Reader fallback when self-hosting fails.    |

The fetcher chain is configured globally (or per-pipeline). Each request
walks the chain top-down with `min_chars` validation and an optional disk
cache:

```toml
[fetcher]
chain         = ["trafilatura", "crawl4ai", "firecrawl", "jina_reader"]
min_chars     = 500
on_all_failed = "skip"             # or "continue_empty" / "use_raw_summary"

[fetcher.cache]
enabled = true
ttl     = "7d"
```

SSRF guard built in: outbound fetches are blocked from hitting
private/loopback IPs to prevent malicious feed entries from probing
internal networks.

If you run behind a TUN/fake-IP proxy (for example Clash/mihomo fake-ip mode),
DNS may resolve public domains to `198.18.0.0/15`. Keep the default strict
guard for normal environments; opt in only when you know your proxy is doing
this:

```bash
SLUICE_SSRF_ALLOW_TUN_FAKE_IP=1 sluice run ai_news
```

</details>

<details>
<summary><b>⚙️ Processors</b></summary>

| `type`                | Purpose                                                                       |
| --------------------- | ----------------------------------------------------------------------------- |
| `dedupe`              | Drop items already in `seen_items` for this pipeline.                         |
| `cross_dedupe`        | Merge duplicates across sources by URL first, then title similarity. Keeps source-priority winners and can merge tags. |
| `fetcher_apply`       | Walk the fetcher chain to populate `item.fulltext`.                           |
| `html_strip`          | Strip HTML from top-level fields or `extras.<key>`, preserving paragraph/header line breaks and dropping script/style/template content. |
| `filter`              | Rule-based keep/drop. 17 operators incl. regex, length, time windows. ReDoS-guarded. |
| `field_filter`        | Mutate fields (truncate, drop, lower, strip, regex_replace) — e.g. trim `fulltext` to 20k chars before an expensive LLM call. |
| `score_tag`           | Per-item LLM scorer that writes `extras.<score_field>` and appends/replaces tags. Handles JSON fences, numeric strings, truncation, and per-item failures. |
| `summarize_score_tag` | Per-item LLM stage that writes a summary, `extras.<score_field>`, and tags in one call. Default `summary_field = "summary"` writes `item.summary`; use `extras.<key>` for extras. |
| `sort`                | Order all items by a numeric, string, or datetime field. `sort_type = "auto"` keeps score-like strings numeric; use `string` for lexical title sorting. |
| `llm_stage`           | LLM call, `per_item` (fan out) or `aggregate` (single call over all items). JSON parsing, max input chars, head-tail truncation, same-model retries, fallback chain, long-context routing, overflow trimming, cost preflight. |
| `render`              | Jinja2 template → markdown into `context.<key>`. Receives a fixed context (items, stats, run_date, …). |
| `limit`               | Sort and cap output. `sort_by`, `group_by`, `per_group_max`, `top_n`.        |
| `enrich`              | Plug in enrichers that augment items with external data (e.g. HN comments).  |
| `mirror_attachments`  | Download item attachments/extras to local disk; rewrite URLs.                 |

</details>

<details>
<summary><b>🎚 <code>filter</code> operator reference</b></summary>

`filter` is a cheap, deterministic, **no-LLM** keep/drop stage. Each rule
is `{ field, op, value }` where `field` is a dot-path resolved through
`Item.get()` (so `extras.relevance`, `tags.0`, `published_at` all work).
Combine rules with one of four `mode`s.

The same `mode` and `rules` shape is also valid under any `[[sources]]` as
`[sources.filter]`. Source-level filters run before stages and can use the
extra `content` field, which maps to source-time article text such as RSS
`summary`, `description`, or Atom `content`.

**Modes:**

| `mode`           | Keep this item if…                       |
| ---------------- | ----------------------------------------- |
| `keep_if_all`    | every rule matches (logical AND)         |
| `keep_if_any`    | at least one rule matches (logical OR)   |
| `drop_if_all`    | NOT all rules match                       |
| `drop_if_any`    | NOT any rule matches                      |

**Operators (17 total):**

| Category    | `op`            | What it checks                                                       |
| ----------- | --------------- | -------------------------------------------------------------------- |
| Existence   | `exists`        | field is present and not None                                        |
|             | `not_exists`    | field is None or missing                                             |
| Numeric     | `gt` / `gte`    | `field > value` / `field >= value`                                   |
|             | `lt` / `lte`    | `field < value` / `field <= value`                                   |
|             | `eq`            | `field == value`                                                     |
| String      | `matches`       | regex search (ReDoS-guarded)                                         |
|             | `not_matches`   | inverse of `matches`                                                 |
|             | `contains`      | substring / element membership in field                              |
|             | `not_contains`  | inverse of `contains`                                                |
| Membership  | `in`            | field value is in the given list                                     |
|             | `not_in`        | inverse of `in`                                                      |
| Length      | `min_length`    | `len(field) >= value`                                                |
|             | `max_length`    | `len(field) <= value`                                                |
| Time        | `newer_than`    | `field` (datetime) is newer than `now - value` (e.g. `"24h"`, `"7d"`) |
|             | `older_than`    | `field` is older than `now - value`                                  |

**Worked examples:**

```toml
# 1. Source-level regex prefilter — skip unwanted feed items before stages.
[[sources]]
type = "rss"
url  = "https://openai.com/blog/rss"

[sources.filter]
mode = "keep_if_any"
rules = [
  { field = "title",   op = "matches", value = "(?i)gpt|agent|model" },
  { field = "content", op = "matches", value = "(?i)gpt|agent|model" },
]

# 2. Pre-LLM cheap stage filter — keep articles long enough to summarize
#    and drop obvious ad/sponsored junk by title.
[[stages]]
name = "prefilter"
type = "filter"
mode = "keep_if_all"
rules = [
  { field = "fulltext", op = "min_length", value = 300 },
  { field = "title",    op = "not_matches", value = "(?i)sponsored|advertisement|广告|赞助" },
  { field = "published_at", op = "newer_than", value = "48h" },
]

# 3. LLM-driven filter — score_tag scores relevance 1-10,
#    then `filter` drops anything below 6.
[[stages]]
name = "score_and_tag"
type = "score_tag"
input_field = "fulltext"
prompt_file = "prompts/score_tag.md"
model       = "openrouter/openai/gpt-4o-mini"
score_field = "relevance"
tags_merge  = "append"

[[stages]]
name = "drop_irrelevant"
type = "filter"
mode = "keep_if_all"
rules = [
  { field = "extras.relevance", op = "gte", value = 6 },
]

[[stages]]
name = "rank_relevant"
type = "sort"
sort_by = "extras.relevance"
sort_type = "number"  # auto | number | string | datetime
sort_order = "desc"
sort_missing = "last"

# 4. Tag whitelist + URL blacklist combo
[[stages]]
name = "scope"
type = "filter"
mode = "keep_if_all"
rules = [
  { field = "tags",       op = "contains",     value = "ai" },
  { field = "url",        op = "not_matches",  value = "^https?://(twitter|x)\\.com/" },
  { field = "source_id",  op = "not_in",       value = ["spammy_feed_1", "spammy_feed_2"] },
]
```

**Why "rule-based filter + LLM scorer" beats pure LLM filtering:** scoring
costs LLM tokens, so do it once via `score_tag`, write the score into
`extras`, and let cheap deterministic rules fan it out. Same prompt money,
unlimited filter combinations downstream.

</details>

<details>
<summary><b>📤 Sinks</b></summary>

| `type`      | Description                                                  |
| ----------- | ------------------------------------------------------------ |
| `file_md`   | Deterministic local markdown file. Useful as an audit trail. |
| `notion`    | Wraps [`notionify`](https://pypi.org/project/notionify/) — markdown → Notion page in your database. |
| `telegram`  | Push messages to Telegram chats via Bot API. MarkdownV2 rendering, safe truncation, split-on-too-long. |
| `feishu`    | Push messages to Feishu/Lark. Two auth modes: `auth_mode = "webhook"` (default) — webhook URL + optional HMAC secret; `auth_mode = "bot_api"` — app_id + app_secret + receive_id, sends Markdown-converted post messages via the Bot API. Supports `post`, `text`, and `interactive` (Card V2) message types. |
| `email`     | Send HTML emails via SMTP. Auto-detects TLS mode from port (465→SSL, 587→STARTTLS). Per-recipient batching, `fail_fast` or `best_effort` delivery. |

**Idempotency modes:**

| `mode`        | Behavior                                                                      |
| ------------- | ----------------------------------------------------------------------------- |
| `upsert`      | Re-running the same `run_key` updates the existing page (no duplicates).      |
| `create_once` | First run creates; later runs no-op.                                          |
| `create_new`  | Always create a new page (intentional non-idempotent for "publish each rerun"). |

For database parents, `properties` may use friendly TOML values such as
`{ Tag = "AI", Source = "sluice" }`; sluice reads the database schema and
expands them to the Notion API shape for `select`, `multi_select`,
`rich_text`, `url`, `date`, and similar property types. Fully explicit Notion
property dictionaries are passed through unchanged.

</details>

---

## Operations & observability

<details>
<summary><b>🔧 CLI</b></summary>

```bash
sluice list                                          # list configured pipelines and their crons
sluice validate                                      # validate all TOML
sluice run <pipeline_id>                             # run once
sluice run <pipeline_id> --dry-run                   # no DB writes, no sink emits
sluice run <pipeline_id> --verbose                   # include Sluice DEBUG logs
sluice run <pipeline_id> --log-file logs/run.jsonl   # write DEBUG JSONL diagnostics
sluice deploy                                        # register all enabled pipelines as Prefect deployments
sluice failures <pipeline_id>                        # list failed_items
sluice failures <pipeline_id> --retry <item_key>     # move dead-letter back to failed
sluice gc                                            # reclaim space from failed_items/url_cache/attachment_mirror
sluice gc --dry-run                                  # show what would be deleted without modifying
sluice gc --older-than 90d --pipeline ai_news        # target specific age and pipeline
sluice stats                                         # show pipeline run stats (last 7 days)
sluice stats ai_news --since 30d --format json       # per-pipeline stats in JSON
sluice metrics-server --host 0.0.0.0 --port 9090    # start Prometheus exposition endpoint
sluice deliveries <pipeline_id>                      # list sink delivery audit log
sluice deliveries <pipeline_id> --run <run_key>      # filter deliveries by specific run
```

</details>

<details>
<summary><b>🪵 Progress and logs</b></summary>

`sluice run` prints a tqdm progress bar while the pipeline is running, then a
Rich **Step Summary** table with per-source and per-stage counts:

```text
source  rss_0           -   2   total=2
stage   fetch_fulltext  22  3   fetched=3 failed=19 AllFetchersFailed=19
sink    local:file_md   3   emitted
```

The console log level is INFO by default. Add `--verbose` to show Sluice DEBUG
events such as individual fetcher attempts, cache hits, too-short pages, and
LLM retryable failures. Third-party internals (`aiosqlite`, `httpx`,
`httpcore`, `feedparser`, `trafilatura`, `prefect`) are kept at WARNING so
verbose mode doesn't turn into transport noise.

Use `--log-file` or `SLUICE_LOG_FILE` for full DEBUG JSONL diagnostics:

```bash
sluice run ai_news --log-file logs/ai_news.jsonl
SLUICE_LOG_FILE=logs/ai_news.jsonl sluice run ai_news --verbose
```

</details>

<details>
<summary><b>📊 State and what's persisted (SQLite)</b></summary>

Eight tables, no ORM, schema migrated via `PRAGMA user_version`:

| Table                  | What it tracks                                                |
| ---------------------- | ------------------------------------------------------------- |
| `seen_items`           | Dedupe registry per pipeline. Includes summary for future RAG. |
| `failed_items`         | Per-item failures with full payload, status (`failed`/`dead_letter`/`resolved`), attempt count. |
| `sink_emissions`       | Maps `(pipeline_id, run_key, sink_id)` → external_id. Powers idempotent retries. |
| `url_cache`            | Article extraction cache, TTL'd. Avoids re-hitting Firecrawl on retries. |
| `run_log`              | Per-run metadata: items in/out, LLM calls, estimated cost, status, error. |
| `sink_delivery_log`    | Per-message push-sink audit trail: ordinal, kind, recipient, external_id, status, error. |
| `attachment_mirror`    | Mirrored attachment metadata: original URL, local path, mime type, size. |
| `gc_log`               | GC run history: timestamp, tables cleaned, rows affected. |

</details>

<details>
<summary><b>🔁 Failure handling lifecycle</b></summary>

```
RSS item ──▶ stage X fails ──▶ failed_items (status=failed, item_json saved)
                                       │
                            next run starts
                                       │
                       ┌───────────────┴────────────────┐
                       ▼                                ▼
              re-queued before processors     succeeds → status=resolved
                       │
              fails again ──▶ attempts++ ──▶ at max_retries → dead_letter
                                                      │
                                       sluice failures --retry → back to failed
```

Items are reconstructed from `item_json` so retry doesn't depend on the RSS
feed re-surfacing them.

</details>

<details>
<summary><b>📅 Scheduling with Prefect — the production upgrade</b></summary>

You can run sluice with raw cron. A one-line crontab entry will do:

```bash
0 8 * * * cd /app && sluice run ai_news
```

That works. But after a few weeks of running real pipelines, you'll start asking
questions that cron can't answer: Did this morning's run succeed? Which item
failed and why? Can I retry just that one stage without re-running the whole
pipeline?

This is where Prefect comes in. `sluice deploy` registers each enabled pipeline
as a **Prefect deployment** — a scheduled job with its own cron expression,
run history, and per-task observability baked in. Think of Prefect as the
missing web UI for your pipelines: not a dependency, not a lock-in, just an
optional upgrade when cron isn't enough.

**The full setup — three terminals, 30 seconds:**

```bash
# Terminal 1: Start the Prefect server (API + UI)
prefect server start

# Terminal 2: Register all your pipelines as scheduled deployments
sluice deploy

# Terminal 3: Start a worker to pick up scheduled runs
prefect worker start --pool default
```

That's it. Your pipelines will fire on their cron schedules, and the Prefect UI
at **http://localhost:4200** gives you per-pipeline run timelines, retry buttons,
and per-task logs — basically the operational dashboard you'd build if you had
a spare weekend.

**A few things worth knowing:**

- `sluice deploy` is idempotent — run it again after editing a pipeline's cron
  and Prefect will pick up the change.
- The worker pool name (`--pool default`) is configurable. Create separate pools
  for GPU-heavy pipelines vs lightweight ones.
- If Prefect is down, sluice still works fine from the CLI. Prefect wraps sluice;
  sluice doesn't require Prefect.
- Run history and logs live in a local SQLite database alongside your sluice
  state — no external Postgres needed for single-machine setups.

Prefect is fully optional. Start with raw cron if you're just kicking the tires,
add `sluice deploy` when you want visibility, and upgrade to a Prefect server
when you have enough pipelines to justify a dashboard.

</details>

---

## Docker deployment

You don't need Docker to run sluice — a Python venv works perfectly. But when
you're deploying to a VPS, a home server, or a cron-triggered cloud function,
containers make life simpler. Here are the two paths.

Docker files live in [`scripts/docker/`](./scripts/docker/).
The compose files use the published GHCR image by default. Set `SLUICE_IMAGE`
in `scripts/docker/.env` to pin a version for production.

### Path A: Standalone — run once, exit

Good for: serverless cron jobs, CI pipelines, or simply "I want to call sluice
from a systemd timer without installing Python tooling on the host."

```bash
cd scripts/docker

# Copy your configs and .env into place
cp -r ../../configs ./configs
cp .env.example .env
$EDITOR .env

# Run once
docker compose run --rm sluice run ai_news

# Dry-run
docker compose run --rm sluice run ai_news --dry-run

# Validate configs
docker compose run --rm sluice validate
```

The compose file mounts `./configs` (read-only) and `./data` (writable SQLite
state). Tweak the cron trigger on your host (systemd timer, Kubernetes CronJob,
AWS EventBridge — whatever you already use) to invoke `docker compose run --rm`.
The container starts, runs the pipeline, shuts down. No long-running process,
no port binding, no stateful server.

### Path B: With Prefect — always-on scheduler

Good for: multiple pipelines with overlapping schedules, run history you can
inspect, a UI for retries and debugging.

```bash
cd scripts/docker
cp -r ../../configs ./configs
cp .env.example .env
$EDITOR .env

docker compose -f docker-compose.prefect.yml up
```

This starts three things:
- **Prefect server** — API + dashboard at **http://localhost:4200**
- **sluice deploy** — auto-registers all cron-enabled pipelines on container start
- **Prefect worker** — picks up scheduled runs as they arrive

Data (SQLite state, run history, cached articles) is persisted across restarts
via Docker volumes. If you stop the container and bring it back up next week,
your pipeline state is intact.

### Build your own image

If the published image doesn't fit your setup, build from source:

```bash
# Build from repo root
docker build -f scripts/docker/Dockerfile -t sluice:local .

# Run
docker run --rm \
  -v $(pwd)/configs:/app/configs:ro \
  -v $(pwd)/data:/app/data \
  sluice:local run ai_news
```

The image includes all optional dependencies (`[all]` extras), so Telegram,
Feishu, Email sinks, Prometheus metrics, and the HN enricher are ready to go
out of the box.

### Generating an `Authorization: Basic` header value

Some self-hosted services (crawl4ai, private Firecrawl instances, internal
proxies) use HTTP Basic Authentication. The header value is
`Basic <base64(username:password)>`. Generate it with any of these:

```bash
# Python (no dependencies)
python3 -c "import base64; print('Basic ' + base64.b64encode(b'alice:s3cr3t').decode())"

# OpenSSL
printf 'alice:s3cr3t' | openssl base64 | tr -d '\n' | sed 's/^/Basic /'

# GNU coreutils
printf 'alice:s3cr3t' | base64 | tr -d '\n' | sed 's/^/Basic /'
```

All three produce the same result: `Basic YWxpY2U6czNjcjN0`

Store the full string (including `Basic `) in your `.env`, then reference it
from the fetcher config:

```bash
# .env
CRAWL4AI_AUTH=Basic YWxpY2U6czNjcjN0
```

```toml
# configs/sluice.toml  — [fetcher.crawl4ai] section
api_headers = { Authorization = "env:CRAWL4AI_AUTH" }
```

> **Never put raw credentials in TOML files or commit them to version control.**
> Always use the `env:VAR_NAME` indirection shown above.

---

## CI/CD

We ship sluice with the same workflows we use ourselves. Everything lives in
[`.github/workflows/`](./.github/workflows/).

### `ci.yml` — runs on every push and PR

Matrix over Python 3.11 through 3.14:

1. Install dependencies (`uv sync --all-extras --frozen`)
2. `ruff check .` — lint
3. `ty check` — type check (0 errors)
4. `pytest -q`

### `publish.yml` — triggered on `v*.*.*` tags

Push a version tag (`git tag v0.2.0 && git push --tags`) and the rest is automatic:

1. Verifies the tag matches `pyproject.toml` version
2. `uv build` → `dist/`
3. Publishes to PyPI via **OIDC trusted publishing** — no API tokens, no secret rotation

**One-time PyPI setup:**

1. Create a `pypi` environment in GitHub → Settings → Environments
2. On PyPI → [sluice-pipeline](https://pypi.org/project/sluice-pipeline/) → Publishing → add a trusted publisher:
   - Owner: `nerdneilsfield`
   - Repository: `sluice-pipeline`
   - Workflow: `publish.yml`
   - Environment: `pypi`

After that, every `git push --tags` publishes a release. No manual steps, no expiring secrets.

---

## Roadmap

✅ **v1 (now)** — RSS source, Notion sink, file_md sink, core processors, 4-tier
LLM fallback, idempotent retries, dry-run, loguru diagnostics, Prefect
scheduling, SSRF guard.

✅ **v1.1**

- [x] Push-channel sinks: Telegram, Feishu, Email
- [x] Attachment mirroring (`mirror_attachments` stage)
- [x] Enricher protocol + `hn_comments`
- [x] Sub-daily pipelines (`run_key_template`)
- [x] `sort` and `limit` stages
- [x] `field_filter` ops: lower, strip, regex_replace
- [x] `cross_dedupe`, `html_strip`, `score_tag`, and `summarize_score_tag` stages
- [x] Crawl4AI fetcher support
- [x] Fetcher fallback (`on_all_failed`)
- [x] URL cache size cap
- [x] GC command + metrics + CLI audit viewer
- [x] Lazy registry

🚧 **v1.2**

- Native Anthropic Messages API
- Per-tier worker counts
- Notion page cover image
- Plugin entry-points (auto-discovery for third-party plugins)

🔮 **v2**

- IMAP / email source
- GitHub repo sink (push markdown → trigger build)
- RAG over historical summaries (semantic search across past digests)

---

## Development

```bash
git clone https://github.com/nerdneilsfield/sluice-pipeline
cd sluice-pipeline
uv sync --all-extras                # or pip install -e '.[dev,all]'
pytest -q                           # 408 tests
pytest --cov=sluice                 # 80% coverage
ruff check .
ty check                            # 0 errors
```

The architecture and design rationale live in
[`docs/superpowers/specs/`](./docs/superpowers/specs/) and the TDD-driven
implementation plan in [`docs/superpowers/plans/`](./docs/superpowers/plans/).

---

## License

MIT © [nerdneilsfield](https://github.com/nerdneilsfield)

<div align="center">

— *open the gate. let the right water through.* —

</div>
