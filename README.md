<div align="center">

# 🚰 sluice

**Code-native, configurable information pipelines —
the "code version of n8n" for RSS, LLMs, and Notion.**

[![PyPI version](https://img.shields.io/pypi/v/sluice.svg?color=blue)](https://pypi.org/project/sluice/)
[![Python](https://img.shields.io/pypi/pyversions/sluice.svg?color=blue)](https://pypi.org/project/sluice/)
[![License](https://img.shields.io/github/license/nerdneilsfield/sluice.svg)](https://github.com/nerdneilsfield/sluice/blob/master/LICENSE)
[![CI](https://img.shields.io/github/actions/workflow/status/nerdneilsfield/sluice/ci.yml?branch=master&label=CI)](https://github.com/nerdneilsfield/sluice/actions)
[![Coverage](https://img.shields.io/badge/coverage-89%25-brightgreen.svg)](https://github.com/nerdneilsfield/sluice)
[![Tests](https://img.shields.io/badge/tests-160%20passing-brightgreen.svg)](https://github.com/nerdneilsfield/sluice/actions)
[![Stars](https://img.shields.io/github/stars/nerdneilsfield/sluice.svg?style=social)](https://github.com/nerdneilsfield/sluice)

[**English**](./README.md) · [**简体中文**](./README_ZH.md) · [PyPI](https://pypi.org/project/sluice/) · [GitHub](https://github.com/nerdneilsfield/sluice)

</div>

> **slu·ice** /sluːs/ — *a water gate that controls flow.*

That's exactly what this project does, but with **information** instead of water:
choose which sources let the stream in, when to open the gate, what processing
channels the water passes through, and which downstream reservoirs it ends up in.

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
- [Roadmap](#roadmap)
- [Development](#development)
- [License](#license)

---

## Why sluice?

You have a list of RSS feeds. You want **a daily digest in Notion** — articles
auto-fetched, LLM-summarized, aggregated into a brief, then pushed to a Notion
database. You want it cheap, observable, and **fully under your control**.

Three options most people consider:

|                            | n8n / Zapier                       | A 200-line Python script               | **sluice**                                                     |
| -------------------------- | ---------------------------------- | -------------------------------------- | -------------------------------------------------------------- |
| Add a new feed             | Click 12 buttons                   | Edit code                              | Add 3 lines to TOML                                            |
| Swap LLM provider          | Hope the integration exists        | Hope you wrote it that way             | Edit `providers.toml`, restart                                 |
| Cost cap per run           | Hard                               | Hand-rolled                            | One-line `max_estimated_cost_usd`                              |
| Failure handling           | "Retry the whole workflow"         | `try: ... except: pass`                | Per-item failed_items lifecycle, dead-letter, `--retry`        |
| Self-hosted observability  | n8n web UI (heavy)                 | `print` + grep                         | Rich progress, loguru diagnostics, Prefect run history         |
| LLM fallback chain         | Manual branching                   | None                                   | 4-tier model chain with weighted routing + key cooldown        |
| Idempotency                | "Did it already run?"              | "Did the script crash midway?"         | `sink_emissions` table, upsert on retry                        |

sluice is **the code version of n8n**, but with all your business logic in
plain Python and TOML. No SaaS dependency, no GUI lock-in, no opaque webhooks.

---

## Quickstart

### 1. Install

```bash
pip install sluice
```

> Requires Python 3.11+. Bring your own [Notion integration token](https://developers.notion.com/docs/create-a-notion-integration)
> and at least one OpenAI-compatible LLM key (DeepSeek, GLM, OpenAI, OpenRouter, …).

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
└── templates/
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
template = "templates/daily.md.j2"
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

The water-gate metaphor maps directly onto the codebase. Five plugin
**Protocols** — every pipeline is a composition of these:

| Protocol      | What it does                            | Built-in implementations                                                    |
| ------------- | --------------------------------------- | --------------------------------------------------------------------------- |
| `Source`      | Bring items into the stream             | `rss`                                                                       |
| `Fetcher`     | Hydrate an article URL → markdown       | `trafilatura`, `firecrawl`, `jina_reader`                                   |
| `Processor`   | Transform the stream                    | `dedupe`, `fetcher_apply`, `filter`, `field_filter`, `llm_stage`, `render`  |
| `Sink`        | Push items downstream                   | `file_md`, `notion`                                                         |
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

### LLM provider pool — the *real* unfair advantage

This is what separates sluice from a 200-line script. Configure a pool
of providers, weighted base URLs, weighted API keys, automatic quota
cooldown, and a **4-tier fallback chain per stage**:

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
  { value = "env:OR_KEY_2", weight = 1, quota_duration = 18000,
    quota_error_tokens = ["exceed", "quota"] },
]
active_windows  = ["00:00-08:00"]   # only use this base off-peak
active_timezone = "Asia/Shanghai"

[[providers.models]]
model_name          = "openai/gpt-4o-mini"
input_price_per_1k  = 0.00015
output_price_per_1k = 0.0006

[[providers]]
name = "ollama"
type = "openai_compatible"
[[providers.base]]
url = "http://localhost:11434/v1"
key = [{ value = "local" }]
[[providers.models]]
model_name = "llama3"
# free local — no price needed
```

Then in any `llm_stage`:

```toml
model            = "openrouter/openai/gpt-4o-mini"
retry_model      = "openrouter/openai/gpt-4o-mini"   # same tier, retry on transient failures
fallback_model   = "openrouter/google/gemini-flash"   # cheaper backup
fallback_model_2 = "ollama/llama3"                    # last-resort local
```

The chain walks main → retry → fallback → fallback_2. Each tier has
independent worker/concurrency caps. Quota-exhausted keys are cooled down
automatically. Time-window routing lets you push expensive traffic to
off-peak hours.

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
workers        = 8
max_input_chars   = 20000
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
template     = "templates/daily.md.j2"
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
| `firecrawl`    | Self-hosted Firecrawl for JS-rendered pages.            |
| `jina_reader`  | Hosted Jina Reader fallback when self-hosting fails.    |

The fetcher chain is configured globally (or per-pipeline). Each request
walks the chain top-down with `min_chars` validation and an optional disk
cache:

```toml
[fetcher]
chain         = ["trafilatura", "firecrawl", "jina_reader"]
min_chars     = 500
on_all_failed = "skip"             # or "continue_empty"

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
<summary><b>⚙️ Processors (the six stage types)</b></summary>

| `type`           | Purpose                                                                       |
| ---------------- | ----------------------------------------------------------------------------- |
| `dedupe`         | Drop items already in `seen_items` for this pipeline.                         |
| `fetcher_apply`  | Walk the fetcher chain to populate `item.fulltext`.                           |
| `filter`         | Rule-based keep/drop. 14 operators incl. regex, length, time windows. ReDoS-guarded. |
| `field_filter`   | Mutate fields (truncate, drop) — e.g. trim `fulltext` to 20k chars before an expensive LLM call. |
| `llm_stage`      | LLM call, `per_item` (fan out) or `aggregate` (single call over all items). JSON parsing, max input chars, head-tail truncation, 4-tier fallback chain, cost preflight. |
| `render`         | Jinja2 template → markdown into `context.<key>`. Receives a fixed context (items, stats, run_date, …). |

</details>

<details>
<summary><b>🎚 <code>filter</code> operator reference</b></summary>

`filter` is a cheap, deterministic, **no-LLM** keep/drop stage. Each rule
is `{ field, op, value }` where `field` is a dot-path resolved through
`Item.get()` (so `extras.relevance`, `tags.0`, `published_at` all work).
Combine rules with one of four `mode`s.

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
# 1. Pre-LLM cheap prefilter — keep articles long enough to summarize
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

# 2. LLM-driven filter — upstream llm_stage scores relevance 0-10,
#    then `filter` drops anything below 6.
[[stages]]
name = "rate_relevance"
type = "llm_stage"
mode = "per_item"
input_field    = "summary"
output_field   = "extras.relevance"
prompt_file    = "prompts/rate.md"
output_parser  = "json"
model          = "openrouter/openai/gpt-4o-mini"

[[stages]]
name = "drop_irrelevant"
type = "filter"
mode = "keep_if_all"
rules = [
  { field = "extras.relevance", op = "gte", value = 6 },
]

# 3. Tag whitelist + URL blacklist combo
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
costs LLM tokens, so do it once via `llm_stage`, write the score into a
field, and let cheap deterministic rules fan it out. Same prompt money,
unlimited filter combinations downstream.

</details>

<details>
<summary><b>📤 Sinks</b></summary>

| `type`     | Description                                                  |
| ---------- | ------------------------------------------------------------ |
| `file_md`  | Deterministic local markdown file. Useful as an audit trail. |
| `notion`   | Wraps [`notionify`](https://pypi.org/project/notionify/) — markdown → Notion page in your database. |

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

> **Coming in v2:** email (HTML newsletter), GitHub repo push, webhook.

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

Five tables, no ORM, schema migrated via `PRAGMA user_version`:

| Table             | What it tracks                                                |
| ----------------- | ------------------------------------------------------------- |
| `seen_items`      | Dedupe registry per pipeline. Includes summary for future RAG. |
| `failed_items`    | Per-item failures with full payload, status (`failed`/`dead_letter`/`resolved`), attempt count. |
| `sink_emissions`  | Maps `(pipeline_id, run_key, sink_id)` → external_id. Powers idempotent retries. |
| `url_cache`       | Article extraction cache, TTL'd. Avoids re-hitting Firecrawl on retries. |
| `run_log`         | Per-run metadata: items in/out, LLM calls, estimated cost, status, error. |

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
<summary><b>📅 Scheduling with Prefect</b></summary>

`sluice deploy` registers each enabled pipeline as a Prefect deployment with
its own cron schedule. The Prefect UI gives you per-pipeline run history,
retry, and per-task observability — basically the n8n web UI you actually
wanted.

```bash
prefect server start                # in one terminal
sluice deploy                       # registers cron deployments
prefect worker start --pool default # processes scheduled runs
```

</details>

---

## Roadmap

✅ **v1 (now)** — RSS source, Notion sink, file_md sink, 6 processors, 4-tier
LLM fallback, idempotent retries, dry-run, loguru diagnostics, Prefect
scheduling, SSRF guard.

🚧 **v1.1**

- Native Anthropic Messages API (currently use OpenRouter for Claude access)
- Per-tier worker counts (config accepts them; runtime currently honors stage-level only)
- Notion page cover image
- Plugin entry-points (auto-discovery for third-party plugins)
- `sluice gc` for cleanup of resolved/dead-lettered rows

🔮 **v2**

- IMAP / email source
- Email HTML sink (with subscriber management)
- GitHub repo sink (push markdown → trigger build)
- RAG over historical summaries (semantic search across past digests)

---

## Development

```bash
git clone https://github.com/nerdneilsfield/sluice
cd sluice
uv sync --all-extras                # or pip install -e '.[dev]'
pytest                              # 160 tests, ~9s
pytest --cov=sluice                 # 89% coverage
ruff check .
ty check .
```

The architecture and design rationale live in
[`docs/superpowers/specs/`](./docs/superpowers/specs/) and the TDD-driven
implementation plan in [`docs/superpowers/plans/`](./docs/superpowers/plans/).

---

## License

MIT © [nerdneilsfield](https://github.com/nerdneilsfield)

<div align="center">

— *open the gate, let the right water through* —

</div>
