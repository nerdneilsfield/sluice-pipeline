# Sluice Multi-Pipeline DAG Architecture

**Status**: Draft
**Date**: 2026-05-02

## Summary

Refactor sluice from a single linear pipeline (source → processors → sinks) into a **task-level DAG of independently scheduled pipelines**, with item-centric DB persistence. Each task is self-contained. Sources, sinks, stages, and pipelines are flat top-level arrays referenced by `name`. Each pipeline is one Prefect deployment with its own cron. All item content lives in one wide master `items` table. Each stage and each pipeline owns a per-item audit table; runs are logged in master `stages` and `pipelines` tables. DB writes are decoupled to a dedicated writer thread. A separate, never-rotated SQLite holds the dedup index.

## Why

The current single-pipeline runner couples concerns with very different cadences:
- RSS sources have heterogeneous fetch frequencies
- LLM enrichment is expensive and runs nightly
- Daily reports run at fixed times
- Some breaking-news items need an immediate fast-path

The new architecture decouples lifecycle phases into a DAG of pipelines, each independently scheduled, with the database as the persistent handoff between pipelines.

## Scope

**In scope:**
- New TOML configuration: 4 top-level arrays per task plus `task.toml`
- Renaming: existing "pipeline" concept becomes "task"; existing inline `[[pipeline.stages]]` processors become first-class `[[stages]]`; new "pipeline" = DAG node and deployment unit
- Multi-pipeline runner with in-memory stage chaining and end-of-run transactional DB writes via a writer thread
- DB schema redesign: master `items` + dynamic per-stage / per-pipeline detail tables; separate seen DB
- DAG validation in loader (acyclicity, reference resolution, shape constraints)
- CLI: `sluice run`, `sluice retry`, `sluice deploy`
- Monthly active-DB rotation (mv + gz)
- New SQL migration

**Non-goals:**
- Cross-task sharing of stages, sources, sinks, items (each task is sandboxed)
- Per-stage cron (cron only at pipeline level)
- DAG cycles
- Per-item routing rules within a pipeline
- Per-pipeline parameter overrides on a stage instance
- Backward compatibility with v1.1 config or DB (drop all)
- Pipeline-level item count or LLM budget limits (push to stage)
- Automatic Prefect retry config (retry is a manual CLI command)
- Resumable mid-stage execution (resume granularity is stage)

## Concepts

| Concept | Definition |
|---|---|
| **Task** | Top-level self-contained unit. Directory under `configs/tasks/`. Owns its own DB and seen DB. State namespace. |
| **Source** | External data feed definition (RSS URL, etc.). Referenced by name. |
| **Sink** | External output target (Telegram, email, etc.). Referenced by name. |
| **Stage** | Atomic processor: one processor type plus its parameters. Equivalent to one current `[[pipeline.stages]]` entry. Owns a small per-item audit table. |
| **Pipeline** | A DAG node and one Prefect deployment. Composes a linear sequence of stage names. References sources (entry) or upstream pipelines (downstream). May reference sinks. Has its own cron. |
| **DAG** | Directed acyclic graph of pipelines connected by `upstream_all` / `upstream_any`. Multiple roots and forks allowed. |

## Configuration

### Directory layout

```
configs/
  sluice.toml             # global runtime / GC settings
  providers.toml          # LLM providers
  tasks/
    airflow/
      task.toml
      sources.toml
      sinks.toml
      stages.toml
      pipelines.toml
```

The loader merges all five TOML files in a task directory into a single resolved config.

### `task.toml`

```toml
[task]
name     = "airflow"
db_path  = "data/airflow.db"          # active DB; rotated monthly
seen_db  = "data/airflow_seen.db"     # dedup index DB; never rotated
enabled  = true
timezone = "Asia/Shanghai"            # optional; falls back to sluice.toml

# Time window (task-level, applied to all pipelines in same cycle)
[task.window]
anchor = "0 8 * * *"      # cron: anchor time (e.g., daily 8 AM)
length = "24h"            # window extends backward from most-recent anchor

# Which pipelines to deploy to Prefect
[task.deployments]
pipelines = "all"          # or explicit list: ["fetch_blog", "summarize_nightly", ...]
```

### `sources.toml`

Each source is one entry; `name` is the id. Per-type fields preserve existing sluice schema.

```toml
[[sources]]
name    = "airflow_blog"
type    = "rss"
url     = "https://airflow.apache.org/blog/rss.xml"
tag     = "airflow"
timeout = 60

[[sources]]
name = "airflow_releases"
type = "rss"
url  = "https://airflow.apache.org/releases/rss.xml"
```

### `sinks.toml`

```toml
[[sinks]]
name      = "tg_main"
type      = "telegram"
chat_id   = "..."
token_env = "TG_TOKEN"

[[sinks]]
name    = "email_digest"
type    = "email"
to      = ["me@example.com"]
subject = "Airflow Daily"
```

### `stages.toml`

Each `[[stages]]` entry is one stage instance with a unique `name` and a `type` from the registry. Type-specific parameters follow.

```toml
[[stages]]
name                      = "fetch_blog_rss"
type                      = "fetch_rss"
sources                   = ["airflow_blog"]    # source names this stage reads

[[stages]]
name                      = "fetch_fulltext"
type                      = "fetcher_apply"
write_field               = "fulltext"
skip_if_field_longer_than = 2000

[[stages]]
name = "prefilter"
type = "filter"
mode = "keep_if_all"
rules = [
  { field = "fulltext",     op = "min_length",  value = 300 },
  { field = "title",        op = "not_matches", value = "(?i)sponsored|hiring" },
  { field = "published_at", op = "newer_than",  value = "48h" },
]

[[stages]]
name        = "deep_summarize"
type        = "summarize_score_tag"
model       = "claude-sonnet-4-6"
retry_model = "claude-haiku-4-5-20251001"
max_calls_per_run    = 100      # stage-local LLM call cap
max_cost_usd_per_run = 2.0      # stage-local USD cap
max_attempts         = 3        # stage retry budget across pipeline runs

[[stages]]
name = "sort_by_score"
type = "sort"
key  = "score"
desc = true

[[stages]]
name = "limit_30"
type = "limit"
n    = 30

[[stages]]
name     = "render_digest"
type     = "render"
template = "daily_digest"
```

### Stage type catalog

Closed set of registered types (extension requires a code change). Each type has fixed semantics for what items columns it reads / writes.

| Type | Purpose | Reads (items columns) | Writes (items columns) | Per-run drop semantic |
|---|---|---|---|---|
| `fetch_rss` | Pull from RSS sources, populate items | — (writes new rows) | source_name, guid, url, title, author, published_at, raw_summary, raw_content, raw_json, tag | — |
| `dedupe` | Cross-pipeline-run dedup via seen DB and active items | item_id | — | reject_in_run |
| `cross_dedupe` | In-run cross-source dedup by url / tag | url, tags, source_name | — | reject_in_run |
| `filter` | Drop items not matching rules | rules-referenced columns | — | reject_in_run |
| `field_filter` | Mutate / truncate fields per ops | ops-referenced columns | ops-targeted columns (in-place) | — |
| `html_strip` | HTML → plain text | raw_summary, raw_content, fulltext | same columns (in place) | — |
| `fetcher_apply` | Fetch URL → markdown into a column | url | `write_field` (defaults to fulltext) | — |
| `mirror_attachments` | Mirror attachment URLs locally | raw_json, fulltext | attachments_json | — |
| `enrich` / `llm_stage` | Generic LLM-driven field fill | input_field | extras_json sub-key | — |
| `score_tag` | LLM scoring + tagging | input_field | score, tags_json | — |
| `summarize_score_tag` | LLM summary + score + tags | input_field | summary, score, tags_json | — |
| `sort` | Reorder in-memory item list | sort key column | — (volatile) | — |
| `limit` | Drop items exceeding `n` | sort key column | — | reject_in_run |
| `render` | Build sink-ready text from template | template-referenced columns | — (volatile, writes only to render context) | — |

**Volatile types** (`sort`, `render`): never persist to items, never resume-skip. Pipelines whose **last stage** is volatile MUST declare `sinks`.

**`reject_in_run`** (`dedupe`/`cross_dedupe`/`filter`/`limit`): the stage records `stage_<S>.status='dropped'` for the affected item; the in-memory item is removed from the pipeline's working set so subsequent stages don't see it. **At pipeline run end**, every item that ended up rejected at any stage in P (i.e., has at least one `stage_<S>.status='dropped'` row in this run) is recorded in `pipeline_<P>.status='dropped'`. Effect: that item won't re-enter P on future cron triggers, but other branches of the DAG are unaffected. Routine drops never touch `items.dropped`.

### `pipelines.toml`

```toml
[[pipelines]]
name    = "fetch_blog"
cron    = "0 */6 * * *"
sources = ["airflow_blog"]                                       # entry pipeline
stages  = ["fetch_blog_rss", "fetch_fulltext", "prefilter"]

[[pipelines]]
name    = "fetch_releases"
cron    = "0 */4 * * *"
sources = ["airflow_releases"]
stages  = ["fetch_releases_rss", "fetch_fulltext", "prefilter"]

[[pipelines]]
name         = "summarize_nightly"
cron         = "0 22 * * *"
upstream_all = ["fetch_blog", "fetch_releases"]                  # both must succeed
stages       = ["deep_summarize"]

[[pipelines]]
name             = "report_telegram"
cron             = "30 22 * * *"
upstream_all     = ["summarize_nightly"]
stages           = ["sort_by_score", "limit_30", "render_digest"]
sinks            = ["tg_main"]
post_run_cleanup = "drop_unprocessed_upstream_local"

[[pipelines]]
name         = "report_email"
cron         = "0 7 * * *"
upstream_all = ["summarize_nightly"]
stages       = ["sort_by_score", "limit_30", "render_digest"]
sinks        = ["email_digest"]
```

### Pipeline shape constraints (loader-validated)

1. Exactly one of (`sources`, `upstream_all` ∪ `upstream_any`) must be present.
2. If `sources` present, the pipeline's `stages` list must include at least one `fetch_<type>` stage.
3. If `upstream_all` or `upstream_any` present, no `fetch_<type>` stage allowed.
4. The last stage in `stages` must either persist to items (any stage type except `sort`/`render`) OR the pipeline declares `sinks`.
5. DAG (over upstream relations) must be acyclic.
6. Every name referenced (source, sink, stage, upstream pipeline) must resolve.

### `upstream_all` / `upstream_any` and `input_mode`

`upstream_all` and `upstream_any` express **run-level dependencies**, not item-level intersections. A pipeline P will only run when:

- All pipelines in `upstream_all` have at least one **successful run** since P's last successful run (or ever, if P never ran)
- At least one pipeline in `upstream_any` has at least one successful run since P's last successful run (if `upstream_any` is non-empty)

The **item input set** for P is determined by `input_mode`:

- `input_mode = "union"` (default): items where `EXISTS pipeline_<U>.status='success'` for **any** U in (`upstream_all` ∪ `upstream_any`). Different upstreams may produce disjoint item sets (e.g., `fetch_blog` produces blog items, `fetch_releases` produces release items); P sees all of them.
- `input_mode = "intersection"`: items where **every** U in `upstream_all` has `pipeline_<U>.status='success'` for that exact item_id. Use only when upstreams produce overlapping item sets and you want the cross.

This separation means `summarize_nightly` with `upstream_all = ["fetch_blog", "fetch_releases"]` waits until both upstream pipelines have run successfully (run-level gating), then consumes the **union** of their items (item-level default).

## Data Model

Each task has two SQLite databases:

1. **Active DB** (`db_path`) — content + audit, rotated monthly
2. **Seen DB** (`seen_db`) — dedup index, never rotated, no GC

### Active DB tables

#### `items` (master content)

```sql
-- Identity
item_id      TEXT PRIMARY KEY      -- sha256(source_name + guid|url)
source_name  TEXT NOT NULL
guid         TEXT
url          TEXT
title        TEXT
author       TEXT

-- Time
published_at TEXT                  -- RSS pubDate (ISO 8601 UTC)
ingested_at  TEXT NOT NULL
ingested_by  TEXT NOT NULL         -- pipeline_name of the entry pipeline that produced this row;
                                   -- used by rotation to compute reach(item) over the DAG
updated_at   TEXT NOT NULL

-- RSS originals
tag          TEXT
raw_summary  TEXT
raw_content  TEXT
raw_json     TEXT NOT NULL

-- Enrichment products
fulltext         TEXT
attachments_json TEXT
summary          TEXT
score            REAL
tags_json        TEXT
extras_json      TEXT

-- Global tombstone (terminal, applies across all branches; see Drop semantics below)
dropped          INTEGER NOT NULL DEFAULT 0
dropped_reason   TEXT
dropped_at       TEXT
```

Indexes: `(source_name)`, `(published_at)`, `(dropped)`.

**Removed (vs. earlier draft)**: `dropped_by_stage`, `is_duplicate`, `duplicate_of`, `last_stage_*`, `last_pipeline_*`. These coupled global state to per-branch decisions and broke DAG forks. Per-branch processing state lives in `pipeline_<P>` and `stage_<S>` tables; query those for branch-specific status.

### Drop semantics (branch-local vs. global)

Three levels of drop, with a defined promotion rule from in-stage to per-pipeline at run end:

| Level | Where recorded | Effect |
|---|---|---|
| **In-stage drop** (filter / limit / dedupe / cross_dedupe rejects an item this run) | `stage_<S>.status='dropped'` AND a row in `stage_attempt_log`. In-memory removal from working set so subsequent stages skip it. | Within this pipeline run only: item bypasses all later stages. **At run end, automatically promoted to per-pipeline drop** (see below). |
| **Per-pipeline drop** (terminal for this pipeline) | `pipeline_<P>.status='dropped'` | Item won't re-enter P; **does not** affect P's siblings or other branches. |
| **Global tombstone** (item is permanently dead) | `items.dropped=1` | Item invisible to all pipelines forever; written ONLY by `post_run_cleanup_*_global` or explicit operator action (`sluice items kill`). |

**Promotion rule (run end):** at the end of pipeline P's run, the writer transaction includes one `pipeline_<P>` upsert per item processed this run:

- If item has any `stage_<S>.status='dropped'` (where pipeline_name=P) from this run → `pipeline_<P>.status='dropped'`, error_msg names the rejecting stage.
- Else if any `stage_<S>.status='failed'` reached `attempts >= max_attempts` → `pipeline_<P>.status='failed'`.
- Else if all stages `done` → `pipeline_<P>.status='success'`.
- Else (some stages failed but under max_attempts, item should be retried next run) → **no `pipeline_<P>` row written this run**; item remains eligible.

Eligibility query treats `pipeline_<P>` row presence with `status IN ('success', 'dropped', 'failed')` as "P is terminal for this item, skip". Absence (no row) = "still eligible".

Sibling pipelines reading the same item via the DAG see no per-pipeline drop — pipeline_<P> row is scoped to P alone.

#### `pipelines` (run master)

```sql
id            INTEGER PRIMARY KEY AUTOINCREMENT
run_key       TEXT NOT NULL UNIQUE   -- stable identity used for sink idempotency:
                                     --   "<task>__<pipeline>__<cycle_anchor_iso>"
                                     --   cycle_anchor = task.window.anchor's prev occurrence at run start
task          TEXT NOT NULL
pipeline_name TEXT NOT NULL
cycle_anchor  TEXT NOT NULL          -- ISO 8601, the anchor occurrence this run belongs to
trigger       TEXT NOT NULL          -- 'cron' | 'manual'
started_at    TEXT NOT NULL
finished_at   TEXT
status        TEXT NOT NULL          -- 'running' | 'success' | 'failed' | 'partial'
items_in      INTEGER
items_out     INTEGER
error_msg     TEXT
```

`run_key` is reused across retries when sink emission failed in a prior run — see "Run_key reuse for sink retry" in Data flow. Sink emissions (keyed by `(run_key, sink_id, ordinal)`) deduplicate naturally across retries, even for multi-chunk sinks. For sinks that emit a single atomic message, `ordinal` is always 0.

**UPSERT semantics on retry**: when a run starts with a reused `run_key` (because some eligible item has a `pipeline_<P>_sink_pending` row pointing to it), the runner UPDATEs the existing `pipelines` row to `status='running'`, refreshes `started_at`, clears `finished_at`, and resets `items_in`/`items_out`/`error_msg`. Drain queries (`SELECT count(*) FROM pipelines WHERE status='running'`) correctly observe the retry. When the run finishes, the row is updated again with terminal status. A fresh `run_key` (no pending state) INSERTs a new row.

#### `stages` (run master)

```sql
id              INTEGER PRIMARY KEY AUTOINCREMENT
pipeline_run_id INTEGER NOT NULL     -- FK pipelines.id
stage_name      TEXT NOT NULL
started_at      TEXT NOT NULL
finished_at     TEXT
status          TEXT NOT NULL
items_in        INTEGER
items_out       INTEGER
error_msg       TEXT
```

#### `pipeline_<pipeline_name>` (per-item current state, one table per pipeline)

This is the **current state** of each item with respect to one pipeline. Upserted on each pipeline run (PK is `(item_id)` since it represents one row per item per pipeline).

```sql
item_id           TEXT PRIMARY KEY     -- FK items.item_id
last_run_id       INTEGER NOT NULL     -- FK pipelines.id, the run that produced current state
status            TEXT NOT NULL        -- per-item terminal state for this pipeline:
                                       --   'success'  = ran all stages successfully through to sinks (or to DB)
                                       --   'dropped'  = some stage in the run rejected the item (filter/limit/dedupe);
                                       --                or post_run_cleanup abandoned it
                                       --   'failed'   = at least one stage hit max_attempts; item won't be retried until manual reset
processed_at      TEXT NOT NULL
error_msg         TEXT                 -- present on 'failed' or for 'dropped' includes which stage rejected
```

Note: there is no per-pipeline-run history table for items. The history is reconstructable by joining `stage_<*>` audit rows (which keep `pipeline_run_id`) with `pipelines` master.

The `pipelines` master table's `status` is run-level: `'running' | 'success' | 'failed' | 'partial'`. A run is `partial` when some items succeeded and some ended up dropped/failed.

#### `pipeline_<pipeline_name>_sink_pending` (sink retry index, one per pipeline that has sinks)

Records items that ran through pipeline P but whose sinks didn't fully emit, so the next P run reuses the same `run_key` (preserving `sink_emissions` ordinal dedup).

```sql
item_id TEXT PRIMARY KEY            -- one row per stuck item
run_key TEXT NOT NULL                -- the run_key whose sinks failed; sink_emissions ordinals reuse this key
pending_since TEXT NOT NULL
```

Inserted in tx2 when sinks fail; deleted in tx2 of a subsequent run when sinks fully emit and `pipeline_<P>.status='success'` is written. Pipelines without `sinks` do not have this table.

#### `stage_<stage_name>` (per-item current state for a stage)

A single stage instance can be reused by multiple pipelines (e.g., `sort_by_score` shared by `report_telegram` and `report_email`). The `pipeline_name` column disambiguates rows so resume queries don't cross-contaminate branches.

This table stores **current state** per `(pipeline_name, item_id)` pair. Each pipeline run **upserts** the row (REPLACE semantics): on success → status='done' with attempts unchanged; on failure → status='failed' with attempts++ from the prior row; on dropped → status='dropped'. Full attempt history lives in the separate `stage_attempt_log`.

```sql
pipeline_name      TEXT NOT NULL        -- which pipeline owns this row
item_id            TEXT NOT NULL        -- FK items.item_id
status             TEXT NOT NULL        -- 'done' | 'failed' | 'dropped' | 'skipped'
attempts           INTEGER NOT NULL DEFAULT 0  -- cumulative across runs; ++ on each failed attempt
last_stage_run_id  INTEGER NOT NULL     -- FK stages.id, run that produced current state
last_pipeline_run_id INTEGER NOT NULL   -- FK pipelines.id (denormalized)
last_processed_at  TEXT NOT NULL
last_error_class   TEXT
last_error_msg     TEXT

PRIMARY KEY (pipeline_name, item_id)
```

Indexes: `(item_id)`, `(last_pipeline_run_id)`, `(status)`.

Resume query: `SELECT 1 FROM stage_<S> WHERE pipeline_name=? AND item_id=? AND status='done'` → if found, skip stage S for that item this run.

#### `stage_attempt_log` (single shared table, append-only)

Tracks every individual stage attempt across all stages and pipelines for full audit. Lets operators inspect why a stage ended up at `attempts=N` and what each retry's error was.

```sql
id                INTEGER PRIMARY KEY AUTOINCREMENT
stage_run_id      INTEGER NOT NULL     -- FK stages.id
pipeline_run_id   INTEGER NOT NULL
pipeline_name     TEXT NOT NULL
stage_name        TEXT NOT NULL
item_id           TEXT NOT NULL
attempt_no        INTEGER NOT NULL     -- monotonic per (pipeline_name, stage_name, item_id)
status            TEXT NOT NULL        -- 'done' | 'failed' | 'dropped' | 'skipped'
processed_at      TEXT NOT NULL
error_class       TEXT
error_msg         TEXT
```

Indexes: `(pipeline_name, stage_name, item_id)`, `(stage_run_id)`.

This is shared (single table) instead of per-stage because audit log doesn't benefit from per-stage table partitioning, and shared makes "show me everything that touched item X" queries simple.

`stage_<name>` and `pipeline_<name>` tables are created at task migration time by the loader from config. Adding a new stage or pipeline name requires running `sluice migrate <task>`.

#### Existing tables retained

- `sink_delivery_log` — retained, but PK fields change to use the new `run_key` from `pipelines.run_key`. Schema:
  ```sql
  id            INTEGER PRIMARY KEY AUTOINCREMENT
  run_key       TEXT NOT NULL          -- FK pipelines.run_key (encodes task + pipeline)
  sink_id       TEXT NOT NULL
  sink_type     TEXT NOT NULL
  attempt_at    TEXT NOT NULL
  ordinal       INTEGER NOT NULL
  message_kind  TEXT NOT NULL
  recipient     TEXT
  external_id   TEXT
  status        TEXT NOT NULL
  error_class   TEXT
  error_msg     TEXT
  ```
- `sink_emissions` — retained; PK changes to `(run_key, sink_id, ordinal)` to support multi-chunk sinks where one sink emits N messages per run (e.g., long Telegram digests split into multiple posts). One row per successful chunk; partial-chunk failures leave a row for each succeeded chunk. Re-runs within the same `run_key` skip already-emitted ordinals and retry only the missing ones.
  ```sql
  run_key     TEXT NOT NULL
  sink_id     TEXT NOT NULL
  ordinal     INTEGER NOT NULL          -- chunk index within this (run_key, sink_id); 0 for atomic-single-message sinks
  sink_type   TEXT NOT NULL
  message_kind TEXT
  external_id TEXT                       -- platform-side id (telegram message_id, email Message-ID, etc.)
  emitted_at  TEXT NOT NULL
  PRIMARY KEY (run_key, sink_id, ordinal)
  ```
- `attachment_mirror` — semantics unchanged
- `gc_log` — semantics unchanged

#### Existing tables retired

- `seen_items` (replaced by seen DB)
- `failed_items` (replaced by stage_<name>.status='failed')
- `run_log` (replaced by `pipelines`)
- `url_cache` (items.fulltext serves as cache; fetcher_apply respects `skip_if_field_longer_than`)

### Seen DB

Single table, never rotated, append-only:

```sql
CREATE TABLE item_seen (
  item_id     TEXT PRIMARY KEY,
  source_hash TEXT NOT NULL,         -- per-source-type natural key:
                                     --   rss → guid if present, else url
                                     --   future API source → that source's natural id field
                                     -- stored for human inspection / debugging only;
                                     -- dedup decisions key off item_id, not source_hash
  archived_at TEXT NOT NULL
);
```

**Dedup at `fetch_rss`** (preventing re-fetch of permanently-known items while still allowing retry of in-flight ones):

An RSS source's just-fetched item with computed `item_id` is **skipped** (not re-ingested) only if any of:

- `item_seen.item_id` exists (item was archived from a previous active DB), OR
- `pipeline_<P>.item_id` exists with `status IN ('success', 'dropped', 'failed')` for the current entry pipeline `P` (item already reached a terminal state in P), OR
- `items.dropped = 1` for this `item_id` (global tombstone)

Otherwise the item is re-ingested (or, if already in `items`, the existing row is kept and the entry pipeline's run includes it as input). This means: items that exist in active `items` but have **no terminal `pipeline_<P>` row yet** (e.g., crashed mid-run, retry pending) are **picked up again** by the next entry-pipeline run. They are merged with newly-fetched source items into one input set for P.

Concretely, entry pipeline P's input set this run =
- (fresh source items not skipped per above)
  ∪
  (items in active DB whose `source_name` matches one of P's declared sources AND `items.dropped = 0` AND no `pipeline_<P>` row exists with `status IN ('success','dropped','failed')`)

`fetch_rss` writes new rows to `items` for fresh items only. The merged "carry-over" items are not re-inserted (PK conflict avoided via the eligibility split above).

**Write order** (consistency-critical):
1. `fetch_rss` discovers candidate items, applies the skip rules, decides on the (fresh, carryover) split.
2. tx1 commits to active DB: new rows into `items`, stage_<*> rows, stages rows, `pipelines` UPSERT.
3. **After tx1 ack** (and before tx2 starts), the writer thread inserts new `item_seen` rows (only for item_ids freshly inserted into `items` in step 2). Best-effort — a failure here is logged but doesn't block tx2 or roll back tx1; on next fetch the active DB lookup still catches the same item_ids.
4. tx2 commits later (sink results folded in). tx2 outcome does not affect step 3 — `item_seen` is unaffected by sink success/failure since it tracks identity, not delivery.

This ordering means an `item_seen` hit is **always backed by either an active items row or a previously-archived item** — no "phantom seen" state. Rotation only inserts into seen DB for archive-set items (those leaving active); carryover items already have their seen rows from this step. Active-only seen rows are reconciled at rotation time as a safety net.

**Rotation safety**: rotation drains in-flight pipeline runs before swapping the active DB. See "Active DB rotation" below for the full procedure. If drain timeout exceeds, rotation aborts and tries again next month.

## Runtime Semantics

### Data flow

1. Pipeline P's cron fires (or `sluice run --pipelines P` invoked).
2. **Read** (pipeline thread): query active DB for eligible items (see Eligibility).
3. **In-memory** (pipeline thread): construct `Item` objects with all columns from items + previously-completed stages' outputs already merged.
4. **Stages** (pipeline thread): run each stage in `pipeline.stages` order, in-memory, mutating `Item` objects.
   - Stages that filter (filter, dedupe, cross_dedupe, limit) mark dropped on the item; subsequent stages skip dropped items.
   - Resume behavior: for items already having `stage_<si>.status='done'` from a prior run, skip stage `si` (the item retains values previously written to items columns).
5. **Submit pre-sink write job** (pipeline thread → writer queue):
   - One job (call it tx1) containing: items column updates + stage_<*> rows + stages rows + `pipelines` row UPSERT (`status='running'`; if a row with this `run_key` already exists from a prior failed run, UPDATE it: refresh `started_at`, clear `finished_at`/`items_in`/`items_out`/`error_msg`, set `status='running'`).
   - **Not in tx1**: `pipeline_<P>` per-item terminal rows and `pipeline_<P>_sink_pending` updates. Both are deferred to tx2 so a sink failure in step 7 doesn't leave items prematurely terminal and so sink_pending state matches sink outcome.
   - Pipeline thread blocks on writer ack.
6. **Writer thread (active DB tx1)**: opens transaction, applies tx1, commits, acks.
6b. **Writer thread (seen DB)**: after tx1 ack, writer inserts new `item_seen` rows (only for items newly inserted into `items` in this run). Best-effort; failures logged but not rolled back.
7. **Sink emission** (pipeline thread, only for pipelines with `sinks`): for each declared sink, for each chunk (ordinal), check `sink_emissions(run_key, sink_id, ordinal)` — if a row already exists from a previous retry, skip; otherwise emit. After each emission attempt, immediately submit a single-row write job to the writer queue:
   - On success: INSERT into `sink_emissions(run_key, sink_id, ordinal, ...)` AND a `sink_delivery_log` row with status='success'. Wait for ack before continuing to the next chunk/sink.
   - On failure: INSERT into `sink_delivery_log` with status='failed' and error fields. No `sink_emissions` row. Wait for ack, then continue (other sinks/chunks may still be tried; failure doesn't abort emission of the rest).
   These are small per-attempt writes, separate from tx1 / tx2; each is its own short transaction. This way ordinal-level dedup is durable the moment a chunk lands externally — a process crash mid-emission doesn't cause double-delivery on retry.
8. **Submit terminal write job** (pipeline thread → writer queue) — `tx2`:
   - `pipeline_<P>` per-item terminal upserts (per run-end promotion rule, with sink results folded in)
   - `pipeline_<P>_sink_pending` inserts for sink-failed-success-eligible items, OR deletes for items that just succeeded a sink retry
   - Final `pipelines` row update (Phase 1 + Phase 2 status, finished_at, items_in/items_out, error_msg)
   - **Not in tx2**: `sink_emissions` / `sink_delivery_log` rows — those were written immediately at step 7.
   - For pipelines without sinks: tx2 fires immediately after tx1.
9. **Writer thread (active DB tx2)**: applies tx2, commits, acks.

**Per-item terminal status with sinks**: for pipelines with `sinks`, the run-end promotion rule (which writes `pipeline_<P>.status` per item) is applied in tx2 with sink outcome folded in:
- An item that would have been `success` per item-level rules is written `pipeline_<P>.status='success'` only if all sinks for this run emitted successfully (all required ordinals present in `sink_emissions`).
- If any sink failed (any required ordinal missing), **do not write a `pipeline_<P>` row** for any item that would have been `success`. Those items remain non-terminal. Additionally, **insert a row into `pipeline_<P>_sink_pending(item_id, run_key, pending_since)`** so the next run reuses the same `run_key` regardless of cycle anchor advancement, preserving `sink_emissions(run_key, sink_id, ordinal)` dedup.
- Items with item-level terminal status of `dropped` or `failed` (max_attempts) get their `pipeline_<P>` row written regardless of sink outcome — sinks can't unstick those.

**Run_key reuse for sink retry**: when pipeline P's cron fires (or manual run), the runner computes a fresh `run_key` from the current cycle anchor by default. Before inserting a new `pipelines` row, it checks `pipeline_<P>_sink_pending` for any eligible item; if any rows exist, the runner adopts the oldest `run_key` from that table as the run's `run_key` instead of the fresh-cycle key. Items whose pending `run_key` differs from the adopted key are deferred to a later cron. The `pipelines` row for the adopted `run_key` is UPSERTed (see UPSERT semantics in `pipelines` schema): `status='running'`, `started_at` refreshed, `finished_at` cleared.

If `pipeline_<P>_sink_pending` is empty for eligible items, the run uses the fresh-cycle `run_key` and INSERTs a new `pipelines` row.

After a successful retry that emits all sinks, tx2 writes `pipeline_<P>.status='success'` for the affected items AND deletes their rows from `pipeline_<P>_sink_pending` in the same transaction.

A pipeline run is atomic at tx1 (items columns + stage audit + `pipelines.status='running'` UPSERT) and atomic at tx2 (per-item terminal + run summary + `pipeline_<P>_sink_pending` insert-or-delete). `sink_emissions` and `sink_delivery_log` rows are written incrementally during step 7 (per emission attempt, in their own short transactions) so ordinal-level dedup is durable as chunks land externally. Sink failures degrade `pipelines.status` (set in tx2) but never silently mark items terminal.

### Eligibility query

Run-level gating (does P run at all?) and item-level selection (which items does P process?) are computed separately.

**Run-level gating** (evaluated when P's cron fires):

- For every U in `upstream_all`: a `pipelines` row with `pipeline_name=U`, `status='success'`, and `cycle_anchor >= P.last_cycle_anchor_success` (or any if P has never succeeded) must exist. If not, P's run is skipped this cycle.
- For at least one U in `upstream_any` (if non-empty): same condition.

**Item-level selection** (within P's run):

For pipeline P with `upstream_all=U_A`, `upstream_any=U_B`, `input_mode=M`:

```sql
SELECT items.* FROM items
WHERE dropped = 0                                    -- global tombstone
  AND <items eligible by upstream union/intersection per M, expanded below>
  AND NOT EXISTS (                                   -- P has reached a terminal state for this item
    SELECT 1 FROM pipeline_<P>
    WHERE item_id = items.item_id
      AND status IN ('success', 'dropped', 'failed')
  )
```

A pipeline_<P> row with `status='failed'` is also terminal for this item — it stays excluded until manual retry resets it. A missing row OR (rare) a row with a non-terminal value means "still eligible".

Upstream selector clause:

- `M = "union"` (default): `EXISTS (SELECT 1 FROM pipeline_<U> WHERE item_id=items.item_id AND status='success')` for **any** U in `U_A ∪ U_B`. Concretely: a chain of `OR EXISTS` clauses, one per upstream pipeline name. Note: only `status='success'` from upstream propagates items downstream; upstream-`dropped` and upstream-`failed` items do not become input to P.
- `M = "intersection"`: `EXISTS ...` with `status='success'` for **every** U in `U_A`. Chain of `AND EXISTS`.

Per-stage resume within P (after items selected): for each stage S in P, skip the stage on items where `EXISTS (SELECT 1 FROM stage_<S> WHERE item_id=? AND pipeline_name='P' AND status='done')`. Volatile stages (`sort`, `render`) are exempt — see Resume rules below.

**Entry pipelines (with `sources`)** do NOT use the upstream eligibility query above. Instead, `fetch_rss` constructs P's input set per the rule defined in **Seen DB → Dedup at fetch_rss**: fresh source items (excluding those terminal in P, terminal-archived in seen DB, or globally tombstoned) UNION items already present in active `items` whose `source_name ∈ P.sources` and which lack a terminal `pipeline_<P>` row. Per-stage resume rules within P apply identically to those listed above.

### Time window

At pipeline P's run start, the runner computes the **task cycle window** `[anchor_prev_occurrence - length, anchor_prev_occurrence]` based on `task.window.anchor` cron and `length`. `anchor_prev_occurrence` is the most-recent moment matching the cron at-or-before `now`.

Entry pipelines (`fetch_rss`) filter source items to those with `published_at` in the window. Downstream pipelines do **not** re-filter by window (upstream already did).

### Failure handling and resume

- **Stage error on item X**: writer transaction upserts `stage_<S>` row → `status='failed'`, `attempts = old_attempts + 1`, `last_error_*` set. An append-only row is also written to `stage_attempt_log`. The in-memory item is removed from this run's working set (X's later stages skipped this run).
- **Pipeline run-level status**: `pipelines.status` is computed in two phases.
  - **Phase 1 — items base status** (computed at tx2 time):
    - `success` if **either** items_in = 0 (no eligible items, no-op run) **or** every item processed reached per-item `success`
    - `success` (also) if items_in > 0 but every item ended up `dropped` (a routine drop-only run is a successful no-op; nothing went wrong)
    - `partial` if some items succeeded and some ended `dropped`/`failed`/non-terminal; or all items ended `failed` due to stage max_attempts (operator visibility)
    - `failed` only if an unrecoverable run-level error aborted the whole run (config error, infra crash, writer thread death). `error_msg` captures it.
  - **Phase 2 — sink degradation** (applied at tx2 for pipelines with `sinks`):
    - **No-op exemption**: if there is no sink payload to emit (items_in = 0, or all items ended up `dropped` so no item reached the sink-feeding render output), Phase 2 does nothing — base status is preserved. A no-emission run is not a failed sink run.
    - If sinks were attempted and all declared sinks emitted successfully → keep base status
    - If sinks were attempted and at least one sink succeeded but others failed → degrade `success` → `partial`; `partial` stays `partial`
    - If sinks were attempted and all sinks failed → degrade `success`/`partial` → `failed`; `error_msg` records the sink failures
  - For pipelines without `sinks`, only Phase 1 applies.
- **Per-item terminal promotion at run end** — for each item processed this run, the writer transaction upserts `pipeline_<P>`:
  - If item had any `stage_<S>.status='dropped'` written this run → `pipeline_<P>.status='dropped'`.
  - Else if item had any `stage_<S>.status='failed'` AND `attempts >= max_attempts` for that stage → `pipeline_<P>.status='failed'`, `error_msg='stage_<S>_max_attempts'`.
  - Else if all stages reached `done` → `pipeline_<P>.status='success'`.
  - Else (item had a transient stage failure, attempts still under max_attempts) → **no `pipeline_<P>` row written**; eligibility re-selects X next cron.
- **Resume on next P cron** (when X is re-eligible because no terminal pipeline_<P> row): runner re-applies per-stage resume:
  - **Persistent stages**: skip if `stage_<S>.status='done'` for `(pipeline_name=P, item_id=X)`. Resume from the first stage whose row is missing or `status IN ('failed','skipped')` (note: not 'dropped' — a dropped row means item already left the run).
  - **Volatile stages** (`sort`, `render`): always re-execute on every run, regardless of prior `done`. Their outputs (in-memory ordering / rendered text) are not persisted; skipping them would leave the pipeline with no sink-ready data.
- **Manual reset**: `sluice retry --task T --pipeline P [--item ID]` deletes the `pipeline_<P>.<item>` row AND clears `stage_<*>` rows where `pipeline_name=P` and `item_id=ID`; the `stage_attempt_log` history is preserved. Next cron retries from scratch.
- **Sink idempotency**: because volatile stages always re-run, sink-emit retries always have a fresh in-memory render context. Sinks themselves are deduped via `sink_emissions(run_key, sink_id, ordinal)` — a partial-emit retry on the same `run_key` skips already-emitted ordinals and retries only the missing chunks.

### Cleanup

Two cleanup options at the pipeline level:

- `post_run_cleanup = "drop_unprocessed_upstream_local"` (branch-local):
  At the end of P's run, identify cleanup candidates as items where ALL of the following hold:
  - `pipeline_<U>.status='success'` exists for at least one U in P's upstream
  - no `pipeline_<P>` row exists for the item (no terminal status from any prior P run)
  - no `pipeline_<P>_sink_pending` row exists for the item (sink retry would still need it)
  - the item was NOT in this run's working set (i.e., not selected by eligibility this run; cleanup should never override the just-completed run's terminal decisions)

  Insert `pipeline_<P>` rows with `status='dropped'`, `error_msg='abandoned_by_<P>'`. Effect: those items will not re-enter P (or P's downstream), but **other branches** of the DAG sharing U as an upstream are unaffected.

  Used on daily-report pipelines so items not summarized in time stop accumulating in P's queue.

  **Skip rule**: if the just-completed P run had any sink failure (Phase 2 degraded the run to `partial`/`failed`), `post_run_cleanup` is skipped this run — it would race with the sink retry that's about to use `pipeline_<P>_sink_pending`.

- `post_run_cleanup = "drop_unprocessed_upstream_global"` (terminal tombstone):
  Same selection rules as `_local` (including sink-pending and just-completed-run exclusions, and the skip rule on sink failure). Additionally sets `items.dropped=1`, `dropped_reason='abandoned_by_<P>'`, `dropped_at=<now>`. The item is now invisible to all pipelines. Use only when you're sure no other branch should still process the item.

Default is no cleanup. Operators can also `sluice items kill --task T --item ID` to explicitly tombstone.

### Concurrency

If P's cron fires while the previous P run is still executing, Prefect skips the new trigger (default behavior; `pause_on_overlap` semantics). Long-running pipelines that need quick re-fires must shorten their work or split into smaller pipelines.

### Active DB rotation

Once monthly (cron in `sluice.toml` `gc.schedule.cron`), the global GC flow runs **drain-and-rotate-with-carryover** per enabled task:

1. **Pause Prefect triggers** for the task's pipelines.
2. **Drain in-flight runs**: poll `pipelines.status='running'` for the task; wait until zero or `rotation.drain_timeout` elapses. Abort on timeout (resume triggers, retry next month).
3. **Determine each item's reachable pipeline set** (`reach(item)`):
   - The entry pipeline that produced the item is read from `items.ingested_by` (the pipeline_name that wrote the items row at fetch time — see items schema).
   - From that entry pipeline, walk the DAG forward (via every other pipeline's `upstream_all` / `upstream_any` lists), but **restrict the walk to pipelines listed in `task.deployments.pipelines`** (or all pipelines if that field is `"all"`). Disabled / undeployed pipelines are not part of `reach(item)` — items don't owe work to pipelines that aren't running.
   - `reach(item)` is independent of what other entry pipelines exist; a `fetch_blog` item only owes work to enabled pipelines reachable from `fetch_blog`, never to `fetch_releases`.
   - If a pipeline is removed from `task.deployments.pipelines` while items still owe work to it, those items become archive-eligible at the next rotation (their pending state is preserved in the archive). To resume them, re-enable the pipeline before rotation.
4. **Compute archive vs. carryover** based on `reach(item)`:
   - **Carryover** (preserved): items where some pipeline P ∈ reach(item) does NOT yet have a terminal `pipeline_<P>` row (status ∈ {success, dropped, failed}) AND `items.dropped = 0`.
   - **Archive** (gz'd, dropped from new active DB): items terminal in every P ∈ reach(item) OR globally tombstoned (`items.dropped = 1`).
5. **Open a new active DB at a temp path** with base schema + per-stage / per-pipeline tables.
6. **Copy carryover** from old active to new active. For each carryover `item_id`, copy:
   - `items` row
   - `pipeline_<P>` rows for every P ∈ reach(item)
   - `pipeline_<P>_sink_pending` rows for every P ∈ reach(item) (essential — tells next P run which `run_key` to reuse)
   - `stage_<S>` rows where `pipeline_name=P` for every P ∈ reach(item)
   - `stage_attempt_log` rows for the same (item_id, pipeline_name) tuples
   - `pipelines` master rows referenced by any of the above
   - `stages` master rows referenced by any of the above
   - **`sink_emissions` rows** keyed by any `run_key` referenced by a copied `pipeline_<P>_sink_pending` row (preserves ordinal dedup for the pending retry)
   - `sink_delivery_log` rows with those same run_keys (history retained for ops visibility)
7. **Reconcile seen DB**: for **archive-set** items only, `INSERT OR IGNORE INTO seen.item_seen` so they remain dedup-skipped after the active row is gone. Carryover items already have a `seen.item_seen` row (inserted at initial fetch via the write-order rule); not touched.
8. **Swap files atomically**: `mv data/<task>.db data/archive/<task>-YYYY-MM.db.tmp`; `mv <new active temp> data/<task>.db`; `gzip data/archive/<task>-YYYY-MM.db.tmp` → `<task>-YYYY-MM.db.gz`.
9. **Resume Prefect triggers**.

The carryover step is critical: items pending a stage retry (transient failure under max_attempts) keep their `stage_<S>.status='failed', attempts=N` rows so resume picks up where it left off. Items with sink-pending retries keep both their `pipelines.run_key` (so retry uses the same key) and the `sink_emissions` ordinals already delivered (so retry doesn't duplicate).

If a task has so many in-flight items that carryover dominates the new DB, that's a signal that pipelines aren't draining their backlog — operator should investigate; rotation still succeeds.

Configurable in `sluice.toml`:
```toml
[gc.rotation]
drain_timeout = "30m"
archive_dir   = "data/archive"
```

## CLI

```
sluice run                              # error: must specify
sluice run -t <task> -p <pipeline>      # run one pipeline once (manual)
sluice run -t <task> -p A -p B -p C     # run pipelines A → B → C in sequence (manual)
                                        # long aliases: --task, --pipeline
sluice retry --task <name> --pipeline P [--item <id>]
                                        # delete failed audit rows for item(s),
                                        # next P cron picks them up;
                                        # if --remote, also calls Prefect API to trigger P now
sluice deploy                           # register all enabled tasks × pipelines as Prefect deployments
sluice items list <task>                # show recent items + per-pipeline status (joined from pipeline_<*>)
sluice items show <task> <item_id>      # full provenance: items row + every stage/pipeline detail row across all branches
sluice items kill <task> <item_id>      # mark items.dropped=1 (terminal tombstone)
sluice migrate <task>                   # apply schema migrations + (re)create per-stage and per-pipeline tables
```

## Prefect Deployment

`sluice deploy` iterates all enabled tasks, then for each task iterates `task.deployments.pipelines` (or all pipelines if `"all"`). Each pipeline becomes one Prefect deployment:

- Deployment name: `sluice-<task_name>-<pipeline_name>`
- Schedule: `CronSchedule(cron=pipeline.cron, timezone=task.timezone)`
- Parameters: `{task: <task_name>, pipeline: <pipeline_name>, config_dir: <abs path>}`
- No automatic Prefect retry (manual retry is via `sluice retry --remote`)

The example task above produces five deployments:
- `sluice-airflow-fetch_blog` (cron 0 */6 * * *)
- `sluice-airflow-fetch_releases` (cron 0 */4 * * *)
- `sluice-airflow-summarize_nightly` (cron 0 22 * * *)
- `sluice-airflow-report_telegram` (cron 30 22 * * *)
- `sluice-airflow-report_email` (cron 0 7 * * *)

## Migration from v1.1

Forced rewrite. Existing configs and DB are dropped:

1. Move `configs/pipelines/<X>.toml` (old) aside; rewrite as `configs/tasks/<X>/{task,sources,sinks,stages,pipelines}.toml` (new).
2. Delete old `data/sluice.db`.
3. Run `sluice migrate <task>` to create active and seen DBs with the new schema and per-stage/per-pipeline tables.
4. Run `sluice deploy` to register Prefect deployments.

A migration helper `sluice scaffold-task --from-old configs/pipelines/X.toml` (best-effort) emits a starter `task.toml` + split sources/sinks/stages/pipelines from the old single-file layout. Hand-tuning required.

## Testing Strategy

- **Unit tests**:
  - Loader: parse all five files; resolve cross-references; reject invalid DAG (cycle, dangling reference, mixed sources+upstream, fetch_<type> in non-source pipeline, sort/render last without sinks).
  - Per-stage and per-pipeline table generation from config.
  - Eligibility query for upstream_all + upstream_any combinations including DAG forks.
  - Resume: stage_<si>.done items skip si in subsequent runs.
  - post_run_cleanup soft drop logic.
  - Stage attempts increment, max_attempts cutoff, retry CLI reset.
  - Time window computation across timezones.
  - Writer thread: queue ordering, transaction atomicity, ack semantics.

- **Integration tests**:
  - Run a 5-pipeline task (the airflow example) end-to-end against in-memory SQLite, asserting items + audit tables at every checkpoint.
  - Inject failure mid-pipeline; verify resume picks up correctly.
  - DAG fork: two report pipelines downstream of one summarize; both consume independently and sink_delivery_log shows two emissions.
  - Active DB rotation under load: rotation drains in-flight runs first; archive-set items get inserted into seen DB; carryover-set items (with their stage/pipeline/sink_pending state) are copied to a fresh active DB; per-item retry resumes correctly post-rotation including sink-pending retries reusing the carried `run_key`.

## Open Questions / Risks

- **Dynamic per-stage / per-pipeline tables** mean schema is partly config-driven. Mitigation: loader runs schema reconciliation on every `sluice migrate`; stage type registry validates stage `type` before deploy.
- **Resume granularity = stage** for persistent stages: an LLM stage that writes partial output before raising leaves items columns mutated but stage row not done. On resume the stage re-runs and overwrites. Acceptable for idempotent stages; loader emits a warning when configured stages are flagged non-idempotent in the registry.
- **Writer thread crash**: in-memory queue lost. Mitigation: pipeline thread blocks on writer ack with a timeout; on writer death, pipeline run aborts with `failed`. The next cron run will re-attempt; the existing cycle's `run_key` lets sinks dedupe partial deliveries.
- **Cycle anchor and clock drift**: `cycle_anchor` is computed at run start from `task.window.anchor` cron in the task's timezone. Long-running pipelines that span an anchor boundary still belong to the cycle in which they started. Cron skew across hosts is not handled (single-anchor source of truth = the host running the GC/scheduler).
- **`input_mode = "intersection"`** is rare; loader requires it to be explicitly set, never implicit, to avoid silently empty inputs.
- **Active DB rotation drain timeout**: if pipelines won't quiesce within the configured window, rotation is deferred. A monitoring metric on `pipelines.status='running'` count is recommended.
- **Field-name collisions across stages**: registry-fixed; if two stages declare the same write column, last-write-wins. Loader emits a warning at config validation.
- **SQLite single-writer**: writer thread already serializes; concurrent pipelines submit to one writer. Sufficient for personal-scale loads.

## Acceptance Criteria

1. The canonical airflow task above runs end-to-end on Prefect, with each pipeline as an independent deployment, and items flow through all five pipelines correctly.
2. Stopping `summarize_nightly` for a day causes that day's `report_*` runs to find no eligible items and complete without emission. The next day, a normal summarize run brings backlog up.
3. Forcing a stage failure (e.g., LLM timeout) leaves the item recoverable: subsequent pipeline runs resume from the failed stage; `sluice retry` reactivates after `max_attempts` cutoff.
4. `post_run_cleanup` on the report pipelines correctly marks items abandoned that weren't summarized in time.
5. Monthly rotation produces a compressed archive and a fresh active DB without losing dedup.
6. `sluice migrate` is idempotent.
