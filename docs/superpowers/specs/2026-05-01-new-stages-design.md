# Sluice V1.2 — New Processor Stages: cross_dedupe, html_strip, score_tag

**Status:** Design approved for implementation planning
**Date:** 2026-05-01
**Owner:** dengqi

## Summary

Three new processor stages extending the sluice pipeline:

1. **`cross_dedupe`** — Within-batch deduplication across sources. Removes duplicate items that arrive from different RSS feeds but point to the same article, using URL matching and title similarity.
2. **`html_strip`** — Converts HTML fields to clean plain text, preserving basic semantic structure. Zero LLM cost, zero new dependencies.
3. **`score_tag`** — Single LLM call per item that produces both a relevance score (1–10) and a list of topic tags, writing both into the item for downstream use by `filter` and `render` stages.

These stages are independent of each other and of existing stages. They each follow the existing `Processor` pattern and slot into the `[[stages]]` pipeline config.

## Dry-Run Behaviour

The existing runner executes all processors in dry-run mode; only sink emission, `seen_items` writes, and `failed_items` writes are skipped. Therefore:

- `cross_dedupe` and `html_strip` run normally in dry-run (they are pure in-memory transformations).
- `score_tag` also runs in dry-run and makes real LLM calls. If you want to skip LLM calls during dry-run, use the pipeline's `--dry-run` flag only for sink testing and run `score_tag` in a separate non-dry-run pass, or comment out the stage during dry-runs.

## Non-Goals

- No cross-pipeline deduplication (seen_items across different pipeline IDs).
- No semantic/embedding-based similarity for cross_dedupe.
- No HTML rendering or sanitisation for email in html_strip (that lives in the email sink).
- No cluster stage in this spec (separate design).
- No changes to the sink layer, state schema, or fetcher chain.

---

## Stage 1: `cross_dedupe`

### Purpose

When multiple RSS sources cover the same story, the collector produces duplicate items with different `item_key` values (different guids). `cross_dedupe` removes these within the current batch before any expensive LLM calls.

### Configuration

```toml
[[stages]]
name = "cross_dedupe"
type = "cross_dedupe"
title_similarity_threshold = 0.85   # float 0.0–1.0, default 0.85
source_priority = ["rss_hn", "rss_blog"]  # source_ids in preference order; [] = first-seen wins
merge_tags = true                   # merge tags from dropped items into kept item, default true
```

`source_id` matches the `name` field of an RSS source, or the auto-generated `rss_0`, `rss_1`, etc.

### Algorithm

Two rounds, all in memory, no database access:

**Round 1 — URL match:**
Build a dict keyed by `item.url` (already canonicalised by the RSS source). When multiple items share the same URL, keep the one whose `source_id` appears earliest in `source_priority`, and merge tags from the dropped items if `merge_tags = true`. If no `source_priority` is set, keep the first item in list order.

**Round 2 — Title similarity:**
For items that survived Round 1, iterate in list order. For each unprocessed item, find all subsequent items whose `difflib.SequenceMatcher(None, a.title.lower(), b.title.lower()).ratio()` ≥ `title_similarity_threshold`. All matching items form one group with the current item as anchor. Keep the anchor (or the source_priority winner within the group), drop and optionally merge tags from the rest. Continue to the next unprocessed item.

This is a **greedy pairwise** approach, not transitive closure. If A~B and B~C but A!~C, then A's group captures B (and B is dropped), but C is compared only against remaining items. This avoids accidental over-merging of loosely related articles.

**Output order:** The output list preserves the relative order of kept items from the original batch.

**Logging:** Each merge emits a DEBUG log with `url`, `kept_source`, `dropped_source`, `method` (`url` or `title`), and `ratio` (for title matches).

### Error Handling

- Items with empty `url` and empty `title` are passed through unchanged.
- A `title_similarity_threshold` outside `[0.0, 1.0]` raises `ConfigError` at load time.
- `source_priority` entries that don't match any item's `source_id` are silently ignored (not an error — the pipeline may not always have all sources active).

---

## Stage 2: `html_strip`

### Purpose

Some RSS feeds embed HTML in `raw_summary` or other fields. `html_strip` converts those fields to clean plain text with preserved semantic structure, so downstream stages (LLM, filter, render) receive readable content rather than raw markup or bare concatenated words.

### Configuration

```toml
[[stages]]
name = "strip_html"
type = "html_strip"
fields = ["raw_summary", "fulltext"]
# Supported field paths:
#   top-level Item field:  "raw_summary", "title", "fulltext"
#   one-level extras key:  "extras.body", "extras.description"
# Deeper nesting is not supported.
```

`fields` is required and must be non-empty. Dot-path is supported for exactly one level of `extras.<key>`. Deeper nesting is not supported and will raise `ConfigError` at load time.

### Conversion Rules (per field, per item)

The converter subclasses `html.parser.HTMLParser` with the following behaviour:

**Tags that are dropped entirely (tag + inner content):**
- `<script>`, `<style>`, `<template>` — content is discarded, not emitted.

**Tags that emit structural whitespace:**
- Block-level: `<p>`, `<div>`, `<section>`, `<article>`, `<blockquote>`, `<li>`, `<td>`, `<th>` → emit `\n\n` before and after content.
- Heading: `<h1>`–`<h6>` → emit `\n\n` before and after (heading level is not preserved in plain text output).
- Line break: `<br>` → emit `\n`.

**All other tags** are stripped silently (their text content is kept).

**Post-processing:**
1. Decode HTML entities with `html.unescape()` — `&amp;` → `&`, `&nbsp;` → space, `&#39;` → `'`.
2. Collapse runs of whitespace within a line to a single space.
3. Collapse more than two consecutive newlines to two.
4. Strip leading/trailing whitespace.
5. Write the result back to the same field.

**If the field value is not a string, skip silently.**

### Error Handling

- A field path that does not exist on the item is silently skipped.
- `ConfigError` at load time if `fields` is empty or a dot-path has more than one level.

---

## Stage 3: `score_tag`

### Purpose

A single LLM call per item that writes a 1–10 relevance score and a list of topic tags. Downstream stages can filter on `extras.score` or render `item.tags`. Combining score and tags in one call halves LLM cost compared to two separate `llm_stage` stages.

`score_tag` is a specialised sibling of `llm_stage` with a fixed JSON output parser and per-item parse-error handling. Unlike `llm_stage` (which fails the whole stage on a parse error), `score_tag` handles parse failures per item.

### Configuration

```toml
[[stages]]
name = "score_and_tag"
type = "score_tag"
input_field    = "fulltext"              # field to read for LLM input; required
prompt_file    = "prompts/score_tag.md"  # Jinja2 template; required
model          = "glm/glm-4-flash"
fallback_model = "deepseek/deepseek-chat"
workers        = 8                       # parallel LLM calls (semaphore); same as llm_stage
timeout        = 60
score_field    = "score"                 # key in item.extras; default "score"
tags_merge     = "append"               # "append" | "replace"
on_parse_error = "skip"                 # "skip" | "fail" | "default"
default_score  = 5                      # used when on_parse_error = "default"
default_tags   = []                     # used when on_parse_error = "default"
max_input_chars = 8000
```

`concurrency` is not a parameter. Worker parallelism is controlled solely by `workers`, consistent with the existing `llm_stage` implementation.

### LLM Output Contract

The LLM must return a JSON object. The prompt template should instruct the model to output **only** JSON with no extra text:

```json
{"score": 7, "tags": ["AI", "开源", "编译器"]}
```

### JSON Parsing Rules

1. Strip leading/trailing whitespace from the raw LLM output.
2. If the output starts with ` ```json` or ` ``` ` (markdown fence), strip the opening fence line and the closing ` ``` ` line before parsing.
3. Call `json.loads()`.
4. Extract `score`:
   - Must be present; if missing → parse error.
   - Must be a number (int or float); if not → parse error.
   - Cast to `int`, clamp to `[1, 10]`.
5. Extract `tags`:
   - If missing → treat as `[]` (not a parse error).
   - Must be a list; if not a list → parse error.
   - Each element must be a string; non-string elements are silently dropped from the list.
6. On any parse error, apply `on_parse_error` policy.

### Parse Error Policy

| `on_parse_error` | Behaviour |
|---|---|
| `"skip"` | Item passes through unchanged; `score_field` and `tags` are not modified. |
| `"fail"` | Item is recorded in `failed_items` (if failures store is available); item is removed from `ctx.items`. |
| `"default"` | `default_score` and `default_tags` are applied as if parsing had succeeded. |

This is per-item handling. A single item's parse failure does not affect other items in the batch.

### Output Fields

- `item.extras[score_field]` → `int` in `[1, 10]`
- `item.tags`:
  - `"append"`: new tags appended and deduplicated (preserving existing order, then new tags in received order).
  - `"replace"`: existing tags overwritten with the new list.

### LLM Infrastructure

Reuses `ProviderPool`, `RunBudget`, and the worker semaphore from `LLMStageProcessor`. `score_tag` respects `RunBudget.max_usd` and logs projected call count and estimated USD before starting.

### Example Prompt Template

`prompts/score_tag.md`:

```markdown
你是一个新闻相关性评分助手。请根据以下文章内容，输出一个 JSON 对象，不要输出任何其他内容。

JSON 格式：{"score": <1-10整数>, "tags": [<标签列表>]}

评分标准：1=完全不相关，10=高度相关且有价值
标签：2-5个，中文或英文，描述文章主题

标题：{{ item.title }}
正文：{{ item.fulltext or item.raw_summary or "" }}
```

---

## Example Pipeline Placement

```toml
# Recommended order: html_strip → cross_dedupe → filter → score_tag → dedupe → llm_stage

[[stages]]
name = "strip_html"
type = "html_strip"
fields = ["raw_summary"]

[[stages]]
name = "cross_dedupe"
type = "cross_dedupe"
title_similarity_threshold = 0.85
source_priority = ["rss_hn"]

[[stages]]
name = "prefilter"
type = "filter"
mode = "keep_if_all"
rules = [
  { field = "fulltext", op = "min_length", value = 200 },
]

[[stages]]
name = "score_and_tag"
type = "score_tag"
input_field    = "fulltext"
prompt_file    = "prompts/score_tag.md"
model          = "glm/glm-4-flash"
workers        = 8
on_parse_error = "skip"

[[stages]]
name = "relevance_filter"
type = "filter"
mode = "keep_if_all"
rules = [
  { field = "extras.score", op = "gte", value = 6 },
]

[[stages]]
name = "dedupe"
type = "dedupe"
```

## Acceptance Criteria

- `cross_dedupe` removes items with identical URLs, keeping the preferred-source item and merging tags when `merge_tags = true`.
- `cross_dedupe` removes items whose title ratio ≥ threshold using greedy pairwise matching; output order matches original batch order of kept items.
- `html_strip` discards `<script>`/`<style>`/`<template>` content, converts block/heading/br tags to newlines, decodes entities, and collapses whitespace.
- `html_strip` dot-path supports exactly one level of `extras.<key>`; deeper paths raise `ConfigError`.
- `score_tag` writes `extras.score` (int 1–10, clamped) and updates `item.tags` per `tags_merge` mode.
- `score_tag` strips markdown fences before JSON parsing.
- `score_tag` `on_parse_error = "skip"` leaves item unchanged; `"fail"` records to `failed_items` and removes item; `"default"` applies configured defaults.
- `score_tag` per-item parse failure does not affect other items in the batch.
- `cross_dedupe` and `html_strip` run in dry-run mode (in-memory only). `score_tag` also runs in dry-run and makes real LLM calls.
- No new pip dependencies introduced by `cross_dedupe` or `html_strip`.
