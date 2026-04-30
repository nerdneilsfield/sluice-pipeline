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
- `score_tag` also runs in dry-run and makes real LLM calls. If you want to skip LLM calls during dry-run, comment out the stage.

## Non-Goals

- No cross-pipeline deduplication (seen_items across different pipeline IDs).
- No semantic/embedding-based similarity for cross_dedupe.
- No HTML rendering or sanitisation for email in html_strip (that lives in the email sink).
- No cluster stage in this spec (separate design).
- No changes to the sink layer, state schema, or fetcher chain.
- No implicit parse retry in score_tag. Parse failures go directly to `on_parse_error` policy.

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
Build a dict keyed by `item.url`. Only items with a **non-empty** URL participate in this round; items with `item.url == ""` are passed through to Round 2 unchanged. When multiple items share the same URL, keep the one whose `source_id` appears earliest in `source_priority`, and merge tags from the dropped items if `merge_tags = true`. If no `source_priority` is set, keep the first item in list order.

**Round 2 — Title similarity:**
For items that survived Round 1, iterate in list order. Items with an **empty title** are skipped entirely for similarity comparison (an empty string would score `ratio = 1.0` against any other empty title). For each unprocessed item with a non-empty title, find all subsequent unprocessed items whose `difflib.SequenceMatcher(None, a.title.lower(), b.title.lower()).ratio()` ≥ `title_similarity_threshold`. All matching items form one group with the current item as anchor. Keep the anchor (or the source_priority winner within the group), drop and optionally merge tags from the rest. Continue to the next unprocessed item.

**Tag merge order (both rounds):** When merging tags from a dropped item into the kept item, append tags in the dropped item's original order, skipping exact duplicates (case-sensitive). The kept item's original tags come first.

This is a **greedy pairwise** approach, not transitive closure. If A~B and B~C but A!~C, then A's group captures B (and B is dropped), but C is compared only against remaining items.

**Output order:** The output list preserves the relative order of kept items from the original batch.

**Logging:** Each merge emits a DEBUG log with `url`, `kept_source`, `dropped_source`, `method` (`url` or `title`), and `ratio` (for title matches).

### Error Handling

- Items with empty `url` skip Round 1; items with empty `title` skip Round 2. Both pass through unchanged.
- A `title_similarity_threshold` outside `[0.0, 1.0]` raises `ConfigError` at load time.
- `source_priority` entries that don't match any item's `source_id` are silently ignored.

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
# Valid field paths:
#   top-level Item field:  "raw_summary", "title", "fulltext"
#   one-level extras key:  "extras.body"
# Invalid: "extras.a.b", "foo.bar" → ConfigError at load time
```

`fields` is required and must be non-empty. The only valid dotted path form is `extras.<key>` (exactly one dot). Any other dotted path — including `foo.bar` or `extras.a.b` — raises `ConfigError` at load time.

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
- `ConfigError` at load time if `fields` is empty, or if any path is dotted but not in the form `extras.<key>`.

---

## Stage 3: `score_tag`

### Purpose

A single LLM call per item that writes a 1–10 relevance score and a list of topic tags. Downstream stages can filter on `extras.score` or render `item.tags`. Combining score and tags in one call halves LLM cost compared to two separate `llm_stage` stages.

`score_tag` is a specialised sibling of `llm_stage` with a fixed JSON output parser and per-item error handling. Parse failures and LLM call failures are both handled per item.

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
score_field    = "score"                 # key name in item.extras; must be non-empty, no dot
tags_merge     = "append"               # "append" | "replace"
on_parse_error = "skip"                 # "skip" | "fail" | "default"
default_score  = 5                      # used when on_parse_error = "default"
default_tags   = []                     # used when on_parse_error = "default"
max_input_chars   = 8000               # truncate input field before sending to LLM
truncate_strategy = "head_tail"        # "head_tail" | "head" | "error"
```

`concurrency` is not a parameter — worker parallelism is `workers` only.

`score_field` must be a plain key name (non-empty, no `.`). A value like `"extras.score"` raises `ConfigError` at load time — the field is always written to `item.extras[score_field]`.

`truncate_strategy`:
- `"head_tail"`: keeps the first half and last half of the input field (default).
- `"head"`: keeps only the beginning.
- `"error"`: if the field value exceeds `max_input_chars` at runtime, treat it as a per-item failure governed by `on_parse_error` (same policy, not a `ConfigError`).

### LLM and Provider Failure Handling

Network errors, rate-limit errors, and provider-exhausted errors from the LLM call are treated as per-item failures and apply the same `on_parse_error` policy. In dry-run mode, these failures are only logged and do not write to `failed_items`.

`score_tag` respects `RunBudget.max_calls` and `RunBudget.max_usd`, as initialised from pipeline limits `max_llm_calls_per_run` and `max_estimated_cost_usd`. The cost preflight logs projected call count and estimated USD before starting, identical to `llm_stage`.

### LLM Output Contract

The LLM must return a JSON object:

```json
{"score": 7, "tags": ["AI", "开源", "编译器"]}
```

### JSON Parsing Rules

**Fence stripping:** If the first non-empty line of the output matches `` ```json `` or ` ``` `, and the last non-empty line is ` ``` `, strip those two fence lines before parsing. If there is non-fence text outside the fenced block, treat the entire output as unfenced (attempt `json.loads()` on the full string).

**Parsing:**
1. Strip leading/trailing whitespace.
2. Apply fence stripping as described above.
3. Call `json.loads()`. A JSON parse error → apply `on_parse_error`.

**Extracting `score`:**
- Must be present; if missing → parse error.
- Accept JSON number (`int`, `float`) or a numeric string (`"7"`, `"7.2"`).
- Convert to `float`. Reject non-finite values (`NaN`, `Inf`) → parse error.
- Round to nearest integer (`round()`), then clamp to `[1, 10]`.
- Conversion failure (e.g. `"high"`, `"N/A"`) → parse error.

**Extracting `tags`:**
- If missing → treat as `[]` (not a parse error).
- Must be a list; if not → parse error.
- Each element must be a string; non-string elements are silently dropped.
- Strip whitespace from each tag string; discard empty strings after stripping.
- No deduplication at parse time — deduplication happens during merge (see below).

### Parse / LLM Error Policy

| `on_parse_error` | Behaviour |
|---|---|
| `"skip"` | Item passes through unchanged; `score_field` and `tags` are not modified. |
| `"fail"` | Item is recorded in `failed_items` and removed from `ctx.items`. In dry-run: logged only, not written. |
| `"default"` | `default_score` (clamped int) and `default_tags` (cleaned list) are applied. |

This policy applies to parse failures, LLM call failures, and truncate errors. The parameter is named `on_parse_error` for config simplicity, but it governs all per-item `score_tag` failures. One item's failure never affects other items in the batch.

### Output Fields

**Score:** `item.extras[score_field]` → `int` in `[1, 10]`.

**Tags — `tags_merge = "append"`:**
1. Strip and filter new tags (non-empty strings only).
2. Append each new tag to `item.tags` if not already present (exact case-sensitive match).
3. Result: original tags in original order, followed by new tags not already in the list.

**Tags — `tags_merge = "replace"`:**
1. Strip and filter new tags (non-empty strings only).
2. Deduplicate in received order (case-sensitive, first occurrence wins).
3. Overwrite `item.tags` with the cleaned list.

### LLM Infrastructure

Reuses `ProviderPool`, `RunBudget`, and the worker semaphore from `LLMStageProcessor`. No new LLM plumbing required.

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
# Recommended order:
# html_strip → cross_dedupe → fetcher_apply → filter → score_tag → dedupe → llm_stage

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
name = "fetch_fulltext"
type = "fetcher_apply"
write_field = "fulltext"

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

- `cross_dedupe` skips URL round for items with empty `url`; skips title round for items with empty `title`.
- `cross_dedupe` removes items with identical non-empty URLs, keeping preferred-source item; merges tags in kept-item-first, dropped-item-original order, case-sensitive exact dedup.
- `cross_dedupe` uses greedy pairwise title matching; output order matches original batch order of kept items.
- `html_strip` rejects dotted paths other than `extras.<key>` with `ConfigError` at load time.
- `html_strip` discards `<script>/<style>/<template>` tag content; converts block/heading/br tags to newlines; decodes entities; collapses whitespace.
- `score_tag` `score_field` containing a dot raises `ConfigError` at load time.
- `score_tag` accepts numeric strings (`"7"`, `"7.2"`); rounds to nearest int; clamps to `[1, 10]`; rejects non-finite values.
- `score_tag` fence-strips only when the entire output is fenced; non-fence text outside the block → no stripping.
- `score_tag` applies `on_parse_error` policy to both parse failures and LLM call failures.
- `score_tag` per-item failure does not affect other items.
- `score_tag` `truncate_strategy = "error"` triggers `on_parse_error` at runtime (not a load-time error).
- `score_tag` `tags_merge = "append"` preserves original tag order then appends new, case-sensitive exact dedup.
- `score_tag` `tags_merge = "replace"` deduplicates new tags in received order before overwriting.
- `score_tag` respects `RunBudget.max_calls` and `RunBudget.max_usd` (from `max_llm_calls_per_run` and `max_estimated_cost_usd`).
- No new pip dependencies introduced by `cross_dedupe` or `html_strip`.
