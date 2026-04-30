# Sluice V1.2 — New Processor Stages: cross_dedupe, html_strip, score_tag

**Status:** Design approved for implementation planning
**Date:** 2026-05-01
**Owner:** dengqi

## Summary

Three new processor stages extending the sluice pipeline:

1. **`cross_dedupe`** — Within-batch deduplication across sources. Removes duplicate items that arrive from different RSS feeds but point to the same article, using URL matching and title similarity.
2. **`html_strip`** — Strips HTML tags and normalises whitespace on configurable item fields. Zero LLM cost, zero new dependencies.
3. **`score_tag`** — Single LLM call per item that produces both a relevance score (1–10) and a list of topic tags, writing both into the item for downstream use by `filter` and `render` stages.

These stages are independent of each other and of existing stages. They each follow the existing `Processor` pattern and slot into the `[[stages]]` pipeline config.

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
For items that survived Round 1, compare every pair using `difflib.SequenceMatcher(None, a.title.lower(), b.title.lower()).ratio()`. If any pair's ratio ≥ `title_similarity_threshold`, treat them as duplicates and apply the same keep/merge rule. O(n²) over the surviving batch; acceptable for typical batch sizes of 20–100 items.

**Logging:** Each merge emits a DEBUG log with `url`, `kept_source`, `dropped_source`, `method` (`url` or `title`), and `ratio` (for title matches).

### Error Handling

- Items with empty `url` and empty `title` are passed through unchanged.
- A `title_similarity_threshold` outside `[0.0, 1.0]` raises `ConfigError` at load time.
- `source_priority` entries that don't match any item's `source_id` are silently ignored (not an error — the pipeline may not always have all sources active).

---

## Stage 2: `html_strip`

### Purpose

Some RSS feeds embed HTML in `raw_summary` or other fields. `html_strip` cleans these fields so downstream stages (LLM, filter, render) receive plain text.

### Configuration

```toml
[[stages]]
name = "strip_html"
type = "html_strip"
fields = ["raw_summary", "fulltext"]   # required; dot-path supported for extras.*
```

`fields` is required and must be non-empty. Using a dot-path like `extras.body` accesses `item.extras["body"]`.

### Logic (per field, per item)

1. If the field value is not a string, skip silently.
2. Parse with `html.parser` from the Python standard library (`html.parser.HTMLParser`), stripping all tags.
3. Decode HTML entities with `html.unescape()` — e.g. `&amp;` → `&`, `&nbsp;` → space, `&#39;` → `'`.
4. Collapse runs of whitespace (spaces, tabs, newlines) to a single space; strip leading/trailing whitespace.
5. Write the cleaned string back to the same field.

### Implementation Note

Use Python's `html.parser.HTMLParser` subclass approach (feed text, collect data between tags). No third-party libraries. The email sink's lxml-based sanitiser is separate and not reused here.

### Error Handling

- A field that does not exist on the item is silently skipped.
- `ConfigError` at load time if `fields` is empty.

---

## Stage 3: `score_tag`

### Purpose

A single LLM call per item that writes a 1–10 relevance score and a list of topic tags. Downstream stages can filter on `extras.score` or render `item.tags`. Combining score and tags in one call halves LLM cost compared to two separate `llm_stage` stages.

### Configuration

```toml
[[stages]]
name = "score_and_tag"
type = "score_tag"
input_field    = "fulltext"          # field to pass to the LLM; required
prompt_file    = "prompts/score_tag.md"  # Jinja2 template; required
model          = "glm/glm-4-flash"
fallback_model = "deepseek/deepseek-chat"
workers        = 8
concurrency    = 4
timeout        = 60
score_field    = "score"             # written to extras.<score_field>, default "score"
tags_merge     = "append"           # "append" | "replace"; how to update item.tags
on_parse_error = "skip"             # "skip" | "fail" | "default"
default_score  = 5                  # used when on_parse_error = "default"
default_tags   = []                 # used when on_parse_error = "default"
max_input_chars = 8000
```

### LLM Output Contract

The LLM must return a JSON object. The prompt template should instruct the model to output **only** JSON with no extra text:

```json
{"score": 7, "tags": ["AI", "开源", "编译器"]}
```

- `score`: integer 1–10. Values outside range are clamped to `[1, 10]`.
- `tags`: list of strings. Empty list is valid.

The stage parses with `json.loads()` on the raw LLM output (after stripping markdown fences if present). On parse failure, `on_parse_error` governs behaviour:
- `"skip"`: item passes through unchanged (score and tags not set).
- `"fail"`: raises, item goes to `failed_items`.
- `"default"`: uses `default_score` and `default_tags`.

### Output Fields

- `item.extras[score_field]` → integer (e.g. `item.extras["score"] = 7`)
- `item.tags` → merged or replaced depending on `tags_merge`

When `tags_merge = "append"`, new tags are appended and deduplicated. When `tags_merge = "replace"`, existing tags are overwritten.

### LLM Infrastructure

Reuses the existing `ProviderPool`, `RunBudget`, and worker/concurrency model from `LLMStageProcessor`. No new LLM plumbing required — `score_tag` is a specialised sibling of `llm_stage` with a fixed JSON output parser.

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

### Cost Preflight

Like `llm_stage`, `score_tag` logs projected call count and estimated USD before starting, and respects `RunBudget.max_usd`.

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
input_field  = "fulltext"
prompt_file  = "prompts/score_tag.md"
model        = "glm/glm-4-flash"
workers      = 8
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

- A pipeline with `cross_dedupe` removes items with identical URLs and items whose titles exceed the similarity threshold, keeping the preferred-source item.
- Tags from dropped items are merged into the kept item when `merge_tags = true`.
- A pipeline with `html_strip` writes plain text back to the configured fields; items without the field pass through unchanged.
- A pipeline with `score_tag` writes `extras.score` (integer 1–10) and updates `item.tags` per `tags_merge` mode for each item where the LLM returns valid JSON.
- `on_parse_error = "skip"` leaves the item unchanged; `"fail"` sends it to `failed_items`; `"default"` applies configured defaults.
- All three stages pass through items unchanged in dry-run mode.
- No new pip dependencies introduced by `cross_dedupe` or `html_strip`.
