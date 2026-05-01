# New Processor Stages Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add three new pipeline stages — `cross_dedupe`, `html_strip`, and `score_tag` — each with config, processor, builder wiring, and tests.

**Architecture:** Each stage follows the existing pattern: a Pydantic config class in `config.py` added to the `StageConfig` union, an async processor class in `sluice/processors/`, and an `isinstance` branch in `build_processors()`. `score_tag` reuses the `LLMClient`/`StageLLMConfig` infrastructure already used by `llm_stage`.

**Tech Stack:** `difflib` (stdlib), `html.parser` (stdlib), `jinja2`, existing `LLMClient`/`RunBudget`/`ProviderPool`, `respx` for LLM mocking in tests.

---

## File Map

| File | Change |
|------|--------|
| `sluice/config.py` | Add `CrossDedupeConfig`, `HtmlStripConfig`, `ScoreTagConfig`; extend `StageConfig` union |
| `sluice/processors/cross_dedupe.py` | **New** — `CrossDedupeProcessor` |
| `sluice/processors/html_strip.py` | **New** — `HtmlStripProcessor` |
| `sluice/processors/score_tag.py` | **New** — `ScoreTagProcessor` |
| `sluice/builders.py` | Add three `isinstance` branches in `build_processors()` |
| `tests/processors/test_cross_dedupe.py` | **New** |
| `tests/processors/test_html_strip.py` | **New** |
| `tests/processors/test_score_tag.py` | **New** |
| `prompts/score_tag.md` | **New** — example prompt template |
| `configs/pipelines/ai_news.toml.example` | Add commented score_tag, cross_dedupe, html_strip examples |

---

## Shared test helper

All new test files use this item factory. Add it to `tests/conftest.py` (it's already there as `make_ctx`; add `_item` alongside it):

```python
# In tests/conftest.py — add after make_ctx
from sluice.core.item import Item as _Item

def make_item(
    url: str = "https://example.com/a",
    title: str = "Test Article",
    source_id: str = "rss_0",
    guid: str | None = None,
    tags: list | None = None,
    extras: dict | None = None,
    raw_summary: str | None = None,
    fulltext: str | None = None,
) -> _Item:
    return _Item(
        source_id=source_id,
        pipeline_id="p",
        guid=guid,
        url=url,
        title=title,
        published_at=None,
        raw_summary=raw_summary,
        fulltext=fulltext,
        tags=list(tags or []),
        extras=dict(extras or {}),
    )
```

---

## Task 1: Config classes

**Files:**
- Modify: `sluice/config.py`

- [ ] **Step 1: Add the three config classes to `config.py`**

Find the `RenderConfig` class (around line 186) and insert after it, before the `StageConfig` union:

```python
class CrossDedupeConfig(BaseModel):
    type: Literal["cross_dedupe"]
    name: str
    title_similarity_threshold: float = 0.85
    source_priority: list[str] = Field(default_factory=list)
    merge_tags: bool = True

    @model_validator(mode="after")
    def _validate_threshold(self) -> "CrossDedupeConfig":
        from sluice.core.errors import ConfigError
        if not 0.0 <= self.title_similarity_threshold <= 1.0:
            raise ConfigError(
                f"cross_dedupe title_similarity_threshold must be in [0.0, 1.0], "
                f"got {self.title_similarity_threshold}"
            )
        return self


class HtmlStripConfig(BaseModel):
    type: Literal["html_strip"]
    name: str
    fields: list[str]

    @model_validator(mode="after")
    def _validate_fields(self) -> "HtmlStripConfig":
        from sluice.core.errors import ConfigError
        if not self.fields:
            raise ConfigError("html_strip: fields must be non-empty")
        for f in self.fields:
            if "." in f and not f.startswith("extras."):
                raise ConfigError(
                    f"html_strip: field {f!r} is not a valid path; "
                    f"only top-level fields or 'extras.<key>' are supported"
                )
            if f.startswith("extras."):
                key = f[len("extras."):]
                if not key or "." in key:
                    raise ConfigError(
                        f"html_strip: field {f!r} must be 'extras.<key>' with a "
                        f"non-empty key and no further dots"
                    )
        return self


class ScoreTagConfig(BaseModel):
    type: Literal["score_tag"]
    name: str
    input_field: str
    prompt_file: str
    model: str
    retry_model: str | None = None
    fallback_model: str | None = None
    fallback_model_2: str | None = None
    workers: int = 8
    timeout: float = 60.0
    score_field: str = "score"
    tags_merge: Literal["append", "replace"] = "append"
    on_parse_error: Literal["skip", "fail", "default"] = "skip"
    default_score: int = 5
    default_tags: list[str] = Field(default_factory=list)
    max_input_chars: int = 8000
    truncate_strategy: Literal["head_tail", "head", "error"] = "head_tail"

    @model_validator(mode="after")
    def _validate_score_field(self) -> "ScoreTagConfig":
        from sluice.core.errors import ConfigError
        if not self.score_field or "." in self.score_field:
            raise ConfigError(
                f"score_tag score_field={self.score_field!r} must be a plain key name "
                f"(non-empty, no dot) — it is written to item.extras[score_field]"
            )
        return self
```

Then extend the `StageConfig` union (around line 230) to include the three new types:

```python
StageConfig = Annotated[
    Union[
        CrossDedupeConfig,
        DedupeConfig,
        EnrichStage,
        FetcherApplyConfig,
        FieldFilterConfig,
        FilterConfig,
        HtmlStripConfig,
        LimitStage,
        LLMStageConfig,
        MirrorAttachmentsStage,
        RenderConfig,
        ScoreTagConfig,
    ],
    Field(discriminator="type"),
]
```

- [ ] **Step 2: Run config tests**

```bash
uv run pytest tests/test_pipeline_config.py tests/test_builders.py -q
```

Expected: all pass (no new tests yet, just sanity check nothing broke).

- [ ] **Step 3: Commit**

```bash
git add sluice/config.py
git commit -m "feat(config): add CrossDedupeConfig, HtmlStripConfig, ScoreTagConfig"
```

---

## Task 2: CrossDedupeProcessor

**Files:**
- Create: `sluice/processors/cross_dedupe.py`
- Modify: `sluice/builders.py`
- Test: `tests/processors/test_cross_dedupe.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/processors/test_cross_dedupe.py`:

```python
import difflib
import pytest
from tests.conftest import make_ctx, make_item
from sluice.processors.cross_dedupe import CrossDedupeProcessor


def _proc(**kw) -> CrossDedupeProcessor:
    return CrossDedupeProcessor(name="cd", **kw)


# ── URL round ─────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_url_dedup_keeps_first_when_no_priority():
    a = make_item(url="https://example.com/a", source_id="rss_0")
    b = make_item(url="https://example.com/a", source_id="rss_1")
    ctx = await _proc().process(make_ctx(items=[a, b]))
    assert len(ctx.items) == 1
    assert ctx.items[0].source_id == "rss_0"


@pytest.mark.asyncio
async def test_url_dedup_respects_source_priority():
    a = make_item(url="https://example.com/a", source_id="rss_0")
    b = make_item(url="https://example.com/a", source_id="rss_hn")
    ctx = await _proc(source_priority=["rss_hn", "rss_0"]).process(make_ctx(items=[a, b]))
    assert ctx.items[0].source_id == "rss_hn"


@pytest.mark.asyncio
async def test_url_dedup_merges_tags():
    a = make_item(url="https://x.com/a", tags=["ai"])
    b = make_item(url="https://x.com/a", tags=["ml", "ai"])
    ctx = await _proc(merge_tags=True).process(make_ctx(items=[a, b]))
    assert ctx.items[0].tags == ["ai", "ml"]


@pytest.mark.asyncio
async def test_url_dedup_no_merge_when_disabled():
    a = make_item(url="https://x.com/a", tags=["ai"])
    b = make_item(url="https://x.com/a", tags=["ml"])
    ctx = await _proc(merge_tags=False).process(make_ctx(items=[a, b]))
    assert ctx.items[0].tags == ["ai"]


@pytest.mark.asyncio
async def test_empty_url_skips_url_round():
    a = make_item(url="", title="Same Title")
    b = make_item(url="", title="Same Title")
    # Both have empty URL — they survive Round 1; Round 2 merges them (same title)
    ctx = await _proc(title_similarity_threshold=0.85).process(make_ctx(items=[a, b]))
    assert len(ctx.items) == 1


@pytest.mark.asyncio
async def test_empty_url_items_not_url_grouped_together():
    a = make_item(url="", title="Article A")
    b = make_item(url="", title="Article B")
    # Different titles, both empty URL — both survive
    ctx = await _proc().process(make_ctx(items=[a, b]))
    assert len(ctx.items) == 2


# ── Title round ───────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_title_dedup_above_threshold():
    a = make_item(url="https://x.com/a", title="GCC 16 released")
    b = make_item(url="https://y.com/b", title="GCC 16 released!")
    ratio = difflib.SequenceMatcher(None, a.title.lower(), b.title.lower()).ratio()
    assert ratio >= 0.85, f"expected >= 0.85 but got {ratio}"
    ctx = await _proc(title_similarity_threshold=0.85).process(make_ctx(items=[a, b]))
    assert len(ctx.items) == 1


@pytest.mark.asyncio
async def test_title_dedup_below_threshold_keeps_both():
    a = make_item(url="https://x.com/a", title="Python 3.13 released")
    b = make_item(url="https://y.com/b", title="Rust 2.0 announced")
    ctx = await _proc(title_similarity_threshold=0.85).process(make_ctx(items=[a, b]))
    assert len(ctx.items) == 2


@pytest.mark.asyncio
async def test_empty_title_skips_title_round():
    a = make_item(url="https://x.com/a", title="")
    b = make_item(url="https://y.com/b", title="")
    # Both empty — SequenceMatcher("","") == 1.0 but they must NOT be merged
    ctx = await _proc(title_similarity_threshold=0.85).process(make_ctx(items=[a, b]))
    assert len(ctx.items) == 2


@pytest.mark.asyncio
async def test_title_dedup_greedy_pairwise_not_transitive():
    # A~B and B~C but A!~C: A's group captures B; C is separate
    a = make_item(url="https://x.com/a", title="Article about Python releases")
    b = make_item(url="https://y.com/b", title="Article about Python release news")
    c = make_item(url="https://z.com/c", title="Something completely different here")
    assert difflib.SequenceMatcher(None, a.title.lower(), b.title.lower()).ratio() >= 0.85
    assert difflib.SequenceMatcher(None, a.title.lower(), c.title.lower()).ratio() < 0.85
    ctx = await _proc(title_similarity_threshold=0.85).process(make_ctx(items=[a, b, c]))
    assert len(ctx.items) == 2
    assert ctx.items[0].url == a.url
    assert ctx.items[1].url == c.url


# ── Output order ──────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_output_preserves_relative_order_of_kept_items():
    a = make_item(url="https://x.com/a", title="A")
    b = make_item(url="https://x.com/b", title="B")
    c = make_item(url="https://x.com/c", title="C")
    ctx = await _proc().process(make_ctx(items=[a, b, c]))
    assert [it.url for it in ctx.items] == ["https://x.com/a", "https://x.com/b", "https://x.com/c"]


@pytest.mark.asyncio
async def test_priority_winner_keeps_its_own_original_position():
    """URL round: winner is at its own original position, not first-occurrence."""
    low = make_item(url="https://x.com/dup", source_id="rss_low", title="Dup")
    unique = make_item(url="https://x.com/unique", source_id="rss_0", title="Unique")
    priority = make_item(url="https://x.com/dup", source_id="rss_hn", title="Dup")
    # Original: [low, unique, priority]; rss_hn wins → drop low
    # priority was at position 2 → output: [unique, priority]
    ctx = await _proc(source_priority=["rss_hn"]).process(
        make_ctx(items=[low, unique, priority])
    )
    assert len(ctx.items) == 2
    assert ctx.items[0].url == "https://x.com/unique"
    assert ctx.items[1].source_id == "rss_hn"


@pytest.mark.asyncio
async def test_title_priority_winner_keeps_its_own_original_position():
    """Title round: winner is at its own original position, not anchor position."""
    low = make_item(url="https://x.com/a", source_id="rss_low", title="GCC 16 released")
    unique = make_item(url="https://x.com/b", source_id="rss_0", title="Unrelated article")
    priority = make_item(url="https://x.com/c", source_id="rss_hn", title="GCC 16 released!")
    # Original: [low, unique, priority]; low~priority (title); rss_hn wins → drop low
    # priority was at position 2 → output: [unique, priority]
    ctx = await _proc(
        source_priority=["rss_hn"], title_similarity_threshold=0.85
    ).process(make_ctx(items=[low, unique, priority]))
    assert len(ctx.items) == 2
    assert ctx.items[0].url == "https://x.com/b"   # unique
    assert ctx.items[1].source_id == "rss_hn"       # priority winner


# ── Config validation ─────────────────────────────────────────────────────────

def test_threshold_out_of_range_raises():
    from sluice.core.errors import ConfigError
    with pytest.raises(ConfigError):
        from sluice.config import CrossDedupeConfig
        CrossDedupeConfig(type="cross_dedupe", name="x", title_similarity_threshold=1.5)
```

- [ ] **Step 2: Run to confirm fail**

```bash
uv run pytest tests/processors/test_cross_dedupe.py -q
```

Expected: FAIL — `ModuleNotFoundError: No module named 'sluice.processors.cross_dedupe'`

- [ ] **Step 3: Create `sluice/processors/cross_dedupe.py`**

```python
import difflib
from dataclasses import replace

from sluice.context import PipelineContext
from sluice.core.item import Item
from sluice.logging_setup import get_logger

log = get_logger(__name__)


class CrossDedupeProcessor:
    name = "cross_dedupe"

    def __init__(
        self,
        *,
        name: str,
        title_similarity_threshold: float = 0.85,
        source_priority: list[str] | None = None,
        merge_tags: bool = True,
    ):
        self.name = name
        self.threshold = title_similarity_threshold
        self.priority = list(source_priority or [])
        self.merge_tags = merge_tags

    def _priority_rank(self, source_id: str) -> int:
        try:
            return self.priority.index(source_id)
        except ValueError:
            return len(self.priority)

    def _pick(self, group: list[Item]) -> tuple[Item, list[Item]]:
        if not self.priority:
            return group[0], group[1:]
        best = min(range(len(group)), key=lambda i: self._priority_rank(group[i].source_id))
        return group[best], group[:best] + group[best + 1:]

    def _merge_tags(self, kept: Item, dropped: list[Item]) -> Item:
        if not self.merge_tags:
            return kept
        seen: set[str] = set(kept.tags)
        new_tags = list(kept.tags)
        for it in dropped:
            for tag in it.tags:
                if tag not in seen:
                    new_tags.append(tag)
                    seen.add(tag)
        return replace(kept, tags=new_tags)

    async def process(self, ctx: PipelineContext) -> PipelineContext:
        items = list(ctx.items)

        # Round 1: URL dedup (non-empty URLs only, maintains first-occurrence order)
        url_to_group: dict[str, list[Item]] = {}
        url_order: list[str] = []
        no_url: list[Item] = []

        for it in items:
            if not it.url:
                no_url.append(it)
                continue
            if it.url not in url_to_group:
                url_to_group[it.url] = []
                url_order.append(it.url)
            url_to_group[it.url].append(it)

        # Determine winner for each URL group
        url_winner_item: dict[str, Item] = {}
        url_winner_id: dict[str, int] = {}  # url -> id() of winning original item
        for url in url_order:
            group = url_to_group[url]
            if len(group) == 1:
                url_winner_item[url] = group[0]
                url_winner_id[url] = id(group[0])
            else:
                kept, rest = self._pick(group)
                merged = self._merge_tags(kept, rest)
                url_winner_item[url] = merged
                url_winner_id[url] = id(kept)  # winner's original position
                log.bind(
                    stage=self.name, method="url", url=url,
                    kept_source=merged.source_id,
                    dropped_sources=[d.source_id for d in rest],
                ).debug("cross_dedupe.merged")

        # Build after_r1 in winner's original position order
        # A winner placed at its own original position (not first-occurrence position)
        dropped_ids: set[int] = set()
        for url in url_order:
            winner_id = url_winner_id[url]
            for it in url_to_group[url]:
                if id(it) != winner_id:
                    dropped_ids.add(id(it))

        seen_urls: set[str] = set()
        after_r1: list[Item] = []
        for it in items:
            if id(it) in dropped_ids:
                continue  # non-winner duplicate, drop
            if not it.url:
                after_r1.append(it)
            elif it.url not in seen_urls:
                seen_urls.add(it.url)
                after_r1.append(url_winner_item[it.url])  # merged winner

        # Round 2: Title similarity (greedy pairwise, non-empty titles only)
        result: list[Item] = []
        dropped_ids: set[int] = set()

        # Identify title groups (greedy pairwise) and their winners
        title_winner_of: dict[int, Item] = {}  # id(item) -> merged winner
        title_dropped: set[int] = set()

        for i, anchor in enumerate(after_r1):
            if id(anchor) in title_dropped or not anchor.title:
                continue
            group = [anchor]
            for other in after_r1[i + 1:]:
                if id(other) in title_dropped or not other.title:
                    continue
                ratio = difflib.SequenceMatcher(
                    None, anchor.title.lower(), other.title.lower()
                ).ratio()
                if ratio >= self.threshold:
                    group.append(other)
                    # Do NOT add to title_dropped yet — winner may be one of these

            if len(group) > 1:
                kept, rest = self._pick(group)
                merged = self._merge_tags(kept, rest)
                for m in group:
                    title_winner_of[id(m)] = merged
                for r in rest:
                    log.bind(
                        stage=self.name, method="title",
                        ratio=round(difflib.SequenceMatcher(
                            None, kept.title.lower(), r.title.lower()
                        ).ratio(), 3),
                        kept_source=kept.source_id,
                        dropped_source=r.source_id,
                    ).debug("cross_dedupe.merged")
                # Only mark non-winners as dropped AFTER _pick() has run
                for m in group:
                    if m is not kept:
                        title_dropped.add(id(m))

        # Build result in original order; emit winner at its own position
        emitted_winners: set[int] = set()
        for it in after_r1:
            if id(it) in title_dropped:
                continue
            if id(it) in title_winner_of:
                winner = title_winner_of[id(it)]
                winner_id = id(winner) if winner is not it else id(it)
                if winner_id not in emitted_winners:
                    result.append(winner)
                    emitted_winners.add(winner_id)
            else:
                result.append(it)

        ctx.items = result
        return ctx
```

- [ ] **Step 4: Wire in `builders.py`**

In `build_processors()`, add after the `DedupeConfig` branch:

```python
        elif isinstance(st, CrossDedupeConfig):
            from sluice.processors.cross_dedupe import CrossDedupeProcessor

            procs.append(
                CrossDedupeProcessor(
                    name=st.name,
                    title_similarity_threshold=st.title_similarity_threshold,
                    source_priority=st.source_priority,
                    merge_tags=st.merge_tags,
                )
            )
```

Also add `CrossDedupeConfig` to the imports at the top of `builders.py`:

```python
from sluice.config import (
    CrossDedupeConfig,
    DedupeConfig,
    ...
)
```

- [ ] **Step 5: Run tests**

```bash
uv run pytest tests/processors/test_cross_dedupe.py -v
```

Expected: all PASS.

- [ ] **Step 6: Full suite**

```bash
uv run pytest -q && uv run ruff check .
```

Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add sluice/processors/cross_dedupe.py sluice/config.py sluice/builders.py \
        tests/processors/test_cross_dedupe.py tests/conftest.py
git commit -m "feat(processors): add cross_dedupe stage"
```

---

## Task 3: HtmlStripProcessor

**Files:**
- Create: `sluice/processors/html_strip.py`
- Modify: `sluice/builders.py`
- Test: `tests/processors/test_html_strip.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/processors/test_html_strip.py`:

```python
import pytest
from tests.conftest import make_ctx, make_item
from sluice.processors.html_strip import HtmlStripProcessor


def _proc(fields) -> HtmlStripProcessor:
    return HtmlStripProcessor(name="hs", fields=fields)


# ── Tag stripping ─────────────────────────────────────────────────────────────

def test_strips_inline_tags():
    it = make_item(raw_summary="<b>bold</b> and <em>italic</em>")
    proc = _proc(["raw_summary"])
    result = proc._convert(it.raw_summary)
    assert result == "bold and italic"


def test_discards_script_content():
    proc = _proc(["raw_summary"])
    result = proc._convert("<p>Hello</p><script>bad()</script>")
    assert "bad" not in result
    assert "Hello" in result


def test_discards_style_content():
    proc = _proc(["raw_summary"])
    result = proc._convert("<p>Text</p><style>body{color:red}</style>")
    assert "color" not in result
    assert "Text" in result


def test_discards_template_content():
    proc = _proc(["raw_summary"])
    result = proc._convert("<p>A</p><template>hidden</template>")
    assert "hidden" not in result


def test_block_tags_produce_newlines():
    proc = _proc(["raw_summary"])
    result = proc._convert("<p>First</p><p>Second</p>")
    assert "First" in result and "Second" in result
    assert "\n" in result


def test_br_produces_newline():
    proc = _proc(["raw_summary"])
    result = proc._convert("line one<br>line two")
    assert "\n" in result
    assert "line one" in result and "line two" in result


def test_heading_produces_newlines():
    proc = _proc(["raw_summary"])
    result = proc._convert("<h1>Title</h1><p>Body</p>")
    assert "Title" in result and "Body" in result
    assert result.index("Title") < result.index("Body")


# ── Entity decoding ───────────────────────────────────────────────────────────

def test_decodes_html_entities():
    proc = _proc(["raw_summary"])
    assert proc._convert("AT&amp;T &lt;3 &nbsp;spaces") == "AT&T <3 spaces"


# ── Whitespace normalisation ──────────────────────────────────────────────────

def test_collapses_whitespace():
    proc = _proc(["raw_summary"])
    result = proc._convert("  too   many    spaces  ")
    assert result == "too many spaces"


def test_collapses_excess_newlines():
    proc = _proc(["raw_summary"])
    result = proc._convert("<p>A</p>\n\n\n\n<p>B</p>")
    assert "\n\n\n" not in result


# ── Field routing ─────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_processes_top_level_field():
    it = make_item(raw_summary="<b>Hello</b>")
    ctx = await _proc(["raw_summary"]).process(make_ctx(items=[it]))
    assert ctx.items[0].raw_summary == "Hello"


@pytest.mark.asyncio
async def test_processes_extras_field():
    it = make_item(extras={"body": "<em>Hi</em>"})
    ctx = await _proc(["extras.body"]).process(make_ctx(items=[it]))
    assert ctx.items[0].extras["body"] == "Hi"


@pytest.mark.asyncio
async def test_skips_missing_field_silently():
    it = make_item()  # no raw_summary set → it.raw_summary is None
    # Should not raise
    ctx = await _proc(["raw_summary"]).process(make_ctx(items=[it]))
    assert ctx.items[0].raw_summary is None


@pytest.mark.asyncio
async def test_skips_non_string_field_silently():
    it = make_item(extras={"count": 42})
    ctx = await _proc(["extras.count"]).process(make_ctx(items=[it]))
    assert ctx.items[0].extras["count"] == 42  # unchanged


# ── Config validation ─────────────────────────────────────────────────────────

def test_empty_fields_raises_at_load():
    from sluice.core.errors import ConfigError
    with pytest.raises(ConfigError):
        from sluice.config import HtmlStripConfig
        HtmlStripConfig(type="html_strip", name="x", fields=[])


def test_invalid_dotpath_raises_at_load():
    from sluice.core.errors import ConfigError
    with pytest.raises(ConfigError):
        from sluice.config import HtmlStripConfig
        HtmlStripConfig(type="html_strip", name="x", fields=["foo.bar"])


def test_deep_extras_path_raises_at_load():
    from sluice.core.errors import ConfigError
    with pytest.raises(ConfigError):
        from sluice.config import HtmlStripConfig
        HtmlStripConfig(type="html_strip", name="x", fields=["extras.a.b"])


def test_empty_extras_key_raises_at_load():
    from sluice.core.errors import ConfigError
    with pytest.raises(ConfigError):
        from sluice.config import HtmlStripConfig
        HtmlStripConfig(type="html_strip", name="x", fields=["extras."])
```

- [ ] **Step 2: Run to confirm fail**

```bash
uv run pytest tests/processors/test_html_strip.py -q
```

Expected: FAIL — `ModuleNotFoundError: No module named 'sluice.processors.html_strip'`

- [ ] **Step 3: Create `sluice/processors/html_strip.py`**

```python
import html as html_lib
from html.parser import HTMLParser

from sluice.context import PipelineContext

_DISCARD_TAGS = frozenset({"script", "style", "template"})
_BLOCK_TAGS = frozenset({
    "p", "div", "section", "article", "blockquote",
    "li", "td", "th", "h1", "h2", "h3", "h4", "h5", "h6",
})


class _StripParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self._parts: list[str] = []
        self._discard_depth: int = 0

    def handle_starttag(self, tag, attrs):
        if tag in _DISCARD_TAGS:
            self._discard_depth += 1
            return
        if self._discard_depth:
            return
        if tag in _BLOCK_TAGS:
            self._parts.append("\n\n")
        elif tag == "br":
            self._parts.append("\n")

    def handle_endtag(self, tag):
        if tag in _DISCARD_TAGS:
            self._discard_depth = max(0, self._discard_depth - 1)
            return
        if self._discard_depth:
            return
        if tag in _BLOCK_TAGS:
            self._parts.append("\n\n")

    def handle_data(self, data):
        if self._discard_depth:
            return
        self._parts.append(data)

    def handle_entityref(self, name):
        if self._discard_depth:
            return
        self._parts.append(html_lib.unescape(f"&{name};"))

    def handle_charref(self, name):
        if self._discard_depth:
            return
        self._parts.append(html_lib.unescape(f"&#{name};"))

    def result(self) -> str:
        raw = "".join(self._parts)
        raw = html_lib.unescape(raw)
        import re
        # Collapse whitespace within lines
        lines = raw.split("\n")
        lines = [" ".join(line.split()) for line in lines]
        text = "\n".join(lines)
        # Collapse 3+ newlines to 2
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()


class HtmlStripProcessor:
    name = "html_strip"

    def __init__(self, *, name: str, fields: list[str]):
        self.name = name
        self.fields = list(fields)

    def _convert(self, text: str) -> str:
        parser = _StripParser()
        parser.feed(text)
        return parser.result()

    def _get_field(self, item, path: str):
        if path.startswith("extras."):
            key = path[len("extras."):]
            return item.extras.get(key)
        return getattr(item, path, None)

    def _set_field(self, item, path: str, value: str):
        if path.startswith("extras."):
            key = path[len("extras."):]
            item.extras[key] = value
        else:
            object.__setattr__(item, path, value)

    async def process(self, ctx: PipelineContext) -> PipelineContext:
        for item in ctx.items:
            for path in self.fields:
                val = self._get_field(item, path)
                if not isinstance(val, str):
                    continue
                self._set_field(item, path, self._convert(val))
        return ctx
```

- [ ] **Step 4: Wire in `builders.py`**

Add after the `CrossDedupeConfig` branch:

```python
        elif isinstance(st, HtmlStripConfig):
            from sluice.processors.html_strip import HtmlStripProcessor

            procs.append(HtmlStripProcessor(name=st.name, fields=st.fields))
```

Add `HtmlStripConfig` to the top-level import in `builders.py`.

- [ ] **Step 5: Run tests**

```bash
uv run pytest tests/processors/test_html_strip.py -v
```

Expected: all PASS.

- [ ] **Step 6: Full suite**

```bash
uv run pytest -q && uv run ruff check .
```

- [ ] **Step 7: Commit**

```bash
git add sluice/processors/html_strip.py sluice/builders.py tests/processors/test_html_strip.py
git commit -m "feat(processors): add html_strip stage"
```

---

## Task 4: ScoreTagProcessor

**Files:**
- Create: `sluice/processors/score_tag.py`
- Modify: `sluice/builders.py`
- Test: `tests/processors/test_score_tag.py`
- Create: `prompts/score_tag.md`

- [ ] **Step 1: Write the failing tests**

Create `tests/processors/test_score_tag.py`:

```python
import pytest
from unittest.mock import AsyncMock, MagicMock
from tests.conftest import make_ctx, make_item
from sluice.processors.score_tag import ScoreTagProcessor, _parse_score_tag, _strip_fence


# ── Unit: _strip_fence ────────────────────────────────────────────────────────

def test_strip_fence_removes_json_fence():
    raw = "```json\n{\"score\": 7, \"tags\": [\"AI\"]}\n```"
    assert _strip_fence(raw) == '{"score": 7, "tags": ["AI"]}'


def test_strip_fence_removes_plain_fence():
    raw = "```\n{\"score\": 5}\n```"
    assert _strip_fence(raw) == '{"score": 5}'


def test_strip_fence_leaves_unfenced_alone():
    raw = '{"score": 7}'
    assert _strip_fence(raw) == '{"score": 7}'


def test_strip_fence_no_strip_if_text_outside_fence():
    raw = "Here is JSON:\n```json\n{\"score\": 7}\n```\nThat was it."
    # Non-fence text outside → return as-is
    result = _strip_fence(raw)
    assert result == raw


# ── Unit: _parse_score_tag ────────────────────────────────────────────────────

def test_parse_integer_score():
    score, tags = _parse_score_tag('{"score": 7, "tags": ["AI"]}')
    assert score == 7
    assert tags == ["AI"]


def test_parse_float_score_rounds_to_nearest():
    score, _ = _parse_score_tag('{"score": 7.4}')
    assert score == 7
    score2, _ = _parse_score_tag('{"score": 7.6}')
    assert score2 == 8


def test_parse_numeric_string_score():
    score, _ = _parse_score_tag('{"score": "7.2"}')
    assert score == 7


def test_parse_clamps_score_below_1():
    score, _ = _parse_score_tag('{"score": -3}')
    assert score == 1


def test_parse_clamps_score_above_10():
    score, _ = _parse_score_tag('{"score": 99}')
    assert score == 10


def test_parse_missing_score_raises():
    with pytest.raises(ValueError, match="score"):
        _parse_score_tag('{"tags": ["AI"]}')


def test_parse_non_numeric_score_raises():
    with pytest.raises(ValueError, match="score"):
        _parse_score_tag('{"score": "high"}')


def test_parse_missing_tags_returns_empty():
    score, tags = _parse_score_tag('{"score": 7}')
    assert tags == []


def test_parse_non_list_tags_raises():
    with pytest.raises(ValueError, match="tags"):
        _parse_score_tag('{"score": 7, "tags": "AI"}')


def test_parse_non_string_tags_dropped():
    _, tags = _parse_score_tag('{"score": 7, "tags": ["AI", 42, null, "ML"]}')
    assert tags == ["AI", "ML"]


def test_parse_strips_and_drops_empty_tags():
    _, tags = _parse_score_tag('{"score": 7, "tags": ["  AI  ", "", "  "]}')
    assert tags == ["AI"]


def test_parse_fence_stripped_before_parse():
    score, tags = _parse_score_tag("```json\n{\"score\": 6, \"tags\": [\"Rust\"]}\n```")
    assert score == 6
    assert tags == ["Rust"]


# ── Integration: process() ────────────────────────────────────────────────────

def _make_proc(llm_output: str, on_parse_error="skip", **kw) -> ScoreTagProcessor:
    mock_llm = MagicMock()
    mock_llm.chat = AsyncMock(return_value=llm_output)
    defaults = dict(
        name="st",
        input_field="fulltext",
        prompt_file="prompts/score_tag.md",
        llm_factory=lambda: mock_llm,
        workers=1,
        score_field="score",
        tags_merge="append",
        on_parse_error=on_parse_error,
        default_score=5,
        default_tags=[],
        max_input_chars=8000,
        truncate_strategy="head_tail",
        budget=None,
        failures=None,
        pipeline_id="p",
        max_retries=3,
        model_spec="test/model",
        price_lookup=lambda _: (0.0, 0.0),
    )
    defaults.update(kw)
    return ScoreTagProcessor(**defaults)


@pytest.mark.asyncio
async def test_writes_score_to_extras():
    it = make_item(fulltext="article text")
    proc = _make_proc('{"score": 8, "tags": ["AI"]}')
    ctx = await proc.process(make_ctx(items=[it]))
    assert ctx.items[0].extras["score"] == 8


@pytest.mark.asyncio
async def test_appends_tags():
    it = make_item(fulltext="text", tags=["existing"])
    proc = _make_proc('{"score": 7, "tags": ["new", "existing"]}')
    ctx = await proc.process(make_ctx(items=[it]))
    assert ctx.items[0].tags == ["existing", "new"]


@pytest.mark.asyncio
async def test_replaces_tags():
    it = make_item(fulltext="text", tags=["old"])
    proc = _make_proc('{"score": 7, "tags": ["new"]}', tags_merge="replace")
    ctx = await proc.process(make_ctx(items=[it]))
    assert ctx.items[0].tags == ["new"]


@pytest.mark.asyncio
async def test_on_parse_error_skip_leaves_item_unchanged():
    it = make_item(fulltext="text")
    proc = _make_proc("not json", on_parse_error="skip")
    ctx = await proc.process(make_ctx(items=[it]))
    assert len(ctx.items) == 1
    assert "score" not in ctx.items[0].extras


@pytest.mark.asyncio
async def test_on_parse_error_default_applies_defaults():
    it = make_item(fulltext="text")
    proc = _make_proc("not json", on_parse_error="default")
    proc.default_score = 3
    proc.default_tags = ["fallback"]
    ctx = await proc.process(make_ctx(items=[it]))
    assert ctx.items[0].extras["score"] == 3
    assert ctx.items[0].tags == ["fallback"]


@pytest.mark.asyncio
async def test_on_parse_error_fail_removes_item():
    it = make_item(fulltext="text")
    proc = _make_proc("not json", on_parse_error="fail")
    ctx = await proc.process(make_ctx(items=[it]))
    assert len(ctx.items) == 0


@pytest.mark.asyncio
async def test_one_item_failure_does_not_affect_others():
    good = make_item(fulltext="good text", url="https://good.com")
    bad = make_item(fulltext="bad text", url="https://bad.com")

    call_count = 0
    async def chat_side_effect(msgs):
        nonlocal call_count
        call_count += 1
        if "bad" in msgs[0]["content"]:
            return "invalid json"
        return '{"score": 9, "tags": ["AI"]}'

    mock_llm = MagicMock()
    mock_llm.chat = AsyncMock(side_effect=chat_side_effect)
    proc = ScoreTagProcessor(
        name="st", input_field="fulltext", prompt_file="prompts/score_tag.md",
        llm_factory=lambda: mock_llm, workers=2,
        score_field="score", tags_merge="append",
        on_parse_error="skip", default_score=5, default_tags=[],
        max_input_chars=8000, truncate_strategy="head_tail",
        budget=None, failures=None, pipeline_id="p", max_retries=3,
        model_spec="test/model", price_lookup=lambda _: (0.0, 0.0),
    )
    ctx = await proc.process(make_ctx(items=[good, bad]))
    assert len(ctx.items) == 2
    scored = next(it for it in ctx.items if it.url == "https://good.com")
    assert scored.extras["score"] == 9


def test_parse_null_tags_raises():
    """tags: null is not a list — should be a parse error, not silently []."""
    with pytest.raises(ValueError, match="tags"):
        _parse_score_tag('{"score": 7, "tags": null}')


# ── Budget preflight ──────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_budget_max_calls_blocks_llm():
    from unittest.mock import MagicMock
    budget = MagicMock()
    budget.project.return_value = False  # budget exhausted

    # Capture the mock LLM that _make_proc injects so we can assert it was not called
    captured: list = []
    chat_mock = AsyncMock(return_value='{"score": 7}')

    def make_llm():
        m = MagicMock()
        m.chat = chat_mock
        captured.append(m)
        return m

    proc = _make_proc('{"score": 7}', budget=budget)
    proc.llm_factory = make_llm  # override with capturing factory

    from sluice.core.errors import BudgetExceeded
    with pytest.raises(BudgetExceeded):
        await proc.process(make_ctx(items=[make_item(fulltext="text")]))
    chat_mock.assert_not_called()


# ── Config validation ─────────────────────────────────────────────────────────

def test_score_field_with_dot_raises():
    from sluice.core.errors import ConfigError
    with pytest.raises(ConfigError):
        from sluice.config import ScoreTagConfig
        ScoreTagConfig(
            type="score_tag", name="x",
            input_field="fulltext", prompt_file="p.md",
            model="m", score_field="extras.score",
        )
```

- [ ] **Step 2: Run to confirm fail**

```bash
uv run pytest tests/processors/test_score_tag.py -q
```

Expected: FAIL — `ModuleNotFoundError: No module named 'sluice.processors.score_tag'`

- [ ] **Step 3: Create `sluice/processors/score_tag.py`**

```python
import asyncio
import json
import math
import re
from dataclasses import replace
from pathlib import Path
from typing import Callable

from jinja2 import Template

from sluice.context import PipelineContext
from sluice.core.item import compute_item_key
from sluice.logging_setup import get_logger

log = get_logger(__name__)

_FENCE_RE = re.compile(r"^```(?:json)?\s*\n(.*)\n```\s*$", re.DOTALL)
_SENTINEL = object()  # distinguishes missing "tags" key from explicit null


def _strip_fence(text: str) -> str:
    """Strip markdown code fence if and only if the entire output is fenced."""
    stripped = text.strip()
    m = _FENCE_RE.match(stripped)
    if m:
        return m.group(1).strip()
    return text


def _parse_score_tag(raw: str) -> tuple[int, list[str]]:
    """Parse LLM output into (score, tags). Raises ValueError on invalid input."""
    text = _strip_fence(raw.strip())
    try:
        data = json.loads(text)
    except json.JSONDecodeError as e:
        raise ValueError(f"JSON decode error: {e}") from e

    if "score" not in data:
        raise ValueError("score field missing from LLM output")

    raw_score = data["score"]
    try:
        score_float = float(raw_score)
    except (TypeError, ValueError) as e:
        raise ValueError(f"score {raw_score!r} is not numeric") from e
    if not math.isfinite(score_float):
        raise ValueError(f"score {raw_score!r} is not finite")
    score = max(1, min(10, round(score_float)))

    raw_tags = data.get("tags", _SENTINEL)
    if raw_tags is _SENTINEL:
        raw_tags = []
    elif not isinstance(raw_tags, list):
        raise ValueError(f"tags must be a list, got {type(raw_tags).__name__}")
    tags: list[str] = []
    for t in raw_tags:
        if isinstance(t, str):
            clean = t.strip()
            if clean:
                tags.append(clean)

    return score, tags


def _truncate(text: str, n: int, strategy: str) -> str | None:
    """Returns truncated text, or None if strategy='error' and text is too long."""
    if len(text) <= n:
        return text
    if strategy == "error":
        return None  # caller applies on_parse_error policy
    if strategy == "head":
        return text[:n]
    half = n // 2
    return text[:half] + "\n…\n" + text[-half:]


class ScoreTagProcessor:
    name = "score_tag"

    _CHARS_PER_TOKEN = 3.0
    _ESTIMATED_OUTPUT_TOKENS = 128  # JSON output is short

    def __init__(
        self,
        *,
        name: str,
        input_field: str,
        prompt_file: str,
        llm_factory: Callable,
        workers: int = 8,
        score_field: str = "score",
        tags_merge: str = "append",
        on_parse_error: str = "skip",
        default_score: int = 5,
        default_tags: list[str] | None = None,
        max_input_chars: int = 8000,
        truncate_strategy: str = "head_tail",
        budget=None,
        failures=None,
        pipeline_id: str | None = None,
        max_retries: int = 3,
        model_spec: str = "",
        price_lookup: Callable = lambda _: (0.0, 0.0),
    ):
        self.name = name
        self.input_field = input_field
        self.template = Template(Path(prompt_file).read_text())
        self.llm_factory = llm_factory
        self.workers = workers
        self.score_field = score_field
        self.tags_merge = tags_merge
        self.on_parse_error = on_parse_error
        self.default_score = max(1, min(10, round(default_score)))
        self.default_tags = list(default_tags or [])
        self.max_input_chars = max_input_chars
        self.truncate_strategy = truncate_strategy
        self.budget = budget
        self.failures = failures
        self.pipeline_id = pipeline_id
        self.max_retries = max_retries
        self.model_spec = model_spec
        self._price_lookup = price_lookup

    def _project_usd(self, prompt_chars: int) -> float:
        if self.budget is None:
            return 0.0
        prompt_tokens = prompt_chars / self._CHARS_PER_TOKEN
        in_price, out_price = self._price_lookup(self.model_spec)
        return (prompt_tokens / 1000.0) * in_price + (
            self._ESTIMATED_OUTPUT_TOKENS / 1000.0
        ) * out_price

    def _apply_result(self, item, score: int, tags: list[str]):
        item.extras[self.score_field] = score
        if self.tags_merge == "replace":
            # deduplicate in received order
            seen: set[str] = set()
            new_tags: list[str] = []
            for t in tags:
                if t not in seen:
                    new_tags.append(t)
                    seen.add(t)
            item.tags = new_tags
        else:  # append
            seen: set[str] = set(item.tags)
            for t in tags:
                if t not in seen:
                    item.tags.append(t)
                    seen.add(t)

    def _apply_default(self, item):
        """Apply default_score and default_tags using the same merge logic as a parse success."""
        self._apply_result(item, self.default_score, list(self.default_tags))

    async def process(self, ctx: PipelineContext) -> PipelineContext:
        items = list(ctx.items)

        # Render prompts with truncated input (mirrors LLMStageProcessor._render_one)
        rendered: list[str | None] = []
        total_chars = 0
        for it in items:
            raw = it.get(self.input_field, default="") or ""
            text = _truncate(str(raw), self.max_input_chars, self.truncate_strategy)
            if text is None:
                rendered.append(None)  # truncate error → per-item failure
                continue
            # Create a view with the truncated field so the prompt sees truncated text
            # (mirrors LLMStageProcessor._render_one / _set_path logic)
            item_view = replace(
                it,
                attachments=list(it.attachments),
                extras=dict(it.extras),
                tags=list(it.tags),
            )
            field = self.input_field
            if field.startswith("extras."):
                key = field[len("extras."):]
                item_view.extras[key] = text
            else:
                try:
                    object.__setattr__(item_view, field, text)
                except AttributeError:
                    item_view.extras[field] = text
            prompt = self.template.render(item=item_view)
            rendered.append(prompt)
            total_chars += len(prompt)

        # Budget preflight
        projected_calls = sum(1 for r in rendered if r is not None)
        projected_usd = self._project_usd(total_chars)
        if self.budget is not None:
            from sluice.core.errors import BudgetExceeded
            if not self.budget.project(
                projected_calls=projected_calls, projected_usd=projected_usd
            ):
                raise BudgetExceeded(
                    f"stage {self.name}: would exceed run budget"
                )
        log.bind(
            stage=self.name,
            items_in=len(items),
            prompt_chars=total_chars,
            projected_calls=projected_calls,
            projected_usd=projected_usd,
        ).info("score_tag.preflight_ok")

        sem = asyncio.Semaphore(self.workers)

        async def one(it, prompt: str | None):
            async with sem:
                # Handle truncate_strategy="error" case
                if prompt is None:
                    return await self._handle_failure(it, ValueError("input too long"))

                try:
                    llm = self.llm_factory()
                    raw_out = await llm.chat([{"role": "user", "content": prompt}])
                    score, tags = _parse_score_tag(raw_out)
                    # LLMClient records actual cost; no manual budget.record() needed here
                    self._apply_result(it, score, tags)
                    return it
                except Exception as e:
                    return await self._handle_failure(it, e)

        async def _run_one(it, prompt):
            return await one(it, prompt)

        results = await asyncio.gather(
            *[_run_one(it, prompt) for it, prompt in zip(items, rendered)],
            return_exceptions=True,
        )

        kept = []
        for result in results:
            if isinstance(result, Exception):
                log.opt(exception=result).error("score_tag: unexpected per-item error")
                continue
            if result is not None:
                kept.append(result)
        ctx.items = kept
        log.bind(stage=self.name, items_out=len(ctx.items)).info("score_tag.done")
        return ctx

    async def _handle_failure(self, item, exc: Exception):
        log.bind(
            stage=self.name,
            error_class=type(exc).__name__,
            error=str(exc),
        ).debug("score_tag.item_failed")
        if self.on_parse_error == "skip":
            return item
        if self.on_parse_error == "default":
            self._apply_default(item)
            return item
        # "fail": record and drop
        if self.failures is not None and self.pipeline_id:
            try:
                await self.failures.record(
                    self.pipeline_id,
                    compute_item_key(item),
                    item,
                    stage=self.name,
                    error_class=type(exc).__name__,
                    error_msg=str(exc),
                    max_retries=self.max_retries,
                )
            except Exception:
                log.exception("score_tag: failed to persist per-item failure")
        return None  # signals drop
```

- [ ] **Step 4: Wire in `builders.py`**

Add after the `HtmlStripConfig` branch:

```python
        elif isinstance(st, ScoreTagConfig):
            from sluice.processors.score_tag import ScoreTagProcessor

            stage_llm = StageLLMConfig(
                model=st.model,
                retry_model=st.retry_model,
                fallback_model=st.fallback_model,
                fallback_model_2=st.fallback_model_2,
                timeout=st.timeout,
            )
            procs.append(
                ScoreTagProcessor(
                    name=st.name,
                    input_field=st.input_field,
                    prompt_file=st.prompt_file,
                    llm_factory=lambda cfg=stage_llm: LLMClient(llm_pool, cfg, budget),
                    workers=st.workers,
                    score_field=st.score_field,
                    tags_merge=st.tags_merge,
                    on_parse_error=st.on_parse_error,
                    default_score=st.default_score,
                    default_tags=st.default_tags,
                    max_input_chars=st.max_input_chars,
                    truncate_strategy=st.truncate_strategy,
                    budget=budget,
                    failures=eff_failures,
                    pipeline_id=pipe.id,
                    max_retries=pipe.failures.max_retries,
                    model_spec=st.model,
                    price_lookup=lambda spec: model_price(llm_pool, spec),
                )
            )
```

Add `ScoreTagConfig` to the top-level import in `builders.py`.

- [ ] **Step 5: Create `prompts/score_tag.md`**

```markdown
你是一个新闻相关性评分助手。请根据以下文章内容，输出一个 JSON 对象，不要输出任何其他内容。

JSON 格式：{"score": <1-10整数>, "tags": [<标签列表>]}

评分标准：1=完全不相关，10=高度相关且有价值
标签：2-5个，中文或英文，描述文章主题

标题：{{ item.title }}
正文：{{ item.fulltext or item.raw_summary or "" }}
```

- [ ] **Step 6: Run tests**

```bash
uv run pytest tests/processors/test_score_tag.py -v
```

Expected: all PASS.

- [ ] **Step 7: Full suite**

```bash
uv run pytest -q && uv run ruff check .
```

- [ ] **Step 8: Commit**

```bash
git add sluice/processors/score_tag.py sluice/builders.py \
        tests/processors/test_score_tag.py prompts/score_tag.md
git commit -m "feat(processors): add score_tag stage"
```

---

## Task 5: Example config update

**Files:**
- Modify: `configs/pipelines/ai_news.toml.example`

- [ ] **Step 1: Add commented examples for all three stages**

In `configs/pipelines/ai_news.toml.example`, add after the `[[stages]] name = "dedupe"` block and before `fetch_fulltext`:

```toml
# ── Cross-source dedup (optional) ────────────────────────────────────────────
# Removes duplicate items pointing to the same article across sources.
# Place before fetcher_apply to avoid fetching the same article twice.
#
# [[stages]]
# name = "cross_dedupe"
# type = "cross_dedupe"
# title_similarity_threshold = 0.85
# source_priority = ["rss_hn"]
# merge_tags = true
```

After `[[stages]] name = "trim_fulltext"`, add:

```toml
# ── HTML strip (optional) ─────────────────────────────────────────────────────
# Strip HTML tags from feed fields before LLM processing.
#
# [[stages]]
# name = "strip_html"
# type = "html_strip"
# fields = ["raw_summary"]
```

After `[[stages]] name = "summarize"`, add:

```toml
# ── Score + tag (optional) ────────────────────────────────────────────────────
# One LLM call per item: writes extras.score (1-10) and item.tags.
# Use relevance_filter after this to drop low-scored items.
# Requires: prompts/score_tag.md
#
# [[stages]]
# name = "score_and_tag"
# type = "score_tag"
# input_field    = "fulltext"
# prompt_file    = "prompts/score_tag.md"
# model          = "glm/glm-4-flash"
# workers        = 8
# on_parse_error = "skip"
# score_field    = "score"
# tags_merge     = "append"

# [[stages]]
# name = "relevance_filter"
# type = "filter"
# mode = "keep_if_all"
# rules = [{ field = "extras.score", op = "gte", value = 6 }]
```

- [ ] **Step 2: Run full suite one final time**

```bash
uv run pytest -q && uv run ruff check . && uv run ty check
```

Expected: all pass, 0 errors.

- [ ] **Step 3: Commit**

```bash
git add configs/pipelines/ai_news.toml.example
git commit -m "docs(configs): add cross_dedupe, html_strip, score_tag examples"
```

---

## Self-Review

**Spec coverage:**

| Requirement | Task |
|---|---|
| cross_dedupe skips empty URL in Round 1 | Task 2 (`test_empty_url_skips_url_round`) |
| cross_dedupe skips empty title in Round 2 | Task 2 (`test_empty_title_skips_title_round`) |
| cross_dedupe URL dedup keeps priority winner | Task 2 (`test_url_dedup_respects_source_priority`) |
| cross_dedupe tag merge order: kept-first, case-sensitive | Task 2 (`test_url_dedup_merges_tags`) |
| cross_dedupe greedy pairwise, not transitive | Task 2 (`test_title_dedup_greedy_pairwise_not_transitive`) |
| cross_dedupe output order preserved | Task 2 (`test_output_preserves_relative_order_of_kept_items`) |
| html_strip discards script/style/template content | Task 3 (`test_discards_script_content`) |
| html_strip semantic whitespace for block/heading/br | Task 3 (`test_block_tags_produce_newlines`) |
| html_strip dotpath only `extras.<key>` | Task 3 (`test_invalid_dotpath_raises_at_load`) |
| score_tag fence strip only when fully fenced | Task 4 (`test_strip_fence_no_strip_if_text_outside_fence`) |
| score_tag accepts numeric string score | Task 4 (`test_parse_numeric_string_score`) |
| score_tag round to nearest, clamp [1,10] | Task 4 (`test_parse_float_score_rounds_to_nearest`) |
| score_tag on_parse_error skip/fail/default | Task 4 (three tests) |
| score_tag one item failure doesn't affect others | Task 4 (`test_one_item_failure_does_not_affect_others`) |
| score_tag `tags_merge = replace` deduplicates | Task 4 (`test_replaces_tags`) |
| score_tag `score_field` with dot raises ConfigError | Task 4 + Task 1 |
| score_tag respects RunBudget.max_calls and max_usd | Task 4 (budget param in constructor, preflight check) |
| score_tag truncate_strategy error → per-item failure | Task 4 (`_truncate` returns None → `_handle_failure`) |

**Placeholder scan:** None found.

**Type consistency:** `_parse_score_tag` returns `tuple[int, list[str]]` used consistently. `_strip_fence(str) -> str` consistent. `CrossDedupeProcessor._pick` returns `tuple[Item, list[Item]]` consistent with `_merge_tags`.
