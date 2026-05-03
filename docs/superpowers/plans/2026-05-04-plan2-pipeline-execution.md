# Plan 2 — Pipeline Execution: Stages, Sinks, Real Runner (v2)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `run_pipeline_stub` with a working `run_pipeline` that loads eligible items, drives stages through pipeline-internal order, emits to sinks idempotently, and writes `tx1` / post-tx1-seen / sink-emit / `tx2` jobs through an injected `WriterThread`. After this plan, `sluice run -t <task> -p <pipeline>` actually fetches RSS, runs filters/LLM stages/render, and delivers to one or more sinks — end-to-end on a real task config.

**Architecture:** A pipeline run is `tx1 → post-tx1-seen (best-effort, swallow) → per-emission sink writes → tx2 → cleanup` bracketing one in-memory stage driver loop. The driver applies `pipeline.stages` (a list of stage *names*) in order; each stage name resolves to a `StageDef` from `stages.toml` and to a `StageExecutor` whose `type` is bound at the registry. Items are mutable Python objects with column views; stage drops mark `stage_<S>.status='dropped'` and remove the item from the working set. Sink emission writes per-chunk `sink_emissions(run_key, sink_id, ordinal)` rows synchronously between tx1 and tx2 so process crashes never duplicate downstream deliveries. Run-key reuse for sink retries adopts the `pending_since`-oldest pending key. The runner accepts an externally-managed `WriterThread` per Plan 2 Interface Contract D1.

**Tech Stack:** Python 3.11+, `aiosqlite`, existing v1 modules ported into the new stage interface (`sluice/sources/`, `sluice/processors/`, `sluice/sinks/`, `sluice/fetchers/`, `sluice/llm/`), `feedparser` (already in v1), `pytest` + `pytest-asyncio`.

**Spec reference:** `docs/superpowers/specs/2026-05-02-multi-pipeline-dag-design.md`.

**Interface contract:** `docs/superpowers/plans/2026-05-03-plan2-interface-contract.md` — D1/D2/D3/D4 are MANDATORY.

**Plan 1 alignment (read carefully — Plan 2 must NOT break these):**
- `StageDef`/`SourceDef`/`SinkDef` use `params: dict[str, Any]` (NOT `config`). All Plan 2 code, tests, and StageContext use `params` consistently.
- `PipelineDef` already has `cron`, `stages`, `sources`, `upstream_all`, `upstream_any`, `sinks`, `post_run_cleanup`, `input_mode`. Plan 2 adds **only** `max_attempts: int = 3` (Task 1).
- `items` NOT NULL columns: `item_id, source_name, ingested_at, ingested_by, updated_at, raw_json, dropped`. All test fixtures and tx1 INSERT must include these.
- `pipelines` NOT NULL: `run_key, task, pipeline_name, cycle_anchor, trigger, started_at, status`. Test fixtures include `task`.
- `sink_emissions` columns: `run_key, sink_id, ordinal, sink_type, message_kind, external_id, emitted_at`. **NO `items_json` column**. Per-chunk item lineage lives in `sink_delivery_log` if needed (it has `recipient`, `external_id` and is append-only — adequate for audit).
- `sink_delivery_log` uses `attempt_at` (NOT `attempted_at`).
- `ItemsStore.insert_new(*, item_id=..., source_name=..., raw_json=..., ingested_at=..., ingested_by=..., ...)` — keyword-only, returns `None`, requires `raw_json` non-null.
- `ItemsStore.update_columns(item_id, cols, *, updated_at)` — restricted column set; `updated_at` is REQUIRED.
- `ItemSeenStore.insert_many(items: Iterable[tuple[item_id, source_hash]], *, archived_at)` — tuple iterable, not list-of-ids.
- `StagesStore.start(*, pipeline_run_id, stage_name, started_at) -> stage_run_id` returns the integer FK used by `stage_attempt_log.stage_run_id`.

**v1 reuse policy:** Stage executors PORT v1 logic — they import the algorithmic core (parsing, scoring, rendering) from `sluice/sources/`, `sluice/processors/`, `sluice/fetchers/`, `sluice/sinks/`, `sluice/llm/`. They DO NOT import from `sluice/runner.py`, `sluice/state/`, `sluice/registry.py` (v1), `sluice/builders.py`, or `sluice/context.py`. Task 22 enforces this with a grep-based test.

---

## File Structure

**Created:**

Compatibility & framework:
- `sluice/dag/item.py` — mutable `Item` with column accessors and per-run mutation tracking
- `sluice/dag/stage_context.py` — `StageContext`, `StageResult`, `ItemDrop`, `ItemError`
- `sluice/dag/stage_executor.py` — `StageExecutor` ABC + `bind_executor()` registry binding
- `sluice/dag/stage_driver.py` — `drive_stages(pipeline, ctx) -> DriverResult` (handles drop/fail/skip + resume)
- `sluice/dag/eligibility.py` — `select_eligible_items()` + `evaluate_run_gating()` + `select_entry_carryover()`

Stage implementations:
- `sluice/dag/stages/__init__.py` — registers all 15 executors at import
- `sluice/dag/stages/fetch_rss.py`
- `sluice/dag/stages/dedupe.py`
- `sluice/dag/stages/cross_dedupe.py`
- `sluice/dag/stages/filter_stage.py`
- `sluice/dag/stages/field_filter.py`
- `sluice/dag/stages/html_strip.py`
- `sluice/dag/stages/fetcher_apply.py`
- `sluice/dag/stages/mirror_attachments.py`
- `sluice/dag/stages/llm_common.py` — shared base for enrich/llm_stage/score_tag/summarize_score_tag
- `sluice/dag/stages/enrich.py`
- `sluice/dag/stages/llm_stage.py`
- `sluice/dag/stages/score_tag.py`
- `sluice/dag/stages/summarize_score_tag.py`
- `sluice/dag/stages/sort.py`
- `sluice/dag/stages/limit.py`
- `sluice/dag/stages/render.py`

Sinks:
- `sluice/dag/sinks/__init__.py` — registers sink adapters
- `sluice/dag/sinks/base.py` — `Sink` protocol, `SinkChunk`, `EmissionResult`
- `sluice/dag/sinks/file_md.py`
- `sluice/dag/sinks/email_sink.py`
- `sluice/dag/sinks/telegram_sink.py`
- `sluice/dag/sinks/feishu_sink.py`
- `sluice/dag/sinks/notion_sink.py`
- `sluice/dag/emission.py` — `SinkEmitter.emit_all()` with `sink_emissions` idempotency

Stores (extending Plan 1):
- `sluice/dag/stores/sink_emissions.py` — `SinkEmissionsStore`
- `sluice/dag/stores/sink_delivery_log.py` — `SinkDeliveryLogStore`
- `sluice/dag/stores/sink_pending.py` — `SinkPendingStore`
- `sluice/dag/stores/stage_attempt_log.py` — `StageAttemptLogStore`
- `sluice/dag/stores/pipeline_state.py` — `PipelineStateStore` (per-pipeline `pipeline_<P>` writes)
- `sluice/dag/stores/stage_state.py` — `StageStateStore` (per-stage `stage_<S>` writes)

Phases / runner:
- `sluice/dag/phases/__init__.py`
- `sluice/dag/phases/tx1.py` — `build_tx1_job(...)`
- `sluice/dag/phases/seen_post.py` — `build_seen_post_job(...)` (D4-allowed, best-effort)
- `sluice/dag/phases/tx2.py` — `build_tx2_job(...)`
- `sluice/dag/phases/run_key_resolve.py` — `resolve_run_key(active, pipeline)`
- `sluice/dag/phases/skip_compute.py` — `compute_skip_ids(active, seen, pipeline)`
- `sluice/dag/phases/resume.py` — `load_resume_done(active, pipeline, item_ids, stage_defs)`
- `sluice/dag/cleanup.py` — `apply_post_run_cleanup(...)`
- `sluice/dag/run_pipeline.py` — real `run_pipeline()` (replaces stub)

Tests: `tests/dag/test_*.py`, `tests/dag/stages/test_*.py`, `tests/dag/sinks/test_*.py`, `tests/dag/phases/test_*.py`, `tests/dag/test_d4_invariant.py`, `tests/dag/test_v1_orchestration_guard.py`, `tests/dag/test_run_pipeline_e2e.py`.

**Modified:**
- `sluice/dag/config.py` — add `PipelineDef.max_attempts: int = 3` (Task 1).
- `sluice/dag/runner.py` — keep `run_pipeline_stub` for now; mark deprecated; CLI is rewired in Task 23.
- `sluice/cli.py` — swap stub call to real `run_pipeline` (preserve `-t/-p/-c` semantics from Plan 1; add `--cycle` long-only).

---

## Phase A — Plan 1 Compatibility & Framework

### Task 1: Add `PipelineDef.max_attempts`

**Files:**
- Modify: `sluice/dag/config.py`
- Modify: `sluice/dag/loader.py` (parse the new optional key)
- Test: extend `tests/dag/test_config.py`, `tests/dag/test_loader.py`

The only `PipelineDef` field Plan 2 needs that Plan 1 doesn't have. Default = 3.

- [ ] **Step 1.1: Failing test (extend `tests/dag/test_config.py`)**

```python
def test_pipeline_def_max_attempts_default():
    p = PipelineDef(
        name="p", cron="0 8 * * *", stages=["s"], sources=None,
        upstream_all=None, upstream_any=None, sinks=None,
        post_run_cleanup="none",
    )
    assert p.max_attempts == 3


def test_pipeline_def_max_attempts_custom():
    p = PipelineDef(
        name="p", cron="0 8 * * *", stages=["s"], sources=None,
        upstream_all=None, upstream_any=None, sinks=None,
        post_run_cleanup="none", max_attempts=5,
    )
    assert p.max_attempts == 5
```

- [ ] **Step 1.2: Run, verify FAIL.**

- [ ] **Step 1.3: Edit `sluice/dag/config.py`** — add field at end of `PipelineDef`:

```python
@dataclass(frozen=True)
class PipelineDef:
    name: str
    cron: str
    stages: list[str]
    sources: list[str] | None
    upstream_all: list[str] | None
    upstream_any: list[str] | None
    sinks: list[str] | None
    post_run_cleanup: str | None
    input_mode: str | None = "union"
    max_attempts: int = 3
    # __post_init__ unchanged
```

- [ ] **Step 1.4: Loader update — accept `max_attempts` from TOML pipeline table**

In `sluice/dag/loader.py`, locate where `PipelineDef(...)` is constructed; pass `max_attempts=int(table.get("max_attempts", 3))`.

- [ ] **Step 1.5: Run all Plan 1 tests + new tests; PASS; commit.**

```bash
git add sluice/dag/config.py sluice/dag/loader.py tests/dag/test_config.py
git commit -m "feat(dag): add PipelineDef.max_attempts with default 3"
```

---

### Task 2: `Item` model

**Files:** `sluice/dag/item.py`, `tests/dag/test_item_model.py`

- [ ] **Step 2.1: Failing test**

```python
# tests/dag/test_item_model.py
import pytest
from sluice.dag.item import Item, ITEM_COLUMNS


def _row(**over):
    base = {
        "item_id": "x", "source_name": "s", "guid": None, "url": None,
        "title": None, "author": None, "published_at": None,
        "ingested_at": "2026-05-04T00:00:00+00:00",
        "ingested_by": "P", "updated_at": "2026-05-04T00:00:00+00:00",
        "tag": None, "raw_summary": None, "raw_content": None,
        "raw_json": "{}", "fulltext": None, "attachments_json": None,
        "summary": None, "score": None, "tags_json": None, "extras_json": None,
        "dropped": 0, "dropped_reason": None, "dropped_at": None,
    }
    base.update(over)
    return base


def test_item_columns_match_active_schema():
    # Sanity: every NOT NULL column of items table is in ITEM_COLUMNS
    must_have = {"item_id", "source_name", "ingested_at", "ingested_by",
                 "updated_at", "raw_json", "dropped", "dropped_at"}
    assert must_have.issubset(ITEM_COLUMNS)


def test_item_construct_from_full_row():
    item = Item.from_row(_row())
    assert item["item_id"] == "x"
    assert item.dirty_columns() == frozenset()


def test_item_set_marks_dirty():
    item = Item.from_row(_row(title="old"))
    item["title"] = "new"
    assert item["title"] == "new"
    assert item.dirty_columns() == frozenset({"title"})


def test_item_set_same_value_does_not_dirty():
    item = Item.from_row(_row(title="t"))
    item["title"] = "t"
    assert item.dirty_columns() == frozenset()


def test_item_unknown_column_rejects_get_and_set():
    item = Item.from_row(_row())
    with pytest.raises(KeyError):
        _ = item["nope"]
    with pytest.raises(KeyError):
        item["nope"] = "v"


def test_item_drop_sets_flag_and_reason():
    item = Item.from_row(_row())
    item.mark_dropped("filter_rule")
    assert item.is_dropped()
    assert item.drop_reason() == "filter_rule"


def test_item_extras_setdefault():
    item = Item.from_row(_row())
    item.set_extra("hn_score", 42)
    assert item.get_extra("hn_score") == 42
    assert "extras_json" in item.dirty_columns()


def test_item_volatile_field_does_not_dirty():
    item = Item.from_row(_row())
    item.set_volatile("rendered_text", "hi")
    assert item.get_volatile("rendered_text") == "hi"
    assert item.dirty_columns() == frozenset()
```

- [ ] **Step 2.2: Run, FAIL.**

- [ ] **Step 2.3: Implement** — `sluice/dag/item.py`:

```python
from __future__ import annotations

import json
from typing import Any

ITEM_COLUMNS: frozenset[str] = frozenset({
    "item_id", "source_name", "guid", "url", "title", "author",
    "published_at", "ingested_at", "ingested_by", "updated_at",
    "tag", "raw_summary", "raw_content", "raw_json",
    "fulltext", "attachments_json", "summary", "score",
    "tags_json", "extras_json",
    "dropped", "dropped_reason", "dropped_at",
})


class Item:
    __slots__ = ("_data", "_dirty", "_volatile", "_dropped", "_drop_reason")

    def __init__(self) -> None:
        self._data: dict[str, Any] = {}
        self._dirty: set[str] = set()
        self._volatile: dict[str, Any] = {}
        self._dropped: bool = False
        self._drop_reason: str | None = None

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "Item":
        item = cls()
        for k in row:
            if k not in ITEM_COLUMNS:
                raise KeyError(f"unknown item column: {k!r}")
        item._data = dict(row)
        return item

    def __getitem__(self, key: str) -> Any:
        if key not in ITEM_COLUMNS:
            raise KeyError(f"unknown item column: {key!r}")
        return self._data.get(key)

    def __setitem__(self, key: str, value: Any) -> None:
        if key not in ITEM_COLUMNS:
            raise KeyError(f"unknown item column: {key!r}")
        if self._data.get(key) != value:
            self._data[key] = value
            self._dirty.add(key)

    def dirty_columns(self) -> frozenset[str]:
        return frozenset(self._dirty)

    def to_dirty_dict(self) -> dict[str, Any]:
        return {k: self._data[k] for k in self._dirty}

    def mark_dropped(self, reason: str) -> None:
        self._dropped = True
        self._drop_reason = reason

    def is_dropped(self) -> bool:
        return self._dropped

    def drop_reason(self) -> str | None:
        return self._drop_reason

    def set_extra(self, key: str, value: Any) -> None:
        raw = self._data.get("extras_json")
        existing: dict[str, Any] = json.loads(raw) if raw else {}
        existing[key] = value
        self._data["extras_json"] = json.dumps(existing, ensure_ascii=False)
        self._dirty.add("extras_json")

    def get_extra(self, key: str, default: Any = None) -> Any:
        raw = self._data.get("extras_json")
        if not raw:
            return default
        return json.loads(raw).get(key, default)

    def set_volatile(self, key: str, value: Any) -> None:
        self._volatile[key] = value

    def get_volatile(self, key: str, default: Any = None) -> Any:
        return self._volatile.get(key, default)
```

- [ ] **Step 2.4: PASS, commit.**

```bash
git add sluice/dag/item.py tests/dag/test_item_model.py
git commit -m "feat(dag): add mutable Item model with dirty tracking"
```

---

### Task 3: `StageContext` / `StageResult` / `StageExecutor` registry

**Files:** `sluice/dag/stage_context.py`, `sluice/dag/stage_executor.py`, `tests/dag/test_stage_context.py`

`StageResult` includes `rendered_output: str | None = None` (M7 — render stage returns its output via this field, no side channel).

- [ ] **Step 3.1: Failing test**

```python
# tests/dag/test_stage_context.py
import pytest

from sluice.dag.item import Item
from sluice.dag.stage_context import (
    ItemDrop, ItemError, StageContext, StageResult,
)
from sluice.dag.stage_executor import (
    StageExecutor, bind_executor, get_executor_class, is_bound,
    reset_registry_for_tests,
)


def _row(iid):
    return {
        "item_id": iid, "source_name": "s", "guid": None, "url": None,
        "title": None, "author": None, "published_at": None,
        "ingested_at": "2026-05-04T00:00:00+00:00",
        "ingested_by": "P", "updated_at": "2026-05-04T00:00:00+00:00",
        "tag": None, "raw_summary": None, "raw_content": None,
        "raw_json": "{}", "fulltext": None, "attachments_json": None,
        "summary": None, "score": None, "tags_json": None, "extras_json": None,
        "dropped": 0, "dropped_reason": None, "dropped_at": None,
    }


def test_live_items_excludes_dropped():
    a = Item.from_row(_row("a"))
    b = Item.from_row(_row("b"))
    a.mark_dropped("x")
    ctx = StageContext(
        task_name="t", pipeline_name="P", stage_name="s", stage_type="filter",
        run_key="rk", cycle_anchor="2026-05-04T00:00:00+00:00",
        items=[a, b], params={},
    )
    assert ctx.live_items() == [b]


def test_stage_result_default():
    res = StageResult()
    assert res.dropped_ids() == set()
    assert res.errored_ids() == set()
    assert res.rendered_output is None


def test_stage_result_with_drops_errors_render():
    res = StageResult(
        drops=[ItemDrop("a", "f")],
        errors=[ItemError("b", "E", "msg")],
        rendered_output="hello",
    )
    assert res.dropped_ids() == {"a"}
    assert res.errored_ids() == {"b"}
    assert res.rendered_output == "hello"


def test_executor_registry_bind_and_lookup():
    reset_registry_for_tests()

    class Foo(StageExecutor):
        type_name = "foo"
        async def execute(self, ctx):
            return StageResult()

    bind_executor("foo", Foo)
    assert is_bound("foo")
    assert get_executor_class("foo") is Foo
    with pytest.raises(ValueError):
        bind_executor("foo", Foo)
    reset_registry_for_tests()
```

- [ ] **Step 3.2: Run, FAIL.**

- [ ] **Step 3.3: Implement `stage_context.py`**

```python
# sluice/dag/stage_context.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from sluice.dag.item import Item


@dataclass(frozen=True)
class ItemDrop:
    item_id: str
    reason: str


@dataclass(frozen=True)
class ItemError:
    item_id: str
    error_class: str
    error_msg: str


@dataclass
class StageContext:
    task_name: str
    pipeline_name: str
    stage_name: str
    stage_type: str
    run_key: str
    cycle_anchor: str
    items: list[Item]
    params: dict[str, Any]

    def live_items(self) -> list[Item]:
        return [it for it in self.items if not it.is_dropped()]


@dataclass
class StageResult:
    drops: list[ItemDrop] = field(default_factory=list)
    errors: list[ItemError] = field(default_factory=list)
    new_items: list[Item] = field(default_factory=list)
    rendered_output: str | None = None

    def dropped_ids(self) -> set[str]:
        return {d.item_id for d in self.drops}

    def errored_ids(self) -> set[str]:
        return {e.item_id for e in self.errors}
```

- [ ] **Step 3.4: Implement `stage_executor.py`**

```python
# sluice/dag/stage_executor.py
from __future__ import annotations

from abc import ABC, abstractmethod

from sluice.dag.stage_context import StageContext, StageResult


class StageExecutor(ABC):
    type_name: str = ""

    @abstractmethod
    async def execute(self, ctx: StageContext) -> StageResult: ...


_REGISTRY: dict[str, type[StageExecutor]] = {}


def bind_executor(type_name: str, cls: type[StageExecutor]) -> None:
    if type_name in _REGISTRY:
        raise ValueError(f"executor already bound for type {type_name!r}")
    _REGISTRY[type_name] = cls


def get_executor_class(type_name: str) -> type[StageExecutor]:
    try:
        return _REGISTRY[type_name]
    except KeyError as exc:
        raise KeyError(f"no executor bound for stage type {type_name!r}") from exc


def is_bound(type_name: str) -> bool:
    return type_name in _REGISTRY


def reset_registry_for_tests() -> None:
    _REGISTRY.clear()
```

- [ ] **Step 3.5: PASS, commit.**

```bash
git add sluice/dag/stage_context.py sluice/dag/stage_executor.py tests/dag/test_stage_context.py
git commit -m "feat(dag): add StageContext, StageResult, executor registry"
```

---

### Task 4: Stage driver

**Files:** `sluice/dag/stage_driver.py`, `tests/dag/test_stage_driver.py`

- [ ] **Step 4.1: Failing test (use `StageDef(name=, type=, params=)` matching Plan 1)**

```python
# tests/dag/test_stage_driver.py
import pytest

from sluice.dag.config import PipelineDef, StageDef
from sluice.dag.item import Item
from sluice.dag.stage_context import (
    ItemDrop, ItemError, StageContext, StageResult,
)
from sluice.dag.stage_driver import DriverResult, drive_stages
from sluice.dag.stage_executor import (
    StageExecutor, bind_executor, reset_registry_for_tests,
)


def _row(iid):
    return {
        "item_id": iid, "source_name": "s", "guid": None, "url": None,
        "title": None, "author": None, "published_at": None,
        "ingested_at": "2026-05-04T00:00:00+00:00",
        "ingested_by": "P", "updated_at": "2026-05-04T00:00:00+00:00",
        "tag": None, "raw_summary": None, "raw_content": None,
        "raw_json": "{}", "fulltext": None, "attachments_json": None,
        "summary": None, "score": None, "tags_json": None, "extras_json": None,
        "dropped": 0, "dropped_reason": None, "dropped_at": None,
    }


class _DropExec(StageExecutor):
    type_name = "test_drop"
    async def execute(self, ctx):
        ctx.items[0].mark_dropped("forced")
        return StageResult(drops=[ItemDrop(ctx.items[0]["item_id"], "forced")])


class _MutateExec(StageExecutor):
    type_name = "test_mutate"
    async def execute(self, ctx):
        for it in ctx.live_items():
            it["title"] = "MUT"
        return StageResult()


class _ErrExec(StageExecutor):
    type_name = "test_err"
    async def execute(self, ctx):
        return StageResult(errors=[
            ItemError(it["item_id"], "Boom", "boom") for it in ctx.live_items()
        ])


class _SortExec(StageExecutor):
    type_name = "sort"
    async def execute(self, ctx):
        return StageResult()


@pytest.fixture(autouse=True)
def _reg():
    reset_registry_for_tests()
    bind_executor("test_drop", _DropExec)
    bind_executor("test_mutate", _MutateExec)
    bind_executor("test_err", _ErrExec)
    bind_executor("sort", _SortExec)
    yield
    reset_registry_for_tests()


def _pipeline(stages):
    return PipelineDef(
        name="P", cron="0 8 * * *", stages=stages, sources=None,
        upstream_all=None, upstream_any=None, sinks=None,
        post_run_cleanup="none", max_attempts=3,
    )


def _stage(name, type_):
    return StageDef(name=name, type=type_, params={})


@pytest.mark.asyncio
async def test_driver_runs_in_order_and_propagates_drops():
    items = [Item.from_row(_row("a")), Item.from_row(_row("b"))]
    pipeline = _pipeline(["s_drop", "s_mut"])
    stage_defs = {"s_drop": _stage("s_drop", "test_drop"),
                  "s_mut":  _stage("s_mut",  "test_mutate")}
    res = await drive_stages(
        task_name="t", pipeline=pipeline, stage_defs=stage_defs,
        run_key="rk", cycle_anchor="2026-05-04T00:00:00+00:00",
        items=items, resume_done={},
    )
    assert {d.item_id for d in res.drops_per_stage["s_drop"]} == {"a"}
    assert items[0].is_dropped()
    assert items[1]["title"] == "MUT"


@pytest.mark.asyncio
async def test_driver_excludes_errored_items_from_later_stages():
    items = [Item.from_row(_row("a"))]
    pipeline = _pipeline(["s_err", "s_mut"])
    stage_defs = {"s_err": _stage("s_err", "test_err"),
                  "s_mut": _stage("s_mut", "test_mutate")}
    res = await drive_stages(
        task_name="t", pipeline=pipeline, stage_defs=stage_defs,
        run_key="rk", cycle_anchor="2026-05-04T00:00:00+00:00",
        items=items, resume_done={},
    )
    assert {e.item_id for e in res.errors_per_stage["s_err"]} == {"a"}
    assert items[0]["title"] is None  # mutate stage skipped


@pytest.mark.asyncio
async def test_driver_skips_done_items_for_persistent_stage():
    items = [Item.from_row(_row("a"))]
    pipeline = _pipeline(["s_mut"])
    stage_defs = {"s_mut": _stage("s_mut", "test_mutate")}
    res = await drive_stages(
        task_name="t", pipeline=pipeline, stage_defs=stage_defs,
        run_key="rk", cycle_anchor="2026-05-04T00:00:00+00:00",
        items=items, resume_done={"s_mut": {"a"}},
    )
    assert items[0]["title"] is None
    assert res.skipped_per_stage["s_mut"] == {"a"}


@pytest.mark.asyncio
async def test_driver_volatile_stage_ignores_resume_done():
    items = [Item.from_row(_row("a"))]
    pipeline = _pipeline(["s_sort"])
    stage_defs = {"s_sort": _stage("s_sort", "sort")}
    res = await drive_stages(
        task_name="t", pipeline=pipeline, stage_defs=stage_defs,
        run_key="rk", cycle_anchor="2026-05-04T00:00:00+00:00",
        items=items, resume_done={"s_sort": {"a"}},
    )
    # volatile stages are exempt from skip-on-done
    assert res.skipped_per_stage["s_sort"] == set()


class _NewItemExec(StageExecutor):
    type_name = "test_new"
    async def execute(self, ctx):
        new = Item.from_row(_row("new"))
        return StageResult(new_items=[new])


@pytest.mark.asyncio
async def test_driver_promotes_new_items_into_working_set():
    reset_registry_for_tests()
    bind_executor("test_new", _NewItemExec)
    bind_executor("test_mutate", _MutateExec)
    pipeline = _pipeline(["s_new", "s_mut"])
    stage_defs = {"s_new": _stage("s_new", "test_new"),
                  "s_mut": _stage("s_mut", "test_mutate")}
    res = await drive_stages(
        task_name="t", pipeline=pipeline, stage_defs=stage_defs,
        run_key="rk", cycle_anchor="2026-05-04T00:00:00+00:00",
        items=[], resume_done={},
    )
    # Newly-fetched item must reach the next stage
    assert len(res.new_items) == 1
    assert res.new_items[0]["title"] == "MUT"
    assert "new" in res.input_ids_per_stage["s_mut"]
    assert "new" in res.done_ids_per_stage["s_mut"]
    # And no resume-skip on freshly-injected items
    assert res.skipped_per_stage["s_mut"] == set()


@pytest.mark.asyncio
async def test_driver_done_ids_exclude_errored_items():
    items = [Item.from_row(_row("a")), Item.from_row(_row("b"))]
    pipeline = _pipeline(["s_err", "s_mut"])
    stage_defs = {"s_err": _stage("s_err", "test_err"),
                  "s_mut": _stage("s_mut", "test_mutate")}

    class _OnlyAErr(StageExecutor):
        type_name = "test_err_a"
        async def execute(self, ctx):
            errs = [ItemError(it["item_id"], "E", "x")
                    for it in ctx.live_items() if it["item_id"] == "a"]
            return StageResult(errors=errs)

    reset_registry_for_tests()
    bind_executor("test_err_a", _OnlyAErr)
    bind_executor("test_mutate", _MutateExec)
    stage_defs = {"s_err": _stage("s_err", "test_err_a"),
                  "s_mut": _stage("s_mut", "test_mutate")}
    res = await drive_stages(
        task_name="t", pipeline=pipeline, stage_defs=stage_defs,
        run_key="rk", cycle_anchor="2026-05-04T00:00:00+00:00",
        items=items, resume_done={},
    )
    # a errored at s_err and is excluded from s_mut; b survives both
    assert res.input_ids_per_stage["s_err"] == {"a", "b"}
    assert res.done_ids_per_stage["s_err"] == {"b"}
    assert res.input_ids_per_stage["s_mut"] == {"b"}
    assert res.done_ids_per_stage["s_mut"] == {"b"}
    # Critical: a must NOT appear in s_mut's done_ids — it never ran there
    assert "a" not in res.done_ids_per_stage["s_mut"]
```

- [ ] **Step 4.2: Implement `stage_driver.py`**

The driver MUST:
1. Merge `result.new_items` (e.g. from `fetch_rss`) into the working set so subsequent
   stages process them. Without this, `fetch_rss → dedupe → render` would never apply
   dedupe/render to freshly-fetched items.
2. Record explicit `input_ids_per_stage` and `done_ids_per_stage` so tx1 doesn't have to
   reverse-engineer who saw what. Errored items in earlier stages are NOT marked dropped,
   so a "live items minus touched ids" approximation in tx1 would mis-classify them.

```python
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping

from sluice.dag.config import PipelineDef, StageDef
from sluice.dag.item import Item
from sluice.dag.stage_context import (
    ItemDrop, ItemError, StageContext, StageResult,
)
from sluice.dag.stage_executor import get_executor_class

VOLATILE_TYPES: frozenset[str] = frozenset({"sort", "render"})


@dataclass
class DriverResult:
    drops_per_stage: dict[str, list[ItemDrop]] = field(default_factory=dict)
    errors_per_stage: dict[str, list[ItemError]] = field(default_factory=dict)
    skipped_per_stage: dict[str, set[str]] = field(default_factory=dict)
    input_ids_per_stage: dict[str, set[str]] = field(default_factory=dict)
    done_ids_per_stage: dict[str, set[str]] = field(default_factory=dict)
    new_items: list[Item] = field(default_factory=list)
    rendered_output: str | None = None
    final_working_set: list[Item] = field(default_factory=list)


async def drive_stages(
    *,
    task_name: str,
    pipeline: PipelineDef,
    stage_defs: Mapping[str, StageDef],
    run_key: str,
    cycle_anchor: str,
    items: list[Item],
    resume_done: Mapping[str, set[str]],
) -> DriverResult:
    out = DriverResult()
    working_set: list[Item] = list(items)   # mutable; new_items appended by stages
    excluded: set[str] = set()              # errored mid-pipeline → skip later stages

    for stage_name in pipeline.stages:
        stage_def = stage_defs[stage_name]
        is_volatile = stage_def.type in VOLATILE_TYPES
        done_for_stage = set() if is_volatile else set(resume_done.get(stage_name, ()))

        stage_items: list[Item] = []
        skipped_ids: set[str] = set()
        for it in working_set:
            if it.is_dropped():
                continue
            iid = it["item_id"]
            if iid in excluded:
                continue
            if iid in done_for_stage:
                skipped_ids.add(iid)
                continue
            stage_items.append(it)

        input_ids = {it["item_id"] for it in stage_items}
        out.input_ids_per_stage[stage_name] = input_ids
        out.skipped_per_stage[stage_name] = skipped_ids

        ctx = StageContext(
            task_name=task_name,
            pipeline_name=pipeline.name,
            stage_name=stage_name,
            stage_type=stage_def.type,
            run_key=run_key,
            cycle_anchor=cycle_anchor,
            items=stage_items,
            params=dict(stage_def.params),
        )

        executor_cls = get_executor_class(stage_def.type)
        result = await executor_cls().execute(ctx)

        out.drops_per_stage[stage_name] = list(result.drops)
        out.errors_per_stage[stage_name] = list(result.errors)
        out.new_items.extend(result.new_items)
        if result.rendered_output is not None:
            out.rendered_output = result.rendered_output
        excluded.update(result.errored_ids())

        # Items that were INPUT to this stage AND survived (not dropped here, not errored)
        dropped_here = result.dropped_ids()
        errored_here = result.errored_ids()
        out.done_ids_per_stage[stage_name] = (
            input_ids - dropped_here - errored_here
        )

        # Promote new items into the working set so subsequent stages see them.
        # New items are eligible for the very next stage (they have no resume_done
        # rows yet, no prior stage has run on them in this run).
        if result.new_items:
            working_set.extend(result.new_items)

    out.final_working_set = working_set
    return out
```

- [ ] **Step 4.3: PASS, commit.**

---

### Task 5: Eligibility (item-level + run-level gating + entry carryover)

**Files:** `sluice/dag/eligibility.py`, `tests/dag/test_eligibility.py`, `tests/dag/test_run_gating.py`

Three functions:
- `evaluate_run_gating(conn, pipeline) -> RunGatingDecision`
- `select_eligible_items(conn, pipeline) -> list[dict]` (non-entry pipelines)
- `select_entry_carryover(conn, pipeline) -> list[dict]` (entry pipelines: items in active where source_name ∈ pipeline.sources AND no terminal pipeline_<P>)

`select_entry_carryover` solves the H1 (review 1) regression: entry pipelines also process previously-fetched items still pending in P (e.g. failed transient stages).

**M3 fix:** never globally mutate `conn.row_factory`. Use explicit column lists or temporary set with try/finally.

- [ ] **Step 5.1: Failing tests** (build fixture: items + pipeline_U success row, run gating + selection)

```python
# tests/dag/test_eligibility.py
import pytest

from sluice.dag.config import PipelineDef
from sluice.dag.dynamic_tables import ensure_pipeline_table, ensure_stage_table
from sluice.dag.eligibility import (
    select_eligible_items, select_entry_carryover,
)


def _pipeline(name, **kw):
    base = dict(
        name=name, cron="0 8 * * *", stages=["s"], sources=None,
        upstream_all=None, upstream_any=None, sinks=None,
        post_run_cleanup="none", max_attempts=3,
    )
    base.update(kw)
    return PipelineDef(**base)


async def _seed_item(conn, item_id, source_name="src"):
    await conn.execute(
        "INSERT INTO items(item_id, source_name, ingested_at, ingested_by, "
        "updated_at, raw_json, dropped) VALUES (?, ?, ?, ?, ?, ?, 0)",
        (item_id, source_name, "2026-05-04T00:00:00+00:00", "U",
         "2026-05-04T00:00:00+00:00", "{}"),
    )


async def _seed_pipeline_run(conn, run_key, pipeline_name, status="success"):
    await conn.execute(
        "INSERT INTO pipelines(run_key, task, pipeline_name, cycle_anchor, "
        "trigger, started_at, status, finished_at) VALUES "
        "(?, 't', ?, '2026-05-04T00:00:00+00:00', 'manual', "
        "'2026-05-04T00:00:00+00:00', ?, '2026-05-04T00:00:01+00:00')",
        (run_key, pipeline_name, status),
    )


@pytest.mark.asyncio
async def test_eligibility_excludes_terminal_p_rows(active_db):
    await ensure_pipeline_table(active_db, "P")
    await ensure_pipeline_table(active_db, "U")
    await _seed_item(active_db, "a")
    await _seed_item(active_db, "b")
    await _seed_pipeline_run(active_db, "rk_u", "U")
    await active_db.execute(
        "INSERT INTO pipeline_U(item_id, last_run_id, status, processed_at) "
        "SELECT 'a', id, 'success', '2026-05-04T00:00:01+00:00' FROM pipelines WHERE run_key='rk_u'"
    )
    await active_db.execute(
        "INSERT INTO pipeline_U(item_id, last_run_id, status, processed_at) "
        "SELECT 'b', id, 'success', '2026-05-04T00:00:01+00:00' FROM pipelines WHERE run_key='rk_u'"
    )
    await _seed_pipeline_run(active_db, "rk_p", "P")
    await active_db.execute(
        "INSERT INTO pipeline_P(item_id, last_run_id, status, processed_at) "
        "SELECT 'b', id, 'success', '2026-05-04T00:00:01+00:00' FROM pipelines WHERE run_key='rk_p'"
    )
    await active_db.commit()

    p = _pipeline("P", upstream_all=["U"])
    rows = await select_eligible_items(active_db, p)
    assert {r["item_id"] for r in rows} == {"a"}


@pytest.mark.asyncio
async def test_entry_carryover_includes_pending_items_from_p_sources(active_db):
    await ensure_pipeline_table(active_db, "P")
    await _seed_item(active_db, "a", source_name="feed_a")
    await _seed_item(active_db, "b", source_name="feed_b")  # not in P.sources
    await _seed_item(active_db, "c", source_name="feed_a")
    await _seed_pipeline_run(active_db, "rk_p", "P")
    # c is terminal in P, a is not, b's source isn't ours
    await active_db.execute(
        "INSERT INTO pipeline_P(item_id, last_run_id, status, processed_at) "
        "SELECT 'c', id, 'success', '2026-05-04T00:00:01+00:00' FROM pipelines WHERE run_key='rk_p'"
    )
    await active_db.commit()

    p = _pipeline("P", sources=["feed_a"])
    rows = await select_entry_carryover(active_db, p)
    assert {r["item_id"] for r in rows} == {"a"}
```

```python
# tests/dag/test_run_gating.py
import pytest

from sluice.dag.config import PipelineDef
from sluice.dag.eligibility import evaluate_run_gating


def _pipeline(**kw):
    base = dict(
        name="P", cron="0 8 * * *", stages=["s"], sources=None,
        upstream_all=None, upstream_any=None, sinks=None,
        post_run_cleanup="none", max_attempts=3,
    )
    base.update(kw)
    return PipelineDef(**base)


async def _seed(conn, run_key, pipeline_name, status, anchor):
    await conn.execute(
        "INSERT INTO pipelines(run_key, task, pipeline_name, cycle_anchor, "
        "trigger, started_at, status, finished_at) VALUES "
        "(?, 't', ?, ?, 'manual', ?, ?, ?)",
        (run_key, pipeline_name, anchor, anchor, status, anchor),
    )


@pytest.mark.asyncio
async def test_gating_passes_when_no_upstream(active_db):
    p = _pipeline()
    decision = await evaluate_run_gating(active_db, p)
    assert decision.proceed


@pytest.mark.asyncio
async def test_gating_blocks_when_required_upstream_never_succeeded(active_db):
    p = _pipeline(upstream_all=["U"])
    decision = await evaluate_run_gating(active_db, p)
    assert not decision.proceed
    assert "U" in decision.reason


@pytest.mark.asyncio
async def test_gating_passes_when_upstream_has_success(active_db):
    await _seed(active_db, "rk_u", "U", "success", "2026-05-04T00:00:00+00:00")
    await active_db.commit()
    p = _pipeline(upstream_all=["U"])
    decision = await evaluate_run_gating(active_db, p)
    assert decision.proceed
```

- [ ] **Step 5.2: Implement `eligibility.py`** (M3 fix: explicit column projection, no row_factory mutation)

```python
from __future__ import annotations

import re
from dataclasses import dataclass

import aiosqlite

from sluice.dag.config import PipelineDef

_NAME_RE = re.compile(r"^[a-zA-Z0-9_]+$")

_ITEM_COLS = (
    "item_id", "source_name", "guid", "url", "title", "author",
    "published_at", "ingested_at", "ingested_by", "updated_at",
    "tag", "raw_summary", "raw_content", "raw_json",
    "fulltext", "attachments_json", "summary", "score",
    "tags_json", "extras_json", "dropped", "dropped_reason", "dropped_at",
)
_ITEM_COLS_SQL = ", ".join(f"items.{c}" for c in _ITEM_COLS)


def _safe(name: str) -> str:
    if _NAME_RE.fullmatch(name) is None:
        raise ValueError(f"unsafe pipeline name: {name!r}")
    return name


@dataclass(frozen=True)
class RunGatingDecision:
    proceed: bool
    reason: str = ""


async def evaluate_run_gating(
    conn: aiosqlite.Connection, pipeline: PipelineDef
) -> RunGatingDecision:
    upstream_all = pipeline.upstream_all or []
    upstream_any = pipeline.upstream_any or []
    if not upstream_all and not upstream_any:
        return RunGatingDecision(proceed=True)

    cur = await conn.execute(
        "SELECT MAX(cycle_anchor) FROM pipelines "
        "WHERE pipeline_name=? AND status='success'",
        (pipeline.name,),
    )
    row = await cur.fetchone()
    p_last = row[0] if row else None

    async def _has_success(u: str) -> bool:
        if p_last is None:
            cur = await conn.execute(
                "SELECT 1 FROM pipelines WHERE pipeline_name=? AND status='success' LIMIT 1",
                (u,),
            )
        else:
            cur = await conn.execute(
                "SELECT 1 FROM pipelines WHERE pipeline_name=? AND status='success' "
                "AND cycle_anchor>=? LIMIT 1",
                (u, p_last),
            )
        return (await cur.fetchone()) is not None

    for u in upstream_all:
        if not await _has_success(u):
            return RunGatingDecision(False, f"upstream_all: {u} has no qualifying success")

    if upstream_any:
        for u in upstream_any:
            if await _has_success(u):
                break
        else:
            return RunGatingDecision(False, "upstream_any: none have a qualifying success")

    return RunGatingDecision(proceed=True)


async def select_eligible_items(
    conn: aiosqlite.Connection, pipeline: PipelineDef
) -> list[dict]:
    upstreams = list(pipeline.upstream_all or []) + list(pipeline.upstream_any or [])
    if not upstreams:
        return []

    safe_p = _safe(pipeline.name)
    if pipeline.input_mode == "intersection":
        joiner = " AND "
        sources = pipeline.upstream_all or []
    else:
        joiner = " OR "
        sources = list(dict.fromkeys(upstreams))

    upstream_clauses = [
        f"EXISTS (SELECT 1 FROM pipeline_{_safe(u)} pu "
        f"WHERE pu.item_id=items.item_id AND pu.status='success')"
        for u in sources
    ]

    sql = (
        f"SELECT {_ITEM_COLS_SQL} FROM items "
        "WHERE items.dropped=0 "
        f"AND ({joiner.join(upstream_clauses)}) "
        f"AND NOT EXISTS (SELECT 1 FROM pipeline_{safe_p} pp "
        f"WHERE pp.item_id=items.item_id AND pp.status IN ('success','dropped','failed'))"
    )
    cur = await conn.execute(sql)
    rows = await cur.fetchall()
    return [dict(zip(_ITEM_COLS, r, strict=True)) for r in rows]


async def select_entry_carryover(
    conn: aiosqlite.Connection, pipeline: PipelineDef
) -> list[dict]:
    """Items already in active.items, sourced from this pipeline, still pending in P."""
    if not pipeline.sources:
        return []
    safe_p = _safe(pipeline.name)
    placeholders = ",".join("?" for _ in pipeline.sources)
    sql = (
        f"SELECT {_ITEM_COLS_SQL} FROM items "
        f"WHERE items.dropped=0 "
        f"AND items.source_name IN ({placeholders}) "
        f"AND NOT EXISTS (SELECT 1 FROM pipeline_{safe_p} pp "
        f"WHERE pp.item_id=items.item_id AND pp.status IN ('success','dropped','failed'))"
    )
    cur = await conn.execute(sql, tuple(pipeline.sources))
    rows = await cur.fetchall()
    return [dict(zip(_ITEM_COLS, r, strict=True)) for r in rows]
```

- [ ] **Step 5.3: PASS, commit.**

---

## Phase B — Stage Implementations

### Task 6: `fetch_rss` stage (with skip-id injection per M1)

**Files:** `sluice/dag/stages/__init__.py`, `sluice/dag/stages/fetch_rss.py`, `tests/dag/stages/test_fetch_rss.py`

Skip set is computed by the runner (`compute_skip_ids`, Task 18) from: `seen.item_seen` (already-archived) ∪ active `items` rows where `pipeline_<P>` terminal exists ∪ `items.dropped=1`. Injected via `ctx.params["skip_item_ids"]`.

- [ ] **Step 6.1: Failing tests**

```python
# tests/dag/stages/test_fetch_rss.py
import pytest
from datetime import datetime, timezone

from sluice.dag.stages.fetch_rss import FetchRssExecutor
from sluice.dag.stage_context import StageContext


_FROZEN = """<?xml version='1.0'?>
<rss version='2.0'><channel><title>F</title>
  <item><title>X</title><link>https://x/1</link>
        <pubDate>Sun, 03 May 2026 00:00:00 +0000</pubDate>
        <description>d</description></item>
  <item><title>Y</title><link>https://x/2</link>
        <pubDate>Sun, 03 May 2026 00:00:00 +0000</pubDate>
        <description>d</description></item>
</channel></rss>"""


def _ctx(params=None, items=None):
    return StageContext(
        task_name="t", pipeline_name="P", stage_name="fetch",
        stage_type="fetch_rss", run_key="rk",
        cycle_anchor="2026-05-04T00:00:00+00:00",
        items=items or [], params=params or {},
    )


@pytest.mark.asyncio
async def test_fetch_rss_emits_new_items_from_feed():
    async def fake(url): return _FROZEN
    params = {
        "feeds": [{"name": "feed_a", "url": "https://feed/1"}],
        "window_start": "2026-05-01T00:00:00+00:00",
        "window_end":   "2026-05-04T00:00:00+00:00",
    }
    exe = FetchRssExecutor(
        _fetch_text=fake,
        _now=lambda: datetime(2026, 5, 4, tzinfo=timezone.utc),
    )
    res = await exe.execute(_ctx(params))
    assert len(res.new_items) == 2
    it = res.new_items[0]
    assert it["url"] == "https://x/1"
    assert it["source_name"] == "feed_a"
    assert it["ingested_by"] == "P"
    assert it["raw_json"] is not None  # required NOT NULL


@pytest.mark.asyncio
async def test_fetch_rss_skips_ids_in_skip_set():
    async def fake(url): return _FROZEN
    params = {
        "feeds": [{"name": "feed_a", "url": "https://feed/1"}],
        "window_start": "2026-05-01T00:00:00+00:00",
        "window_end":   "2026-05-04T00:00:00+00:00",
    }
    exe = FetchRssExecutor(
        _fetch_text=fake,
        _now=lambda: datetime(2026, 5, 4, tzinfo=timezone.utc),
    )
    # First run: see what id is computed for the first item
    res = await exe.execute(_ctx(params))
    skip = {res.new_items[0]["item_id"]}

    # Second run with that id in skip set
    params2 = {**params, "skip_item_ids": skip}
    res2 = await exe.execute(_ctx(params2))
    assert len(res2.new_items) == 1
    assert res2.new_items[0]["url"] == "https://x/2"


@pytest.mark.asyncio
async def test_fetch_rss_filters_by_window():
    feed = """<?xml version='1.0'?>
<rss version='2.0'><channel><title>F</title>
  <item><title>old</title><link>https://x/0</link>
        <pubDate>Sun, 01 Jan 2026 00:00:00 +0000</pubDate>
        <description>d</description></item>
</channel></rss>"""
    async def fake(url): return feed
    params = {
        "feeds": [{"name": "feed_a", "url": "https://feed/1"}],
        "window_start": "2026-05-01T00:00:00+00:00",
        "window_end":   "2026-05-04T00:00:00+00:00",
    }
    exe = FetchRssExecutor(_fetch_text=fake, _now=lambda: datetime(2026,5,4,tzinfo=timezone.utc))
    res = await exe.execute(_ctx(params))
    assert res.new_items == []
```

- [ ] **Step 6.2: Implement**

```python
# sluice/dag/stages/fetch_rss.py
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Awaitable, Callable

import feedparser

from sluice.dag.item import Item
from sluice.dag.stage_context import (
    ItemError, StageContext, StageResult,
)
from sluice.dag.stage_executor import StageExecutor, bind_executor


def compute_item_id(source_name: str, guid: str | None, url: str) -> str:
    base = f"{source_name}::{guid or url}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


class FetchRssExecutor(StageExecutor):
    type_name = "fetch_rss"

    def __init__(
        self,
        *,
        _fetch_text: Callable[[str], Awaitable[str]] | None = None,
        _now: Callable[[], datetime] | None = None,
    ):
        self._fetch_text = _fetch_text or _default_fetch_text
        self._now = _now or (lambda: datetime.now(timezone.utc))

    async def execute(self, ctx: StageContext) -> StageResult:
        feeds = ctx.params.get("feeds") or []
        win_start = _parse_iso(ctx.params.get("window_start"))
        win_end = _parse_iso(ctx.params.get("window_end"))
        skip_ids: set[str] = set(ctx.params.get("skip_item_ids") or ())

        ingested_at = self._now().isoformat()
        new_items: list[Item] = []
        errors: list[ItemError] = []

        for feed_cfg in feeds:
            try:
                text = await self._fetch_text(feed_cfg["url"])
            except Exception as exc:  # noqa: BLE001
                errors.append(ItemError(
                    item_id=feed_cfg.get("name", feed_cfg["url"]),
                    error_class=type(exc).__name__,
                    error_msg=str(exc)[:500],
                ))
                continue

            parsed = feedparser.parse(text)
            for entry in parsed.entries:
                pub = _entry_published(entry)
                if win_start and pub and pub < win_start:
                    continue
                if win_end and pub and pub > win_end:
                    continue

                url = entry.get("link") or ""
                guid = entry.get("id") or entry.get("guid")
                source_name = feed_cfg["name"]
                item_id = compute_item_id(source_name, guid, url)
                if item_id in skip_ids:
                    continue

                row = {
                    "item_id": item_id,
                    "source_name": source_name,
                    "guid": guid,
                    "url": url,
                    "title": entry.get("title"),
                    "author": entry.get("author"),
                    "published_at": pub.isoformat() if pub else None,
                    "ingested_at": ingested_at,
                    "ingested_by": ctx.pipeline_name,
                    "updated_at": ingested_at,
                    "tag": None,
                    "raw_summary": entry.get("summary"),
                    "raw_content": _entry_content(entry),
                    "raw_json": json.dumps(_entry_to_raw(entry), ensure_ascii=False),
                    "fulltext": None,
                    "attachments_json": None,
                    "summary": None,
                    "score": None,
                    "tags_json": None,
                    "extras_json": None,
                    "dropped": 0,
                    "dropped_reason": None,
                    "dropped_at": None,
                }
                new_items.append(Item.from_row(row))

        return StageResult(new_items=new_items, errors=errors)


def _parse_iso(s):
    return datetime.fromisoformat(s) if s else None


def _entry_published(entry):
    s = entry.get("published") or entry.get("updated")
    if not s:
        return None
    try:
        return parsedate_to_datetime(s).astimezone(timezone.utc)
    except Exception:
        return None


def _entry_content(entry):
    content = entry.get("content")
    if isinstance(content, list) and content:
        return content[0].get("value")
    return None


def _entry_to_raw(entry):
    out = {}
    for k in ("title", "link", "id", "guid", "author", "published",
              "updated", "summary"):
        if k in entry:
            out[k] = entry.get(k)
    return out


async def _default_fetch_text(url: str) -> str:
    import httpx
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as cli:
        resp = await cli.get(url)
        resp.raise_for_status()
        return resp.text


bind_executor("fetch_rss", FetchRssExecutor)
```

`sluice/dag/stages/__init__.py`:

```python
"""Importing this package binds all stage executors."""
from sluice.dag.stages import fetch_rss as _  # noqa: F401
```

- [ ] **Step 6.3: PASS, commit.**

---

### Task 7: `dedupe` + `cross_dedupe` stages

`dedupe` reads `ctx.params["already_seen_ids"]`. The runner is responsible for injecting
this set on every run via `_override_stage_params` (Task 21) — pre-queried from
`ItemSeenStore.filter_seen` against the working-set item ids. Without that injection the
stage no-ops; tests must assert the injection happens (Task 21 unit test covers this).
`cross_dedupe` is in-run only, by `key` param (`url` default; `tags_json`).

- [ ] **Step 7.1: Tests**, **Step 7.2: Impl**, **Step 7.3: Commit.** (Code identical to v1 prior plan; full bodies inline.)

```python
# sluice/dag/stages/dedupe.py
from sluice.dag.stage_context import ItemDrop, StageContext, StageResult
from sluice.dag.stage_executor import StageExecutor, bind_executor


class DedupeExecutor(StageExecutor):
    type_name = "dedupe"

    async def execute(self, ctx: StageContext) -> StageResult:
        seen = set(ctx.params.get("already_seen_ids") or ())
        drops = []
        for it in ctx.live_items():
            if it["item_id"] in seen:
                it.mark_dropped("seen")
                drops.append(ItemDrop(it["item_id"], "seen"))
        return StageResult(drops=drops)


bind_executor("dedupe", DedupeExecutor)
```

```python
# sluice/dag/stages/cross_dedupe.py
import json

from sluice.dag.stage_context import ItemDrop, StageContext, StageResult
from sluice.dag.stage_executor import StageExecutor, bind_executor


def _key_value(item, key: str):
    if key == "tags_json":
        raw = item["tags_json"]
        return tuple(sorted(json.loads(raw))) if raw else ()
    return item[key]


class CrossDedupeExecutor(StageExecutor):
    type_name = "cross_dedupe"

    async def execute(self, ctx):
        key = ctx.params.get("key", "url")
        seen = set()
        drops = []
        for it in ctx.live_items():
            k = _key_value(it, key)
            if k in seen:
                it.mark_dropped("cross_dedupe")
                drops.append(ItemDrop(it["item_id"], "cross_dedupe"))
            else:
                seen.add(k)
        return StageResult(drops=drops)


bind_executor("cross_dedupe", CrossDedupeExecutor)
```

Add both imports to `sluice/dag/stages/__init__.py`.

---

### Task 8: `filter` + `field_filter` + `html_strip`

Port v1 logic. `filter` = rule evaluator (port from `sluice/processors/filter.py`); `field_filter` = op runner (truncate/regex/strip from `sluice/processors/field_filter.py`); `html_strip` = HTML→text (port from `sluice/processors/html_strip.py`).

If v1 modules expose private helpers, refactor v1 to expose top-level functions (`evaluate_rule(item_dict, rule) -> bool`, `apply_op(value, op) -> value`, `strip_html(text) -> text`). Tests + impl + commit per stage.

```python
# sluice/dag/stages/filter_stage.py
from sluice.processors.filter import evaluate_rule  # exposed by v1 refactor
from sluice.dag.stage_context import ItemDrop, StageContext, StageResult
from sluice.dag.stage_executor import StageExecutor, bind_executor


class FilterExecutor(StageExecutor):
    type_name = "filter"

    async def execute(self, ctx):
        rules = ctx.params.get("rules", [])
        drops = []
        for it in ctx.live_items():
            row = {c: it[c] for c in (
                "title", "url", "author", "raw_summary", "raw_content",
                "fulltext", "summary", "tag", "tags_json", "score",
            )}
            if not all(evaluate_rule(row, r) for r in rules):
                it.mark_dropped("filter_rule")
                drops.append(ItemDrop(it["item_id"], "filter_rule"))
        return StageResult(drops=drops)


bind_executor("filter", FilterExecutor)
```

```python
# sluice/dag/stages/field_filter.py
from sluice.processors.field_filter import apply_op
from sluice.dag.stage_context import StageContext, StageResult
from sluice.dag.stage_executor import StageExecutor, bind_executor


class FieldFilterExecutor(StageExecutor):
    type_name = "field_filter"

    async def execute(self, ctx):
        ops = ctx.params.get("ops", [])
        for it in ctx.live_items():
            for op in ops:
                col = op["field"]
                cur = it[col]
                if cur is None:
                    continue
                it[col] = apply_op(cur, op)
        return StageResult()


bind_executor("field_filter", FieldFilterExecutor)
```

```python
# sluice/dag/stages/html_strip.py
from sluice.processors.html_strip import strip_html
from sluice.dag.stage_context import StageContext, StageResult
from sluice.dag.stage_executor import StageExecutor, bind_executor


_DEFAULT_FIELDS = ("raw_summary", "raw_content", "fulltext")


class HtmlStripExecutor(StageExecutor):
    type_name = "html_strip"

    async def execute(self, ctx):
        fields = tuple(ctx.params.get("fields") or _DEFAULT_FIELDS)
        for it in ctx.live_items():
            for f in fields:
                v = it[f]
                if v:
                    it[f] = strip_html(v)
        return StageResult()


bind_executor("html_strip", HtmlStripExecutor)
```

- [ ] **3 stages × (failing test, impl, PASS, commit) = 3 commits**

---

### Task 9: `fetcher_apply` + `mirror_attachments`

Port v1 fetcher chain. Tests inject fake `fetch_url`. ~2 commits.

```python
# sluice/dag/stages/fetcher_apply.py
from sluice.fetchers.chain import FetcherChain
from sluice.dag.stage_context import ItemError, StageContext, StageResult
from sluice.dag.stage_executor import StageExecutor, bind_executor


class FetcherApplyExecutor(StageExecutor):
    type_name = "fetcher_apply"

    def __init__(self, *, _chain: FetcherChain | None = None):
        self._chain = _chain

    async def execute(self, ctx):
        if self._chain is None:
            self._chain = FetcherChain.from_params(ctx.params)
        write_field = ctx.params.get("write_field", "fulltext")
        errors = []
        for it in ctx.live_items():
            url = it["url"]
            if not url:
                continue
            try:
                text = await self._chain.fetch(url)
                it[write_field] = text
            except Exception as exc:  # noqa: BLE001
                errors.append(ItemError(it["item_id"], type(exc).__name__, str(exc)[:500]))
        return StageResult(errors=errors)


bind_executor("fetcher_apply", FetcherApplyExecutor)
```

If `FetcherChain.from_params` doesn't exist in v1, add it (small refactor wrapping the existing chain constructor).

`mirror_attachments`: port from `sluice/processors/mirror_attachments.py`. The v1 logic that resolves attachment URLs from `raw_json`/`fulltext`, downloads them, writes `attachments_json` is reused; the executor wraps with the new I/O surface.

---

### Task 10: LLM stages — `enrich`, `llm_stage`, `score_tag`, `summarize_score_tag`

**Files:** `sluice/dag/stages/llm_common.py` + 4 thin per-stage modules + tests

`llm_common.py` exposes `LlmStageBase(StageExecutor)` that:
- Reads from `ctx.params`: `input_field`, `model`, `temperature`, `max_tokens`, `prompt_template`, `output_columns` (per subclass)
- Uses `sluice/llm/` middleware (existing v1) for the actual call
- Per-item failures → `StageResult.errors`; successes mutate the item
- Subclass overrides `_apply_response(item, parsed_response)` to write the right column(s)

Subclasses:
- `EnrichExecutor`: writes via `it.set_extra(output_key, parsed)`
- `LlmStageExecutor`: writes a configurable column
- `ScoreTagExecutor`: writes `score: int`, `tags_json: list[str] (json.dumps)`
- `SummarizeScoreTagExecutor`: writes `summary: str`, `score: int`, `tags_json: str`

Tests use a fake `_llm` callable (returns canned JSON). 4 stages × (test, impl, PASS, commit) = 4 commits.

---

### Task 11: `sort` + `limit` + `render`

```python
# sluice/dag/stages/sort.py
from sluice.dag.stage_context import StageContext, StageResult
from sluice.dag.stage_executor import StageExecutor, bind_executor


class SortExecutor(StageExecutor):
    type_name = "sort"

    async def execute(self, ctx):
        key = ctx.params.get("key", "score")
        desc = bool(ctx.params.get("desc", True))
        ctx.items.sort(
            key=lambda it: (it[key] is None, it[key] if it[key] is not None else 0),
            reverse=desc,
        )
        return StageResult()


bind_executor("sort", SortExecutor)
```

```python
# sluice/dag/stages/limit.py
from sluice.dag.stage_context import ItemDrop, StageContext, StageResult
from sluice.dag.stage_executor import StageExecutor, bind_executor


class LimitExecutor(StageExecutor):
    type_name = "limit"

    async def execute(self, ctx):
        n = int(ctx.params.get("n", 0))
        if n <= 0:
            return StageResult()
        live = ctx.live_items()
        drops = []
        for it in live[n:]:
            it.mark_dropped("limit")
            drops.append(ItemDrop(it["item_id"], "limit"))
        return StageResult(drops=drops)


bind_executor("limit", LimitExecutor)
```

```python
# sluice/dag/stages/render.py
from sluice.processors.render import render_template  # v1 helper, may need exposure refactor
from sluice.dag.stage_context import StageContext, StageResult
from sluice.dag.stage_executor import StageExecutor, bind_executor


class RenderExecutor(StageExecutor):
    type_name = "render"

    async def execute(self, ctx):
        template = ctx.params["template"]
        rendered = render_template(template, ctx.live_items(), ctx.params)
        return StageResult(rendered_output=rendered)


bind_executor("render", RenderExecutor)
```

3 stages × (test, impl, PASS, commit) = 3 commits.

---

## Phase C — Sinks

### Task 12: Sink protocol, emitter framework, sink stores

**Files:** `sluice/dag/sinks/base.py`, `sluice/dag/sinks/__init__.py`, `sluice/dag/emission.py`, `sluice/dag/stores/{sink_emissions,sink_delivery_log}.py`, tests.

Match the **real** `sink_emissions` and `sink_delivery_log` schemas (no `items_json`; use `attempt_at`).

```python
# sluice/dag/sinks/base.py
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from importlib import import_module
from typing import Any


@dataclass(frozen=True)
class SinkChunk:
    ordinal: int
    payload: Any
    message_kind: str | None = None
    items_in_chunk: tuple[str, ...] = ()   # not persisted; lineage hint for delivery_log


@dataclass(frozen=True)
class EmissionResult:
    sink_id: str
    ordinal: int
    success: bool
    error_class: str | None = None
    error_msg: str | None = None
    external_id: str | None = None
    recipient: str | None = None


class Sink(ABC):
    type_name: str = ""

    @abstractmethod
    async def chunks(self, rendered: Any, params: dict) -> list[SinkChunk]: ...

    @abstractmethod
    async def emit(self, chunk: SinkChunk, params: dict) -> EmissionResult: ...


_REGISTRY: dict[str, type[Sink]] = {}
_BUILTIN_MODULES = {
    "file_md": "sluice.dag.sinks.file_md",
    "email": "sluice.dag.sinks.email_sink",
    "feishu": "sluice.dag.sinks.feishu_sink",
    "notion": "sluice.dag.sinks.notion_sink",
    "telegram": "sluice.dag.sinks.telegram_sink",
}


def bind_sink(type_name: str, cls: type[Sink]) -> None:
    if type_name in _REGISTRY:
        raise ValueError(f"sink already bound for type {type_name!r}")
    _REGISTRY[type_name] = cls


def get_sink_class(type_name: str) -> type[Sink]:
    if type_name not in _REGISTRY and type_name in _BUILTIN_MODULES:
        import_module(_BUILTIN_MODULES[type_name])
    return _REGISTRY[type_name]


def reset_sink_registry_for_tests() -> None:
    _REGISTRY.clear()
```

`SinkEmissionsStore` / `SinkDeliveryLogStore` (real columns):

```python
# sluice/dag/stores/sink_emissions.py
from __future__ import annotations
import aiosqlite


class SinkEmissionsStore:
    def __init__(self, conn: aiosqlite.Connection):
        self.conn = conn

    async def exists(self, run_key: str, sink_id: str, ordinal: int) -> bool:
        cur = await self.conn.execute(
            "SELECT 1 FROM sink_emissions WHERE run_key=? AND sink_id=? AND ordinal=?",
            (run_key, sink_id, ordinal),
        )
        return (await cur.fetchone()) is not None

    async def insert(
        self, *, run_key: str, sink_id: str, ordinal: int,
        sink_type: str, message_kind: str | None,
        external_id: str | None, emitted_at: str,
    ) -> None:
        await self.conn.execute(
            "INSERT INTO sink_emissions(run_key, sink_id, ordinal, sink_type, "
            "message_kind, external_id, emitted_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (run_key, sink_id, ordinal, sink_type, message_kind, external_id, emitted_at),
        )
```

```python
# sluice/dag/stores/sink_delivery_log.py
from __future__ import annotations
import aiosqlite


class SinkDeliveryLogStore:
    def __init__(self, conn: aiosqlite.Connection):
        self.conn = conn

    async def append(
        self, *, run_key: str, sink_id: str, sink_type: str, attempt_at: str,
        ordinal: int, message_kind: str | None, recipient: str | None,
        external_id: str | None, status: str,
        error_class: str | None, error_msg: str | None,
    ) -> None:
        await self.conn.execute(
            "INSERT INTO sink_delivery_log(run_key, sink_id, sink_type, attempt_at, "
            "ordinal, message_kind, recipient, external_id, status, error_class, "
            "error_msg) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (run_key, sink_id, sink_type, attempt_at, ordinal, message_kind,
             recipient, external_id, status, error_class, error_msg),
        )
```

`SinkEmitter`:

```python
# sluice/dag/emission.py
from __future__ import annotations

from datetime import datetime, timezone

from sluice.dag.sinks.base import EmissionResult, Sink, SinkChunk
from sluice.dag.stores.sink_delivery_log import SinkDeliveryLogStore
from sluice.dag.stores.sink_emissions import SinkEmissionsStore
from sluice.dag.writer import WriteJob, WriterThread


class SinkEmitter:
    def __init__(self, *, writer: WriterThread, run_key: str, pipeline_name: str):
        self._writer = writer
        self._run_key = run_key
        self._pipeline_name = pipeline_name

    async def emit_all(
        self,
        *,
        sink_id: str,
        sink_type: str,
        sink: Sink,
        rendered,
        params: dict,
    ) -> list[EmissionResult]:
        try:
            chunks = await sink.chunks(rendered, params)
        except Exception as exc:  # noqa: BLE001
            res = EmissionResult(
                sink_id=sink_id, ordinal=0, success=False,
                error_class=type(exc).__name__, error_msg=str(exc)[:500],
            )
            await self._record(sink_id, sink_type, SinkChunk(0, None), res)
            return [res]
        results: list[EmissionResult] = []
        for chunk in chunks:
            if await self._already_emitted(sink_id, chunk.ordinal):
                results.append(EmissionResult(
                    sink_id=sink_id, ordinal=chunk.ordinal, success=True,
                    error_msg="already_emitted",
                ))
                continue
            try:
                res = await sink.emit(chunk, params)
            except Exception as exc:  # noqa: BLE001
                res = EmissionResult(
                    sink_id=sink_id, ordinal=chunk.ordinal, success=False,
                    error_class=type(exc).__name__, error_msg=str(exc)[:500],
                )
            await self._record(sink_id, sink_type, chunk, res)
            results.append(res)
        return results

    async def _already_emitted(self, sink_id, ordinal) -> bool:
        out = {"hit": False}
        async def job(active, seen):
            store = SinkEmissionsStore(active)
            out["hit"] = await store.exists(self._run_key, sink_id, ordinal)
        await (await self._writer.submit(WriteJob(run=job)))
        return out["hit"]

    async def _record(self, sink_id, sink_type, chunk, res):
        now = datetime.now(timezone.utc).isoformat()
        run_key = self._run_key

        async def job(active, seen):
            # D4: writes to active only — never seen
            log = SinkDeliveryLogStore(active)
            await log.append(
                run_key=run_key, sink_id=sink_id, sink_type=sink_type,
                attempt_at=now, ordinal=chunk.ordinal,
                message_kind=chunk.message_kind, recipient=res.recipient,
                external_id=res.external_id,
                status="success" if res.success else "failed",
                error_class=res.error_class, error_msg=res.error_msg,
            )
            if res.success:
                emis = SinkEmissionsStore(active)
                await emis.insert(
                    run_key=run_key, sink_id=sink_id, ordinal=chunk.ordinal,
                    sink_type=sink_type, message_kind=chunk.message_kind,
                    external_id=res.external_id, emitted_at=now,
                )
        await (await self._writer.submit(WriteJob(run=job)))
```

D4 invariant: emission jobs write only `active`. Task 22's grep test enforces this.

- [ ] Tests + commits (~3-4 commits including idempotency test).

---

### Task 13: `file_md` sink (test bench)

```python
# sluice/dag/sinks/file_md.py
import os
from pathlib import Path
from sluice.dag.sinks.base import EmissionResult, Sink, SinkChunk, bind_sink


class FileMdSink(Sink):
    type_name = "file_md"

    async def chunks(self, rendered, params):
        return [SinkChunk(ordinal=0, payload=rendered, message_kind="markdown")]

    async def emit(self, chunk, params):
        fields = {
            **{k: params[k] for k in ("task", "pipeline", "run_key", "cycle_anchor", "sink_id") if k in params},
            "ordinal": chunk.ordinal,
        }
        if "path" in params:
            path = Path(params["path"].format(**fields))   # v1-compatible schema
        else:
            out_dir = Path(params["out_dir"])
            path = out_dir / params.get("filename_template", "{ordinal}.md").format(**fields)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(chunk.payload or "")
        return EmissionResult(
            sink_id=params.get("sink_id", "file_md"),
            ordinal=chunk.ordinal, success=True,
            external_id=str(path), recipient=str(path.parent),
        )


bind_sink("file_md", FileMdSink)
```

- [ ] Test with tmp_path + commit.

---

### Task 14: Port v1 sinks (`email`, `feishu`, `telegram`, `notion`)

Each sink wraps v1 send logic. `chunks()` produces chunks per sink's max-payload rule (Telegram 4096 chars, Feishu card limits). `emit()` calls v1 send. Inject HTTP client / SDK.

4 sinks × (test, impl, PASS, commit). Per-sink test mocks the network layer; assertions cover `chunks()` boundaries + a successful + failed emit path.

Register adapters lazily from `get_sink_class()`. `sluice/dag/sinks/__init__.py` must not eagerly import optional v1 sink modules, so `file_md`-only runs do not require email/Feishu/Telegram extras. Template-valued params (`items_template`, `html_template`, `card_template`, `style_block_file`, `footer_template`) resolve relative to `TaskConfig.task_dir` via `params["config_root"]`.

---

## Phase D — Wiring

### Task 15: Per-task dynamic stores

**Files:** `sluice/dag/stores/{sink_pending,stage_attempt_log,pipeline_state,stage_state}.py`

```python
# sluice/dag/stores/sink_pending.py
from __future__ import annotations

import re
from collections.abc import Iterable

import aiosqlite

_NAME = re.compile(r"^[a-zA-Z0-9_]+$")


def _safe(name: str) -> str:
    if _NAME.fullmatch(name) is None:
        raise ValueError(f"unsafe pipeline name: {name!r}")
    return name


class SinkPendingStore:
    def __init__(self, conn: aiosqlite.Connection, pipeline_name: str):
        self.conn = conn
        self.pipeline = _safe(pipeline_name)

    async def upsert(self, *, item_id: str, run_key: str, pending_since: str) -> None:
        await self.conn.execute(
            f"INSERT INTO pipeline_{self.pipeline}_sink_pending"
            f"(item_id, run_key, pending_since) VALUES (?, ?, ?) "
            f"ON CONFLICT(item_id) DO UPDATE SET run_key=excluded.run_key, "
            f"pending_since=excluded.pending_since",
            (item_id, run_key, pending_since),
        )

    async def delete_for_items(self, item_ids: Iterable[str]) -> None:
        ids = list(item_ids)
        if not ids:
            return
        placeholders = ",".join("?" for _ in ids)
        await self.conn.execute(
            f"DELETE FROM pipeline_{self.pipeline}_sink_pending "
            f"WHERE item_id IN ({placeholders})",
            tuple(ids),
        )

    async def oldest_run_key(self) -> str | None:
        cur = await self.conn.execute(
            f"SELECT run_key FROM pipeline_{self.pipeline}_sink_pending "
            f"ORDER BY pending_since ASC LIMIT 1"
        )
        row = await cur.fetchone()
        return row[0] if row else None

    async def items_for_run_key(self, run_key: str) -> set[str]:
        cur = await self.conn.execute(
            f"SELECT item_id FROM pipeline_{self.pipeline}_sink_pending WHERE run_key=?",
            (run_key,),
        )
        return {r[0] for r in await cur.fetchall()}

    async def items_with_other_keys(self, run_key: str) -> set[str]:
        cur = await self.conn.execute(
            f"SELECT item_id FROM pipeline_{self.pipeline}_sink_pending WHERE run_key<>?",
            (run_key,),
        )
        return {r[0] for r in await cur.fetchall()}
```

```python
# sluice/dag/stores/stage_attempt_log.py
from __future__ import annotations
import aiosqlite


class StageAttemptLogStore:
    def __init__(self, conn: aiosqlite.Connection):
        self.conn = conn

    async def next_attempt_no(
        self, *, pipeline_name: str, stage_name: str, item_id: str
    ) -> int:
        cur = await self.conn.execute(
            "SELECT COALESCE(MAX(attempt_no), 0) FROM stage_attempt_log "
            "WHERE pipeline_name=? AND stage_name=? AND item_id=?",
            (pipeline_name, stage_name, item_id),
        )
        row = await cur.fetchone()
        return int(row[0]) + 1

    async def append(
        self, *, stage_run_id: int, pipeline_run_id: int,
        pipeline_name: str, stage_name: str, item_id: str,
        attempt_no: int, status: str, processed_at: str,
        error_class: str | None, error_msg: str | None,
    ) -> None:
        await self.conn.execute(
            "INSERT INTO stage_attempt_log(stage_run_id, pipeline_run_id, "
            "pipeline_name, stage_name, item_id, attempt_no, status, "
            "processed_at, error_class, error_msg) VALUES "
            "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (stage_run_id, pipeline_run_id, pipeline_name, stage_name,
             item_id, attempt_no, status, processed_at, error_class, error_msg),
        )
```

```python
# sluice/dag/stores/pipeline_state.py
from __future__ import annotations
import re
import aiosqlite

_NAME = re.compile(r"^[a-zA-Z0-9_]+$")


def _safe(n):
    if _NAME.fullmatch(n) is None:
        raise ValueError(n)
    return n


class PipelineStateStore:
    def __init__(self, conn: aiosqlite.Connection, pipeline_name: str):
        self.conn = conn
        self.pipeline = _safe(pipeline_name)

    async def upsert(
        self, *, item_id: str, last_run_id: int, status: str,
        processed_at: str, error_msg: str | None,
    ) -> None:
        await self.conn.execute(
            f"INSERT INTO pipeline_{self.pipeline}"
            f"(item_id, last_run_id, status, processed_at, error_msg) "
            f"VALUES (?, ?, ?, ?, ?) ON CONFLICT(item_id) DO UPDATE SET "
            f"last_run_id=excluded.last_run_id, status=excluded.status, "
            f"processed_at=excluded.processed_at, error_msg=excluded.error_msg",
            (item_id, last_run_id, status, processed_at, error_msg),
        )
```

```python
# sluice/dag/stores/stage_state.py
from __future__ import annotations
import re
import aiosqlite

_NAME = re.compile(r"^[a-zA-Z0-9_]+$")


def _safe(n):
    if _NAME.fullmatch(n) is None:
        raise ValueError(n)
    return n


class StageStateStore:
    def __init__(self, conn: aiosqlite.Connection, stage_name: str):
        self.conn = conn
        self.stage = _safe(stage_name)

    async def upsert(
        self, *, pipeline_name: str, item_id: str, status: str,
        attempts: int, last_stage_run_id: int, last_pipeline_run_id: int,
        last_processed_at: str,
        last_error_class: str | None, last_error_msg: str | None,
    ) -> None:
        await self.conn.execute(
            f"INSERT INTO stage_{self.stage}"
            f"(pipeline_name, item_id, status, attempts, last_stage_run_id, "
            f"last_pipeline_run_id, last_processed_at, last_error_class, last_error_msg) "
            f"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
            f"ON CONFLICT(pipeline_name, item_id) DO UPDATE SET "
            f"status=excluded.status, attempts=excluded.attempts, "
            f"last_stage_run_id=excluded.last_stage_run_id, "
            f"last_pipeline_run_id=excluded.last_pipeline_run_id, "
            f"last_processed_at=excluded.last_processed_at, "
            f"last_error_class=excluded.last_error_class, "
            f"last_error_msg=excluded.last_error_msg",
            (pipeline_name, item_id, status, attempts, last_stage_run_id,
             last_pipeline_run_id, last_processed_at, last_error_class, last_error_msg),
        )

    async def attempts(self, *, pipeline_name: str, item_id: str) -> int:
        cur = await self.conn.execute(
            f"SELECT attempts FROM stage_{self.stage} "
            f"WHERE pipeline_name=? AND item_id=?",
            (pipeline_name, item_id),
        )
        row = await cur.fetchone()
        return int(row[0]) if row else 0

    async def done_item_ids(
        self, *, pipeline_name: str, item_ids: list[str]
    ) -> set[str]:
        if not item_ids:
            return set()
        placeholders = ",".join("?" for _ in item_ids)
        cur = await self.conn.execute(
            f"SELECT item_id FROM stage_{self.stage} "
            f"WHERE pipeline_name=? AND item_id IN ({placeholders}) AND status='done'",
            (pipeline_name, *item_ids),
        )
        return {r[0] for r in await cur.fetchall()}
```

- [ ] Tests + 4 commits (one per store).

---

### Task 16: `tx1` build (no seen writes — D4)

**File:** `sluice/dag/phases/tx1.py`, `tests/dag/phases/test_tx1.py`

`build_tx1_job` returns a `WriteJob` whose `run(active, seen)`:

1. UPSERT `pipelines` row to `status='running'` via `PipelinesStore.upsert_running` → captures `pipeline_run_id`.
2. For each stage in `pipeline.stages`:
   a. `StagesStore.start(pipeline_run_id, stage_name, started_at)` → captures `stage_run_id`.
   b. For each item touched by this stage (drops + errors + done items from working set):
      - Compute `attempt_no` via `StageAttemptLogStore.next_attempt_no(...)`.
      - Append `stage_attempt_log` row.
      - Upsert `stage_<S>` via `StageStateStore.upsert(...)` (status: `done`/`dropped`/`failed`/`skipped`; `attempts` = sum of prior + this run's; `last_*` fields populated).
   c. `StagesStore.finalize(stage_run_id, ...)` with stage-level summary.
3. INSERT new items (from `driver_result.new_items`) via `ItemsStore.insert_new(**kwargs)` for each. `raw_json` defaults to `"{}"` if not set; `ingested_at`/`ingested_by`/`updated_at` come from item.
4. UPDATE existing items' dirty columns via `ItemsStore.update_columns(item_id, dirty, updated_at=now)`. Only the restricted `_UPDATABLE` set is allowed; non-updatable keys are silently filtered (since `Item.__setitem__` allows them but tx1 won't push them — instead, the runner-side audit will assert no item has dirty non-updatable cols, raising at construct time).

   **Note**: `Item.dirty_columns()` may include columns not in `_UPDATABLE` (e.g. `dropped`, `published_at`). Filter these in the runner before calling `update_columns`. The drop tombstone path uses `ItemsStore.mark_dropped` instead — but that's `_global` cleanup territory, not tx1.

5. **Seen writes are NOT in this job.** Spec requires post-tx1 best-effort.
6. The job populates a caller-provided `out: dict` with `pipeline_run_id` and `items_in` so the runner can use them for tx2 / sink emit.

**D4 sanity:** `from sluice.dag.stores.item_seen import ItemSeenStore` is **deliberately absent** from this file. Task 22's test allows tx1 to import it, but Task 16 chooses not to.

```python
# sluice/dag/phases/tx1.py
from __future__ import annotations

from datetime import datetime, timezone

from sluice.dag.config import PipelineDef, StageDef, TaskConfig
from sluice.dag.item import Item
from sluice.dag.stage_driver import DriverResult, VOLATILE_TYPES
from sluice.dag.stores.items import ItemsStore, _UPDATABLE
from sluice.dag.stores.pipelines import PipelinesStore
from sluice.dag.stores.stages import StagesStore
from sluice.dag.stores.stage_attempt_log import StageAttemptLogStore
from sluice.dag.stores.stage_state import StageStateStore
from sluice.dag.writer import WriteJob


def build_tx1_job(
    *,
    task: TaskConfig,
    pipeline: PipelineDef,
    stage_defs: dict[str, StageDef],
    run_key: str,
    cycle_anchor: str,
    started_at: str,
    trigger: str,
    items: list[Item],
    driver_result: DriverResult,
    out: dict,
) -> WriteJob:
    async def job(active, seen):  # noqa: ARG001 — D4: tx1 never touches seen
        # 1. pipelines UPSERT running
        run_id = await PipelinesStore(active).upsert_running(
            run_key=run_key, task=task.name, pipeline_name=pipeline.name,
            cycle_anchor=cycle_anchor, trigger=trigger, started_at=started_at,
        )

        stages_store = StagesStore(active)
        log_store = StageAttemptLogStore(active)
        now = datetime.now(timezone.utc).isoformat()

        # 2. New items FIRST (foreign keys: stage_<S> rows reference items via item_id
        #    only by string — no FK — but seen_post and downstream must see the row).
        items_store = ItemsStore(active)
        for it in driver_result.new_items:
            await items_store.insert_new(
                item_id=it["item_id"], source_name=it["source_name"],
                guid=it["guid"], url=it["url"], title=it["title"],
                author=it["author"], published_at=it["published_at"],
                raw_json=it["raw_json"] or "{}",
                ingested_at=it["ingested_at"], ingested_by=it["ingested_by"],
                tag=it["tag"], raw_summary=it["raw_summary"],
                raw_content=it["raw_content"],
            )

        # 3. Per-stage audit using EXPLICIT done/drop/error/skip sets from the driver.
        #    Do NOT reverse-engineer done from "live items minus touched".
        for stage_name in pipeline.stages:
            sd = stage_defs[stage_name]
            if sd.type in VOLATILE_TYPES:
                continue

            stage_run_id = await stages_store.start(
                pipeline_run_id=run_id, stage_name=stage_name, started_at=started_at,
            )

            drops = {d.item_id: d for d in driver_result.drops_per_stage.get(stage_name, ())}
            errors = {e.item_id: e for e in driver_result.errors_per_stage.get(stage_name, ())}
            skipped = driver_result.skipped_per_stage.get(stage_name, set())
            done_ids = driver_result.done_ids_per_stage.get(stage_name, set())

            state_store = StageStateStore(active, stage_name)

            # Order matters: a single item must appear in only one bucket.
            # done_ids (from driver) excludes drops/errors/skipped by construction.
            for item_id in done_ids | set(drops) | set(errors):
                if item_id in done_ids:
                    status, ec, em = "done", None, None
                elif item_id in drops:
                    status, ec, em = "dropped", None, drops[item_id].reason
                else:
                    status = "failed"
                    e = errors[item_id]
                    ec, em = e.error_class, e.error_msg

                attempt_no = await log_store.next_attempt_no(
                    pipeline_name=pipeline.name, stage_name=stage_name, item_id=item_id,
                )
                await log_store.append(
                    stage_run_id=stage_run_id, pipeline_run_id=run_id,
                    pipeline_name=pipeline.name, stage_name=stage_name,
                    item_id=item_id, attempt_no=attempt_no, status=status,
                    processed_at=now, error_class=ec, error_msg=em,
                )
                prior = await state_store.attempts(
                    pipeline_name=pipeline.name, item_id=item_id,
                )
                # Spec: attempts increments only on failure. done/dropped keep prior value.
                new_attempts = prior + 1 if status == "failed" else prior
                # On the very first run for an item that succeeded, prior=0 → attempts=0.
                # Bump done/dropped to max(prior, 1) so the row exists with a sane count.
                if status in ("done", "dropped") and new_attempts == 0:
                    new_attempts = 1

                await state_store.upsert(
                    pipeline_name=pipeline.name, item_id=item_id, status=status,
                    attempts=new_attempts, last_stage_run_id=stage_run_id,
                    last_pipeline_run_id=run_id, last_processed_at=now,
                    last_error_class=ec, last_error_msg=em,
                )

            # Skipped items: append-only audit row, do not bump attempts or rewrite state.
            for item_id in skipped:
                attempt_no = await log_store.next_attempt_no(
                    pipeline_name=pipeline.name, stage_name=stage_name, item_id=item_id,
                )
                await log_store.append(
                    stage_run_id=stage_run_id, pipeline_run_id=run_id,
                    pipeline_name=pipeline.name, stage_name=stage_name,
                    item_id=item_id, attempt_no=attempt_no, status="skipped",
                    processed_at=now, error_class=None, error_msg=None,
                )

            await stages_store.finalize(
                stage_run_id, finished_at=now, status="success",
                items_in=len(done_ids) + len(drops) + len(errors),
                items_out=len(done_ids), error_msg=None,
            )

        # 4. Dirty column updates for ALL items in the working set.
        #    `items` is the runner's `all_items` (initial + driver's new_items), so this
        #    loop also captures post-fetch mutations on freshly-fetched items
        #    (e.g. fulltext written by fetcher_apply, summary by llm_stage).
        #    `Item.from_row` populates `_data` directly without marking dirty, so
        #    new_items only have dirty columns set by stages that ran AFTER fetch_rss
        #    — exactly what we need to UPDATE on top of the row inserted in step 2.
        for it in items:
            dirty = it.to_dirty_dict()
            updatable = {k: v for k, v in dirty.items() if k in _UPDATABLE}
            if updatable:
                await items_store.update_columns(it["item_id"], updatable, updated_at=now)

        out["pipeline_run_id"] = run_id
        # `items` already includes new_items (runner passes `all_items`); no double-count.
        out["items_in"] = len(items)

    return WriteJob(run=job)
```

- [ ] Tests + impl + PASS + commit.

---

### Task 17: post-tx1 seen best-effort job (D4-allowed seen write)

**File:** `sluice/dag/phases/seen_post.py`, `tests/dag/phases/test_seen_post.py`

Spec: "After tx1 ack, writer inserts new `item_seen` rows. Best-effort; failures logged but not rolled back." With Plan 1 B1 fix, the writer's `seen.commit()` failure raises on the future. So the runner submits this as a separate job and **catches** the exception, logging without aborting the run.

```python
# sluice/dag/phases/seen_post.py
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Iterable

from sluice.dag.item import Item
from sluice.dag.stores.item_seen import ItemSeenStore
from sluice.dag.writer import WriteJob


def _source_hash(item: Item) -> str:
    base = f"{item['source_name']}::{item['guid'] or item['url'] or ''}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def build_seen_post_job(new_items: Iterable[Item]) -> WriteJob:
    pairs = [(it["item_id"], _source_hash(it)) for it in new_items]
    archived_at = datetime.now(timezone.utc).isoformat()

    async def job(active, seen):  # noqa: ARG001  (active unused — D4-allowed: this IS the seen-only path)
        if not pairs:
            return
        store = ItemSeenStore(seen)
        await store.insert_many(pairs, archived_at=archived_at)

    return WriteJob(run=job)
```

The runner's call pattern (Task 21):

```python
if driver_result.new_items:
    seen_job = build_seen_post_job(driver_result.new_items)
    try:
        await (await writer.submit(seen_job))
    except Exception as exc:  # noqa: BLE001
        logger.warning("seen post-tx1 write failed: %s", exc)
```

- [ ] Test that exception in `seen.commit()` does not abort the run; test that successful path inserts rows; commit.

---

### Task 18: `compute_skip_ids`, `resolve_run_key`, `load_resume_done`

**Files:** `sluice/dag/phases/{skip_compute,run_key_resolve,resume}.py`, tests.

```python
# sluice/dag/phases/run_key_resolve.py
from __future__ import annotations

from sluice.dag.stores.sink_pending import SinkPendingStore


async def resolve_run_key(
    active, pipeline_name: str, fresh_run_key: str,
) -> tuple[str, list[str]]:
    """Returns (effective_run_key, deferred_item_ids).

    Adopts pending_since-oldest pending key if present.
    Items with a different pending key are deferred to a later cycle.
    """
    pending = SinkPendingStore(active, pipeline_name)
    oldest = await pending.oldest_run_key()
    if oldest is None:
        return (fresh_run_key, [])
    deferred = sorted(await pending.items_with_other_keys(oldest))
    return (oldest, deferred)
```

```python
# sluice/dag/phases/resume.py
from __future__ import annotations

from sluice.dag.stage_driver import VOLATILE_TYPES
from sluice.dag.stores.stage_state import StageStateStore


async def load_resume_done(
    active,
    *,
    pipeline_name: str,
    stage_names: list[str],
    stage_types: dict[str, str],
    item_ids: list[str],
) -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    if not item_ids:
        return out
    for stage_name in stage_names:
        if stage_types[stage_name] in VOLATILE_TYPES:
            continue
        store = StageStateStore(active, stage_name)
        out[stage_name] = await store.done_item_ids(
            pipeline_name=pipeline_name, item_ids=item_ids,
        )
    return out
```

```python
# sluice/dag/phases/skip_compute.py
from __future__ import annotations

from sluice.dag.config import PipelineDef
from sluice.dag.stores.item_seen import ItemSeenStore


async def compute_skip_ids(
    active, seen, pipeline: PipelineDef,
) -> set[str]:
    """For entry pipelines: items the next fetch should skip.

    Includes:
      - Items terminal (success/dropped/failed) in pipeline_<P>
      - Items with items.dropped=1
      - Items present in seen.item_seen
    """
    if not pipeline.sources:
        return set()
    safe = pipeline.name  # already validated upstream
    skip: set[str] = set()
    cur = await active.execute(
        f"SELECT item_id FROM pipeline_{safe} WHERE status IN ('success','dropped','failed')"
    )
    skip |= {r[0] for r in await cur.fetchall()}
    cur = await active.execute("SELECT item_id FROM items WHERE dropped=1")
    skip |= {r[0] for r in await cur.fetchall()}
    cur = await seen.execute("SELECT item_id FROM item_seen")
    skip |= {r[0] for r in await cur.fetchall()}
    return skip
```

- [ ] Tests for each (3 commits).

---

### Task 19: `tx2` build (Phase 1 + Phase 2 status, sink_pending, no seen)

**File:** `sluice/dag/phases/tx2.py`, `tests/dag/phases/test_tx2.py`

`build_tx2_job(...)`:

1. Compute per-item terminal promotion using `driver_result`:
   - Item dropped at any stage → `pipeline_<P>.status='dropped'`
   - Item errored & `attempts>=max_attempts` for that stage → `pipeline_<P>.status='failed'`
   - All non-volatile stages 'done' → eligible for 'success'
   - Else → no `pipeline_<P>` row this run (transient).
2. Fold sink results into per-item promotion:
   - For pipelines with sinks: items eligible for 'success' get `pipeline_<P>.status='success'` ONLY IF all sinks emitted all required ordinals (i.e. no failed `EmissionResult` for any sink_id).
   - If any sink failed: items eligible for success get an entry in `pipeline_<P>_sink_pending(item_id, run_key, pending_since)` and NO `pipeline_<P>` row.
   - Items with terminal `dropped`/`failed` get their `pipeline_<P>` row regardless.
   - On successful sink retry: delete the matching rows from `pipeline_<P>_sink_pending`.
3. Compute Phase 1 + Phase 2 pipeline status; call `PipelinesStore.finalize`.

Phase 1 base status:
- `success` if `items_in == 0` OR every processed item ended `success`-eligible
- `success` if `items_in > 0` and every item ended `dropped` (drop-only run)
- `partial` if some succeeded and some dropped/failed/non-terminal
- `partial` if all items ended `failed` due to max_attempts
- `failed` only on unrecoverable run-level error (caller passes `run_level_error`)

Phase 2 sink degradation:
- No-op exemption: zero items eligible for emit → keep base status
- All sinks succeeded → keep base
- Some succeeded, some failed → degrade `success`→`partial`
- All sinks failed → degrade `success`/`partial`→`failed`, set `error_msg`

```python
# sluice/dag/phases/tx2.py
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone

from sluice.dag.config import PipelineDef, StageDef
from sluice.dag.item import Item
from sluice.dag.stage_driver import DriverResult, VOLATILE_TYPES
from sluice.dag.stores.pipeline_state import PipelineStateStore
from sluice.dag.stores.pipelines import PipelinesStore
from sluice.dag.stores.sink_pending import SinkPendingStore
from sluice.dag.stores.stage_state import StageStateStore
from sluice.dag.writer import WriteJob


def build_tx2_job(
    *,
    pipeline: PipelineDef,
    stage_defs: dict,
    pipeline_run_id: int,
    run_key: str,
    items: list[Item],
    driver_result: DriverResult,
    emission_results: list,            # list[EmissionResult]
    items_in: int,
    run_level_error: str | None,
    out: dict,
) -> WriteJob:
    async def job(active, seen):  # noqa: ARG001 — D4: tx2 never touches seen
        now = datetime.now(timezone.utc).isoformat()
        state = PipelineStateStore(active, pipeline.name)
        pending = SinkPendingStore(active, pipeline.name) if pipeline.sinks else None

        # 1. Per-item base terminal classification
        non_volatile_stages = [
            s for s in pipeline.stages if stage_defs[s].type not in VOLATILE_TYPES
        ]
        per_item_status: dict[str, tuple[str, str | None]] = {}
        for it in items:
            iid = it["item_id"]
            dropped_at_stage = next((
                s for s in non_volatile_stages
                if iid in {d.item_id for d in driver_result.drops_per_stage.get(s, ())}
            ), None)
            errored_stage = next((
                s for s in non_volatile_stages
                if iid in {e.item_id for e in driver_result.errors_per_stage.get(s, ())}
            ), None)

            if dropped_at_stage:
                per_item_status[iid] = ("dropped", None)
            elif errored_stage:
                attempts = await StageStateStore(active, errored_stage).attempts(
                    pipeline_name=pipeline.name, item_id=iid,
                )
                if attempts >= pipeline.max_attempts:
                    per_item_status[iid] = ("failed", f"stage_{errored_stage}_max_attempts")
                else:
                    per_item_status[iid] = ("transient", None)
            else:
                per_item_status[iid] = ("eligible_success", None)

        # 2. Fold sink results
        all_sinks_ok = all(r.success for r in emission_results) if emission_results else True
        had_emit_attempts = bool(emission_results)
        items_out = 0

        for iid, (kind, err) in per_item_status.items():
            if kind == "transient":
                continue  # no row written
            if kind == "dropped":
                await state.upsert(
                    item_id=iid, last_run_id=pipeline_run_id, status="dropped",
                    processed_at=now, error_msg=None,
                )
            elif kind == "failed":
                await state.upsert(
                    item_id=iid, last_run_id=pipeline_run_id, status="failed",
                    processed_at=now, error_msg=err,
                )
            elif kind == "eligible_success":
                if pipeline.sinks and not all_sinks_ok:
                    # defer: sink_pending entry, no terminal row
                    await pending.upsert(
                        item_id=iid, run_key=run_key, pending_since=now,
                    )
                else:
                    await state.upsert(
                        item_id=iid, last_run_id=pipeline_run_id, status="success",
                        processed_at=now, error_msg=None,
                    )
                    items_out += 1
                    if pipeline.sinks and pending is not None:
                        # successful retry: clear the pending row if present
                        await pending.delete_for_items([iid])

        # 3. Phase 1 + Phase 2 status
        if run_level_error:
            status = "failed"
            error_msg = run_level_error
        else:
            kinds = [v[0] for v in per_item_status.values()]
            if not kinds:
                base = "success"
            elif all(k == "dropped" for k in kinds):
                base = "success"
            elif all(k in ("eligible_success",) for k in kinds):
                base = "success"
            elif all(k == "failed" for k in kinds):
                base = "partial"
            elif any(k == "eligible_success" for k in kinds) and any(
                k in ("dropped", "failed", "transient") for k in kinds
            ):
                base = "partial"
            else:
                base = "partial"

            # Phase 2
            success_eligible = sum(1 for v in per_item_status.values() if v[0] == "eligible_success")
            error_msg = None
            if pipeline.sinks and had_emit_attempts and success_eligible:
                ok = sum(1 for r in emission_results if r.success)
                bad = sum(1 for r in emission_results if not r.success)
                if bad == 0:
                    status = base
                elif ok == 0:
                    status = "failed"
                    error_msg = "all_sinks_failed: " + "; ".join(
                        f"{r.sink_id}#{r.ordinal}:{r.error_msg}" for r in emission_results if not r.success
                    )[:500]
                else:
                    status = "partial" if base == "success" else base
            else:
                status = base

        await PipelinesStore(active).finalize(
            pipeline_run_id,
            finished_at=now, status=status,
            items_in=items_in, items_out=items_out,
            error_msg=error_msg,
        )

        out["status"] = status
        out["items_out"] = items_out
        out["error_msg"] = error_msg

    return WriteJob(run=job)
```

- [ ] Tests covering: drop-only run → success; all success no sinks → success; all success sinks ok → success; all success sinks failed → failed + sink_pending rows; mixed eligible/dropped → partial.
- [ ] Commit.

---

### Task 20: `apply_post_run_cleanup`

**File:** `sluice/dag/cleanup.py`

```python
# sluice/dag/cleanup.py
from __future__ import annotations

import re
from datetime import datetime, timezone

from sluice.dag.config import PipelineDef, TaskConfig
from sluice.dag.writer import WriteJob, WriterThread

_NAME = re.compile(r"^[a-zA-Z0-9_]+$")


def _safe(n):
    if _NAME.fullmatch(n) is None:
        raise ValueError(n)
    return n


async def apply_post_run_cleanup(
    *,
    writer: WriterThread,
    task: TaskConfig,
    pipeline: PipelineDef,
    sink_failed: bool,
    working_set_ids: set[str],
) -> int:
    """Returns number of items cleaned. Skips on sink failure or 'none'."""
    if pipeline.post_run_cleanup in (None, "none"):
        return 0
    if sink_failed:
        return 0

    mode = pipeline.post_run_cleanup
    safe_p = _safe(pipeline.name)
    upstreams = list(pipeline.upstream_all or []) + list(pipeline.upstream_any or [])
    if not upstreams:
        return 0

    upstream_clauses = " OR ".join(
        f"EXISTS (SELECT 1 FROM pipeline_{_safe(u)} pu "
        f"WHERE pu.item_id=items.item_id AND pu.status='success')"
        for u in upstreams
    )
    working_clause = ""
    params: tuple = ()
    if working_set_ids:
        placeholders = ",".join("?" for _ in working_set_ids)
        working_clause = f"AND items.item_id NOT IN ({placeholders}) "
        params = tuple(working_set_ids)

    sink_pending_clause = ""
    if pipeline.sinks:
        sink_pending_clause = (
            f"AND NOT EXISTS (SELECT 1 FROM pipeline_{safe_p}_sink_pending sp "
            f"WHERE sp.item_id=items.item_id) "
        )

    sql_select = (
        f"SELECT items.item_id FROM items "
        f"WHERE items.dropped=0 "
        f"AND ({upstream_clauses}) "
        f"AND NOT EXISTS (SELECT 1 FROM pipeline_{safe_p} pp "
        f"WHERE pp.item_id=items.item_id) "
        f"{sink_pending_clause}"
        f"{working_clause}"
    )

    out = {"count": 0}
    now = datetime.now(timezone.utc).isoformat()

    async def cleanup_job(active, seen):  # noqa: ARG001  (D4: no seen)
        cur = await active.execute(sql_select, params)
        ids = [r[0] for r in await cur.fetchall()]
        if not ids:
            return

        for item_id in ids:
            await active.execute(
                f"INSERT INTO pipeline_{safe_p}(item_id, last_run_id, status, "
                f"processed_at, error_msg) VALUES "
                f"(?, (SELECT MAX(id) FROM pipelines WHERE pipeline_name=?), "
                f"'dropped', ?, ?) "
                f"ON CONFLICT(item_id) DO NOTHING",
                (item_id, pipeline.name, now, f"abandoned_by_{pipeline.name}"),
            )

        if mode == "drop_unprocessed_upstream_global":
            placeholders = ",".join("?" for _ in ids)
            await active.execute(
                f"UPDATE items SET dropped=1, dropped_reason=?, dropped_at=?, "
                f"updated_at=? WHERE item_id IN ({placeholders})",
                (f"abandoned_by_{pipeline.name}", now, now, *ids),
            )
        out["count"] = len(ids)

    await (await writer.submit(WriteJob(run=cleanup_job)))
    return out["count"]
```

- [ ] Tests for: `none` skipped; sink failure skipped; `_local` writes pipeline_P only; `_global` writes pipeline_P + items.dropped; working_set excluded; sink_pending excluded.
- [ ] Commit.

---

### Task 21: Real `run_pipeline` (D1 injected writer; pre-flight ensure outside)

**File:** `sluice/dag/run_pipeline.py`, `tests/dag/test_run_pipeline.py`

**D1/D2 lifecycle reaffirmed:** the caller (CLI / Prefect flow) is responsible for:
1. Calling `await ensure_dynamic_tables(task)` (active DDL + seen DB base migration preflight, own connections, idempotent)
2. Constructing and starting the `WriterThread`
3. Passing the started writer into `run_pipeline`
4. Stopping the writer when done

`run_pipeline` itself **does not** call `migrate_task_dynamic_tables`. This separates writer
lifecycle from per-run logic and matches the interface contract D2.

A small helper `ensure_dynamic_tables(task)` lives in `sluice/dag/run_pipeline.py` so both
tests and the CLI import the same call.

```python
# sluice/dag/run_pipeline.py
from __future__ import annotations

import logging
from dataclasses import dataclass, replace
from datetime import datetime, timezone

from sluice.dag.cleanup import apply_post_run_cleanup
from sluice.dag.config import StageDef, TaskConfig
from sluice.dag.cycle import compute_cycle_anchor, make_run_key
from sluice.dag.db import open_active_db, open_seen_db
from sluice.dag.dynamic_tables import migrate_task_dynamic_tables
from sluice.dag.eligibility import (
    evaluate_run_gating, select_eligible_items, select_entry_carryover,
)
from sluice.dag.emission import SinkEmitter
from sluice.dag.item import Item
from sluice.dag.phases.resume import load_resume_done
from sluice.dag.phases.run_key_resolve import resolve_run_key
from sluice.dag.phases.seen_post import build_seen_post_job
from sluice.dag.phases.skip_compute import compute_skip_ids
from sluice.dag.phases.tx1 import build_tx1_job
from sluice.dag.phases.tx2 import build_tx2_job
from sluice.dag.sinks.base import get_sink_class
from sluice.dag.stage_driver import drive_stages
from sluice.dag.stores.item_seen import ItemSeenStore
from sluice.dag.validator import validate_task
from sluice.dag.writer import WriterThread

import sluice.dag.stages   # noqa: F401  (binds executors)
import sluice.dag.sinks    # noqa: F401  (binds sinks)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RunResult:
    pipeline_name: str
    run_key: str
    status: str
    items_in: int
    items_out: int
    error_msg: str | None = None


async def ensure_dynamic_tables(task: TaskConfig) -> None:
    """Caller MUST invoke before WriterThread.start(). Idempotent preflight."""
    async with open_active_db(task.db_path) as conn:
        await migrate_task_dynamic_tables(conn, task)
    async with open_seen_db(task.seen_db):
        pass


def _override_stage_params(
    stage_defs: dict[str, StageDef],
    *,
    overrides_by_type: dict[str, dict],
) -> dict[str, StageDef]:
    """Return a NEW stage_defs map with per-type param overrides.

    Uses dataclasses.replace so the input task config is never mutated.
    """
    if not overrides_by_type:
        return stage_defs
    out = dict(stage_defs)
    for name, sd in stage_defs.items():
        extra = overrides_by_type.get(sd.type)
        if extra:
            out[name] = replace(sd, params={**sd.params, **extra})
    return out


async def run_pipeline(
    task: TaskConfig,
    pipeline_name: str,
    *,
    writer: WriterThread,
    now: datetime | None = None,
    trigger: str = "manual",
) -> RunResult:
    validate_task(task)
    pipeline = next(p for p in task.pipelines if p.name == pipeline_name)
    stage_defs: dict[str, StageDef] = {s.name: s for s in task.stages}
    stage_types = {n: sd.type for n, sd in stage_defs.items()}

    now = now or datetime.now(timezone.utc)
    cycle_anchor = compute_cycle_anchor(task.window.anchor, task.timezone or "UTC", now)
    fresh_run_key = make_run_key(task.name, pipeline.name, cycle_anchor)
    started_at = now.isoformat()

    skip_ids: set[str] = set()
    rows: list[dict] = []
    items_carryover: list[dict] = []
    effective_run_key = fresh_run_key
    deferred_ids: list[str] = []
    already_seen_ids: set[str] = set()

    # Read-only pre-flight on its own connection. Migration is the CALLER's job
    # (already executed via ensure_dynamic_tables before writer.start()).
    async with open_active_db(task.db_path) as conn_pre:
        gating = await evaluate_run_gating(conn_pre, pipeline)
        if not gating.proceed:
            return RunResult(
                pipeline_name=pipeline.name, run_key=fresh_run_key,
                status="skipped_gating", items_in=0, items_out=0,
                error_msg=gating.reason,
            )

        effective_run_key, deferred_ids = await resolve_run_key(
            conn_pre, pipeline.name, fresh_run_key,
        )

        if pipeline.upstream_all or pipeline.upstream_any:
            rows = await select_eligible_items(conn_pre, pipeline)
        else:
            async with open_seen_db(task.seen_db) as conn_seen:
                skip_ids = await compute_skip_ids(conn_pre, conn_seen, pipeline)
            items_carryover = await select_entry_carryover(conn_pre, pipeline)

        deferred = set(deferred_ids)
        rows = [r for r in rows if r["item_id"] not in deferred]
        items_carryover = [r for r in items_carryover if r["item_id"] not in deferred]

        items: list[Item] = [Item.from_row(r) for r in rows + items_carryover]

        # B3-anti-dup: when an entry pipeline ALSO carries over pending active items,
        # fetch_rss must not re-emit the same item_id (would cause double-processing
        # in driver / sinks even though INSERT OR IGNORE protects items table).
        carryover_ids = {r["item_id"] for r in items_carryover}
        skip_ids |= carryover_ids

        # already_seen_ids for the dedupe stage: items present in seen DB OR
        # already-existing in active items table (carryover/cross-source dup).
        if any(stage_types.get(s) == "dedupe" for s in pipeline.stages):
            ids_to_check = {it["item_id"] for it in items}
            # Dedup signal = "already in seen DB" (true cross-run dedup).
            # Carryover from active.items is already excluded via eligibility queries.
            async with open_seen_db(task.seen_db) as conn_seen:
                already_seen_ids = await ItemSeenStore(conn_seen).filter_seen(ids_to_check)

        resume_done = await load_resume_done(
            conn_pre, pipeline_name=pipeline.name,
            stage_names=pipeline.stages, stage_types=stage_types,
            item_ids=[it["item_id"] for it in items],
        )

    # M1 + immutability: build a per-run stage_defs override without mutating task.stages.
    overrides: dict[str, dict] = {}
    if pipeline.sources and skip_ids:
        overrides["fetch_rss"] = {"skip_item_ids": frozenset(skip_ids)}
    if already_seen_ids:
        overrides["dedupe"] = {"already_seen_ids": frozenset(already_seen_ids)}
    stage_defs = _override_stage_params(stage_defs, overrides_by_type=overrides)

    # Drive stages
    driver_result = await drive_stages(
        task_name=task.name, pipeline=pipeline, stage_defs=stage_defs,
        run_key=effective_run_key, cycle_anchor=cycle_anchor.isoformat(),
        items=items, resume_done=resume_done,
    )

    # After driver: the authoritative working set for tx1/tx2/cleanup is the
    # post-driver list (initial items + promoted new_items). Using `items` here
    # would silently drop fetch_rss-produced items from the terminal-status
    # write path. `driver_result.new_items` is reserved for the items_store
    # INSERT and the seen_post job — both want only the freshly-fetched rows.
    all_items = driver_result.final_working_set

    # tx1
    tx1_out: dict = {}
    await (await writer.submit(build_tx1_job(
        task=task, pipeline=pipeline, stage_defs=stage_defs,
        run_key=effective_run_key, cycle_anchor=cycle_anchor.isoformat(),
        started_at=started_at, trigger=trigger, items=all_items,
        driver_result=driver_result, out=tx1_out,
    )))
    pipeline_run_id = tx1_out["pipeline_run_id"]
    items_in = tx1_out["items_in"]

    # post-tx1 seen best-effort (seen_post takes new_items only, never all_items)
    if driver_result.new_items:
        try:
            await (await writer.submit(build_seen_post_job(driver_result.new_items)))
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "seen post-tx1 write failed for %s items: %s",
                len(driver_result.new_items), exc,
            )

    # Sink emission (between tx1 and tx2)
    emission_results = []
    if pipeline.sinks:
        emitter = SinkEmitter(
            writer=writer, run_key=effective_run_key, pipeline_name=pipeline.name,
        )
        for sink_id in pipeline.sinks:
            sink_def = next(s for s in task.sinks if s.name == sink_id)
            sink = get_sink_class(sink_def.type)()
            sink_params = dict(sink_def.params)
            sink_params.update(
                task=task.name, pipeline=pipeline.name,
                run_key=effective_run_key,
                cycle_anchor=cycle_anchor.isoformat(), sink_id=sink_id,
                config_root=task.task_dir,
            )
            res = await emitter.emit_all(
                sink_id=sink_id, sink_type=sink_def.type, sink=sink,
                rendered=driver_result.rendered_output, params=sink_params,
            )
            emission_results.extend(res)

    # tx2 — uses all_items so per-item terminal rows cover fetched items too
    tx2_out: dict = {}
    await (await writer.submit(build_tx2_job(
        pipeline=pipeline, stage_defs=stage_defs,
        pipeline_run_id=pipeline_run_id, run_key=effective_run_key,
        items=all_items, driver_result=driver_result,
        emission_results=emission_results, items_in=items_in,
        run_level_error=None, out=tx2_out,
    )))

    # Cleanup — working_set_ids includes fetched items so cleanup never tombstones them
    if pipeline.post_run_cleanup not in (None, "none"):
        sink_failed = bool(pipeline.sinks) and any(not r.success for r in emission_results)
        await apply_post_run_cleanup(
            writer=writer, task=task, pipeline=pipeline,
            sink_failed=sink_failed,
            working_set_ids={it["item_id"] for it in all_items},
        )

    return RunResult(
        pipeline_name=pipeline.name, run_key=effective_run_key,
        status=tx2_out["status"], items_in=items_in,
        items_out=tx2_out["items_out"], error_msg=tx2_out["error_msg"],
    )
```

- [ ] Unit tests with mocked stages/sinks/writer; commit.

---

### Task 22: D4 invariant + v1 orchestration import guards

**File:** `tests/dag/test_d4_invariant.py`, `tests/dag/test_v1_orchestration_guard.py`

```python
# tests/dag/test_d4_invariant.py
from pathlib import Path

import pytest


@pytest.mark.parametrize("path", [
    "sluice/dag/phases/tx2.py",
    "sluice/dag/emission.py",
    "sluice/dag/cleanup.py",
])
def test_d4_no_seen_writes_outside_seen_post(path):
    src = Path(path).read_text()
    assert "ItemSeenStore" not in src, (
        f"D4 violated: {path} imports ItemSeenStore. "
        f"Seen writes are confined to sluice/dag/phases/seen_post.py only."
    )
    assert "INTO item_seen" not in src, (
        f"D4 violated: {path} writes to item_seen directly. "
        f"Seen writes are confined to seen_post.py only."
    )


def test_d4_seen_post_only_writes_seen():
    src = Path("sluice/dag/phases/seen_post.py").read_text()
    # The job body must use the seen connection (parameter named 'seen')
    assert "store.insert_many" in src
    # And must not write to the active DB tables
    for forbidden in ("INTO items", "INTO pipelines", "INTO stages",
                      "INTO sink_emissions", "INTO sink_delivery_log"):
        assert forbidden not in src, \
            f"seen_post wrote to active table: {forbidden}"
```

```python
# tests/dag/test_v1_orchestration_guard.py
from pathlib import Path

import pytest

FORBIDDEN_PREFIXES = (
    "from sluice.runner",
    "import sluice.runner",
    "from sluice.state",
    "import sluice.state",
    "from sluice.builders",
    "import sluice.builders",
    "from sluice.context",
    "import sluice.context",
    "from sluice.registry",  # v1 registry — not the new dag registry
    "import sluice.registry",
)


def _stage_files():
    base = Path("sluice/dag/stages")
    return sorted(p for p in base.glob("*.py") if p.name != "__init__.py")


@pytest.mark.parametrize("path", _stage_files())
def test_stage_does_not_import_v1_orchestration(path):
    src = path.read_text()
    for prefix in FORBIDDEN_PREFIXES:
        assert prefix not in src, \
            f"{path}: imports v1 orchestration module ({prefix!r}); wrap algorithm only"
```

- [ ] Run, PASS, commit.

---

### Task 23: CLI replacement (preserves Plan 1 path + flag semantics)

Modify `sluice/cli.py` `dag_run` to call the real runner. **Path semantics are exactly Plan 1**:
default `--config-dir = ./configs` and `task_dir = config_dir / "tasks" / task`. The CLI also
becomes responsible for the D1/D2 lifecycle: `ensure_dynamic_tables` BEFORE `writer.start()`.

Also add a module-level `import json` near the top of `sluice/cli.py` (Plan 1 uses
`import json` inline in one function only — promote it to module scope so the new run
output works without local-import noise).

```python
# at the top of sluice/cli.py
import json   # added by Plan 2 Task 23
```

```python
@app.command("run")
def dag_run(
    task: str = typer.Option(..., "-t", "--task",
                             help="task name under <config-dir>/tasks/"),
    pipeline: str = typer.Option(..., "-p", "--pipeline",
                                 help="pipeline name within the task"),
    config_dir: Path = typer.Option(Path("./configs"), "-c", "--config-dir"),
    cycle: str | None = typer.Option(
        None, "--cycle", help="ISO-8601 timestamp to override 'now' (default: utcnow)"
    ),
):
    """Run a single pipeline once (real runner — Plan 2)."""
    task_dir = config_dir / "tasks" / task
    if not task_dir.exists():
        typer.echo(f"task not found: {task_dir}", err=True)
        raise typer.Exit(code=2)

    from sluice.dag.loader import load_task
    from sluice.dag.run_pipeline import ensure_dynamic_tables, run_pipeline
    from sluice.dag.writer import WriterThread

    cfg = load_task(task_dir)
    if not any(p.name == pipeline for p in cfg.pipelines):
        typer.echo(f"pipeline {pipeline!r} not found in task {task!r}", err=True)
        raise typer.Exit(code=2)

    async def _go():
        # D2: caller does migration BEFORE starting the writer.
        await ensure_dynamic_tables(cfg)
        writer = WriterThread(active_path=cfg.db_path, seen_path=cfg.seen_db)
        await writer.start()
        try:
            now = datetime.fromisoformat(cycle) if cycle else None
            return await run_pipeline(cfg, pipeline, writer=writer, now=now)
        finally:
            await writer.stop()

    result = asyncio.run(_go())
    typer.echo(json.dumps({
        "pipeline_name": result.pipeline_name,
        "run_key": result.run_key,
        "status": result.status,
        "items_in": result.items_in,
        "items_out": result.items_out,
        "error_msg": result.error_msg,
    }, ensure_ascii=False))
```

- [ ] Step 23.1: Add module-level `import json` to `sluice/cli.py`.
- [ ] Step 23.2: Replace `dag_run` body per above (preserve Plan 1 `-c` default and path).
- [ ] Step 23.3: Run via subprocess on the e2e fixture; assert output JSON contains expected fields; commit.

---

### Task 24: End-to-end integration test

**Files:** `tests/dag/fixtures/e2e_task/{task,sources,sinks,stages,pipelines}.toml`, `tests/dag/test_run_pipeline_e2e.py`

Fixture: one pipeline `daily` with stages `fetch_rss → dedupe → cross_dedupe → filter → render`, sink `file_md` writing to a tmp dir.

Test:
1. Patch RSS fetch with frozen XML.
2. Run `run_pipeline(task, "daily", writer=writer, now=fixed_now)`.
3. Assert `RunResult.status='success'`, `items_in==2`, `items_out==2`.
4. Assert `.md` file exists with rendered content.
5. Assert `pipeline_daily(item_id, status='success')` for both items.
6. Assert `sink_emissions` has one row.
7. Assert `seen.item_seen` has both items.
8. Run again with same fixed_now — second run is no-op (eligibility skips terminal items, sink_emissions ordinal 0 already exists).

- [ ] Build fixture, write test, iterate to PASS, commit.

---

## Self-Review Checklist (run before considering Plan 2 done)

1. **D1**: `run_pipeline` accepts `writer: WriterThread` parameter; tests pass it explicitly. ✓
2. **D2**: `migrate_task_dynamic_tables` runs in its own connection block, before `writer.start()`. ✓
3. **D3**: tx1, post-tx1-seen, sink-emit, tx2, cleanup are separate `WriteJob` instances. No phase enum. ✓
4. **D4**: `tests/dag/test_d4_invariant.py` passes — tx2/emission/cleanup never reference `ItemSeenStore` or write `item_seen`. ✓
5. **B1 alignment**: post-tx1 seen job is wrapped in caller try/except so seen failure never aborts the run (spec best-effort). ✓
6. **Plan 1 schema**: every test SQL includes NOT NULL columns (`raw_json`, `updated_at`, `ingested_*`, `task`). ✓
7. **Plan 1 store API**: tx1 uses `ItemsStore.insert_new(**kwargs)`, `update_columns(item_id, cols, *, updated_at)`, `ItemSeenStore.insert_many(pairs, *, archived_at)`. ✓
8. **Real sink schema**: `sink_emissions` writes `sink_type/message_kind`; `sink_delivery_log` uses `attempt_at`. ✓
9. **Run-key adoption**: `oldest_run_key` uses `ORDER BY pending_since ASC`. ✓
10. **No placeholder ellipses**: no `...` is used as an implementation placeholder. Abstract method bodies may still use `...` where that is valid Python syntax.
11. **v1 orchestration guard**: `tests/dag/test_v1_orchestration_guard.py` passes for all stage files.
12. **fetch_rss skip injection**: runner pre-computes `skip_item_ids` and injects via `_override_stage_params` (NOT by mutating `task.stages`).
13. **dedupe injection**: runner injects `already_seen_ids` into the dedupe stage's params via the same per-run override mechanism. A test asserts `dedupe` actually drops a known-seen item end-to-end.
14. **No `task.stages` mutation**: runner uses `dataclasses.replace` to build a per-run `stage_defs` map. The original `task.stages` list is never modified, so re-running the same `TaskConfig` is side-effect-free.
15. **D1/D2 lifecycle**: CLI calls `ensure_dynamic_tables(task)` BEFORE `WriterThread.start()`. `run_pipeline` does NOT call `migrate_task_dynamic_tables`. `ensure_dynamic_tables` also opens the seen DB once before writer start so base seen migration cannot race the writer. A unit test invokes `run_pipeline` against a freshly-migrated DB without ever calling migration inside the test body.
16. **Driver new-item flow**: a test asserts `fetch_rss → dedupe → ...` actually applies dedupe to fetched items (the fix for the B1 regression in v2 review).
17. **Driver done_ids**: a test asserts an item that errors in stage S does NOT appear in `done_ids_per_stage` for any later stage.
18. **Attempts counter**: tx1 unit test asserts `attempts` increments only on `failed`; `done`/`dropped` keep the prior value (with first-time bump from 0 to 1 for visibility).
19. **all_items wiring**: runner builds `all_items = driver_result.final_working_set` after the driver and passes it (NOT the initial `items`) to `build_tx1_job`, `build_tx2_job`, and `apply_post_run_cleanup`. `driver_result.new_items` is reserved for `items_store.insert_new` (step 2 of tx1) and `build_seen_post_job`. A test asserts a fetch_rss-produced item ends up with `pipeline_<P>.status='success'` after a full run.
20. **New-item dirty persistence**: a tx1 unit test asserts that an item produced by a stub fetch_rss whose subsequent stage sets `it["fulltext"] = "x"` ends up with `items.fulltext = 'x'` after tx1 commits — proving the dirty-update loop iterates `all_items` (not just initial items).
21. **Entry carryover ↔ fetch dedup**: a runner unit test asserts that when an entry pipeline has both `items_carryover` containing item `X` and a frozen RSS feed that would re-emit `X`, fetch_rss's `skip_item_ids` includes `X` and the driver's working set has `X` exactly once (carryover instance, not the freshly-fetched one).
22. **Sink adapters**: built-in DAG sink adapters are lazily loaded; `file_md` accepts v1 `path`; push sinks resolve template files relative to `TaskConfig.task_dir`; sink params include `run_key` for footer/context rendering.

---

## Out of Scope (deferred to Plans 3-4)

- Active DB rotation + carryover + archive (Plan 3)
- Seen DB consistency reconciliation at rotation (Plan 3)
- GC flow (Plan 3)
- Prefect deployment generation (Plan 4)
- Cron scheduling (Plan 4)
- Migration tooling from v1 (Plan 4)
- The legacy `sluice/runner.py` v1 entry point (kept side-by-side until Plan 4 retirement)

---

## Execution Handoff

**Subagent-Driven (recommended)** — fresh subagent per task, review between tasks. With 24 tasks and ~70 sub-commits, this is the saner mode. Phase B's stage porting is highly parallelizable: tasks 7-11 are independent.

Alternative: **Inline Execution** — current session, batch checkpoints every 4-5 tasks.

Which approach?
