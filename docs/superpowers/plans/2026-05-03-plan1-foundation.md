# Plan 1 — Foundation: Schema, Loader, Writer Thread

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Lay the foundation for the multi-pipeline DAG architecture: new DB schema (active + seen), config loader for the 4-array TOML task format, DAG/shape validators, stage type registry (metadata only), a single-writer thread that owns all DB writes, and a CLI entry point. After this plan, `sluice run -t <task> -p <pipeline>` can load a config, migrate a fresh DB, drive a stub pipeline run (stages configured but not yet executed — actual stage execution lands in Plan 2), and produce correct audit rows.

**Architecture:** All new code lives under `sluice/dag/` so the existing codebase keeps working during the transition. `sluice/dag/db.py` owns connection + migration; `sluice/dag/config.py` defines the dataclasses; `sluice/dag/loader.py` parses 5 TOML files into a `TaskConfig`; `sluice/dag/validator.py` enforces shape + acyclicity + computes `reach(item)`; `sluice/dag/registry.py` declares stage types with their read/write column contracts; `sluice/dag/dynamic_tables.py` creates per-stage / per-pipeline tables from resolved config; `sluice/dag/stores/*` are thin per-table CRUD wrappers; `sluice/dag/writer.py` is the writer thread; `sluice/dag/runner.py` is a minimal stub that drives the run through tx1/tx2; `sluice/cli.py` gets a new top-level `run` subcommand wired to the stub runner.

**Tech Stack:** Python 3.11+, `aiosqlite`, `tomllib` (stdlib), stdlib `dataclasses`, `croniter` for cron parsing, `typer` (existing sluice CLI dep) for the CLI, `pytest` + `pytest-asyncio` for tests, `ruff` for linting.

**Spec reference:** `docs/superpowers/specs/2026-05-02-multi-pipeline-dag-design.md`.

---

## File Structure

**Created:**
- `sluice/dag/__init__.py`
- `sluice/dag/config.py` — dataclasses: `SourceDef`, `SinkDef`, `StageDef`, `PipelineDef`, `WindowDef`, `DeploymentsDef`, `TaskConfig`
- `sluice/dag/loader.py` — `load_task(task_dir: Path) -> TaskConfig`
- `sluice/dag/validator.py` — `validate_task(task: TaskConfig) -> None`, `compute_reach(task: TaskConfig, entry_pipeline: str) -> set[str]`
- `sluice/dag/registry.py` — `StageType`, `StageRegistry`, `register_stage_type()`, plus the closed-set 15 type registrations (metadata only)
- `sluice/dag/db.py` — `open_active_db()`, `open_seen_db()`, `apply_migrations()`
- `sluice/dag/dynamic_tables.py` — `ensure_pipeline_table()`, `ensure_pipeline_sink_pending_table()`, `ensure_stage_table()`, `migrate_task_dynamic_tables()`
- `sluice/dag/stores/__init__.py`
- `sluice/dag/stores/items.py` — `ItemsStore`
- `sluice/dag/stores/pipelines.py` — `PipelinesStore`
- `sluice/dag/stores/stages.py` — `StagesStore`
- `sluice/dag/stores/item_seen.py` — `ItemSeenStore`
- `sluice/dag/writer.py` — `WriteJob`, `WriterThread`
- `sluice/dag/runner.py` — `run_pipeline_stub()`
- `sluice/dag/migrations/v2_0001_active_init.sql`
- `sluice/dag/migrations/v2_0001_seen_init.sql`
- `tests/dag/__init__.py`
- `tests/dag/conftest.py` — fixtures: `tmp_task_dir`, `active_db`, `seen_db`
- `tests/dag/test_config.py`
- `tests/dag/test_loader.py`
- `tests/dag/test_validator.py`
- `tests/dag/test_db.py`
- `tests/dag/test_dynamic_tables.py`
- `tests/dag/test_registry.py`
- `tests/dag/test_stores.py`
- `tests/dag/test_writer.py`
- `tests/dag/test_runner_stub.py`
- `tests/dag/fixtures/example_task/task.toml`
- `tests/dag/fixtures/example_task/sources.toml`
- `tests/dag/fixtures/example_task/sinks.toml`
- `tests/dag/fixtures/example_task/stages.toml`
- `tests/dag/fixtures/example_task/pipelines.toml`

**Modified:**
- `pyproject.toml:1-50` — confirm `aiosqlite` and `pytest-asyncio` are in deps (likely already there)
- `sluice/__init__.py:1` — no functional change; nothing exported from `sluice.dag` yet

**Modified in this plan:** `sluice/cli.py` gets a new `run` subcommand (Task 19). The old commands stay intact alongside the new one until Plan 4 deletes them.

**Untouched:** existing `sluice/state/`, `sluice/processors/`, `sluice/runner.py`, `sluice/config.py`, `sluice/loader.py`. They keep working for the v1.1 codebase. Plan 4 deletes them.

---

## Task 1: Skeleton + dependencies

**Files:**
- Create: `sluice/dag/__init__.py`
- Create: `tests/dag/__init__.py`
- Create: `tests/dag/conftest.py`
- Modify: `pyproject.toml:1-50`

- [ ] **Step 1: Verify deps present**

```bash
rtk grep "aiosqlite" pyproject.toml
rtk grep "pytest-asyncio" pyproject.toml
```

Expected: both appear under `[project.dependencies]` and `[project.optional-dependencies.dev]` (or equivalent). If missing, add them.

- [ ] **Step 2: Create empty packages**

Write `sluice/dag/__init__.py`:
```python
"""Multi-pipeline DAG architecture (v2). See docs/superpowers/specs/2026-05-02-multi-pipeline-dag-design.md."""
```

Write `tests/dag/__init__.py`:
```python
```

- [ ] **Step 3: conftest with shared fixtures (placeholders, fleshed out as we go)**

Write `tests/dag/conftest.py`:
```python
from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture
def tmp_task_dir(tmp_path: Path) -> Path:
    d = tmp_path / "task"
    d.mkdir()
    return d
```

- [ ] **Step 4: Smoke test — package imports**

Write `tests/dag/test_smoke.py`:
```python
def test_dag_package_imports():
    import sluice.dag  # noqa: F401
```

Run: `pytest tests/dag/test_smoke.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add sluice/dag/__init__.py tests/dag/__init__.py tests/dag/conftest.py tests/dag/test_smoke.py
git commit -m "feat(dag): skeleton package and test scaffolding"
```

---

## Task 2: Active DB base schema migration

**Files:**
- Create: `sluice/dag/migrations/v2_0001_active_init.sql`

This migration sets up the **static** active-DB tables: `items`, `pipelines`, `stages`, `stage_attempt_log`, plus retained-but-modified `sink_emissions` / `sink_delivery_log` / `attachment_mirror` / `gc_log`. Per-stage and per-pipeline tables are created later by `dynamic_tables.py` from resolved config (Task 11).

- [ ] **Step 1: Write the migration SQL**

Write `sluice/dag/migrations/v2_0001_active_init.sql`:
```sql
-- items master content
CREATE TABLE items (
  item_id      TEXT PRIMARY KEY,
  source_name  TEXT NOT NULL,
  guid         TEXT,
  url          TEXT,
  title        TEXT,
  author       TEXT,
  published_at TEXT,
  ingested_at  TEXT NOT NULL,
  ingested_by  TEXT NOT NULL,
  updated_at   TEXT NOT NULL,
  tag          TEXT,
  raw_summary  TEXT,
  raw_content  TEXT,
  raw_json     TEXT NOT NULL,
  fulltext         TEXT,
  attachments_json TEXT,
  summary          TEXT,
  score            REAL,
  tags_json        TEXT,
  extras_json      TEXT,
  dropped          INTEGER NOT NULL DEFAULT 0,
  dropped_reason   TEXT,
  dropped_at       TEXT
);
CREATE INDEX idx_items_source     ON items(source_name);
CREATE INDEX idx_items_published  ON items(published_at);
CREATE INDEX idx_items_dropped    ON items(dropped);

-- pipelines run master
CREATE TABLE pipelines (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  run_key       TEXT NOT NULL UNIQUE,
  task          TEXT NOT NULL,
  pipeline_name TEXT NOT NULL,
  cycle_anchor  TEXT NOT NULL,
  trigger       TEXT NOT NULL,
  started_at    TEXT NOT NULL,
  finished_at   TEXT,
  status        TEXT NOT NULL,
  items_in      INTEGER,
  items_out     INTEGER,
  error_msg     TEXT
);
CREATE INDEX idx_pipelines_task     ON pipelines(task, pipeline_name);
CREATE INDEX idx_pipelines_started  ON pipelines(started_at);
CREATE INDEX idx_pipelines_run_key  ON pipelines(run_key);

-- stages run master
CREATE TABLE stages (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  pipeline_run_id INTEGER NOT NULL REFERENCES pipelines(id),
  stage_name      TEXT NOT NULL,
  started_at      TEXT NOT NULL,
  finished_at     TEXT,
  status          TEXT NOT NULL,
  items_in        INTEGER,
  items_out       INTEGER,
  error_msg       TEXT
);
CREATE INDEX idx_stages_pipeline ON stages(pipeline_run_id);
CREATE INDEX idx_stages_name     ON stages(stage_name);

-- stage attempt log (single shared table, append-only)
CREATE TABLE stage_attempt_log (
  id                INTEGER PRIMARY KEY AUTOINCREMENT,
  stage_run_id      INTEGER NOT NULL REFERENCES stages(id),
  pipeline_run_id   INTEGER NOT NULL REFERENCES pipelines(id),
  pipeline_name     TEXT NOT NULL,
  stage_name        TEXT NOT NULL,
  item_id           TEXT NOT NULL,
  attempt_no        INTEGER NOT NULL,
  status            TEXT NOT NULL,
  processed_at      TEXT NOT NULL,
  error_class       TEXT,
  error_msg         TEXT
);
CREATE INDEX idx_attempt_log_lookup ON stage_attempt_log(pipeline_name, stage_name, item_id);
CREATE INDEX idx_attempt_log_run    ON stage_attempt_log(stage_run_id);

-- sink emission idempotency (PK = run_key, sink_id, ordinal)
CREATE TABLE sink_emissions (
  run_key      TEXT NOT NULL,
  sink_id      TEXT NOT NULL,
  ordinal      INTEGER NOT NULL,
  sink_type    TEXT NOT NULL,
  message_kind TEXT,
  external_id  TEXT,
  emitted_at   TEXT NOT NULL,
  PRIMARY KEY (run_key, sink_id, ordinal)
);

-- sink delivery log
CREATE TABLE sink_delivery_log (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  run_key       TEXT NOT NULL,
  sink_id       TEXT NOT NULL,
  sink_type     TEXT NOT NULL,
  attempt_at    TEXT NOT NULL,
  ordinal       INTEGER NOT NULL,
  message_kind  TEXT,
  recipient     TEXT,
  external_id   TEXT,
  status        TEXT NOT NULL,
  error_class   TEXT,
  error_msg     TEXT
);
CREATE INDEX idx_delivery_run    ON sink_delivery_log(run_key, sink_id);
CREATE INDEX idx_delivery_status ON sink_delivery_log(status);

-- attachment mirror (carried forward unchanged)
CREATE TABLE attachment_mirror (
  url_hash           TEXT PRIMARY KEY,
  url                TEXT NOT NULL,
  local_path         TEXT NOT NULL,
  mime_type          TEXT,
  byte_size          INTEGER,
  fetched_at         TEXT NOT NULL,
  last_referenced_at TEXT NOT NULL,
  task               TEXT NOT NULL
);
CREATE INDEX idx_attachment_task       ON attachment_mirror(task);
CREATE INDEX idx_attachment_referenced ON attachment_mirror(last_referenced_at);

-- gc log
CREATE TABLE gc_log (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  ran_at       TEXT NOT NULL,
  table_name   TEXT NOT NULL,
  rows_deleted INTEGER NOT NULL,
  criteria     TEXT NOT NULL
);

-- schema version sentinel
CREATE TABLE schema_version (
  version  INTEGER PRIMARY KEY,
  applied_at TEXT NOT NULL
);
INSERT INTO schema_version (version, applied_at) VALUES (1, CURRENT_TIMESTAMP);
```

- [ ] **Step 2: Commit**

```bash
git add sluice/dag/migrations/v2_0001_active_init.sql
git commit -m "feat(dag): active-db base schema migration"
```

---

## Task 3: Seen DB migration

**Files:**
- Create: `sluice/dag/migrations/v2_0001_seen_init.sql`

- [ ] **Step 1: Write seen DB schema**

Write `sluice/dag/migrations/v2_0001_seen_init.sql`:
```sql
CREATE TABLE item_seen (
  item_id     TEXT PRIMARY KEY,
  source_hash TEXT NOT NULL,
  archived_at TEXT NOT NULL
);

CREATE TABLE schema_version (
  version    INTEGER PRIMARY KEY,
  applied_at TEXT NOT NULL
);
INSERT INTO schema_version (version, applied_at) VALUES (1, CURRENT_TIMESTAMP);
```

- [ ] **Step 2: Commit**

```bash
git add sluice/dag/migrations/v2_0001_seen_init.sql
git commit -m "feat(dag): seen-db base schema migration"
```

---

## Task 4: DB module — open + apply migrations

**Files:**
- Create: `sluice/dag/db.py`
- Test: `tests/dag/test_db.py`

- [ ] **Step 1: Write the failing test**

Write `tests/dag/test_db.py`:
```python
from __future__ import annotations

from pathlib import Path

import pytest
import aiosqlite

from sluice.dag import db


@pytest.mark.asyncio
async def test_open_active_db_creates_tables(tmp_path: Path):
    db_path = tmp_path / "active.db"
    async with db.open_active_db(str(db_path)) as conn:
        cursor = await conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        rows = await cursor.fetchall()
        names = {r[0] for r in rows}
    expected = {
        "items", "pipelines", "stages", "stage_attempt_log",
        "sink_emissions", "sink_delivery_log",
        "attachment_mirror", "gc_log", "schema_version",
    }
    assert expected.issubset(names), f"missing tables: {expected - names}"


@pytest.mark.asyncio
async def test_open_seen_db_creates_tables(tmp_path: Path):
    db_path = tmp_path / "seen.db"
    async with db.open_seen_db(str(db_path)) as conn:
        cursor = await conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        rows = await cursor.fetchall()
        names = {r[0] for r in rows}
    assert {"item_seen", "schema_version"}.issubset(names)


@pytest.mark.asyncio
async def test_open_active_db_idempotent(tmp_path: Path):
    db_path = tmp_path / "active.db"
    async with db.open_active_db(str(db_path)):
        pass
    # second open should not raise
    async with db.open_active_db(str(db_path)) as conn:
        cur = await conn.execute("SELECT version FROM schema_version")
        row = await cur.fetchone()
    assert row == (1,)


@pytest.mark.asyncio
async def test_active_db_pragmas(tmp_path: Path):
    db_path = tmp_path / "active.db"
    async with db.open_active_db(str(db_path)) as conn:
        cur = await conn.execute("PRAGMA foreign_keys")
        fk = (await cur.fetchone())[0]
        cur = await conn.execute("PRAGMA journal_mode")
        jm = (await cur.fetchone())[0]
    assert fk == 1
    assert jm.lower() == "wal"


@pytest.mark.asyncio
async def test_migration_picks_up_future_versions(tmp_path: Path, monkeypatch):
    """Drop a fake v2_0002_active migration into the migrations dir and verify it applies."""
    from sluice.dag import db as db_mod
    fake_dir = tmp_path / "migrations"
    fake_dir.mkdir()
    # copy the existing init file so v1 still works
    real_init = (db_mod._MIGRATIONS_DIR / "v2_0001_active_init.sql").read_text()
    (fake_dir / "v2_0001_active_init.sql").write_text(real_init)
    (fake_dir / "v2_0002_active_addcol.sql").write_text(
        "ALTER TABLE items ADD COLUMN extra_v2 TEXT;\n"
        "INSERT INTO schema_version (version, applied_at) VALUES (2, CURRENT_TIMESTAMP);\n"
    )
    monkeypatch.setattr(db_mod, "_MIGRATIONS_DIR", fake_dir)

    db_path = tmp_path / "active.db"
    async with db.open_active_db(str(db_path)) as conn:
        cur = await conn.execute("PRAGMA table_info(items)")
        cols = {r[1] for r in await cur.fetchall()}
        cur = await conn.execute("SELECT MAX(version) FROM schema_version")
        ver = (await cur.fetchone())[0]
    assert "extra_v2" in cols
    assert ver == 2
```

- [ ] **Step 2: Run tests — verify failure**

Run: `pytest tests/dag/test_db.py -v`
Expected: FAIL — module `sluice.dag.db` not found.

- [ ] **Step 3: Implement `db.py`**

Write `sluice/dag/db.py`:
```python
from __future__ import annotations

import re
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator

import aiosqlite

_MIGRATIONS_DIR = Path(__file__).parent / "migrations"


_MIGRATION_RE = re.compile(r"^v(\d+)_(\d+)_(active|seen)_.+\.sql$")


async def _apply_migrations(conn: aiosqlite.Connection, kind: str) -> None:
    cur = await conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_version'"
    )
    has_table = (await cur.fetchone()) is not None
    current = 0
    if has_table:
        cur = await conn.execute("SELECT MAX(version) FROM schema_version")
        row = await cur.fetchone()
        if row and row[0] is not None:
            current = int(row[0])
    candidates: list[tuple[int, Path]] = []
    for p in _MIGRATIONS_DIR.glob("*.sql"):
        m = _MIGRATION_RE.match(p.name)
        if not m or m.group(3) != kind:
            continue
        version = int(m.group(2))
        candidates.append((version, p))
    candidates.sort()
    for version, f in candidates:
        if version <= current:
            continue
        sql = f.read_text()
        await conn.executescript(sql)
        await conn.commit()


async def _set_pragmas(conn: aiosqlite.Connection) -> None:
    await conn.execute("PRAGMA foreign_keys = ON")
    await conn.execute("PRAGMA journal_mode = WAL")
    await conn.execute("PRAGMA busy_timeout = 5000")


@asynccontextmanager
async def open_active_db(path: str) -> AsyncIterator[aiosqlite.Connection]:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = await aiosqlite.connect(path)
    try:
        await _set_pragmas(conn)
        await _apply_migrations(conn, kind="active")
        yield conn
    finally:
        await conn.close()


@asynccontextmanager
async def open_seen_db(path: str) -> AsyncIterator[aiosqlite.Connection]:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = await aiosqlite.connect(path)
    try:
        await _set_pragmas(conn)
        await _apply_migrations(conn, kind="seen")
        yield conn
    finally:
        await conn.close()
```

- [ ] **Step 4: Run tests — verify pass**

Run: `pytest tests/dag/test_db.py -v`
Expected: PASS (all 4 tests)

- [ ] **Step 5: Commit**

```bash
git add sluice/dag/db.py tests/dag/test_db.py
git commit -m "feat(dag): open_active_db / open_seen_db with migrations"
```

---

## Task 5: Config dataclasses

**Files:**
- Create: `sluice/dag/config.py`
- Test: `tests/dag/test_config.py`

- [ ] **Step 1: Write the failing test**

Write `tests/dag/test_config.py`:
```python
from __future__ import annotations

from sluice.dag.config import (
    DeploymentsDef, PipelineDef, SinkDef, SourceDef, StageDef,
    TaskConfig, WindowDef,
)


def test_source_def_basic():
    s = SourceDef(name="rss1", type="rss", params={"url": "https://x"})
    assert s.name == "rss1"
    assert s.type == "rss"
    assert s.params["url"] == "https://x"


def test_pipeline_def_validates_input_mode():
    import pytest
    with pytest.raises(ValueError):
        PipelineDef(
            name="p", cron="* * * * *", stages=["s"],
            sources=None, upstream_all=None, upstream_any=None,
            sinks=None, post_run_cleanup=None, input_mode="weird",
        )


def test_pipeline_def_input_mode_default():
    p = PipelineDef(
        name="p", cron="* * * * *", stages=["s"],
        sources=["src1"], upstream_all=None, upstream_any=None,
        sinks=None, post_run_cleanup=None, input_mode=None,
    )
    assert p.input_mode == "union"


def test_task_config_round_trip():
    t = TaskConfig(
        name="airflow",
        db_path="data/airflow.db",
        seen_db="data/airflow_seen.db",
        enabled=True,
        timezone="Asia/Shanghai",
        window=WindowDef(anchor="0 8 * * *", length="24h"),
        deployments=DeploymentsDef(pipelines="all"),
        sources=[SourceDef(name="s", type="rss", params={"url": "x"})],
        sinks=[SinkDef(name="k", type="telegram", params={})],
        stages=[StageDef(name="st", type="dedupe", params={})],
        pipelines=[
            PipelineDef(
                name="p", cron="0 8 * * *", stages=["st"],
                sources=["s"], upstream_all=None, upstream_any=None,
                sinks=None, post_run_cleanup=None, input_mode="union",
            )
        ],
    )
    assert t.name == "airflow"
    assert t.pipelines[0].input_mode == "union"
```

- [ ] **Step 2: Run tests — verify failure**

Run: `pytest tests/dag/test_config.py -v`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement `config.py`**

Write `sluice/dag/config.py`:
```python
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


@dataclass(frozen=True)
class SourceDef:
    name: str
    type: str
    params: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SinkDef:
    name: str
    type: str
    params: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class StageDef:
    name: str
    type: str
    params: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class WindowDef:
    anchor: str    # cron expression
    length: str    # duration string e.g. "24h"


@dataclass(frozen=True)
class DeploymentsDef:
    pipelines: list[str] | Literal["all"] = "all"


@dataclass(frozen=True)
class PipelineDef:
    name: str
    cron: str
    stages: list[str]
    sources: list[str] | None
    upstream_all: list[str] | None
    upstream_any: list[str] | None
    sinks: list[str] | None
    post_run_cleanup: str | None       # 'drop_unprocessed_upstream_local' | 'drop_unprocessed_upstream_global' | None
    input_mode: str | None = "union"   # 'union' | 'intersection'

    def __post_init__(self):
        if self.input_mode not in (None, "union", "intersection"):
            raise ValueError(f"input_mode must be 'union' or 'intersection', got {self.input_mode!r}")
        if self.input_mode is None:
            object.__setattr__(self, "input_mode", "union")


@dataclass(frozen=True)
class TaskConfig:
    name: str
    db_path: str
    seen_db: str
    enabled: bool
    timezone: str | None
    window: WindowDef
    deployments: DeploymentsDef
    sources: list[SourceDef]
    sinks: list[SinkDef]
    stages: list[StageDef]
    pipelines: list[PipelineDef]
```

- [ ] **Step 4: Run tests — verify pass**

Run: `pytest tests/dag/test_config.py -v`
Expected: PASS (4 tests)

- [ ] **Step 5: Commit**

```bash
git add sluice/dag/config.py tests/dag/test_config.py
git commit -m "feat(dag): config dataclasses"
```

---

## Task 6: Loader — parse 5 TOML files

**Files:**
- Create: `sluice/dag/loader.py`
- Create: `tests/dag/fixtures/example_task/task.toml`
- Create: `tests/dag/fixtures/example_task/sources.toml`
- Create: `tests/dag/fixtures/example_task/sinks.toml`
- Create: `tests/dag/fixtures/example_task/stages.toml`
- Create: `tests/dag/fixtures/example_task/pipelines.toml`
- Test: `tests/dag/test_loader.py`

- [ ] **Step 1: Create fixture task directory**

Write `tests/dag/fixtures/example_task/task.toml`:
```toml
[task]
name     = "example"
db_path  = "data/example.db"
seen_db  = "data/example_seen.db"
enabled  = true
timezone = "UTC"

[task.window]
anchor = "0 8 * * *"
length = "24h"

[task.deployments]
pipelines = "all"
```

Write `tests/dag/fixtures/example_task/sources.toml`:
```toml
[[sources]]
name = "blog"
type = "rss"
url  = "https://example.com/blog.rss"
tag  = "ex"

[[sources]]
name = "releases"
type = "rss"
url  = "https://example.com/releases.rss"
```

Write `tests/dag/fixtures/example_task/sinks.toml`:
```toml
[[sinks]]
name      = "tg"
type      = "telegram"
chat_id   = "abc"
token_env = "TG_TOKEN"
```

Write `tests/dag/fixtures/example_task/stages.toml`:
```toml
[[stages]]
name    = "fetch_blog_rss"
type    = "fetch_rss"
sources = ["blog"]

[[stages]]
name        = "fetch_releases_rss"
type        = "fetch_rss"
sources     = ["releases"]

[[stages]]
name = "dd"
type = "dedupe"

[[stages]]
name = "summarize"
type = "summarize_score_tag"
model = "claude-sonnet-4-6"

[[stages]]
name = "sort"
type = "sort"
key  = "score"

[[stages]]
name = "limit"
type = "limit"
n    = 10

[[stages]]
name     = "render"
type     = "render"
template = "digest"
```

Write `tests/dag/fixtures/example_task/pipelines.toml`:
```toml
[[pipelines]]
name    = "fetch_blog"
cron    = "0 */6 * * *"
sources = ["blog"]
stages  = ["fetch_blog_rss", "dd"]

[[pipelines]]
name    = "fetch_releases"
cron    = "0 */4 * * *"
sources = ["releases"]
stages  = ["fetch_releases_rss", "dd"]

[[pipelines]]
name         = "summarize_nightly"
cron         = "0 22 * * *"
upstream_all = ["fetch_blog", "fetch_releases"]
stages       = ["summarize"]

[[pipelines]]
name             = "report_telegram"
cron             = "30 22 * * *"
upstream_all     = ["summarize_nightly"]
stages           = ["sort", "limit", "render"]
sinks            = ["tg"]
post_run_cleanup = "drop_unprocessed_upstream_local"
```

- [ ] **Step 2: Write the failing test**

Write `tests/dag/test_loader.py`:
```python
from __future__ import annotations

from pathlib import Path

import pytest

from sluice.dag.loader import load_task

FIXTURE = Path(__file__).parent / "fixtures" / "example_task"


def test_load_task_basic():
    t = load_task(FIXTURE)
    assert t.name == "example"
    assert t.db_path == "data/example.db"
    assert t.window.anchor == "0 8 * * *"
    assert t.window.length == "24h"
    assert t.deployments.pipelines == "all"
    assert len(t.sources) == 2
    assert {s.name for s in t.sources} == {"blog", "releases"}
    assert len(t.sinks) == 1 and t.sinks[0].type == "telegram"
    assert len(t.stages) == 7
    assert len(t.pipelines) == 4
    p = next(p for p in t.pipelines if p.name == "summarize_nightly")
    assert p.upstream_all == ["fetch_blog", "fetch_releases"]
    assert p.input_mode == "union"


def test_load_task_missing_directory(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        load_task(tmp_path / "nope")


def test_load_task_missing_required_file(tmp_path: Path):
    d = tmp_path / "incomplete"
    d.mkdir()
    (d / "task.toml").write_text("[task]\nname='x'\ndb_path='a.db'\nseen_db='b.db'\nenabled=true\n[task.window]\nanchor='0 8 * * *'\nlength='24h'\n[task.deployments]\npipelines='all'\n")
    with pytest.raises(FileNotFoundError, match="sources.toml"):
        load_task(d)


def test_load_task_explicit_deployments_list(tmp_path: Path, monkeypatch):
    import shutil
    d = tmp_path / "task"
    shutil.copytree(FIXTURE, d)
    (d / "task.toml").write_text(
        (FIXTURE / "task.toml").read_text().replace(
            'pipelines = "all"',
            'pipelines = ["fetch_blog", "summarize_nightly"]',
        )
    )
    t = load_task(d)
    assert t.deployments.pipelines == ["fetch_blog", "summarize_nightly"]
```

- [ ] **Step 3: Run tests — verify failure**

Run: `pytest tests/dag/test_loader.py -v`
Expected: FAIL — module not found.

- [ ] **Step 4: Implement `loader.py`**

Write `sluice/dag/loader.py`:
```python
from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any

from sluice.dag.config import (
    DeploymentsDef, PipelineDef, SinkDef, SourceDef, StageDef,
    TaskConfig, WindowDef,
)

_REQUIRED_FILES = ("task.toml", "sources.toml", "sinks.toml", "stages.toml", "pipelines.toml")


def _read_toml(path: Path) -> dict[str, Any]:
    with path.open("rb") as f:
        return tomllib.load(f)


def _split_named(d: dict[str, Any], extra_keys: tuple[str, ...] = ()) -> tuple[dict[str, Any], dict[str, Any]]:
    known = {"name", "type", *extra_keys}
    head = {k: v for k, v in d.items() if k in known}
    params = {k: v for k, v in d.items() if k not in known}
    return head, params


def load_task(task_dir: Path) -> TaskConfig:
    if not task_dir.exists():
        raise FileNotFoundError(f"task directory not found: {task_dir}")
    for fname in _REQUIRED_FILES:
        if not (task_dir / fname).exists():
            raise FileNotFoundError(f"missing required file: {task_dir / fname}")

    task_doc = _read_toml(task_dir / "task.toml")["task"]
    window = WindowDef(anchor=task_doc["window"]["anchor"], length=task_doc["window"]["length"])
    deployments = DeploymentsDef(pipelines=task_doc["deployments"]["pipelines"])

    sources_doc = _read_toml(task_dir / "sources.toml").get("sources", [])
    sources = []
    for entry in sources_doc:
        head, params = _split_named(entry)
        sources.append(SourceDef(name=head["name"], type=head["type"], params=params))

    sinks_doc = _read_toml(task_dir / "sinks.toml").get("sinks", [])
    sinks = []
    for entry in sinks_doc:
        head, params = _split_named(entry)
        sinks.append(SinkDef(name=head["name"], type=head["type"], params=params))

    stages_doc = _read_toml(task_dir / "stages.toml").get("stages", [])
    stages = []
    for entry in stages_doc:
        head, params = _split_named(entry)
        stages.append(StageDef(name=head["name"], type=head["type"], params=params))

    pipelines_doc = _read_toml(task_dir / "pipelines.toml").get("pipelines", [])
    pipelines = []
    for entry in pipelines_doc:
        pipelines.append(PipelineDef(
            name=entry["name"],
            cron=entry["cron"],
            stages=entry["stages"],
            sources=entry.get("sources"),
            upstream_all=entry.get("upstream_all"),
            upstream_any=entry.get("upstream_any"),
            sinks=entry.get("sinks"),
            post_run_cleanup=entry.get("post_run_cleanup"),
            input_mode=entry.get("input_mode", "union"),
        ))

    return TaskConfig(
        name=task_doc["name"],
        db_path=task_doc["db_path"],
        seen_db=task_doc["seen_db"],
        enabled=task_doc.get("enabled", True),
        timezone=task_doc.get("timezone"),
        window=window,
        deployments=deployments,
        sources=sources,
        sinks=sinks,
        stages=stages,
        pipelines=pipelines,
    )
```

- [ ] **Step 5: Run tests — verify pass**

Run: `pytest tests/dag/test_loader.py -v`
Expected: PASS (4 tests)

- [ ] **Step 6: Commit**

```bash
git add sluice/dag/loader.py tests/dag/fixtures/example_task tests/dag/test_loader.py
git commit -m "feat(dag): loader for 5-file TOML task layout"
```

---

## Task 7: Stage type registry (metadata only)

**Files:**
- Create: `sluice/dag/registry.py`
- Test: `tests/dag/test_registry.py`

This task registers the 15 stage types as metadata only — no actual processor implementations yet. The registry holds: type name, reads (items columns), writes (items columns), drop semantic, persistent vs volatile, can be entry/exit (helpers for shape validation).

- [ ] **Step 1: Write the failing test**

Write `tests/dag/test_registry.py`:
```python
from __future__ import annotations

import pytest

from sluice.dag.registry import REGISTRY, StageType, register_stage_type


def test_all_15_types_registered():
    expected = {
        "fetch_rss",
        "dedupe", "cross_dedupe", "filter", "field_filter",
        "html_strip", "fetcher_apply", "mirror_attachments",
        "enrich", "llm_stage", "score_tag", "summarize_score_tag",
        "sort", "limit", "render",
    }
    assert set(REGISTRY) == expected


def test_volatile_classification():
    assert REGISTRY["sort"].volatile is True
    assert REGISTRY["render"].volatile is True
    assert REGISTRY["filter"].volatile is False
    assert REGISTRY["summarize_score_tag"].volatile is False


def test_drop_semantic():
    assert REGISTRY["filter"].drops is True
    assert REGISTRY["limit"].drops is True
    assert REGISTRY["dedupe"].drops is True
    assert REGISTRY["cross_dedupe"].drops is True
    assert REGISTRY["fetcher_apply"].drops is False


def test_writes_columns_are_subset_of_known_items_columns():
    KNOWN = {
        "source_name", "guid", "url", "title", "author",
        "published_at", "tag",
        "raw_summary", "raw_content", "raw_json",
        "fulltext", "attachments_json",
        "summary", "score", "tags_json", "extras_json",
    }
    for name, st in REGISTRY.items():
        for col in st.writes:
            assert col in KNOWN, f"stage {name!r} writes unknown column {col!r}"


def test_register_duplicate_raises():
    with pytest.raises(ValueError):
        register_stage_type(StageType(
            name="dedupe", reads=(), writes=(), drops=False, volatile=False,
        ))


def test_fetch_rss_writes_identity_columns():
    st = REGISTRY["fetch_rss"]
    assert "url" in st.writes
    assert "title" in st.writes
    assert "raw_json" in st.writes


def test_summarize_score_tag_writes():
    st = REGISTRY["summarize_score_tag"]
    assert set(st.writes) >= {"summary", "score", "tags_json"}
```

- [ ] **Step 2: Run tests — verify failure**

Run: `pytest tests/dag/test_registry.py -v`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement `registry.py`**

Write `sluice/dag/registry.py`:
```python
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class StageType:
    name: str
    reads: tuple[str, ...]
    writes: tuple[str, ...]
    drops: bool       # True iff this stage may filter/limit/dedupe items in this run
    volatile: bool    # True iff outputs are not persisted (sort, render); never resume-skipped


REGISTRY: dict[str, StageType] = {}


def register_stage_type(st: StageType) -> None:
    if st.name in REGISTRY:
        raise ValueError(f"stage type already registered: {st.name!r}")
    REGISTRY[st.name] = st


def _bootstrap() -> None:
    types = [
        StageType(
            name="fetch_rss",
            reads=(),
            writes=("source_name", "guid", "url", "title", "author",
                    "published_at", "raw_summary", "raw_content", "raw_json", "tag"),
            drops=False, volatile=False,
        ),
        StageType(name="dedupe",       reads=(),                 writes=(), drops=True,  volatile=False),
        StageType(name="cross_dedupe", reads=(),                 writes=(), drops=True,  volatile=False),
        StageType(name="filter",       reads=(),                 writes=(), drops=True,  volatile=False),
        StageType(name="field_filter", reads=(),                 writes=(), drops=False, volatile=False),
        StageType(name="html_strip",
                  reads=("raw_summary", "raw_content", "fulltext"),
                  writes=("raw_summary", "raw_content", "fulltext"),
                  drops=False, volatile=False),
        StageType(name="fetcher_apply",
                  reads=("url",),
                  writes=("fulltext",),
                  drops=False, volatile=False),
        StageType(name="mirror_attachments",
                  reads=("raw_json", "fulltext"),
                  writes=("attachments_json",),
                  drops=False, volatile=False),
        StageType(name="enrich",
                  reads=(),
                  writes=("extras_json",),
                  drops=False, volatile=False),
        StageType(name="llm_stage",
                  reads=(),
                  writes=("extras_json",),
                  drops=False, volatile=False),
        StageType(name="score_tag",
                  reads=(),
                  writes=("score", "tags_json"),
                  drops=False, volatile=False),
        StageType(name="summarize_score_tag",
                  reads=(),
                  writes=("summary", "score", "tags_json"),
                  drops=False, volatile=False),
        StageType(name="sort",   reads=(), writes=(), drops=False, volatile=True),
        StageType(name="limit",  reads=(), writes=(), drops=True,  volatile=False),
        StageType(name="render", reads=(), writes=(), drops=False, volatile=True),
    ]
    for st in types:
        register_stage_type(st)


_bootstrap()
```

- [ ] **Step 4: Run tests — verify pass**

Run: `pytest tests/dag/test_registry.py -v`
Expected: PASS (7 tests)

- [ ] **Step 5: Commit**

```bash
git add sluice/dag/registry.py tests/dag/test_registry.py
git commit -m "feat(dag): stage type registry (metadata for 15 closed-set types)"
```

---

## Task 8: Validator — shape constraints

**Files:**
- Create: `sluice/dag/validator.py`
- Test: `tests/dag/test_validator.py`

- [ ] **Step 1: Write failing tests for shape rules**

Write `tests/dag/test_validator.py`:
```python
from __future__ import annotations

import pytest
from pathlib import Path

from sluice.dag.config import (
    DeploymentsDef, PipelineDef, SinkDef, SourceDef, StageDef,
    TaskConfig, WindowDef,
)
from sluice.dag.loader import load_task
from sluice.dag.validator import (
    ValidationError, validate_task, compute_reach,
)

FIXTURE = Path(__file__).parent / "fixtures" / "example_task"


def _empty_task(**overrides) -> TaskConfig:
    base = dict(
        name="t", db_path="a.db", seen_db="b.db", enabled=True, timezone=None,
        window=WindowDef(anchor="0 8 * * *", length="24h"),
        deployments=DeploymentsDef(pipelines="all"),
        sources=[SourceDef(name="s", type="rss")],
        sinks=[],
        stages=[
            StageDef(name="fr", type="fetch_rss", params={"sources": ["s"]}),
            StageDef(name="dd", type="dedupe"),
            StageDef(name="sm", type="summarize_score_tag"),
            StageDef(name="srt", type="sort"),
            StageDef(name="rdr", type="render"),
        ],
        pipelines=[],
    )
    base.update(overrides)
    return TaskConfig(**base)


def test_fixture_validates_clean():
    t = load_task(FIXTURE)
    validate_task(t)  # must not raise


def test_pipeline_with_both_sources_and_upstream_rejected():
    t = _empty_task(pipelines=[
        PipelineDef(name="p", cron="* * * * *", stages=["fr"],
                    sources=["s"], upstream_all=["other"], upstream_any=None,
                    sinks=None, post_run_cleanup=None, input_mode="union"),
    ])
    with pytest.raises(ValidationError, match="exactly one"):
        validate_task(t)


def test_pipeline_with_neither_rejected():
    t = _empty_task(pipelines=[
        PipelineDef(name="p", cron="* * * * *", stages=["fr"],
                    sources=None, upstream_all=None, upstream_any=None,
                    sinks=None, post_run_cleanup=None, input_mode="union"),
    ])
    with pytest.raises(ValidationError, match="exactly one"):
        validate_task(t)


def test_fetch_rss_in_upstream_pipeline_rejected():
    t = _empty_task(pipelines=[
        PipelineDef(name="entry", cron="* * * * *", stages=["fr"],
                    sources=["s"], upstream_all=None, upstream_any=None,
                    sinks=None, post_run_cleanup=None, input_mode="union"),
        PipelineDef(name="bad", cron="* * * * *", stages=["fr", "dd"],
                    sources=None, upstream_all=["entry"], upstream_any=None,
                    sinks=None, post_run_cleanup=None, input_mode="union"),
    ])
    with pytest.raises(ValidationError, match="fetch_"):
        validate_task(t)


def test_entry_pipeline_must_contain_fetch():
    t = _empty_task(pipelines=[
        PipelineDef(name="p", cron="* * * * *", stages=["dd"],
                    sources=["s"], upstream_all=None, upstream_any=None,
                    sinks=None, post_run_cleanup=None, input_mode="union"),
    ])
    with pytest.raises(ValidationError, match="fetch_"):
        validate_task(t)


def test_last_stage_volatile_without_sinks_rejected():
    t = _empty_task(pipelines=[
        PipelineDef(name="entry", cron="* * * * *", stages=["fr"],
                    sources=["s"], upstream_all=None, upstream_any=None,
                    sinks=None, post_run_cleanup=None, input_mode="union"),
        PipelineDef(name="bad", cron="* * * * *", stages=["sm", "srt"],
                    sources=None, upstream_all=["entry"], upstream_any=None,
                    sinks=None, post_run_cleanup=None, input_mode="union"),
    ])
    with pytest.raises(ValidationError, match="last stage"):
        validate_task(t)


def test_last_stage_volatile_with_sinks_ok():
    t = _empty_task(
        sinks=[SinkDef(name="k", type="telegram")],
        pipelines=[
            PipelineDef(name="entry", cron="* * * * *", stages=["fr"],
                        sources=["s"], upstream_all=None, upstream_any=None,
                        sinks=None, post_run_cleanup=None, input_mode="union"),
            PipelineDef(name="rep", cron="* * * * *", stages=["sm", "srt", "rdr"],
                        sources=None, upstream_all=["entry"], upstream_any=None,
                        sinks=["k"], post_run_cleanup=None, input_mode="union"),
        ])
    validate_task(t)  # ok


def test_unknown_source_reference():
    t = _empty_task(pipelines=[
        PipelineDef(name="p", cron="* * * * *", stages=["fr"],
                    sources=["nope"], upstream_all=None, upstream_any=None,
                    sinks=None, post_run_cleanup=None, input_mode="union"),
    ])
    with pytest.raises(ValidationError, match="source"):
        validate_task(t)


def test_unknown_stage_type_rejected():
    t = _empty_task(stages=[StageDef(name="x", type="not_a_real_type")])
    with pytest.raises(ValidationError, match="unknown stage type"):
        validate_task(t)
```

- [ ] **Step 2: Run tests — verify failure**

Run: `pytest tests/dag/test_validator.py -v`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement `validator.py` (shape only; DAG / reach come in next tasks)**

Write `sluice/dag/validator.py`:
```python
from __future__ import annotations

from collections import defaultdict, deque
from typing import Iterable

from sluice.dag.config import PipelineDef, TaskConfig
from sluice.dag.registry import REGISTRY


class ValidationError(ValueError):
    pass


def _stage_type_of(task: TaskConfig, stage_name: str) -> str:
    for s in task.stages:
        if s.name == stage_name:
            return s.type
    raise ValidationError(f"unknown stage reference: {stage_name!r}")


def _check_references(task: TaskConfig) -> None:
    src_names = {s.name for s in task.sources}
    sink_names = {s.name for s in task.sinks}
    stage_names = {s.name for s in task.stages}
    pipe_names = {p.name for p in task.pipelines}

    for s in task.stages:
        if s.type not in REGISTRY:
            raise ValidationError(f"unknown stage type: {s.type!r} (stage {s.name!r})")
        for src in s.params.get("sources", []) or []:
            if src not in src_names:
                raise ValidationError(f"stage {s.name!r} references unknown source {src!r}")

    for p in task.pipelines:
        for stage in p.stages:
            if stage not in stage_names:
                raise ValidationError(f"pipeline {p.name!r} references unknown stage {stage!r}")
        for src in p.sources or []:
            if src not in src_names:
                raise ValidationError(f"pipeline {p.name!r} references unknown source {src!r}")
        for sink in p.sinks or []:
            if sink not in sink_names:
                raise ValidationError(f"pipeline {p.name!r} references unknown sink {sink!r}")
        for u in (p.upstream_all or []) + (p.upstream_any or []):
            if u not in pipe_names:
                raise ValidationError(f"pipeline {p.name!r} references unknown upstream {u!r}")


def _check_shape(task: TaskConfig) -> None:
    for p in task.pipelines:
        has_sources = bool(p.sources)
        has_upstream = bool(p.upstream_all) or bool(p.upstream_any)
        if has_sources == has_upstream:
            raise ValidationError(
                f"pipeline {p.name!r}: must have exactly one of sources or upstream"
            )
        contains_fetch = any(
            _stage_type_of(task, s).startswith("fetch_") for s in p.stages
        )
        if has_sources and not contains_fetch:
            raise ValidationError(
                f"pipeline {p.name!r} declares sources but contains no fetch_<type> stage"
            )
        if has_upstream and contains_fetch:
            raise ValidationError(
                f"pipeline {p.name!r} has upstream but contains a fetch_<type> stage"
            )

        if not p.stages:
            raise ValidationError(f"pipeline {p.name!r} has no stages")
        last_type = _stage_type_of(task, p.stages[-1])
        last_st = REGISTRY[last_type]
        if last_st.volatile and not p.sinks:
            raise ValidationError(
                f"pipeline {p.name!r} last stage {p.stages[-1]!r} is volatile "
                f"({last_type}) but pipeline has no sinks"
            )


def _check_acyclic(task: TaskConfig) -> None:
    pipe_by_name = {p.name: p for p in task.pipelines}
    incoming: dict[str, set[str]] = defaultdict(set)
    indeg: dict[str, int] = {p.name: 0 for p in task.pipelines}
    for p in task.pipelines:
        for u in (p.upstream_all or []) + (p.upstream_any or []):
            incoming[p.name].add(u)
            indeg[p.name] += 1
    queue = deque([n for n, d in indeg.items() if d == 0])
    visited = 0
    while queue:
        n = queue.popleft()
        visited += 1
        for child, parents in incoming.items():
            if n in parents:
                indeg[child] -= 1
                if indeg[child] == 0:
                    queue.append(child)
    if visited != len(task.pipelines):
        raise ValidationError("DAG cycle detected in pipelines")


def validate_task(task: TaskConfig) -> None:
    _check_references(task)
    _check_shape(task)
    _check_acyclic(task)


def compute_reach(task: TaskConfig, entry_pipeline: str) -> set[str]:
    """All pipelines reachable downstream from `entry_pipeline`, intersected with
    enabled deployment list. Inclusive of `entry_pipeline`."""
    pipe_by_name = {p.name: p for p in task.pipelines}
    if entry_pipeline not in pipe_by_name:
        raise ValidationError(f"unknown entry pipeline: {entry_pipeline!r}")
    if isinstance(task.deployments.pipelines, list):
        enabled = set(task.deployments.pipelines)
    else:
        enabled = {p.name for p in task.pipelines}

    reach: set[str] = set()
    stack = [entry_pipeline]
    while stack:
        n = stack.pop()
        if n in reach or n not in enabled:
            continue
        reach.add(n)
        for p in task.pipelines:
            ups = (p.upstream_all or []) + (p.upstream_any or [])
            if n in ups:
                stack.append(p.name)
    return reach
```

- [ ] **Step 4: Run tests — verify pass**

Run: `pytest tests/dag/test_validator.py -v`
Expected: PASS (9 tests)

- [ ] **Step 5: Commit**

```bash
git add sluice/dag/validator.py tests/dag/test_validator.py
git commit -m "feat(dag): validator — references, shape, acyclic, reach"
```

---

## Task 9: Validator — DAG cycle and reach() tests

**Files:**
- Modify: `tests/dag/test_validator.py:end`

- [ ] **Step 1: Add cycle + reach tests**

Append to `tests/dag/test_validator.py`:
```python
def test_dag_cycle_rejected():
    t = _empty_task(pipelines=[
        PipelineDef(name="entry", cron="* * * * *", stages=["fr"],
                    sources=["s"], upstream_all=None, upstream_any=None,
                    sinks=None, post_run_cleanup=None, input_mode="union"),
        PipelineDef(name="a", cron="* * * * *", stages=["sm"],
                    sources=None, upstream_all=["entry", "b"], upstream_any=None,
                    sinks=None, post_run_cleanup=None, input_mode="union"),
        PipelineDef(name="b", cron="* * * * *", stages=["sm"],
                    sources=None, upstream_all=["a"], upstream_any=None,
                    sinks=None, post_run_cleanup=None, input_mode="union"),
    ])
    with pytest.raises(ValidationError, match="cycle"):
        validate_task(t)


def test_reach_from_fixture():
    t = load_task(FIXTURE)
    r = compute_reach(t, "fetch_blog")
    assert r == {"fetch_blog", "summarize_nightly", "report_telegram"}
    r2 = compute_reach(t, "fetch_releases")
    assert r2 == {"fetch_releases", "summarize_nightly", "report_telegram"}


def test_reach_excludes_disabled(tmp_path: Path):
    import shutil
    d = tmp_path / "task"
    shutil.copytree(FIXTURE, d)
    (d / "task.toml").write_text(
        (FIXTURE / "task.toml").read_text().replace(
            'pipelines = "all"',
            'pipelines = ["fetch_blog", "summarize_nightly"]',
        )
    )
    t = load_task(d)
    r = compute_reach(t, "fetch_blog")
    assert "report_telegram" not in r
    assert r == {"fetch_blog", "summarize_nightly"}
```

- [ ] **Step 2: Run tests — verify pass**

Run: `pytest tests/dag/test_validator.py -v`
Expected: PASS (12 tests).

- [ ] **Step 3: Commit**

```bash
git add tests/dag/test_validator.py
git commit -m "test(dag): cycle detection and reach() tests"
```

---

## Task 10: Dynamic per-stage and per-pipeline tables

**Files:**
- Create: `sluice/dag/dynamic_tables.py`
- Test: `tests/dag/test_dynamic_tables.py`

- [ ] **Step 1: Write failing test**

Write `tests/dag/test_dynamic_tables.py`:
```python
from __future__ import annotations

from pathlib import Path

import pytest

from sluice.dag import db
from sluice.dag.dynamic_tables import (
    ensure_pipeline_table, ensure_pipeline_sink_pending_table,
    ensure_stage_table, migrate_task_dynamic_tables,
)
from sluice.dag.loader import load_task

FIXTURE = Path(__file__).parent / "fixtures" / "example_task"


@pytest.mark.asyncio
async def test_ensure_pipeline_table(tmp_path):
    db_path = tmp_path / "active.db"
    async with db.open_active_db(str(db_path)) as conn:
        await ensure_pipeline_table(conn, "report_telegram")
        cur = await conn.execute(
            "PRAGMA table_info(pipeline_report_telegram)"
        )
        cols = {r[1] for r in await cur.fetchall()}
    assert {"item_id", "last_run_id", "status", "processed_at", "error_msg"}.issubset(cols)


@pytest.mark.asyncio
async def test_ensure_pipeline_sink_pending_table(tmp_path):
    db_path = tmp_path / "active.db"
    async with db.open_active_db(str(db_path)) as conn:
        await ensure_pipeline_sink_pending_table(conn, "report_telegram")
        cur = await conn.execute(
            "PRAGMA table_info(pipeline_report_telegram_sink_pending)"
        )
        cols = {r[1] for r in await cur.fetchall()}
    assert {"item_id", "run_key", "pending_since"}.issubset(cols)


@pytest.mark.asyncio
async def test_ensure_stage_table(tmp_path):
    db_path = tmp_path / "active.db"
    async with db.open_active_db(str(db_path)) as conn:
        await ensure_stage_table(conn, "summarize")
        cur = await conn.execute("PRAGMA table_info(stage_summarize)")
        cols = {r[1] for r in await cur.fetchall()}
    assert {
        "pipeline_name", "item_id", "status", "attempts",
        "last_stage_run_id", "last_pipeline_run_id",
        "last_processed_at", "last_error_class", "last_error_msg",
    }.issubset(cols)


@pytest.mark.asyncio
async def test_migrate_task_dynamic_tables_creates_all(tmp_path):
    db_path = tmp_path / "active.db"
    t = load_task(FIXTURE)
    async with db.open_active_db(str(db_path)) as conn:
        await migrate_task_dynamic_tables(conn, t)
        cur = await conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
        names = {r[0] for r in await cur.fetchall()}
    expected_pipe = {f"pipeline_{p.name}" for p in t.pipelines}
    expected_sink_pending = {
        f"pipeline_{p.name}_sink_pending" for p in t.pipelines if p.sinks
    }
    expected_stage = {f"stage_{s.name}" for s in t.stages}
    assert expected_pipe.issubset(names)
    assert expected_sink_pending.issubset(names)
    assert expected_stage.issubset(names)


@pytest.mark.asyncio
async def test_migrate_idempotent(tmp_path):
    db_path = tmp_path / "active.db"
    t = load_task(FIXTURE)
    async with db.open_active_db(str(db_path)) as conn:
        await migrate_task_dynamic_tables(conn, t)
        await migrate_task_dynamic_tables(conn, t)  # second time must not raise


@pytest.mark.asyncio
async def test_pipeline_status_check_constraint(tmp_path):
    """Per-pipeline status must be one of {success, dropped, failed}."""
    import aiosqlite as _asq
    db_path = tmp_path / "active.db"
    async with db.open_active_db(str(db_path)) as conn:
        await ensure_pipeline_table(conn, "p")
        # need a pipelines row first for the FK
        cur = await conn.execute(
            "INSERT INTO pipelines (run_key, task, pipeline_name, cycle_anchor, "
            "trigger, started_at, status) VALUES ('rk', 't', 'p', 'c', 'cron', 'n', 'success')"
        )
        rid = cur.lastrowid
        with pytest.raises(_asq.IntegrityError):
            await conn.execute(
                "INSERT INTO pipeline_p (item_id, last_run_id, status, processed_at) "
                "VALUES ('x', ?, 'invalid_value', 'n')",
                (rid,),
            )


@pytest.mark.asyncio
async def test_stage_status_check_constraint(tmp_path):
    """Per-stage status must be one of {done, failed, dropped, skipped}."""
    import aiosqlite as _asq
    db_path = tmp_path / "active.db"
    async with db.open_active_db(str(db_path)) as conn:
        await ensure_stage_table(conn, "s")
        cur = await conn.execute(
            "INSERT INTO pipelines (run_key, task, pipeline_name, cycle_anchor, "
            "trigger, started_at, status) VALUES ('rk', 't', 'p', 'c', 'cron', 'n', 'success')"
        )
        prid = cur.lastrowid
        cur = await conn.execute(
            "INSERT INTO stages (pipeline_run_id, stage_name, started_at, status) "
            "VALUES (?, 's', 'n', 'success')",
            (prid,),
        )
        srid = cur.lastrowid
        with pytest.raises(_asq.IntegrityError):
            await conn.execute(
                "INSERT INTO stage_s (pipeline_name, item_id, status, last_stage_run_id, "
                "last_pipeline_run_id, last_processed_at) "
                "VALUES ('p', 'x', 'bogus', ?, ?, 'n')",
                (srid, prid),
            )
```

- [ ] **Step 2: Run tests — verify failure**

Run: `pytest tests/dag/test_dynamic_tables.py -v`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement `dynamic_tables.py`**

Write `sluice/dag/dynamic_tables.py`:
```python
from __future__ import annotations

import re

import aiosqlite

from sluice.dag.config import TaskConfig

_NAME_RE = re.compile(r"^[a-zA-Z0-9_]+$")


def _safe(name: str) -> str:
    if not _NAME_RE.match(name):
        raise ValueError(f"unsafe identifier: {name!r}")
    return name


async def ensure_pipeline_table(conn: aiosqlite.Connection, pipeline_name: str) -> None:
    n = _safe(pipeline_name)
    sql = f"""
    CREATE TABLE IF NOT EXISTS pipeline_{n} (
      item_id      TEXT PRIMARY KEY,
      last_run_id  INTEGER NOT NULL REFERENCES pipelines(id),
      status       TEXT NOT NULL CHECK (status IN ('success', 'dropped', 'failed')),
      processed_at TEXT NOT NULL,
      error_msg    TEXT
    )
    """
    await conn.execute(sql)
    await conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_pipeline_{n}_status ON pipeline_{n}(status)"
    )


async def ensure_pipeline_sink_pending_table(
    conn: aiosqlite.Connection, pipeline_name: str
) -> None:
    n = _safe(pipeline_name)
    sql = f"""
    CREATE TABLE IF NOT EXISTS pipeline_{n}_sink_pending (
      item_id       TEXT PRIMARY KEY,
      run_key       TEXT NOT NULL,
      pending_since TEXT NOT NULL
    )
    """
    await conn.execute(sql)
    await conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_pipeline_{n}_sink_pending_runkey "
        f"ON pipeline_{n}_sink_pending(run_key)"
    )


async def ensure_stage_table(conn: aiosqlite.Connection, stage_name: str) -> None:
    n = _safe(stage_name)
    sql = f"""
    CREATE TABLE IF NOT EXISTS stage_{n} (
      pipeline_name        TEXT NOT NULL,
      item_id              TEXT NOT NULL,
      status               TEXT NOT NULL CHECK (status IN ('done', 'failed', 'dropped', 'skipped')),
      attempts             INTEGER NOT NULL DEFAULT 0,
      last_stage_run_id    INTEGER NOT NULL REFERENCES stages(id),
      last_pipeline_run_id INTEGER NOT NULL REFERENCES pipelines(id),
      last_processed_at    TEXT NOT NULL,
      last_error_class     TEXT,
      last_error_msg       TEXT,
      PRIMARY KEY (pipeline_name, item_id)
    )
    """
    await conn.execute(sql)
    await conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_stage_{n}_item ON stage_{n}(item_id)"
    )
    await conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_stage_{n}_run ON stage_{n}(last_pipeline_run_id)"
    )
    await conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_stage_{n}_status ON stage_{n}(status)"
    )


async def migrate_task_dynamic_tables(
    conn: aiosqlite.Connection, task: TaskConfig
) -> None:
    for p in task.pipelines:
        await ensure_pipeline_table(conn, p.name)
        if p.sinks:
            await ensure_pipeline_sink_pending_table(conn, p.name)
    for s in task.stages:
        await ensure_stage_table(conn, s.name)
    await conn.commit()
```

- [ ] **Step 4: Run tests — verify pass**

Run: `pytest tests/dag/test_dynamic_tables.py -v`
Expected: PASS (7 tests)

- [ ] **Step 5: Commit**

```bash
git add sluice/dag/dynamic_tables.py tests/dag/test_dynamic_tables.py
git commit -m "feat(dag): dynamic per-stage / per-pipeline / sink-pending tables"
```

---

## Task 11: ItemsStore

**Files:**
- Create: `sluice/dag/stores/__init__.py`
- Create: `sluice/dag/stores/items.py`
- Test: `tests/dag/test_stores.py`

- [ ] **Step 1: Write failing test**

Write `sluice/dag/stores/__init__.py`:
```python
```

Write `tests/dag/test_stores.py`:
```python
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from sluice.dag import db
from sluice.dag.stores.items import ItemsStore


@pytest.mark.asyncio
async def test_insert_and_lookup(tmp_path):
    p = tmp_path / "active.db"
    async with db.open_active_db(str(p)) as conn:
        store = ItemsStore(conn)
        now = datetime(2026, 5, 3, 10, 0, tzinfo=timezone.utc).isoformat()
        await store.insert_new(
            item_id="abc",
            source_name="src",
            url="https://x",
            title="T",
            published_at="2026-05-01T00:00:00+00:00",
            raw_json='{"a":1}',
            ingested_at=now,
            ingested_by="entry",
        )
        await conn.commit()
        rec = await store.get("abc")
    assert rec is not None
    assert rec["url"] == "https://x"
    assert rec["dropped"] == 0


@pytest.mark.asyncio
async def test_insert_or_ignore_duplicate(tmp_path):
    p = tmp_path / "active.db"
    async with db.open_active_db(str(p)) as conn:
        store = ItemsStore(conn)
        now = "2026-05-03T00:00:00+00:00"
        await store.insert_new(
            item_id="abc", source_name="s", url="u", title="t",
            published_at=None, raw_json="{}", ingested_at=now, ingested_by="e",
        )
        await store.insert_new(
            item_id="abc", source_name="s", url="u", title="other",
            published_at=None, raw_json="{}", ingested_at=now, ingested_by="e",
        )
        await conn.commit()
        rec = await store.get("abc")
    assert rec["title"] == "t"  # second insert ignored


@pytest.mark.asyncio
async def test_update_columns(tmp_path):
    p = tmp_path / "active.db"
    async with db.open_active_db(str(p)) as conn:
        store = ItemsStore(conn)
        now = "2026-05-03T00:00:00+00:00"
        await store.insert_new(
            item_id="abc", source_name="s", url="u", title="t",
            published_at=None, raw_json="{}", ingested_at=now, ingested_by="e",
        )
        await store.update_columns("abc", {"summary": "S", "score": 0.9}, updated_at=now)
        await conn.commit()
        rec = await store.get("abc")
    assert rec["summary"] == "S"
    assert rec["score"] == 0.9


@pytest.mark.asyncio
async def test_mark_dropped(tmp_path):
    p = tmp_path / "active.db"
    async with db.open_active_db(str(p)) as conn:
        store = ItemsStore(conn)
        now = "2026-05-03T00:00:00+00:00"
        await store.insert_new(
            item_id="abc", source_name="s", url="u", title="t",
            published_at=None, raw_json="{}", ingested_at=now, ingested_by="e",
        )
        await store.mark_dropped("abc", reason="manual_kill", at=now)
        await conn.commit()
        rec = await store.get("abc")
    assert rec["dropped"] == 1
    assert rec["dropped_reason"] == "manual_kill"


@pytest.mark.asyncio
async def test_exists_check(tmp_path):
    p = tmp_path / "active.db"
    async with db.open_active_db(str(p)) as conn:
        store = ItemsStore(conn)
        assert await store.exists("nope") is False
        now = "2026-05-03T00:00:00+00:00"
        await store.insert_new(
            item_id="abc", source_name="s", url="u", title="t",
            published_at=None, raw_json="{}", ingested_at=now, ingested_by="e",
        )
        assert await store.exists("abc") is True
```

- [ ] **Step 2: Run tests — verify failure**

Run: `pytest tests/dag/test_stores.py -v`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement `items.py`**

Write `sluice/dag/stores/items.py`:
```python
from __future__ import annotations

from typing import Any

import aiosqlite

# Closed-set columns whitelist for update_columns()
_UPDATABLE = frozenset({
    "title", "author", "tag",
    "raw_summary", "raw_content",
    "fulltext", "attachments_json",
    "summary", "score", "tags_json", "extras_json",
})


class ItemsStore:
    def __init__(self, conn: aiosqlite.Connection):
        self.conn = conn

    async def insert_new(
        self,
        *,
        item_id: str,
        source_name: str,
        url: str | None,
        title: str | None,
        published_at: str | None,
        raw_json: str,
        ingested_at: str,
        ingested_by: str,
        guid: str | None = None,
        author: str | None = None,
        tag: str | None = None,
        raw_summary: str | None = None,
        raw_content: str | None = None,
    ) -> None:
        await self.conn.execute(
            """
            INSERT OR IGNORE INTO items (
              item_id, source_name, guid, url, title, author,
              published_at, ingested_at, ingested_by, updated_at,
              tag, raw_summary, raw_content, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (item_id, source_name, guid, url, title, author,
             published_at, ingested_at, ingested_by, ingested_at,
             tag, raw_summary, raw_content, raw_json),
        )

    async def update_columns(
        self, item_id: str, cols: dict[str, Any], *, updated_at: str
    ) -> None:
        if not cols:
            return
        bad = set(cols) - _UPDATABLE
        if bad:
            raise ValueError(f"non-updatable columns: {bad}")
        sets = ", ".join(f"{c} = ?" for c in cols)
        params: list[Any] = list(cols.values()) + [updated_at, item_id]
        await self.conn.execute(
            f"UPDATE items SET {sets}, updated_at = ? WHERE item_id = ?",
            params,
        )

    async def mark_dropped(self, item_id: str, *, reason: str, at: str) -> None:
        await self.conn.execute(
            "UPDATE items SET dropped = 1, dropped_reason = ?, dropped_at = ?, updated_at = ? "
            "WHERE item_id = ?",
            (reason, at, at, item_id),
        )

    async def exists(self, item_id: str) -> bool:
        cur = await self.conn.execute(
            "SELECT 1 FROM items WHERE item_id = ?", (item_id,)
        )
        return (await cur.fetchone()) is not None

    async def get(self, item_id: str) -> dict[str, Any] | None:
        cur = await self.conn.execute(
            "SELECT * FROM items WHERE item_id = ?", (item_id,)
        )
        row = await cur.fetchone()
        if row is None:
            return None
        cols = [d[0] for d in cur.description]
        return dict(zip(cols, row))
```

- [ ] **Step 4: Run tests — verify pass**

Run: `pytest tests/dag/test_stores.py -v`
Expected: PASS (5 tests)

- [ ] **Step 5: Commit**

```bash
git add sluice/dag/stores/__init__.py sluice/dag/stores/items.py tests/dag/test_stores.py
git commit -m "feat(dag): ItemsStore — insert/update/drop/exists"
```

---

## Task 12: PipelinesStore (run UPSERT)

**Files:**
- Create: `sluice/dag/stores/pipelines.py`
- Modify: `tests/dag/test_stores.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/dag/test_stores.py`:
```python
from sluice.dag.stores.pipelines import PipelinesStore


@pytest.mark.asyncio
async def test_pipelines_insert_running(tmp_path):
    p = tmp_path / "active.db"
    async with db.open_active_db(str(p)) as conn:
        store = PipelinesStore(conn)
        run_id = await store.upsert_running(
            run_key="example__report__2026-05-03T08:00",
            task="example", pipeline_name="report",
            cycle_anchor="2026-05-03T08:00:00+00:00",
            trigger="cron", started_at="2026-05-03T08:01:00+00:00",
        )
        assert isinstance(run_id, int) and run_id > 0
        await conn.commit()
        cur = await conn.execute("SELECT status FROM pipelines WHERE id = ?", (run_id,))
        assert (await cur.fetchone())[0] == "running"


@pytest.mark.asyncio
async def test_pipelines_upsert_resets_on_retry(tmp_path):
    p = tmp_path / "active.db"
    async with db.open_active_db(str(p)) as conn:
        store = PipelinesStore(conn)
        rk = "rk1"
        run_id = await store.upsert_running(
            run_key=rk, task="t", pipeline_name="p", cycle_anchor="c",
            trigger="cron", started_at="2026-05-03T08:00:00+00:00",
        )
        await store.finalize(
            run_id, finished_at="2026-05-03T08:05:00+00:00",
            status="failed", items_in=10, items_out=0, error_msg="boom",
        )
        await conn.commit()
        run_id_again = await store.upsert_running(
            run_key=rk, task="t", pipeline_name="p", cycle_anchor="c",
            trigger="cron", started_at="2026-05-03T08:30:00+00:00",
        )
        await conn.commit()
        assert run_id_again == run_id
        cur = await conn.execute(
            "SELECT status, finished_at, error_msg FROM pipelines WHERE id = ?", (run_id,)
        )
        st, fin, err = await cur.fetchone()
        assert st == "running"
        assert fin is None
        assert err is None


@pytest.mark.asyncio
async def test_pipelines_finalize(tmp_path):
    p = tmp_path / "active.db"
    async with db.open_active_db(str(p)) as conn:
        store = PipelinesStore(conn)
        run_id = await store.upsert_running(
            run_key="rk2", task="t", pipeline_name="p", cycle_anchor="c",
            trigger="cron", started_at="2026-05-03T08:00:00+00:00",
        )
        await store.finalize(
            run_id, finished_at="2026-05-03T08:05:00+00:00",
            status="success", items_in=10, items_out=8, error_msg=None,
        )
        await conn.commit()
        cur = await conn.execute(
            "SELECT status, items_out FROM pipelines WHERE id = ?", (run_id,)
        )
        st, out = await cur.fetchone()
        assert st == "success" and out == 8
```

- [ ] **Step 2: Run tests — verify failure**

Run: `pytest tests/dag/test_stores.py -v`
Expected: FAIL — module not found (PipelinesStore).

- [ ] **Step 3: Implement `pipelines.py`**

Write `sluice/dag/stores/pipelines.py`:
```python
from __future__ import annotations

from typing import Any

import aiosqlite


class PipelinesStore:
    def __init__(self, conn: aiosqlite.Connection):
        self.conn = conn

    async def get_run_id(self, run_key: str) -> int | None:
        cur = await self.conn.execute(
            "SELECT id FROM pipelines WHERE run_key = ?", (run_key,)
        )
        row = await cur.fetchone()
        return None if row is None else int(row[0])

    async def upsert_running(
        self, *,
        run_key: str, task: str, pipeline_name: str,
        cycle_anchor: str, trigger: str, started_at: str,
    ) -> int:
        existing = await self.get_run_id(run_key)
        if existing is None:
            cur = await self.conn.execute(
                """
                INSERT INTO pipelines (
                  run_key, task, pipeline_name, cycle_anchor,
                  trigger, started_at, status
                ) VALUES (?, ?, ?, ?, ?, ?, 'running')
                """,
                (run_key, task, pipeline_name, cycle_anchor, trigger, started_at),
            )
            return cur.lastrowid  # type: ignore[return-value]
        await self.conn.execute(
            """
            UPDATE pipelines SET
              status = 'running',
              started_at = ?,
              finished_at = NULL,
              items_in = NULL,
              items_out = NULL,
              error_msg = NULL
            WHERE id = ?
            """,
            (started_at, existing),
        )
        return existing

    async def finalize(
        self, run_id: int, *,
        finished_at: str, status: str,
        items_in: int | None, items_out: int | None, error_msg: str | None,
    ) -> None:
        await self.conn.execute(
            """
            UPDATE pipelines SET
              finished_at = ?, status = ?, items_in = ?, items_out = ?, error_msg = ?
            WHERE id = ?
            """,
            (finished_at, status, items_in, items_out, error_msg, run_id),
        )
```

- [ ] **Step 4: Run tests — verify pass**

Run: `pytest tests/dag/test_stores.py -v`
Expected: PASS (8 tests total).

- [ ] **Step 5: Commit**

```bash
git add sluice/dag/stores/pipelines.py tests/dag/test_stores.py
git commit -m "feat(dag): PipelinesStore — upsert running / finalize"
```

---

## Task 13: StagesStore

**Files:**
- Create: `sluice/dag/stores/stages.py`
- Modify: `tests/dag/test_stores.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/dag/test_stores.py`:
```python
from sluice.dag.stores.stages import StagesStore


@pytest.mark.asyncio
async def test_stages_insert_run(tmp_path):
    p = tmp_path / "active.db"
    async with db.open_active_db(str(p)) as conn:
        ps = PipelinesStore(conn)
        ss = StagesStore(conn)
        run_id = await ps.upsert_running(
            run_key="rk", task="t", pipeline_name="p",
            cycle_anchor="c", trigger="cron",
            started_at="2026-05-03T00:00:00+00:00",
        )
        sid = await ss.start(
            pipeline_run_id=run_id, stage_name="dd",
            started_at="2026-05-03T00:01:00+00:00",
        )
        await ss.finalize(
            sid, finished_at="2026-05-03T00:02:00+00:00",
            status="success", items_in=10, items_out=9, error_msg=None,
        )
        await conn.commit()
        cur = await conn.execute(
            "SELECT status, items_out FROM stages WHERE id = ?", (sid,)
        )
        st, n = await cur.fetchone()
        assert st == "success" and n == 9
```

- [ ] **Step 2: Run tests — verify failure**

Run: `pytest tests/dag/test_stores.py -v`
Expected: FAIL — StagesStore missing.

- [ ] **Step 3: Implement `stages.py`**

Write `sluice/dag/stores/stages.py`:
```python
from __future__ import annotations

import aiosqlite


class StagesStore:
    def __init__(self, conn: aiosqlite.Connection):
        self.conn = conn

    async def start(
        self, *,
        pipeline_run_id: int, stage_name: str, started_at: str,
    ) -> int:
        cur = await self.conn.execute(
            """
            INSERT INTO stages (
              pipeline_run_id, stage_name, started_at, status
            ) VALUES (?, ?, ?, 'running')
            """,
            (pipeline_run_id, stage_name, started_at),
        )
        return cur.lastrowid  # type: ignore[return-value]

    async def finalize(
        self, stage_run_id: int, *,
        finished_at: str, status: str,
        items_in: int | None, items_out: int | None, error_msg: str | None,
    ) -> None:
        await self.conn.execute(
            """
            UPDATE stages SET
              finished_at = ?, status = ?, items_in = ?, items_out = ?, error_msg = ?
            WHERE id = ?
            """,
            (finished_at, status, items_in, items_out, error_msg, stage_run_id),
        )
```

- [ ] **Step 4: Run tests — verify pass**

Run: `pytest tests/dag/test_stores.py -v`
Expected: PASS (9 tests).

- [ ] **Step 5: Commit**

```bash
git add sluice/dag/stores/stages.py tests/dag/test_stores.py
git commit -m "feat(dag): StagesStore — start / finalize"
```

---

## Task 14: ItemSeenStore

**Files:**
- Create: `sluice/dag/stores/item_seen.py`
- Modify: `tests/dag/test_stores.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/dag/test_stores.py`:
```python
from sluice.dag.stores.item_seen import ItemSeenStore


@pytest.mark.asyncio
async def test_item_seen_insert_and_filter_seen(tmp_path):
    p = tmp_path / "seen.db"
    async with db.open_seen_db(str(p)) as conn:
        store = ItemSeenStore(conn)
        await store.insert_many([
            ("a", "url-a"),
            ("b", "url-b"),
        ], archived_at="2026-05-03T00:00:00+00:00")
        await conn.commit()
        seen = await store.filter_seen({"a", "b", "c"})
    assert seen == {"a", "b"}


@pytest.mark.asyncio
async def test_item_seen_insert_or_ignore_duplicates(tmp_path):
    p = tmp_path / "seen.db"
    async with db.open_seen_db(str(p)) as conn:
        store = ItemSeenStore(conn)
        await store.insert_many([("a", "url-a")], archived_at="t1")
        await store.insert_many([("a", "url-a")], archived_at="t2")
        await conn.commit()
        cur = await conn.execute("SELECT archived_at FROM item_seen WHERE item_id='a'")
        assert (await cur.fetchone())[0] == "t1"
```

- [ ] **Step 2: Run tests — verify failure**

Run: `pytest tests/dag/test_stores.py -v`
Expected: FAIL — module missing.

- [ ] **Step 3: Implement `item_seen.py`**

Write `sluice/dag/stores/item_seen.py`:
```python
from __future__ import annotations

from collections.abc import Iterable

import aiosqlite


class ItemSeenStore:
    def __init__(self, conn: aiosqlite.Connection):
        self.conn = conn

    async def insert_many(
        self, items: Iterable[tuple[str, str]], *, archived_at: str
    ) -> None:
        await self.conn.executemany(
            "INSERT OR IGNORE INTO item_seen (item_id, source_hash, archived_at) "
            "VALUES (?, ?, ?)",
            [(iid, h, archived_at) for iid, h in items],
        )

    async def filter_seen(self, item_ids: set[str]) -> set[str]:
        if not item_ids:
            return set()
        placeholders = ",".join("?" * len(item_ids))
        cur = await self.conn.execute(
            f"SELECT item_id FROM item_seen WHERE item_id IN ({placeholders})",
            tuple(item_ids),
        )
        return {r[0] for r in await cur.fetchall()}
```

- [ ] **Step 4: Run tests — verify pass**

Run: `pytest tests/dag/test_stores.py -v`
Expected: PASS (11 tests).

- [ ] **Step 5: Commit**

```bash
git add sluice/dag/stores/item_seen.py tests/dag/test_stores.py
git commit -m "feat(dag): ItemSeenStore — insert / filter seen"
```

---

## Task 15: WriterThread + WriteJob

**Files:**
- Create: `sluice/dag/writer.py`
- Test: `tests/dag/test_writer.py`

The writer thread owns all DB writes for active and seen DBs. It receives `WriteJob` instances via an `asyncio.Queue` and processes them sequentially. **The writer is responsible for transaction lifecycle on each connection independently** — there is no cross-DB atomic commit, since active and seen are separate SQLite connections.

- **active** gets `BEGIN IMMEDIATE` before the job, `COMMIT` on success, `ROLLBACK` on exception. This guarantees write-then-raise leaks nothing into a subsequent job's active transaction.
- **seen** is committed only after active commits (success path); rolled back on exception. The narrow failure window — active commit succeeded, seen commit failed — is rare and recoverable: dedup still works via the active items table, and rotation's reconcile step (Plan 4) re-syncs seen.

Jobs MUST NOT call `commit()` themselves on either connection.

- [ ] **Step 1: Write the failing test**

Write `tests/dag/test_writer.py`:
```python
from __future__ import annotations

from pathlib import Path
import asyncio

import pytest

from sluice.dag.writer import WriteJob, WriterThread


@pytest.mark.asyncio
async def test_writer_processes_jobs_in_order(tmp_path):
    active = tmp_path / "active.db"
    seen = tmp_path / "seen.db"
    writer = WriterThread(active_path=str(active), seen_path=str(seen))
    await writer.start()
    try:
        order = []

        async def job_1(active_conn, seen_conn):
            order.append(1)
            await active_conn.execute("CREATE TABLE t (n INTEGER)")
            await active_conn.execute("INSERT INTO t (n) VALUES (1)")

        async def job_2(active_conn, seen_conn):
            order.append(2)
            await active_conn.execute("INSERT INTO t (n) VALUES (2)")

        f1 = await writer.submit(WriteJob(run=job_1))
        f2 = await writer.submit(WriteJob(run=job_2))
        await f1
        await f2
        assert order == [1, 2]
    finally:
        await writer.stop()


@pytest.mark.asyncio
async def test_writer_propagates_errors(tmp_path):
    active = tmp_path / "active.db"
    seen = tmp_path / "seen.db"
    writer = WriterThread(active_path=str(active), seen_path=str(seen))
    await writer.start()
    try:
        async def bad(active_conn, seen_conn):
            raise RuntimeError("nope")

        f = await writer.submit(WriteJob(run=bad))
        with pytest.raises(RuntimeError, match="nope"):
            await f
    finally:
        await writer.stop()


@pytest.mark.asyncio
async def test_writer_rolls_back_on_exception(tmp_path):
    """A job that writes then raises must leave NO trace; subsequent jobs see clean DB."""
    active = tmp_path / "active.db"
    seen = tmp_path / "seen.db"
    writer = WriterThread(active_path=str(active), seen_path=str(seen))
    await writer.start()
    try:
        async def setup(active_conn, seen_conn):
            await active_conn.execute("CREATE TABLE t (n INTEGER)")

        await (await writer.submit(WriteJob(run=setup)))

        async def write_then_raise(active_conn, seen_conn):
            await active_conn.execute("INSERT INTO t (n) VALUES (999)")
            raise RuntimeError("rollback me")

        f = await writer.submit(WriteJob(run=write_then_raise))
        with pytest.raises(RuntimeError, match="rollback me"):
            await f

        async def assert_clean(active_conn, seen_conn):
            cur = await active_conn.execute("SELECT count(*) FROM t WHERE n=999")
            assert (await cur.fetchone())[0] == 0

        await (await writer.submit(WriteJob(run=assert_clean)))
    finally:
        await writer.stop()


@pytest.mark.asyncio
async def test_writer_concurrent_submitters(tmp_path):
    active = tmp_path / "active.db"
    seen = tmp_path / "seen.db"
    writer = WriterThread(active_path=str(active), seen_path=str(seen))
    await writer.start()
    try:
        async def setup(active_conn, seen_conn):
            await active_conn.execute("CREATE TABLE t (n INTEGER)")

        await (await writer.submit(WriteJob(run=setup)))

        async def insert(i):
            async def job(active_conn, seen_conn):
                await active_conn.execute("INSERT INTO t (n) VALUES (?)", (i,))
            return await (await writer.submit(WriteJob(run=job)))

        await asyncio.gather(*(insert(i) for i in range(20)))
    finally:
        await writer.stop()
```

- [ ] **Step 2: Run tests — verify failure**

Run: `pytest tests/dag/test_writer.py -v`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement `writer.py`**

Write `sluice/dag/writer.py`:
```python
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Awaitable, Callable

import aiosqlite

from sluice.dag.db import open_active_db, open_seen_db


WriteJobCallable = Callable[[aiosqlite.Connection, aiosqlite.Connection], Awaitable[None]]


@dataclass
class WriteJob:
    run: WriteJobCallable


class WriterThread:
    """Single-writer thread over active+seen DBs.

    submit(job) -> Future that resolves when job has been applied (or rejects on error).
    Jobs run sequentially in submission order.
    """

    def __init__(self, *, active_path: str, seen_path: str):
        self.active_path = active_path
        self.seen_path = seen_path
        self._queue: asyncio.Queue[tuple[WriteJob, asyncio.Future[None]] | None] = asyncio.Queue()
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="dag-writer")

    async def stop(self) -> None:
        await self._queue.put(None)
        if self._task is not None:
            await self._task
            self._task = None

    async def submit(self, job: WriteJob) -> asyncio.Future[None]:
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[None] = loop.create_future()
        await self._queue.put((job, fut))
        return fut

    async def _run(self) -> None:
        # NOTE: active and seen are TWO independent SQLite connections. We do
        # NOT have cross-DB atomic commit. Per-connection transaction discipline
        # is best-effort:
        #   - active gets BEGIN IMMEDIATE on every job, COMMIT on success,
        #     ROLLBACK on exception. This guarantees write-then-raise leaves
        #     active untouched.
        #   - seen has its own implicit transaction. We commit only after
        #     active commits; we roll back on exception. There is a narrow
        #     window where active.commit() succeeds and seen.commit() fails;
        #     in that case active is durable but seen is not, and rotation's
        #     reconcile step (Plan 4) is the safety net that re-syncs them.
        async with open_active_db(self.active_path) as active, \
                   open_seen_db(self.seen_path) as seen:
            while True:
                item = await self._queue.get()
                if item is None:
                    return
                job, fut = item
                await active.execute("BEGIN IMMEDIATE")
                try:
                    await job.run(active, seen)
                    await active.commit()
                    try:
                        await seen.commit()
                    except Exception:
                        # active already durable; log + carry on. Reconcile at rotation.
                        pass
                    if not fut.done():
                        fut.set_result(None)
                except Exception as e:  # noqa: BLE001 — propagate to submitter
                    try:
                        await active.rollback()
                    except Exception:
                        pass
                    try:
                        await seen.rollback()
                    except Exception:
                        pass
                    if not fut.done():
                        fut.set_exception(e)
```

- [ ] **Step 4: Run tests — verify pass**

Run: `pytest tests/dag/test_writer.py -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add sluice/dag/writer.py tests/dag/test_writer.py
git commit -m "feat(dag): writer thread + WriteJob queue"
```

---

## Task 16: Run-key generator + cycle anchor

**Files:**
- Create: `sluice/dag/cycle.py`
- Test: `tests/dag/test_cycle.py`

`compute_cycle_anchor()` returns the most-recent cron-anchor occurrence at-or-before `now` for a given anchor cron and timezone. `make_run_key()` formats `<task>__<pipeline>__<cycle_anchor_iso>`.

- [ ] **Step 1: Write the failing test**

Write `tests/dag/test_cycle.py`:
```python
from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pytest

from sluice.dag.cycle import compute_cycle_anchor, make_run_key


def test_compute_anchor_daily_8am_utc():
    now = datetime(2026, 5, 3, 14, 0, tzinfo=timezone.utc)
    anchor = compute_cycle_anchor("0 8 * * *", "UTC", now)
    assert anchor == datetime(2026, 5, 3, 8, 0, tzinfo=timezone.utc)


def test_compute_anchor_before_today_anchor():
    now = datetime(2026, 5, 3, 7, 0, tzinfo=timezone.utc)
    anchor = compute_cycle_anchor("0 8 * * *", "UTC", now)
    assert anchor == datetime(2026, 5, 2, 8, 0, tzinfo=timezone.utc)


def test_compute_anchor_with_tz():
    now = datetime(2026, 5, 3, 0, 0, tzinfo=timezone.utc)
    # 8 AM Asia/Shanghai is 0 AM UTC
    anchor = compute_cycle_anchor("0 8 * * *", "Asia/Shanghai", now)
    assert anchor == datetime(2026, 5, 3, 8, 0, tzinfo=ZoneInfo("Asia/Shanghai"))


def test_compute_anchor_at_exact_anchor_returns_now():
    """When now == anchor, return now (at-or-before, not strictly before)."""
    now = datetime(2026, 5, 3, 8, 0, tzinfo=timezone.utc)
    anchor = compute_cycle_anchor("0 8 * * *", "UTC", now)
    assert anchor == datetime(2026, 5, 3, 8, 0, tzinfo=timezone.utc)


def test_compute_anchor_one_second_after_anchor():
    now = datetime(2026, 5, 3, 8, 0, 1, tzinfo=timezone.utc)
    anchor = compute_cycle_anchor("0 8 * * *", "UTC", now)
    assert anchor == datetime(2026, 5, 3, 8, 0, tzinfo=timezone.utc)


def test_make_run_key_format():
    anchor = datetime(2026, 5, 3, 8, 0, tzinfo=timezone.utc)
    rk = make_run_key("airflow", "report_telegram", anchor)
    assert rk == "airflow__report_telegram__2026-05-03T08:00:00+00:00"
```

- [ ] **Step 2: Run tests — verify failure**

Run: `pytest tests/dag/test_cycle.py -v`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement `cycle.py`**

```bash
rtk grep "croniter" pyproject.toml
```

If not present, add to dependencies. Then write `sluice/dag/cycle.py`:
```python
from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from croniter import croniter


def compute_cycle_anchor(cron: str, tz: str, now: datetime) -> datetime:
    """Return the most-recent occurrence of cron <= now (at-or-before) in timezone tz.

    Only treats ``now`` as an exact anchor when its second/microsecond fields are zero,
    since 5-field cron expressions have minute granularity. ``08:00:01`` is NOT an
    anchor for ``0 8 * * *`` and falls through to the previous occurrence.
    """
    zone = ZoneInfo(tz)
    local_now = now.astimezone(zone)
    if (
        local_now.second == 0
        and local_now.microsecond == 0
        and croniter.match(cron, local_now)
    ):
        return local_now
    it = croniter(cron, local_now)
    prev = it.get_prev(datetime)
    if prev.tzinfo is None:
        prev = prev.replace(tzinfo=zone)
    return prev


def make_run_key(task: str, pipeline: str, cycle_anchor: datetime) -> str:
    return f"{task}__{pipeline}__{cycle_anchor.isoformat()}"
```

- [ ] **Step 4: Run tests — verify pass**

Run: `pytest tests/dag/test_cycle.py -v`
Expected: PASS (6 tests).

- [ ] **Step 5: Commit**

```bash
git add sluice/dag/cycle.py tests/dag/test_cycle.py
git commit -m "feat(dag): compute_cycle_anchor + make_run_key"
```

---

## Task 17: Runner stub — no-op pipeline

**Files:**
- Create: `sluice/dag/runner.py`
- Test: `tests/dag/test_runner_stub.py`

The runner stub:
1. Loads task config + validates.
2. Computes cycle anchor + run_key.
3. Starts WriterThread.
4. Submits tx1 stub (UPSERT pipelines `running`, no items, no stage rows).
5. Submits tx2 stub (UPDATE pipelines `success`, items_in=0, items_out=0).
6. Stops writer.

It does NOT execute any stage logic yet — those come in Plan 2. But the run completes and produces a `pipelines` row.

- [ ] **Step 1: Write failing test**

Write `tests/dag/test_runner_stub.py`:
```python
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from sluice.dag.loader import load_task
from sluice.dag.runner import run_pipeline_stub
from sluice.dag import db

FIXTURE = Path(__file__).parent / "fixtures" / "example_task"


@pytest.mark.asyncio
async def test_runner_stub_records_pipeline_run(tmp_path):
    import shutil
    d = tmp_path / "task"
    shutil.copytree(FIXTURE, d)
    # rewrite db paths to live under tmp_path
    text = (d / "task.toml").read_text()
    text = text.replace('db_path  = "data/example.db"', f'db_path  = "{tmp_path}/active.db"')
    text = text.replace('seen_db  = "data/example_seen.db"', f'seen_db  = "{tmp_path}/seen.db"')
    (d / "task.toml").write_text(text)
    t = load_task(d)
    now = datetime(2026, 5, 3, 14, 0, tzinfo=timezone.utc)

    result = await run_pipeline_stub(t, pipeline_name="fetch_blog", now=now)
    assert result.status == "success"
    assert result.items_in == 0

    async with db.open_active_db(t.db_path) as conn:
        cur = await conn.execute(
            "SELECT pipeline_name, status, items_in FROM pipelines WHERE pipeline_name='fetch_blog'"
        )
        row = await cur.fetchone()
    assert row[0] == "fetch_blog"
    assert row[1] == "success"
    assert row[2] == 0


@pytest.mark.asyncio
async def test_runner_stub_idempotent_within_cycle(tmp_path):
    import shutil
    d = tmp_path / "task"
    shutil.copytree(FIXTURE, d)
    text = (d / "task.toml").read_text()
    text = text.replace('db_path  = "data/example.db"', f'db_path  = "{tmp_path}/active.db"')
    text = text.replace('seen_db  = "data/example_seen.db"', f'seen_db  = "{tmp_path}/seen.db"')
    (d / "task.toml").write_text(text)
    t = load_task(d)
    now = datetime(2026, 5, 3, 14, 0, tzinfo=timezone.utc)

    await run_pipeline_stub(t, pipeline_name="fetch_blog", now=now)
    await run_pipeline_stub(t, pipeline_name="fetch_blog", now=now)

    async with db.open_active_db(t.db_path) as conn:
        cur = await conn.execute(
            "SELECT count(*) FROM pipelines WHERE pipeline_name='fetch_blog'"
        )
        n = (await cur.fetchone())[0]
    assert n == 1  # same run_key reused, no duplicate row
```

- [ ] **Step 2: Run tests — verify failure**

Run: `pytest tests/dag/test_runner_stub.py -v`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement `runner.py`**

Write `sluice/dag/runner.py`:
```python
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from sluice.dag.config import TaskConfig
from sluice.dag.cycle import compute_cycle_anchor, make_run_key
from sluice.dag.db import open_active_db
from sluice.dag.dynamic_tables import migrate_task_dynamic_tables
from sluice.dag.stores.pipelines import PipelinesStore
from sluice.dag.validator import validate_task
from sluice.dag.writer import WriteJob, WriterThread


@dataclass
class RunResult:
    pipeline_name: str
    run_key: str
    status: str
    items_in: int
    items_out: int


async def _ensure_dynamic_tables(task: TaskConfig) -> None:
    async with open_active_db(task.db_path) as conn:
        await migrate_task_dynamic_tables(conn, task)


async def run_pipeline_stub(
    task: TaskConfig,
    *,
    pipeline_name: str,
    now: datetime | None = None,
    trigger: str = "manual",
) -> RunResult:
    validate_task(task)
    p = next((p for p in task.pipelines if p.name == pipeline_name), None)
    if p is None:
        raise ValueError(f"unknown pipeline: {pipeline_name!r}")

    now = now or datetime.now(timezone.utc)
    anchor = compute_cycle_anchor(
        task.window.anchor, task.timezone or "UTC", now,
    )
    run_key = make_run_key(task.name, pipeline_name, anchor)
    started_at = now.isoformat()

    await _ensure_dynamic_tables(task)

    writer = WriterThread(active_path=task.db_path, seen_path=task.seen_db)
    await writer.start()
    try:
        run_id_holder: dict[str, int] = {}

        async def tx1(active_conn, seen_conn):
            ps = PipelinesStore(active_conn)
            run_id = await ps.upsert_running(
                run_key=run_key, task=task.name, pipeline_name=pipeline_name,
                cycle_anchor=anchor.isoformat(),
                trigger=trigger, started_at=started_at,
            )
            run_id_holder["id"] = run_id
            # Note: writer handles BEGIN/COMMIT/ROLLBACK — do NOT call commit() here.

        await (await writer.submit(WriteJob(run=tx1)))

        async def tx2(active_conn, seen_conn):
            ps = PipelinesStore(active_conn)
            await ps.finalize(
                run_id_holder["id"],
                finished_at=datetime.now(timezone.utc).isoformat(),
                status="success",
                items_in=0, items_out=0, error_msg=None,
            )
            # writer commits

        await (await writer.submit(WriteJob(run=tx2)))

    finally:
        await writer.stop()

    return RunResult(
        pipeline_name=pipeline_name, run_key=run_key,
        status="success", items_in=0, items_out=0,
    )
```

- [ ] **Step 4: Run tests — verify pass**

Run: `pytest tests/dag/test_runner_stub.py -v`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add sluice/dag/runner.py tests/dag/test_runner_stub.py
git commit -m "feat(dag): runner stub — load task, migrate, no-op tx1+tx2"
```

---

## Task 18: End-to-end smoke — multi-pipeline stub

**Files:**
- Modify: `tests/dag/test_runner_stub.py`

Verify all 4 pipelines in the fixture task can run as no-ops without interfering, and that re-runs across cycle boundaries create new rows.

- [ ] **Step 1: Add the test**

Append to `tests/dag/test_runner_stub.py`:
```python
@pytest.mark.asyncio
async def test_runner_stub_all_pipelines_no_op(tmp_path):
    import shutil
    d = tmp_path / "task"
    shutil.copytree(FIXTURE, d)
    text = (d / "task.toml").read_text()
    text = text.replace('db_path  = "data/example.db"', f'db_path  = "{tmp_path}/active.db"')
    text = text.replace('seen_db  = "data/example_seen.db"', f'seen_db  = "{tmp_path}/seen.db"')
    (d / "task.toml").write_text(text)
    t = load_task(d)
    now = datetime(2026, 5, 3, 14, 0, tzinfo=timezone.utc)
    for p in t.pipelines:
        await run_pipeline_stub(t, pipeline_name=p.name, now=now)

    async with db.open_active_db(t.db_path) as conn:
        cur = await conn.execute(
            "SELECT pipeline_name, status FROM pipelines ORDER BY pipeline_name"
        )
        rows = await cur.fetchall()
    assert {r[0] for r in rows} == {p.name for p in t.pipelines}
    assert all(r[1] == "success" for r in rows)


@pytest.mark.asyncio
async def test_runner_stub_new_cycle_new_row(tmp_path):
    import shutil
    d = tmp_path / "task"
    shutil.copytree(FIXTURE, d)
    text = (d / "task.toml").read_text()
    text = text.replace('db_path  = "data/example.db"', f'db_path  = "{tmp_path}/active.db"')
    text = text.replace('seen_db  = "data/example_seen.db"', f'seen_db  = "{tmp_path}/seen.db"')
    (d / "task.toml").write_text(text)
    t = load_task(d)

    day1 = datetime(2026, 5, 3, 14, 0, tzinfo=timezone.utc)
    day2 = datetime(2026, 5, 4, 14, 0, tzinfo=timezone.utc)
    await run_pipeline_stub(t, pipeline_name="fetch_blog", now=day1)
    await run_pipeline_stub(t, pipeline_name="fetch_blog", now=day2)

    async with db.open_active_db(t.db_path) as conn:
        cur = await conn.execute(
            "SELECT count(*) FROM pipelines WHERE pipeline_name='fetch_blog'"
        )
        n = (await cur.fetchone())[0]
    assert n == 2  # different cycle anchors -> different run_keys -> two rows
```

- [ ] **Step 2: Run all tests for the plan**

Run: `pytest tests/dag/ -v`
Expected: ALL PASS.

- [ ] **Step 3: Lint check**

Run: `ruff check sluice/dag tests/dag`
Expected: no errors. Fix any.

- [ ] **Step 4: Commit**

```bash
git add tests/dag/test_runner_stub.py
git commit -m "test(dag): end-to-end stub multi-pipeline + cycle boundary"
```

---

## Task 19: CLI — `sluice run -t <task> -p <pipeline>`

**Files:**
- Modify: `sluice/cli.py` — touch two regions: (a) the existing `@app.command()` block around line 50 that defines `def run(...)` (rename it to `run-legacy` / `def run_legacy(...)`); (b) append the new DAG `run` command at end of file
- Test: `tests/dag/test_cli.py`

The new top-level command discovers the task directory under a configurable `--config-dir` (defaulting to `./configs`), loads the task, and invokes `run_pipeline_stub` (Plan 2 will swap to the real runner). Existing CLI commands stay intact.

- [ ] **Step 1: Inspect current CLI**

```bash
rtk grep "^def \|^app = \|@app" sluice/cli.py
```

- [ ] **Step 2: Write the failing test**

Write `tests/dag/test_cli.py`:
```python
from __future__ import annotations

from pathlib import Path
import shutil

from typer.testing import CliRunner

from sluice.cli import app


FIXTURE = Path(__file__).parent / "fixtures" / "example_task"


def test_run_command_invokes_stub(tmp_path: Path):
    config_dir = tmp_path / "configs"
    (config_dir / "tasks").mkdir(parents=True)
    task_dir = config_dir / "tasks" / "example"
    shutil.copytree(FIXTURE, task_dir)
    text = (task_dir / "task.toml").read_text()
    text = text.replace('db_path  = "data/example.db"', f'db_path  = "{tmp_path}/active.db"')
    text = text.replace('seen_db  = "data/example_seen.db"', f'seen_db  = "{tmp_path}/seen.db"')
    (task_dir / "task.toml").write_text(text)

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["run", "-t", "example", "-p", "fetch_blog", "--config-dir", str(config_dir)],
    )
    assert result.exit_code == 0, result.output
    assert (tmp_path / "active.db").exists()


def test_run_command_unknown_pipeline(tmp_path: Path):
    config_dir = tmp_path / "configs"
    (config_dir / "tasks").mkdir(parents=True)
    task_dir = config_dir / "tasks" / "example"
    shutil.copytree(FIXTURE, task_dir)
    text = (task_dir / "task.toml").read_text()
    text = text.replace('db_path  = "data/example.db"', f'db_path  = "{tmp_path}/active.db"')
    text = text.replace('seen_db  = "data/example_seen.db"', f'seen_db  = "{tmp_path}/seen.db"')
    (task_dir / "task.toml").write_text(text)

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["run", "-t", "example", "-p", "no_such_pipeline", "--config-dir", str(config_dir)],
    )
    assert result.exit_code != 0


def test_run_command_unknown_task(tmp_path: Path):
    config_dir = tmp_path / "configs"
    (config_dir / "tasks").mkdir(parents=True)
    runner = CliRunner()
    result = runner.invoke(
        app,
        ["run", "-t", "nope", "-p", "p", "--config-dir", str(config_dir)],
    )
    assert result.exit_code != 0
```

- [ ] **Step 3: Run tests — verify failure**

Run: `pytest tests/dag/test_cli.py -v`
Expected: FAIL — `run` subcommand not registered (or not accepting `-t`/`-p`).

- [ ] **Step 4a: Rename existing v1 `run` to `run-legacy`**

The current `sluice/cli.py:50` has `@app.command()` followed by `def run(...)`. Edit that block:

```python
@app.command("run-legacy")
def run_legacy(...):
    """[v1, removed in Plan 4] Original sluice run for the legacy pipeline format."""
    # ... body unchanged ...
```

Only the decorator argument and function name change. The body is identical. The v1 command is now invocable as `sluice run-legacy ...`. Plan 4 deletes it entirely.

- [ ] **Step 4b: Add the new DAG `run` command at the end of `sluice/cli.py`**

```python
# ----------------------------------------------------------------------------
# DAG v2 commands (Plan 1: stub-only)
# ----------------------------------------------------------------------------

@app.command("run")
def dag_run(
    task: str = typer.Option(..., "-t", "--task", help="task name under <config-dir>/tasks/"),
    pipeline: str = typer.Option(..., "-p", "--pipeline", help="pipeline name within the task"),
    config_dir: Path = typer.Option(Path("./configs"), "--config-dir", "-c"),
):
    """Run a single pipeline once (stub: no stages executed yet — Plan 2)."""
    import asyncio
    from sluice.dag.loader import load_task
    from sluice.dag.runner import run_pipeline_stub

    task_dir = config_dir / "tasks" / task
    if not task_dir.exists():
        typer.echo(f"task not found: {task_dir}", err=True)
        raise typer.Exit(code=2)

    cfg = load_task(task_dir)
    if not any(p.name == pipeline for p in cfg.pipelines):
        typer.echo(f"pipeline {pipeline!r} not found in task {task!r}", err=True)
        raise typer.Exit(code=2)

    result = asyncio.run(run_pipeline_stub(cfg, pipeline_name=pipeline))
    typer.echo(
        f"{result.pipeline_name} {result.run_key} status={result.status} "
        f"items_in={result.items_in} items_out={result.items_out}"
    )
```

- [ ] **Step 5: Run tests — verify pass**

Run: `pytest tests/dag/test_cli.py -v`
Expected: PASS (3 tests).

- [ ] **Step 6: Commit**

```bash
git add sluice/cli.py tests/dag/test_cli.py
git commit -m "feat(cli): sluice run -t/-p wired to stub runner"
```

---

## Plan 1 Complete

After all 19 tasks:
- `sluice.dag.config` / `loader` / `validator` / `registry` / `cycle` / `db` / `dynamic_tables` / `stores/{items,pipelines,stages,item_seen}` / `writer` / `runner` are in place.
- The 15 stage types are registered as metadata; no actual processor adapters yet (Plan 2).
- A stub pipeline run (no stages executed yet) produces a correctly-keyed `pipelines` row; all dynamic per-stage and per-pipeline tables are created with CHECK constraints on `status` columns.
- The CLI `sluice run -t <task> -p <pipeline>` is wired end-to-end to the stub runner.
- Plan 2 will implement actual stage execution, items writes, eligibility query, and resume.

**Verify before merging:**
```bash
pytest tests/dag/ -v
ruff check sluice/dag tests/dag
```
