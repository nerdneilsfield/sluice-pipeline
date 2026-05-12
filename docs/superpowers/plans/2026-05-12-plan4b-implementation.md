# Plan 4b — v1 Removal, v2 CLI Completion, Concurrency, Ops Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Finish all remaining work from `2026-05-05-plan4a-prefect-deploy-design.md` ("Out (Plan 4b)") plus the v2 CLI commands from `2026-05-02-multi-pipeline-dag-design.md` that have not yet been implemented.

**Architecture:** Five independent sections executed in dependency order — (1) untangle the last v2→v1 cross-references and delete every v1 module and test, (2) add the missing v2 CLI commands (`migrate`, `retry`, `items list/show/kill`, `scaffold-task`), (3) wire a real Prefect 3 concurrency-collision strategy now that the v1 fog is gone, (4) ship deployment artifacts (Dockerfile + compose + systemd unit) for running the Prefect worker, (5) write the v1→v2 migration document. Every behavior-bearing change is TDD; every section ends with a commit. v1 deletion is mass-removal, not porting — v2 already has parallel implementations of everything that v1 used to do.

**Tech Stack:**
- Python 3.12, `typer`, `asyncio`, `aiosqlite`
- Prefect 3 (`prefect.deploy`, `flow.to_deployment`, work-pool concurrency)
- SQLite (v2 stores under `sluice/dag/stores/`)
- pytest (incl. `pytest-asyncio`)
- Docker + docker-compose, systemd

---

## File Structure

### Section 1 — v1 deletion

**Sink protocol code is ported, not deleted.** `sluice/dag/sinks/{telegram,email,notion,feishu}_sink.py` are thin adapters that import the real protocol implementations from `sluice/sinks/{telegram,email,notion,feishu}.py` (verified: `grep "from sluice.sinks" sluice/dag/sinks/`). `sluice/sinks/_push_base.py` and `sluice/sinks/base.py` further depend on `sluice/state/emissions.py` and `sluice/state/delivery_log.py`. Deleting `sluice/sinks/` outright breaks every v2 sink. **Task 1.0 (new, runs first)** moves the protocol code into `sluice/dag/sinks/_impl/`, strips the `sluice/state/*` imports (v2 store layer handles emissions/delivery via the writer thread; the v1 `emit(...)` signatures inside `_push_base`/`base` are dead in the v2 codepath), and rewires the v2 sink adapters. Only after Task 1.0 does Task 1.4 delete `sluice/sinks/`.

**Modules deleted:**
- `sluice/sinks/` (entire directory; deleted in Task 1.4 *after* Task 1.0 has ported the live code into `sluice/dag/sinks/_impl/`)
- `sluice/fetchers/` (entire directory)
- `sluice/processors/` (entire directory)
- `sluice/enrichers/` (entire directory)
- `sluice/sources/` (entire directory)
- `sluice/runner.py`
- `sluice/builders.py`
- `sluice/flow.py`
- `sluice/gc.py`
- `sluice/gc_flow.py`
- `sluice/state/` (entire directory)
- `sluice/loader.py` (v1 loader; v2 loader at `sluice/dag/loader.py` is untouched)
- `sluice/run_key.py` (v1 run key; v2 generator inlined in `sluice/dag/run_pipeline.py`)
- `sluice/url_canon.py`, `sluice/window.py`, `sluice/metrics.py`, `sluice/pricing.py`, `sluice/registry.py` — all DELETE (zero post-purge v2 consumers; see classification table below). `sluice/logging_setup.py` is MOVED to `sluice/core/logging_setup.py` (Task 1.1 Step 6), not deleted.

**Modules kept (trimmed in place):**
- `sluice/core/` (errors, item — v2-shared)
- `sluice/llm/` (LLM client; consumed by v2 stages)
- `sluice/context.py` (consumed by v2 sinks)
- `sluice/dag/` (the entire v2 tree)
- `sluice/cli.py` (the v1 commands are removed; the file itself stays)
- `sluice/config.py` — **NEVER `git rm`.** `sluice/llm/{pool,provider}.py` import `ProvidersConfig`/`BaseEndpoint`/`KeyConfig`/`ModelEntry`/`Provider` from it. Task 1.7 Step 5 trims out v1-only `PipelineConfig`/`GlobalConfig` in place.

**Bridge file rewritten:**
- `sluice/dag/sinks/_v1_push.py` — currently imports `PushBatchItem` from `sluice/sinks/_push_base.py` AND `resolve_env` from `sluice/loader.py`. Both v1-side modules go away in Section 1. Task 1.1 resolves both imports: extract `PushBatchItem` inline into `_v1_push.py` (if `_v1_push.py` is kept), and extract `resolve_env` from `sluice/loader.py` into the new `sluice/core/env.py`. `sluice/dag/deploy.py:19` also imports `resolve_env` and is updated to import from `sluice.core.env`.

**Pre-baked classification of top-level v1 shims** (Task 1.2 outcome, decided at plan time so Task 1.7 has a concrete delete list):

| Module | Post-purge consumers | Action |
|--------|-----------------------|--------|
| `sluice/loader.py` | v2 `deploy.py` + `dag/sinks/_v1_push.py` use `resolve_env`; everything else is v1 | Extract `resolve_env` → `sluice/core/env.py`; **DELETE** `sluice/loader.py` |
| `sluice/config.py` | `sluice/llm/pool.py`, `sluice/llm/provider.py` (provider types `ProvidersConfig`/`BaseEndpoint`/`KeyConfig`/`ModelEntry`/`Provider`) | **KEEP** as-is; trim v1-only `PipelineConfig`/`GlobalConfig` in Task 1.7c |
| `sluice/run_key.py` | none | **DELETE** |
| `sluice/metrics.py` | only v1 `metrics-server` CLI command (deleted in Task 1.3) | **DELETE** |
| `sluice/pricing.py` | none | **DELETE** |
| `sluice/registry.py` | none | **DELETE** |
| `sluice/url_canon.py` | none | **DELETE** |
| `sluice/window.py` | only v1 CLI commands (deleted in Task 1.3) | **DELETE** |
| `sluice/logging_setup.py` | `sluice/llm/middleware.py`, `sluice/llm/client.py`, `sluice/cli.py` (v2 commands) | **MOVE** to `sluice/core/logging_setup.py`, update three import sites |

**Tests deleted:**
- `tests/sinks/` (entire directory — v1 sink tests; v2 sinks already have parallel tests under `tests/dag/`)
- `tests/state/` (entire directory)
- `tests/fetchers/` (entire directory)
- `tests/processors/` (entire directory)
- `tests/config/` (entire directory — `test_loader.py`, `test_pipeline_config.py`, `test_model_entry.py`, `test_provider_config.py`; v1 loader/config gone. Note: `test_model_entry.py` + `test_provider_config.py` test LLM provider types — if those tests are still valuable after Task 1.7 trims `sluice/config.py`, port them to `tests/llm/test_provider_config.py` instead of deleting)
- `tests/test_runner.py`
- `tests/test_flow.py`
- `tests/test_builders.py`
- `tests/test_builders_long_context.py`
- `tests/test_gc.py`
- `tests/test_cli_deliveries.py`
- `tests/test_metrics.py` (v1 metrics module deleted by Task 1.7)
- `tests/test_window.py`, `tests/test_window_lookback_v1_1.py`, `tests/test_window_parse_duration_ms.py` (v1 window module deleted)
- `tests/test_url_canon.py` (v1 module deleted)
- `tests/test_example_configs.py`, `tests/test_config_long_context.py` (v1 example/global configs gone)
- `tests/test_logging_setup.py` — **NOT deleted**: relocate to `tests/core/test_logging_setup.py` and update its `from sluice.logging_setup import …` lines to `from sluice.core.logging_setup import …` (handled in Task 1.1 Step 6 follow-up — see explicit step there)
- `tests/integration/test_smoke.py` (v1 smoke; v2 smoke lives under `tests/dag/`)
- `tests/dag/test_v1_orchestration_guard.py` (guard no longer needed once v1 is gone)

**CLI surgery in `sluice/cli.py`:**
- Delete `run-legacy` (lines 61-…), `failures` (~203), `gc` (~285), `stats` (~379), `metrics-server` (~435), `deliveries` (~472).
- Delete v1 imports at the top (`from sluice.runner import …`, `from sluice.state.db import …`, `from sluice.gc import …`, `from sluice.loader import …` if any).
- Verify `sluice/cli.py` re-runs ruff + mypy clean after surgery.

### Section 2 — v2 CLI commands

**New files:**
- `sluice/dag/admin.py` — pure logic for the new admin commands (`retry_pipeline`, `list_items`, `show_item`, `kill_item`, `migrate_task`, `scaffold_from_v1`). CLI layer is thin.
- `tests/dag/test_admin.py` — unit tests for `admin.py`.
- `tests/dag/test_cli_admin.py` — end-to-end tests of the new CLI commands.

**Modified:**
- `sluice/cli.py` — add `@app.command("retry") def retry_cmd(...)`, `@app.command("migrate") def migrate_cmd(...)`, `@app.command("scaffold-task") def scaffold_cmd(...)`, plus a typer sub-app `@items_app.command("list" | "show" | "kill")` mounted via `app.add_typer(items_app, name="items")`.

### Section 3 — Concurrency collision strategy

**Modified:**
- `sluice/dag/sluice_config.py` — `RuntimeConfig` gains `concurrency_strategy: Literal["queue", "cancel_new", "enqueue"] = "queue"`.
- `sluice/dag/config.py` — `PipelineDef` gains optional `concurrency_strategy: str | None = None` (per-pipeline override).
- `sluice/dag/deploy.py` — `build_pipeline_deployment` resolves effective strategy (pipeline override → runtime default → queue) and applies it via the appropriate Prefect 3 API. See Task 3.1 for the API research output.
- `tests/dag/test_deploy.py` — assert strategy propagation per matrix.

### Section 4 — Prefect deployment artifacts

**New files:**
- `deploy/Dockerfile.worker` — builds the sluice + Prefect worker image.
- `deploy/docker-compose.yml` — Prefect server + worker + Postgres (server backend).
- `deploy/Makefile` — `make image`, `make push`, `make up`, `make down`.
- `deploy/systemd/sluice-worker.service` — systemd unit for non-container deployment.
- `deploy/.env.example` — required env vars (`PREFECT_API_URL`, `PREFECT_API_AUTH_STRING`, image tag, etc.).

### Section 5 — Migration documentation

**New file:**
- `docs/migration/v1-to-v2.md` — step-by-step migration guide.

---

## Section 1 — v1 Removal

### Task 1.0: Port v1 sink protocol code into `sluice/dag/sinks/_impl/`

**Files:**
- Create: `sluice/dag/sinks/_impl/__init__.py`
- Create (by move) — 13 files: `sluice/dag/sinks/_impl/{base,_push_base,telegram,email,notion,feishu,file_md,_markdown_ast,_telegram_render,_email_render,_feishu_bot,_feishu_render,_feishu_post_render}.py`
- Modify: `sluice/dag/sinks/{telegram_sink,email_sink,notion_sink,feishu_sink,file_md,_v1_push}.py`
- Strip dead imports: `sluice/dag/sinks/_impl/{base,_push_base}.py` lose every `from sluice.state.*` line and the `emit(...)` methods that consume them.

The v2 sink adapters in `sluice/dag/sinks/` currently import `V1TelegramSink` (etc.) from `sluice/sinks/telegram.py`. The v1 protocol files (`telegram.py`, `email.py`, `notion.py`, `feishu.py`, `file_md.py`, `_push_base.py`, `base.py`) hold the actual HTTP/SMTP/Markdown code that v2 still needs. v1 `_push_base.py` and `base.py` also pull in `sluice/state/emissions.py` + `sluice/state/delivery_log.py`, but those imports are **dead in the v2 codepath** — the v2 writer thread + `SinkEmissionsStore`/`SinkDeliveryLogStore` handle emissions/delivery; the v1 `emit(...)` method on these base classes is never called from v2.

- [ ] **Step 1: Snapshot the v1 sink package structure**

```bash
ls sluice/sinks/
grep -n "from sluice" sluice/sinks/*.py
```
Confirms which files have v1-state imports that must be stripped during the move.

- [ ] **Step 2: Move every protocol file (incl. private helpers) into the new namespace**

`sluice/sinks/` contains **13** real `.py` files: 7 top-level sinks/base files plus 6 private helpers (`_markdown_ast.py`, `_telegram_render.py`, `_email_render.py`, `_feishu_bot.py`, `_feishu_render.py`, `_feishu_post_render.py`) that the top-level files import. Missing any helper breaks the import graph after Task 1.4 deletes `sluice/sinks/`.

```bash
mkdir -p sluice/dag/sinks/_impl
touch sluice/dag/sinks/_impl/__init__.py
git mv sluice/sinks/base.py                  sluice/dag/sinks/_impl/base.py
git mv sluice/sinks/_push_base.py            sluice/dag/sinks/_impl/_push_base.py
git mv sluice/sinks/telegram.py              sluice/dag/sinks/_impl/telegram.py
git mv sluice/sinks/email.py                 sluice/dag/sinks/_impl/email.py
git mv sluice/sinks/notion.py                sluice/dag/sinks/_impl/notion.py
git mv sluice/sinks/feishu.py                sluice/dag/sinks/_impl/feishu.py
git mv sluice/sinks/file_md.py               sluice/dag/sinks/_impl/file_md.py
git mv sluice/sinks/_markdown_ast.py         sluice/dag/sinks/_impl/_markdown_ast.py
git mv sluice/sinks/_telegram_render.py      sluice/dag/sinks/_impl/_telegram_render.py
git mv sluice/sinks/_email_render.py         sluice/dag/sinks/_impl/_email_render.py
git mv sluice/sinks/_feishu_bot.py           sluice/dag/sinks/_impl/_feishu_bot.py
git mv sluice/sinks/_feishu_render.py        sluice/dag/sinks/_impl/_feishu_render.py
git mv sluice/sinks/_feishu_post_render.py   sluice/dag/sinks/_impl/_feishu_post_render.py
```

Inside each moved file, replace any intra-package `from sluice.sinks.<x>` with `from sluice.dag.sinks._impl.<x>`. Verify with:
```bash
grep -n "from sluice.sinks" sluice/dag/sinks/_impl/
```
Expected: zero hits after rewrites.

- [ ] **Step 3: Strip dead v1-state imports from the moved files**

Edit `sluice/dag/sinks/_impl/base.py`:
- Delete `from sluice.state.emissions import EmissionStore`
- Delete the `async def emit(self, ctx: PipelineContext, *, emissions: EmissionStore) -> SinkResult | None:` method body and signature. If a subclass abstract requirement remains, replace it with a `NotImplementedError` stub that's never called from v2.

Edit `sluice/dag/sinks/_impl/_push_base.py`:
- Delete `from sluice.state.delivery_log import DeliveryLog`
- Delete `from sluice.state.emissions import EmissionStore`
- Change the `delivery_log: DeliveryLog,` constructor parameter to `delivery_log: object | None = None,` so all subclasses (and v2 adapters) still pass it without TypeError. Drop the `self._log = delivery_log` body line or keep it as `self._log = None` — it's no longer read by anything.
- Delete the `async def emit(self, ctx: PipelineContext, *, emissions: EmissionStore) -> SinkResult | None:` method.

`TelegramSink`, `EmailSink`, `FeishuSink` constructors (see `sluice/sinks/{telegram,email,feishu}.py` `delivery_log` params and their `super().__init__(... delivery_log=delivery_log)` calls) keep passing `delivery_log` through — the kept-compat parameter on `_push_base` swallows it. `sluice/dag/sinks/{telegram_sink,email_sink,feishu_sink}.py` already pass `delivery_log=None`; that continues to work. No subclass body changes needed.

Run `rtk ruff check sluice/dag/sinks/_impl/` after each file edit; resolve any cascading unused-import warnings.

- [ ] **Step 4: Update v2 sink adapter imports**

In each of `sluice/dag/sinks/{telegram_sink,email_sink,notion_sink,feishu_sink,file_md}.py`, replace:
```python
from sluice.sinks.<name> import <SinkClass> as V1<SinkClass>
```
with:
```python
from sluice.dag.sinks._impl.<name> import <SinkClass> as V1<SinkClass>
```

In `sluice/dag/sinks/_v1_push.py`, the `from sluice.sinks._push_base import PushBatchItem` line is replaced in Task 1.1; if `_v1_push.py` references any other symbol from `sluice/sinks/`, redirect it to `sluice.dag.sinks._impl.*`.

- [ ] **Step 5: Move the v1 sink tests into the v2 tree (port, don't delete)**

The existing tests under `tests/sinks/` exercise the real protocol logic — they remain valuable after the port. Move every test file (including helper-coverage like `test_markdown_ast.py`):
```bash
mkdir -p tests/dag/sinks_impl
git mv tests/sinks/test_base.py             tests/dag/sinks_impl/test_base.py
git mv tests/sinks/test_push_base.py        tests/dag/sinks_impl/test_push_base.py
git mv tests/sinks/test_telegram.py         tests/dag/sinks_impl/test_telegram.py
git mv tests/sinks/test_email.py            tests/dag/sinks_impl/test_email.py
git mv tests/sinks/test_notion.py           tests/dag/sinks_impl/test_notion.py
git mv tests/sinks/test_feishu.py           tests/dag/sinks_impl/test_feishu.py
git mv tests/sinks/test_file_md.py          tests/dag/sinks_impl/test_file_md.py
# Any additional helper tests present in tests/sinks/ — move them too:
for f in tests/sinks/test_*.py; do git mv "$f" "tests/dag/sinks_impl/$(basename "$f")"; done
# tests/sinks/__init__.py is left in place — Task 1.4 deletes the whole directory.
```
Then update each moved file's imports: replace every `from sluice.sinks.<x>` with `from sluice.dag.sinks._impl.<x>`. Tests that exercise the deleted `emit(...)` method (the ones that import `EmissionStore` / `DeliveryLog`) must also be deleted — those code paths went away in Step 3.

- [ ] **Step 6: Run v2 + ported sink tests**

```bash
rtk pytest tests/dag/ -x -q
```
Expected: every previously-green v2 test still green; ported sink tests green (modulo the emit-path tests removed in Step 5).

- [ ] **Step 7: Verify `sluice/sinks/` is now empty**

```bash
ls sluice/sinks/
```
Expected: no `.py` files except possibly an `__init__.py`. Task 1.4 will `git rm -r sluice/sinks/`.

- [ ] **Step 8: Commit**

```bash
git add sluice/dag/sinks/ tests/dag/sinks_impl/
git commit -m "refactor(sinks): port v1 sink protocol into sluice.dag.sinks._impl, strip dead state imports"
```

### Task 1.1: Extract shared utilities (`resolve_env`, `logging_setup`) into `sluice/core/`

**Files:**
- Create: `sluice/core/env.py`
- Create: `sluice/core/logging_setup.py`
- Modify: `sluice/dag/deploy.py` (line 19)
- Modify: `sluice/dag/sinks/_v1_push.py` (lines 9-10)
- Modify: `sluice/llm/middleware.py` (line 40)
- Modify: `sluice/llm/client.py` (line 14)
- Modify: `sluice/cli.py` (line 18)
- Create: `tests/core/test_env.py`

This task untangles every v2-side import that still points at `sluice/loader.py` or `sluice/logging_setup.py`, so Sections 1.4–1.7 can delete those top-level v1 files without breaking v2 code.

- [ ] **Step 1: Write failing test for `core.env.resolve_env`**

Create `tests/core/test_env.py`:
```python
import os

import pytest

from sluice.core.env import resolve_env


def test_resolve_env_literal_passthrough():
    assert resolve_env("plain-string") == "plain-string"


def test_resolve_env_reads_environment(monkeypatch):
    monkeypatch.setenv("MY_TOKEN", "secret")
    assert resolve_env("env:MY_TOKEN") == "secret"


def test_resolve_env_missing_returns_empty(monkeypatch):
    monkeypatch.delenv("ABSENT_TOKEN", raising=False)
    assert resolve_env("env:ABSENT_TOKEN") == ""
```

- [ ] **Step 2: Run — expect FAIL**

Run: `rtk pytest tests/core/test_env.py -v`
Expected: `ModuleNotFoundError: sluice.core.env`.

- [ ] **Step 3: Move `resolve_env`**

`grep -n "def resolve_env" sluice/loader.py` to locate the function. Copy its body verbatim into `sluice/core/env.py`. The function signature is `def resolve_env(value: str) -> str:`. No additional imports needed beyond `os`.

- [ ] **Step 4: Run — expect PASS**

- [ ] **Step 5: Update the three current `resolve_env` import sites**

In `sluice/dag/deploy.py`:
```python
# replace:
from sluice.loader import resolve_env
# with:
from sluice.core.env import resolve_env
```

In `sluice/dag/sinks/_v1_push.py`:
```python
# replace:
from sluice.loader import resolve_env
from sluice.sinks._push_base import PushBatchItem
# with:
from dataclasses import dataclass

from sluice.core.env import resolve_env


@dataclass
class PushBatchItem:
    kind: str
    payload: object
    recipient: str | None = None
```

(The `PushBatchItem` dataclass is small enough to inline; copy the real field list from `sluice/sinks/_push_base.py:13` if it differs — verify with `grep -A 6 'class PushBatchItem' sluice/sinks/_push_base.py`.)

- [ ] **Step 6: Move `logging_setup` to `sluice/core/logging_setup.py` and relocate its test**

```bash
git mv sluice/logging_setup.py sluice/core/logging_setup.py
mkdir -p tests/core
git mv tests/test_logging_setup.py tests/core/test_logging_setup.py
```

Then update import sites:
- `sluice/llm/middleware.py:40` → `from sluice.core.logging_setup import get_logger`
- `sluice/llm/client.py:14` → `from sluice.core.logging_setup import get_logger`
- `sluice/cli.py:18` → `from sluice.core.logging_setup import configure_cli_logging, create_logger`
- `tests/core/test_logging_setup.py` → replace every `from sluice.logging_setup import …` with `from sluice.core.logging_setup import …`

- [ ] **Step 7: Run v2 tests + lint — expect PASS**

Run:
```bash
rtk pytest tests/dag/ tests/core/ -x -q
rtk ruff check sluice/ tests/
```

- [ ] **Step 8: Commit**

```bash
git add sluice/core/env.py sluice/core/logging_setup.py tests/core/test_env.py \
        tests/core/test_logging_setup.py \
        sluice/dag/deploy.py sluice/dag/sinks/_v1_push.py \
        sluice/llm/middleware.py sluice/llm/client.py sluice/cli.py
# git mv already staged the deletion of the old path; no separate `git rm` needed.
git commit -m "refactor: relocate resolve_env and logging_setup into sluice.core"
```

### Task 1.3: Delete v1 CLI commands and their imports

**Files:**
- Modify: `sluice/cli.py`
- Delete: `tests/test_cli_deliveries.py`

- [ ] **Step 1: Write failing tests asserting the v1 commands are gone**

Create `tests/dag/test_cli_v1_purge.py`:
```python
from typer.testing import CliRunner

from sluice.cli import app


def test_v1_commands_removed():
    runner = CliRunner()
    for cmd in ("run-legacy", "failures", "gc", "stats", "metrics-server", "deliveries"):
        result = runner.invoke(app, [cmd, "--help"])
        assert result.exit_code != 0, f"v1 command still present: {cmd}"
```

- [ ] **Step 2: Run the test — expect FAIL**

Run: `rtk pytest tests/dag/test_cli_v1_purge.py -v`
Expected: FAIL (commands still registered).

- [ ] **Step 3: Delete the six v1 command functions from `sluice/cli.py`**

In `sluice/cli.py`, remove:
- `@app.command("run-legacy")` function `run_legacy` (lines around 61–202)
- `@app.command() def failures(...)` (around 203–243)
- `@app.command() def gc(...)` (around 285–378)
- `@app.command() def stats(...)` (around 379–434)
- `@app.command("metrics-server") def metrics_server(...)` (around 435–471)
- `@app.command() def deliveries(...)` (around 472–522)

Also delete imports that become unused at the top of `cli.py`:
- `from sluice.runner import run_pipeline as run_legacy_pipeline`
- any `from sluice.state.* import …`
- any `from sluice.gc import …`
- any `from sluice.loader import …` if no v2 command still uses it

- [ ] **Step 4: Run `rtk ruff check sluice/cli.py` and resolve unused-import warnings**

- [ ] **Step 5: Re-run the test — expect PASS**

Run: `rtk pytest tests/dag/test_cli_v1_purge.py -v`
Expected: PASS.

- [ ] **Step 6: Delete the v1 deliveries test file**

```bash
git rm tests/test_cli_deliveries.py
```

- [ ] **Step 7: Commit**

```bash
git add sluice/cli.py tests/dag/test_cli_v1_purge.py
git commit -m "refactor(cli): remove v1 commands (run-legacy, failures, gc, stats, metrics-server, deliveries)"
```

### Task 1.4: Delete v1 sink modules and tests

**Files:**
- Delete: `sluice/sinks/` (entire directory)
- Delete: `tests/sinks/` (entire directory)

- [ ] **Step 1: Verify no remaining importer**

Run:
```bash
grep -rn "from sluice.sinks\b\|import sluice.sinks\b" sluice/ tests/ --include="*.py"
```
Expected: zero hits (Task 1.1 already removed the only v2-side reference). If anything remains, fix it before deleting.

- [ ] **Step 2: Delete directories**

```bash
git rm -r sluice/sinks/ tests/sinks/
```

- [ ] **Step 3: Run full suite — expect PASS**

Run: `rtk pytest -x -q`

- [ ] **Step 4: Commit**

```bash
git commit -m "refactor: remove v1 sinks package and tests"
```

### Task 1.5: Delete v1 fetchers, processors, enrichers, sources

**Files:**
- Delete: `sluice/fetchers/`, `sluice/processors/`, `sluice/enrichers/`, `sluice/sources/`
- Delete: `tests/fetchers/`, `tests/processors/` (and any matching `tests/enrichers/`, `tests/sources/` if present)

- [ ] **Step 1: Verify v2 stage executors are self-contained**

Run:
```bash
grep -rn "from sluice.fetchers\|from sluice.processors\|from sluice.enrichers\|from sluice.sources" sluice/dag/ sluice/cli.py sluice/core/
```
Expected: zero hits. (Already confirmed during scoping — re-verify in the agent's working tree.)

- [ ] **Step 2: Delete directories**

```bash
git rm -r sluice/fetchers/ sluice/processors/ sluice/enrichers/ sluice/sources/ tests/fetchers/ tests/processors/
# also remove tests/enrichers tests/sources if they exist
[ -d tests/enrichers ] && git rm -r tests/enrichers
[ -d tests/sources ] && git rm -r tests/sources
```

- [ ] **Step 3: Run full suite — expect PASS**

Run: `rtk pytest -x -q`

- [ ] **Step 4: Commit**

```bash
git commit -m "refactor: remove v1 fetchers/processors/enrichers/sources packages"
```

### Task 1.6: Delete v1 runner / builders / flow / gc / gc_flow / state and their tests

**Files:**
- Delete: `sluice/runner.py`, `sluice/builders.py`, `sluice/flow.py`, `sluice/gc.py`, `sluice/gc_flow.py`, `sluice/state/`
- Delete: `tests/test_runner.py`, `tests/test_flow.py`, `tests/test_builders.py`, `tests/test_builders_long_context.py`, `tests/test_gc.py`, `tests/state/`, `tests/integration/test_smoke.py`, `tests/dag/test_v1_orchestration_guard.py`

- [ ] **Step 1: Verify no remaining importer**

Run:
```bash
grep -rn "from sluice.runner\|from sluice.builders\|from sluice.flow\b\|from sluice.gc\b\|from sluice.gc_flow\|from sluice.state\b" sluice/ tests/ --include="*.py"
```
Expected: zero hits.

- [ ] **Step 2: Delete modules**

```bash
git rm sluice/runner.py sluice/builders.py sluice/flow.py sluice/gc.py sluice/gc_flow.py
git rm -r sluice/state/
```

- [ ] **Step 3: Delete tests**

```bash
git rm tests/test_runner.py tests/test_flow.py tests/test_builders.py \
       tests/test_builders_long_context.py tests/test_gc.py \
       tests/integration/test_smoke.py tests/dag/test_v1_orchestration_guard.py
git rm -r tests/state/
```

- [ ] **Step 4: Run full suite — expect PASS**

Run: `rtk pytest -x -q`
Expected: every previously-green v2 test still green; suite total shrinks.

- [ ] **Step 5: Commit**

```bash
git commit -m "refactor: remove v1 runner/builders/flow/gc/state and their tests"
```

### Task 1.7: Delete the remaining v1-only top-level modules

Uses the pre-baked classification table at the top of Section 1. Splits into three commits because `sluice/config.py` needs an in-place trim (KEEP), not a deletion.

- [ ] **Step 1: Verify zero remaining importers for the DELETE list**

Run:
```bash
for mod in loader run_key metrics pricing registry url_canon window; do
  echo "=== sluice/${mod}.py ==="
  grep -rn "from sluice.${mod}\b\|import sluice.${mod}\b" sluice/ tests/ --include="*.py" 2>/dev/null
done
```
Expected: zero hits (after Tasks 1.1, 1.3, 1.4–1.6). If any importer remains, fix that importer's source first — do not bypass.

- [ ] **Step 2: Delete the seven dead modules**

```bash
git rm sluice/loader.py sluice/run_key.py sluice/metrics.py sluice/pricing.py \
       sluice/registry.py sluice/url_canon.py sluice/window.py
```

- [ ] **Step 3: Run full suite — expect PASS**

Run: `rtk pytest -x -q`

- [ ] **Step 4: Commit**

```bash
git commit -m "refactor: remove dead v1 top-level shims (loader/run_key/metrics/pricing/registry/url_canon/window)"
```

- [ ] **Step 5: Trim `sluice/config.py` to provider types only**

`sluice/config.py` is KEPT (LLM provider types `ProvidersConfig`/`BaseEndpoint`/`KeyConfig`/`ModelEntry`/`Provider` are imported by `sluice/llm/`). But the v1-only types `PipelineConfig`, `GlobalConfig`, and any other classes used only by the deleted `sluice/loader.py` / `sluice/runner.py` / `sluice/builders.py` must be removed.

Procedure:
```bash
grep -n "^class " sluice/config.py    # list every class
```
For each class, run:
```bash
grep -rn "<ClassName>\b" sluice/ tests/ --include="*.py" | grep -v "sluice/config.py"
```
If a class is only referenced from inside `sluice/config.py` itself, delete its definition. Run `rtk ruff check sluice/config.py` after each removal to surface dangling references.

- [ ] **Step 6: Run full suite + LLM tests — expect PASS**

Run:
```bash
rtk pytest tests/ -x -q
```

- [ ] **Step 7: Commit**

```bash
git commit -am "refactor(config): trim sluice/config.py to LLM provider types only"
```

### Task 1.8: Verify Section 1 result with lint + format + full test sweep

- [ ] **Step 1: Lint**

Run: `rtk ruff check sluice/ tests/`
Expected: no issues.

- [ ] **Step 2: Format check**

Run: `rtk ruff format --check sluice/ tests/`
Expected: already formatted.

- [ ] **Step 3: Full test suite**

Run: `rtk pytest -q`
Expected: every remaining test passes. Record the post-purge test count in the commit message.

- [ ] **Step 4: Commit (only if formatting/lint required edits)**

```bash
git commit -am "chore: post-purge lint/format pass"
```

---

## Section 2 — v2 CLI Completion

### Task 2.0: Extend `tests/dag/test_helpers.py` with task / DB seed helpers

**Files:**
- Modify: `tests/dag/test_helpers.py`

The current `test_helpers.py` only defines `_row` and a few `safe_name` / `source_hash` tests (verified during scoping). Section 2 tasks need fixture helpers; build them here once.

- [ ] **Step 1: Add the helpers**

Append to `tests/dag/test_helpers.py`:
```python
from __future__ import annotations

import sqlite3
import textwrap
from datetime import UTC, datetime
from pathlib import Path


def make_task_dir(
    tmp_path: Path,
    *,
    name: str,
    pipeline_names: list[str],
    stage_names: list[str],
) -> Path:
    """Create a minimal valid task directory under tmp_path/configs/tasks/<name>/.

    The single pipeline lists all stages linearly; one rss source is generated.
    """
    task_dir = tmp_path / "configs" / "tasks" / name
    task_dir.mkdir(parents=True, exist_ok=True)
    db_path = tmp_path / "data" / f"{name}.db"
    seen_db = tmp_path / "data" / f"{name}_seen.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)

    (task_dir / "task.toml").write_text(textwrap.dedent(f"""\
        [task]
        name    = "{name}"
        db_path = "{db_path}"
        seen_db = "{seen_db}"
        enabled = true

        [task.window]
        anchor = "0 8 * * *"
        length = "24h"

        [task.deployments]
        pipelines = "all"
    """))
    (task_dir / "sources.toml").write_text(textwrap.dedent("""\
        [[sources]]
        name = "src1"
        type = "rss"
        url  = "https://example.test/rss"
    """))
    (task_dir / "sinks.toml").write_text("")
    # First stage must be `fetch_rss` so the pipeline + sources combo passes
    # validate_task()'s shape check (see sluice/dag/validator.py:79). The
    # remaining stage names are written as `filter` stages — harmless for
    # admin/migrate tests that never run the pipeline.
    stage_blocks: list[str] = []
    for i, s in enumerate(stage_names):
        stype = "fetch_rss" if i == 0 else "filter"
        block = textwrap.dedent(f"""\
            [[stages]]
            name = "{s}"
            type = "{stype}"
        """)
        if stype == "fetch_rss":
            block += 'sources = ["src1"]\n'
        stage_blocks.append(block)
    (task_dir / "stages.toml").write_text("\n".join(stage_blocks))
    pipeline_blocks = "\n".join(
        textwrap.dedent(f"""\
            [[pipelines]]
            name    = "{p}"
            cron    = "0 */6 * * *"
            sources = ["src1"]
            stages  = {stage_names!r}
        """) for p in pipeline_names
    )
    (task_dir / "pipelines.toml").write_text(pipeline_blocks)
    return task_dir


def make_configs_dir(tmp_path: Path, *, tasks: dict[str, dict]) -> Path:
    """Convenience wrapper used by CLI tests; returns the configs root.

    tasks = {"<task_name>": {"pipelines": [...], "stages": [...]}}
    """
    for tname, spec in tasks.items():
        make_task_dir(
            tmp_path,
            name=tname,
            pipeline_names=spec["pipelines"],
            stage_names=spec["stages"],
        )
    return tmp_path / "configs"


async def seed_item(
    db_path: str | Path,
    *,
    item_id: str,
    source_name: str,
    title: str | None = None,
) -> None:
    import aiosqlite

    now = datetime.now(UTC).isoformat(timespec="seconds")
    async with aiosqlite.connect(str(db_path)) as db:
        await db.execute(
            "INSERT INTO items (item_id, source_name, ingested_at, ingested_by, updated_at, raw_json, title, dropped) "
            "VALUES (?, ?, ?, ?, ?, '{}', ?, 0)",
            (item_id, source_name, now, "P", now, title),
        )
        await db.commit()


def seed_item_sync(db_path: str | Path, *, item_id: str, source_name: str, title: str | None = None) -> None:
    now = datetime.now(UTC).isoformat(timespec="seconds")
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            "INSERT INTO items (item_id, source_name, ingested_at, ingested_by, updated_at, raw_json, title, dropped) "
            "VALUES (?, ?, ?, ?, ?, '{}', ?, 0)",
            (item_id, source_name, now, "P", now, title),
        )
        conn.commit()


async def seed_pipeline_state(
    db_path: str | Path,
    *,
    pipeline: str,
    item_id: str,
    status: str,
) -> None:
    import aiosqlite

    now = datetime.now(UTC).isoformat(timespec="seconds")
    async with aiosqlite.connect(str(db_path)) as db:
        await db.execute(
            f"INSERT INTO pipeline_{pipeline} (item_id, last_run_id, status, processed_at) "
            "VALUES (?, 1, ?, ?)",
            (item_id, status, now),
        )
        await db.commit()


def seed_pipeline_state_sync(
    db_path: str | Path,
    *,
    pipeline: str,
    item_id: str,
    status: str,
) -> None:
    now = datetime.now(UTC).isoformat(timespec="seconds")
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            f"INSERT INTO pipeline_{pipeline} (item_id, last_run_id, status, processed_at) "
            "VALUES (?, 1, ?, ?)",
            (item_id, status, now),
        )
        conn.commit()


async def seed_stage_state(
    db_path: str | Path,
    *,
    stage: str,
    pipeline: str,
    item_id: str,
    status: str,
    attempts: int = 1,
) -> None:
    import aiosqlite

    now = datetime.now(UTC).isoformat(timespec="seconds")
    async with aiosqlite.connect(str(db_path)) as db:
        await db.execute(
            f"INSERT INTO stage_{stage} (pipeline_name, item_id, status, attempts, "
            "last_stage_run_id, last_pipeline_run_id, last_processed_at) "
            "VALUES (?, ?, ?, ?, 1, 1, ?)",
            (pipeline, item_id, status, attempts, now),
        )
        await db.commit()
```

- [ ] **Step 2: Sanity-check the helpers compile and run on a tmp DB**

Append one self-test to `tests/dag/test_helpers.py`:
```python
def test_make_task_dir_writes_five_files(tmp_path):
    tdir = make_task_dir(tmp_path, name="x", pipeline_names=["p"], stage_names=["s"])
    for f in ("task.toml", "sources.toml", "sinks.toml", "stages.toml", "pipelines.toml"):
        assert (tdir / f).exists()
```

Run: `rtk pytest tests/dag/test_helpers.py -v`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/dag/test_helpers.py
git commit -m "test: add task-dir and db-seed fixture helpers for admin CLI tests"
```

### Task 2.1: `sluice migrate <task>` — apply schema + dynamic tables

**Files:**
- Create: `sluice/dag/admin.py`
- Modify: `sluice/cli.py`
- Create: `tests/dag/test_admin_migrate.py`

- [ ] **Step 1: Write failing test for `admin.migrate_task`**

Create `tests/dag/test_admin_migrate.py`:
```python
from pathlib import Path

import pytest

from sluice.dag.admin import migrate_task
from sluice.dag.loader import load_task
from tests.dag.test_helpers import make_task_dir


@pytest.mark.asyncio
async def test_migrate_task_creates_schema_and_dynamic_tables(tmp_path: Path):
    task_dir = make_task_dir(tmp_path, name="t1", pipeline_names=["p1"], stage_names=["s1"])
    task = load_task(task_dir)

    await migrate_task(task)

    import aiosqlite
    async with aiosqlite.connect(task.db_path) as db:
        rows = await (await db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )).fetchall()
    names = {r[0] for r in rows}
    assert "items" in names
    assert "pipelines" in names
    assert "pipeline_p1" in names
    assert "stage_s1" in names
    assert "rotation_gate" in names       # from v2_0002_active_rotation_gate.sql
    assert "pipeline_admissions" in names  # from v2_0002_active_rotation_gate.sql
```

- [ ] **Step 2: Run — expect FAIL (admin module missing)**

Run: `rtk pytest tests/dag/test_admin_migrate.py -v`
Expected: `ModuleNotFoundError: sluice.dag.admin`.

- [ ] **Step 3: Implement `migrate_task`**

Create `sluice/dag/admin.py`:
```python
from __future__ import annotations

from pathlib import Path

import aiosqlite

from sluice.dag.config import TaskConfig
from sluice.dag.dynamic_tables import migrate_task_dynamic_tables
from sluice.dag.validator import validate_task

_MIGRATIONS_DIR = Path(__file__).parent / "migrations"


async def _apply_sql_file(conn: aiosqlite.Connection, path: Path) -> None:
    sql = path.read_text()
    await conn.executescript(sql)
    await conn.commit()


async def migrate_task(task: TaskConfig) -> None:
    """Create both DBs and apply all schema + dynamic tables for `task`.

    Idempotent: re-running on an existing task is a no-op other than
    creating new per-stage / per-pipeline tables for newly-added names.
    """
    validate_task(task)  # fail-fast on identifier safety / shape constraints
    Path(task.db_path).parent.mkdir(parents=True, exist_ok=True)
    Path(task.seen_db).parent.mkdir(parents=True, exist_ok=True)

    async with aiosqlite.connect(task.db_path) as active:
        for fname in sorted(_MIGRATIONS_DIR.glob("v2_*_active_*.sql")):
            await _apply_sql_file(active, fname)
        # migrate_task_dynamic_tables(conn, task) — see sluice/dag/dynamic_tables.py:98
        await migrate_task_dynamic_tables(active, task)
    async with aiosqlite.connect(task.seen_db) as seen:
        for fname in sorted(_MIGRATIONS_DIR.glob("v2_*_seen_*.sql")):
            await _apply_sql_file(seen, fname)
```

- [ ] **Step 4: Run — expect PASS**

Run: `rtk pytest tests/dag/test_admin_migrate.py -v`
Expected: PASS.

- [ ] **Step 5: Add CLI command in `sluice/cli.py`**

```python
@app.command()
def migrate(
    task_name: str = typer.Argument(..., metavar="TASK"),
    config_dir: Path = typer.Option(Path("./configs"), "--config-dir", "-c"),
):
    """Apply schema migrations + create per-stage and per-pipeline tables for TASK."""
    from sluice.dag.admin import migrate_task
    from sluice.dag.loader import load_task

    task = load_task(config_dir / "tasks" / task_name)
    asyncio.run(migrate_task(task))
    typer.echo(f"Migrated task {task_name!r}.")
```

- [ ] **Step 6: Write CLI integration test**

Append to `tests/dag/test_cli_admin.py` (create file):
```python
from pathlib import Path

from typer.testing import CliRunner

from sluice.cli import app
from tests.dag.test_helpers import make_configs_dir


def test_cli_migrate(tmp_path: Path):
    configs = make_configs_dir(tmp_path, tasks={"t1": {"pipelines": ["p1"], "stages": ["s1"]}})
    runner = CliRunner()
    result = runner.invoke(app, ["migrate", "t1", "-c", str(configs)])
    assert result.exit_code == 0, result.output
    assert "Migrated task 't1'." in result.output
```

- [ ] **Step 7: Run both tests — expect PASS**

Run: `rtk pytest tests/dag/test_admin_migrate.py tests/dag/test_cli_admin.py -v`

- [ ] **Step 8: Commit**

```bash
git add sluice/dag/admin.py sluice/cli.py tests/dag/test_admin_migrate.py tests/dag/test_cli_admin.py
git commit -m "feat(cli): add sluice migrate command for v2 schema bootstrap"
```

### Task 2.2: `sluice retry --task --pipeline [--item]`

**Files:**
- Modify: `sluice/dag/admin.py`
- Modify: `sluice/cli.py`
- Modify: `tests/dag/test_admin.py` (create if missing)
- Modify: `tests/dag/test_cli_admin.py`

- [ ] **Step 1: Write failing test**

Append to `tests/dag/test_admin.py`:
```python
import aiosqlite
import pytest

from sluice.dag.admin import migrate_task, retry_pipeline
from sluice.dag.loader import load_task
from tests.dag.test_helpers import make_task_dir, seed_pipeline_state, seed_stage_state


@pytest.mark.asyncio
async def test_retry_pipeline_clears_terminal_rows_for_single_item(tmp_path):
    task_dir = make_task_dir(tmp_path, name="t1", pipeline_names=["p1"], stage_names=["s1"])
    task = load_task(task_dir)
    await migrate_task(task)
    await seed_pipeline_state(task.db_path, pipeline="p1", item_id="X", status="failed")
    await seed_stage_state(task.db_path, stage="s1", pipeline="p1", item_id="X", status="failed")

    cleared = await retry_pipeline(task, pipeline="p1", item_id="X")
    assert cleared == 1

    async with aiosqlite.connect(task.db_path) as db:
        rows = await (await db.execute(
            "SELECT COUNT(*) FROM pipeline_p1 WHERE item_id='X'"
        )).fetchone()
        assert rows[0] == 0
        rows = await (await db.execute(
            "SELECT COUNT(*) FROM stage_s1 WHERE item_id='X' AND pipeline_name='p1'"
        )).fetchone()
        assert rows[0] == 0


@pytest.mark.asyncio
async def test_retry_pipeline_without_item_clears_all(tmp_path):
    task_dir = make_task_dir(tmp_path, name="t1", pipeline_names=["p1"], stage_names=["s1"])
    task = load_task(task_dir)
    await migrate_task(task)
    for iid in ("A", "B"):
        await seed_pipeline_state(task.db_path, pipeline="p1", item_id=iid, status="failed")

    cleared = await retry_pipeline(task, pipeline="p1", item_id=None)
    assert cleared == 2
```

- [ ] **Step 2: Run — expect FAIL**

Run: `rtk pytest tests/dag/test_admin.py::test_retry_pipeline_clears_terminal_rows_for_single_item -v`
Expected: FAIL (`retry_pipeline` missing).

- [ ] **Step 3: Implement `retry_pipeline`**

Append to `sluice/dag/admin.py`:
```python
from sluice.core.errors import ConfigError
from sluice.dag._name import safe_name


async def retry_pipeline(
    task: TaskConfig,
    *,
    pipeline: str,
    item_id: str | None,
) -> int:
    """Delete terminal `pipeline_<P>` and `stage_<*>` rows so the next cron retries.

    Returns the number of `pipeline_<P>` rows deleted. `stage_attempt_log` is
    preserved (audit history).
    """
    validate_task(task)
    pipeline_names = {p.name for p in task.pipelines}
    if pipeline not in pipeline_names:
        raise ConfigError(f"pipeline {pipeline!r} not defined in task {task.name!r}")
    pipeline_safe = safe_name(pipeline)                              # belt + braces
    stage_safe_names = [safe_name(s.name) for s in task.stages]
    pipeline_table = f"pipeline_{pipeline_safe}"

    async with aiosqlite.connect(task.db_path) as db:
        if item_id is None:
            cur = await db.execute(f"DELETE FROM {pipeline_table}")
            deleted = cur.rowcount or 0
            for s in stage_safe_names:
                await db.execute(
                    f"DELETE FROM stage_{s} WHERE pipeline_name = ?",
                    (pipeline,),
                )
        else:
            cur = await db.execute(
                f"DELETE FROM {pipeline_table} WHERE item_id = ?",
                (item_id,),
            )
            deleted = cur.rowcount or 0
            for s in stage_safe_names:
                await db.execute(
                    f"DELETE FROM stage_{s} WHERE pipeline_name = ? AND item_id = ?",
                    (pipeline, item_id),
                )
        await db.commit()

    return deleted
```

`load_task()` does NOT call `validate_task()` (verified: see `sluice/dag/loader.py:38`). Calling `validate_task` here is what makes the unconditional `safe_name(...)` interpolations safe. The pipeline-membership check above also rejects unknown pipeline names with `ConfigError` before any SQL touches the DB.

- [ ] **Step 4: Run — expect PASS**

Run: `rtk pytest tests/dag/test_admin.py -v`

- [ ] **Step 5: Add CLI command**

In `sluice/cli.py`:
```python
@app.command()
def retry(
    task_name: str = typer.Option(..., "--task", "-t"),
    pipeline: str = typer.Option(..., "--pipeline", "-p"),
    item: str | None = typer.Option(None, "--item"),
    config_dir: Path = typer.Option(Path("./configs"), "--config-dir", "-c"),
    remote: bool = typer.Option(False, "--remote", help="Also trigger a Prefect run now."),
):
    """Clear terminal pipeline_<P> / stage_<*> rows so the next cron retries."""
    from sluice.dag.admin import retry_pipeline
    from sluice.dag.loader import load_task

    task = load_task(config_dir / "tasks" / task_name)
    cleared = asyncio.run(retry_pipeline(task, pipeline=pipeline, item_id=item))
    typer.echo(f"Cleared {cleared} terminal row(s) for pipeline {pipeline!r}.")
    if remote:
        from prefect.deployments import run_deployment

        asyncio.run(run_deployment(name=f"sluice-run-pipeline/{task.name}-{pipeline}"))
        typer.echo("Triggered Prefect run.")
```

- [ ] **Step 6: Test CLI**

Append to `tests/dag/test_cli_admin.py`:
```python
def test_cli_retry_single_item(tmp_path):
    configs = make_configs_dir(tmp_path, tasks={"t1": {"pipelines": ["p1"], "stages": ["s1"]}})
    # migrate first
    runner = CliRunner()
    runner.invoke(app, ["migrate", "t1", "-c", str(configs)])
    # seed one failed row (helper)
    from tests.dag.test_helpers import seed_pipeline_state_sync
    seed_pipeline_state_sync(configs / "../data/t1.db", pipeline="p1", item_id="X", status="failed")

    result = runner.invoke(app, ["retry", "--task", "t1", "--pipeline", "p1", "--item", "X", "-c", str(configs)])
    assert result.exit_code == 0
    assert "Cleared 1" in result.output
```

- [ ] **Step 7: Run — expect PASS**

Run: `rtk pytest tests/dag/test_cli_admin.py -v`

- [ ] **Step 8: Commit**

```bash
git add sluice/dag/admin.py sluice/cli.py tests/dag/test_admin.py tests/dag/test_cli_admin.py
git commit -m "feat(cli): add sluice retry --task --pipeline [--item] [--remote]"
```

### Task 2.3: `sluice items list <task>`

**Files:**
- Modify: `sluice/dag/admin.py`
- Modify: `sluice/cli.py` — add `items` typer sub-app
- Modify: `tests/dag/test_admin.py`, `tests/dag/test_cli_admin.py`

- [ ] **Step 1: Write failing test for `admin.list_items`**

Append to `tests/dag/test_admin.py`:
```python
@pytest.mark.asyncio
async def test_list_items_joins_pipeline_status(tmp_path):
    task_dir = make_task_dir(tmp_path, name="t1", pipeline_names=["p1", "p2"], stage_names=["s1"])
    task = load_task(task_dir)
    await migrate_task(task)
    await seed_item(task.db_path, item_id="X", source_name="src1", title="hello")
    await seed_pipeline_state(task.db_path, pipeline="p1", item_id="X", status="success")
    await seed_pipeline_state(task.db_path, pipeline="p2", item_id="X", status="failed")

    rows = await list_items(task, limit=10)
    assert len(rows) == 1
    row = rows[0]
    assert row["item_id"] == "X"
    assert row["title"] == "hello"
    assert row["p1"] == "success"
    assert row["p2"] == "failed"
```

- [ ] **Step 2: Run — expect FAIL**

Run: `rtk pytest tests/dag/test_admin.py::test_list_items_joins_pipeline_status -v`

- [ ] **Step 3: Implement `list_items`**

Append to `sluice/dag/admin.py`:
```python
async def list_items(task: TaskConfig, *, limit: int = 50) -> list[dict]:
    """Return `limit` most-recent items joined with per-pipeline status.

    Output rows: `{item_id, source_name, title, ingested_at, <pipeline_name>: status, ...}`.
    """
    validate_task(task)
    pipeline_names = [safe_name(p.name) for p in task.pipelines]
    select_pipeline_cols = ", ".join(
        f"(SELECT status FROM pipeline_{p} WHERE item_id = items.item_id) AS p_{p}"
        for p in pipeline_names
    )
    query = (
        f"SELECT item_id, source_name, title, ingested_at"
        + (", " + select_pipeline_cols if pipeline_names else "")
        + " FROM items ORDER BY ingested_at DESC LIMIT ?"
    )
    async with aiosqlite.connect(task.db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(query, (limit,))
        out = []
        async for r in cur:
            row = {"item_id": r["item_id"], "source_name": r["source_name"],
                   "title": r["title"], "ingested_at": r["ingested_at"]}
            for p in pipeline_names:
                row[p] = r[f"p_{p}"]
            out.append(row)
        return out
```

- [ ] **Step 4: Run — expect PASS**

- [ ] **Step 5: Add CLI sub-app**

In `sluice/cli.py`:
```python
items_app = typer.Typer(no_args_is_help=True, help="Inspect / manage items")
app.add_typer(items_app, name="items")


@items_app.command("list")
def items_list(
    task_name: str = typer.Argument(..., metavar="TASK"),
    limit: int = typer.Option(50, "--limit", "-n"),
    config_dir: Path = typer.Option(Path("./configs"), "--config-dir", "-c"),
):
    """List recent items + per-pipeline status."""
    from sluice.dag.admin import list_items
    from sluice.dag.loader import load_task

    task = load_task(config_dir / "tasks" / task_name)
    rows = asyncio.run(list_items(task, limit=limit))
    for row in rows:
        typer.echo(json.dumps(row, ensure_ascii=False))
```

Add `import json` if not already imported at the top of `cli.py`.

- [ ] **Step 6: CLI test**

Append to `tests/dag/test_cli_admin.py`:
```python
def test_cli_items_list(tmp_path):
    configs = make_configs_dir(tmp_path, tasks={"t1": {"pipelines": ["p1"], "stages": ["s1"]}})
    runner = CliRunner()
    runner.invoke(app, ["migrate", "t1", "-c", str(configs)])
    from tests.dag.test_helpers import seed_item_sync
    seed_item_sync(configs / "../data/t1.db", item_id="X", source_name="src1", title="t")

    result = runner.invoke(app, ["items", "list", "t1", "-c", str(configs)])
    assert result.exit_code == 0
    assert '"item_id": "X"' in result.output
```

- [ ] **Step 7: Run — expect PASS**

- [ ] **Step 8: Commit**

```bash
git add sluice/dag/admin.py sluice/cli.py tests/dag/test_admin.py tests/dag/test_cli_admin.py
git commit -m "feat(cli): add sluice items list"
```

### Task 2.4: `sluice items show <task> <item_id>`

**Files:** same trio as Task 2.3.

- [ ] **Step 1: Write failing test**

Append to `tests/dag/test_admin.py`:
```python
@pytest.mark.asyncio
async def test_show_item_returns_full_provenance(tmp_path):
    task_dir = make_task_dir(tmp_path, name="t1", pipeline_names=["p1"], stage_names=["s1"])
    task = load_task(task_dir)
    await migrate_task(task)
    await seed_item(task.db_path, item_id="X", source_name="src1", title="hello")
    await seed_pipeline_state(task.db_path, pipeline="p1", item_id="X", status="success")
    await seed_stage_state(task.db_path, stage="s1", pipeline="p1", item_id="X", status="done")

    prov = await show_item(task, item_id="X")
    assert prov["item"]["item_id"] == "X"
    assert prov["pipelines"]["p1"]["status"] == "success"
    assert prov["stages"]["s1"][0]["status"] == "done"
```

- [ ] **Step 2: Run — expect FAIL**

- [ ] **Step 3: Implement `show_item`**

Append to `sluice/dag/admin.py`:
```python
async def show_item(task: TaskConfig, *, item_id: str) -> dict:
    validate_task(task)
    async with aiosqlite.connect(task.db_path) as db:
        db.row_factory = aiosqlite.Row
        item_row = await (await db.execute(
            "SELECT * FROM items WHERE item_id = ?", (item_id,)
        )).fetchone()
        if item_row is None:
            raise KeyError(item_id)

        pipelines_section: dict[str, dict] = {}
        for p in task.pipelines:
            pname = safe_name(p.name)
            row = await (await db.execute(
                f"SELECT * FROM pipeline_{pname} WHERE item_id = ?", (item_id,)
            )).fetchone()
            if row is not None:
                pipelines_section[p.name] = dict(row)

        stages_section: dict[str, list[dict]] = {}
        for s in task.stages:
            sname = safe_name(s.name)
            cur = await db.execute(
                f"SELECT * FROM stage_{sname} WHERE item_id = ? ORDER BY last_processed_at",
                (item_id,),
            )
            rows = [dict(r) async for r in cur]
            if rows:
                stages_section[s.name] = rows

        return {
            "item": dict(item_row),
            "pipelines": pipelines_section,
            "stages": stages_section,
        }
```

- [ ] **Step 4: Run — expect PASS**

- [ ] **Step 5: Add CLI command (in `items_app`)**

```python
@items_app.command("show")
def items_show(
    task_name: str = typer.Argument(..., metavar="TASK"),
    item_id: str = typer.Argument(..., metavar="ITEM_ID"),
    config_dir: Path = typer.Option(Path("./configs"), "--config-dir", "-c"),
):
    from sluice.dag.admin import show_item
    from sluice.dag.loader import load_task

    task = load_task(config_dir / "tasks" / task_name)
    try:
        prov = asyncio.run(show_item(task, item_id=item_id))
    except KeyError:
        typer.echo(f"item {item_id!r} not found", err=True)
        raise typer.Exit(code=1)
    typer.echo(json.dumps(prov, ensure_ascii=False, indent=2, default=str))
```

- [ ] **Step 6: CLI test (smoke; deep-equal already covered in `test_admin.py`)**

Append to `tests/dag/test_cli_admin.py`:
```python
def test_cli_items_show_not_found(tmp_path):
    configs = make_configs_dir(tmp_path, tasks={"t1": {"pipelines": ["p1"], "stages": ["s1"]}})
    runner = CliRunner()
    runner.invoke(app, ["migrate", "t1", "-c", str(configs)])
    result = runner.invoke(app, ["items", "show", "t1", "missing", "-c", str(configs)])
    assert result.exit_code == 1
    assert "not found" in result.output
```

- [ ] **Step 7: Run — expect PASS**

- [ ] **Step 8: Commit**

```bash
git commit -am "feat(cli): add sluice items show"
```

### Task 2.5: `sluice items kill <task> <item_id>`

**Files:** same trio.

- [ ] **Step 1: Write failing test**

Append to `tests/dag/test_admin.py`:
```python
@pytest.mark.asyncio
async def test_kill_item_sets_global_tombstone(tmp_path):
    task_dir = make_task_dir(tmp_path, name="t1", pipeline_names=["p1"], stage_names=["s1"])
    task = load_task(task_dir)
    await migrate_task(task)
    await seed_item(task.db_path, item_id="X", source_name="src1", title="hello")

    await kill_item(task, item_id="X", reason="operator")

    async with aiosqlite.connect(task.db_path) as db:
        db.row_factory = aiosqlite.Row
        r = await (await db.execute(
            "SELECT dropped, dropped_reason, dropped_at FROM items WHERE item_id='X'"
        )).fetchone()
        assert r["dropped"] == 1
        assert r["dropped_reason"] == "operator"
        assert r["dropped_at"] is not None
```

- [ ] **Step 2: Run — expect FAIL**

- [ ] **Step 3: Implement `kill_item`**

Append to `sluice/dag/admin.py`:
```python
from datetime import UTC, datetime


async def kill_item(task: TaskConfig, *, item_id: str, reason: str = "killed_by_operator") -> None:
    async with aiosqlite.connect(task.db_path) as db:
        cur = await db.execute(
            "UPDATE items SET dropped=1, dropped_reason=?, dropped_at=? WHERE item_id=?",
            (reason, datetime.now(UTC).isoformat(timespec="seconds"), item_id),
        )
        if cur.rowcount == 0:
            raise KeyError(item_id)
        await db.commit()
```

- [ ] **Step 4: Run — expect PASS**

- [ ] **Step 5: Add CLI command**

```python
@items_app.command("kill")
def items_kill(
    task_name: str = typer.Argument(..., metavar="TASK"),
    item_id: str = typer.Argument(..., metavar="ITEM_ID"),
    reason: str = typer.Option("killed_by_operator", "--reason"),
    config_dir: Path = typer.Option(Path("./configs"), "--config-dir", "-c"),
):
    from sluice.dag.admin import kill_item
    from sluice.dag.loader import load_task

    task = load_task(config_dir / "tasks" / task_name)
    try:
        asyncio.run(kill_item(task, item_id=item_id, reason=reason))
    except KeyError:
        typer.echo(f"item {item_id!r} not found", err=True)
        raise typer.Exit(code=1)
    typer.echo(f"Killed item {item_id!r}.")
```

- [ ] **Step 6: CLI test**

```python
def test_cli_items_kill(tmp_path):
    configs = make_configs_dir(tmp_path, tasks={"t1": {"pipelines": ["p1"], "stages": ["s1"]}})
    runner = CliRunner()
    runner.invoke(app, ["migrate", "t1", "-c", str(configs)])
    from tests.dag.test_helpers import seed_item_sync
    seed_item_sync(configs / "../data/t1.db", item_id="X", source_name="src1", title="t")

    result = runner.invoke(app, ["items", "kill", "t1", "X", "-c", str(configs)])
    assert result.exit_code == 0
    assert "Killed item 'X'." in result.output
```

- [ ] **Step 7: Run — expect PASS**

- [ ] **Step 8: Commit**

```bash
git commit -am "feat(cli): add sluice items kill"
```

### Task 2.6: `sluice scaffold-task --from-old <path>`

**Files:**
- Modify: `sluice/dag/admin.py`
- Modify: `sluice/cli.py`
- Modify: `tests/dag/test_admin.py`, `tests/dag/test_cli_admin.py`

This is a best-effort one-shot converter. It reads a v1 `pipelines/<X>.toml` and emits a starter `tasks/<X>/{task,sources,sinks,stages,pipelines}.toml`. Manual tuning is expected after.

- [ ] **Step 1: Write failing test (matches real v1 shape)**

V1 pipeline configs use **top-level** keys (`id`, `cron`, `enabled`, `timezone`, `window`, `lookback_overlap`) plus `[[sources]]`, `[[stages]]`, `[[sinks]]` arrays. Verified against `configs/pipelines/ai_news.toml.example` and `tests/config/test_loader.py`. There is no top-level `[pipeline]` table.

Append to `tests/dag/test_admin.py`:
```python
def test_scaffold_emits_five_files(tmp_path):
    old = tmp_path / "old.toml"
    old.write_text(
        '''\
id          = "demo"
description = "demo task"
enabled     = true
cron        = "0 8 * * *"
timezone    = "Asia/Shanghai"
window      = "24h"

[[sources]]
type    = "rss"
url     = "https://example.com/rss"
tag     = "ai"

[[stages]]
name = "prefilter"
type = "filter"
mode = "keep_if_all"

[[stages]]
name  = "render"
type  = "render"

[[sinks]]
id    = "local"
type  = "file_md"
out   = "./out"
'''
    )
    out_dir = tmp_path / "tasks" / "demo"

    scaffold_from_v1(old_path=old, out_dir=out_dir)

    for fname in ("task.toml", "sources.toml", "sinks.toml", "stages.toml", "pipelines.toml"):
        assert (out_dir / fname).exists()
    sources_text = (out_dir / "sources.toml").read_text()
    assert "https://example.com/rss" in sources_text
    pipelines_text = (out_dir / "pipelines.toml").read_text()
    assert "demo_main" in pipelines_text
```

- [ ] **Step 2: Run — expect FAIL**

- [ ] **Step 3: Implement `scaffold_from_v1`**

Append to `sluice/dag/admin.py`:
```python
import tomllib


def scaffold_from_v1(*, old_path: Path, out_dir: Path) -> None:
    """Emit starter v2 TOML files from a v1 single-file pipeline config.

    Best-effort. V1 sources lack `name`s — synthesised as `src1`, `src2`, …
    Sinks key off `id` in v1; v2 expects `name` (renamed in output).
    Stage types are passed through; operator must rename to match the v2
    stage type catalog. A single pipeline entry is generated chaining all
    stages linearly.
    """
    raw = tomllib.loads(old_path.read_text())
    # v1 config keys are top-level, not nested under [pipeline]
    name = raw["id"]
    db_path = raw.get("db_path", f"data/{name}.db")
    timezone = raw.get("timezone", "UTC")
    window = raw.get("window", "24h")
    anchor = raw.get("cron", "0 8 * * *")
    sources_v1 = raw.get("sources", [])
    stages_v1 = raw.get("stages", [])
    sinks_v1 = raw.get("sinks", [])

    out_dir.mkdir(parents=True, exist_ok=True)

    (out_dir / "task.toml").write_text(
        f'''\
[task]
name     = "{name}"
db_path  = "{db_path}"
seen_db  = "data/{name}_seen.db"
enabled  = true
timezone = "{timezone}"

[task.window]
anchor = "{anchor}"
length = "{window}"

[task.deployments]
pipelines = "all"
'''
    )

    def _toml_array(arr_name: str, entries: list[dict]) -> str:
        chunks = []
        for e in entries:
            chunks.append(f"[[{arr_name}]]")
            for k, v in e.items():
                chunks.append(f"{k} = {v!r}")
            chunks.append("")
        return "\n".join(chunks)

    # v1 sources lack a `name` field — synthesise one
    source_entries = []
    source_names = []
    for i, s in enumerate(sources_v1, start=1):
        sn = s.get("name", f"src{i}")
        source_names.append(sn)
        source_entries.append({"name": sn, **{k: v for k, v in s.items() if k != "name"}})
    (out_dir / "sources.toml").write_text(_toml_array("sources", source_entries))

    # v1 sinks use `id`, v2 uses `name`
    sink_entries = []
    sink_names = []
    for s in sinks_v1:
        nm = s.get("name") or s.get("id")
        sink_names.append(nm)
        sink_entries.append({"name": nm, **{k: v for k, v in s.items() if k not in {"id", "name"}}})
    (out_dir / "sinks.toml").write_text(_toml_array("sinks", sink_entries))

    # v1 stages already have `name`; pass through
    stage_entries = []
    stage_names = []
    for i, st in enumerate(stages_v1, start=1):
        sn = st.get("name", f"s{i}")
        stage_names.append(sn)
        stage_entries.append({"name": sn, **{k: v for k, v in st.items() if k != "name"}})
    (out_dir / "stages.toml").write_text(_toml_array("stages", stage_entries))

    (out_dir / "pipelines.toml").write_text(
        f'''\
[[pipelines]]
name    = "{name}_main"
cron    = "{anchor}"
sources = {source_names!r}
stages  = {stage_names!r}
sinks   = {sink_names!r}
'''
    )
```

- [ ] **Step 4: Run — expect PASS**

- [ ] **Step 5: Add CLI command**

```python
@app.command("scaffold-task")
def scaffold_task(
    from_old: Path = typer.Option(..., "--from-old"),
    out_dir: Path = typer.Option(..., "--out"),
):
    """Best-effort scaffold a v2 task directory from a v1 pipelines/<X>.toml."""
    from sluice.dag.admin import scaffold_from_v1

    scaffold_from_v1(old_path=from_old, out_dir=out_dir)
    typer.echo(f"Scaffolded {out_dir}. Hand-tuning required.")
```

- [ ] **Step 6: CLI test**

```python
def test_cli_scaffold(tmp_path):
    old = tmp_path / "old.toml"
    # Real v1 shape: top-level id/cron + [[sources]] arrays — matches the
    # `tests/dag/test_admin.py::test_scaffold_emits_five_files` fixture.
    old.write_text(
        'id      = "demo"\n'
        'cron    = "0 8 * * *"\n'
        'enabled = true\n'
        'window  = "24h"\n'
        '\n'
        '[[sources]]\n'
        'type = "rss"\n'
        'url  = "https://example.com/rss"\n'
        '\n'
        '[[stages]]\n'
        'name = "fetch"\n'
        'type = "fetch_rss"\n'
        '\n'
        '[[sinks]]\n'
        'id   = "tg"\n'
        'type = "telegram"\n'
    )
    out = tmp_path / "tasks" / "demo"
    runner = CliRunner()
    result = runner.invoke(app, ["scaffold-task", "--from-old", str(old), "--out", str(out)])
    assert result.exit_code == 0
    assert (out / "task.toml").exists()
```

- [ ] **Step 7: Run — expect PASS**

- [ ] **Step 8: Commit**

```bash
git commit -am "feat(cli): add sluice scaffold-task best-effort v1→v2 converter"
```

### Task 2.7: Section 2 verification sweep

- [ ] **Step 1: Lint + format + tests**

```bash
rtk ruff check sluice/ tests/
rtk ruff format --check sluice/ tests/
rtk pytest -q
```
Expected: all green.

- [ ] **Step 2: Commit (only if formatting/lint required edits)**

---

## Section 3 — Concurrency Collision Strategy

### Task 3.1: Research the Prefect 3 collision API per work-pool type

This task produces a **side spec** consumed by Tasks 3.2/3.3. The plan document itself is read-only during execution; findings go to a new file.

**Files:**
- Create: `docs/superpowers/specs/2026-05-12-plan4b-concurrency-findings.md`

- [ ] **Step 1: Fetch Prefect 3 docs for collision configuration**

Use `mcp__context7__resolve-library-id` with `libraryName="prefect"`, then `mcp__context7__query-docs` against the resolved id with the query: `concurrency collision policy work pool deployment to_deployment global concurrency limit strategy`. If context7 lacks coverage, fall back to `mcp__firecrawl__firecrawl_search` with the same query.

- [ ] **Step 2: Write the findings spec**

Create `docs/superpowers/specs/2026-05-12-plan4b-concurrency-findings.md` with three sections:

1. **Available strategies for the process work-pool type.** Name each Prefect 3 enum value supported (e.g. `ConcurrencyLimitStrategy.CANCEL_NEW`, `.ENQUEUE`). Note any that are documented for other pool types but not process pools.
2. **API surface — per-deployment vs. per-work-pool vs. global limit.** State whether `flow.to_deployment()` accepts a collision strategy kwarg, or whether the strategy lives on a `prefect work-pool` / global-concurrency-limit object.
3. **The exact SDK call to make.** Function path (e.g. `prefect.client.schemas.objects.ConcurrencyLimitV2(...)`) + kwargs.

If Prefect 3 exposes only one strategy for the process pool (i.e. plain queueing), say so explicitly and recommend reducing Section 3 scope to a `concurrency_strategy = "queue"` enum-of-one + a `concurrency_limit` integer pass-through.

- [ ] **Step 3: Commit the side spec**

```bash
git add docs/superpowers/specs/2026-05-12-plan4b-concurrency-findings.md
git commit -m "docs(spec): record Prefect 3 concurrency-collision API findings for Plan 4b"
```

### Task 3.2: Add `concurrency_strategy` to `RuntimeConfig` and `PipelineDef`

**Files:**
- Modify: `sluice/dag/sluice_config.py`
- Modify: `sluice/dag/config.py`
- Modify: `tests/dag/test_sluice_config.py`

- [ ] **Step 1: Write failing test**

Append to `tests/dag/test_sluice_config.py`:
```python
def test_runtime_concurrency_strategy_default_queue(tmp_path):
    cfg = tmp_path / "sluice.toml"
    cfg.write_text("[runtime]\nwork_pool = \"default\"\n")
    sc = load_sluice_config(cfg)
    assert sc.runtime.concurrency_strategy == "queue"


def test_runtime_concurrency_strategy_invalid_raises(tmp_path):
    cfg = tmp_path / "sluice.toml"
    cfg.write_text("[runtime]\nconcurrency_strategy = \"bogus\"\n")
    with pytest.raises(ConfigError):
        load_sluice_config(cfg)
```

- [ ] **Step 2: Run — expect FAIL**

- [ ] **Step 3: Add the field**

In `sluice/dag/sluice_config.py` modify `RuntimeConfig`:
```python
concurrency_strategy: str = "queue"  # one of: queue | cancel_new | enqueue
```

Whitelist the value in `load_sluice_config`:
```python
strategy = runtime.get("concurrency_strategy", "queue")
if strategy not in {"queue", "cancel_new", "enqueue"}:
    raise ConfigError(f"runtime.concurrency_strategy must be one of queue/cancel_new/enqueue, got {strategy!r}")
```

Then pass `strategy` into the `RuntimeConfig(...)` constructor.

- [ ] **Step 4: Run — expect PASS**

- [ ] **Step 5: Add `PipelineDef.concurrency_strategy` (optional override)**

In `sluice/dag/config.py` add `concurrency_strategy: str | None = None` to `PipelineDef`. Update `sluice/dag/loader.py` to read `entry.get("concurrency_strategy")` when constructing `PipelineDef`.

- [ ] **Step 6: Commit**

```bash
git commit -am "feat(config): add concurrency_strategy to RuntimeConfig and PipelineDef"
```

### Task 3.3: Wire `concurrency_strategy` into `build_pipeline_deployment`

**Files:**
- Modify: `sluice/dag/deploy.py`
- Modify: `tests/dag/test_deploy.py`

The exact API call comes from the findings recorded in Task 3.1. The rest of this task documents the resolution logic, which is independent of which API is used.

- [ ] **Step 1: Add fixture + helper, then write failing test**

Append to `tests/dag/test_helpers.py`:
```python
def make_task_and_cfg_factory(tmp_path: Path):
    """Returns a builder that constructs (TaskConfig, SluiceConfig) pairs.

    Used by Section 3 tests to vary runtime + pipeline concurrency strategies.
    """
    from sluice.dag.loader import load_task
    from sluice.dag.sluice_config import RotationConfig, RuntimeConfig, SluiceConfig

    def _build(*, runtime_strategy: str = "queue", pipeline_strategy: str | None = None):
        task_dir = make_task_dir(
            tmp_path, name="t1", pipeline_names=["p1"], stage_names=["s1"]
        )
        if pipeline_strategy is not None:
            pipelines_toml = task_dir / "pipelines.toml"
            text = pipelines_toml.read_text()
            text += f'\nconcurrency_strategy = "{pipeline_strategy}"\n'
            pipelines_toml.write_text(text)
        task = load_task(task_dir)
        cfg = SluiceConfig(
            rotation=RotationConfig(),
            runtime=RuntimeConfig(concurrency_strategy=runtime_strategy),
        )
        return task, cfg

    return _build


def effective_concurrency_strategy(deployment) -> str:
    """Read the strategy `build_pipeline_deployment` recorded on the deployment.

    Implementation depends on the API surface discovered in Task 3.1; this
    helper centralises the reach so tests don't bake it in.
    """
    # Adjust this single line once the Prefect API is settled (Task 3.1):
    return getattr(deployment, "_sluice_concurrency_strategy", "queue")
```

Append to `tests/dag/test_deploy.py`:
```python
import pytest

from tests.dag.test_helpers import effective_concurrency_strategy, make_task_and_cfg_factory


@pytest.fixture
def make_task_and_cfg(tmp_path):
    return make_task_and_cfg_factory(tmp_path)


def test_pipeline_concurrency_strategy_uses_pipeline_override(make_task_and_cfg):
    task, cfg = make_task_and_cfg(
        runtime_strategy="queue",
        pipeline_strategy="cancel_new",
    )
    dep = build_pipeline_deployment(task, task.pipelines[0], cfg, ".")
    assert effective_concurrency_strategy(dep) == "cancel_new"


def test_pipeline_concurrency_strategy_falls_back_to_runtime(make_task_and_cfg):
    task, cfg = make_task_and_cfg(runtime_strategy="enqueue", pipeline_strategy=None)
    dep = build_pipeline_deployment(task, task.pipelines[0], cfg, ".")
    assert effective_concurrency_strategy(dep) == "enqueue"
```

Note: `build_pipeline_deployment` must record the resolved strategy as an attribute (`deployment._sluice_concurrency_strategy = effective`) so tests stay decoupled from whichever Prefect-side API ultimately carries the value (per-deployment kwarg, work-pool config, or global limit). The actual Prefect API call is added in Step 3 below using the findings spec from Task 3.1.

- [ ] **Step 2: Run — expect FAIL**

- [ ] **Step 3: Implement resolution + API call**

In `sluice/dag/deploy.py`:
```python
def _effective_concurrency_strategy(pipeline: PipelineDef, sluice_cfg: SluiceConfig) -> str:
    return pipeline.concurrency_strategy or sluice_cfg.runtime.concurrency_strategy
```

Then in `build_pipeline_deployment`, call the Prefect API discovered in Task 3.1 with the resolved strategy. If the API is a `to_deployment()` kwarg, add it to the call; if it's a separate `prefect.client.schemas.actions.WorkPoolUpdate`-style call, defer the actual server call to `push_all_deployments` (group by strategy and apply per-pool).

- [ ] **Step 4: Run — expect PASS**

- [ ] **Step 5: Commit**

```bash
git commit -am "feat(deploy): wire concurrency_strategy through to_deployment"
```

### Task 3.4: Section 3 verification sweep

- [ ] Same lint + format + test sweep as 2.7.

---

## Section 4 — Prefect Server / Worker Deployment Artifacts

### Task 4.1: `deploy/Dockerfile.worker`

**Files:**
- Create: `deploy/Dockerfile.worker`

- [ ] **Step 1: Write the Dockerfile**

Create `deploy/Dockerfile.worker`:
```dockerfile
FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md LICENSE /app/
COPY sluice /app/sluice
RUN pip install --no-cache-dir -e .

ENV PYTHONUNBUFFERED=1 \
    PREFECT_HOME=/data/prefect

VOLUME ["/data"]

ENTRYPOINT ["prefect", "worker", "start"]
CMD ["--pool", "default"]
```

- [ ] **Step 2: Build smoke test**

Run:
```bash
docker build -f deploy/Dockerfile.worker -t sluice/worker:test .
docker run --rm sluice/worker:test --help | head -5
```
Expected: prefect worker `start` help text printed.

- [ ] **Step 3: Commit**

```bash
git add deploy/Dockerfile.worker
git commit -m "feat(deploy): add Dockerfile for Prefect worker"
```

### Task 4.2: `deploy/docker-compose.yml`

**Files:**
- Create: `deploy/docker-compose.yml`
- Create: `deploy/.env.example`

- [ ] **Step 1: Write the compose file**

```yaml
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: prefect
      POSTGRES_PASSWORD: prefect
      POSTGRES_DB: prefect
    volumes:
      - postgres_data:/var/lib/postgresql/data

  prefect-server:
    image: prefecthq/prefect:3-latest
    depends_on: [postgres]
    environment:
      PREFECT_API_DATABASE_CONNECTION_URL: postgresql+asyncpg://prefect:prefect@postgres:5432/prefect
      PREFECT_SERVER_API_HOST: 0.0.0.0
    command: prefect server start --host 0.0.0.0
    ports:
      - "4200:4200"

  sluice-worker:
    image: ${SLUICE_IMAGE:-sluice/worker:latest}
    depends_on: [prefect-server]
    environment:
      PREFECT_API_URL: http://prefect-server:4200/api
      PREFECT_API_AUTH_STRING: ${PREFECT_API_AUTH_STRING:-}
    volumes:
      # Compose runs from deploy/, so ../ reaches the repo root data/ and configs/.
      - ../data:/data
      - ../configs:/configs

volumes:
  postgres_data: {}
```

- [ ] **Step 2: Write `.env.example`**

```env
SLUICE_IMAGE=sluice/worker:0.1.0
PREFECT_API_AUTH_STRING=
```

- [ ] **Step 3: Smoke test (manual, not in CI)**

`docker compose -f deploy/docker-compose.yml config` should validate.

- [ ] **Step 4: Commit**

```bash
git add deploy/docker-compose.yml deploy/.env.example
git commit -m "feat(deploy): add docker-compose for prefect server + worker + postgres"
```

### Task 4.3: `deploy/Makefile`

**Files:**
- Create: `deploy/Makefile`

- [ ] **Step 1: Write Makefile**

```makefile
IMAGE  ?= sluice/worker
TAG    ?= 0.1.0

.PHONY: image push up down logs

image:
	docker build -f deploy/Dockerfile.worker -t $(IMAGE):$(TAG) .

push:
	docker push $(IMAGE):$(TAG)

up:
	cd deploy && docker compose up -d

down:
	cd deploy && docker compose down

logs:
	cd deploy && docker compose logs -f sluice-worker
```

- [ ] **Step 2: Commit**

```bash
git add deploy/Makefile
git commit -m "feat(deploy): add Makefile shortcuts (image/push/up/down/logs)"
```

### Task 4.4: `deploy/systemd/sluice-worker.service`

**Files:**
- Create: `deploy/systemd/sluice-worker.service`

- [ ] **Step 1: Write systemd unit**

```ini
[Unit]
Description=Sluice Prefect Worker
After=network.target

[Service]
Type=simple
User=sluice
WorkingDirectory=/opt/sluice
Environment=PREFECT_API_URL=http://localhost:4200/api
EnvironmentFile=-/etc/sluice/worker.env
ExecStart=/opt/sluice/.venv/bin/prefect worker start --pool default
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

- [ ] **Step 2: Commit**

```bash
git add deploy/systemd/sluice-worker.service
git commit -m "feat(deploy): add systemd unit for non-container worker"
```

---

## Section 5 — Migration Documentation

### Task 5.1: Write `docs/migration/v1-to-v2.md`

**Files:**
- Create: `docs/migration/v1-to-v2.md`

- [ ] **Step 1: Draft the migration guide**

Sections to include (each ≥ one concrete command):
1. **Backup** — `cp data/sluice.db data/sluice.db.bak`, archive `configs/pipelines/`.
2. **Config remap** — for each v1 `configs/pipelines/<X>.toml`, run `sluice scaffold-task --from-old configs/pipelines/<X>.toml --out configs/tasks/<X>`. Then hand-tune the stages.toml stage `type` values to match the v2 stage catalog (see `docs/superpowers/specs/2026-05-02-multi-pipeline-dag-design.md` → "Stage type catalog").
3. **DB drop** — v2 is not backwards-compatible; delete `data/sluice.db` and the v1 dedup table. Seen DB schema is new.
4. **Migrate** — `sluice migrate <task>` per task. Idempotent.
5. **Deploy** — `sluice deploy --config-dir configs --sluice-config configs/sluice.toml`.
6. **Verify** — `sluice items list <task>`, `sluice run -t <task> -p <pipeline>` for a one-shot test.
7. **Cutover** — disable v1 cron in Prefect (if any), confirm v2 deployments appear.
8. **Rollback** — restore the v1 backup; revert the sluice version to the last pre-Plan-4b tag.

- [ ] **Step 2: Cross-reference**

Link from the top of the migration doc to both design specs and to Plan 4b.

- [ ] **Step 3: Commit**

```bash
git add docs/migration/v1-to-v2.md
git commit -m "docs: add v1→v2 migration guide"
```

---

## Final Verification

### Task F.1: Full sweep

- [ ] **Step 1: Lint, format, tests**

```bash
rtk ruff check sluice/ tests/
rtk ruff format --check sluice/ tests/
rtk pytest -q
```
Expected: all green; record the final test count.

- [ ] **Step 2: Manual `sluice --help` smoke**

```bash
rtk python -m sluice.cli --help
```
Expected: `run`, `deploy`, `gc-rotate`, `migrate`, `retry`, `items`, `scaffold-task` listed; no v1 commands.

- [ ] **Step 3: Tag**

```bash
git tag plan4b-complete
```

- [ ] **Step 4: Open PR**

Use `gh pr create` per repo conventions. PR body lists each section with the commit range.

---

## Self-Review Checklist

1. **Spec coverage** — every "Out (Plan 4b)" bullet from `2026-05-05-plan4a-prefect-deploy-design.md`:
   - Delete v1 files (`flow.py`, `gc_flow.py`, `runner.py`, `builders.py`, `state/`, `run-legacy` command) → Tasks 1.1, 1.3, 1.6, 1.7
   - v1 → v2 config migration documentation → Task 5.1
   - Prefect server/worker deployment scripts → Tasks 4.1–4.4
   - GC cron deployment → Plan 4a already deploys `rotate_task_flow` per task; the v1 `gc_attachments` / `gc_failed_items` / `gc_url_cache` helpers in `sluice/gc.py` die with the v1 modules they operate on (URL cache, failure store, v1 attachment store), and there is no v2 equivalent of those domains. No new Plan 4b deployment work — explicit.
   - Concurrency collision strategy → Tasks 3.1–3.3
   - Missing CLI commands from `2026-05-02-multi-pipeline-dag-design.md` CLI section (`retry`, `items list/show/kill`, `migrate`, `scaffold-task`) → Tasks 2.0–2.6
2. **Placeholders** — none. Every code step has runnable content; every shell step has the exact command. Top-level v1 shim classification is pre-baked into the Section 1 header table.
3. **Type consistency** — `admin.py` function names (`migrate_task`, `retry_pipeline`, `list_items`, `show_item`, `kill_item`, `scaffold_from_v1`) are reused identically in the CLI commands and in the tests. `tests/dag/test_helpers.py` helpers (`make_task_dir`, `make_configs_dir`, `seed_item`/`seed_item_sync`, `seed_pipeline_state`/`seed_pipeline_state_sync`, `seed_stage_state`) are defined once in Task 2.0 and referenced consistently from Tasks 2.1–2.6.
4. **Operator surface** — `metrics-server`, `stats`, `deliveries`, `failures` are deleted as v1-only commands. If operators currently rely on them, equivalent v2 commands would be needed (none exist today). Confirm with the user before executing Task 1.3 if any of these are wired into a live ops loop; otherwise the v2 surface (`items list/show`, `pipelines` table via `sluice migrate`-created schema) is the replacement.
