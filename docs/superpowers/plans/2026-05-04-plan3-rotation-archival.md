# Plan 3 — Rotation, Archival, Seen Reconciliation, GC

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the monthly **drain-and-rotate-with-carryover** flow for per-task active DBs. After Plan 3, `sluice gc-rotate -t <task>` (and the same flow invoked programmatically) will: pause new pipeline runs for the task, drain in-flight runs to a quiescent state, partition items into carryover (still pending work in some reachable pipeline) vs archive (terminal everywhere it owes work), build a fresh active DB containing only carryover state, atomically swap the active file, gzip the old file under `data/archive/<task>-YYYY-MM.db.gz`, and reconcile the seen DB so archived items remain dedup-skipped on future runs.

**Architecture:** Rotation runs as a stand-alone async orchestrator separate from the pipeline writer. It does NOT use `WriterThread` — it works on **closed** SQLite files (the active is closed after drain; the new active is built and closed before the swap). New pipeline runs are gated in two layers: any `WriterThread` holds an advisory **file-lock fast-path** (`RotationLock`) for its lifetime (the CLI acquires the same lock before preflight and passes `hold_rotation_lock=False` to avoid double-acquire), and `run_pipeline` registers through a **SQL admission gate** (`rotation_gate` + `pipeline_admissions` tables) enforced inside `BEGIN IMMEDIATE` before doing any pipeline work. If either layer rejects, the run returns `status='skipped_rotation'`. Carryover copy is bulk SQL (table-by-table INSERT … SELECT with parameterized item_id IN (…) chunks); per-pipeline / per-stage tables are enumerated from the validated `TaskConfig` so identifier interpolation stays safe. Seen reconciliation runs after carryover copy succeeds but before the file swap; a swap failure can leave the seen DB momentarily ahead of the active DB, which is benign (those item_ids are already considered "seen" — at worst future fetches dedup them, which matches the post-rotation steady state anyway).

**Tech Stack:** Python 3.11+, `aiosqlite`, stdlib `gzip` / `shutil` / `os.replace`, existing Plan 1 + Plan 2 modules. No new third-party deps.

**Spec reference:** `docs/superpowers/specs/2026-05-02-multi-pipeline-dag-design.md` § "Active DB rotation" + "Seen DB".

**Interface contract reuse:**
- D1 (writer injection): rotation does not start a `WriterThread`. New writer instances created during rotation MUST NOT overlap with the active-file swap window — orchestrator owns lifecycle.
- D2 (migration pre-flight): the new active DB is built using the same `ensure_dynamic_tables(task)` shape (own connection, idempotent DDL).
- D4 (seen writes): seen DB writes during reconciliation are append-only `INSERT OR IGNORE` and happen on a connection separate from the writer's. The D4 grep test (Task 22 of Plan 2) is extended to allow `sluice/dag/rotation/seen_reconcile.py` (mirrors the `seen_post.py` exemption).

**Out of scope (deferred to Plan 4):**
- Prefect-native pause/resume (Plan 3 uses a file-based `RotationLock`; Plan 4 swaps to Prefect's deployment pause API)
- Cron-driven rotation scheduling (Plan 3 ships the `sluice gc-rotate` CLI; Plan 4 wires the cron from `sluice.toml [gc.schedule]`)
- Multi-host coordination (single-host assumption; the file lock is process-local + filesystem-durable, sufficient for the current Prefect-on-one-VM deployment)

---

## File Structure

**Created:**

Shared helpers (extracted from Plan 1/2 duplication):
- `sluice/dag/_name.py` — `safe_name(name)` (single source of truth for `[a-zA-Z0-9_]+` validation)
- `sluice/dag/_source_hash.py` — `source_hash_from_row(row)` (single source of truth for the seen DB hash; used by `seen_post.py` and `seen_reconcile.py`)

New schema migration:
- `sluice/dag/migrations/v2_0002_active_rotation_gate.sql` — adds `rotation_gate` (single-row gate flag) and `pipeline_admissions` (in-flight registry); bumps schema_version to 2

Rotation module:
- `sluice/dag/rotation/__init__.py`
- `sluice/dag/rotation/lock.py` — `RotationLock` (file + in-process; fast-path advisory only)
- `sluice/dag/rotation/admission.py` — `build_admission_job`, `build_deadmission_job`, `RotationGateClosed`
- `sluice/dag/rotation/gate.py` — `close_gate(active_path)`, `open_gate(active_path)` (rotation-side helpers)
- `sluice/dag/rotation/reach.py` — `compute_item_reach(task, items_rows) -> dict[item_id, frozenset[pipeline_name]]`
- `sluice/dag/rotation/partition.py` — `partition_items(active, task) -> Partition`
- `sluice/dag/rotation/drain.py` — `drain_running_pipelines(active_path, task_name, *, timeout) -> bool`
- `sluice/dag/rotation/new_active.py` — `build_new_active(new_path, task)` (DDL + dynamic tables)
- `sluice/dag/rotation/copy.py` — `copy_carryover(old_path, new_path, partition, task)`
- `sluice/dag/rotation/seen_reconcile.py` — `reconcile_seen_for_archive(seen_path, archive_items, archived_at)`
- `sluice/dag/rotation/swap.py` — `swap_active_file(old_path, new_path, archive_dir, task_name, anchor)` (atomic move + gzip)
- `sluice/dag/rotation/orchestrator.py` — `rotate_task(task, *, config, now)` top-level

Config:
- `sluice/dag/sluice_config.py` — `SluiceConfig`, `RotationConfig`; loader from `sluice.toml`

Run-pipeline integration:
- `sluice/dag/run_pipeline.py` — gated by the SQL admission gate; new return status `'skipped_rotation'`
- `sluice/dag/writer.py` — `WriterThread` acquires `RotationLock` by default from `start()` until `stop()` so programmatic idle writers block rotation too
- `sluice/cli.py` (`sluice run` / `dag_run`) — acquires `RotationLock` **before** `ensure_dynamic_tables(cfg)` and `WriterThread.start()`, creates the writer with `hold_rotation_lock=False`, and releases the lock only after the writer stops, so rotations never see an unadmitted writer or migration open the active DB
- `sluice/dag/sinks/base.py` — N/A (no change)

Tests: `tests/dag/rotation/test_*.py`, `tests/dag/test_sluice_config.py`, `tests/dag/test_run_pipeline_rotation_gate.py`, `tests/dag/test_d4_invariant.py` (extended).

**Modified:**
- `sluice/cli.py` — add `gc-rotate -t <task> [--dry-run]` subcommand (single-token name; matches the `@app.command("gc-rotate")` registration)
- `tests/dag/test_d4_invariant.py` — add `seen_reconcile.py` to allow-list, forbid every other rotation file from touching `item_seen`

---

## Phase A — Locking & Run-Pipeline Gate

### Task 0: Extract shared helpers

**Files:**
- Create: `sluice/dag/_name.py`, `sluice/dag/_source_hash.py`
- Modify: `sluice/dag/eligibility.py`, `sluice/dag/cleanup.py`, `sluice/dag/dynamic_tables.py`, `sluice/dag/stores/sink_pending.py`, `sluice/dag/stores/pipeline_state.py`, `sluice/dag/stores/stage_state.py`, `sluice/dag/phases/skip_compute.py`, `sluice/dag/phases/run_key_resolve.py` (replace local `_NAME_RE`/`_safe()` with import); `sluice/dag/phases/seen_post.py` (use shared `source_hash_from_row`)
- Test: `tests/dag/test_helpers.py`

Reasons to do this first:
- Plan 2 audit flagged `_NAME_RE`/`_safe()` duplication 6 places. Plan 3 adds 2 more (`partition.py`, `copy.py`). Extract once, fix all callers in one mechanical pass before any new rotation code lands.
- `seen_post.py` and `seen_reconcile.py` MUST agree on the hash. Sharing the helper turns "drift bug" into "compile error".

```python
# sluice/dag/_name.py
from __future__ import annotations

import re

_NAME_RE = re.compile(r"^[a-zA-Z0-9_]+$")


def safe_name(name: str) -> str:
    """Return name iff it matches [a-zA-Z0-9_]+; else raise ValueError."""
    if _NAME_RE.fullmatch(name) is None:
        raise ValueError(f"unsafe identifier: {name!r}")
    return name
```

```python
# sluice/dag/_source_hash.py
from __future__ import annotations

import hashlib
from typing import Mapping


def source_hash_from_row(row: Mapping[str, object]) -> str:
    """SHA1 of '<source_name>::<guid_or_url>'.

    NOTE: items whose `guid` and `url` are both NULL/empty share a single hash
    per source_name. This matches v1 behavior and is intentional — fully empty
    items from the same source are treated as one entity for dedup purposes.
    """
    source_name = row.get("source_name") or ""
    natural = row.get("guid") or row.get("url") or ""
    base = f"{source_name}::{natural}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()
```

- [ ] **Step 0.1: Failing tests** for `safe_name` (rejects `"a;b"`, `"a-b"`, accepts `"a_1"`); `source_hash_from_row` (deterministic, matches existing `seen_post._source_hash` byte-for-byte on a known row).

- [ ] **Step 0.2: Implement both files.**

- [ ] **Step 0.3: Mechanical replace across every callsite.** Use a single commit so the refactor is reviewable as one diff. After this commit, every existing test must still pass — the helpers are byte-identical to the inline versions they replace. Run full suite:

```bash
rtk uv run pytest tests/dag/ -q
rtk uv run ruff check sluice tests
```

- [ ] **Step 0.4: Commit.**

```bash
git add sluice/dag/_name.py sluice/dag/_source_hash.py \
        sluice/dag/eligibility.py sluice/dag/cleanup.py \
        sluice/dag/dynamic_tables.py sluice/dag/stores/ \
        sluice/dag/phases/skip_compute.py sluice/dag/phases/run_key_resolve.py \
        sluice/dag/phases/seen_post.py tests/dag/test_helpers.py
git commit -m "refactor(dag): extract safe_name + source_hash_from_row helpers"
```

---

### Task 1: `RotationLock` (file + in-process)

**Files:**
- Create: `sluice/dag/rotation/__init__.py` (empty marker)
- Create: `sluice/dag/rotation/lock.py`
- Test:   `tests/dag/rotation/test_lock.py`

The lock has two layers:
- **File**: `<active_path>.rotation.lock` — durable across process crashes (an orphaned lock from a crashed rotation must be detectable).
- **In-process** `RotationLock._inprocess_holds: set[str]` (CLASS-level set, keyed by absolute lock path) — same-process pipelines see "held" instantly even before the OS file flush is durable.

The file format stores `pid` + iso timestamp. `is_held()` returns true if the in-process set has the path OR the file exists AND the recorded pid is alive. Dead-pid locks are unlinked unconditionally — the process that wrote them cannot recover. The `stale_after_seconds` threshold only matters for malformed lock files (e.g. truncated content) where pid liveness can't be checked; in that path we treat anything older than the threshold as stale and unlink.

`acquire()` MUST swallow the `FileExistsError` raised by `os.open(O_EXCL)` on a race and re-raise as `RuntimeError("rotation lock already held: ...")` so callers see the documented "lock-busy" semantic and don't have to know the OS error type.

- [ ] **Step 1.1: Failing test**

```python
# tests/dag/rotation/test_lock.py
from __future__ import annotations

import os
from pathlib import Path

import pytest

from sluice.dag.rotation.lock import RotationLock


def _path(tmp_path: Path) -> Path:
    return tmp_path / "x.db"


def test_lock_not_held_initially(tmp_path):
    db = _path(tmp_path)
    lock = RotationLock(db)
    assert not lock.is_held()


def test_acquire_release_cycle(tmp_path):
    db = _path(tmp_path)
    lock = RotationLock(db)
    lock.acquire()
    assert lock.is_held()
    lock.release()
    assert not lock.is_held()


def test_double_acquire_raises(tmp_path):
    db = _path(tmp_path)
    lock = RotationLock(db)
    lock.acquire()
    try:
        with pytest.raises(RuntimeError, match="already held"):
            lock.acquire()
    finally:
        lock.release()


def test_stale_lock_from_dead_pid_is_cleared(tmp_path):
    db = _path(tmp_path)
    lock_file = Path(str(db) + ".rotation.lock")
    lock_file.write_text("99999999\t2026-01-01T00:00:00+00:00")  # pid almost certainly dead
    lock = RotationLock(db, stale_after_seconds=0)
    assert not lock.is_held()
    assert not lock_file.exists()


def test_lock_held_by_other_live_process_blocks(tmp_path):
    db = _path(tmp_path)
    lock_file = Path(str(db) + ".rotation.lock")
    lock_file.write_text(f"{os.getpid()}\t2099-01-01T00:00:00+00:00")
    lock = RotationLock(db, stale_after_seconds=999_999)
    assert lock.is_held()


def test_context_manager_releases_on_exception(tmp_path):
    db = _path(tmp_path)
    lock = RotationLock(db)
    with pytest.raises(ValueError):
        with lock:
            raise ValueError("boom")
    assert not lock.is_held()
```

- [ ] **Step 1.2: Run, FAIL.**

- [ ] **Step 1.3: Implement `lock.py`**

```python
# sluice/dag/rotation/lock.py
from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path


def _is_pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True   # exists, owned by another user
    return True


class RotationLock:
    _inprocess_holds: set[str] = set()  # class-level: shared across instances

    def __init__(self, active_path: str | Path, *, stale_after_seconds: int = 3 * 3600):
        self._lock_file = Path(str(active_path) + ".rotation.lock").resolve()
        self._stale_after = stale_after_seconds
        self._holding = False

    @property
    def _key(self) -> str:
        return str(self._lock_file)

    def is_held(self) -> bool:
        if self._holding or self._key in RotationLock._inprocess_holds:
            return True
        if not self._lock_file.exists():
            return False
        try:
            text = self._lock_file.read_text().strip()
            pid_str, ts_str = text.split("\t", 1)
            pid = int(pid_str)
            datetime.fromisoformat(ts_str)  # parse-only validation
        except (ValueError, OSError):
            # Malformed file — only sweep if it's also old enough to be safe.
            try:
                age = (
                    datetime.now(timezone.utc).timestamp()
                    - self._lock_file.stat().st_mtime
                )
                if age > self._stale_after:
                    self._lock_file.unlink(missing_ok=True)
                    return False
            except OSError:
                pass
            return True   # malformed but recent → treat as held; refuse acquire
        if not _is_pid_alive(pid):
            self._lock_file.unlink(missing_ok=True)
            return False
        return True

    def acquire(self) -> None:
        if self._key in RotationLock._inprocess_holds:
            raise RuntimeError(f"rotation lock already held: {self._lock_file}")
        # Sweep stale before O_EXCL so crashed-pid lock files don't permanently block.
        if self._lock_file.exists() and not self.is_held():
            pass  # is_held() already unlinked stale
        self._lock_file.parent.mkdir(parents=True, exist_ok=True)
        try:
            fd = os.open(
                self._lock_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644
            )
        except FileExistsError as exc:
            # Either another live process holds it, or stale-sweep didn't apply.
            raise RuntimeError(
                f"rotation lock already held: {self._lock_file}"
            ) from exc
        try:
            payload = f"{os.getpid()}\t{datetime.now(timezone.utc).isoformat()}"
            os.write(fd, payload.encode("utf-8"))
        finally:
            os.close(fd)
        RotationLock._inprocess_holds.add(self._key)
        self._holding = True

    def release(self) -> None:
        if self._holding:
            self._lock_file.unlink(missing_ok=True)
            RotationLock._inprocess_holds.discard(self._key)
            self._holding = False

    def __enter__(self) -> "RotationLock":
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()
```

Add a test for the in-process layer:

```python
def test_lock_inprocess_set_blocks_second_instance(tmp_path):
    db = _path(tmp_path)
    a = RotationLock(db); b = RotationLock(db)
    a.acquire()
    try:
        assert b.is_held()
        with pytest.raises(RuntimeError, match="already held"):
            b.acquire()
    finally:
        a.release()
    assert not b.is_held()


def test_lock_acquire_translates_o_excl_race(tmp_path):
    # Simulate file appearing between is_held() and os.open by writing it
    # directly with a live PID outside our in-process set.
    db = _path(tmp_path)
    lock_file = Path(str(db) + ".rotation.lock")
    lock_file.write_text(f"{os.getpid()}\t{datetime.now(timezone.utc).isoformat()}")
    lock = RotationLock(db, stale_after_seconds=999_999)
    with pytest.raises(RuntimeError, match="already held"):
        lock.acquire()
```

- [ ] **Step 1.4: PASS, commit.**

```bash
git add sluice/dag/rotation/__init__.py sluice/dag/rotation/lock.py tests/dag/rotation/test_lock.py
git commit -m "feat(rotation): add RotationLock with file+in-process layers"
```

---

### Task 2: `run_pipeline` rotation gate (SQL admission)

**Files:** `sluice/dag/run_pipeline.py`, `tests/dag/test_run_pipeline_rotation_gate.py`

**Why two layers, not one:**

A naïve `if RotationLock.is_held(): return skipped_rotation` at the top of `run_pipeline` is **insufficient**. Plan 2's runner does all preflight queries + stage execution BEFORE submitting tx1, so `pipelines.status='running'` is not visible to a concurrent rotation drain. Race:

```
t0  pipeline A: is_held() → False
t1  pipeline A: starts preflight + stage execution (no DB writes yet)
t2  rotation:  acquire RotationLock
t3  rotation:  count_running() → 0  ✗ A is invisible
t4  rotation:  partition + copy + swap  ✗ writes to old DB
t5  pipeline A: tx1 submits to writer  ✗ writes to whichever DB the writer happens to hold
```

Fix: **a SQL admission gate** (Task 2b), with the CLI file lock handled before the writer exists (Task 2c):
- Atomic admission via writer (BEGIN IMMEDIATE on active) → registers the run in `pipeline_admissions`. Drain waits for admissions to drain. Rotation closes the gate inside the same transactional context, so any new admission attempt either sees the gate open (proceed) or sees it closed (return `skipped_rotation`).
- Do not put a file-lock fast-path inside `run_pipeline`: by that point the injected `WriterThread` is already open. Returning before SQL admission would make the writer invisible to drain.

`run_pipeline` therefore becomes:

```python
async def run_pipeline(task, pipeline_name, *, writer, now=None, trigger="manual"):
    validate_task(task)
    pipeline = ...

    # Atomic admission. Generates a uuid-based admission_id; if the gate is
    # closed, raises RotationGateClosed which we translate to skipped_rotation.
    from sluice.dag.rotation.admission import (
        RotationGateClosed, build_admission_job, build_deadmission_job,
    )
    import uuid
    admission_id = str(uuid.uuid4())
    try:
        await (await writer.submit(build_admission_job(
            admission_id=admission_id, task_name=task.name, pipeline_name=pipeline.name,
        )))
    except RotationGateClosed as exc:
        return RunResult(..., status="skipped_rotation", error_msg=str(exc))

    try:
        # ... existing run_pipeline body unchanged ...
        return RunResult(...)
    finally:
        # Always deregister, even on exception. Best-effort: a failure here
        # leaves a row in pipeline_admissions which the next rotation will
        # eventually time-out on (Plan 4 adds zombie admission cleanup).
        try:
            await (await writer.submit(build_deadmission_job(admission_id)))
        except Exception as exc:  # noqa: BLE001
            logger.warning("deadmission failed for %s: %s", admission_id, exc)
```

- [ ] **Step 2.1: Failing tests**

```python
# tests/dag/test_run_pipeline_rotation_gate.py
import pytest

from sluice.dag.rotation.lock import RotationLock
from sluice.dag.rotation.gate import close_gate, open_gate
from sluice.dag.run_pipeline import run_pipeline


@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_run_pipeline_admission_blocked_when_gate_closed(
    minimal_task, fresh_writer
):
    await close_gate(minimal_task.db_path)
    try:
        result = await run_pipeline(minimal_task, "p_simple", writer=fresh_writer)
    finally:
        await open_gate(minimal_task.db_path)
    assert result.status == "skipped_rotation"


@pytest.mark.asyncio
async def test_run_pipeline_deadmission_clears_admissions_row(
    minimal_task, fresh_writer, active_db
):
    # Successful run leaves no rows in pipeline_admissions.
    result = await run_pipeline(minimal_task, "p_simple", writer=fresh_writer)
    assert result.status == "success"
    cur = await active_db.execute("SELECT COUNT(*) FROM pipeline_admissions")
    assert (await cur.fetchone())[0] == 0
```

- [ ] **Step 2.2: Implement** the admission/deadmission wrapping per the snippet above. CLI exit-status set:

```python
if result.status not in {"success", "skipped_gating", "skipped_rotation"}:
    raise typer.Exit(code=1)
```

- [ ] **Step 2.3: PASS, commit.**

---

### Task 2c: CLI pre-run rotation gate (avoid opening active DB / starting writer during rotation)

**Files:**
- Modify: `sluice/cli.py` (the `dag_run` / `sluice run` command — currently calls `ensure_dynamic_tables(cfg)` then `WriterThread.start()` before `run_pipeline`)
- Create: `tests/dag/test_cli_rotation_fast_path.py`

**Why:** Task 2 only gates inside `run_pipeline()`. But `sluice run` does `ensure_dynamic_tables(cfg)` (opens active DB, runs migrations) and `WriterThread.start()` (opens active+seen, holds them) **before** ever calling `run_pipeline`. While rotation holds `RotationLock` and is in (or about to enter) the swap window, that pre-flight will:
- open the active SQLite file (potentially the very file rotation is `os.replace`-swapping),
- run DDL on it,
- start a writer that begins acquiring write txns.

Even though SQL admission inside `run_pipeline` will eventually reject the run, the writer/migration side-effects already happened. That contradicts the architectural goal "active is closed after drain; no writer overlaps swap." The fix is to make the CLI acquire `RotationLock` **before** opening the DB and hold it until the writer is stopped. If the lock is already held, the CLI short-circuits to `status='skipped_rotation'` without calling `ensure_dynamic_tables` or instantiating `WriterThread`.

The SQL admission gate inside `run_pipeline` (Task 2) is still authoritative after the writer starts. Holding the CLI lock through admission prevents the remaining race where rotation acquires the file lock between a one-shot pre-check and `WriterThread.start()`.

**About `sluice/dag/runner.py::run_pipeline_stub`:** This is a Plan 1 legacy/test-only stub that pre-dates the real `run_pipeline` and Plan 2's `WriterThread` injection model. It is **not** a production entrypoint — neither the CLI nor (post-Plan 4) Prefect calls it. It will be removed when Plan 4 lands. To prevent accidental production use:

- Add a guard test (in this same task's test file) that grep-asserts `run_pipeline_stub` is not referenced from `sluice/cli.py` or any future `sluice/prefect/` flow:

```python
# tests/dag/test_cli_rotation_fast_path.py — add at bottom
import pathlib

def test_run_pipeline_stub_not_used_in_production_entrypoints():
    """run_pipeline_stub is Plan 1 legacy; production entrypoints must not call it."""
    repo = pathlib.Path(__file__).resolve().parents[2]
    forbidden_dirs = [repo / "sluice" / "cli.py"]
    for path in forbidden_dirs:
        if path.exists():
            text = path.read_text()
            assert "run_pipeline_stub" not in text, (
                f"{path} references run_pipeline_stub — use real run_pipeline instead"
            )
```

Plan 4 deletes `sluice/dag/runner.py` outright; until then the grep test pins the contract.

The check is encapsulated as a tiny helper so other future entrypoints (Prefect flow runner in Plan 4) can reuse it. `RunResult` is `frozen=True`, so the helper takes `pipeline_name` as a parameter and constructs the result fully populated:

```python
# sluice/dag/rotation/lock.py  (extend; small helper alongside RotationLock)
def check_rotation_fast_path(task, pipeline_name: str):
    """Return a skipped_rotation RunResult if the lock is held, else None.

    Use BEFORE opening the active DB or starting WriterThread.
    """
    from sluice.dag.run_pipeline import RunResult  # local import to avoid cycle
    if RotationLock(task.db_path).is_held():
        return RunResult(
            pipeline_name=pipeline_name,
            run_key="",
            status="skipped_rotation",
            items_in=0,
            items_out=0,
            error_msg=f"rotation in progress for {task.name!r}",
        )
    return None
```

The CLI uses it like this. Note: `ensure_dynamic_tables` and `WriterThread` are currently locally imported inside `dag_run` — to make them patchable in tests AND to give the fast-path a clean place to land, hoist them to **module-level** imports in `sluice/cli.py`:

```python
# sluice/cli.py — module top (alongside other imports):
from sluice.dag.rotation.lock import RotationLock, check_rotation_fast_path
from sluice.dag.run_pipeline import RunResult, ensure_dynamic_tables, run_pipeline
from sluice.dag.writer import WriterThread
```

```python
# sluice/cli.py — inside dag_run, replace _go() with:
async def _go():
    lock = RotationLock(cfg.db_path)
    try:
        lock.acquire()
    except RuntimeError:
        fast = check_rotation_fast_path(cfg, pipeline)
        if fast is not None:
            return fast
        raise

    try:
        await ensure_dynamic_tables(cfg)
        writer = WriterThread(
            active_path=cfg.db_path,
            seen_path=cfg.seen_db,
            hold_rotation_lock=False,
        )
        await writer.start()
        try:
            now = datetime.fromisoformat(cycle) if cycle else None
            return await run_pipeline(cfg, pipeline, writer=writer, now=now)
        finally:
            await writer.stop()
    finally:
        lock.release()
```

Remove the matching local imports inside `dag_run`. Tests can now `patch("sluice.cli.ensure_dynamic_tables", ...)` and `patch("sluice.cli.WriterThread", ...)` reliably.

The CLI exit-status set already includes `skipped_rotation` (Task 2.2), so no further CLI change is needed.

- [ ] **Step 2c.1: Failing test**

```python
# tests/dag/test_cli_rotation_fast_path.py
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from sluice.cli import app
from sluice.dag.rotation.lock import RotationLock


def test_cli_run_short_circuits_when_lock_held(tmp_path, monkeypatch, minimal_task_dir):
    """When RotationLock is held, sluice run must NOT call ensure_dynamic_tables
    and must NOT instantiate WriterThread. It returns status='skipped_rotation'."""
    runner = CliRunner()

    # Pre-acquire the lock from another "process" (in-process holder is enough
    # for fast-path test; integration with real flock is exercised in test_lock.py).
    db_path = minimal_task_dir / "active.db"
    lock = RotationLock(db_path)
    lock.acquire()

    called = {"ensure": False, "writer_started": False}

    async def fake_ensure(cfg):
        called["ensure"] = True

    class FakeWriter:
        def __init__(self, *a, **kw): ...
        async def start(self): called["writer_started"] = True
        async def stop(self): ...

    try:
        with patch("sluice.cli.ensure_dynamic_tables", fake_ensure), \
             patch("sluice.cli.WriterThread", FakeWriter):
            result = runner.invoke(app, [
                "run", "-t", "minimal", "-p", "p_simple",
                "-c", str(minimal_task_dir.parent.parent),
            ])
    finally:
        lock.release()

    assert result.exit_code == 0, result.output
    assert '"status": "skipped_rotation"' in result.output
    assert called["ensure"] is False
    assert called["writer_started"] is False
```

- [ ] **Step 2c.2: Run test, verify it fails** (current CLI calls `ensure_dynamic_tables` unconditionally):

```bash
pytest tests/dag/test_cli_rotation_fast_path.py -v
# Expected: FAIL — called["ensure"] is True
```

- [ ] **Step 2c.3: Implement** the `_go()` change in `sluice/cli.py` and the optional `check_rotation_fast_path` helper in `lock.py`.

- [ ] **Step 2c.4: Run test, verify it passes.**

```bash
pytest tests/dag/test_cli_rotation_fast_path.py -v
# Expected: PASS
```

- [ ] **Step 2c.5: Commit.**

```bash
git add sluice/cli.py sluice/dag/rotation/lock.py tests/dag/test_cli_rotation_fast_path.py
git commit -m "feat(rotation): CLI pre-run fast-path skips DB/writer when lock held"
```

---

### Task 2b: SQL admission gate (the actual quiescence mechanism)

**Files:**
- Create: `sluice/dag/migrations/v2_0002_active_rotation_gate.sql`
- Create: `sluice/dag/rotation/admission.py`, `sluice/dag/rotation/gate.py`
- Test:   `tests/dag/rotation/test_admission_gate.py`

The migration:

```sql
-- v2_0002_active_rotation_gate.sql
CREATE TABLE rotation_gate (
  id     INTEGER PRIMARY KEY CHECK (id = 1),
  closed INTEGER NOT NULL DEFAULT 0
);
INSERT INTO rotation_gate(id, closed) VALUES (1, 0);

CREATE TABLE pipeline_admissions (
  admission_id   TEXT PRIMARY KEY,
  task           TEXT NOT NULL,
  pipeline_name  TEXT NOT NULL,
  registered_at  TEXT NOT NULL
);
CREATE INDEX idx_admissions_task ON pipeline_admissions(task);

INSERT INTO schema_version(version, applied_at) VALUES (2, CURRENT_TIMESTAMP);
```

Plan 1's `apply_migrations` (in `sluice/dag/db.py`) already filters by `version > current` so this migration applies cleanly to existing v1 DBs without touching v1 data.

The admission helpers:

```python
# sluice/dag/rotation/admission.py
from __future__ import annotations

from datetime import datetime, timezone

from sluice.dag.writer import WriteJob


class RotationGateClosed(RuntimeError):
    """Raised inside the writer when an admission attempt finds gate=closed.

    The writer rolls back the transaction; run_pipeline catches this and
    returns RunResult(status='skipped_rotation', ...).
    """


def build_admission_job(
    *, admission_id: str, task_name: str, pipeline_name: str
) -> WriteJob:
    registered_at = datetime.now(timezone.utc).isoformat()

    async def job(active, seen) -> None:  # noqa: ARG001 — D4: no seen writes
        # The writer wraps this in BEGIN IMMEDIATE / COMMIT, so the gate
        # check + admission INSERT are atomic against any other writer job
        # (including rotation's gate-close transaction).
        cur = await active.execute(
            "SELECT closed FROM rotation_gate WHERE id = 1"
        )
        row = await cur.fetchone()
        if row is None:
            # Migration v2_0002 should have seeded this row. A missing row
            # means either (a) the DB hasn't been migrated yet — impossible
            # since `apply_migrations` runs on every `open_active_db` — or
            # (b) someone manually deleted the row. Either way, fail-closed:
            # we cannot guarantee no rotation is in progress.
            raise RotationGateClosed(
                f"rotation_gate row missing for task {task_name!r}; "
                f"refusing admission (fail-closed)"
            )
        if row[0]:
            raise RotationGateClosed(
                f"rotation gate closed for task {task_name!r}"
            )
        await active.execute(
            "INSERT INTO pipeline_admissions"
            "(admission_id, task, pipeline_name, registered_at) "
            "VALUES (?, ?, ?, ?)",
            (admission_id, task_name, pipeline_name, registered_at),
        )

    return WriteJob(run=job)


def build_deadmission_job(admission_id: str) -> WriteJob:
    async def job(active, seen) -> None:  # noqa: ARG001 — D4: no seen writes
        await active.execute(
            "DELETE FROM pipeline_admissions WHERE admission_id = ?",
            (admission_id,),
        )

    return WriteJob(run=job)
```

The rotation-side gate helpers (the rotation orchestrator does NOT go through `WriterThread` — it is the only writer at this point in the lifecycle):

```python
# sluice/dag/rotation/gate.py
from __future__ import annotations

from sluice.dag.db import open_active_db


async def close_gate(active_path: str) -> None:
    # UPSERT instead of UPDATE: a missing row would silently no-op the UPDATE
    # and admission would later observe gate-open even though the operator
    # asked us to close. ON CONFLICT keeps the migration-seeded row in sync.
    async with open_active_db(active_path) as conn:
        await conn.execute("BEGIN IMMEDIATE")
        await conn.execute(
            "INSERT INTO rotation_gate(id, closed) VALUES (1, 1) "
            "ON CONFLICT(id) DO UPDATE SET closed = 1"
        )
        await conn.commit()


async def open_gate(active_path: str) -> None:
    async with open_active_db(active_path) as conn:
        await conn.execute("BEGIN IMMEDIATE")
        await conn.execute(
            "INSERT INTO rotation_gate(id, closed) VALUES (1, 0) "
            "ON CONFLICT(id) DO UPDATE SET closed = 0"
        )
        await conn.commit()


async def admissions_count(active_path: str, task_name: str) -> int:
    async with open_active_db(active_path) as conn:
        cur = await conn.execute(
            "SELECT COUNT(*) FROM pipeline_admissions WHERE task = ?",
            (task_name,),
        )
        row = await cur.fetchone()
        return int(row[0]) if row else 0
```

**Race-freeness argument**: the writer serializes all jobs through `BEGIN IMMEDIATE`. `close_gate` uses `BEGIN IMMEDIATE` on a separate connection. SQLite's WAL+IMMEDIATE-write protocol ensures no two writers commit concurrently. So either:
- `close_gate` commits FIRST → next `build_admission_job` reads `closed=1`, raises, runner returns `skipped_rotation`.
- `build_admission_job` commits FIRST → admission is durably registered before drain begins, and drain will see it.

There is no in-between: a runner whose admission committed CANNOT have started before `close_gate` ran (otherwise the admission would have committed before `close_gate`'s `BEGIN IMMEDIATE` could acquire the write lock, contradicting "close_gate committed FIRST").

- [ ] **Step 2b.1: Failing tests** covering:
  - Migration applies cleanly on a v1 DB; schema_version goes 1 → 2; tables exist
  - `build_admission_job` succeeds with gate=0 → row in `pipeline_admissions`
  - `build_admission_job` with gate=1 → `RotationGateClosed` raised; transaction rolled back; no row inserted
  - `build_deadmission_job` removes the matching row; idempotent (no error if row missing)
  - `close_gate` then `admissions_count` returns 0; `build_admission_job` raises; reopen → admission succeeds
  - **Fail-closed test**: manually `DELETE FROM rotation_gate WHERE id=1`, run `build_admission_job` → `RotationGateClosed("rotation_gate row missing ... fail-closed")`; assert no admission row inserted
  - **Idempotent gate ops**: `close_gate` followed by another `close_gate` succeeds (UPSERT); `close_gate` on a fresh DB whose row was deleted recreates it as `closed=1`
  - **Race test**: starting from gate=0, two parallel `asyncio.gather()` invocations of `build_admission_job` + `close_gate`. Both writers serialize through `BEGIN IMMEDIATE`, so exactly ONE of these two end states must hold (gate ends `closed=1` either way because `close_gate` always runs to completion):
    - `admission committed first` → `admissions_count == 1` (the row landed) AND `gate.closed == 1` (close_gate then took the lock and flipped it) AND no exception
    - `close_gate committed first` → `admissions_count == 0` (admission rolled back) AND `gate.closed == 1` AND `RotationGateClosed` raised on the admission future
    The forbidden inconsistent state would be `admissions_count == 1 AND gate=1 AND admission rejected`, or any commit visible without the corresponding write — neither can occur under SQLite's IMMEDIATE-write serialization.

- [ ] **Step 2b.2: Implement** all three files, run, PASS, commit.

---

## Phase B — Drain

### Task 3: `drain_running_pipelines`

**File:** `sluice/dag/rotation/drain.py`, `tests/dag/rotation/test_drain.py`

```python
# sluice/dag/rotation/drain.py
from __future__ import annotations

import asyncio

from sluice.dag.db import open_active_db


async def count_in_flight(active_path: str, task_name: str) -> tuple[int, int]:
    """Returns (running_pipelines, pending_admissions) for the task."""
    async with open_active_db(active_path) as conn:
        cur = await conn.execute(
            "SELECT COUNT(*) FROM pipelines WHERE task = ? AND status = 'running'",
            (task_name,),
        )
        running = int((await cur.fetchone())[0])
        cur = await conn.execute(
            "SELECT COUNT(*) FROM pipeline_admissions WHERE task = ?",
            (task_name,),
        )
        admitted = int((await cur.fetchone())[0])
        return running, admitted


# Backward-compat shim (some tests import this name)
async def count_running(active_path: str, task_name: str) -> int:
    running, _ = await count_in_flight(active_path, task_name)
    return running


async def drain_running_pipelines(
    active_path: str,
    task_name: str,
    *,
    timeout_seconds: float,
    poll_interval_seconds: float = 1.0,
) -> bool:
    """Returns True if drained within timeout, False on timeout.

    Drain is satisfied when BOTH counts are zero:
      - `pipelines.status='running'` for the task (post-tx1 in-flight runs)
      - `pipeline_admissions` for the task (admitted but pre-tx1 runs)

    The orchestrator MUST call `close_gate(active_path)` before invoking this
    so no new admissions can register; otherwise drain may race indefinitely.

    KNOWN LIMITATION (Plan 3 deferral): if a writer crashes mid-run without
    finalizing `pipelines.status` OR without deleting its `pipeline_admissions`
    row, that artifact persists indefinitely and every drain call will time
    out. Plan 4 adds zombie detection (started_at / registered_at age check +
    writer liveness probe) and a `--force` flag to clear zombies before
    drain. Operator workaround for Plan 3: manually
    `UPDATE pipelines SET status='failed' WHERE ...` and
    `DELETE FROM pipeline_admissions WHERE registered_at < ...`
    before invoking `gc-rotate`.
    """
    deadline = asyncio.get_event_loop().time() + timeout_seconds
    while True:
        running, admitted = await count_in_flight(active_path, task_name)
        if running == 0 and admitted == 0:
            return True
        if asyncio.get_event_loop().time() >= deadline:
            return False
        await asyncio.sleep(poll_interval_seconds)
```

- [ ] **Step 3.1: Failing test**

```python
# tests/dag/rotation/test_drain.py
import pytest

from sluice.dag.rotation.drain import count_running, drain_running_pipelines


async def _seed(active, run_key, status):
    await active.execute(
        "INSERT INTO pipelines(run_key, task, pipeline_name, cycle_anchor, "
        "trigger, started_at, status) VALUES "
        "(?, 't', 'p', '2026-05-04T00:00:00+00:00', 'manual', "
        "'2026-05-04T00:00:00+00:00', ?)",
        (run_key, status),
    )
    await active.commit()


@pytest.mark.asyncio
async def test_count_running_zero(active_db, active_path):
    assert await count_running(active_path, "t") == 0


@pytest.mark.asyncio
async def test_count_running_excludes_finished(active_db, active_path):
    await _seed(active_db, "rk1", "success")
    await _seed(active_db, "rk2", "running")
    assert await count_running(active_path, "t") == 1


@pytest.mark.asyncio
async def test_drain_returns_true_when_already_idle(active_db, active_path):
    drained = await drain_running_pipelines(
        active_path, "t", timeout_seconds=0.5, poll_interval_seconds=0.05,
    )
    assert drained is True


@pytest.mark.asyncio
async def test_drain_returns_false_on_timeout(active_db, active_path):
    await _seed(active_db, "rk_running", "running")
    drained = await drain_running_pipelines(
        active_path, "t", timeout_seconds=0.3, poll_interval_seconds=0.05,
    )
    assert drained is False


@pytest.mark.asyncio
async def test_drain_waits_on_admissions(active_db, active_path):
    # No 'running' rows but a stale admission → must NOT return drained.
    await active_db.execute(
        "INSERT INTO pipeline_admissions(admission_id, task, pipeline_name, "
        "registered_at) VALUES ('aid-1', 't', 'p', '2026-05-04T00:00:00+00:00')"
    )
    await active_db.commit()
    drained = await drain_running_pipelines(
        active_path, "t", timeout_seconds=0.3, poll_interval_seconds=0.05,
    )
    assert drained is False


@pytest.mark.asyncio
async def test_drain_succeeds_when_admissions_clear(active_db, active_path):
    await active_db.execute(
        "INSERT INTO pipeline_admissions(admission_id, task, pipeline_name, "
        "registered_at) VALUES ('aid-1', 't', 'p', '2026-05-04T00:00:00+00:00')"
    )
    await active_db.commit()

    async def clear_after_delay():
        await asyncio.sleep(0.1)
        await active_db.execute(
            "DELETE FROM pipeline_admissions WHERE admission_id = 'aid-1'"
        )
        await active_db.commit()

    asyncio.create_task(clear_after_delay())
    drained = await drain_running_pipelines(
        active_path, "t", timeout_seconds=1.0, poll_interval_seconds=0.05,
    )
    assert drained is True
```

(Fixtures: `active_db` is an `aiosqlite.Connection` from `open_active_db(tmp_path/"t.db")`; `active_path` is the string path. Add to `tests/dag/conftest.py` if not present.)

- [ ] **Step 3.2-3.4: Run, impl, PASS, commit.**

---

## Phase C — Reach + Partition

### Task 4: `compute_item_reach`

**File:** `sluice/dag/rotation/reach.py`, `tests/dag/rotation/test_reach.py`

Reuses `validator.compute_reach(task, entry_pipeline)` per distinct `ingested_by` value. Items whose `ingested_by` is no longer a pipeline in `task.pipelines` (deleted from config) get an empty reach (they owe work to nothing → archive-eligible by default in partition logic). The orchestrator runs `validate_task(task)` BEFORE calling reach, so `compute_reach` cannot fail on validation grounds — propagate any unexpected error rather than silently swallowing.

```python
# sluice/dag/rotation/reach.py
from __future__ import annotations

from collections.abc import Iterable

from sluice.dag.config import TaskConfig
from sluice.dag.validator import compute_reach


def compute_item_reach(
    task: TaskConfig, items_rows: Iterable[dict]
) -> dict[str, frozenset[str]]:
    pipeline_names = {p.name for p in task.pipelines}
    cache: dict[str, frozenset[str]] = {}
    out: dict[str, frozenset[str]] = {}
    for row in items_rows:
        item_id = row["item_id"]
        entry = row.get("ingested_by")
        if entry not in pipeline_names:
            # Entry pipeline removed from config; this item owes nothing
            # to anyone reachable. Partition will archive it.
            out[item_id] = frozenset()
            continue
        if entry not in cache:
            # Caller MUST have invoked validate_task(task) first; any
            # ValidationError from here is a programming error and propagates.
            cache[entry] = frozenset(compute_reach(task, entry))
        out[item_id] = cache[entry]
    return out
```

- [ ] **Step 4.1: Failing test**

```python
# tests/dag/rotation/test_reach.py
from sluice.dag.rotation.reach import compute_item_reach


def test_reach_resolves_per_ingested_by(simple_diamond_task):
    # simple_diamond_task fixture: entry pipelines [fetch_a, fetch_b],
    # downstream both feed `merge`, with `merge` enabled in deployments.
    rows = [
        {"item_id": "a1", "ingested_by": "fetch_a"},
        {"item_id": "a2", "ingested_by": "fetch_a"},
        {"item_id": "b1", "ingested_by": "fetch_b"},
    ]
    reach = compute_item_reach(simple_diamond_task, rows)
    assert reach["a1"] == reach["a2"]
    assert "fetch_a" in reach["a1"]
    assert "merge" in reach["a1"]
    assert "fetch_b" not in reach["a1"]
    assert "fetch_b" in reach["b1"]


def test_reach_empty_when_ingested_by_missing(simple_diamond_task):
    rows = [{"item_id": "x", "ingested_by": "deleted_pipeline"}]
    reach = compute_item_reach(simple_diamond_task, rows)
    assert reach["x"] == frozenset()


def test_reach_excludes_disabled_pipelines(diamond_task_with_disabled_merge):
    rows = [{"item_id": "a", "ingested_by": "fetch_a"}]
    reach = compute_item_reach(diamond_task_with_disabled_merge, rows)
    assert "merge" not in reach["a"]
```

- [ ] **Step 4.2-4.4: impl, PASS, commit.**

---

### Task 5: `partition_items`

**File:** `sluice/dag/rotation/partition.py`, `tests/dag/rotation/test_partition.py`

Returns `Partition(carryover_ids: set[str], archive_ids: set[str], item_reach: dict[str, frozenset[str]])`. An item is **archive** if `items.dropped=1` OR every P ∈ reach has a terminal `pipeline_<P>` row. Otherwise **carryover**.

Items with `reach == frozenset()` (e.g. `ingested_by` no longer a pipeline) go to **archive** — they owe no work to anything reachable.

Edge: items whose `pipeline_<P>` table doesn't exist for some P ∈ reach (e.g. P added after this item) — treat as "no terminal row" → carryover. The partition query enumerates per-pipeline tables and tolerates missing ones via try/except on `OperationalError("no such table")`.

```python
# sluice/dag/rotation/partition.py
from __future__ import annotations

import sqlite3
from dataclasses import dataclass

import aiosqlite

from sluice.dag._name import safe_name
from sluice.dag.config import TaskConfig
from sluice.dag.rotation.reach import compute_item_reach


@dataclass(frozen=True)
class Partition:
    carryover_ids: set[str]
    archive_ids: set[str]
    item_reach: dict[str, frozenset[str]]


async def partition_items(
    conn: aiosqlite.Connection, task: TaskConfig
) -> Partition:
    cur = await conn.execute(
        "SELECT item_id, ingested_by, dropped FROM items"
    )
    items_rows = [
        {"item_id": r[0], "ingested_by": r[1], "dropped": int(r[2])}
        for r in await cur.fetchall()
    ]
    item_reach = compute_item_reach(task, items_rows)

    # Pre-collect terminal item ids per pipeline once. Cheaper than per-item EXISTS.
    terminal_by_pipeline: dict[str, set[str]] = {}
    for pipeline in task.pipelines:
        safe_p = safe_name(pipeline.name)
        try:
            cur = await conn.execute(
                f"SELECT item_id FROM pipeline_{safe_p} "
                "WHERE status IN ('success','dropped','failed')"
            )
            terminal_by_pipeline[pipeline.name] = {
                r[0] for r in await cur.fetchall()
            }
        except sqlite3.OperationalError as exc:
            if "no such table" not in str(exc):
                raise
            terminal_by_pipeline[pipeline.name] = set()

    carryover: set[str] = set()
    archive: set[str] = set()

    for row in items_rows:
        item_id = row["item_id"]
        if row["dropped"] == 1:
            archive.add(item_id)
            continue
        reach = item_reach[item_id]
        if not reach:
            archive.add(item_id)
            continue
        all_terminal = all(
            item_id in terminal_by_pipeline[p] for p in reach
        )
        if all_terminal:
            archive.add(item_id)
        else:
            carryover.add(item_id)

    return Partition(
        carryover_ids=carryover,
        archive_ids=archive,
        item_reach=item_reach,
    )
```

- [ ] **Step 5.1: Failing tests** covering:
  - dropped=1 → archive regardless of pipeline state
  - reach empty (deleted entry pipeline) → archive
  - all reach terminal success → archive
  - one reach terminal, one missing → carryover
  - one reach status='failed' (terminal), other 'success' → archive
  - missing pipeline_<P> table for a reach pipeline → carryover (item still owes work)

- [ ] **Step 5.2-5.4: impl, PASS, commit.**

---

## Phase D — New Active + Carryover Copy

### Task 6: `build_new_active`

**File:** `sluice/dag/rotation/new_active.py`, `tests/dag/rotation/test_new_active.py`

Opens a fresh DB at the target path, applies base migrations + dynamic per-task tables. Uses `sluice.dag.db.open_active_db` (which already runs base migrations on open) + `sluice.dag.dynamic_tables.migrate_task_dynamic_tables`.

```python
# sluice/dag/rotation/new_active.py
from __future__ import annotations

from pathlib import Path

from sluice.dag.config import TaskConfig
from sluice.dag.db import open_active_db
from sluice.dag.dynamic_tables import migrate_task_dynamic_tables


async def build_new_active(new_path: str | Path, task: TaskConfig) -> None:
    p = Path(new_path)
    if p.exists():
        raise FileExistsError(f"refuse to overwrite existing file: {p}")
    p.parent.mkdir(parents=True, exist_ok=True)
    async with open_active_db(str(p)) as conn:
        await migrate_task_dynamic_tables(conn, task)
```

- [ ] Tests: file doesn't exist beforehand → success, schema_version row written, all per-pipeline tables exist; existing file → FileExistsError. Commit.

---

### Task 7: `copy_carryover`

**File:** `sluice/dag/rotation/copy.py`, `tests/dag/rotation/test_copy.py`

Bulk-copies carryover state from the old DB to the new DB. Uses SQLite's `ATTACH DATABASE` so we can write `INSERT INTO main.<table> SELECT … FROM old.<table>` natively.

**Order matters — every per-pipeline/per-stage child table has FK references to `pipelines.id` / `stages.id`. Copy MUST happen master-first or FK enforcement raises. Approach:**

1. Copy `items` rows for carryover ids.
2. **Resolve required master ids first** by reading the OLD DB (no inserts yet):
   - From old `pipeline_<P>` (for each P ∈ task.pipelines) where `item_id IN carryover ∩ reach(*, P)`: collect `last_run_id` → `pipeline_ids`.
   - From old `pipeline_<P>_sink_pending` similarly: collect `run_key` → look up `pipelines.id` for those keys → add to `pipeline_ids`.
   - From old `stage_<S>` where `pipeline_name=P AND item_id ∈ ...`: collect `last_pipeline_run_id` → `pipeline_ids`; collect `last_stage_run_id` → `stage_ids`.
   - From old `stage_attempt_log` for those (pipeline_name, stage_name, item_id) tuples: collect `pipeline_run_id` → `pipeline_ids`; collect `stage_run_id` → `stage_ids`.
3. **Insert masters first**: `pipelines` rows whose id ∈ `pipeline_ids`; `stages` rows whose id ∈ `stage_ids`.
4. **Then insert children**: per-pipeline tables, sink_pending, per-stage tables, stage_attempt_log.
5. Copy `sink_emissions` + `sink_delivery_log` for every `run_key` referenced by any copied `pipeline_<P>_sink_pending`.

To keep SQL simple: gate every copy by `id IN (?,?,...,?)` chunks of ≤ 400 (SQLite parameter limit guard).

The function takes a `Partition` (so it knows reach per item) and the validated `TaskConfig` (so it can enumerate per-pipeline / per-stage tables safely).

**Foreign keys stay ON throughout.** Master-first ordering plus the explicit ID-resolution walk satisfies every FK; disabling FK enforcement would mask the very kind of bug the v1 review caught (a missed master reference). Keeping FKs on means a future schema change that adds a new FK without a corresponding walk update will fail loudly in the rotation tests rather than silently produce a corrupt new DB.

Implementation:

```python
# sluice/dag/rotation/copy.py
from __future__ import annotations

import sqlite3
from collections.abc import Iterable

import aiosqlite

from sluice.dag._name import safe_name
from sluice.dag.config import PipelineDef, TaskConfig
from sluice.dag.rotation.partition import Partition
from sluice.dag.stage_driver import VOLATILE_TYPES

_CHUNK = 400  # < SQLite default 999 parameter limit, safe headroom


def _chunked(ids: Iterable):
    buf: list[str] = []
    for x in ids:
        buf.append(x)
        if len(buf) >= _CHUNK:
            yield buf
            buf = []
    if buf:
        yield buf


async def _ignore_no_such_table(coro):
    try:
        return await coro
    except sqlite3.OperationalError as exc:
        if "no such table" not in str(exc):
            raise
        return None


async def copy_carryover(
    new_conn: aiosqlite.Connection,
    *,
    old_path: str,
    partition: Partition,
    task: TaskConfig,
) -> None:
    """Copy carryover state from old DB into the already-opened new connection.

    Order: attachment_mirror (always) → items → resolve master ids by inspecting
    OLD → insert masters → insert children → copy sink rows. FKs stay ON; the
    master-first walk satisfies every constraint, and any future FK addition
    without a matching walk update will fail loudly in tests.

    `attachment_mirror` is copied unconditionally (even on full-archive
    rotations with zero carryover items) because it is a global URL → local
    file cache, not item-scoped state. Losing it on rotation forces every
    `mirror_attachments` run to re-download attachments that are still
    perfectly valid on disk.
    """
    await new_conn.execute("ATTACH DATABASE ? AS old", (old_path,))
    try:
        # 0. attachment_mirror — always copy, regardless of carryover.
        #    Keyed by url_hash, not item_id; whole-table is correct.
        await new_conn.execute(
            "INSERT OR IGNORE INTO main.attachment_mirror "
            "SELECT * FROM old.attachment_mirror"
        )

        if not partition.carryover_ids:
            return

        items_per_pipeline = _items_per_pipeline(partition)
        stage_def_by_name = {s.name: s for s in task.stages}

        # 1. items
        await _copy_items(new_conn, partition.carryover_ids)

        # NOTE: rotation_gate and pipeline_admissions are intentionally NOT
        # copied. They are runtime coordination state, not data:
        #   - rotation_gate: new DB ships with closed=0 from migration v2_0002
        #   - pipeline_admissions: by drain definition this table is empty on
        #     the OLD DB at this point (drain waited for it). Copying would
        #     either be a no-op or a bug indicator.

        # 2. Resolve required master ids by reading OLD only
        pipeline_ids: set[int] = set()
        stage_ids: set[int] = set()

        for pipeline in task.pipelines:
            ids = items_per_pipeline.get(pipeline.name, set())
            if not ids:
                continue
            safe_p = safe_name(pipeline.name)

            # pipeline_<P>.last_run_id → pipelines.id
            for chunk in _chunked(ids):
                ph = ",".join("?" for _ in chunk)
                rows = await _select_or_empty(
                    new_conn,
                    f"SELECT DISTINCT last_run_id FROM old.pipeline_{safe_p} "
                    f"WHERE item_id IN ({ph})",
                    tuple(chunk),
                )
                pipeline_ids.update(r[0] for r in rows if r[0] is not None)

            # pipeline_<P>_sink_pending.run_key → pipelines.run_key (then to id)
            if pipeline.sinks:
                for chunk in _chunked(ids):
                    ph = ",".join("?" for _ in chunk)
                    rows = await _select_or_empty(
                        new_conn,
                        f"SELECT DISTINCT run_key FROM old.pipeline_{safe_p}_sink_pending "
                        f"WHERE item_id IN ({ph})",
                        tuple(chunk),
                    )
                    sink_run_keys = [r[0] for r in rows]
                    for kchunk in _chunked(sink_run_keys):
                        kph = ",".join("?" for _ in kchunk)
                        cur = await new_conn.execute(
                            f"SELECT id FROM old.pipelines WHERE run_key IN ({kph})",
                            tuple(kchunk),
                        )
                        pipeline_ids.update(r[0] for r in await cur.fetchall())

            # stage_<S> for each non-volatile stage in this pipeline
            for stage_name in pipeline.stages:
                stage_def = stage_def_by_name[stage_name]
                if stage_def.type in VOLATILE_TYPES:
                    continue
                safe_s = safe_name(stage_name)
                for chunk in _chunked(ids):
                    ph = ",".join("?" for _ in chunk)
                    rows = await _select_or_empty(
                        new_conn,
                        f"SELECT DISTINCT last_pipeline_run_id, last_stage_run_id "
                        f"FROM old.stage_{safe_s} "
                        f"WHERE pipeline_name = ? AND item_id IN ({ph})",
                        (pipeline.name, *chunk),
                    )
                    for r in rows:
                        if r[0] is not None:
                            pipeline_ids.add(r[0])
                        if r[1] is not None:
                            stage_ids.add(r[1])

            # stage_attempt_log refs (per (pipeline_name, item_id))
            for chunk in _chunked(ids):
                ph = ",".join("?" for _ in chunk)
                rows = await _select_or_empty(
                    new_conn,
                    "SELECT DISTINCT pipeline_run_id, stage_run_id "
                    "FROM old.stage_attempt_log "
                    f"WHERE pipeline_name = ? AND item_id IN ({ph})",
                    (pipeline.name, *chunk),
                )
                for r in rows:
                    if r[0] is not None:
                        pipeline_ids.add(r[0])
                    if r[1] is not None:
                        stage_ids.add(r[1])

        # 3. Insert masters BEFORE any children (FK satisfaction)
        for chunk in _chunked(pipeline_ids):
            ph = ",".join("?" for _ in chunk)
            await new_conn.execute(
                f"INSERT OR IGNORE INTO main.pipelines "
                f"SELECT * FROM old.pipelines WHERE id IN ({ph})",
                tuple(chunk),
            )
        for chunk in _chunked(stage_ids):
            ph = ",".join("?" for _ in chunk)
            await new_conn.execute(
                f"INSERT OR IGNORE INTO main.stages "
                f"SELECT * FROM old.stages WHERE id IN ({ph})",
                tuple(chunk),
            )

        # 4. Children
        for pipeline in task.pipelines:
            ids = items_per_pipeline.get(pipeline.name, set())
            if not ids:
                continue
            await _copy_pipeline_state(new_conn, pipeline, ids)
            if pipeline.sinks:
                await _copy_sink_pending(new_conn, pipeline, ids)
            for stage_name in pipeline.stages:
                stage_def = stage_def_by_name[stage_name]
                if stage_def.type in VOLATILE_TYPES:
                    continue
                await _copy_stage_state(new_conn, stage_name, pipeline.name, ids)
                await _copy_stage_attempt_log(new_conn, stage_name, pipeline.name, ids)

        # 5. sink_emissions + sink_delivery_log keyed by run_keys in just-copied
        #    sink_pending rows. sqlite_master enumeration is safe: every name
        #    matched by the LIKE comes from a CREATE TABLE we issued via
        #    safe_name()-validated identifiers.
        await _copy_referenced_sink_rows(new_conn)
    finally:
        await new_conn.execute("DETACH DATABASE old")


def _items_per_pipeline(partition: Partition) -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    for item_id in partition.carryover_ids:
        for pipeline_name in partition.item_reach[item_id]:
            out.setdefault(pipeline_name, set()).add(item_id)
    return out


async def _select_or_empty(conn, sql: str, params: tuple) -> list:
    """Run SELECT, return rows; if table missing return []."""
    try:
        cur = await conn.execute(sql, params)
        return await cur.fetchall()
    except sqlite3.OperationalError as exc:
        if "no such table" not in str(exc):
            raise
        return []


async def _copy_items(conn, ids: set[str]) -> None:
    for chunk in _chunked(ids):
        ph = ",".join("?" for _ in chunk)
        await conn.execute(
            f"INSERT OR IGNORE INTO main.items "
            f"SELECT * FROM old.items WHERE item_id IN ({ph})",
            tuple(chunk),
        )


async def _copy_pipeline_state(conn, pipeline: PipelineDef, ids: set[str]) -> None:
    safe_p = safe_name(pipeline.name)
    for chunk in _chunked(ids):
        ph = ",".join("?" for _ in chunk)
        try:
            await conn.execute(
                f"INSERT OR IGNORE INTO main.pipeline_{safe_p} "
                f"SELECT * FROM old.pipeline_{safe_p} "
                f"WHERE item_id IN ({ph})",
                tuple(chunk),
            )
        except sqlite3.OperationalError as exc:
            if "no such table" not in str(exc):
                raise


async def _copy_sink_pending(conn, pipeline: PipelineDef, ids: set[str]) -> None:
    safe_p = safe_name(pipeline.name)
    for chunk in _chunked(ids):
        ph = ",".join("?" for _ in chunk)
        try:
            await conn.execute(
                f"INSERT OR IGNORE INTO main.pipeline_{safe_p}_sink_pending "
                f"SELECT * FROM old.pipeline_{safe_p}_sink_pending "
                f"WHERE item_id IN ({ph})",
                tuple(chunk),
            )
        except sqlite3.OperationalError as exc:
            if "no such table" not in str(exc):
                raise


async def _copy_stage_state(
    conn, stage_name: str, pipeline_name: str, ids: set[str]
) -> None:
    safe_s = safe_name(stage_name)
    for chunk in _chunked(ids):
        ph = ",".join("?" for _ in chunk)
        try:
            await conn.execute(
                f"INSERT OR IGNORE INTO main.stage_{safe_s} "
                f"SELECT * FROM old.stage_{safe_s} "
                f"WHERE pipeline_name = ? AND item_id IN ({ph})",
                (pipeline_name, *chunk),
            )
        except sqlite3.OperationalError as exc:
            if "no such table" not in str(exc):
                raise


async def _copy_stage_attempt_log(
    conn, stage_name: str, pipeline_name: str, ids: set[str]
) -> None:
    for chunk in _chunked(ids):
        ph = ",".join("?" for _ in chunk)
        await conn.execute(
            "INSERT OR IGNORE INTO main.stage_attempt_log "
            "SELECT * FROM old.stage_attempt_log "
            "WHERE pipeline_name = ? AND stage_name = ? "
            f"AND item_id IN ({ph})",
            (pipeline_name, stage_name, *chunk),
        )


async def _copy_referenced_sink_rows(conn) -> None:
    # Enumerate just-copied sink_pending tables. Names matched by this LIKE
    # pattern always come from CREATE TABLE statements gated by safe_name()
    # in dynamic_tables.py — no SQL-injection risk despite f-string interp.
    cur = await conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' "
        "AND name LIKE 'pipeline\\_%\\_sink\\_pending' ESCAPE '\\'"
    )
    pending_tables = [row[0] for row in await cur.fetchall()]
    run_keys: set[str] = set()
    for tbl in pending_tables:
        cur = await conn.execute(f"SELECT DISTINCT run_key FROM main.{tbl}")
        run_keys.update(row[0] for row in await cur.fetchall())
    for chunk in _chunked(run_keys):
        ph = ",".join("?" for _ in chunk)
        await conn.execute(
            f"INSERT OR IGNORE INTO main.sink_emissions "
            f"SELECT * FROM old.sink_emissions WHERE run_key IN ({ph})",
            tuple(chunk),
        )
        await conn.execute(
            f"INSERT OR IGNORE INTO main.sink_delivery_log "
            f"SELECT * FROM old.sink_delivery_log WHERE run_key IN ({ph})",
            tuple(chunk),
        )
    # Caller owns commit; do NOT commit here.
```

- [ ] **Step 7.1: Failing tests** covering:
  - Empty carryover but archive non-empty → `attachment_mirror` STILL gets copied (full-archive rotation must preserve the global URL cache); no other tables touched
  - Carryover with one item one pipeline → `pipelines` row appears BEFORE `pipeline_<P>` row (verify by setting `PRAGMA foreign_keys=ON` on the test connection and asserting no constraint error)
  - One item, multiple stages, mix of done/failed → all referenced `pipelines.id` and `stages.id` rows present
  - `pipeline_<P>.last_run_id` references a `pipelines.id` whose row is NOT touched by stage_attempt_log → still copied (the dedicated walk over `pipeline_<P>` resolves it)
  - sink_pending row carried + matching sink_emissions / sink_delivery_log copied via run_key
  - sink_pending references a `pipelines.run_key` whose pipeline_<P> row's `last_run_id` differs → both pipelines master rows copied
  - Items in archive set are NOT in new
  - Reach-aware: an item with reach={A,B} carries `pipeline_A` and `pipeline_B` but if reach is just {A}, `pipeline_B` rows are NOT copied
  - FK enforcement holds end-to-end: open the new DB after copy, run `PRAGMA foreign_key_check` → empty result set
  - **`attachment_mirror` whole-table copy**: seed old DB with N attachment_mirror rows (some referenced by carryover, some not), assert new DB has all N rows post-copy

- [ ] **Step 7.2-7.4: impl, PASS, commit.**

---

## Phase E — Seen Reconciliation

### Task 8: `reconcile_seen_for_archive`

**File:** `sluice/dag/rotation/seen_reconcile.py`, `tests/dag/rotation/test_seen_reconcile.py`

For each archive-set item, compute `source_hash` (same hash as `seen_post.py`) and `INSERT OR IGNORE INTO item_seen` with the rotation's `archived_at` timestamp.

Reads source rows from the OLD active DB (still readable; rotation hasn't moved files yet). Writes to seen DB.

```python
# sluice/dag/rotation/seen_reconcile.py
from __future__ import annotations

import aiosqlite

from sluice.dag._source_hash import source_hash_from_row
from sluice.dag.db import open_active_db, open_seen_db
from sluice.dag.stores.item_seen import ItemSeenStore


async def reconcile_seen_for_archive(
    *,
    old_active_path: str,
    seen_path: str,
    archive_ids: set[str],
    archived_at: str,
) -> int:
    """INSERT OR IGNORE INTO seen.item_seen for every archive item.

    Returns number of (item_id, source_hash) pairs offered (not necessarily new
    inserts; existing rows are ignored).
    """
    if not archive_ids:
        return 0

    async with open_active_db(old_active_path) as old_conn:
        pairs: list[tuple[str, str]] = []
        ids = list(archive_ids)
        # Chunked SELECT to stay under parameter limit
        chunk_size = 400
        for start in range(0, len(ids), chunk_size):
            chunk = ids[start : start + chunk_size]
            placeholders = ",".join("?" for _ in chunk)
            cur = await old_conn.execute(
                f"SELECT item_id, source_name, guid, url FROM items "
                f"WHERE item_id IN ({placeholders})",
                tuple(chunk),
            )
            for r in await cur.fetchall():
                row = {
                    "item_id": r[0], "source_name": r[1],
                    "guid": r[2], "url": r[3],
                }
                pairs.append((row["item_id"], source_hash_from_row(row)))

    async with open_seen_db(seen_path) as seen_conn:
        await ItemSeenStore(seen_conn).insert_many(pairs, archived_at=archived_at)
        await seen_conn.commit()
    return len(pairs)
```

- [ ] **Step 8.1: Failing tests** covering:
  - Empty archive → 0 written
  - Archive with N items → N rows in seen.item_seen (or all already-existing → 0 new but call succeeds)
  - source_hash matches the v2_0001 seen schema NOT NULL constraint (no items with all-null source attrs sneak through — produce an empty-string hash but still satisfies the column)

- [ ] **Step 8.2-8.4: impl, PASS, commit.**

**D4 extension:** add `sluice/dag/rotation/seen_reconcile.py` to the seen-allowed list in `tests/dag/test_d4_invariant.py`. Every other rotation file is forbidden from touching `item_seen` — extend the parametrized test:

```python
@pytest.mark.parametrize("path", [
    "sluice/dag/phases/tx2.py",
    "sluice/dag/emission.py",
    "sluice/dag/cleanup.py",
    "sluice/dag/rotation/lock.py",
    "sluice/dag/rotation/drain.py",
    "sluice/dag/rotation/reach.py",
    "sluice/dag/rotation/partition.py",
    "sluice/dag/rotation/new_active.py",
    "sluice/dag/rotation/copy.py",
    "sluice/dag/rotation/swap.py",
    "sluice/dag/rotation/orchestrator.py",
    "sluice/dag/rotation/admission.py",
    "sluice/dag/rotation/gate.py",
])
def test_d4_no_seen_writes_outside_seen_post_or_reconcile(path):
    src = Path(path).read_text()
    assert "ItemSeenStore" not in src, f"D4 violated: {path}"
    assert "INTO item_seen" not in src, f"D4 violated: {path}"
```

---

## Phase F — Atomic File Swap

### Task 9: `swap_active_file`

**File:** `sluice/dag/rotation/swap.py`, `tests/dag/rotation/test_swap.py`

Sequence:
1. **Checkpoint old AND new**: both DBs have WALs (the new one because `copy_carryover` just wrote to it). `PRAGMA wal_checkpoint(TRUNCATE)` on each, then close. Without checkpointing the new DB, `os.replace(new, active)` would lose any data still in `new.db-wal`.
2. **Clean up `-wal`/`-shm` companions on both paths** before the moves. If we move only `.db`, an orphan WAL at the destination path can corrupt the next open.
3. **Move old → archive temp**: `os.replace(old, archive_temp)`. Atomic on POSIX.
4. **Move new → active**: `os.replace(new, old_path)`. Atomic on POSIX. **If this fails, immediately move the archive temp back** so we never leave a hole at `old_path`.
5. **Gzip the archive temp**: stream-copy through `gzip.open`, then `os.unlink` source. This step crashing leaves the `.tmp` on disk for operator recovery; it does NOT affect the live system.

Anchor stamp uses `strftime("%Y%m%dT%H%M%S")` (15 chars, second precision) so two rotations in the same minute don't collide. If the same second is hit twice (extremely unlikely outside test loops), `FileExistsError` raises and operator rotates with `now=` override.

```python
# sluice/dag/rotation/swap.py
from __future__ import annotations

import gzip
import os
import shutil
from datetime import datetime
from pathlib import Path

import aiosqlite


async def _checkpoint(db_path: str) -> None:
    conn = await aiosqlite.connect(db_path)
    try:
        await conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        await conn.commit()
    finally:
        await conn.close()


def _clean_companions(base_path: str) -> None:
    for suffix in ("-wal", "-shm"):
        p = Path(base_path + suffix)
        if p.exists():
            p.unlink()


def _stamp_from_iso(iso: str) -> str:
    # ISO can be "2026-05-04T00:00:00+00:00" or with microseconds; normalize.
    dt = datetime.fromisoformat(iso)
    return dt.strftime("%Y%m%dT%H%M%S")


async def swap_active_file(
    *,
    old_active_path: str,
    new_active_path: str,
    archive_dir: str,
    task_name: str,
    cycle_anchor_iso: str,
) -> str:
    """Atomically swap the active file with the new one and archive the old.

    Returns the path of the resulting `.gz` archive.

    Failure modes:
      - Pre-replace failures: original active untouched (no destructive op yet).
      - Mid-replace (after old→archive_temp succeeded but new→active failed):
        the function moves archive_temp BACK to old_active_path before raising,
        so the live system is never left without an active file.
      - Post-replace gzip failure: live system is fine; archive_temp remains on
        disk for operator recovery (logged in the orchestrator).
    """
    Path(archive_dir).mkdir(parents=True, exist_ok=True)
    stamp = _stamp_from_iso(cycle_anchor_iso)
    archive_temp = Path(archive_dir) / f"{task_name}-{stamp}.db.tmp"
    archive_gz = Path(archive_dir) / f"{task_name}-{stamp}.db.gz"

    if archive_temp.exists():
        # A leftover .tmp means the previous rotation crashed AFTER the
        # active→archive_temp move but BEFORE gzip completed. The file is
        # the only surviving copy of that earlier active DB. Refuse to
        # auto-delete; require operator action (gzip manually or remove).
        raise FileExistsError(
            f"stale archive temp from a prior crash: {archive_temp}; "
            f"inspect, gzip-or-remove manually, then retry"
        )
    if archive_gz.exists():
        raise FileExistsError(f"archive already exists: {archive_gz}")

    # 1. Checkpoint BOTH DBs so all data is in the main file.
    await _checkpoint(old_active_path)
    await _checkpoint(new_active_path)

    # 2. Wipe orphan -wal/-shm on both paths BEFORE the moves so the
    #    destination's main file isn't paired with a stale WAL afterward.
    _clean_companions(old_active_path)
    _clean_companions(new_active_path)

    # 3. Move old → archive_temp (atomic).
    os.replace(old_active_path, archive_temp)

    # 4. Move new → active. On failure, undo step 3 so the system still has an
    #    active DB file at the expected path.
    try:
        os.replace(new_active_path, old_active_path)
    except Exception:
        try:
            os.replace(archive_temp, old_active_path)
        except Exception:
            pass  # best-effort rollback; original error propagates
        raise

    # Clean any companion files that materialized between steps (defensive).
    _clean_companions(old_active_path)

    # 5. Gzip archive_temp.
    with open(archive_temp, "rb") as src, gzip.open(archive_gz, "wb") as dst:
        shutil.copyfileobj(src, dst, length=1024 * 1024)
    archive_temp.unlink()
    return str(archive_gz)
```

- [ ] **Step 9.1: Failing tests**:
  - Old DB exists, new DB exists at temp → after swap, `<old_path>` contains the new DB's contents, `<archive_dir>/<task>-<stamp>.db.gz` exists
  - WAL/SHM files cleaned up on both old and new paths
  - **Data in new DB's WAL is preserved across swap**: write rows to new DB, do NOT explicitly checkpoint, run `swap_active_file`, open the resulting active file, assert rows are readable (proves checkpoint ran)
  - Archive `.gz` is a valid gzip file containing the old DB's `items` rows
  - **Rollback path**: monkey-patch the second `os.replace` to raise once, assert the original active path STILL contains the old DB (rollback restored it), assert archive_temp no longer exists at the temp location
  - Re-running with same anchor (`strftime` 15-char stamp) raises FileExistsError on existing `.gz` (no silent overwrite)
  - **Stale `.tmp` from prior crash refuses to auto-delete**: pre-create `<archive_dir>/<task>-<stamp>.db.tmp`, run swap, assert `FileExistsError` mentioning "stale archive temp"; assert the pre-created `.tmp` file is still on disk (operator can recover it manually)
  - Two anchors in the same minute but different seconds → distinct archive paths (no collision)

- [ ] **Step 9.2-9.4: impl, PASS, commit.**

---

## Phase G — Orchestrator + Config + CLI

### Task 10: `SluiceConfig` + `RotationConfig`

**File:** `sluice/dag/sluice_config.py`, `tests/dag/test_sluice_config.py`

Loads `sluice.toml` from the project root (or `--config` override). Plan 3 only consumes `[gc.rotation]`; other sections deferred to Plan 4.

```toml
# sluice.toml
[gc.rotation]
drain_timeout = "30m"
archive_dir   = "data/archive"
```

```python
# sluice/dag/sluice_config.py
from __future__ import annotations

import re
import tomllib
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class RotationConfig:
    drain_timeout_seconds: float = 30 * 60.0
    archive_dir: str = "data/archive"


@dataclass(frozen=True)
class SluiceConfig:
    rotation: RotationConfig = field(default_factory=RotationConfig)


def _parse_duration(text: str, default: float) -> float:
    if text is None:
        return default
    text = text.strip().lower()
    if not text:
        return default
    m = re.fullmatch(r"(\d+(?:\.\d+)?)([smhd])", text)
    if not m:
        raise ValueError(f"cannot parse duration: {text!r}")
    value, unit = float(m.group(1)), m.group(2)
    return value * {"s": 1, "m": 60, "h": 3600, "d": 86400}[unit]


def load_sluice_config(config_path: Path | None = None) -> SluiceConfig:
    path = config_path or Path("sluice.toml")
    if not path.exists():
        return SluiceConfig()
    data = tomllib.loads(path.read_text())
    rotation = data.get("gc", {}).get("rotation", {})
    return SluiceConfig(
        rotation=RotationConfig(
            drain_timeout_seconds=_parse_duration(
                rotation.get("drain_timeout", "30m"), 30 * 60.0,
            ),
            archive_dir=rotation.get("archive_dir", "data/archive"),
        ),
    )
```

- [ ] Tests: missing sluice.toml → defaults; valid file → parsed values; invalid duration → raises. Commit.

---

### Task 11: `rotate_task` orchestrator

**File:** `sluice/dag/rotation/orchestrator.py`, `tests/dag/rotation/test_orchestrator.py`

Top-level coroutine. Sequence:

```
0. validate_task(task) — fail fast on bad config; reach computation depends on it.
1. Try acquire RotationLock; FileExistsError → lock_busy result, return.
2. close_gate(task.db_path) — atomic UPDATE on rotation_gate. From now on,
   any new admission attempt on the OLD active raises RotationGateClosed.
3. Drain in-flight: wait for pipelines.running=0 AND pipeline_admissions=0
   for the task. On timeout, open_gate (so future runs aren't permanently
   stuck), release lock, return drain_timeout.
4. Open old DB read-only; partition_items().
5. Build new active at <task.db_path>.new (DDL + dynamic tables; the new
   DB has rotation_gate.closed=0 by default from the migration).
6. copy_carryover() into new (also copies attachment_mirror).
   IMPORTANT: copy_carryover does NOT carry pipeline_admissions or
   rotation_gate state — those are runtime coordination tables, not data.
7. reconcile_seen_for_archive() against the OLD path (still on disk).
8. swap_active_file(old, new, archive_dir).
9. Release lock. (No need to open_gate on the new DB — it ships open.)
10. Return RotationResult with counts + archive path.

On any failure between steps 2 and 8, run open_gate(task.db_path) BEFORE
releasing the lock, so the OLD active DB is left in a usable state for the
next run. (After a successful swap, the new DB is already open.)
```

**`attachment_mirror` policy:** the active schema has an `attachment_mirror` table (URL → local file metadata, used by `mirror_attachments` stage to avoid re-downloading). Plan 3 copies the **entire** `attachment_mirror` table (not item-scoped) because:
- Mirror entries are keyed by `url_hash`, not `item_id`, so we cannot filter by carryover ids.
- Losing the mirror cache means the next run re-downloads everything previously mirrored — a real regression.
- The table is small (one row per unique attachment URL); copying it whole costs nothing.

This is implemented as a one-line addition to `copy_carryover` (after `_copy_items`):

```python
await new_conn.execute(
    "INSERT OR IGNORE INTO main.attachment_mirror SELECT * FROM old.attachment_mirror"
)
```

(Note: this whole-table copy is only correct because the mirror table contents are valid for the new active DB regardless of which items survived. Future schemas adding per-task or per-item filtering would need to revisit.)

Error handling:
- Step 0 failure (validation): raise; nothing acquired yet.
- Step 1 failure (lock_busy): return result; nothing acquired.
- Step 2 timeout: return drain_timeout result; lock released by `finally`.
- Steps 3-6 failure: delete the partial new DB, raise (lock released by `finally`).
- Step 7 failure: rollback inside `swap_active_file` restored old at active path; lock released by `finally`. Operator inspects logs.

```python
# sluice/dag/rotation/orchestrator.py
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from sluice.dag.config import TaskConfig
from sluice.dag.db import open_active_db
from sluice.dag.rotation.copy import copy_carryover
from sluice.dag.rotation.drain import drain_running_pipelines
from sluice.dag.rotation.lock import RotationLock
from sluice.dag.rotation.new_active import build_new_active
from sluice.dag.rotation.partition import partition_items
from sluice.dag.rotation.seen_reconcile import reconcile_seen_for_archive
from sluice.dag.rotation.swap import swap_active_file
from sluice.dag.rotation.gate import close_gate, open_gate
from sluice.dag.sluice_config import SluiceConfig
from sluice.dag.validator import validate_task

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RotationResult:
    task_name: str
    status: str        # "rotated" | "no_op" | "lock_busy" | "drain_timeout"
    carryover_count: int
    archive_count: int
    archive_path: str | None
    error_msg: str | None = None


async def rotate_task(
    task: TaskConfig,
    *,
    config: SluiceConfig,
    now: datetime | None = None,
) -> RotationResult:
    validate_task(task)
    now = now or datetime.now(timezone.utc)
    anchor_iso = now.isoformat()

    lock = RotationLock(task.db_path)
    try:
        lock.acquire()
    except RuntimeError as exc:
        return RotationResult(
            task_name=task.name, status="lock_busy",
            carryover_count=0, archive_count=0, archive_path=None,
            error_msg=str(exc),
        )

    swap_succeeded = False
    try:
        await close_gate(task.db_path)

        drained = await drain_running_pipelines(
            task.db_path, task.name,
            timeout_seconds=config.rotation.drain_timeout_seconds,
        )
        if not drained:
            await open_gate(task.db_path)
            return RotationResult(
                task_name=task.name, status="drain_timeout",
                carryover_count=0, archive_count=0, archive_path=None,
                error_msg="drain timed out; will retry next cycle",
            )

        async with open_active_db(task.db_path) as conn_old:
            partition = await partition_items(conn_old, task)

        if not partition.carryover_ids and not partition.archive_ids:
            await open_gate(task.db_path)
            return RotationResult(
                task_name=task.name, status="no_op",
                carryover_count=0, archive_count=0, archive_path=None,
            )

        new_path = str(Path(str(task.db_path) + ".new"))
        if Path(new_path).exists():
            Path(new_path).unlink()

        try:
            await build_new_active(new_path, task)
            async with open_active_db(new_path) as conn_new:
                await copy_carryover(
                    conn_new, old_path=task.db_path,
                    partition=partition, task=task,
                )
                await conn_new.commit()

            await reconcile_seen_for_archive(
                old_active_path=task.db_path,
                seen_path=task.seen_db,
                archive_ids=partition.archive_ids,
                archived_at=anchor_iso,
            )

            archive_path = await swap_active_file(
                old_active_path=task.db_path,
                new_active_path=new_path,
                archive_dir=config.rotation.archive_dir,
                task_name=task.name,
                cycle_anchor_iso=anchor_iso,
            )
            swap_succeeded = True
        except Exception:
            if Path(new_path).exists():
                Path(new_path).unlink()
            raise

        return RotationResult(
            task_name=task.name, status="rotated",
            carryover_count=len(partition.carryover_ids),
            archive_count=len(partition.archive_ids),
            archive_path=archive_path,
        )
    except Exception:
        # Pre-swap failure: leave OLD DB usable by reopening its gate.
        # Post-swap (swap_succeeded=True) the new DB is already gate-open,
        # so this branch is a no-op then.
        if not swap_succeeded:
            try:
                await open_gate(task.db_path)
            except Exception:
                logger.warning("failed to reopen rotation_gate; manual reset may be required")
        raise
    finally:
        lock.release()
```

- [ ] **Step 11.1: Failing tests**:
  - Lock-busy → `lock_busy` status, no file changes
  - Drain timeout (seed a `running` pipeline, set tiny timeout) → `drain_timeout` status, no file changes
  - Empty task (no items) → `no_op` status
  - Happy path (mix of carryover + archive items) → `rotated`, counts match, `<task>.db.gz` exists, new DB at `<task>.db` has only carryover items, seen DB has new rows for archive items
  - Pre-swap exception (mock `swap_active_file` to raise) → no archive .gz, original DB intact, new path cleaned up

- [ ] **Step 11.2-11.4: impl, PASS, commit.**

---

### Task 12: CLI command `sluice gc-rotate -t <task>`

**File:** `sluice/cli.py`, `tests/dag/test_cli_gc.py`

```python
@app.command("gc-rotate")
def gc_rotate(
    task: str = typer.Option(..., "-t", "--task"),
    config_dir: Path = typer.Option(Path("./configs"), "-c", "--config-dir"),
    sluice_config_path: Path | None = typer.Option(
        None, "--sluice-config", help="Path to sluice.toml; defaults to ./sluice.toml"
    ),
    dry_run: bool = typer.Option(False, "--dry-run",
                                 help="Compute partition only; no file writes"),
):
    """Rotate the active DB for a task (drain, partition, archive)."""
    task_dir = config_dir / "tasks" / task
    if not task_dir.exists():
        typer.echo(f"task not found: {task_dir}", err=True)
        raise typer.Exit(code=2)

    from sluice.dag.loader import load_task
    from sluice.dag.rotation.orchestrator import rotate_task
    from sluice.dag.rotation.partition import partition_items
    from sluice.dag.sluice_config import load_sluice_config
    from sluice.dag.db import open_active_db

    cfg = load_task(task_dir)
    sluice_cfg = load_sluice_config(sluice_config_path)

    if dry_run:
        async def _dry():
            async with open_active_db(cfg.db_path) as conn:
                p = await partition_items(conn, cfg)
            return p
        partition = asyncio.run(_dry())
        typer.echo(json.dumps({
            "carryover_count": len(partition.carryover_ids),
            "archive_count": len(partition.archive_ids),
        }, ensure_ascii=False))
        return

    result = asyncio.run(rotate_task(cfg, config=sluice_cfg))
    typer.echo(json.dumps({
        "task": result.task_name,
        "status": result.status,
        "carryover": result.carryover_count,
        "archive": result.archive_count,
        "archive_path": result.archive_path,
        "error": result.error_msg,
    }, ensure_ascii=False))
    # rotated/no_op = work done; lock_busy/drain_timeout = transient retry-next
    # state, NOT an error (cron will retry next cycle). Only true failures —
    # validation, FS errors, FK constraint, etc. — raise as exit 1.
    if result.status not in {"rotated", "no_op", "lock_busy", "drain_timeout"}:
        raise typer.Exit(code=1)
```

- [ ] Tests via subprocess: dry-run prints counts; real run rotates and prints `archive_path`. Commit.

---

### Task 13: End-to-end test

**File:** `tests/dag/rotation/test_e2e.py`

Scenario: real-ish task with one entry pipeline `daily` (sources, fetch_rss → render → file_md sink). All counts below are deterministic, derived from the partition rules in Phase C.

1. Build fixture task with a frozen RSS feed that emits 4 items: `i1, i2, i3, i4`.
2. Run `daily` once → all 4 reach `pipeline_daily.status='success'`. Database now has 4 items, all terminal.
3. Mutate state DIRECTLY (no second pipeline run — keeps the test scenario crisp):
   - Set `items.dropped = 1` for `i2` (global tombstone).
   - Delete `pipeline_daily` row for `i3` (simulates a transient failure under max_attempts: row was never written this run, item is non-terminal).
   - Leave `i1` and `i4` untouched (terminal success).
4. Invoke `await rotate_task(task, config=...)`.
5. Assert:
   - `result.status == "rotated"`
   - `result.archive_count == 3`: `i1`, `i2`, `i4` (i1 + i4 terminal-success; i2 dropped).
   - `result.carryover_count == 1`: `i3` (no terminal row, not dropped → still owes work).
   - **`seen.item_seen` already has rows for all 4 items** (Plan 2's `seen_post` job wrote them at initial fetch on step 2). `reconcile_seen_for_archive` runs `INSERT OR IGNORE` for `i1`, `i2`, `i4`; all three are no-ops because rows already exist. The new row count delta in `seen.item_seen` for this rotation is 0 — and that is correct.
   - New active DB at `<task.db_path>` has exactly 1 row in `items`: `i3`. The carryover walk preserved its `pipeline_daily` absence and any `stage_<S>` non-terminal rows.
   - `<archive_dir>/<task>-<stamp>.db.gz` exists. Gunzip → valid SQLite file with all 4 original items.
6. Run `daily` once more with the SAME frozen RSS:
   - `i1`, `i2`, `i4` are blocked by `seen.item_seen` (skip via `compute_skip_ids`).
   - `i3` is in active.items still (carryover) and not in `pipeline_daily` → `select_entry_carryover` picks it up, the run finishes it through to `pipeline_daily.status='success'`.
7. Invoke `rotate_task` again:
   - `result.status == "rotated"`
   - `result.archive_count == 1` (`i3` is now terminal everywhere it owes work).
   - `result.carryover_count == 0`.
   - A third rotation immediately after this one would return `no_op` (empty items table after archive of `i3`).

8. **Concurrency property test** (separate `tests/dag/rotation/test_e2e_concurrency.py`):
   - Spawn an `asyncio.gather(rotate_task(...), run_pipeline(..., now=fixed))`.
   - Assert no FK violation in the resulting DB (`PRAGMA foreign_key_check` empty).
   - Assert exactly one of the following holds:
     (a) rotation completed, run_pipeline returned `skipped_rotation`, and `pipeline_admissions` is empty after both finish.
     (b) run_pipeline completed (status='success'), rotation returned either `lock_busy` or `drain_timeout` (depending on poll timing), and the active DB still contains the run's tx2 outputs.
   - This is the property the SQL admission gate exists to enforce.

- [ ] Build fixture, write test, iterate to PASS, commit.

---

## Self-Review Checklist

1. **D1 unaffected**: rotation does not start a `WriterThread`; existing writer-using tests still pass.
2. **D2 unaffected**: `build_new_active` follows the same `open_active_db` + `migrate_task_dynamic_tables` pre-flight pattern.
3. **D4 extended**: `tests/dag/test_d4_invariant.py` now covers all rotation files; only `seen_post.py` and `seen_reconcile.py` are allowed to write `item_seen`.
4. **Reconcile-before-swap is the SAFE failure direction.** Reconcile runs BEFORE the file swap (Step 6 → 7). If reconcile succeeds and swap then fails, seen ends up with rows for items that are still in the (not-yet-rotated) active DB. This "seen ahead of active" state is benign: future fetch_rss runs skip those items (they're already terminal in active too, so skip is correct), and a retried rotation will recompute the partition and `INSERT OR IGNORE` the same rows (idempotent). The opposite ordering — reconcile AFTER swap — would have the dangerous failure mode: post-swap items have neither an active row nor a seen row, so a fresh fetch would re-emit them as "new". We accept the benign window over the dangerous one.
5. **Sink-pending preservation**: a carryover item with a sink-pending row keeps both the row and the sink_emissions ordinals, so the next pipeline run's `resolve_run_key` adopts the same key and emit_all skips already-delivered ordinals.
6. **Unsafe identifier protection**: every per-pipeline / per-stage SQL identifier flows through `_safe()`. Validator already rejects unsafe names at config load.
7. **WAL checkpoint before swap**: ensures the file-level move captures all data; otherwise `<old>.db-wal` would lag.
8. **Lock auto-clears on dead PID**: a crashed rotation (process killed) leaves a lock file with a dead PID; the next `is_held()` call clears it unconditionally — a dead process cannot resume rotation, so holding the lock for it serves no purpose. `stale_after_seconds` only applies to malformed lock files (parse-failure on pid/timestamp): those are cleared after the threshold elapses, treating them as held in the meantime.
9. **Orchestrator counts**: `carryover_count` and `archive_count` both come from the partition step. They don't include items that arrived AFTER drain (impossible because the lock is held).
10. **Empty pipelines**: a task with no `pipelines` would skip Plan 2's runs entirely; rotation on such a task partitions all items as archive (reach is empty for all entries) — correct fall-out.
11. **Archive .gz determinism**: anchor_iso is normalized to a stamp like `20260504T120000` via `strftime("%Y%m%dT%H%M%S")` (15 chars, second precision). Collisions only occur if rotation runs twice within the same second (`FileExistsError`). Operator override: pass an explicit `now=` to the orchestrator to retry under a different stamp. A leftover `.tmp` from a prior crash also raises `FileExistsError` and refuses to auto-delete — operator must inspect.
12. **Plan 2 contract preservation**: `run_pipeline.py` only adds the rotation gate; existing behavior for non-rotation runs is unchanged. All Plan 2 tests pass.
13. **`sluice.toml` parser scope**: only `[gc.rotation]` is consumed; other sections (e.g. future `[prefect]`, `[gc.schedule]`) are ignored without error.
14. **FK ordering**: `copy_carryover` resolves all required `pipelines.id` and `stages.id` from the OLD DB by walking every child table (`pipeline_<P>.last_run_id`, `pipeline_<P>_sink_pending.run_key → pipelines.id`, `stage_<S>.last_pipeline_run_id`, `stage_<S>.last_stage_run_id`, `stage_attempt_log.pipeline_run_id`, `stage_attempt_log.stage_run_id`) and inserts those master rows BEFORE any child row. **Foreign keys stay ON** for the duration — disabling them would mask the very bug the v1 review caught (a missed master reference). A test runs `PRAGMA foreign_key_check` on the new DB after copy → must be empty; a future schema change that adds a new FK without a matching walk update will fail this test instantly.
15. **Swap rollback**: if `os.replace(new → active)` fails, the swap function moves the archive temp BACK to the active path so the live system never lacks an active file. Test mock-injects this failure and asserts old data is intact at `<task.db_path>`.
16. **Both DBs checkpointed before swap**: the new DB's WAL contains carryover writes; without `PRAGMA wal_checkpoint(TRUNCATE)` on it before `os.replace`, those writes would be lost. Test writes rows to new without explicit checkpoint, runs swap, asserts rows readable post-swap.
17. **WAL/SHM cleanup**: orphan `<path>-wal` / `<path>-shm` left over from previous runs are removed before any `os.replace` to prevent the destination from being paired with a stale WAL.
18. **Anchor stamp precision**: `_stamp_from_iso` uses `strftime("%Y%m%dT%H%M%S")` (15 chars, second precision). Two rotations in the same minute don't collide.
19. **Validation up-front**: orchestrator calls `validate_task(task)` before everything else. `compute_item_reach` therefore propagates any unexpected `ValidationError` (programming error) instead of silently archiving items.
20. **Lock acquire race**: orchestrator uses `try: lock.acquire() except RuntimeError → lock_busy`. The `O_EXCL` race inside `acquire` raises `FileExistsError` which the lock translates to `RuntimeError` for caller uniformity.
21. **In-process lock layer**: `RotationLock._inprocess_holds` is a class-level set so two `RotationLock(same_path)` instances within the same process see each other's hold instantly, before any FS sync.
22. **Writer lifecycle lock**: `WriterThread.start()` acquires `RotationLock` by default and `stop()` releases it. This covers programmatic / long-lived idle writers that never pass through the CLI. The CLI is the only caller that disables the writer's internal lock because it already holds the same lock across preflight and writer lifetime.
23. **Shared helpers**: `safe_name` and `source_hash_from_row` live in `sluice/dag/_name.py` and `sluice/dag/_source_hash.py` respectively. `seen_post.py` and `seen_reconcile.py` import the same hash function — drift between them is impossible.
24. **Volatile types from one source**: `copy.py` imports `VOLATILE_TYPES` from `sluice/dag/stage_driver.py`. Adding a new volatile stage type requires only one constant update.
25. **`attachment_mirror` policy**: copied whole-table (not per-item) because the table is keyed by `url_hash`, not `item_id`. Cache loss would force re-downloading; the table is small.
26. **Drain zombie limitation**: documented in `drain.py` docstring. Plan 4 adds zombie detection; Plan 3 operator workaround is a manual UPDATE/DELETE on `pipelines` and `pipeline_admissions` before `gc-rotate`.
27. **Admission protocol race-freeness**: `RotationLock.is_held()` is an advisory fast-path only. The authoritative quiescence mechanism is the SQL gate (`rotation_gate.closed`) plus admission registry (`pipeline_admissions`). Both `close_gate` (rotation side) and `build_admission_job` (runner side) execute under `BEGIN IMMEDIATE`, so SQLite serializes their writes. Either the admission commits before close_gate (drain sees the row and waits) or close_gate commits first (admission reads `closed=1` and runner returns `skipped_rotation`). No interleaving leaves a pipeline in flight with an open admission window.
28. **`pipeline_admissions` not carried over**: `copy_carryover` does NOT copy `pipeline_admissions` rows. Drain ensures the table is empty on the old DB at swap time; copying would either be a no-op or indicate a drain bug. Same reasoning for `rotation_gate`: the new DB starts fresh with `closed=0` from migration v2_0002.
29. **Gate reopen on failure**: orchestrator's `except` branch calls `open_gate(task.db_path)` if the swap did NOT succeed, so a pre-swap failure (validation, copy, reconcile, or swap step itself) doesn't leave the OLD active DB permanently gated. After successful swap, the new DB ships open.
30. **Migration ordering**: `v2_0002_active_rotation_gate.sql` is applied automatically by `apply_migrations` on first writer open of any existing v1 DB. Rotation infrastructure is therefore available everywhere without an explicit upgrade step.

---

## Out of Scope (deferred to Plan 4)

- Prefect-native pause / resume (replace `RotationLock` with deployment pause API).
- Cron-driven rotation scheduling (`[gc.schedule]` parse + Prefect deployment registration).
- Multi-host rotation coordination (would need a distributed lock; current single-host model sufficient).
- Archive retention / pruning policy (gz files accumulate forever in Plan 3; Plan 4 adds retention).
- Operator dashboards and rotation telemetry.
- **Zombie pipeline detection**: `pipelines.status='running'` rows AND `pipeline_admissions` rows from crashed writers permanently block drain. Plan 4 adds `--force` + age-based auto-fail. Plan 3 operator workaround: manual UPDATE / DELETE on those two tables before `gc-rotate`.

---

## Execution Handoff

**Subagent-Driven (recommended)** — fresh subagent per task, review between tasks. With 17 task slices (`0`, `1`, `2`, `2b`, `2c`, `3`-`13`) the load is moderate; Phase D (copy.py) is the largest single piece.

Alternative: **Inline Execution** — current session, batch checkpoints every 4 tasks (after Task 4, 8, 12).

Which approach?
