# Sluice v3 Core Redesign Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the unsafe runner core with cycle-scoped state, durable sink outbox delivery, leased running work, and crash-recoverable rotation while preserving existing config and stage APIs.

**Architecture:** Add a v3 active-db migration, then move runner writes onto `cycle_id` and `run_id`. External sink delivery is persisted as outbox rows before dispatch. Rotation uses a manifest to make every file transition recoverable.

**Tech Stack:** Python 3.12, `aiosqlite`, existing `WriteJob`/`WriterThread`, pytest, ruff, ty.

---

### Task 1: Add v3 Core Migration

**Files:**
- Create: `sluice/dag/migrations/v2_0003_active_v3_core.sql`
- Modify: `tests/dag/test_db_migrations.py` or create if missing

**Step 1: Write failing migration tests**

Create tests that open an active DB at migration version 2, apply migrations,
and assert:

- `pipelines` has `cycle_id`, `lease_until`, `heartbeat_at`;
- `pipeline_admissions` has `run_id`, `lease_until`, `heartbeat_at`;
- `sink_outbox` exists with `UNIQUE(run_id, sink_id, ordinal)`;
- `rotation_manifest` exists;
- legacy dynamic `pipeline_<name>` and `stage_<name>` tables can be migrated to
  include `cycle_id`.

Run:

```bash
rtk env COVERAGE_FILE=/tmp/sluice-v3-migration.coverage uv run pytest -q tests/dag/test_db_migrations.py
```

Expected: FAIL because migration does not exist.

**Step 2: Add migration**

Implement `v2_0003_active_v3_core.sql` with `ALTER TABLE` for fixed tables and
documented helper SQL for dynamic tables where SQLite cannot loop. Dynamic
table conversion may be done by Python migration helper in Task 2 if raw SQL is
not enough.

**Step 3: Verify**

Run the migration tests until they pass.

**Step 4: Commit**

```bash
rtk git add sluice/dag/migrations/v2_0003_active_v3_core.sql tests/dag/test_db_migrations.py
rtk git commit -m "feat(schema): Add v3 core runtime tables"
```

### Task 2: Make Pipeline and Stage State Cycle-Scoped

**Files:**
- Modify: `sluice/dag/dynamic_tables.py`
- Modify: `sluice/dag/stores/pipeline_state.py`
- Modify: `sluice/dag/stores/stage_state.py`
- Modify: `sluice/dag/phases/tx1.py`
- Modify: `sluice/dag/phases/tx2.py`
- Modify: `sluice/dag/phases/resume.py`
- Test: `tests/dag/test_runtime_stores.py`
- Test: `tests/dag/test_run_pipeline.py`

**Step 1: Write failing tests**

Assert that two runs with the same `item_id` but different `cycle_id` produce
two pipeline-state rows and separate stage attempts.

**Step 2: Update dynamic table creation**

New `pipeline_<name>` tables must use:

```sql
PRIMARY KEY (cycle_id, item_id)
```

New `stage_<name>` tables must use:

```sql
PRIMARY KEY (pipeline_name, cycle_id, item_id)
```

**Step 3: Thread `cycle_id` through stores**

Store APIs should require `cycle_id` for read/write paths that represent
cycle-local state. Attempts and done lookups must include `cycle_id`.

**Step 4: Update tx1/tx2/resume**

Use `cycle_id = cycle_anchor` initially. Keep `run_key` unchanged for external
compatibility.

**Step 5: Verify and commit**

```bash
rtk env COVERAGE_FILE=/tmp/sluice-v3-state.coverage uv run pytest -q tests/dag/test_runtime_stores.py tests/dag/test_run_pipeline.py
rtk ruff check sluice/dag tests/dag/test_runtime_stores.py tests/dag/test_run_pipeline.py
rtk git add sluice/dag/dynamic_tables.py sluice/dag/stores/pipeline_state.py sluice/dag/stores/stage_state.py sluice/dag/phases/tx1.py sluice/dag/phases/tx2.py sluice/dag/phases/resume.py tests/dag/test_runtime_stores.py tests/dag/test_run_pipeline.py
rtk git commit -m "feat(runtime): Scope state by cycle"
```

### Task 3: Add Durable Sink Outbox

**Files:**
- Create: `sluice/dag/stores/sink_outbox.py`
- Modify: `sluice/dag/emission.py`
- Modify: `sluice/dag/phases/tx2.py`
- Modify: `sluice/dag/run_pipeline.py`
- Test: `tests/dag/test_emission.py`
- Test: `tests/dag/test_run_pipeline.py`

**Step 1: Write failing tests**

Cover:

- tx2 creates one outbox row per sink chunk before external send;
- dispatch marks rows sent and records `sink_delivery_log`;
- retry reuses `idempotency_key`;
- crash between send and record does not rebuild payload.

**Step 2: Implement outbox store**

Provide `enqueue_many`, `claim_pending`, `mark_sent`, `mark_retryable`, and
`mark_failed` methods.

**Step 3: Split enqueue from dispatch**

`SinkEmitter` should stop doing `_already_emitted -> emit -> _record`. It should
enqueue deterministic payload rows and dispatch claimed rows.

**Step 4: Update pipeline status semantics**

Use `pending_delivery` when eligible items exist but outbox rows remain pending
or retryable.

**Step 5: Verify and commit**

```bash
rtk env COVERAGE_FILE=/tmp/sluice-v3-outbox.coverage uv run pytest -q tests/dag/test_emission.py tests/dag/test_run_pipeline.py
rtk git add sluice/dag/stores/sink_outbox.py sluice/dag/emission.py sluice/dag/phases/tx2.py sluice/dag/run_pipeline.py tests/dag/test_emission.py tests/dag/test_run_pipeline.py
rtk git commit -m "feat(sinks): Deliver through durable outbox"
```

### Task 4: Add Leases and Zombie Recovery

**Files:**
- Modify: `sluice/dag/rotation/admission.py`
- Modify: `sluice/dag/rotation/drain.py`
- Modify: `sluice/dag/stores/pipelines.py`
- Modify: `sluice/dag/run_pipeline.py`
- Test: `tests/dag/rotation/test_drain.py`
- Test: `tests/dag/test_run_pipeline.py`

**Step 1: Write failing tests**

Assert that expired admissions and expired running pipelines are cleaned during
drain and do not block rotation.

**Step 2: Add lease helpers**

Admissions and pipeline rows get `lease_until` and `heartbeat_at`. Use a
configurable but initially hard-coded lease duration of 10 minutes.

**Step 3: Heartbeat active runs**

Runner refreshes lease before/after long phases. Keep the implementation simple:
heartbeat around stage execution and post-tx1 work, then finalize clears lease.

**Step 4: Drain recovery**

Drain marks expired running rows `failed` with `error_msg='lease_expired'` and
deletes expired admissions.

**Step 5: Verify and commit**

```bash
rtk env COVERAGE_FILE=/tmp/sluice-v3-lease.coverage uv run pytest -q tests/dag/rotation/test_drain.py tests/dag/test_run_pipeline.py
rtk git add sluice/dag/rotation/admission.py sluice/dag/rotation/drain.py sluice/dag/stores/pipelines.py sluice/dag/run_pipeline.py tests/dag/rotation/test_drain.py tests/dag/test_run_pipeline.py
rtk git commit -m "feat(runtime): Recover expired pipeline leases"
```

### Task 5: Add Rotation Manifest and Recovery

**Files:**
- Create: `sluice/dag/rotation/manifest.py`
- Modify: `sluice/dag/rotation/orchestrator.py`
- Modify: `sluice/dag/rotation/swap.py`
- Test: `tests/dag/rotation/test_manifest.py`
- Test: `tests/dag/rotation/test_orchestrator.py`

**Step 1: Write failing tests**

Simulate crashes at:

- copied new DB before seen reconcile;
- archived old active before installing new active;
- installed new active before gzip;
- gzip temp exists.

Expected recovery either completes the phase or fails closed with manifest paths.

**Step 2: Implement manifest store**

Write phase transitions to `rotation_manifest` inside the active DB before file
operations whenever the active DB is still available.

**Step 3: Refactor swap**

Expose smaller operations: checkpoint, archive old, install new, gzip archive.
Each operation updates manifest before and after the file move.

**Step 4: Startup/pre-rotation recovery**

Before rotation closes the gate, call recovery. It must inspect active/new/temp
paths and finish safe incomplete work.

**Step 5: Verify and commit**

```bash
rtk env COVERAGE_FILE=/tmp/sluice-v3-rotation.coverage uv run pytest -q tests/dag/rotation/test_manifest.py tests/dag/rotation/test_orchestrator.py
rtk git add sluice/dag/rotation/manifest.py sluice/dag/rotation/orchestrator.py sluice/dag/rotation/swap.py tests/dag/rotation/test_manifest.py tests/dag/rotation/test_orchestrator.py
rtk git commit -m "feat(rotation): Recover manifest-tracked swaps"
```

### Task 6: Full Verification

Run:

```bash
rtk env COVERAGE_FILE=/tmp/sluice-v3-full.coverage uv run pytest -q
rtk ruff check .
rtk uv run ty check
rtk git diff --check
```

Update README test count if changed, then commit docs if needed.
