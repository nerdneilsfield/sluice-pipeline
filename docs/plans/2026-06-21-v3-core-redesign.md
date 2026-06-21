# Sluice v3 Core Redesign Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the unsafe runner core with run-scoped state, durable at-least-once sink outbox delivery, leased/fenced running work, and crash-recoverable rotation while preserving existing config and stage APIs.

**Architecture:** Add v3 migration artifacts behind an upgrade gate; the upgrade service applies fixed schema changes after backup, then runs task-aware dynamic migrations. Introduce `tx0` so every pipeline has `run_id` before stage execution. Persist sink payloads before dispatch and use a DB-external rotation sidecar manifest for recovery before opening active DB.

**Tech Stack:** Python 3.11+, `aiosqlite`, existing `WriteJob`/`WriterThread`, pytest, ruff, ty.

---

### Task 1: Add Fixed v3 Schema and Runtime Flags

**Files:**
- Create: `sluice/dag/migrations/v2_0003_active_v3_core.sql`
- Modify: `sluice/dag/db.py`
- Modify: `sluice/dag/config.py`
- Test: `tests/dag/test_db_migrations.py`
- Test: `tests/dag/test_config.py`

**Step 1: Write failing tests**

Assert fixed migration adds:

- `pipelines.cycle_id`, `lease_until`, `heartbeat_at`, `lease_owner`,
  `lease_version`;
- `pipeline_admissions.run_id NULL`, `lease_until`, `heartbeat_at`,
  `lease_owner`, `lease_version`;
- `stage_attempt_log.cycle_id`, `run_id`, FK, and
  `(run_id, pipeline_name, stage_name, item_id, attempt_no)` index;
- `dynamic_schema_version(table_name TEXT PRIMARY KEY, version INTEGER NOT NULL,
  migrated_at TEXT NOT NULL, checksum TEXT)`;
- `sink_outbox` with FK, status CHECK, `claim_owner`, `claim_token`,
  `claim_until`, `sent_at`, unique `(run_id, sink_id, ordinal)`, unique
  `idempotency_key`, and claim indexes;
- `rotation_manifest` history mirror table;
- `schema_version = 3` inserted last;
- normal `runtime_core = "v2"` startup does not auto-apply v3 migration;
- `PRAGMA foreign_key_check` returns no rows.

Run:

```bash
rtk env COVERAGE_FILE=/tmp/sluice-v3-fixed.coverage uv run pytest -q tests/dag/test_db_migrations.py tests/dag/test_config.py
```

Expected: FAIL because fixed migration and flags do not exist.

**Step 2: Implement fixed migration**

Use explicit `BEGIN IMMEDIATE ... COMMIT`. Do not touch dynamic
`pipeline_*`/`stage_*` tables here. Gate `db.py` so this migration is not
auto-discovered or auto-applied by normal v2 startup; only the upgrade service
may execute it after backup. Add runtime flags:

```toml
runtime_core = "v2" # or "v3"
enable_outbox_dispatch = false
enable_lease_recovery = false
enable_rotation_manifest = false
```

**Step 3: Verify**

Run the tests until they pass, including v2 compatibility smoke:

```bash
rtk env COVERAGE_FILE=/tmp/sluice-v3-compat.coverage uv run pytest -q tests/dag/test_cli_compat.py
```

**Step 4: Commit**

```bash
rtk git add sluice/dag/migrations/v2_0003_active_v3_core.sql sluice/dag/db.py sluice/dag/config.py tests/dag/test_db_migrations.py tests/dag/test_config.py
rtk git commit -m "feat(schema): Add v3 fixed runtime schema"
```

### Task 2: Add Task-Aware Dynamic Table Migration

**Files:**
- Modify: `sluice/dag/dynamic_tables.py`
- Modify: `sluice/dag/admin.py`
- Modify: `sluice/dag/rotation/copy.py`
- Test: `tests/dag/test_dynamic_tables.py`
- Test: `tests/dag/test_admin.py`

**Step 1: Write failing tests**

Create a v2 DB with legacy:

- `pipeline_<safe>` primary key on `item_id`;
- `stage_<safe>` primary key on `(pipeline_name, item_id)`;
- `pipeline_<safe>_sink_pending`.

Assert dynamic migration:

- rebuilds pipeline state to `PRIMARY KEY(run_id, item_id)`;
- rebuilds stage state to `PRIMARY KEY(run_id, pipeline_name, item_id)`;
- sets `run_id` from `last_run_id` or `last_pipeline_run_id`;
- derives `cycle_id` from `pipelines.cycle_anchor`;
- backfills `stage_attempt_log.run_id/cycle_id` from stage or pipeline state;
- fails closed if legacy attempts cannot be joined to a run;
- fails closed if a referenced run row is missing;
- fails dry-run/upgrade if `pipeline_<safe>_sink_pending` is non-empty;
- is idempotent and retryable after injected failure.

**Step 2: Implement `upgrade_task_dynamic_tables(conn, task)`**

Use `BEGIN IMMEDIATE`, `__new` tables with `run_id INTEGER NOT NULL REFERENCES
pipelines(id) ON DELETE CASCADE`, copy, `foreign_key_check`, rename, index
recreation, `dynamic_schema_version`, drop old, commit. On failure, keep old
tables and do not advance dynamic version.

**Step 3: Wire migration runner**

Call dynamic upgrade from `migrate_task_dynamic_tables()` after fixed DB
migrations and before runtime use.

**Step 4: Verify and commit**

```bash
rtk env COVERAGE_FILE=/tmp/sluice-v3-dynamic.coverage uv run pytest -q tests/dag/test_dynamic_tables.py tests/dag/test_admin.py
rtk env COVERAGE_FILE=/tmp/sluice-v3-compat.coverage uv run pytest -q tests/dag/test_cli_compat.py
rtk git add sluice/dag/dynamic_tables.py sluice/dag/admin.py sluice/dag/rotation/copy.py tests/dag/test_dynamic_tables.py tests/dag/test_admin.py
rtk git commit -m "feat(schema): Upgrade dynamic state tables"
```

### Task 3: Add v3 Upgrade Command and Compatibility Smoke

**Files:**
- Modify: `sluice/cli.py`
- Create: `sluice/dag/upgrade.py`
- Create: `sluice/dag/runtime_lock.py`
- Modify: `sluice/dag/run_pipeline.py`
- Modify: `sluice/dag/admin.py`
- Test: `tests/dag/test_upgrade.py`
- Test: `tests/dag/test_cli_compat.py`

**Step 1: Write failing tests**

Assert:

- `sluice upgrade v3 --dry-run` validates schema, dynamic table discovery,
  pending sink tables, disk space, backup path, and sidecar directory without
  mutating DBs;
- `sluice upgrade v3 --backup <dir>` writes active/seen DB backups and
  `backup_manifest.json`;
- restore validation runs `integrity_check` and verifies hashes;
- `sluice upgrade v3 --restore <backup_dir>` refuses live runners, verifies
  `backup_manifest.json` hashes, validates `PRAGMA integrity_check`, restores
  active DB, seen DB, and sidecar files, then writes a fresh runtime marker;
- runner startup locks `<task-dir>/.sluice/runtime-core.lock`, reads
  `<task-dir>/.sluice/runtime-core.json`, holds the shared lock for the entire
  runner/admin lifetime, and fails closed when sidecar marker, DB marker, config
  `runtime_core`, and process `runtime_core` disagree;
- `sluice upgrade v3 --backup`, `--restore`, and `--recover` fail while an
  already-running runner holds the shared runtime lock;
- upgrade, restore, and recovery commands take an exclusive flock and reject
  concurrent runner startup or already-running runners;
- upgrade writes phase manifest
  `backup_written -> fixed_migrated -> dynamic_migrated -> marker_written`;
- restore writes phase manifest
  `validating -> restoring_active -> restoring_seen -> restoring_sidecar -> marker_written`;
- startup finds incomplete upgrade/restore manifests and fails closed with
  explicit recover/restore instructions;
- `sluice upgrade v3 --recover` can safely finish `marker_written`, retry
  idempotent post-backup migration phases when hashes and schema match, or fail
  closed with a required `--restore <backup_dir>` when the manifest phase is
  ambiguous;
- startup fails closed when DB marker, task `runtime_core`, and process
  `runtime_core` mismatch;
- `runtime_core = "v2"` startup does not apply v3 fixed migration;
- `runtime_core = "v2"` smoke still accepts existing `sluice run`, `gc-rotate`,
  and retry config.

**Step 2: Implement upgrade service**

Add backup manifest fields: active DB path/hash/schema/runtime, seen DB
path/hash/schema/runtime, sidecar dir, task name, timestamp, and restore
instructions. Mixed v2/v3 runners are forbidden once v3 marker exists. Persist
`<task-dir>/.sluice/runtime-core.lock` as the stable lock file and
`<task-dir>/.sluice/runtime-core.json` with task name, runtime core, schema
version, upgrade id, and backup manifest hash. Add `--restore <backup_dir>` to
validate the backup manifest and restore active DB, seen DB, and sidecar state
only while holding the exclusive runtime lock for the full operation. The
restored marker must match the restored DB runtime/schema; a v2 backup restores
a v2 marker unless an explicit migrated target is provided.

Add `sluice/dag/runtime_lock.py` as the common lock helper. Runner and admin
entrypoints acquire a shared lock for their whole lifetime. Upgrade, restore,
and recover acquire an exclusive lock for their whole operation.

**Step 3: Verify and commit**

```bash
rtk env COVERAGE_FILE=/tmp/sluice-v3-upgrade.coverage uv run pytest -q tests/dag/test_upgrade.py tests/dag/test_cli_compat.py
rtk git add sluice/cli.py sluice/dag/upgrade.py sluice/dag/runtime_lock.py sluice/dag/run_pipeline.py sluice/dag/admin.py tests/dag/test_upgrade.py tests/dag/test_cli_compat.py
rtk git commit -m "feat(upgrade): Add v3 upgrade workflow"
```

### Task 4a: Store-Layer Run-Scoped State

**Files:**
- Modify: `sluice/dag/stores/pipelines.py`
- Modify: `sluice/dag/stores/pipeline_state.py`
- Modify: `sluice/dag/stores/stage_state.py`
- Modify: `sluice/dag/stores/stage_attempt_log.py`
- Test: `tests/dag/test_runtime_stores.py`

**Step 1: Write failing tests**

Assert:

- two runs with same `item_id` in same cycle create two state rows;
- `next_attempt_no` is scoped by `run_id`;
- all dynamic readers require `run_id`, or explicitly document cycle aggregate
  lookup.

**Step 2: Update store APIs**

Thread `run_id` and `cycle_id` through `PipelineStateStore`,
`StageStateStore`, `StageAttemptLogStore`, and `PipelineStore` helpers. Bare
`item_id` lookup is forbidden in new v3 APIs. Keep temporary compatibility
wrappers for unchanged callers with an explicit `legacy=True` or
`scope="legacy"` argument so this commit can pass v2 smoke tests; Task 4b/4c
remove runtime use of those wrappers.

**Step 3: Verify and commit**

```bash
rtk env COVERAGE_FILE=/tmp/sluice-v3-store-state.coverage uv run pytest -q tests/dag/test_runtime_stores.py
rtk env COVERAGE_FILE=/tmp/sluice-v3-compat.coverage uv run pytest -q tests/dag/test_cli_compat.py
rtk ruff check sluice/dag/stores tests/dag/test_runtime_stores.py
rtk git add sluice/dag/stores/pipelines.py sluice/dag/stores/pipeline_state.py sluice/dag/stores/stage_state.py sluice/dag/stores/stage_attempt_log.py tests/dag/test_runtime_stores.py
rtk git commit -m "feat(runtime): Scope stores by run"
```

### Task 4b: Phase-Layer tx0 and Run Threading

**Files:**
- Modify: `sluice/dag/rotation/admission.py`
- Modify: `sluice/dag/run_pipeline.py`
- Modify: `sluice/dag/phases/tx1.py`
- Modify: `sluice/dag/phases/tx2.py`
- Modify: `sluice/dag/phases/resume.py`
- Modify: `sluice/dag/phases/skip_compute.py`
- Test: `tests/dag/test_run_pipeline.py`

**Step 1: Write failing tests**

Assert:

- admission atomically inserts `pipelines(status='running')` before stage
  execution;
- admission row is linked to `run_id`;
- tx0 never takes over expired/fenced rows and does not overwrite live rows with
  another lease owner/version;
- tx1, tx2, resume, and skip paths use the `run_id` produced by tx0.

**Step 2: Implement tx0**

Add a writer job that inserts the pipeline run, stores `cycle_id`, creates lease
owner/version, and links admission. If `run_key` already exists, attach only
when owner/version match the same admission; otherwise skip or fail closed.
Expired rows are recovered only by drain fencing. Stage execution receives
`run_id` before any stage runs.

**Step 3: Thread `run_id` and `cycle_id` through phases**

Update phase call sites so they receive explicit run context instead of deriving
state from pipeline/item names.

**Step 4: Verify and commit**

```bash
rtk env COVERAGE_FILE=/tmp/sluice-v3-phase-state.coverage uv run pytest -q tests/dag/test_run_pipeline.py
rtk env COVERAGE_FILE=/tmp/sluice-v3-compat.coverage uv run pytest -q tests/dag/test_cli_compat.py
rtk ruff check sluice/dag/rotation/admission.py sluice/dag/run_pipeline.py sluice/dag/phases tests/dag/test_run_pipeline.py
rtk git add sluice/dag/rotation/admission.py sluice/dag/run_pipeline.py sluice/dag/phases/tx1.py sluice/dag/phases/tx2.py sluice/dag/phases/resume.py sluice/dag/phases/skip_compute.py tests/dag/test_run_pipeline.py
rtk git commit -m "feat(runtime): Create runs before stages"
```

### Task 4c: Eligibility and Legacy Readers Use Explicit Scope

**Files:**
- Modify: `sluice/dag/eligibility.py`
- Modify: `sluice/dag/cleanup.py`
- Modify: `sluice/dag/admin.py`
- Modify: `sluice/dag/rotation/partition.py`
- Modify: `sluice/dag/rotation/copy.py`
- Test: `tests/dag/test_eligibility.py`
- Test: `tests/dag/test_admin.py`
- Test: `tests/dag/rotation/test_copy.py`

**Step 1: Write failing tests**

Assert eligibility, cleanup, admin inspection, partitioning, and rotation copy
paths never silently aggregate by bare `item_id`. Current-run reads use
`run_id`; cycle views use `cycle_id` plus a deterministic latest-run rule;
legacy reads are allowed only in migration/admin commands that label the data as
legacy.

**Step 2: Update readers**

Convert every runtime reader to require one of: `run_id`, `cycle_id`, or an
explicit `legacy=True` inspection mode.

**Step 3: Verify and commit**

```bash
rtk env COVERAGE_FILE=/tmp/sluice-v3-scope-readers.coverage uv run pytest -q tests/dag/test_eligibility.py tests/dag/test_admin.py tests/dag/rotation/test_copy.py
rtk env COVERAGE_FILE=/tmp/sluice-v3-compat.coverage uv run pytest -q tests/dag/test_cli_compat.py
rtk git add sluice/dag/eligibility.py sluice/dag/cleanup.py sluice/dag/admin.py sluice/dag/rotation/partition.py sluice/dag/rotation/copy.py tests/dag/test_eligibility.py tests/dag/test_admin.py tests/dag/rotation/test_copy.py
rtk git commit -m "feat(runtime): Require scoped state reads"
```

### Task 5: Add Leases, Heartbeats, and Fencing

**Files:**
- Modify: `sluice/dag/rotation/admission.py`
- Modify: `sluice/dag/rotation/drain.py`
- Modify: `sluice/dag/stores/pipelines.py`
- Modify: `sluice/dag/run_pipeline.py`
- Modify: `sluice/llm/pool.py`
- Test: `tests/dag/rotation/test_drain.py`
- Test: `tests/dag/test_run_pipeline.py`
- Test: `tests/llm/test_pool.py`

**Step 1: Write failing tests**

Assert:

- unlinked expired admissions are deleted;
- linked expired admissions are recovered with their pipeline row;
- unexpired heartbeat leases are not collected;
- expired running rows are fenced by incrementing `lease_version`;
- late finalize with old owner/version fails and cannot overwrite failed;
- fake clock proves heartbeat extends long-running stages.
- runner closes `ProviderPool` in `finally` after success, stage failure,
  cancellation, and fenced finalize failure.

**Step 2: Implement heartbeat task**

Runner starts a background heartbeat at `lease_duration / 3` and stops it in
`finally`. Heartbeat refreshes admission and pipeline leases using fenced
`WHERE id/run_id, lease_owner, lease_version`.

Close the runner-owned LLM `ProviderPool` in the same `finally` path after the
heartbeat task is cancelled and awaited. The close path must run whether the
pipeline succeeds, fails during stages, is cancelled, or loses the fencing
token.

**Step 3: Implement drain recovery**

Drain classifies admissions:

- `run_id IS NULL AND lease_until < now`: delete;
- linked to live unexpired pipeline: keep;
- linked to expired pipeline: recover in same transaction as pipeline fencing.

**Step 4: Verify and commit**

```bash
rtk env COVERAGE_FILE=/tmp/sluice-v3-lease.coverage uv run pytest -q tests/dag/rotation/test_drain.py tests/dag/test_run_pipeline.py tests/llm/test_pool.py
rtk env COVERAGE_FILE=/tmp/sluice-v3-compat.coverage uv run pytest -q tests/dag/test_cli_compat.py
rtk git add sluice/dag/rotation/admission.py sluice/dag/rotation/drain.py sluice/dag/stores/pipelines.py sluice/dag/run_pipeline.py sluice/llm/pool.py tests/dag/rotation/test_drain.py tests/dag/test_run_pipeline.py tests/llm/test_pool.py
rtk git commit -m "feat(runtime): Fence expired pipeline leases"
```

### Task 6: Add Outbox Store and Enqueue-Only tx2

**Files:**
- Create: `sluice/dag/stores/sink_outbox.py`
- Modify: `sluice/dag/stores/sink_pending.py`
- Modify: `sluice/dag/phases/run_key_resolve.py`
- Modify: `sluice/dag/phases/tx2.py`
- Modify: `sluice/dag/run_pipeline.py`
- Modify: `sluice/cli.py`
- Test: `tests/dag/test_emission.py`
- Test: `tests/dag/test_run_pipeline.py`
- Test: `tests/dag/test_cli_compat.py`

**Step 1: Write failing tests**

Assert tx2 writes deterministic outbox rows with stable `payload_json`,
`ordinal`, and `idempotency_key`; no external sink send occurs inside a writer
transaction; pipeline status becomes `pending_delivery` when required rows are
pending; legacy CLI/API presents that status as `partial` with
`delivery_pending = true`; when `enable_outbox_dispatch = false`, formal v3 runs
fail closed before enqueueing delivery work; v3 never replays legacy
`sink_pending` rows or resolves run keys from legacy pending delivery tables.

**Step 2: Implement store enqueue**

Add `enqueue_many` with idempotent insert/update on `(run_id, sink_id, ordinal)`.
It may update only unclaimed `pending`/`retryable` rows. For existing claimed,
sent, or failed rows, it no-ops after verifying payload hash and
`idempotency_key`; mismatch raises.

Disable legacy `sink_pending` runtime paths when `runtime_core = "v3"`. Existing
v2 `sink_pending` rows are migration blockers from Task 2; after upgrade, the
outbox dispatcher is the only delivery replay source.

**Step 3: Verify and commit**

```bash
rtk env COVERAGE_FILE=/tmp/sluice-v3-outbox-enqueue.coverage uv run pytest -q tests/dag/test_emission.py tests/dag/test_run_pipeline.py
rtk env COVERAGE_FILE=/tmp/sluice-v3-compat.coverage uv run pytest -q tests/dag/test_cli_compat.py
rtk git add sluice/dag/stores/sink_outbox.py sluice/dag/stores/sink_pending.py sluice/dag/phases/run_key_resolve.py sluice/dag/phases/tx2.py sluice/dag/run_pipeline.py sluice/cli.py tests/dag/test_emission.py tests/dag/test_run_pipeline.py tests/dag/test_cli_compat.py
rtk git commit -m "feat(sinks): Enqueue durable sink outbox rows"
```

### Task 7: Add Outbox Dispatcher and Delivery Finalization

**Files:**
- Modify: `sluice/dag/emission.py`
- Modify: `sluice/dag/sinks/base.py`
- Modify: `sluice/dag/sinks/_v1_push.py`
- Modify: `sluice/dag/stores/sink_outbox.py`
- Modify: `sluice/dag/stores/pipelines.py`
- Modify: `sluice/dag/run_pipeline.py`
- Test: `tests/dag/test_emission.py`
- Test: `tests/dag/sinks/test_sink_adapters.py`

**Step 1: Write failing tests**

Assert:

- two dispatchers cannot claim the same row;
- expired claim can be reclaimed;
- only claim owner can mark sent/retryable/failed;
- external send happens outside writer transaction;
- after every mark, `finalize_delivery(run_id)` advances
  `pending_delivery` to `success`, `partial`, or `failed`;
- retry preserves payload, ordinal, and idempotency key.
- dispatcher closes sink clients on sent, retryable, failed, and cancellation
  paths.

**Step 2: Implement claim/send/mark**

`claim_pending` must atomically update eligible rows:

```sql
status IN ('pending','retryable')
AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
AND (claim_until IS NULL OR claim_until < ?)
```

Claiming leaves `status` as `pending` or `retryable`; it sets `claim_owner` and
`claim_until` plus a fresh `claim_token` for the batch. Then return only rows
claimed by the current `claim_owner` and `claim_token`. `mark_*` uses
`WHERE id = ? AND claim_owner = ? AND claim_token = ?`, and
`mark_*` plus `finalize_delivery(run_id)` run in the same short writer
transaction. `finalize_delivery` updates only
`WHERE id = ? AND status = 'pending_delivery'`, computes terminal counts in that
transaction, and never moves terminal statuses backward.

**Step 3: Add best-effort idempotency**

Email sends deterministic `Message-ID`. Webhook sinks pass
`Idempotency-Key`/dedupe fields where providers support them. Docs and tests
must state unsupported providers remain at-least-once.

**Step 4: Add sink lifecycle cleanup**

Define a sink lifecycle helper used by the dispatcher. It must prefer
`async with sink` when a sink implements async context management and otherwise
call `aclose()` in `finally` when present. The helper is part of the dispatcher
contract; individual sinks must not rely on process exit to close network
clients. The lifecycle scope is one claimed batch for one sink instance; the
dispatcher must not carry unmanaged sink instances across batches.

**Step 5: Verify and commit**

```bash
rtk env COVERAGE_FILE=/tmp/sluice-v3-dispatch.coverage uv run pytest -q tests/dag/test_emission.py tests/dag/sinks/test_sink_adapters.py
rtk env COVERAGE_FILE=/tmp/sluice-v3-compat.coverage uv run pytest -q tests/dag/test_cli_compat.py
rtk git add sluice/dag/emission.py sluice/dag/sinks/base.py sluice/dag/sinks/_v1_push.py sluice/dag/stores/sink_outbox.py sluice/dag/stores/pipelines.py sluice/dag/run_pipeline.py tests/dag/test_emission.py tests/dag/sinks/test_sink_adapters.py
rtk git commit -m "feat(sinks): Dispatch outbox with claim leases"
```

### Task 8: Add Rotation Sidecar Manifest

**Files:**
- Create: `sluice/dag/rotation/manifest.py`
- Modify: `sluice/dag/rotation/orchestrator.py`
- Test: `tests/dag/rotation/test_manifest.py`

**Step 1: Write failing tests**

Assert atomic sidecar writes:

- temp file write, file fsync, replace, parent fsync;
- DB artifact fsync failure fails closed before phase advance;
- corrupt sidecar creates `.failed` and refuses recovery.

**Step 2: Implement manifest persistence**

Sidecar is recovery source of truth. Active DB table is best-effort history
mirror only.

**Step 3: Verify and commit**

```bash
rtk env COVERAGE_FILE=/tmp/sluice-v3-manifest.coverage uv run pytest -q tests/dag/rotation/test_manifest.py
rtk git add sluice/dag/rotation/manifest.py sluice/dag/rotation/orchestrator.py tests/dag/rotation/test_manifest.py
rtk git commit -m "feat(rotation): Persist sidecar manifests"
```

### Task 9a: Refactor Swap Happy Path and Primitives

**Files:**
- Modify: `sluice/dag/rotation/swap.py`
- Modify: `sluice/dag/rotation/orchestrator.py`
- Test: `tests/dag/rotation/test_swap.py`

**Step 1: Write failing tests**

Assert the happy path uses manifest-backed primitives for checkpoint, integrity
check, archive old, install new, seen reconcile, and gzip archive. Assert each
DB artifact and parent directory is fsynced before the manifest advances.

**Step 2: Refactor swap**

Expose small primitives: checkpoint, integrity check, archive old, install new,
run seen reconcile, gzip archive. Each primitive is bracketed by sidecar phase
transition. Rotation must close the gate, drain or stop `WriterThread`, reject
new admissions, and verify no active write connection remains before checkpoint.
Checkpoint must return busy=0 and leave no nonempty `-wal`. New temp DB,
archive temp, and gzip temp files are fsynced before advancing phases.
`seen_reconcile` runs only after new active install, integrity check, DB file
fsync, and parent fsync.

**Step 3: Verify and commit**

```bash
rtk env COVERAGE_FILE=/tmp/sluice-v3-swap-primitives.coverage uv run pytest -q tests/dag/rotation/test_swap.py
rtk env COVERAGE_FILE=/tmp/sluice-v3-compat.coverage uv run pytest -q tests/dag/test_cli_compat.py
rtk git add sluice/dag/rotation/swap.py sluice/dag/rotation/orchestrator.py tests/dag/rotation/test_swap.py
rtk git commit -m "feat(rotation): Refactor manifest-backed swaps"
```

### Task 9b: Add Crash Recovery Matrix and Startup Guard

**Files:**
- Modify: `sluice/dag/rotation/swap.py`
- Modify: `sluice/dag/rotation/orchestrator.py`
- Modify: `sluice/dag/db.py`
- Modify: `sluice/dag/run_pipeline.py`
- Modify: `sluice/dag/admin.py`
- Modify: `sluice/cli.py`
- Test: `tests/dag/rotation/test_orchestrator.py`
- Test: `tests/dag/rotation/test_swap.py`
- Test: `tests/dag/test_cli_compat.py`

**Step 1: Write failing tests**

Simulate crash/recovery for:

- `copying`, `copied`, `archiving_old`, `installing_new`, `installed`,
  `seen_reconciling`, `seen_reconciled`, `gzipping`;
- after manifest write before file op;
- after `os.replace` before parent fsync;
- active DB missing but sidecar present does not create empty active DB;
- runner/admin/CLI/gc-rotate entrypoints run sidecar recovery before any
  `open_active_db()` path can create an empty active DB;
- old active missing/new temp exists;
- both active and new exist;
- WAL checkpoint failure;
- checkpoint result busy nonzero;
- `-wal` exists with nonzero size after checkpoint;
- integrity check failure;
- DB file fsync failure;
- gzip temp exists.

**Step 2: Enforce startup order**

Startup/pre-rotation must: acquire runtime lock, acquire rotation lock, run
sidecar recovery, validate an existing active DB, then open gate/start writer.
Runner, admin, CLI, and `gc-rotate` entrypoints must pass through this guard
before any `open_active_db()` call. If `.failed` exists, or if active DB is
missing without a valid sidecar recovery path, refuse runner/admin actions
except explicit init/recovery commands. Do not auto-create an empty active DB.

**Step 3: Verify and commit**

```bash
rtk env COVERAGE_FILE=/tmp/sluice-v3-rotation.coverage uv run pytest -q tests/dag/rotation/test_orchestrator.py tests/dag/rotation/test_swap.py
rtk env COVERAGE_FILE=/tmp/sluice-v3-compat.coverage uv run pytest -q tests/dag/test_cli_compat.py
rtk git add sluice/dag/rotation/swap.py sluice/dag/rotation/orchestrator.py sluice/dag/db.py sluice/dag/run_pipeline.py sluice/dag/admin.py sluice/cli.py tests/dag/rotation/test_orchestrator.py tests/dag/rotation/test_swap.py tests/dag/test_cli_compat.py
rtk git commit -m "feat(rotation): Recover manifest-tracked swaps"
```

### Task 10: Compatibility, Docs, and Full Verification

**Files:**
- Modify: `README.md`
- Modify: `README_ZH.md`
- Modify: `CHANGELOG.md`
- Create/Modify: `docs/migration/*`
- Create/Modify: `tests/dag/test_cli_compat.py`

**Step 1: Add compatibility acceptance tests**

Use existing sample TOML and assert:

- `sluice run`, `gc-rotate`, and retry paths still accept existing config;
- stage executor params are unchanged;
- sink config remains accepted;
- internal `pending_delivery` displays as legacy `partial` with
  `delivery_pending = true`.

**Step 2: Add operator docs**

Document:

- `sluice upgrade v3 --dry-run`;
- backup and restore path;
- `sluice upgrade v3 --restore <backup_dir>` hash/integrity validation;
- `backup_manifest.json` fields and restore validation;
- mixed v2/v3 runner prohibition;
- runtime sidecar marker and startup flock behavior;
- outbox retry operations;
- failed rotation sidecar recovery;
- stuck `pending_delivery` triage.
- v3.1 security hardening implementation plan:
  `docs/plans/2026-06-21-v3.1-security-hardening.md`.
- production gate: if v3.1 hardening is not merged, `runtime_core = "v3"` must
  remain documented as non-production/security-hardening-pending.
- release blocker: without v3.1 Task 4 merged, operators must not
  production-enable `runtime_core = "v3"`.

**Step 3: Full verification**

Run:

```bash
rtk env COVERAGE_FILE=/tmp/sluice-v3-full.coverage uv run pytest -q
rtk ruff check .
rtk uv run ty check
rtk git diff --check
```

**Step 4: Commit**

```bash
rtk git add README.md README_ZH.md CHANGELOG.md docs/migration tests/dag/test_cli_compat.py
rtk git commit -m "docs: Document v3 core rollout"
```
