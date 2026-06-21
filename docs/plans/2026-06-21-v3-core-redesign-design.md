# Sluice v3 Core Redesign

## Goal

Rework the runner core without replacing the CLI, task config, or stage
configuration surface. The new core must make pipeline state run-scoped, make
external sink delivery recoverable, let rotation recover from crashes, and
collect stale running work without manual database surgery.

## Recommended Approach

Use an incremental v3 core migration. Keep current task files and stage
executors compatible, but replace the unsafe runtime contracts beneath them:

- durable state is keyed by `run_id`; `cycle_id` is an indexed grouping field;
- sink delivery is durable at-least-once with deterministic payloads and
  best-effort provider idempotency;
- every running pipeline has a lease owner and fencing version;
- rotation recovery reads a DB-external sidecar manifest before opening active
  DB, so missing active DB never causes an empty replacement DB;
- rollout is gated by runtime flags and an explicit upgrade command.

This avoids a full rewrite while removing the failure modes that caused the
audit findings.

## Data Model

### Fixed Active Schema

Add active DB migration `v2_0003_active_v3_core.sql` for fixed tables only.
It must run inside `BEGIN IMMEDIATE ... COMMIT`, insert `schema_version = 3`
last, and be safe to retry if a previous attempt failed before the version row.
It must not be auto-applied by normal `open_active_db()` while
`runtime_core = "v2"`. The v3 migration is executed only by the upgrade service
after dry-run validation and backup creation.

`pipelines` gains:

- `cycle_id TEXT NOT NULL DEFAULT ''`
- `lease_until TEXT`
- `heartbeat_at TEXT`
- `lease_owner TEXT`
- `lease_version INTEGER NOT NULL DEFAULT 0`
- indexes on `(task, status, lease_until)`, `(cycle_id, pipeline_name)`, and
  `(lease_owner, lease_version)`

Legacy rows get `cycle_id = cycle_anchor`.

`pipeline_admissions` gains:

- `run_id INTEGER NULL REFERENCES pipelines(id) ON DELETE SET NULL`
- `lease_until TEXT`
- `heartbeat_at TEXT`
- `lease_owner TEXT`
- `lease_version INTEGER NOT NULL DEFAULT 0`

Existing admissions get `run_id = NULL` and an expired lease so drain can
delete them.

`stage_attempt_log` gains:

- `cycle_id TEXT`
- `run_id INTEGER REFERENCES pipelines(id) ON DELETE CASCADE`
- index `(run_id, pipeline_name, stage_name, item_id, attempt_no)`

Attempt numbering becomes run-scoped. If an implementation keeps retry policy
cycle-scoped, it must explicitly scope by `(pipeline_name, stage_name, cycle_id,
item_id)` and must not read across cycles by accident.

Legacy attempt rows are backfilled during upgrade. Rows that can be joined to
existing stage or pipeline state get `run_id` and `cycle_id` from that state.
Rows that cannot be joined make upgrade fail closed unless an explicit repair
command creates a synthetic legacy run.

Add `dynamic_schema_version`:

- `table_name TEXT PRIMARY KEY`
- `version INTEGER NOT NULL`
- `migrated_at TEXT NOT NULL`
- `checksum TEXT`

### Dynamic Table Migration

Dynamic tables cannot be converted by the pure SQL migration because
`apply_migrations()` has no task config and SQLite cannot alter a primary key.
Add a task-aware Python migration:

```python
upgrade_task_dynamic_tables(conn, task)
```

It is called from `migrate_task_dynamic_tables()` after fixed migrations. It
uses `dynamic_schema_version(table_name, version, migrated_at)` or idempotent
`PRAGMA table_info` checks so it can retry safely.

Dynamic table classes are handled separately:

- `pipeline_{safe}`: rebuild with
  `run_id INTEGER NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE`,
  `PRIMARY KEY(run_id, item_id)`, `cycle_id TEXT NOT NULL`, and index
  `(cycle_id, item_id)`;
- `stage_{safe}`: rebuild with
  `run_id INTEGER NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE`,
  `PRIMARY KEY(run_id, pipeline_name, item_id)`, `cycle_id TEXT NOT NULL`, and
  index `(pipeline_name, cycle_id, item_id)`;
- `pipeline_{safe}_sink_pending`: do not treat as pipeline state. v3 upgrade
  fails closed if any sink-pending table is non-empty; the operator must drain
  or explicitly clear pending delivery before migration.

Rebuild algorithm:

1. `BEGIN IMMEDIATE`;
2. create `<table>__new` with v3 schema;
3. copy rows, setting `run_id = last_run_id` or `last_pipeline_run_id` and
   deriving `cycle_id` from `pipelines.cycle_anchor`;
4. if the referenced run row is missing, fail closed by default. A later
   explicit repair command may create a synthetic legacy `pipelines` row, but
   automatic migration must not invent one silently;
5. run `PRAGMA foreign_key_check`;
6. rename old table to `<table>__old`, rename new table to original name;
7. recreate indexes;
8. insert/update `dynamic_schema_version`;
9. drop old table and commit.

If any step fails, rollback keeps the old table and the dynamic version is not
advanced.

### Sink Outbox

Add `sink_outbox`:

- `id INTEGER PRIMARY KEY AUTOINCREMENT`
- `run_id INTEGER NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE`
- `run_key TEXT NOT NULL`
- `sink_id TEXT NOT NULL`
- `sink_type TEXT NOT NULL`
- `ordinal INTEGER NOT NULL`
- `idempotency_key TEXT NOT NULL`
- `message_kind TEXT`
- `payload_json TEXT NOT NULL`
- `status TEXT NOT NULL CHECK(status IN ('pending','sent','retryable','failed'))`
- `attempts INTEGER NOT NULL DEFAULT 0`
- `next_attempt_at TEXT`
- `claim_owner TEXT`
- `claim_token TEXT`
- `claim_until TEXT`
- `sent_at TEXT`
- `last_error_class TEXT`
- `last_error_msg TEXT`
- `created_at TEXT NOT NULL`
- `updated_at TEXT NOT NULL`
- `UNIQUE(run_id, sink_id, ordinal)`
- `UNIQUE(idempotency_key)`
- index `(status, next_attempt_at, id)`
- index `(run_id, status)`

`sink_emissions` and `sink_delivery_log` remain compatibility/history tables,
but dispatch writes them after an outbox row is marked.

Claiming does not change `status`; it sets `claim_owner`, `claim_until`, and a
fresh `claim_token` for the batch. Rows are claimable when status is `pending`
or `retryable`, next retry time is null or due, and the claim is empty or
expired. Returned rows must match both `claim_owner` and `claim_token`;
mark/retry/fail updates must use
`WHERE id = ? AND claim_owner = ? AND claim_token = ?`. `enqueue_many` may
update only unclaimed `pending`/`retryable` rows. If a conflict row is claimed,
sent, or failed, enqueue is a no-op after verifying `payload_json` hash and
`idempotency_key` are unchanged; mismatch raises.

The guarantee is not exactly-once. It is durable at-least-once: payload,
ordinal, and idempotency key are stable across retries. Email uses deterministic
`Message-ID`; webhook sinks send provider-specific idempotency or dedupe fields
where supported. Providers without idempotency can still receive duplicates
after crash-after-send-before-mark.

### Rotation Manifest

The source of truth is a DB-external sidecar manifest, not the active DB:

```text
<task-dir>/.sluice/rotation/<task>.manifest.json
<task-dir>/.sluice/rotation/<task>.failed
```

The active DB `rotation_manifest` table is only a query/history mirror. Startup
recovery reads the sidecar before any call that could auto-create active DB.

Manifest phases:

```text
copying -> copied -> archiving_old -> installing_new -> installed
-> seen_reconciling -> seen_reconciled -> gzipping -> done
```

`seen_reconcile` is forbidden before new active DB is installed, integrity
checked, and its parent directory is fsynced.

Every manifest write is atomic: write temp, fsync file, replace, fsync parent.
Every new or rewritten DB artifact, including new active temp, archive temp,
and gzip temp, is fsynced before the manifest advances. Every `os.replace`
during swap is followed by parent directory fsync. Cross filesystem rename is
forbidden.

Before checkpoint and swap, rotation closes the gate, drains or stops the
writer, rejects new admissions, and verifies no active write connection remains.
`PRAGMA wal_checkpoint(TRUNCATE)` must report success with `busy = 0`, and the
`-wal` file must be absent or size zero before file moves start. If checkpoint
or `PRAGMA integrity_check` fails, rotation fails closed before file moves.

Recovery phase matrix:

| Phase | Expected Files | Validation | Allowed Action | Fail-Closed Condition |
| --- | --- | --- | --- | --- |
| `copying` | old active, maybe new temp | old active valid | remove incomplete new temp, reopen gate | old active invalid |
| `copied` | old active, complete new temp | both integrity checks pass | continue to archive old | any DB invalid |
| `archiving_old` | old active or archive temp, new temp | one valid old copy exists | finish old archive move or restore old active | neither old copy valid |
| `installing_new` | archive temp, maybe active, new temp | new temp valid | install new active or restore old active | both active and new invalid |
| `installed` | active is new, archive temp | active integrity check passes | run seen reconcile | active missing/invalid |
| `seen_reconciling` | active is new, archive temp | active valid | retry idempotent seen reconcile | seen DB unavailable after retries |
| `seen_reconciled` | active is new, archive temp | active valid | gzip archive temp | archive temp missing |
| `gzipping` | active is new, archive temp/gz tmp | active valid | retry gzip or cleanup gz tmp | archive temp missing and gz missing |
| `done` | active is new, archive gz | active valid | remove sidecar manifest | active invalid |

If recovery cannot act safely, write `.failed`, keep gate closed, and refuse
runner/admin actions except explicit recovery commands.

## Runtime Flow

### Admission and Run Creation

Add `tx0`: before stage execution, admission atomically inserts the
`pipelines(status='running')` row, writes `cycle_id`, `lease_owner`, and
`lease_version`, and links `pipeline_admissions.run_id`.

`tx0` must not take over expired or fenced rows. If the same `run_key` already
has a live running row, it may attach only when `lease_owner` and
`lease_version` match the same admission; otherwise it skips or fails closed.
Expired rows are recovered only by drain fencing.

Stage execution starts only after `run_id` exists. A background heartbeat task
refreshes both admission and pipeline leases every `lease_duration / 3` until
finalize or failure.

Heartbeat and finalize are fenced:

```sql
WHERE id = ? AND lease_owner = ? AND lease_version = ? AND status = 'running'
```

Drain recovery increments `lease_version`, clears `lease_owner`, and marks
expired rows failed in the same transaction. A late runner with an old fencing
token cannot finalize over the drain decision.

### State Writes

tx1 writes items, stages, stage attempts, and stage-state data with both
`run_id` and `cycle_id`. tx2 writes pipeline-state data with `run_id`.

All dynamic table readers and writers must choose an explicit scope:

- current run: `run_id = ?`;
- current cycle aggregate: `cycle_id = ?` plus deterministic latest-run rule;
- legacy fallback: only for migration/admin inspection paths.

No query may rely on bare `item_id`.

### Emission and Delivery

Emission changes from "check, send, record" to:

1. render chunks in memory;
2. tx2 writes deterministic pending outbox rows for eligible chunks;
3. dispatcher claims rows in a short writer transaction;
4. external sink send happens outside the writer transaction;
5. dispatcher marks `sent`, `retryable`, or `failed` in a short writer
   transaction;
6. in the same short writer transaction as every mark,
   `finalize_delivery(run_id)` advances
   `pending_delivery -> success|partial|failed` when all required rows are
   terminal.

`finalize_delivery` is monotonic. It updates only
`WHERE id = ? AND status = 'pending_delivery'`, computes terminal counts in the
same transaction, and never moves `success`, `partial`, or `failed` backward.

`pending_delivery` is an internal status. Legacy CLI/API output presents it as
`partial` with `delivery_pending = true`; detailed/admin views may show the
internal status.

### Rotation Startup Order

Process startup and pre-rotation order is fixed:

1. acquire rotation lock;
2. read sidecar manifest and run recovery;
3. validate the existing active DB and run integrity check;
4. open or keep closed the rotation gate;
5. start writer/admissions only after recovery succeeds.

If `.failed` exists, or if active DB is missing without a valid sidecar
recovery path, `open_active_db()` must not create an empty active DB. Only
explicit `sluice init` or explicit recovery commands may create/restore one.

## Rollout Flags

Add explicit runtime gates:

- `runtime_core = "v2" | "v3"`
- `enable_outbox_dispatch = false | true`
- `enable_lease_recovery = false | true`
- `enable_rotation_manifest = false | true`

Flag semantics:

- `runtime_core = "v2"` must keep the legacy runtime path working after every
  incremental commit;
- `runtime_core = "v3"` enables v3 tx0/state semantics;
- `enable_outbox_dispatch = false` is allowed only for dry-run/upgrade
  validation, or a v3 run must fail closed before enqueueing delivery work;
- `enable_lease_recovery` and `enable_rotation_manifest` gate their respective
  recovery paths until tests and rollout enable them.

Upgrade path:

1. `sluice upgrade v3 --dry-run` validates fixed schema, dynamic table
   discovery, available disk, backup path, and rotation sidecar location.
2. `sluice upgrade v3 --backup <dir>` checkpoints DBs, writes backups, applies
   fixed migration, runs dynamic migration, and records a v3 marker.
3. Mixed v2/v3 runners are forbidden for the same task once v3 migration starts.
4. Rollback is backup restore only after stopping all runners; no automatic
   downgrade is promised.

The upgrade command writes `backup_manifest.json` with active DB, seen DB,
sidecar directory, schema versions, file hashes, and restore instructions.
Runner startup must compare DB v3 marker, task flag, and process
`runtime_core`; mismatch fails closed.
Normal startup with `runtime_core = "v2"` must not mutate a v2 DB to v3.

## Error Handling

- Lease expiry plus fencing is the source of truth for stale work, not process
  PID.
- Outbox send failures advance retry state; they do not mutate pipeline state
  outside `finalize_delivery`.
- Rotation recovery never relies on active DB being present.
- `verbose` logging remains `diagnose=False`; debug data must be structured and
  redacted, not collected from Python locals.

## Testing Strategy

Add focused regression tests before implementation:

- fixed migration is transactional and retryable;
- upgrade dry-run/backup writes and validates `backup_manifest.json`;
- flag-off `runtime_core = "v2"` smoke tests pass after every task;
- dynamic migration rebuilds legacy pipeline/stage tables and fails closed on
  non-empty `sink_pending`;
- two runs for the same `item_id` in the same cycle cannot overwrite each other;
- `tx0` creates `run_id` before stages and heartbeat prevents drain collection;
- drain fencing prevents late finalize;
- outbox claim is atomic across two dispatchers and expired claims are retried;
- outbox retry keeps ordinal, payload, and idempotency key stable;
- provider without idempotency is documented as at-least-once;
- rotation recovery covers every manifest phase, missing active DB, corrupt
  sidecar, missing active DB without sidecar, WAL checkpoint failure, DB file
  fsync failure, and parent fsync failure injection;
- existing CLI/config/stage/sink black-box tests pass unchanged.

## Rollout

Ship as separate reviewable commits or PRs:

1. fixed schema migration and v3 flags;
2. task-aware dynamic migration;
3. upgrade command and compatibility smoke tests;
4. tx0, run-scoped state, and attempt scoping;
5. leased heartbeat and fencing recovery;
6. outbox schema/store and enqueue-only tx2;
7. dispatcher claim/send/mark and delivery finalization;
8. sidecar rotation manifest persistence;
9. swap primitive refactor and startup recovery;
10. compatibility docs, migration guide, and operator recovery guide.

Then do lower-risk cleanup: merge sink layers, add store helpers, and split the
executor god module.
