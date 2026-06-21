# Sluice v3 Core Redesign

## Goal

Rework the runner core without replacing the CLI, task config, stage API, or
sink API surface. The new core must make pipeline state cycle-scoped, make
external sink delivery recoverable, let rotation recover from crashes, and
collect stale running work without manual database surgery.

## Recommended Approach

Use an incremental v3 core migration. Keep current config files and stage
executors compatible, but replace the unsafe runtime contracts beneath them:

- state is keyed by `run_id` and `cycle_id`, never by bare `item_id`;
- sink side effects flow through a durable outbox before external delivery;
- running pipelines and admissions use leases and heartbeats;
- rotation records each file-swap phase in a manifest and runs startup recovery;
- sink/store/executor cleanup follows after correctness boundaries are stable.

This avoids a full rewrite while removing the failure modes that caused the
audit findings.

## Data Model

Add active DB migration `v2_0003_active_v3_core.sql`.

`pipelines` gains:

- `cycle_id TEXT`
- `lease_until TEXT`
- `heartbeat_at TEXT`
- indexes on `(task, status, lease_until)` and `(cycle_id, pipeline_name)`

Legacy rows get `cycle_id = cycle_anchor`.

`pipeline_admissions` gains:

- `run_id INTEGER`
- `lease_until TEXT`
- `heartbeat_at TEXT`

Existing admissions get a short expired lease so drain can recover them.

Per-pipeline state tables change from `PRIMARY KEY(item_id)` to
`PRIMARY KEY(cycle_id, item_id)`. The migration creates replacement tables for
every dynamic `pipeline_*` table and copies legacy rows under a legacy cycle.

Per-stage state tables change from `PRIMARY KEY(pipeline_name, item_id)` to
`PRIMARY KEY(pipeline_name, cycle_id, item_id)` so retry accounting is isolated
per cycle.

Add durable outbox tables:

- `sink_outbox(id, run_id, run_key, sink_id, sink_type, ordinal,
  idempotency_key, message_kind, payload_json, status, attempts, next_attempt_at,
  last_error_class, last_error_msg, created_at, updated_at)`
- `UNIQUE(run_id, sink_id, ordinal)`

Keep `sink_emissions` and `sink_delivery_log` for compatibility, but feed them
from outbox dispatch results.

Add rotation manifest:

- `rotation_manifest(id, task, cycle_anchor, old_active_path, new_active_path,
  archive_temp_path, archive_gz_path, phase, error_msg, created_at, updated_at)`

Phases are `copying`, `copied`, `seen_reconciled`, `archiving_old`,
`installing_new`, `installed`, `gzipped`, `done`, `failed`.

## Runtime Flow

Admission happens before stage execution as today, but it writes a leased
admission row. After tx1 creates or reuses the pipeline run, it links the
admission to `run_id` and starts a pipeline lease. The runner heartbeats while
stage work and post-tx1 work are active.

tx1 writes item, stage, and stage-state data with `cycle_id`.

Emission changes from "check, send, record" to:

1. render chunks in memory;
2. tx2 writes pending `sink_outbox` rows for eligible successful items/chunks;
3. dispatcher claims pending rows, sends external sinks, then records delivery
   and marks outbox row `sent` or `failed_retryable`;
4. retry uses the existing row and idempotency key, not a rebuilt payload.

Pipeline finalize treats pending outbox rows as pending work, not as immediate
success. A run is `success` only when eligible state is written and all required
outbox rows are sent; otherwise it is `partial`, `failed`, or `pending_delivery`.

Drain treats any `running` pipeline or admission whose `lease_until < now` as
stale. It marks stale pipeline rows failed and deletes stale admissions before
deciding whether rotation may proceed.

Rotation writes a manifest before touching files. Each file operation advances
the phase. On startup and before every rotation, recovery inspects the manifest
and either completes the swap, restores a valid active DB, or leaves a clear
`failed` row with manual paths. `seen_reconcile` moves after active DB install
or becomes recoverable via manifest phase.

## Error Handling

- Lease expiry is the source of truth for stale work, not process PID.
- Outbox send failures do not corrupt pipeline state; they advance retry state.
- A crash between file moves is recovered from manifest paths.
- `verbose` logging remains `diagnose=False`; debug data must be structured
  and redacted, not collected from Python locals.

## Testing Strategy

Add focused regression tests before implementation:

- migration preserves legacy pipeline/stage state under a legacy cycle;
- two runs for the same `item_id` in different cycles cannot overwrite each
  other;
- outbox crash after external send does not rebuild a different payload;
- stale admission and running pipeline leases unblock drain;
- rotation crash at each manifest phase can recover or fail closed;
- existing runner tests still pass without config changes.

## Rollout

Ship in four commits or PRs:

1. schema and run-state isolation;
2. outbox emission and dispatcher;
3. leases, heartbeat, and zombie recovery;
4. rotation manifest and recovery.

Then do lower-risk cleanup: merge sink layers, add store helpers, and split the
executor god module.
