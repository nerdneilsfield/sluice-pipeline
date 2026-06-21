# Sluice v3 Core Migration Guide

This document describes how to upgrade a v2 task to the v3 runtime core.

> **Status:** v3 is currently **non-production / hardening-pending**. The v3
> security hardening plan (v3.1) must be merged before v3 is recommended for
> production use. See `docs/plans/2026-06-21-v3.1-security-hardening.md`.

## What changes in v3

The v3 core replaces four unsafe runtime contracts:

1. **Pipeline state is run-scoped.** `pipeline_<safe>` and `stage_<safe>`
   dynamic tables gain `run_id` and `cycle_id` columns; the primary key
   becomes `(run_id, item_id)` (or `(run_id, pipeline_name, item_id)` for
   stage tables). Two runs for the same item in the same cycle no longer
   overwrite each other.
2. **Pipeline runs have leases.** `tx0` creates a `pipelines(status='running')`
   row with `lease_owner`, `lease_version`, `lease_until`, and `heartbeat_at`
   *before* stage execution starts. `drain.recover_expired_leases` cleans up
   crashed runners; `PipelinesStore.finalize_v3` uses fencing tokens so a
   late finalizer cannot overwrite a drained run.
3. **Sink delivery is durable at-least-once.** The new `sink_outbox` table
   records every required delivery with a stable `idempotency_key` and
   `payload_json`. The dispatcher (Task 7) claims rows via `claim_token`,
   sends the payload, and marks sent/retryable/failed. Finalization is
   monotonic: a pipeline advances from `pending_delivery` to `success`,
   `partial`, or `failed` only after all outbox rows reach a terminal status.
4. **Rotation has a crash-recoverable sidecar.** A DB-external manifest at
   `<task-dir>/.sluice/rotation/rotation.manifest.json` records the current
   rotation phase. `assert_clean_rotation_state` blocks startup if a
   `.failed` marker exists or the manifest is not in the `done` phase.

## Runtime flags

Add to `sluice.toml`:

```toml
[runtime]
runtime_core = "v2"                # default; no v3 schema applied
enable_outbox_dispatch = false      # v3 dispatcher gate
enable_lease_recovery = false       # drain.recover_expired_leases gate
enable_rotation_manifest = false    # sidecar manifest gate
```

`runtime_core = "v3"` is required for tx0 / fenced finalize / outbox paths
to activate. The other three flags allow operators to enable individual
subsystems once tested in their environment.

## Upgrade workflow

```bash
# 1. Validate preconditions (no mutation):
sluice upgrade v3 -t <task> --dry-run

# 2. Backup + apply fixed schema + rebuild dynamic tables + write marker:
sluice upgrade v3 -t <task> --backup /backups/sluice/<task>-<timestamp>

# 3. Edit sluice.toml: set runtime_core = "v3"

# 4. (Optional) Enable individual subsystems:
#    enable_outbox_dispatch = true
#    enable_lease_recovery = true
#    enable_rotation_manifest = true

# 5. Smoke test with a single pipeline run:
sluice run -t <task> -p <pipeline>
```

## Rollback

Rollback is **backup-restore only**:

```bash
# Stop all runners for the task first.
sluice upgrade v3 -t <task> --restore /backups/sluice/<task>-<timestamp>
```

The restore command verifies `backup_manifest.json` hashes, runs
`PRAGMA integrity_check`, and overwrites the live active/seen DBs and
sidecar state. The runtime marker is rewritten to match the restored DB.

No automatic downgrade is provided. Mixed v2/v3 runners on the same task
are forbidden once the v3 marker exists.

## Recovery from interrupted upgrade

If `sluice upgrade v3 --backup` is interrupted (process killed, OOM,
etc.), the `<task-dir>/.sluice/upgrade-phase.json` sidecar records which
phases completed. Recovery options:

```bash
# Safe completion (idempotent post-backup phases):
sluice upgrade v3 -t <task> --recover

# If --recover refuses (ambiguous state), restore from backup:
sluice upgrade v3 -t <task> --restore <backup_dir>
```

## Operator triage runbook

### Stuck `pending_delivery` pipeline

A pipeline row stays `status='pending_delivery'` when its outbox has
unclaimed or retryable rows. Check:

```sql
SELECT status, COUNT(*) FROM sink_outbox WHERE run_id = ? GROUP BY status;
```

- `retryable` rows: the dispatcher keeps trying. Check `last_error_msg` for
  the failure. If the sink is permanently broken (revoked token, deleted
  channel), mark the rows failed manually:
  ```sql
  UPDATE sink_outbox SET status='failed', last_error_msg='operator_manual'
  WHERE run_id = ? AND status = 'retryable';
  ```
- All `sent` but pipeline still `pending_delivery`: run
  `finalize_delivery(conn, run_id)` via Python; this is a bug.

### Failed rotation sidecar

If `<task-dir>/.sluice/rotation/rotation.failed` exists:

1. Inspect the file: `cat .sluice/rotation/rotation.failed`
2. Check the manifest: `cat .sluice/rotation/rotation.manifest.json`
3. Decide: recover (if phase is unambiguous) or restore from backup.

### Stuck `running` pipeline after a runner crash

Drain fences expired leases automatically on the next rotation cycle. To
force recovery:

```sql
-- Identify the orphan
SELECT id, lease_owner, lease_until, started_at FROM pipelines
WHERE status = 'running' AND lease_until < datetime('now');
-- Fence it manually (mirrors recover_expired_leases)
UPDATE pipelines
SET status = 'failed', error_msg = 'manual_fencing',
    lease_version = lease_version + 1, lease_owner = NULL
WHERE id = ?;
```

## Current limitations (as of this commit)

The following are documented gaps to be closed in v3.1 / Task 9b
follow-up:

- **HeartbeatTask is not yet wired into `run_pipeline`.** Long-running
  pipelines (> 30 minutes default lease) will see their lease expire and
  finalize will return `lease_fenced_during_finalize`. Mitigation: raise
  `lease_duration_seconds` via the runner kwarg until heartbeat is wired.
- **Startup guard is not yet wired into `cli.run` / `cli.gc-rotate`.**
  Operators must check `.sluice/rotation/rotation.failed` manually until
  Task 9b integrates `assert_clean_rotation_state` into entrypoints.
- **Legacy `sink_pending` runtime paths are still active under v3.** Both
  SinkPendingStore (v2) and SinkOutboxStore (v3) coexist. Until tx2 grows
  a `runtime_core='v3'` branch that switches to outbox enqueue, the outbox
  table is populated only by the test harness.
- **No crash-recovery matrix tests.** The 11 fault-injection scenarios
  listed in Task 9b (WAL checkpoint failure, integrity check failure, DB
  file fsync failure, parent fsync failure, every manifest phase crash)
  are tracked as follow-up. The current tests cover only the happy path
  and the `.failed` marker.
- **ProviderPool finally-cleanup is not wired into run_pipeline.**
  Resource leak remains on long-running Prefect workers.
- **v3.1 security hardening not yet merged.** Jinja sandbox, prompt
  boundary, LLM output schema validation. v3 must not be production-enabled
  until v3.1 lands.

## See also

- `docs/plans/2026-06-21-v3-core-redesign-design.md` — design rationale
- `docs/plans/2026-06-21-v3-core-redesign.md` — implementation plan
- `docs/plans/2026-06-21-v3.1-security-hardening.md` — security follow-up
