# Sluice v1 to v2 Migration

This guide covers the operational cutover from the retired v1 runtime to the
v2 task/pipeline DAG runtime.

## 1. Back Up Current State

Before changing configs or databases, snapshot the current runtime state:

```bash
cp -a configs configs.v1.backup
cp -a data data.v1.backup
```

If the deployment runs under systemd, cron, or an external scheduler, disable
the old job before starting the v2 migration so v1 cannot write while tables are
being prepared.

## 2. Convert Pipeline Configs

v2 stores each task as a directory under `configs/tasks/<task>/` with
task-level metadata and one or more pipeline definitions.

Use the scaffold command for each legacy pipeline file:

```bash
sluice scaffold-task \
  --from-old configs/pipelines/ai_news.toml \
  --out configs/tasks/ai_news
```

Review the generated files before deploying. In particular:

- Confirm the task id matches the directory name.
- Confirm source, stage, and sink ids are ASCII identifiers.
- Move credentials to environment variables where possible.
- Set each pipeline `cron`, `enabled`, and optional `concurrency_strategy`.

## 3. Prepare the Database

v2 creates task-specific dynamic tables. Run the migration for each task after
reviewing the generated task directory:

```bash
sluice migrate ai_news --config-dir configs
```

The command is idempotent. Re-running it is safe after changing task schemas.

If you are intentionally starting from a clean database, archive the old SQLite
files first rather than deleting them in place:

```bash
mkdir -p data/archive
mv data/*.sqlite data/archive/
```

## 4. Deploy Prefect Schedules

Deploy all enabled v2 pipelines and the rotation deployment:

```bash
sluice deploy \
  --config-dir configs \
  --sluice-config configs/sluice.toml
```

When `runtime.image` is set in `sluice.toml`, the image must already be built
and pushed to the registry. The deploy command passes the image reference to
Prefect with `build=False` and `push=False`.

## 5. Verify the Cutover

Check task inventory and run one pipeline manually before enabling the worker:

```bash
sluice list --config-dir configs
sluice items list ai_news --config-dir configs
sluice run --task ai_news --pipeline main --config-dir configs
```

Then start the Prefect worker. For local Compose:

```bash
make -C deploy up
make -C deploy logs
```

For systemd, install `deploy/systemd/sluice-worker.service`, create
`/etc/sluice/worker.env`, reload systemd, and start the service:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now sluice-worker.service
```

## 6. Retry and Item Operations

Use the v2 admin commands during validation:

```bash
sluice retry --task ai_news --pipeline main --config-dir configs
sluice items show ai_news <item-id> --config-dir configs
sluice items kill ai_news <item-id> --config-dir configs
```

`retry` resets failed pipeline admissions for the selected task/pipeline.
`items kill` marks an item as killed so it is skipped by future runs.

## 7. Rollback

If cutover fails, stop the v2 worker first:

```bash
make -C deploy down
# or
sudo systemctl stop sluice-worker.service
```

Restore the archived v1 configs and data:

```bash
rm -rf configs data
mv configs.v1.backup configs
mv data.v1.backup data
```

Re-enable the old scheduler only after confirming v2 workers and Prefect
schedules are stopped.

## References

- `docs/superpowers/plans/2026-05-12-plan4b-implementation.md`
- `docs/superpowers/specs/2026-05-12-plan4b-concurrency-findings.md`
