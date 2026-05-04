# Plan 4a — Prefect/cron Wire-up (v2) Design

## Summary

Wire the v2 DAG (`sluice/dag/`) into Prefect 3 so that pipeline runs and rotation are scheduled and executed by an external Prefect server + worker. Replace the existing `sluice deploy` command (which used the broken v1 `build_flow` closure approach) with a v2 implementation that pushes top-level flow deployments via `flow.deploy()`.

## Why

The v1 `deploy` command wraps each pipeline in a `build_flow.<locals>._flow` closure. Prefect workers cannot resolve closure-defined functions from storage entrypoints, producing:

```
RuntimeError: Function with name 'build_flow.<locals>._flow' not found in 'sluice/flow.py'
```

The fix is to define flows as top-level module-level functions, which is also the correct pattern for `flow.deploy()` (push-to-server) deployments.

## Scope

**In:**
- Two top-level `@flow` functions in `sluice/dag/flows.py`.
- `deploy.py` — pure deployment-building logic (testable without Prefect server).
- `sluice.toml` schema extended: `[runtime]` + `[gc.rotation] default_cron`.
- `TaskConfig.rotation_cron` per-task override.
- `sluice deploy` command rewritten for v2.
- Unit + integration tests (no live Prefect server required).

**Out (Plan 4b):**
- Deleting v1 files (`flow.py`, `gc_flow.py`, `runner.py`, `builders.py`, `state/`, `run-legacy` command).
- v1 → v2 config migration documentation.
- Prefect server/worker deployment scripts.
- GC cron deployment (follows same pattern; add when GC is ported to v2).

## Architecture

### Flow definitions — `sluice/dag/flows.py`

Two top-level functions, fixed entrypoints:

```
sluice.dag.flows:run_pipeline_flow
sluice.dag.flows:rotate_task_flow
```

```python
@flow(name="sluice-run-pipeline")
async def run_pipeline_flow(task: str, pipeline: str, config_dir: str = "./configs") -> dict:
    """Entrypoint for cron-triggered pipeline runs."""
    ...

@flow(name="sluice-rotate-task")
async def rotate_task_flow(task: str, config_dir: str = "./configs", sluice_config: str | None = None) -> dict:
    """Entrypoint for cron-triggered task rotation."""
    ...
```

Both return serialisable dicts (Prefect requires serialisable return values). Internally they call the same logic as the CLI `run` and `gc-rotate` commands — no new functionality.

### Deployment builder — `sluice/dag/deploy.py`

Pure functions with no Prefect server I/O, returning `RunnerDeployment` objects:

```python
async def build_pipeline_deployments(tasks, sluice_cfg, config_dir) -> list[...]: ...
async def build_rotation_deployments(tasks, sluice_cfg, config_dir) -> list[...]: ...
async def push_all_deployments(tasks, sluice_cfg, config_dir) -> tuple[int, int]: ...
    # returns (n_pipeline_deployments, n_rotation_deployments)
```

### Deployment naming

| Type | Prefect deployment name |
|------|------------------------|
| Pipeline | `{task_name}-{pipeline_name}` |
| Rotation | `{task_name}-rotate` |

### Concurrency

Every deployment is created with `concurrency_limit=1` and `concurrency_options=ConcurrencyOptions(collision_strategy="CANCEL_NEW")`. When a cron fires while the previous run is still executing, the new run is immediately cancelled — no queue build-up.

### Pipeline eligibility

A pipeline deployment is created when **all** of:
- `task.enabled is True`
- `pipeline.name in task.deployments.pipelines` (or `task.deployments.pipelines == "all"`)

### Rotation cron resolution

```
effective_rotation_cron = task.rotation_cron or sluice_cfg.rotation.default_cron
```

If `effective_rotation_cron is None` → no rotation deployment for this task. Silent skip.

## Config Schema Changes

### `sluice.toml`

```toml
[runtime]
prefect_api_url         = ""           # optional; default: local server
prefect_api_auth_string = ""           # optional; supports env:/ resolution
work_pool               = "default"    # required for push-deploy
default_timezone        = "UTC"

[gc.rotation]
drain_timeout   = "30m"
archive_dir     = "data/archive"
default_cron    = "0 3 * * *"         # new; omit to disable rotation scheduling
```

### `TaskConfig` (`sluice/dag/config.py`)

```python
rotation_cron: str | None = None   # new optional field; overrides sluice.toml default
```

### `SluiceConfig` / `sluice_config.py`

- `RotationConfig` adds `default_cron: str | None = None`.
- New `RuntimeConfig` dataclass: `prefect_api_url`, `prefect_api_auth_string`, `work_pool`, `default_timezone`.
- `SluiceConfig` adds `runtime: RuntimeConfig`.

## CLI change

`sluice deploy` (currently v1) is replaced in-place:

```python
@app.command()
def deploy(
    config_dir: Path = ...,
    sluice_config_path: Path | None = ...,
):
    """Register all v2 task pipelines + rotation as Prefect deployments."""
    tasks = load_all_tasks(config_dir)
    sluice_cfg = load_sluice_config(sluice_config_path)
    with _prefect_api_settings(sluice_cfg.runtime):
        n_pip, n_rot = asyncio.run(push_all_deployments(tasks, sluice_cfg, str(config_dir)))
    typer.echo(f"Registered {n_pip} pipeline deployment(s) + {n_rot} rotation deployment(s).")
```

`_prefect_api_settings` moves from `cli.py` to `deploy.py` and accepts `RuntimeConfig`.

v1 `run-legacy` and other v1 commands are untouched this plan.

## Data Flow

```
sluice deploy
  └─ load_all_tasks(configs/tasks/*/)
  └─ load_sluice_config(sluice.toml)
  └─ push_all_deployments
       ├─ for each task × eligible pipeline:
       │    run_pipeline_flow.deploy(name="{task}-{pipeline}", cron=pipeline.cron, ...)
       └─ for each task with effective rotation cron:
            rotate_task_flow.deploy(name="{task}-rotate", cron=effective_rotation_cron, ...)

[Prefect worker, on cron trigger]
  └─ run_pipeline_flow(task, pipeline, config_dir)
       └─ load_task → ensure_dynamic_tables → WriterThread → run_pipeline → return dict

  └─ rotate_task_flow(task, config_dir, sluice_config)
       └─ load_task → load_sluice_config → rotate_task → return dict
```

## Error Handling

| Scenario | Behaviour |
|----------|-----------|
| `work_pool` missing from config | `ConfigError` at deploy time |
| `prefect_api_auth_string` resolves to empty | `ConfigError` (existing behaviour) |
| No tasks found under `config_dir/tasks/` | Deploy exits with 0 deployments, prints warning |
| Pipeline run fails inside flow | Exception propagates to Prefect, run marked FAILED |
| Rotation `lock_busy` / `drain_timeout` | Flow returns `{"status": "lock_busy"}` dict, Prefect marks COMPLETED (not FAILED) — transient, retry on next cron |

## Testing Strategy

### `tests/dag/test_deploy.py` (unit)

- `build_pipeline_deployments` returns correct count, names, cron, parameters, concurrency_limit for a set of TaskConfig fixtures.
- `build_rotation_deployments`: task with per-task `rotation_cron` uses it over global default; task with `rotation_cron=None` and global default uses global; task with neither produces no deployment.
- `push_all_deployments` mocks `flow.deploy()` and verifies call count + args.

### `tests/dag/test_flows.py` (unit)

- `run_pipeline_flow` mocks `run_pipeline`; verifies return value is JSON-serialisable dict.
- `rotate_task_flow` mocks `rotate_task`; verifies `RotationResult` serialised correctly, `lock_busy` does not raise.

### `tests/dag/test_flow_entrypoints.py` (contract)

Grep test: `sluice.dag.flows` module exposes `run_pipeline_flow` and `rotate_task_flow` as module-level names, preventing recurrence of the `<locals>` closure bug.

## Non-Goals

- No Prefect server/worker setup scripts.
- No auto-discovery of new task directories at runtime (redeploy required when tasks are added).
- No webhook / event-based triggers (cron only).
- No GC cron deployment (GC not yet ported to v2 dag).
