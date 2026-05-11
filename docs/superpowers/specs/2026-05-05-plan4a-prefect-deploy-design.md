# Plan 4a — Prefect/cron Wire-up (v2) Design

## Summary

Wire the v2 DAG (`sluice/dag/`) into Prefect 3 so that pipeline runs and rotation are scheduled and executed by an external Prefect server + worker. Replace the existing `sluice deploy` command (which used the broken v1 `build_flow` closure approach) with a v2 implementation that pushes top-level flow deployments via `flow.to_deployment()` + `prefect.deploy(...)`.

## Why

The v1 `deploy` command wraps each pipeline in a `build_flow.<locals>._flow` closure. Prefect workers cannot resolve closure-defined functions from storage entrypoints, producing:

```
RuntimeError: Function with name 'build_flow.<locals>._flow' not found in 'sluice/flow.py'
```

The fix is to define flows as top-level module-level functions and use the `to_deployment()` + `prefect.deploy(...)` path, which separates deployment-object construction (pure, testable) from server I/O (one `prefect.deploy()` call).

## Scope

**In:**
- Two top-level `@flow` functions in `sluice/dag/flows.py`.
- `deploy.py` — deployment-building logic (`build_*` are pure; `push_all_deployments` owns the single `prefect.deploy()` call).
- `sluice.toml` schema extended: `[runtime]` + `[gc.rotation] default_cron`.
- `TaskConfig.rotation_cron` per-task override.
- `load_all_tasks(config_dir)` in `sluice/dag/loader.py`.
- `sluice deploy` command rewritten for v2.
- Unit + integration tests (no live Prefect server required).

**Out (Plan 4b):**
- Deleting v1 files (`flow.py`, `gc_flow.py`, `runner.py`, `builders.py`, `state/`, `run-legacy` command).
- v1 → v2 config migration documentation.
- Prefect server/worker deployment scripts.
- GC cron deployment (follows same pattern; add when GC is ported to v2).
- Concurrency collision strategy (`CANCEL_NEW` / `ENQUEUE`) — deferred to Plan 4b once correct Prefect 3 concurrency API is confirmed per work-pool type.

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
    """Entrypoint for cron-triggered pipeline runs.

    `task` is the subdirectory name under config_dir/tasks/ (e.g. "my_task").
    `pipeline` is the pipeline logical name as defined in task.toml.
    """
    ...

@flow(name="sluice-rotate-task")
async def rotate_task_flow(task: str, config_dir: str = "./configs", sluice_config: str | None = None) -> dict:
    """Entrypoint for cron-triggered task rotation.

    `sluice_config` is an absolute path string to sluice.toml; resolved inside
    the flow via load_sluice_config(Path(sluice_config)).
    """
    ...
```

Both return serialisable dicts (Prefect requires serialisable return values). Internally they replicate the logic of `cli.py dag_run` and `cli.py gc_rotate` — no new functionality beyond serialising the result.

**`run_pipeline_flow` internal sequence (mirrors `dag_run`):**
1. `load_task(config_dir/tasks/task/)`
2. `check_rotation_fast_path(...)` → if rotation in progress: return `{"status": "skipped_rotation"}` (matches CLI `dag_run` semantics; uses same `skipped_rotation` status, not `lock_busy`)
3. Acquire `RotationLock` (if fast-path passes; returns `{"status": "lock_busy"}` only if lock unexpectedly held after fast path)
4. `ensure_dynamic_tables(...)`
5. Start `WriterThread`
6. `run_pipeline(...)`
7. Stop `WriterThread`
8. Release `RotationLock`
9. Return serialisable result dict

`WriterThread` is created, started, and stopped inside the flow body. The flow owns its lifecycle.

### Deployment builder — `sluice/dag/deploy.py`

`build_*` functions are pure (no Prefect server I/O). They call `flow.to_deployment()` which returns a `RunnerDeployment` object locally. `push_all_deployments` then issues a single `await prefect.deploy(...)` call.

```python
from prefect.types.entrypoint import EntrypointType  # prefect.flows also re-exports this

def build_pipeline_deployment(task, pipeline, sluice_cfg, config_dir) -> RunnerDeployment:
    """Pure — no server call. Uses flow.to_deployment().

    entrypoint_type=MODULE_PATH required for process worker without image;
    without it prefect.deploy(build=False, push=False, image=None) raises at
    deploy time (Prefect 3.6.28 requires image, remote storage, or MODULE_PATH).
    """
    tz = task.timezone or sluice_cfg.runtime.default_timezone or "UTC"
    return run_pipeline_flow.to_deployment(
        name=f"{task.name}-{pipeline.name}",  # names pre-validated; no sanitize
        schedule=CronSchedule(cron=pipeline.cron, timezone=tz),
        concurrency_limit=1,
        parameters={"task": task.name, "pipeline": pipeline.name, "config_dir": config_dir},
        work_pool_name=sluice_cfg.runtime.work_pool,
        entrypoint_type=EntrypointType.MODULE_PATH,
    )

def build_rotation_deployment(task, effective_cron, sluice_cfg, config_dir, sluice_config_path) -> RunnerDeployment:
    """Pure — no server call."""
    tz = task.timezone or sluice_cfg.runtime.default_timezone or "UTC"
    return rotate_task_flow.to_deployment(
        name=f"{task.name}-rotate",  # names pre-validated; no sanitize
        schedule=CronSchedule(cron=effective_cron, timezone=tz),
        concurrency_limit=1,
        parameters={"task": task.name, "config_dir": config_dir, "sluice_config": sluice_config_path},
        work_pool_name=sluice_cfg.runtime.work_pool,
        entrypoint_type=EntrypointType.MODULE_PATH,
    )

async def push_all_deployments(tasks, sluice_cfg, config_dir, sluice_config_path) -> tuple[int, int]:
    """Single prefect.deploy() call; returns (n_pipeline, n_rotation)."""
    deployments = build_pipeline_deployments(...) + build_rotation_deployments(...)
    deploy_kwargs: dict = {"build": False, "push": False}
    if sluice_cfg.runtime.image:
        deploy_kwargs["image"] = sluice_cfg.runtime.image  # only pass image when set
    await prefect.deploy(*deployments, work_pool_name=sluice_cfg.runtime.work_pool, **deploy_kwargs)
    return (n_pip, n_rot)
```

**API path chosen:** `flow.to_deployment()` (returns `RunnerDeployment`, no I/O) + `prefect.deploy(*deployments)` (one server round-trip). This is the correct Prefect 3 push-deploy pattern for batching multiple deployments.

### Deployment naming

| Type | Prefect deployment name |
|------|------------------------|
| Pipeline | `{task_name}-{pipeline_name}` |
| Rotation | `{task_name}-rotate` |

**Name validation (fail-fast, not silent sanitize):** v2 task and pipeline names already enforce `[a-zA-Z0-9_]` via `validate_task` / config validation. Deployment builder calls `validate_task(task)` before constructing names. `validate_task` currently raises `ValidationError` (a `ValueError` subclass); the deployment builder wraps this as `ConfigError` with the offending name before re-raising. No silent character replacement — a sanitized name could collide with an existing deployment silently. `_sanitize_name` is not introduced.

### Concurrency

Every deployment is created with `concurrency_limit=1`. When a cron fires while the previous run is still executing, Prefect enforces the limit at the work-pool or deployment level.

**Collision strategy (`CANCEL_NEW` / `ENQUEUE`) is deferred to Plan 4b.** Prefect 3 concurrency collision configuration varies by work-pool type and is not a `to_deployment()` parameter; it requires work-pool configuration or `@flow(on_concurrency_limit_reached=...)`. This plan only sets the limit; the strategy is the Prefect default (queue).

### Pipeline eligibility

A pipeline deployment is created when **all** of:
- `task.enabled is True`
- `pipeline.name in task.deployments.pipelines` (or `task.deployments.pipelines == "all"`)
- `pipeline.cron is not None` (pipelines without a cron schedule are not deployed)

**`PipelineDef.cron` type change (in scope):** Currently `cron: str` (required). This plan changes it to `cron: str | None = None` in `sluice/dag/config.py` and updates the loader to treat missing/null `cron` entries as `None`. Pipelines with `cron = null` or no `cron` key are valid — they run on-demand only, no Prefect deployment is created.

### Rotation cron resolution

```
effective_rotation_cron = task.rotation_cron or sluice_cfg.rotation.default_cron
```

If `effective_rotation_cron is None` → no rotation deployment for this task. Silent skip.

### Timezone resolution

```
effective_timezone = task.timezone or sluice_cfg.runtime.default_timezone or "UTC"
```

Used in `CronSchedule(cron=..., timezone=effective_timezone)`. `default_timezone` in `RuntimeConfig` exists solely for this purpose.

## Config Schema Changes

### `sluice.toml`

```toml
[runtime]
prefect_api_url         = ""                # optional; default: local server
# prefect_api_auth_string = "env:MY_TOKEN" # optional; omit if unauthenticated; env: reads env var by name
work_pool               = "default"         # required for push-deploy; ConfigError if missing
default_timezone        = "UTC"             # used for CronSchedule timezone; default "UTC"
# image = "my-registry/sluice:latest"       # optional; if set, passed to prefect.deploy(image=...)

[gc.rotation]
drain_timeout   = "30m"
archive_dir     = "data/archive"
default_cron    = "0 3 * * *"              # new; omit or leave blank to disable rotation scheduling
```

**`prefect_api_auth_string` `env:` resolution** is existing behaviour inherited from v1 `_prefect_api_settings`. No new implementation needed; mark as-is in code.

**`image` / `build` / `push`:** `prefect.deploy()` defaults to `build=True, push=True`, which triggers Docker builds and registry pushes. This plan defaults to `build=False, push=False` (process worker, no image). If `runtime.image` is set, it is passed as `image=sluice_cfg.runtime.image`; otherwise `image` is omitted and `build=False, push=False` are explicit.

### `TaskConfig` (`sluice/dag/config.py`)

```python
rotation_cron: str | None = None   # new; overrides sluice.toml [gc.rotation] default_cron
timezone: str | None = None        # new (if not already present); per-task CronSchedule timezone
```

### `SluiceConfig` / `sluice_config.py`

- `RotationConfig` adds `default_cron: str | None = None`.
- New `RuntimeConfig` dataclass added to `sluice/dag/sluice_config.py`:

```python
@dataclass
class RuntimeConfig:
    prefect_api_url: str = ""
    prefect_api_auth_string: str | None = None  # None = unauthenticated; ConfigError only if env: resolves empty
    work_pool: str = "default"
    default_timezone: str = "UTC"
    image: str | None = None  # container image; None = process worker (build=False, push=False)
```

**Naming note:** v1 `sluice/config.py` already defines a `RuntimeConfig` (Pydantic model). These are in separate modules and do not collide at runtime. However, to avoid confusion during the Plan 4a/4b transition period, `_prefect_api_settings` in `deploy.py` accepts the v2 `sluice_config.RuntimeConfig` dataclass explicitly. The v1 `RuntimeConfig` is deleted in Plan 4b.

- `SluiceConfig` adds `runtime: RuntimeConfig = field(default_factory=RuntimeConfig)`.
- `load_sluice_config` extended to parse `[runtime]` section.

### `load_all_tasks` — `sluice/dag/loader.py`

New function:

```python
def load_all_tasks(config_dir: Path) -> list[TaskConfig]:
    """Glob config_dir/tasks/*/task.toml and load each."""
    task_dirs = sorted(config_dir.glob("tasks/*/"))
    return [load_task(d) for d in task_dirs if (d / "task.toml").exists()]
```

Returns empty list (not error) if no task directories exist; `push_all_deployments` will print a warning and register 0 deployments.

### `PipelineDef.cron`

`PipelineDef.cron` currently typed as `str` (required) in `sluice/dag/config.py`. **This plan changes it to `str | None = None`** (see Pipeline eligibility — `cron` type change in scope). The loader is updated to treat a missing or `null` `cron` entry as `None`. No other `PipelineDef` fields change.

## CLI change

`sluice deploy` (currently v1) is replaced in-place:

```python
@app.command()
def deploy(
    config_dir: Path = typer.Option(Path("./configs"), "--config-dir", "-c"),
    sluice_config_path: Path | None = typer.Option(None, "--sluice-config"),
):
    """Register all v2 task pipelines + rotation as Prefect deployments."""
    config_dir = config_dir.resolve()
    # If --sluice-config not provided, derive absolute default: <config_dir>/sluice.toml
    # Pass this resolved path to both load_sluice_config and flow parameters so
    # CLI and worker always reference the same file regardless of worker cwd.
    effective_sluice_config = (
        sluice_config_path.resolve() if sluice_config_path
        else (config_dir / "sluice.toml").resolve()
    )
    tasks = load_all_tasks(config_dir)
    sluice_cfg = load_sluice_config(effective_sluice_config)
    with _prefect_api_settings(sluice_cfg.runtime):
        n_pip, n_rot = asyncio.run(
            push_all_deployments(tasks, sluice_cfg, str(config_dir), str(effective_sluice_config))
        )
    typer.echo(f"Registered {n_pip} pipeline deployment(s) + {n_rot} rotation deployment(s).")
```

**CLI interface preserved:** `--config-dir / -c` remains an Option (not Argument), matching existing usage. `--sluice-config` default is `<config_dir>/sluice.toml` (resolved absolute), ensuring CLI and Prefect worker read the same file.

`_prefect_api_settings` moves from `cli.py` to `deploy.py` and accepts v2 `RuntimeConfig`.

v1 `run-legacy` and other v1 commands are untouched this plan.

## Data Flow

```
sluice deploy
  └─ config_dir.resolve(), sluice_config_path.resolve()
  └─ load_all_tasks(config_dir/tasks/*/)
  └─ load_sluice_config(sluice_config_path)
  └─ push_all_deployments(tasks, sluice_cfg, config_dir_str, sluice_config_str)
       ├─ for each task × eligible pipeline:
       │    run_pipeline_flow.to_deployment(
       │        name="{task}-{pipeline}",  # validated by validate_task; no silent sanitize
       │        schedule=CronSchedule(cron=pipeline.cron, timezone=effective_tz),
       │        concurrency_limit=1,
       │        parameters={task, pipeline, config_dir},
       │    )
       ├─ for each task with effective rotation cron:
       │    rotate_task_flow.to_deployment(
       │        name="{task}-rotate",  # validated; no silent sanitize
       │        schedule=CronSchedule(cron=effective_rotation_cron, timezone=effective_tz),
       │        concurrency_limit=1,
       │        parameters={task, config_dir, sluice_config},
       │    )
       └─ await prefect.deploy(*all_deployments, work_pool_name=sluice_cfg.runtime.work_pool)

[Prefect worker, on cron trigger]
  └─ run_pipeline_flow(task, pipeline, config_dir)
       ├─ load_task(config_dir/tasks/task/)
       ├─ check_rotation_fast_path  ──► if rotation active: return {"status": "skipped_rotation"}
       ├─ acquire RotationLock  ──► if busy (post fast-path): return {"status": "lock_busy"}
       ├─ ensure_dynamic_tables(...)
       ├─ start WriterThread
       ├─ run_pipeline(...)
       ├─ stop WriterThread
       ├─ release RotationLock
       └─ return {"status": "ok", ...}

  └─ rotate_task_flow(task, config_dir, sluice_config)
       ├─ load_task(config_dir/tasks/task/)
       ├─ load_sluice_config(Path(sluice_config))
       ├─ rotate_task(...)
       └─ return {"status": "ok", ...} or {"status": "lock_busy"}
```

## Error Handling

| Scenario | Behaviour |
|----------|-----------|
| `work_pool` missing from config | `ConfigError` at deploy time |
| `prefect_api_auth_string` resolves to empty | `ConfigError` (existing behaviour) |
| No tasks found under `config_dir/tasks/` | Deploy exits with 0 deployments, prints warning |
| Pipeline run fails inside flow | Exception propagates to Prefect; run marked FAILED |
| Rotation `lock_busy` / `drain_timeout` | Flow returns `{"status": "lock_busy"}`; Prefect marks COMPLETED (transient, retry on next cron) |
| Partial deployment failure (`prefect.deploy` raises mid-batch) | No rollback; already-registered deployments in this batch remain. `push_all_deployments` propagates the exception unchanged. CLI exits non-zero. Operator re-runs `sluice deploy` (idempotent by name). No per-deployment success count reported — batch call is atomic from caller's view. |

## Testing Strategy

### `tests/dag/test_deploy.py` (unit)

- `build_pipeline_deployment` returns `RunnerDeployment` with correct name, `CronSchedule` (including timezone), parameters, `concurrency_limit=1`.
- `build_rotation_deployment`: per-task `rotation_cron` takes priority over global default; task with neither → no deployment.
- `push_all_deployments` patches `prefect.deploy` (not `flow.deploy`); verifies call count, deployment names, `build=False`/`push=False` kwargs, parameter values.
- Test that invalid task name (non-`[a-zA-Z0-9_]`) raises `ConfigError`, not `ValidationError`.

### `tests/dag/test_flows.py` (unit)

- Patch `@flow` decorator as identity during tests (`monkeypatch` or module-level import patch) so flow functions run as plain async functions without Prefect runtime.
- `run_pipeline_flow`: mocks `run_pipeline`, `WriterThread`, `RotationLock`; verifies return value is JSON-serialisable; verifies `lock_busy` path returns dict without raising.
- `rotate_task_flow`: mocks `rotate_task`; verifies `RotationResult` serialised correctly.

### `tests/dag/test_flow_entrypoints.py` (contract)

Grep test: `sluice.dag.flows` module exposes `run_pipeline_flow` and `rotate_task_flow` as module-level names (not `<locals>`), preventing recurrence of the closure bug.

### Migration: existing deploy tests

`tests/test_cli_deploy.py` currently asserts against v1 `sluice.flow.build_flow` / `prefect_serve`. These tests must be **replaced** (not updated) with the new `tests/dag/test_deploy.py` suite. The v1 test file is deleted as part of this plan; v1 behaviour is covered by the `run-legacy` path which is untouched.

## Non-Goals

- No Prefect server/worker setup scripts.
- No auto-discovery of new task directories at runtime (redeploy required when tasks are added).
- No webhook / event-based triggers (cron only).
- No GC cron deployment (GC not yet ported to v2 dag).
- No concurrency collision strategy configuration (`CANCEL_NEW` / `ENQUEUE`) — deferred to Plan 4b.
