# Plan 4a — Prefect/cron Wire-up (v2) Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace v1 `sluice deploy` with v2 push-deploy using top-level `@flow` functions in `sluice/dag/flows.py`.

**Architecture:** Two `@flow` entrypoints (`run_pipeline_flow`, `rotate_task_flow`) as module-level top-level functions. `deploy.py` builds `RunnerDeployment` objects via `flow.to_deployment()` (pure, no I/O) then batches them in one `prefect.deploy()` call. `load_all_tasks` globs `config_dir/tasks/*/task.toml`. Config extended with `RuntimeConfig` and `RotationConfig.default_cron`.

**Tech Stack:** Python 3.11+, Prefect 3, Typer, dataclasses, pytest + pytest-asyncio

**Spec:** `docs/superpowers/specs/2026-05-05-plan4a-prefect-deploy-design.md`

---

### Task 1: Extend SluiceConfig — RuntimeConfig + default_cron

**Files:**
- Modify: `sluice/dag/sluice_config.py` (whole file)
- Modify: `tests/dag/test_sluice_config.py` (add tests)

**Step 1: Write the failing test — RuntimeConfig parsing + default_cron**

Add to `tests/dag/test_sluice_config.py`:

```python
from sluice.dag.sluice_config import RuntimeConfig, SluiceConfig


def test_runtime_config_defaults():
    rt = RuntimeConfig()
    assert rt.prefect_api_url == ""
    assert rt.prefect_api_auth_string == ""
    assert rt.work_pool == "default"
    assert rt.default_timezone == "UTC"


def test_load_sluice_config_parses_runtime(tmp_path):
    path = tmp_path / "sluice.toml"
    path.write_text(
        """
        [gc.rotation]
        drain_timeout = "2h"
        archive_dir = "archive"

        [runtime]
        prefect_api_url = "http://localhost:4200"
        prefect_api_auth_string = "env:PREFECT_KEY"
        work_pool = "prod-pool"
        default_timezone = "Asia/Shanghai"
        """
    )
    cfg = load_sluice_config(path)
    assert cfg.runtime.prefect_api_url == "http://localhost:4200"
    assert cfg.runtime.prefect_api_auth_string == "env:PREFECT_KEY"
    assert cfg.runtime.work_pool == "prod-pool"
    assert cfg.runtime.default_timezone == "Asia/Shanghai"


def test_load_sluice_config_runtime_defaults_when_missing(tmp_path):
    path = tmp_path / "sluice.toml"
    path.write_text("[gc.rotation]\ndrain_timeout = \"1h\"\n")
    cfg = load_sluice_config(path)
    assert isinstance(cfg.runtime, RuntimeConfig)
    assert cfg.runtime.work_pool == "default"
    assert cfg.runtime.default_timezone == "UTC"


def test_load_sluice_config_parses_default_cron(tmp_path):
    path = tmp_path / "sluice.toml"
    path.write_text(
        """
        [gc.rotation]
        default_cron = "0 4 * * *"
        """
    )
    cfg = load_sluice_config(path)
    assert cfg.rotation.default_cron == "0 4 * * *"


def test_load_sluice_config_default_cron_none_when_missing(tmp_path):
    path = tmp_path / "sluice.toml"
    path.write_text("[gc.rotation]\n")
    cfg = load_sluice_config(path)
    assert cfg.rotation.default_cron is None
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/dag/test_sluice_config.py::test_runtime_config_defaults -v`
Expected: `ImportError: cannot import name 'RuntimeConfig'`

**Step 3: Write implementation**

Replace `sluice/dag/sluice_config.py`:

```python
from __future__ import annotations

import re
import tomllib
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class RuntimeConfig:
    prefect_api_url: str = ""
    prefect_api_auth_string: str = ""
    work_pool: str = "default"
    default_timezone: str = "UTC"


@dataclass(frozen=True)
class RotationConfig:
    drain_timeout_seconds: float = 30 * 60.0
    archive_dir: str = "data/archive"
    default_cron: str | None = None


@dataclass(frozen=True)
class SluiceConfig:
    rotation: RotationConfig = field(default_factory=RotationConfig)
    runtime: RuntimeConfig = field(default_factory=RuntimeConfig)


def _parse_duration(text: str | None, default: float) -> float:
    if text is None:
        return default
    value = text.strip().lower()
    if not value:
        return default
    match = re.fullmatch(r"(\d+(?:\.\d+)?)([smhd]?)", value)
    if match is None:
        raise ValueError(f"invalid duration: {text!r}")
    number = float(match.group(1))
    unit = match.group(2) or "s"
    multiplier = {"s": 1.0, "m": 60.0, "h": 3600.0, "d": 86400.0}[unit]
    return number * multiplier


def load_sluice_config(config_path: Path | None = None) -> SluiceConfig:
    path = config_path or Path("sluice.toml")
    if not path.exists():
        return SluiceConfig()
    data = tomllib.loads(path.read_text())
    rotation = data.get("gc", {}).get("rotation", {})
    runtime = data.get("runtime", {})
    return SluiceConfig(
        rotation=RotationConfig(
            drain_timeout_seconds=_parse_duration(
                rotation.get("drain_timeout", "30m"), 30 * 60.0
            ),
            archive_dir=rotation.get("archive_dir", "data/archive"),
            default_cron=rotation.get("default_cron"),
        ),
        runtime=RuntimeConfig(
            prefect_api_url=runtime.get("prefect_api_url", ""),
            prefect_api_auth_string=runtime.get("prefect_api_auth_string", ""),
            work_pool=runtime.get("work_pool", "default"),
            default_timezone=runtime.get("default_timezone", "UTC"),
        ),
    )
```

**Step 4: Run tests to verify they pass**

Run: `pytest tests/dag/test_sluice_config.py -v`
Expected: All tests PASS

**Step 5: Verify existing tests still pass**

Run: `pytest tests/dag/ -v -k "not test_deploy" -k "not test_flow"`
Expected: All existing DAG tests PASS (SluiceConfig now has `runtime` attr — existing code doesn't reference it)

**Step 6: Commit**

```bash
git add sluice/dag/sluice_config.py tests/dag/test_sluice_config.py
git commit -m "feat(config): add RuntimeConfig and RotationConfig.default_cron to v2 SluiceConfig"
```

---

### Task 2: Add rotation_cron to TaskConfig

**Files:**
- Modify: `sluice/dag/config.py:67-79`
- Modify: `sluice/dag/loader.py:86-104`
- Modify: `tests/dag/test_config.py` (add test)
- Modify: `tests/dag/test_loader.py` (add test)

**Step 1: Write the failing tests**

Add to `tests/dag/test_config.py`:

```python
def test_task_config_rotation_cron_default():
    t = TaskConfig(
        name="x", db_path="a.db", seen_db="s.db", enabled=True,
        timezone=None,
        window=WindowDef(anchor="* * * * *", length="1h"),
        deployments=DeploymentsDef(pipelines="all"),
        sources=[SourceDef(name="s", type="rss")],
        sinks=[],
        stages=[],
        pipelines=[],
    )
    assert t.rotation_cron is None


def test_task_config_rotation_cron_explicit():
    t = TaskConfig(
        name="x", db_path="a.db", seen_db="s.db", enabled=True,
        timezone=None,
        window=WindowDef(anchor="* * * * *", length="1h"),
        deployments=DeploymentsDef(pipelines="all"),
        sources=[SourceDef(name="s", type="rss")],
        sinks=[],
        stages=[],
        pipelines=[],
        rotation_cron="0 6 * * *",
    )
    assert t.rotation_cron == "0 6 * * *"
```

Add to `tests/dag/test_loader.py`:

```python
def test_load_task_rotation_cron(tmp_path: Path):
    import shutil
    d = tmp_path / "task"
    shutil.copytree(FIXTURE, d)
    text = (d / "task.toml").read_text()
    text = text.replace('enabled = true', 'enabled = true\nrotation_cron = "0 6 * * *"')
    (d / "task.toml").write_text(text)
    t = load_task(d)
    assert t.rotation_cron == "0 6 * * *"
```

**Step 2: Run to verify failure**

Run: `pytest tests/dag/test_config.py::test_task_config_rotation_cron_default -v`
Expected: `TypeError: TaskConfig.__init__() got an unexpected keyword argument 'rotation_cron'`

**Step 3: Implement**

Edit `sluice/dag/config.py` line 78, add field:

```python
rotation_cron: str | None = None
```

Edit `sluice/dag/loader.py` line 86, parse the field:

```python
return TaskConfig(
    ...
    rotation_cron=task_doc.get("rotation_cron"),
    ...
)
```

**Step 4: Verify tests pass**

Run: `pytest tests/dag/test_config.py::test_task_config_rotation_cron_default tests/dag/test_config.py::test_task_config_rotation_cron_explicit tests/dag/test_loader.py::test_load_task_rotation_cron -v`
Expected: All PASS

**Step 5: Commit**

```bash
git add sluice/dag/config.py sluice/dag/loader.py tests/dag/test_config.py tests/dag/test_loader.py
git commit -m "feat(config): add rotation_cron field to TaskConfig"
```

---

### Task 3: Add load_all_tasks

**Files:**
- Modify: `sluice/dag/loader.py` (add function)
- Modify: `tests/dag/test_loader.py` (add tests)

**Step 1: Write the failing tests**

Add to `tests/dag/test_loader.py`:

```python
from sluice.dag.loader import load_all_tasks


def test_load_all_tasks_discovers_all(tmp_path: Path):
    import shutil
    src = FIXTURE
    for name in ("a", "b", "c"):
        dst = tmp_path / "tasks" / name
        dst.mkdir(parents=True)
        shutil.copytree(src, dst, dirs_exist_ok=True)
        (dst / "task.toml").write_text(
            (src / "task.toml").read_text().replace(
                'name = "example"', f'name = "{name}"'
            ).replace(
                'db_path  = "data/example.db"', f'db_path  = "data/{name}.db"'
            ).replace(
                'seen_db  = "data/example_seen.db"', f'seen_db  = "data/{name}_seen.db"'
            )
        )
    tasks = load_all_tasks(tmp_path)
    names = {t.name for t in tasks}
    assert names == {"a", "b", "c"}


def test_load_all_tasks_empty_when_no_task_dirs(tmp_path: Path):
    (tmp_path / "tasks").mkdir()
    tasks = load_all_tasks(tmp_path)
    assert tasks == []


def test_load_all_tasks_skips_missing_toml(tmp_path: Path):
    import shutil
    src = FIXTURE
    good = tmp_path / "tasks" / "good"
    good.mkdir(parents=True)
    shutil.copytree(src, good, dirs_exist_ok=True)
    (tmp_path / "tasks" / "empty").mkdir(parents=True)
    tasks = load_all_tasks(tmp_path)
    assert len(tasks) == 1
    assert tasks[0].name == "example"
```

**Step 2: Run to verify failure**

Run: `pytest tests/dag/test_loader.py::test_load_all_tasks_discovers_all -v`
Expected: `ImportError: cannot import name 'load_all_tasks'`

**Step 3: Implement**

Add to `sluice/dag/loader.py` after `load_task`:

```python
def load_all_tasks(config_dir: Path) -> list[TaskConfig]:
    """Glob config_dir/tasks/*/task.toml and load each."""
    task_dirs = sorted(config_dir.glob("tasks/*/"))
    return [load_task(d) for d in task_dirs if (d / "task.toml").exists()]
```

Note: `Path` is already imported.

**Step 4: Verify tests pass**

Run: `pytest tests/dag/test_loader.py -v`
Expected: All PASS (6 tests: 4 existing + 3 new, minus 1 that becomes redundant? No — existing tests test `load_task`, new ones test `load_all_tasks`)

**Step 5: Commit**

```bash
git add sluice/dag/loader.py tests/dag/test_loader.py
git commit -m "feat(loader): add load_all_tasks for batch task directory discovery"
```

---

### Task 4: Write flow definitions (flows.py)

**Files:**
- Create: `sluice/dag/flows.py`
- Create: `tests/dag/test_flows.py`

**Step 1: Write the failing test**

Create `tests/dag/test_flows.py`:

```python
from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Patch @flow to identity BEFORE importing sluice.dag.flows
# ---------------------------------------------------------------------------
import prefect

prefect.flow = lambda **kwargs: lambda fn: fn


@pytest.fixture
def mock_dependencies(monkeypatch):
    """Replace run_pipeline, rotate_task, WriterThread, RotationLock with mocks."""
    fake_run_pipeline = AsyncMock()
    fake_run_pipeline.return_value = MagicMock(
        pipeline_name="test_pipeline",
        run_key="test_key",
        status="success",
        items_in=10,
        items_out=8,
        error_msg=None,
    )
    monkeypatch.setattr("sluice.dag.flows.run_pipeline", fake_run_pipeline)

    fake_rotate_task = AsyncMock()
    fake_rotate_task.return_value = MagicMock(
        task_name="test_task",
        status="ok",
        carryover_count=5,
        archive_count=20,
        archive_path="/tmp/archive.db",
        error_msg=None,
    )
    monkeypatch.setattr("sluice.dag.flows.rotate_task", fake_rotate_task)

    fake_lock = MagicMock()
    fake_lock.acquire.return_value = None
    fake_lock.release.return_value = None
    fake_lock_cls = MagicMock(return_value=fake_lock)
    monkeypatch.setattr("sluice.dag.flows.RotationLock", fake_lock_cls)

    fake_writer = MagicMock()
    fake_writer.start = AsyncMock()
    fake_writer.stop = AsyncMock()
    fake_writer_cls = MagicMock(return_value=fake_writer)
    monkeypatch.setattr("sluice.dag.flows.WriterThread", fake_writer_cls)

    fake_ensure = AsyncMock()
    monkeypatch.setattr("sluice.dag.flows.ensure_dynamic_tables", fake_ensure)

    # Mock load_task to return a valid TaskConfig
    from sluice.dag.config import (
        DeploymentsDef,
        PipelineDef,
        SinkDef,
        SourceDef,
        StageDef,
        TaskConfig,
        WindowDef,
    )

    fake_task = TaskConfig(
        name="test_task",
        db_path="/tmp/test.db",
        seen_db="/tmp/test_seen.db",
        enabled=True,
        timezone="UTC",
        window=WindowDef(anchor="* * * * *", length="1h"),
        deployments=DeploymentsDef(pipelines="all"),
        sources=[SourceDef(name="src", type="rss")],
        sinks=[SinkDef(name="sk", type="file_md")],
        stages=[StageDef(name="st", type="noop")],
        pipelines=[PipelineDef(name="p1", cron="0 * * * *", stages=["st"], sources=["src"], upstream_all=None, upstream_any=None, sinks=["sk"], post_run_cleanup=None)],
        rotation_cron="0 3 * * *",
    )
    monkeypatch.setattr("sluice.dag.flows.load_task", lambda path: fake_task)

    from sluice.dag.sluice_config import SluiceConfig

    sluice_cfg = SluiceConfig()
    monkeypatch.setattr(
        "sluice.dag.flows.load_sluice_config",
        lambda path=None: sluice_cfg,
    )

    return {
        "run_pipeline": fake_run_pipeline,
        "rotate_task": fake_rotate_task,
        "lock": fake_lock,
        "writer": fake_writer,
        "ensure_tables": fake_ensure,
    }


# Import flows AFTER patching prefect.flow
from sluice.dag.flows import run_pipeline_flow, rotate_task_flow  # noqa: E402


class TestRunPipelineFlow:
    async def test_success_path(self, mock_dependencies):
        result = await run_pipeline_flow(
            task="test_task",
            pipeline="p1",
            config_dir="/tmp/configs",
        )
        assert result["status"] == "success"
        assert result["pipeline_name"] == "test_pipeline"
        assert isinstance(result, dict)

        # Verify lock lifecycle
        mock_dependencies["lock"].acquire.assert_called_once()
        mock_dependencies["lock"].release.assert_called_once()

        # Verify WriterThread lifecycle
        mock_dependencies["writer"].start.assert_awaited_once()
        mock_dependencies["writer"].stop.assert_awaited_once()

        # Verify pipeline was called
        mock_dependencies["run_pipeline"].assert_awaited_once()

    async def test_lock_busy_returns_dict(self, mock_dependencies):
        mock_dependencies["lock"].acquire.side_effect = RuntimeError("lock busy")
        result = await run_pipeline_flow(
            task="test_task",
            pipeline="p1",
            config_dir="/tmp/configs",
        )
        assert result["status"] == "lock_busy"
        # No pipeline should run when lock is busy
        mock_dependencies["run_pipeline"].assert_not_awaited()

    async def test_exception_propagates(self, mock_dependencies):
        mock_dependencies["run_pipeline"].side_effect = ValueError("boom")
        with pytest.raises(ValueError, match="boom"):
            await run_pipeline_flow(
                task="test_task",
                pipeline="p1",
                config_dir="/tmp/configs",
            )
        # WriterThread should still be stopped even on failure
        mock_dependencies["writer"].stop.assert_awaited_once()


class TestRotateTaskFlow:
    async def test_success_path(self, mock_dependencies):
        result = await rotate_task_flow(
            task="test_task",
            config_dir="/tmp/configs",
        )
        assert result["status"] == "ok"
        assert result["task_name"] == "test_task"
        assert isinstance(result, dict)
        mock_dependencies["rotate_task"].assert_awaited_once()

    async def test_lock_busy_serialised(self, mock_dependencies):
        mock_dependencies["rotate_task"].return_value = MagicMock(
            task_name="test_task",
            status="lock_busy",
            carryover_count=0,
            archive_count=0,
            archive_path=None,
            error_msg=None,
        )
        result = await rotate_task_flow(
            task="test_task",
            config_dir="/tmp/configs",
        )
        assert result["status"] == "lock_busy"
        # lock_busy is NOT an exception — flow should complete normally
```

**Wait — flow testing issue:** The `run_pipeline` import inside `flows.py` is `from sluice.dag.run_pipeline import run_pipeline`. When we monkeypatch `sluice.dag.flows.run_pipeline`, we patch the reference after `flows.py` is imported. But `flows.py` does `from sluice.dag.run_pipeline import run_pipeline` at module level, so monkeypatching `sluice.dag.flows.run_pipeline` should work since it's a module-level attribute.

Actually, the flow functions reference the imports within the function body, not at call time. Since `flows.py` imports `run_pipeline` at the top and assigns it to a module attribute, monkeypatching `sluice.dag.flows.run_pipeline` replaces that reference. So when `run_pipeline_flow` calls `run_pipeline(...)`, it will use the mocked version. This should work.

But wait — `flows.py` does `from sluice.dag.run_pipeline import run_pipeline`. If we monkeypatch `sluice.dag.flows.run_pipeline`, that references the same function object. Yes, this works.

However, `flows.py` also imports `WriterThread`, `RotationLock`, `ensure_dynamic_tables`, `load_task`, `load_sluice_config`, `rotate_task`. So we monkeypatch all of these on `sluice.dag.flows.<name>`.

**Step 2: Run to verify failure**

Run: `pytest tests/dag/test_flows.py -v`
Expected: `ModuleNotFoundError: No module named 'sluice.dag.flows'`

**Step 3: Write implementation**

Create `sluice/dag/flows.py`:

```python
from __future__ import annotations

from pathlib import Path

from prefect import flow

from sluice.dag.config import TaskConfig
from sluice.dag.loader import load_task
from sluice.dag.rotation.lock import RotationLock
from sluice.dag.rotation.orchestrator import rotate_task
from sluice.dag.run_pipeline import (
    RunResult,
    ensure_dynamic_tables,
    run_pipeline,
)
from sluice.dag.sluice_config import load_sluice_config
from sluice.dag.writer import WriterThread


def _serialise_run_result(result: RunResult) -> dict:
    return {
        "pipeline_name": result.pipeline_name,
        "run_key": result.run_key,
        "status": result.status,
        "items_in": result.items_in,
        "items_out": result.items_out,
        "error_msg": result.error_msg,
    }


def _serialise_rotation_result(result) -> dict:
    return {
        "task_name": result.task_name,
        "status": result.status,
        "carryover_count": result.carryover_count,
        "archive_count": result.archive_count,
        "archive_path": result.archive_path,
        "error_msg": result.error_msg,
    }


@flow(name="sluice-run-pipeline")
async def run_pipeline_flow(
    task: str, pipeline: str, config_dir: str = "./configs"
) -> dict:
    """Entrypoint for cron-triggered pipeline runs.

    ``task`` is the subdirectory name under config_dir/tasks/ (e.g. "my_task").
    ``pipeline`` is the pipeline logical name as defined in task.toml.
    """
    cfg: TaskConfig = load_task(Path(config_dir) / "tasks" / task)

    lock = RotationLock(cfg.db_path)
    try:
        lock.acquire()
    except RuntimeError:
        return {"status": "lock_busy", "pipeline_name": pipeline}

    try:
        await ensure_dynamic_tables(cfg)
        writer = WriterThread(
            active_path=cfg.db_path,
            seen_path=cfg.seen_db,
            hold_rotation_lock=False,
        )
        await writer.start()
        try:
            result = await run_pipeline(cfg, pipeline, writer=writer)
        finally:
            await writer.stop()
    finally:
        lock.release()

    return _serialise_run_result(result)


@flow(name="sluice-rotate-task")
async def rotate_task_flow(
    task: str,
    config_dir: str = "./configs",
    sluice_config: str | None = None,
) -> dict:
    """Entrypoint for cron-triggered task rotation.

    ``sluice_config`` is an absolute path string to sluice.toml; resolved inside
    the flow via load_sluice_config(Path(sluice_config)).
    """
    cfg: TaskConfig = load_task(Path(config_dir) / "tasks" / task)
    sluice_cfg = load_sluice_config(
        Path(sluice_config) if sluice_config else None
    )
    result = await rotate_task(cfg, config=sluice_cfg)
    return _serialise_rotation_result(result)
```

**Step 4: Verify tests pass**

Run: `pytest tests/dag/test_flows.py -v`
Expected: All PASS (4 tests)

**Potential failure:** `prefect.flow = lambda **kwargs: lambda fn: fn` must happen at module level in the test file BEFORE `from sluice.dag.flows import ...`. Since Python caches modules, this works if `sluice.dag.flows` hasn't been imported yet. If another test already imported it, we may need `importlib.reload`. Pytest isolation should handle this per-file.

**Step 5: Commit**

```bash
git add sluice/dag/flows.py tests/dag/test_flows.py
git commit -m "feat(dag): add Prefect flow entrypoints run_pipeline_flow and rotate_task_flow"
```

---

### Task 5: Write deployment builder (deploy.py)

**Files:**
- Create: `sluice/dag/deploy.py`
- Create: `tests/dag/test_deploy.py`

**Step 1: Write the failing test**

Create `tests/dag/test_deploy.py`:

```python
from __future__ import annotations

import re
from unittest.mock import AsyncMock, patch

import pytest
from prefect.client.schemas.schedules import CronSchedule

from sluice.dag.config import (
    DeploymentsDef,
    PipelineDef,
    SinkDef,
    SourceDef,
    StageDef,
    TaskConfig,
    WindowDef,
)
from sluice.dag.deploy import (
    _sanitize_name,
    build_pipeline_deployment,
    build_rotation_deployment,
    push_all_deployments,
)
from sluice.dag.sluice_config import RuntimeConfig, SluiceConfig


@pytest.fixture
def sluice_cfg() -> SluiceConfig:
    return SluiceConfig(
        runtime=RuntimeConfig(
            work_pool="test-pool",
            default_timezone="Asia/Shanghai",
        ),
    )


@pytest.fixture
def task_cfg() -> TaskConfig:
    return TaskConfig(
        name="my_task",
        db_path="/tmp/db",
        seen_db="/tmp/seen",
        enabled=True,
        timezone="UTC",
        window=WindowDef(anchor="* * * * *", length="1h"),
        deployments=DeploymentsDef(pipelines="all"),
        sources=[SourceDef(name="src", type="rss")],
        sinks=[SinkDef(name="sk", type="file_md")],
        stages=[StageDef(name="st", type="noop")],
        pipelines=[
            PipelineDef(
                name="fetch",
                cron="0 * * * *",
                stages=["st"],
                sources=["src"],
                upstream_all=None,
                upstream_any=None,
                sinks=["sk"],
                post_run_cleanup=None,
            ),
        ],
        rotation_cron="0 3 * * *",
    )


# ---------------------------------------------------------------------------
# Unit tests — _sanitize_name
# ---------------------------------------------------------------------------
class TestSanitizeName:
    def test_preserves_alphanumeric_hyphen_underscore(self):
        assert _sanitize_name("abc-123_XYZ") == "abc-123_XYZ"

    def test_replaces_special_chars(self):
        assert _sanitize_name("a.b/c") == "a-b-c"

    def test_replaces_chinese_chars(self):
        result = _sanitize_name("任务名称")
        assert all(c in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for c in result if c not in "-")


# ---------------------------------------------------------------------------
# Unit tests — build_pipeline_deployment
# ---------------------------------------------------------------------------
class TestBuildPipelineDeployment:
    def test_creates_deployment_with_correct_name(self, task_cfg, sluice_cfg):
        pipeline = task_cfg.pipelines[0]
        dep = build_pipeline_deployment(task_cfg, pipeline, sluice_cfg, "/configs")
        assert dep.name == "my_task-fetch"

    def test_uses_task_timezone_over_config(self, task_cfg, sluice_cfg):
        pipeline = task_cfg.pipelines[0]
        dep = build_pipeline_deployment(task_cfg, pipeline, sluice_cfg, "/configs")
        assert dep.schedule.timezone == "UTC"

    def test_falls_back_to_default_timezone(self, task_cfg, sluice_cfg):
        cfg_no_tz = TaskConfig(
            name="x", db_path="d", seen_db="s", enabled=True,
            timezone=None,
            window=WindowDef(anchor="* * * * *", length="1h"),
            deployments=DeploymentsDef(pipelines="all"),
            sources=[], sinks=[], stages=[],
            pipelines=[PipelineDef(name="p", cron="* * * * *", stages=[], sources=None, upstream_all=None, upstream_any=None, sinks=None, post_run_cleanup=None)],
        )
        dep = build_pipeline_deployment(cfg_no_tz, cfg_no_tz.pipelines[0], sluice_cfg, "/c")
        assert dep.schedule.timezone == "Asia/Shanghai"

    def test_concurrency_limit_is_one(self, task_cfg, sluice_cfg):
        pipeline = task_cfg.pipelines[0]
        dep = build_pipeline_deployment(task_cfg, pipeline, sluice_cfg, "/c")
        assert dep.concurrency_limit == 1

    def test_parameters_include_task_pipeline_config_dir(self, task_cfg, sluice_cfg):
        pipeline = task_cfg.pipelines[0]
        dep = build_pipeline_deployment(task_cfg, pipeline, sluice_cfg, "/my/configs")
        assert dep.parameters == {
            "task": "my_task",
            "pipeline": "fetch",
            "config_dir": "/my/configs",
        }

    def test_work_pool_from_config(self, task_cfg, sluice_cfg):
        pipeline = task_cfg.pipelines[0]
        dep = build_pipeline_deployment(task_cfg, pipeline, sluice_cfg, "/c")
        assert dep.work_pool_name == "test-pool"


# ---------------------------------------------------------------------------
# Unit tests — build_rotation_deployment
# ---------------------------------------------------------------------------
class TestBuildRotationDeployment:
    def test_creates_rotation_deployment(self, task_cfg, sluice_cfg):
        dep = build_rotation_deployment(
            task_cfg, "0 3 * * *", sluice_cfg, "/configs", "/configs/sluice.toml"
        )
        assert dep.name == "my_task-rotate"

    def test_parameters_include_sluice_config_path(self, task_cfg, sluice_cfg):
        dep = build_rotation_deployment(
            task_cfg, "0 3 * * *", sluice_cfg, "/configs", "/configs/sluice.toml"
        )
        assert dep.parameters == {
            "task": "my_task",
            "config_dir": "/configs",
            "sluice_config": "/configs/sluice.toml",
        }

    def test_schedule_uses_effective_cron(self, task_cfg, sluice_cfg):
        dep = build_rotation_deployment(
            task_cfg, "0 4 * * *", sluice_cfg, "/configs", None
        )
        assert str(dep.schedule.cron) == "0 4 * * *"


# ---------------------------------------------------------------------------
# Integration test — push_all_deployments
# ---------------------------------------------------------------------------
class TestPushAllDeployments:
    @pytest.mark.asyncio
    async def test_returns_correct_counts(self, task_cfg, sluice_cfg):
        with patch("sluice.dag.deploy.prefect.deploy", new=AsyncMock()):
            n_pip, n_rot = await push_all_deployments(
                [task_cfg], sluice_cfg, "/configs", "/configs/sluice.toml"
            )
        assert n_pip == 1
        assert n_rot == 1

    @pytest.mark.asyncio
    async def test_skips_disabled_task(self, sluice_cfg):
        disabled = TaskConfig(
            name="disabled", db_path="d", seen_db="s", enabled=False,
            timezone=None,
            window=WindowDef(anchor="* * * * *", length="1h"),
            deployments=DeploymentsDef(pipelines="all"),
            sources=[], sinks=[], stages=[],
            pipelines=[PipelineDef(name="p", cron="* * * * *", stages=[], sources=None, upstream_all=None, upstream_any=None, sinks=None, post_run_cleanup=None)],
        )
        with patch("sluice.dag.deploy.prefect.deploy", new=AsyncMock()) as mock_deploy:
            n_pip, n_rot = await push_all_deployments(
                [disabled], sluice_cfg, "/c", None
            )
        assert n_pip == 0
        assert n_rot == 0
        mock_deploy.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_pipeline_without_cron(self, sluice_cfg):
        t = TaskConfig(
            name="no_cron", db_path="d", seen_db="s", enabled=True,
            timezone=None,
            window=WindowDef(anchor="* * * * *", length="1h"),
            deployments=DeploymentsDef(pipelines="all"),
            sources=[], sinks=[], stages=[],
            pipelines=[PipelineDef(name="p", cron="", stages=[], sources=None, upstream_all=None, upstream_any=None, sinks=None, post_run_cleanup=None)],
        )
        with patch("sluice.dag.deploy.prefect.deploy", new=AsyncMock()) as mock_deploy:
            n_pip, n_rot = await push_all_deployments(
                [t], sluice_cfg, "/c", None
            )
        assert n_pip == 0
        assert n_rot == 0

    @pytest.mark.asyncio
    async def test_skips_rotation_when_no_cron(self, task_cfg, sluice_cfg):
        no_rot = TaskConfig(
            name="x", db_path="d", seen_db="s", enabled=True,
            timezone=None,
            window=WindowDef(anchor="* * * * *", length="1h"),
            deployments=DeploymentsDef(pipelines="all"),
            sources=[], sinks=[], stages=[],
            pipelines=[],
            rotation_cron=None,
        )
        cfg_no_default = SluiceConfig(
            runtime=RuntimeConfig(),
        )
        # default_cron is None in RotationConfig
        with patch("sluice.dag.deploy.prefect.deploy", new=AsyncMock()) as mock_deploy:
            n_pip, n_rot = await push_all_deployments(
                [no_rot], cfg_no_default, "/c", None
            )
        assert n_rot == 0

    @pytest.mark.asyncio
    async def test_respects_deployments_pipelines_allowlist(self, sluice_cfg):
        t = TaskConfig(
            name="multi", db_path="d", seen_db="s", enabled=True,
            timezone=None,
            window=WindowDef(anchor="* * * * *", length="1h"),
            deployments=DeploymentsDef(pipelines=["p2"]),
            sources=[], sinks=[], stages=[],
            pipelines=[
                PipelineDef(name="p1", cron="0 * * * *", stages=[], sources=None, upstream_all=None, upstream_any=None, sinks=None, post_run_cleanup=None),
                PipelineDef(name="p2", cron="0 * * * *", stages=[], sources=None, upstream_all=None, upstream_any=None, sinks=None, post_run_cleanup=None),
            ],
        )
        with patch("sluice.dag.deploy.prefect.deploy", new=AsyncMock()):
            n_pip, n_rot = await push_all_deployments(
                [t], sluice_cfg, "/c", None
            )
        assert n_pip == 1  # only p2

    @pytest.mark.asyncio
    async def test_deploy_called_with_work_pool(self, task_cfg, sluice_cfg):
        with patch("sluice.dag.deploy.prefect.deploy", new=AsyncMock()) as mock_deploy:
            await push_all_deployments(
                [task_cfg], sluice_cfg, "/c", None
            )
        mock_deploy.assert_awaited_once()
        # Verify work_pool_name kwarg passed
        call_kwargs = mock_deploy.call_args.kwargs
        assert call_kwargs["work_pool_name"] == "test-pool"

    @pytest.mark.asyncio
    async def test_deploy_not_called_when_no_deployments(self, sluice_cfg):
        empty_task = TaskConfig(
            name="empty", db_path="d", seen_db="s", enabled=True,
            timezone=None,
            window=WindowDef(anchor="* * * * *", length="1h"),
            deployments=DeploymentsDef(pipelines="all"),
            sources=[], sinks=[], stages=[],
            pipelines=[],
        )
        cfg_no_cron = SluiceConfig(runtime=RuntimeConfig())
        with patch("sluice.dag.deploy.prefect.deploy", new=AsyncMock()) as mock_deploy:
            n_pip, n_rot = await push_all_deployments(
                [empty_task], cfg_no_cron, "/c", None
            )
        assert n_pip == 0
        assert n_rot == 0
        mock_deploy.assert_not_called()
```

**Step 2: Run to verify failure**

Run: `pytest tests/dag/test_deploy.py -v`
Expected: `ModuleNotFoundError: No module named 'sluice.dag.deploy'`

**Step 3: Write implementation**

Create `sluice/dag/deploy.py`:

```python
from __future__ import annotations

import re
from contextlib import nullcontext
from pathlib import Path

from prefect import deploy as prefect_deploy
from prefect.client.schemas.schedules import CronSchedule
from prefect.settings import (
    PREFECT_API_AUTH_STRING,
    PREFECT_API_URL,
    temporary_settings,
)

from sluice.dag.config import PipelineDef, TaskConfig
from sluice.dag.flows import rotate_task_flow, run_pipeline_flow
from sluice.dag.sluice_config import RuntimeConfig, SluiceConfig


def _effective_cron(task: TaskConfig, sluice_cfg: SluiceConfig) -> str | None:
    return task.rotation_cron or sluice_cfg.rotation.default_cron


def _effective_timezone(task: TaskConfig, sluice_cfg: SluiceConfig) -> str:
    return task.timezone or sluice_cfg.runtime.default_timezone or "UTC"


def _sanitize_name(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_-]", "-", name)


def build_pipeline_deployment(
    task: TaskConfig,
    pipeline: PipelineDef,
    sluice_cfg: SluiceConfig,
    config_dir: str,
):
    tz = _effective_timezone(task, sluice_cfg)
    return run_pipeline_flow.to_deployment(
        name=_sanitize_name(f"{task.name}-{pipeline.name}"),
        schedule=CronSchedule(cron=pipeline.cron, timezone=tz),
        concurrency_limit=1,
        parameters={
            "task": task.name,
            "pipeline": pipeline.name,
            "config_dir": config_dir,
        },
        work_pool_name=sluice_cfg.runtime.work_pool,
    )


def build_rotation_deployment(
    task: TaskConfig,
    effective_cron: str,
    sluice_cfg: SluiceConfig,
    config_dir: str,
    sluice_config_path: str | None,
):
    tz = _effective_timezone(task, sluice_cfg)
    parameters: dict[str, str] = {
        "task": task.name,
        "config_dir": config_dir,
    }
    if sluice_config_path is not None:
        parameters["sluice_config"] = sluice_config_path
    return rotate_task_flow.to_deployment(
        name=_sanitize_name(f"{task.name}-rotate"),
        schedule=CronSchedule(cron=effective_cron, timezone=tz),
        concurrency_limit=1,
        parameters=parameters,
        work_pool_name=sluice_cfg.runtime.work_pool,
    )


async def push_all_deployments(
    tasks: list[TaskConfig],
    sluice_cfg: SluiceConfig,
    config_dir: str,
    sluice_config_path: str | None,
) -> tuple[int, int]:
    deployments = []
    n_pip = 0
    n_rot = 0

    for task in tasks:
        if not task.enabled:
            continue

        for pipeline in task.pipelines:
            if not pipeline.cron:
                continue
            if (
                task.deployments.pipelines != "all"
                and pipeline.name not in task.deployments.pipelines
            ):
                continue
            dep = build_pipeline_deployment(task, pipeline, sluice_cfg, config_dir)
            deployments.append(dep)
            n_pip += 1

        cron = _effective_cron(task, sluice_cfg)
        if cron is not None:
            dep = build_rotation_deployment(
                task, cron, sluice_cfg, config_dir, sluice_config_path
            )
            deployments.append(dep)
            n_rot += 1

    if deployments:
        await prefect_deploy(
            *deployments, work_pool_name=sluice_cfg.runtime.work_pool
        )

    return (n_pip, n_rot)


def _prefect_api_settings(runtime: RuntimeConfig):
    """Temporary override PREFECT_API_URL and PREFECT_API_AUTH_STRING."""
    from sluice.core.errors import ConfigError
    from sluice.loader import resolve_env

    updates = {}
    if runtime.prefect_api_url:
        updates[PREFECT_API_URL] = runtime.prefect_api_url
    if runtime.prefect_api_auth_string is not None and runtime.prefect_api_auth_string != "":
        auth_string = resolve_env(runtime.prefect_api_auth_string)
        if not auth_string.strip():
            raise ConfigError(
                "runtime.prefect_api_auth_string must not be empty"
            )
        updates[PREFECT_API_AUTH_STRING] = auth_string
    if not updates:
        return nullcontext()
    return temporary_settings(updates)
```

**Step 4: Verify tests pass**

Run: `pytest tests/dag/test_deploy.py -v`
Expected: All PASS (~14 tests)

**Potential issue:** `PipelineDef.cron` is `str`, not `str | None` — so `not pipeline.cron` will skip empty strings but won't be None. The test for "pipeline without cron" uses `cron=""`. Empty string cron should indeed produce no deployment.

**Step 5: Commit**

```bash
git add sluice/dag/deploy.py tests/dag/test_deploy.py
git commit -m "feat(deploy): add v2 deployment builder with build_* pure functions and push_all_deployments"
```

---

### Task 6: Contract test — flow entrypoints are module-level

**Files:**
- Create: `tests/dag/test_flow_entrypoints.py`

**Step 1: Write the test**

Create `tests/dag/test_flow_entrypoints.py`:

```python
from __future__ import annotations

import ast
from importlib import resources
from pathlib import Path


def test_flow_module_has_no_locals_in_name():
    """Verify run_pipeline_flow and rotate_task_flow are top-level, not closures."""
    flows_path = Path(__file__).resolve().parent.parent / "sluice" / "dag" / "flows.py"
    source = flows_path.read_text()
    tree = ast.parse(source)

    flow_func_names = {"run_pipeline_flow", "rotate_task_flow"}
    found = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name in flow_func_names:
            # Check decorator: @flow(...)
            for decorator in node.decorator_list:
                if isinstance(decorator, ast.Call):
                    if isinstance(decorator.func, ast.Name) and decorator.func.id == "flow":
                        found.add(node.name)
                        break

    assert found == flow_func_names, (
        f"Expected top-level @flow functions {flow_func_names}, found {found}. "
        "All Prefect flows MUST be module-level to avoid 'build_flow.<locals>._flow' errors."
    )


def test_flow_names_are_ascii():
    """Deployment names are sanitized; flow Prefect names should be clean."""
    flows_path = Path(__file__).resolve().parent.parent / "sluice" / "dag" / "flows.py"
    source = flows_path.read_text()
    tree = ast.parse(source)

    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            for decorator in node.decorator_list:
                if isinstance(decorator, ast.Call):
                    if isinstance(decorator.func, ast.Name) and decorator.func.id == "flow":
                        for kw in decorator.keywords:
                            if kw.arg == "name" and isinstance(kw.value, ast.Constant):
                                name_value = kw.value.value
                                assert isinstance(name_value, str)
                                # name should be ASCII and use kebab-case convention
                                assert name_value.isascii(), (
                                    f"Flow name {name_value!r} must be ASCII"
                                )
```

**Step 2: Run to verify failure**

Run: `pytest tests/dag/test_flow_entrypoints.py -v`
Expected: PASS (flows.py already exists, but contract test validates properties we've already implemented correctly)

**Step 3: Commit**

```bash
git add tests/dag/test_flow_entrypoints.py
git commit -m "test: add contract test verifying flow entrypoints are module-level"
```

---

### Task 7: Rewrite `sluice deploy` CLI command

**Files:**
- Modify: `sluice/cli.py` (replace deploy command, remove `_prefect_api_settings`, remove unused module-level imports)
- Delete: `tests/test_cli_deploy.py`

**Strategy:** Deploy-related functions are lazy-imported inside the command body so `cli.py` doesn't import Prefect at module level. Tests patch `sluice.dag.deploy.push_all_deployments` before the lazy import runs; Python returns the patched version from `sys.modules`.

**Step 1: Write the CLI deploy test**

Add to `tests/dag/test_cli.py`:

```python
from contextlib import nullcontext


class TestCliDeploy:
    def test_deploy_registers_deployments(self, tmp_path: Path):
        config_dir = _copy_task(tmp_path)
        (config_dir / "sluice.toml").write_text("[runtime]\nwork_pool = \"test-pool\"\n")

        with patch("sluice.dag.deploy.push_all_deployments",
                   new=AsyncMock(return_value=(3, 1))), \
             patch("sluice.dag.deploy._prefect_api_settings",
                   return_value=nullcontext()):
            result = runner.invoke(app, ["deploy", str(config_dir)])

        assert result.exit_code == 0
        assert "3 pipeline deployment" in result.stdout
        assert "1 rotation deployment" in result.stdout

    def test_deploy_no_tasks_prints_warning(self, tmp_path: Path):
        config_dir = tmp_path / "configs"
        (config_dir / "tasks").mkdir(parents=True)
        (config_dir / "sluice.toml").write_text("[runtime]\nwork_pool = \"p\"\n")

        with patch("sluice.dag.deploy.push_all_deployments",
                   new=AsyncMock(return_value=(0, 0))), \
             patch("sluice.dag.deploy._prefect_api_settings",
                   return_value=nullcontext()):
            result = runner.invoke(app, ["deploy", str(config_dir)])

        assert result.exit_code == 0
        assert "Warning" in result.stdout or "0 pipeline deployment" in result.stdout

    def test_deploy_resolves_paths(self, tmp_path: Path):
        config_dir = _copy_task(tmp_path)
        (config_dir / "sluice.toml").write_text("[runtime]\nwork_pool = \"test-pool\"\n")

        called_with_config_dir = []

        async def capture(*args, **kwargs):
            called_with_config_dir.append(kwargs.get("config_dir"))
            return (1, 0)

        push_mock = AsyncMock(side_effect=capture)

        with patch("sluice.dag.deploy.push_all_deployments", new=push_mock), \
             patch("sluice.dag.deploy._prefect_api_settings", return_value=nullcontext()):
            result = runner.invoke(app, ["deploy", str(config_dir)])

        assert result.exit_code == 0
        # config_dir passed to push_all_deployments should be absolute
        assert called_with_config_dir[0].startswith("/")
```

Note: `_prefect_api_settings` is patched to return `nullcontext()` so the `with` block runs without touching Prefect settings. `push_all_deployments` is patched to an `AsyncMock` returning synthetic counts. With lazy imports, the deploy command gets the mocked versions from `sys.modules`.

**Step 2: Run test to verify it fails**

Run: `pytest tests/dag/test_cli.py::TestCliDeploy::test_deploy_registers_deployments -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'sluice.dag.deploy'` (if deploy.py not yet imported) or existing deploy test mismatch

**Step 3: Implement — rewrite deploy command**

**Remove** lines 10-11 from `sluice/cli.py`:
```python
from prefect import serve as prefect_serve       # DELETE
from prefect.client.schemas.schedules import CronSchedule  # DELETE
```

**Replace** deploy command (lines 246-283) with:

```python
@app.command()
def deploy(
    config_dir: Path = typer.Argument(
        Path("./configs"),
        help="Path to config directory containing tasks/ subdirectory.",
    ),
    sluice_config_path: Path | None = typer.Option(
        None,
        "--sluice-config-path",
        help="Path to sluice.toml. Defaults to <config_dir>/sluice.toml.",
    ),
):
    """Register all v2 task pipelines + rotation as Prefect deployments."""
    from sluice.dag.deploy import _prefect_api_settings, push_all_deployments
    from sluice.dag.loader import load_all_tasks
    from sluice.dag.sluice_config import load_sluice_config

    config_dir = config_dir.resolve()
    if sluice_config_path is not None:
        sluice_config_path = sluice_config_path.resolve()

    tasks = load_all_tasks(config_dir)
    sluice_cfg = load_sluice_config(sluice_config_path)

    config_dir_str = str(config_dir)
    sluice_config_str = str(sluice_config_path) if sluice_config_path else None

    with _prefect_api_settings(sluice_cfg.runtime):
        n_pip, n_rot = asyncio.run(
            push_all_deployments(
                tasks, sluice_cfg, config_dir_str, sluice_config_str
            )
        )

    if n_pip == 0 and n_rot == 0:
        typer.echo("Warning: No v2 task directories found.")

    typer.echo(
        f"Registered {n_pip} pipeline deployment(s) + {n_rot} rotation deployment(s)."
    )
```

**Delete** `_prefect_api_settings` function (lines 286-309) — it's now in `deploy.py`.

**Step 4: Delete old deploy test**

```bash
rm tests/test_cli_deploy.py
```

**Step 5: Verify CLI deploy tests pass**

Run: `pytest tests/dag/test_cli.py::TestCliDeploy -v`
Expected: All PASS (3 tests)

**Step 6: Verify existing CLI tests still pass**

Run: `pytest tests/dag/test_cli.py -v -k "not TestCliDeploy"`
Expected: All existing tests PASS

**Step 7: Commit**

```bash
git add sluice/cli.py tests/dag/test_cli.py
git rm tests/test_cli_deploy.py
git commit -m "feat(cli): rewrite sluice deploy for v2 push-deploy, remove v1 deploy"
```

---

### Task 8: Integration verification

**Files:** None (verify only)

**Step 1: Run full DAG test suite**

Run: `pytest tests/dag/ -v`
Expected: All tests PASS (~30+ tests across all files)

**Step 2: Run ruff lint**

Run: `ruff check sluice/dag/flows.py sluice/dag/deploy.py sluice/cli.py`
Expected: No errors

**Step 3: Run ruff format**

Run: `ruff format --check sluice/dag/flows.py sluice/dag/deploy.py sluice/cli.py`
Expected: Already formatted (or fix inline)

**Step 4: Commit final**

```bash
git add -A
git commit -m "chore: lint and format new Plan 4a files"
```

---

## Summary

| Task | Files | New/Modify |
|------|-------|-----------|
| 1 | `sluice/dag/sluice_config.py` + tests | Modify |
| 2 | `sluice/dag/config.py`, `loader.py` + tests | Modify |
| 3 | `sluice/dag/loader.py` + tests | Modify |
| 4 | `sluice/dag/flows.py` + tests | Create |
| 5 | `sluice/dag/deploy.py` + tests | Create |
| 6 | `tests/dag/test_flow_entrypoints.py` | Create |
| 7 | `sluice/cli.py`, delete `tests/test_cli_deploy.py` | Modify/Delete |
| 8 | — | Verify |

**Total:** 5 new files, 4 modified files, 1 deleted file.

**Expected commits:** 8 (one per task).
