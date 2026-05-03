from __future__ import annotations

import shutil
import sys
import types
from pathlib import Path

import pytest
from typer.testing import CliRunner

FIXTURE = Path(__file__).parent / "fixtures" / "example_task"


def _cli_app():
    prefect_mod = types.ModuleType("prefect")
    prefect_mod.serve = lambda *args, **kwargs: None
    prefect_client_mod = types.ModuleType("prefect.client")
    prefect_client_schemas_mod = types.ModuleType("prefect.client.schemas")
    prefect_schedules_mod = types.ModuleType("prefect.client.schemas.schedules")

    class CronSchedule:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

    prefect_schedules_mod.CronSchedule = CronSchedule
    sys.modules.setdefault("prefect", prefect_mod)
    sys.modules.setdefault("prefect.client", prefect_client_mod)
    sys.modules.setdefault("prefect.client.schemas", prefect_client_schemas_mod)
    sys.modules.setdefault("prefect.client.schemas.schedules", prefect_schedules_mod)

    sluice_runner_mod = types.ModuleType("sluice.runner")

    async def run_pipeline(*args, **kwargs):
        raise AssertionError("legacy runner should not be used by DAG CLI tests")

    sluice_runner_mod.run_pipeline = run_pipeline
    sys.modules.setdefault("sluice.runner", sluice_runner_mod)

    from sluice.cli import app

    return app


def _copy_task(tmp_path: Path) -> Path:
    config_dir = tmp_path / "configs"
    (config_dir / "tasks").mkdir(parents=True)
    task_dir = config_dir / "tasks" / "example"
    shutil.copytree(FIXTURE, task_dir)
    text = (task_dir / "task.toml").read_text()
    text = text.replace(
        'db_path  = "data/example.db"',
        f'db_path  = "{tmp_path}/active.db"',
    )
    text = text.replace(
        'seen_db  = "data/example_seen.db"',
        f'seen_db  = "{tmp_path}/seen.db"',
    )
    (task_dir / "task.toml").write_text(text)
    return config_dir


def test_run_command_invokes_real_runner(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    config_dir = _copy_task(tmp_path)
    import sluice.dag.run_pipeline as dag_run_pipeline

    async def fake_ensure(task):
        Path(task.db_path).touch()

    async def fake_run_pipeline(task, pipeline, *, writer, now=None, trigger="manual"):
        from sluice.dag.run_pipeline import RunResult

        return RunResult(
            pipeline_name=pipeline,
            run_key=f"{task.name}__{pipeline}__fake",
            status="success",
            items_in=0,
            items_out=0,
        )

    monkeypatch.setattr(dag_run_pipeline, "ensure_dynamic_tables", fake_ensure)
    monkeypatch.setattr(dag_run_pipeline, "run_pipeline", fake_run_pipeline)

    runner = CliRunner()
    result = runner.invoke(
        _cli_app(),
        ["run", "-t", "example", "-p", "fetch_blog", "--config-dir", str(config_dir)],
    )

    assert result.exit_code == 0, result.output
    assert '"pipeline_name": "fetch_blog"' in result.output
    assert '"run_key": "example__fetch_blog__fake"' in result.output
    assert '"status": "success"' in result.output
    assert '"items_in": 0' in result.output
    assert '"items_out": 0' in result.output
    assert (tmp_path / "active.db").exists()


def test_run_command_unknown_pipeline(tmp_path: Path):
    config_dir = _copy_task(tmp_path)

    runner = CliRunner()
    result = runner.invoke(
        _cli_app(),
        [
            "run",
            "-t",
            "example",
            "-p",
            "no_such_pipeline",
            "--config-dir",
            str(config_dir),
        ],
    )

    assert result.exit_code == 2


def test_run_command_unknown_task(tmp_path: Path):
    config_dir = tmp_path / "configs"
    (config_dir / "tasks").mkdir(parents=True)

    runner = CliRunner()
    result = runner.invoke(
        _cli_app(),
        ["run", "-t", "nope", "-p", "p", "--config-dir", str(config_dir)],
    )

    assert result.exit_code == 2


def test_run_command_non_success_exits_one(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    config_dir = _copy_task(tmp_path)
    import sluice.dag.run_pipeline as dag_run_pipeline

    async def fake_ensure(task):
        return None

    async def fake_run_pipeline(task, pipeline, *, writer, now=None, trigger="manual"):
        from sluice.dag.run_pipeline import RunResult

        return RunResult(
            pipeline_name=pipeline,
            run_key=f"{task.name}__{pipeline}__fake",
            status="failed",
            items_in=1,
            items_out=0,
            error_msg="boom",
        )

    monkeypatch.setattr(dag_run_pipeline, "ensure_dynamic_tables", fake_ensure)
    monkeypatch.setattr(dag_run_pipeline, "run_pipeline", fake_run_pipeline)
    runner = CliRunner()
    result = runner.invoke(
        _cli_app(),
        ["run", "-t", "example", "-p", "fetch_blog", "--config-dir", str(config_dir)],
    )
    assert result.exit_code == 1
    assert '"status": "failed"' in result.output
