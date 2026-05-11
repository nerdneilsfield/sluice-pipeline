from __future__ import annotations

import shutil
import sys
import types
from contextlib import nullcontext
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from typer.testing import CliRunner

FIXTURE = Path(__file__).parent / "fixtures" / "example_task"


def _cli_app():
    prefect_mod = types.ModuleType("prefect")

    async def deploy(*args, **kwargs):
        return None

    prefect_mod.deploy = deploy
    prefect_mod.flow = lambda **kwargs: lambda fn: fn
    prefect_mod.serve = lambda *args, **kwargs: None
    prefect_client_mod = types.ModuleType("prefect.client")
    prefect_client_schemas_mod = types.ModuleType("prefect.client.schemas")
    prefect_schedules_mod = types.ModuleType("prefect.client.schemas.schedules")

    class CronSchedule:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

    prefect_schedules_mod.CronSchedule = CronSchedule
    prefect_settings_mod = types.ModuleType("prefect.settings")
    prefect_settings_mod.PREFECT_API_URL = "PREFECT_API_URL"
    prefect_settings_mod.PREFECT_API_AUTH_STRING = "PREFECT_API_AUTH_STRING"
    prefect_settings_mod.temporary_settings = lambda updates: nullcontext()
    prefect_types_mod = types.ModuleType("prefect.types")
    prefect_entrypoint_mod = types.ModuleType("prefect.types.entrypoint")

    class EntrypointType:
        MODULE_PATH = "module_path"

    prefect_entrypoint_mod.EntrypointType = EntrypointType
    sys.modules.setdefault("prefect", prefect_mod)
    sys.modules.setdefault("prefect.client", prefect_client_mod)
    sys.modules.setdefault("prefect.client.schemas", prefect_client_schemas_mod)
    sys.modules.setdefault("prefect.client.schemas.schedules", prefect_schedules_mod)
    sys.modules.setdefault("prefect.settings", prefect_settings_mod)
    sys.modules.setdefault("prefect.types", prefect_types_mod)
    sys.modules.setdefault("prefect.types.entrypoint", prefect_entrypoint_mod)

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


class TestCliDeploy:
    def test_deploy_registers_deployments(self, tmp_path: Path):
        config_dir = _copy_task(tmp_path)
        (config_dir / "sluice.toml").write_text('[runtime]\nwork_pool = "test-pool"\n')
        app = _cli_app()

        with (
            patch(
                "sluice.dag.deploy.push_all_deployments",
                new=AsyncMock(return_value=(3, 1)),
            ),
            patch(
                "sluice.dag.deploy._prefect_api_settings",
                return_value=nullcontext(),
            ),
        ):
            result = CliRunner().invoke(app, ["deploy", "--config-dir", str(config_dir)])

        assert result.exit_code == 0, result.output
        assert "3 pipeline deployment" in result.output
        assert "1 rotation deployment" in result.output

    def test_deploy_no_tasks_prints_warning(self, tmp_path: Path):
        config_dir = tmp_path / "configs"
        (config_dir / "tasks").mkdir(parents=True)
        (config_dir / "sluice.toml").write_text('[runtime]\nwork_pool = "test-pool"\n')
        app = _cli_app()

        with (
            patch(
                "sluice.dag.deploy.push_all_deployments",
                new=AsyncMock(return_value=(0, 0)),
            ),
            patch(
                "sluice.dag.deploy._prefect_api_settings",
                return_value=nullcontext(),
            ),
        ):
            result = CliRunner().invoke(app, ["deploy", "--config-dir", str(config_dir)])

        assert result.exit_code == 0, result.output
        assert "Warning" in result.output
        assert "0 pipeline deployment" in result.output

    def test_deploy_resolves_paths_and_default_sluice_config(self, tmp_path: Path):
        config_dir = _copy_task(tmp_path)
        (config_dir / "sluice.toml").write_text('[runtime]\nwork_pool = "test-pool"\n')
        app = _cli_app()
        seen: dict[str, object] = {}

        async def capture(tasks, sluice_cfg, config_dir, sluice_config_path):
            seen["tasks"] = tasks
            seen["config_dir"] = config_dir
            seen["sluice_config_path"] = sluice_config_path
            seen["work_pool"] = sluice_cfg.runtime.work_pool
            return (1, 0)

        with (
            patch("sluice.dag.deploy.push_all_deployments", new=AsyncMock(side_effect=capture)),
            patch(
                "sluice.dag.deploy._prefect_api_settings",
                return_value=nullcontext(),
            ),
        ):
            result = CliRunner().invoke(app, ["deploy", "--config-dir", str(config_dir)])

        assert result.exit_code == 0, result.output
        assert seen["config_dir"] == str(config_dir.resolve())
        assert seen["sluice_config_path"] == str((config_dir / "sluice.toml").resolve())
        assert seen["work_pool"] == "test-pool"
        assert [task.name for task in seen["tasks"]] == ["example"]

    def test_deploy_uses_explicit_sluice_config(self, tmp_path: Path):
        config_dir = _copy_task(tmp_path)
        explicit_config = config_dir / "explicit-sluice.toml"
        explicit_config.write_text('[runtime]\nwork_pool = "explicit-pool"\n')
        app = _cli_app()
        seen: dict[str, object] = {}

        async def capture(tasks, sluice_cfg, config_dir, sluice_config_path):
            seen["config_dir"] = config_dir
            seen["sluice_config_path"] = sluice_config_path
            seen["work_pool"] = sluice_cfg.runtime.work_pool
            return (1, 1)

        with (
            patch("sluice.dag.deploy.push_all_deployments", new=AsyncMock(side_effect=capture)),
            patch(
                "sluice.dag.deploy._prefect_api_settings",
                return_value=nullcontext(),
            ),
        ):
            result = CliRunner().invoke(
                app,
                [
                    "deploy",
                    "--config-dir",
                    str(config_dir),
                    "--sluice-config",
                    str(explicit_config),
                ],
            )

        assert result.exit_code == 0, result.output
        assert seen["config_dir"] == str(config_dir.resolve())
        assert seen["sluice_config_path"] == str(explicit_config.resolve())
        assert seen["work_pool"] == "explicit-pool"

    def test_deploy_rejects_sluice_config_outside_config_dir(self, tmp_path: Path):
        config_dir = _copy_task(tmp_path)
        outside_config = tmp_path / "outside-sluice.toml"
        outside_config.write_text('[runtime]\nwork_pool = "outside-pool"\n')
        app = _cli_app()

        result = CliRunner().invoke(
            app,
            [
                "deploy",
                "--config-dir",
                str(config_dir),
                "--sluice-config",
                str(outside_config),
            ],
        )

        assert result.exit_code == 2
        assert "--sluice-config must be inside --config-dir" in result.output


def test_run_command_invokes_real_runner(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    config_dir = _copy_task(tmp_path)
    import sluice.dag.run_pipeline as dag_run_pipeline

    async def fake_ensure(task):
        from sluice.dag.rotation.lock import RotationLock

        assert RotationLock(task.db_path).is_held()
        Path(task.db_path).touch()

    async def fake_run_pipeline(task, pipeline, *, writer, now=None, trigger="manual"):
        from sluice.dag.rotation.lock import RotationLock
        from sluice.dag.run_pipeline import RunResult

        assert RotationLock(task.db_path).is_held()
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


@pytest.mark.parametrize(
    ("status", "exit_code"),
    [
        ("failed", 1),
        ("partial", 1),
        ("skipped_gating", 0),
        ("skipped_rotation", 0),
    ],
)
def test_run_command_status_exit_code(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    status: str,
    exit_code: int,
):
    config_dir = _copy_task(tmp_path)
    import sluice.dag.run_pipeline as dag_run_pipeline

    async def fake_ensure(task):
        return None

    async def fake_run_pipeline(task, pipeline, *, writer, now=None, trigger="manual"):
        from sluice.dag.run_pipeline import RunResult

        return RunResult(
            pipeline_name=pipeline,
            run_key=f"{task.name}__{pipeline}__fake",
            status=status,
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
    assert result.exit_code == exit_code
    assert f'"status": "{status}"' in result.output


def test_run_command_exceptions_exit_cleanly(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    config_dir = _copy_task(tmp_path)
    import sluice.dag.run_pipeline as dag_run_pipeline

    async def fake_ensure(task):
        return None

    async def fake_run_pipeline(task, pipeline, *, writer, now=None, trigger="manual"):
        raise RuntimeError("boom")

    monkeypatch.setattr(dag_run_pipeline, "ensure_dynamic_tables", fake_ensure)
    monkeypatch.setattr(dag_run_pipeline, "run_pipeline", fake_run_pipeline)
    runner = CliRunner()
    result = runner.invoke(
        _cli_app(),
        ["run", "-t", "example", "-p", "fetch_blog", "--config-dir", str(config_dir)],
    )
    assert result.exit_code == 1
    assert "pipeline run failed: boom" in result.output


def test_run_command_short_circuits_when_rotation_lock_held(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    config_dir = _copy_task(tmp_path)
    import sluice.cli as cli
    from sluice.dag.rotation.lock import RotationLock

    async def fake_ensure(task):
        raise AssertionError("ensure_dynamic_tables should not run")

    class FakeWriter:
        def __init__(self, *args, **kwargs):
            raise AssertionError("WriterThread should not be instantiated")

    lock = RotationLock(tmp_path / "active.db")
    lock.acquire()
    monkeypatch.setattr(cli, "ensure_dynamic_tables", fake_ensure)
    monkeypatch.setattr(cli, "WriterThread", FakeWriter)

    try:
        result = CliRunner().invoke(
            _cli_app(),
            [
                "run",
                "-t",
                "example",
                "-p",
                "fetch_blog",
                "--config-dir",
                str(config_dir),
            ],
        )
    finally:
        lock.release()

    assert result.exit_code == 0, result.output
    assert '"status": "skipped_rotation"' in result.output
