import sys
import types
from pathlib import Path

from typer.testing import CliRunner

from sluice.cli import app
from tests.dag.test_helpers import make_configs_dir, seed_item_sync, seed_pipeline_state_sync


def test_cli_migrate(tmp_path: Path):
    configs = make_configs_dir(tmp_path, tasks={"t1": {"pipelines": ["p1"], "stages": ["s1"]}})
    result = CliRunner().invoke(app, ["migrate", "t1", "-c", str(configs)])
    assert result.exit_code == 0, result.output
    assert "Migrated task 't1'." in result.output


def test_cli_retry_single_item(tmp_path: Path):
    configs = make_configs_dir(tmp_path, tasks={"t1": {"pipelines": ["p1"], "stages": ["s1"]}})
    runner = CliRunner()
    result = runner.invoke(app, ["migrate", "t1", "-c", str(configs)])
    assert result.exit_code == 0, result.output
    seed_pipeline_state_sync(configs / "../data/t1.db", pipeline="p1", item_id="X", status="failed")

    result = runner.invoke(
        app, ["retry", "--task", "t1", "--pipeline", "p1", "--item", "X", "-c", str(configs)]
    )
    assert result.exit_code == 0, result.output
    assert "Cleared 1" in result.output


def test_cli_retry_remote_triggers_prefect_deployment(tmp_path: Path, monkeypatch):
    configs = make_configs_dir(tmp_path, tasks={"t1": {"pipelines": ["p1"], "stages": ["s1"]}})
    runner = CliRunner()
    result = runner.invoke(app, ["migrate", "t1", "-c", str(configs)])
    assert result.exit_code == 0, result.output
    seed_pipeline_state_sync(configs / "../data/t1.db", pipeline="p1", item_id="X", status="failed")
    calls = []

    async def fake_run_deployment(*, name: str):
        calls.append(name)

    prefect_module = types.ModuleType("prefect")
    deployments_module = types.ModuleType("prefect.deployments")
    deployments_module.run_deployment = fake_run_deployment
    prefect_module.deployments = deployments_module
    monkeypatch.setitem(sys.modules, "prefect", prefect_module)
    monkeypatch.setitem(sys.modules, "prefect.deployments", deployments_module)

    result = runner.invoke(
        app, ["retry", "--task", "t1", "--pipeline", "p1", "--remote", "-c", str(configs)]
    )

    assert result.exit_code == 0, result.output
    assert calls == ["sluice-run-pipeline/t1-p1"]
    assert "Triggered Prefect run." in result.output


def test_cli_items_list(tmp_path: Path):
    configs = make_configs_dir(tmp_path, tasks={"t1": {"pipelines": ["p1"], "stages": ["s1"]}})
    runner = CliRunner()
    result = runner.invoke(app, ["migrate", "t1", "-c", str(configs)])
    assert result.exit_code == 0, result.output
    seed_item_sync(configs / "../data/t1.db", item_id="X", source_name="src1", title="t")

    result = runner.invoke(app, ["items", "list", "t1", "-c", str(configs)])
    assert result.exit_code == 0, result.output
    assert '"item_id": "X"' in result.output


def test_cli_items_show_not_found(tmp_path: Path):
    configs = make_configs_dir(tmp_path, tasks={"t1": {"pipelines": ["p1"], "stages": ["s1"]}})
    runner = CliRunner()
    result = runner.invoke(app, ["migrate", "t1", "-c", str(configs)])
    assert result.exit_code == 0, result.output
    result = runner.invoke(app, ["items", "show", "t1", "missing", "-c", str(configs)])
    assert result.exit_code == 1
    assert "not found" in result.output


def test_cli_items_kill(tmp_path: Path):
    configs = make_configs_dir(tmp_path, tasks={"t1": {"pipelines": ["p1"], "stages": ["s1"]}})
    runner = CliRunner()
    result = runner.invoke(app, ["migrate", "t1", "-c", str(configs)])
    assert result.exit_code == 0, result.output
    seed_item_sync(configs / "../data/t1.db", item_id="X", source_name="src1", title="t")

    result = runner.invoke(app, ["items", "kill", "t1", "X", "-c", str(configs)])
    assert result.exit_code == 0, result.output
    assert "Killed item 'X'." in result.output


def test_cli_scaffold(tmp_path: Path):
    old = tmp_path / "old.toml"
    old.write_text(
        'id = "demo"\n'
        'cron = "0 8 * * *"\n'
        "enabled = true\n"
        'window = "24h"\n\n'
        "[[sources]]\n"
        'type = "rss"\n'
        'url = "https://example.com/rss"\n\n'
        "[[stages]]\n"
        'name = "fetch"\n'
        'type = "fetch_rss"\n\n'
        "[[sinks]]\n"
        'id = "tg"\n'
        'type = "telegram"\n'
    )
    out = tmp_path / "tasks" / "demo"
    result = CliRunner().invoke(app, ["scaffold-task", "--from-old", str(old), "--out", str(out)])
    assert result.exit_code == 0, result.output
    assert (out / "task.toml").exists()
