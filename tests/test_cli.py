import pytest
from typer.testing import CliRunner
from unittest.mock import AsyncMock, patch
from sluice.cli import app

runner = CliRunner()

def test_validate_ok(tmp_path, monkeypatch):
    cfg = tmp_path / "configs"; (cfg / "pipelines").mkdir(parents=True)
    (cfg / "sluice.toml").write_text("""
[state]
db_path="x.db"
[runtime]
timezone="UTC"
default_cron="0 8 * * *"
prefect_api_url="http://x"
[fetcher]
chain=["trafilatura"]
min_chars=10
on_all_failed="skip"
[fetchers.trafilatura]
type="trafilatura"
timeout=10
[fetcher.cache]
enabled=false
ttl="1d"
""")
    (cfg / "providers.toml").write_text("""
[[providers]]
name="x"
type="openai_compatible"
[[providers.base]]
url="https://x"
weight=1
key=[{value="env:K", weight=1}]
[[providers.models]]
model_name="m"
""")
    res = runner.invoke(app, ["validate", "--config-dir", str(cfg)])
    assert res.exit_code == 0
    assert "OK" in res.output

def test_run_invokes_runner(tmp_path):
    fake = AsyncMock(return_value=type("R", (), {"status": "success",
                                                  "run_key": "p/r",
                                                  "items_out": 3,
                                                  "error": None})())
    with patch("sluice.cli.run_pipeline", fake), \
         patch("sluice.cli.load_all") as load:
        load.return_value = type("B", (), {"pipelines": {"p": object()}})()
        res = runner.invoke(app, ["run", "p", "--config-dir", str(tmp_path)])
    assert res.exit_code == 0
    assert "success" in res.output
    assert fake.call_args.kwargs["dry_run"] is False

def test_run_passes_dry_run_flag(tmp_path):
    fake = AsyncMock(return_value=type("R", (), {"status": "success",
                                                  "run_key": "p/r",
                                                  "items_out": 0,
                                                  "error": None})())
    with patch("sluice.cli.run_pipeline", fake), \
         patch("sluice.cli.load_all") as load:
        load.return_value = type("B", (), {"pipelines": {"p": object()}})()
        res = runner.invoke(app, ["run", "p", "--config-dir", str(tmp_path),
                                   "--dry-run"])
    assert res.exit_code == 0
    assert fake.call_args.kwargs["dry_run"] is True
