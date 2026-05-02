from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from sluice.cli import app

runner = CliRunner()


def test_deploy_creates_all_pipelines(tmp_path):
    cfg = tmp_path / "configs"
    (cfg / "pipelines").mkdir(parents=True)
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
    (cfg / "pipelines" / "p1.toml").write_text(
        'id="p1"\nwindow="24h"\n[[sources]]\ntype="rss"\nurl="https://x"\n[[stages]]\nname="r"\ntype="render"\ntemplate="/dev/null"\noutput_field="context.x"\n[[sinks]]\nid="s"\ntype="file_md"\ninput="context.x"\npath="/dev/null"\n'
    )
    (cfg / "pipelines" / "p2.toml").write_text(
        'id="p2"\nwindow="24h"\n[[sources]]\ntype="rss"\nurl="https://y"\n[[stages]]\nname="r"\ntype="render"\ntemplate="/dev/null"\noutput_field="context.x"\n[[sinks]]\nid="s"\ntype="file_md"\ninput="context.x"\npath="/dev/null"\n'
    )

    with (
        patch("prefect.client.schemas.schedules.CronSchedule"),
        patch("sluice.flow.build_flow") as mock_build_flow,
        patch("sluice.cli.prefect_serve"),
    ):

        def make_flow(pid):
            flow_obj = MagicMock()
            flow_obj.to_deployment = MagicMock(return_value=MagicMock(name=f"sluice-{pid}"))
            return flow_obj

        mock_build_flow.side_effect = make_flow
        result = runner.invoke(app, ["deploy", "--config-dir", str(cfg)])

    assert result.exit_code == 0
    assert mock_build_flow.call_count == 2


@pytest.mark.prefect_api
def test_deploy_applies_prefect_api_settings(tmp_path, monkeypatch):
    cfg = tmp_path / "configs"
    (cfg / "pipelines").mkdir(parents=True)
    (cfg / "sluice.toml").write_text("""
[state]
db_path="x.db"
[runtime]
timezone="UTC"
default_cron="0 8 * * *"
prefect_api_url="http://x"
prefect_api_auth_string="env:PREFECT_AUTH"
""")
    (cfg / "providers.toml").write_text("""
providers = []
""")
    (cfg / "pipelines" / "p1.toml").write_text(
        'id="p1"\nwindow="24h"\n[[sources]]\ntype="rss"\nurl="https://x"\n[[stages]]\nname="r"\ntype="render"\ntemplate="/dev/null"\noutput_field="context.x"\n[[sinks]]\nid="s"\ntype="file_md"\ninput="context.x"\npath="/dev/null"\n'
    )
    observed_settings = {}

    def fake_serve(*deployments):
        from prefect.settings import get_current_settings

        settings = get_current_settings()
        observed_settings["url"] = settings.api.url
        observed_settings["auth_string"] = settings.api.auth_string.get_secret_value()

    with (
        patch("prefect.client.schemas.schedules.CronSchedule"),
        patch("sluice.flow.build_flow") as mock_build_flow,
        patch("sluice.cli.prefect_serve", side_effect=fake_serve),
    ):
        flow_obj = MagicMock()
        flow_obj.to_deployment = MagicMock(return_value=MagicMock(name="sluice-p1"))
        mock_build_flow.return_value = flow_obj
        monkeypatch.setenv("PREFECT_AUTH", "admin:pass")
        result = runner.invoke(app, ["deploy", "--config-dir", str(cfg)])

    assert result.exit_code == 0
    assert observed_settings == {
        "url": "http://x",
        "auth_string": "admin:pass",
    }


def test_prefect_api_settings_rejects_empty_resolved_auth(monkeypatch):
    from sluice.cli import _prefect_api_settings
    from sluice.config import RuntimeConfig
    from sluice.core.errors import ConfigError

    monkeypatch.setenv("PREFECT_AUTH", "")
    runtime = RuntimeConfig(
        prefect_api_url="http://x",
        prefect_api_auth_string="env:PREFECT_AUTH",
    )

    with pytest.raises(ConfigError, match="prefect_api_auth_string"):
        with _prefect_api_settings(runtime):
            pass
