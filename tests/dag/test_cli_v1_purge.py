from typer.testing import CliRunner

from sluice.cli import app


def test_v1_commands_removed():
    runner = CliRunner()
    for command in ("run-legacy", "failures", "gc", "stats", "metrics-server", "deliveries"):
        result = runner.invoke(app, [command, "--help"])
        assert result.exit_code != 0, f"v1 command still present: {command}"
