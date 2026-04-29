import tomllib
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent


def test_pyproject_has_v1_1_extras():
    data = tomllib.loads((_ROOT / "pyproject.toml").read_text())
    extras = data["project"]["optional-dependencies"]
    assert "telegram" in extras
    assert "feishu" in extras
    assert "email" in extras
    assert "channels" in extras
    assert "metrics" in extras
    assert "enrich-hn" in extras
    assert "all" in extras
    assert any(d.startswith("markdown-it-py") for d in extras["channels"])
    assert any(d.startswith("aiosmtplib") for d in extras["email"])
    assert any(
        d.startswith("prometheus_client") or d.startswith("prometheus-client")
        for d in extras["metrics"]
    )
    assert any(d.startswith("beautifulsoup4") for d in extras["enrich-hn"])


def test_pyproject_aiofiles_and_croniter_in_core():
    data = tomllib.loads((_ROOT / "pyproject.toml").read_text())
    deps = data["project"]["dependencies"]
    assert any(d.startswith("aiofiles") for d in deps)
    assert any(d.startswith("croniter") for d in deps)
