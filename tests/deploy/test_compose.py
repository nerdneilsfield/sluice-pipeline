from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
DEPLOY_DIR = ROOT / "deploy"
SCRIPTS_DOCKER_DIR = ROOT / "scripts" / "docker"

REQUIRED_DEPLOY_ENV = {
    "POSTGRES_USER": "prefect",
    "POSTGRES_PASSWORD": "0123456789abcdef",
    "POSTGRES_DB": "prefect",
    "PREFECT_API_AUTH_STRING": "admin:0123456789abcdef",
}


def _docker_compose_cmd() -> list[str]:
    docker = shutil.which("docker")
    if docker is None:
        pytest.skip("docker CLI is not installed")
    result = subprocess.run(
        [docker, "compose", "version"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        pytest.skip("docker compose is not available")
    return [docker, "compose"]


def _compose_env(**extra: str) -> dict[str, str]:
    env = {"PATH": os.environ.get("PATH", "")}
    env.update(extra)
    return env


@pytest.mark.parametrize(
    "missing_var",
    ["POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB"],
)
def test_compose_config_fails_when_required_database_env_is_missing(missing_var: str):
    cmd = _docker_compose_cmd()
    env_values = REQUIRED_DEPLOY_ENV.copy()
    env_values.pop(missing_var)

    result = subprocess.run(
        [*cmd, "--env-file", "/dev/null", "config"],
        cwd=DEPLOY_DIR,
        env=_compose_env(**env_values),
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0
    assert missing_var in result.stderr


def test_compose_config_fails_when_prefect_auth_is_missing():
    cmd = _docker_compose_cmd()
    env_values = REQUIRED_DEPLOY_ENV.copy()
    env_values.pop("PREFECT_API_AUTH_STRING")

    result = subprocess.run(
        [*cmd, "--env-file", "/dev/null", "config"],
        cwd=DEPLOY_DIR,
        env=_compose_env(**env_values),
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0
    assert "PREFECT_API_AUTH_STRING" in result.stderr


def test_compose_config_accepts_required_env_and_binds_prefect_to_loopback():
    cmd = _docker_compose_cmd()

    result = subprocess.run(
        [*cmd, "--env-file", "/dev/null", "config"],
        cwd=DEPLOY_DIR,
        env=_compose_env(**REQUIRED_DEPLOY_ENV),
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "host_ip: 127.0.0.1" in result.stdout
    assert 'published: "4200"' in result.stdout
    assert "PREFECT_SERVER_API_AUTH_STRING: admin:0123456789abcdef" in result.stdout
    assert "PREFECT_API_AUTH_STRING: admin:0123456789abcdef" in result.stdout


def test_deploy_compose_text_keeps_required_env_and_loopback_binding():
    text = (DEPLOY_DIR / "docker-compose.yml").read_text()
    env_example = (DEPLOY_DIR / ".env.example").read_text()

    for name in REQUIRED_DEPLOY_ENV:
        assert f"${{{name}:?" in text
    assert '"127.0.0.1:4200:4200"' in text
    assert "PREFECT_SERVER_API_AUTH_STRING" in text
    assert "Authorization" in text
    assert "base64.b64encode" in text
    assert "POSTGRES_PASSWORD" in env_example
    assert "URL-safe characters only" in env_example
    assert "PREFECT_API_AUTH_STRING=" in env_example


def test_legacy_prefect_compose_text_keeps_required_auth_and_loopback_binding():
    compose = (SCRIPTS_DOCKER_DIR / "docker-compose.prefect.yml").read_text()
    env_example = (SCRIPTS_DOCKER_DIR / ".env.example").read_text()

    assert '"127.0.0.1:4200:4200"' in compose
    assert "${PREFECT_API_AUTH_STRING:?PREFECT_API_AUTH_STRING must be set}" in compose
    assert "PREFECT_SERVER_API_AUTH_STRING" in compose
    assert "PREFECT_API_AUTH_STRING" in compose
    assert "PREFECT_AUTH_STRING" not in compose
    assert "PREFECT_API_AUTH_STRING=" in env_example
    assert "change-me" not in env_example


def test_legacy_prefect_compose_config_accepts_required_auth(tmp_path: Path):
    cmd = _docker_compose_cmd()
    workdir = tmp_path / "docker"
    workdir.mkdir()
    shutil.copy(
        SCRIPTS_DOCKER_DIR / "docker-compose.prefect.yml",
        workdir / "docker-compose.prefect.yml",
    )
    (workdir / ".env").write_text(
        "\n".join(
            [
                "PREFECT_API_AUTH_STRING=admin:0123456789abcdef",
                "PREFECT_UI_API_URL=http://localhost:4200/api",
                "PREFECT_API_URL=http://prefect-server:4200/api",
                "TZ=UTC",
                "SLUICE_IMAGE=ghcr.io/nerdneilsfield/sluice-pipeline:test",
                "",
            ]
        )
    )

    result = subprocess.run(
        [*cmd, "-f", "docker-compose.prefect.yml", "config"],
        cwd=workdir,
        env=_compose_env(),
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "host_ip: 127.0.0.1" in result.stdout
    assert "PREFECT_SERVER_API_AUTH_STRING: admin:0123456789abcdef" in result.stdout
    assert "PREFECT_API_AUTH_STRING: admin:0123456789abcdef" in result.stdout
    assert "Authorization" in result.stdout
