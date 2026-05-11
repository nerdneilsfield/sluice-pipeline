from __future__ import annotations

import pytest

from sluice.dag.sluice_config import RuntimeConfig, SluiceConfig, load_sluice_config


def test_runtime_config_defaults():
    cfg = RuntimeConfig()
    assert cfg.prefect_api_url == ""
    assert cfg.prefect_api_auth_string == ""
    assert cfg.work_pool == "default"
    assert cfg.default_timezone == "UTC"
    assert cfg.image is None


def test_load_sluice_config_defaults_when_missing(tmp_path):
    cfg = load_sluice_config(tmp_path / "missing.toml")
    assert isinstance(cfg, SluiceConfig)
    assert cfg.rotation.drain_timeout_seconds == 30 * 60
    assert cfg.rotation.archive_dir == "data/archive"


def test_load_sluice_config_parses_rotation(tmp_path):
    path = tmp_path / "sluice.toml"
    path.write_text(
        """
        [gc.rotation]
        drain_timeout = "2h"
        archive_dir = "archive"
        """
    )
    cfg = load_sluice_config(path)
    assert cfg.rotation.drain_timeout_seconds == 7200
    assert cfg.rotation.archive_dir == "archive"


def test_load_sluice_config_parses_runtime(tmp_path):
    path = tmp_path / "sluice.toml"
    path.write_text(
        """
        [runtime]
        prefect_api_url = "http://localhost:4200/api"
        prefect_api_auth_string = "Bearer token"
        work_pool = "workers"
        default_timezone = "Asia/Shanghai"
        image = "registry.example/sluice:latest"
        """
    )
    cfg = load_sluice_config(path)
    assert cfg.runtime.prefect_api_url == "http://localhost:4200/api"
    assert cfg.runtime.prefect_api_auth_string == "Bearer token"
    assert cfg.runtime.work_pool == "workers"
    assert cfg.runtime.default_timezone == "Asia/Shanghai"
    assert cfg.runtime.image == "registry.example/sluice:latest"


def test_load_sluice_config_runtime_defaults_when_missing(tmp_path):
    path = tmp_path / "sluice.toml"
    path.write_text("[gc.rotation]\narchive_dir = 'archive'\n")
    cfg = load_sluice_config(path)
    assert cfg.runtime == RuntimeConfig()


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
    path.write_text("[gc.rotation]\narchive_dir = 'archive'\n")
    cfg = load_sluice_config(path)
    assert cfg.rotation.default_cron is None


def test_load_sluice_config_rejects_bad_duration(tmp_path):
    path = tmp_path / "sluice.toml"
    path.write_text("[gc.rotation]\ndrain_timeout = 'soon'\n")
    with pytest.raises(ValueError, match="invalid duration"):
        load_sluice_config(path)
