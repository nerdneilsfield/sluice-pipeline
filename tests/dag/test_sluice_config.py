from __future__ import annotations

import pytest

from sluice.core.errors import ConfigError
from sluice.dag.sluice_config import RuntimeConfig, SluiceConfig, load_sluice_config


def test_runtime_config_defaults():
    cfg = RuntimeConfig()
    assert cfg.prefect_api_url == ""
    assert cfg.prefect_api_auth_string == ""
    assert cfg.work_pool == "default"
    assert cfg.default_timezone == "UTC"
    assert cfg.image is None
    assert cfg.concurrency_strategy == "queue"


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
        concurrency_strategy = "cancel_new"
        """
    )
    cfg = load_sluice_config(path)
    assert cfg.runtime.prefect_api_url == "http://localhost:4200/api"
    assert cfg.runtime.prefect_api_auth_string == "Bearer token"
    assert cfg.runtime.work_pool == "workers"
    assert cfg.runtime.default_timezone == "Asia/Shanghai"
    assert cfg.runtime.image == "registry.example/sluice:latest"
    assert cfg.runtime.concurrency_strategy == "cancel_new"


def test_runtime_concurrency_strategy_invalid_raises(tmp_path):
    path = tmp_path / "sluice.toml"
    path.write_text("[runtime]\nconcurrency_strategy = 'bogus'\n")
    with pytest.raises(ConfigError, match="concurrency_strategy"):
        load_sluice_config(path)


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


def test_load_sluice_config_wraps_invalid_encoding(tmp_path):
    path = tmp_path / "sluice.toml"
    path.write_bytes(b"\xff")

    with pytest.raises(ConfigError, match="invalid encoding"):
        load_sluice_config(path)


def test_runtime_config_v3_flags_default_off():
    cfg = RuntimeConfig()
    assert cfg.runtime_core == "v2"
    assert cfg.enable_experimental_v3 is False
    assert cfg.enable_outbox_dispatch is False
    assert cfg.enable_lease_recovery is False
    assert cfg.enable_rotation_manifest is False


def test_load_sluice_config_parses_v3_runtime_flags(tmp_path):
    path = tmp_path / "sluice.toml"
    path.write_text(
        """
        [runtime]
        runtime_core = "v3"
        enable_experimental_v3 = true
        enable_outbox_dispatch = true
        enable_lease_recovery = true
        enable_rotation_manifest = true
        """
    )
    cfg = load_sluice_config(path)
    assert cfg.runtime.runtime_core == "v3"
    assert cfg.runtime.enable_experimental_v3 is True
    assert cfg.runtime.enable_outbox_dispatch is True
    assert cfg.runtime.enable_lease_recovery is True
    assert cfg.runtime.enable_rotation_manifest is True


def test_load_sluice_config_rejects_v3_without_experimental_flag(tmp_path):
    """v3 is non-production; operators must acknowledge by setting the flag."""
    path = tmp_path / "sluice.toml"
    path.write_text(
        """
        [runtime]
        runtime_core = "v3"
        """
    )
    with pytest.raises(ConfigError, match="enable_experimental_v3"):
        load_sluice_config(path)


def test_load_sluice_config_rejects_invalid_runtime_core(tmp_path):
    path = tmp_path / "sluice.toml"
    path.write_text("[runtime]\nruntime_core = 'v4'\n")
    with pytest.raises(ConfigError, match="runtime_core"):
        load_sluice_config(path)


def test_load_sluice_config_rejects_non_bool_outbox_flag(tmp_path):
    path = tmp_path / "sluice.toml"
    path.write_text("[runtime]\nenable_outbox_dispatch = 'yes'\n")
    with pytest.raises(ConfigError, match="enable_outbox_dispatch"):
        load_sluice_config(path)
