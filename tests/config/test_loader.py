import textwrap

import pytest

from sluice.core.errors import ConfigError
from sluice.loader import load_all


def test_load_all(tmp_path, monkeypatch):
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "pipelines").mkdir()
    (tmp_path / "configs" / "sluice.toml").write_text(
        textwrap.dedent("""
        [state]
        db_path = "./data/sluice.db"
        [runtime]
        timezone = "Asia/Shanghai"
        default_cron = "0 8 * * *"
        prefect_api_url = "http://localhost:4200/api"
        prefect_api_auth_string = "env:PREFECT_AUTH"
        [fetcher]
        chain = ["trafilatura", "firecrawl"]
        min_chars = 500
        on_all_failed = "skip"
        [fetchers.trafilatura]
        type = "trafilatura"
        timeout = 10
        [fetchers.firecrawl]
        type = "firecrawl"
        base_url = "http://localhost:3002"
        api_key = "env:FC_KEY"
        timeout = 60
        [fetcher.cache]
        enabled = true
        ttl = "7d"
    """)
    )
    (tmp_path / "configs" / "providers.toml").write_text(
        textwrap.dedent("""
        [[providers]]
        name = "glm"
        type = "openai_compatible"
        [[providers.base]]
        url = "https://x"
        weight = 1
        key = [{ value = "env:K", weight = 1 }]
        [[providers.models]]
        model_name = "glm-4-flash"
    """)
    )
    (tmp_path / "configs" / "pipelines" / "p1.toml").write_text(
        textwrap.dedent("""
        id = "p1"
        window = "24h"
        [[sources]]
        type = "rss"
        url  = "https://x/feed"
        [[stages]]
        name = "d"
        type = "dedupe"
        [[sinks]]
        id = "local"
        type = "file_md"
        input = "context.markdown"
        path  = "./out/{run_date}.md"
    """)
    )
    monkeypatch.setenv("PREFECT_AUTH", "admin:pass")
    bundle = load_all(tmp_path / "configs")
    assert bundle.global_cfg.state.db_path == "./data/sluice.db"
    assert bundle.global_cfg.runtime.prefect_api_url == "http://localhost:4200/api"
    assert bundle.global_cfg.runtime.prefect_api_auth_string == "admin:pass"
    assert bundle.providers.providers[0].name == "glm"
    assert bundle.pipelines["p1"].id == "p1"


def test_load_rejects_invalid_prefect_api_url(tmp_path):
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "pipelines").mkdir()
    (tmp_path / "configs" / "sluice.toml").write_text(
        textwrap.dedent("""
        [runtime]
        prefect_api_url = "localhost:4200/api"
    """)
    )
    (tmp_path / "configs" / "providers.toml").write_text(
        textwrap.dedent("""
        providers = []
    """)
    )

    with pytest.raises(ConfigError, match="prefect_api_url"):
        load_all(tmp_path / "configs")


def test_load_rejects_empty_prefect_api_auth_string(tmp_path):
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "pipelines").mkdir()
    (tmp_path / "configs" / "sluice.toml").write_text(
        textwrap.dedent("""
        [runtime]
        prefect_api_url = "http://localhost:4200/api"
        prefect_api_auth_string = ""
    """)
    )
    (tmp_path / "configs" / "providers.toml").write_text(
        textwrap.dedent("""
        providers = []
    """)
    )

    with pytest.raises(ConfigError, match="prefect_api_auth_string"):
        load_all(tmp_path / "configs")
