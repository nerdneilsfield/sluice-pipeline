from __future__ import annotations

import json
from pathlib import Path

import pytest


@pytest.mark.asyncio
async def test_cli_compat_v2_startup_does_not_apply_v3_migration(tmp_path: Path):
    """runtime_core=v2 startup must leave schema at v2."""
    from sluice.dag import db

    db_path = tmp_path / "active.db"
    async with db.open_active_db(str(db_path)) as conn:
        cur = await conn.execute("SELECT MAX(version) FROM schema_version")
        version = (await cur.fetchone())[0]
        cur = await conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='sink_outbox'"
        )
        sink_outbox_exists = await cur.fetchone()
    assert version == 2
    assert sink_outbox_exists is None


def test_cli_compat_sluice_run_command_accepts_v2_config(tmp_path: Path):
    """The `sluice run` CLI surface still parses existing v2 task configs."""
    from sluice.dag.loader import load_task

    task_dir = tmp_path / "tasks" / "demo"
    task_dir.mkdir(parents=True)
    (task_dir / "task.toml").write_text(
        """
[task]
name = "demo"
db_path = "demo.db"
seen_db = "demo_seen.db"
enabled = true
timezone = "UTC"

[task.window]
anchor = "0 8 * * *"
length = "24h"

[task.deployments]
pipelines = "all"
"""
    )
    (task_dir / "sources.toml").write_text(
        """
[[sources]]
name = "blog"
type = "rss"
url = "https://example.com/rss"
"""
    )
    (task_dir / "stages.toml").write_text(
        """
[[stages]]
name = "dedupe"
type = "dedupe"
"""
    )
    (task_dir / "sinks.toml").write_text(
        """
[[sinks]]
name = "telegram"
type = "telegram"
token = "env:TELEGRAM_BOT_TOKEN"
chat_id = "env:TELEGRAM_CHAT_ID"
"""
    )
    (task_dir / "pipelines.toml").write_text(
        """
[[pipelines]]
name = "main"
cron = "0 8 * * *"
sources = ["blog"]
stages = ["dedupe"]
sinks = ["telegram"]
"""
    )
    task = load_task(task_dir)
    assert task.name == "demo"
    assert task.pipelines[0].name == "main"
    assert task.sources[0].name == "blog"


def test_cli_compat_runtime_config_defaults_to_v2(tmp_path: Path):
    """A sluice.toml without v3 flags still loads cleanly with v2 defaults."""
    from sluice.dag.sluice_config import load_sluice_config

    sluice_toml = tmp_path / "sluice.toml"
    sluice_toml.write_text(
        """
[runtime]
work_pool = "default"
"""
    )
    cfg = load_sluice_config(sluice_toml)
    assert cfg.runtime.runtime_core == "v2"
    assert cfg.runtime.enable_outbox_dispatch is False
