import gc
import warnings

import pytest
from typer.testing import CliRunner

from sluice.cli import app
from sluice.state.db import open_db

runner = CliRunner()


def _sqlite_resource_warnings(caught):
    return [
        w
        for w in caught
        if issubclass(w.category, ResourceWarning)
        and (
            "aiosqlite.core.Connection" in str(w.message)
            or "sqlite3.Connection" in str(w.message)
            or "unclosed database" in str(w.message)
        )
    ]


@pytest.mark.asyncio
async def test_deliveries_lists_rows(tmp_path):
    cfg_dir = tmp_path / "configs"
    cfg_dir.mkdir()
    (cfg_dir / "pipelines").mkdir()
    db = tmp_path / "s.db"
    (cfg_dir / "sluice.toml").write_text(
        f'[state]\ndb_path = "{db}"\n'
        f'attachment_dir = "{tmp_path / "att"}"\n'
        f'attachment_url_prefix = ""\n'
    )
    (cfg_dir / "providers.toml").write_text(
        "[[providers]]\nname='dummy'\ntype='openai_compatible'\nbase=[]\nmodels=[]\n"
    )
    (cfg_dir / "pipelines" / "p1.toml").write_text(
        'id = "p1"\ncron = "0 8 * * *"\n'
        '[[sources]]\ntype="rss"\nurl="https://example.com/feed.xml"\n'
        '[[stages]]\nname="d"\ntype="dedupe"\n'
        '[[sinks]]\nid="f"\ntype="file_md"\n'
        'input="context.md"\npath="/tmp/out.md"\n'
    )
    async with open_db(db) as conn:
        await conn.execute(
            "INSERT INTO sink_delivery_log "
            "(pipeline_id, run_key, sink_id, sink_type, attempt_at, ordinal, "
            " message_kind, recipient, external_id, status, error_class, error_msg) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "p1",
                "p1/x",
                "tg",
                "telegram",
                "2026-04-29T00:00:00+00:00",
                1,
                "brief",
                None,
                "ext1",
                "success",
                None,
                None,
            ),
        )
        await conn.commit()
    res = runner.invoke(app, ["deliveries", "p1", "--config-dir", str(cfg_dir)])
    assert res.exit_code == 0, res.stdout
    assert "tg" in res.stdout


@pytest.mark.asyncio
async def test_deliveries_closes_sqlite_connection(tmp_path):
    cfg_dir = tmp_path / "configs"
    cfg_dir.mkdir()
    (cfg_dir / "pipelines").mkdir()
    db = tmp_path / "s.db"
    (cfg_dir / "sluice.toml").write_text(
        f'[state]\ndb_path = "{db}"\n'
        f'attachment_dir = "{tmp_path / "att"}"\n'
        f'attachment_url_prefix = ""\n'
    )
    (cfg_dir / "providers.toml").write_text(
        "[[providers]]\nname='dummy'\ntype='openai_compatible'\nbase=[]\nmodels=[]\n"
    )
    (cfg_dir / "pipelines" / "p1.toml").write_text(
        'id = "p1"\ncron = "0 8 * * *"\n'
        '[[sources]]\ntype="rss"\nurl="https://example.com/feed.xml"\n'
        '[[stages]]\nname="d"\ntype="dedupe"\n'
        '[[sinks]]\nid="f"\ntype="file_md"\n'
        'input="context.md"\npath="/tmp/out.md"\n'
    )
    async with open_db(db) as conn:
        await conn.execute(
            "INSERT INTO sink_delivery_log "
            "(pipeline_id, run_key, sink_id, sink_type, attempt_at, ordinal, "
            " message_kind, recipient, external_id, status, error_class, error_msg) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "p1",
                "p1/x",
                "tg",
                "telegram",
                "2026-04-29T00:00:00+00:00",
                1,
                "brief",
                None,
                "ext1",
                "success",
                None,
                None,
            ),
        )
        await conn.commit()

    # Clear unrelated objects from previous tests before checking this CLI path.
    gc.collect()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", ResourceWarning)
        res = runner.invoke(app, ["deliveries", "p1", "--config-dir", str(cfg_dir)])
        assert res.exit_code == 0, res.stdout
        gc.collect()

    assert _sqlite_resource_warnings(caught) == []
