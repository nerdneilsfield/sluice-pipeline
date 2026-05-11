from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from sluice.dag import db
from sluice.dag.loader import load_task
from sluice.dag.runner import run_pipeline_stub

FIXTURE = Path(__file__).parent / "fixtures" / "example_task"


def _load_tmp_task(tmp_path):
    import shutil

    d = tmp_path / "task"
    shutil.copytree(FIXTURE, d)
    text = (d / "task.toml").read_text()
    text = text.replace('db_path  = "data/example.db"', f'db_path  = "{tmp_path}/active.db"')
    text = text.replace('seen_db  = "data/example_seen.db"', f'seen_db  = "{tmp_path}/seen.db"')
    (d / "task.toml").write_text(text)
    return load_task(d)


@pytest.mark.asyncio
async def test_runner_stub_records_pipeline_run(tmp_path):
    t = _load_tmp_task(tmp_path)
    now = datetime(2026, 5, 3, 14, 0, tzinfo=timezone.utc)

    result = await run_pipeline_stub(t, pipeline_name="fetch_blog", now=now)
    assert result.status == "success"
    assert result.items_in == 0

    async with db.open_active_db(t.db_path) as conn:
        cur = await conn.execute(
            "SELECT pipeline_name, status, items_in FROM pipelines WHERE pipeline_name='fetch_blog'"
        )
        row = await cur.fetchone()
    assert row[0] == "fetch_blog"
    assert row[1] == "success"
    assert row[2] == 0


@pytest.mark.asyncio
async def test_runner_stub_idempotent_within_cycle(tmp_path):
    t = _load_tmp_task(tmp_path)
    now = datetime(2026, 5, 3, 14, 0, tzinfo=timezone.utc)

    await run_pipeline_stub(t, pipeline_name="fetch_blog", now=now)
    await run_pipeline_stub(t, pipeline_name="fetch_blog", now=now)

    async with db.open_active_db(t.db_path) as conn:
        cur = await conn.execute("SELECT count(*) FROM pipelines WHERE pipeline_name='fetch_blog'")
        n = (await cur.fetchone())[0]
    assert n == 1


@pytest.mark.asyncio
async def test_runner_stub_all_pipelines_no_op(tmp_path):
    t = _load_tmp_task(tmp_path)
    now = datetime(2026, 5, 3, 14, 0, tzinfo=timezone.utc)
    for pipeline in t.pipelines:
        await run_pipeline_stub(t, pipeline_name=pipeline.name, now=now)

    async with db.open_active_db(t.db_path) as conn:
        cur = await conn.execute(
            "SELECT pipeline_name, status FROM pipelines ORDER BY pipeline_name"
        )
        rows = await cur.fetchall()
    assert {r[0] for r in rows} == {p.name for p in t.pipelines}
    assert all(r[1] == "success" for r in rows)


@pytest.mark.asyncio
async def test_runner_stub_new_cycle_new_row(tmp_path):
    t = _load_tmp_task(tmp_path)

    day1 = datetime(2026, 5, 3, 14, 0, tzinfo=timezone.utc)
    day2 = datetime(2026, 5, 4, 14, 0, tzinfo=timezone.utc)
    await run_pipeline_stub(t, pipeline_name="fetch_blog", now=day1)
    await run_pipeline_stub(t, pipeline_name="fetch_blog", now=day2)

    async with db.open_active_db(t.db_path) as conn:
        cur = await conn.execute("SELECT count(*) FROM pipelines WHERE pipeline_name='fetch_blog'")
        n = (await cur.fetchone())[0]
    assert n == 2
