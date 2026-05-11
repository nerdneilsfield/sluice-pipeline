from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from sluice.dag import db
from sluice.dag.rotation.drain import (
    count_in_flight,
    count_running,
    drain_running_pipelines,
)


async def _seed_pipeline(active, run_key: str, status: str):
    await active.execute(
        "INSERT INTO pipelines(run_key, task, pipeline_name, cycle_anchor, "
        "trigger, started_at, status) VALUES "
        "(?, 't', 'p', '2026-05-04T00:00:00+00:00', 'manual', "
        "'2026-05-04T00:00:00+00:00', ?)",
        (run_key, status),
    )
    await active.commit()


async def _seed_admission(active, admission_id: str = "aid-1"):
    await active.execute(
        "INSERT INTO pipeline_admissions(admission_id, task, pipeline_name, "
        "registered_at) VALUES (?, 't', 'p', '2026-05-04T00:00:00+00:00')",
        (admission_id,),
    )
    await active.commit()


@pytest.mark.asyncio
async def test_count_running_zero(tmp_path: Path):
    active_path = str(tmp_path / "active.db")
    async with db.open_active_db(active_path):
        pass

    assert await count_running(active_path, "t") == 0
    assert await count_in_flight(active_path, "t") == (0, 0)


@pytest.mark.asyncio
async def test_count_running_excludes_finished_and_counts_admissions(tmp_path: Path):
    active_path = str(tmp_path / "active.db")
    async with db.open_active_db(active_path) as active:
        await _seed_pipeline(active, "rk1", "success")
        await _seed_pipeline(active, "rk2", "running")
        await _seed_admission(active)

    assert await count_running(active_path, "t") == 1
    assert await count_in_flight(active_path, "t") == (1, 1)


@pytest.mark.asyncio
async def test_drain_returns_true_when_already_idle(tmp_path: Path):
    active_path = str(tmp_path / "active.db")
    async with db.open_active_db(active_path):
        pass

    drained = await drain_running_pipelines(
        active_path,
        "t",
        timeout_seconds=0.5,
        poll_interval_seconds=0.05,
    )

    assert drained is True


@pytest.mark.asyncio
async def test_drain_returns_false_on_running_timeout(tmp_path: Path):
    active_path = str(tmp_path / "active.db")
    async with db.open_active_db(active_path) as active:
        await _seed_pipeline(active, "rk_running", "running")

    drained = await drain_running_pipelines(
        active_path,
        "t",
        timeout_seconds=0.1,
        poll_interval_seconds=0.02,
    )

    assert drained is False


@pytest.mark.asyncio
async def test_drain_waits_on_admissions(tmp_path: Path):
    active_path = str(tmp_path / "active.db")
    async with db.open_active_db(active_path) as active:
        await _seed_admission(active)

    drained = await drain_running_pipelines(
        active_path,
        "t",
        timeout_seconds=0.1,
        poll_interval_seconds=0.02,
    )

    assert drained is False


@pytest.mark.asyncio
async def test_drain_succeeds_when_admissions_clear(tmp_path: Path):
    active_path = str(tmp_path / "active.db")
    async with db.open_active_db(active_path) as active:
        await _seed_admission(active)

    async def clear_after_delay():
        await asyncio.sleep(0.05)
        async with db.open_active_db(active_path) as active:
            await active.execute("DELETE FROM pipeline_admissions WHERE admission_id = 'aid-1'")
            await active.commit()

    clear_task = asyncio.create_task(clear_after_delay())
    drained = await drain_running_pipelines(
        active_path,
        "t",
        timeout_seconds=0.5,
        poll_interval_seconds=0.02,
    )
    await clear_task

    assert drained is True
