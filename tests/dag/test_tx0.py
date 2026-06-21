from __future__ import annotations

from pathlib import Path

import pytest

from sluice.dag import db
from sluice.dag.config import (
    DeploymentsDef,
    PipelineDef,
    SinkDef,
    SourceDef,
    StageDef,
    TaskConfig,
    WindowDef,
)
from sluice.dag.dynamic_tables import migrate_task_dynamic_tables, upgrade_task_dynamic_tables
from sluice.dag.phases.tx0 import build_tx0_job, make_lease_owner
from sluice.dag.stores.pipelines import PipelinesStore
from sluice.dag.writer import WriterThread


def _task(tmp_path: Path) -> TaskConfig:
    return TaskConfig(
        name="t",
        db_path=str(tmp_path / "active.db"),
        seen_db=str(tmp_path / "seen.db"),
        enabled=True,
        timezone="UTC",
        window=WindowDef(anchor="0 8 * * *", length="24h"),
        deployments=DeploymentsDef(pipelines="all"),
        sources=[SourceDef("feed", "rss", {"url": "x"})],
        sinks=[SinkDef(
            "out", "file_md",
            {"out_dir": str(tmp_path / "out"), "filename_template": "d.md"},
        )],
        stages=[StageDef("fetch", "fetch_rss", {}), StageDef("dedupe", "dedupe", {})],
        pipelines=[
            PipelineDef(
                name="P", cron="0 8 * * *", stages=["fetch", "dedupe"],
                sources=["feed"], upstream_all=None, upstream_any=None,
                sinks=["out"], post_run_cleanup="none",
            )
        ],
        task_dir=str(tmp_path),
    )


async def _v3_task_db(tmp_path: Path) -> TaskConfig:
    task = _task(tmp_path)
    async with db.open_active_db(task.db_path) as conn:
        await migrate_task_dynamic_tables(conn, task)
        await conn.commit()
        await db.apply_v3_migrations(conn)
        await upgrade_task_dynamic_tables(conn, task)
    async with db.open_seen_db(task.seen_db):
        pass
    return task


@pytest.mark.asyncio
async def test_tx0_creates_pipeline_run_with_lease(tmp_path: Path):
    task = await _v3_task_db(tmp_path)
    writer = WriterThread(
        active_path=task.db_path, seen_path=task.seen_db, hold_rotation_lock=False
    )
    await writer.start()
    try:
        out: dict = {}
        await (
            await writer.submit(
                build_tx0_job(
                    task=task,
                    pipeline=task.pipelines[0],
                    run_key="rk-1",
                    cycle_anchor="2026-06-21T08:00:00+00:00",
                    trigger="cron",
                    started_at="2026-06-21T08:00:00+00:00",
                    lease_owner="worker-A",
                    lease_duration_seconds=3600.0,
                    out=out,
                )
            )
        )
    finally:
        await writer.stop()

    assert out["pipeline_run_id"] > 0
    assert out["lease_version"] == 1
    async with db.open_active_db(task.db_path) as conn:
        store = PipelinesStore(conn)
        state = await store.get_lease_state(out["pipeline_run_id"])
        cycle_id = await store.get_cycle_id(out["pipeline_run_id"])
    assert state["lease_owner"] == "worker-A"
    assert state["lease_version"] == 1
    assert state["status"] == "running"
    assert state["lease_until"] is not None
    assert cycle_id == "2026-06-21T08:00:00+00:00"


@pytest.mark.asyncio
async def test_tx0_idempotent_same_owner_reattaches(tmp_path: Path):
    task = await _v3_task_db(tmp_path)
    writer = WriterThread(
        active_path=task.db_path, seen_path=task.seen_db, hold_rotation_lock=False
    )
    await writer.start()
    try:
        out1: dict = {}
        await (
            await writer.submit(
                build_tx0_job(
                    task=task, pipeline=task.pipelines[0], run_key="rk-1",
                    cycle_anchor="c", trigger="cron",
                    started_at="2026-06-21T08:00:00+00:00",
                    lease_owner="worker-A", lease_duration_seconds=3600.0, out=out1,
                )
            )
        )
        out2: dict = {}
        await (
            await writer.submit(
                build_tx0_job(
                    task=task, pipeline=task.pipelines[0], run_key="rk-1",
                    cycle_anchor="c", trigger="cron",
                    started_at="2026-06-21T08:00:30+00:00",
                    lease_owner="worker-A", lease_duration_seconds=3600.0, out=out2,
                )
            )
        )
    finally:
        await writer.stop()
    assert out1["pipeline_run_id"] == out2["pipeline_run_id"]
    assert out2["lease_version"] == 1


@pytest.mark.asyncio
async def test_tx0_rejects_attach_when_owned_by_another(tmp_path: Path):
    task = await _v3_task_db(tmp_path)
    writer = WriterThread(
        active_path=task.db_path, seen_path=task.seen_db, hold_rotation_lock=False
    )
    await writer.start()
    try:
        out1: dict = {}
        await (
            await writer.submit(
                build_tx0_job(
                    task=task, pipeline=task.pipelines[0], run_key="rk-1",
                    cycle_anchor="c", trigger="cron",
                    started_at="2026-06-21T08:00:00+00:00",
                    lease_owner="worker-A", lease_duration_seconds=3600.0, out=out1,
                )
            )
        )
        out2: dict = {}
        with pytest.raises(Exception, match="already held|cannot attach"):
            await (
                await writer.submit(
                    build_tx0_job(
                        task=task, pipeline=task.pipelines[0], run_key="rk-1",
                        cycle_anchor="c", trigger="cron",
                        started_at="2026-06-21T08:00:30+00:00",
                        lease_owner="worker-B", lease_duration_seconds=3600.0, out=out2,
                    )
                )
            )
    finally:
        await writer.stop()


def test_make_lease_owner_unique():
    a = make_lease_owner()
    b = make_lease_owner()
    assert a != b
    assert a.startswith("runner-")
