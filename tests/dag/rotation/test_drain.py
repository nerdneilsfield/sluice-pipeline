from __future__ import annotations

from datetime import datetime, timedelta, timezone
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
from sluice.dag.dynamic_tables import (
    migrate_task_dynamic_tables,
    upgrade_task_dynamic_tables,
)
from sluice.dag.rotation.drain import (
    count_in_flight,
    drain_running_pipelines,
    recover_expired_leases,
)
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
        sinks=[],
        stages=[StageDef("extract", "dedupe", {})],
        pipelines=[
            PipelineDef(
                name="main", cron="0 8 * * *", stages=["extract"],
                sources=["feed"], upstream_all=None, upstream_any=None,
                sinks=None, post_run_cleanup="none",
            )
        ],
        task_dir=str(tmp_path),
    )


async def _v3_setup(tmp_path: Path) -> TaskConfig:
    task = _task(tmp_path)
    async with db.open_active_db(task.db_path) as conn:
        await migrate_task_dynamic_tables(conn, task)
        await conn.commit()
        await db.apply_v3_migrations(conn)
        await upgrade_task_dynamic_tables(conn, task)
    return task


async def _seed_run(
    conn, *, run_key="rk1", started_iso="2026-06-21T00:00:00+00:00",
    lease_until_iso="2026-06-21T01:00:00+00:00", owner="worker-A",
    status="running",
) -> int:
    cur = await conn.execute(
        "INSERT INTO pipelines(run_key, task, pipeline_name, cycle_anchor, cycle_id, "
        "trigger, started_at, status, lease_owner, lease_until, heartbeat_at, "
        "lease_version) VALUES (?, 't', 'main', 'c', 'c', 'cron', ?, ?, ?, ?, ?, 1)",
        (run_key, started_iso, status, owner, lease_until_iso, started_iso),
    )
    return cur.lastrowid


@pytest.mark.asyncio
async def test_recover_deletes_unlinked_expired_admissions(tmp_path: Path):
    task = await _v3_setup(tmp_path)
    async with db.open_active_db(task.db_path) as conn:
        await conn.execute(
            "INSERT INTO pipeline_admissions(admission_id, task, pipeline_name, "
            "registered_at, lease_until) VALUES "
            "('a1', 't', 'main', '2026-06-21T00:00:00+00:00', '2026-06-21T00:30:00+00:00')"
        )
        await conn.commit()
    counts = await recover_expired_leases(
        task.db_path, task.name, now=datetime(2026, 6, 21, 1, 0, tzinfo=timezone.utc)
    )
    assert counts["unlinked_deleted"] == 1
    assert counts["linked_fenced"] == 0
    async with db.open_active_db(task.db_path) as conn:
        cur = await conn.execute("SELECT COUNT(*) FROM pipeline_admissions")
        n = (await cur.fetchone())[0]
    assert n == 0


@pytest.mark.asyncio
async def test_recover_fences_expired_running_pipeline(tmp_path: Path):
    task = await _v3_setup(tmp_path)
    async with db.open_active_db(task.db_path) as conn:
        run_id = await _seed_run(
            conn,
            lease_until_iso="2026-06-21T00:30:00+00:00",
            owner="dead-worker",
        )
        await conn.commit()

    counts = await recover_expired_leases(
        task.db_path, task.name, now=datetime(2026, 6, 21, 1, 0, tzinfo=timezone.utc)
    )
    assert counts["linked_fenced"] == 1
    async with db.open_active_db(task.db_path) as conn:
        cur = await conn.execute(
            "SELECT status, lease_owner, lease_version, error_msg FROM pipelines WHERE id=?",
            (run_id,),
        )
        row = await cur.fetchone()
    assert row[0] == "failed"
    assert row[1] is None
    assert row[2] == 2  # 1 -> 2 (incremented)


@pytest.mark.asyncio
async def test_recover_preserves_unexpired_running_pipeline(tmp_path: Path):
    task = await _v3_setup(tmp_path)
    async with db.open_active_db(task.db_path) as conn:
        run_id = await _seed_run(
            conn, lease_until_iso="2026-06-21T03:00:00+00:00", owner="worker-A"
        )
        await conn.commit()
    counts = await recover_expired_leases(
        task.db_path, task.name, now=datetime(2026, 6, 21, 1, 0, tzinfo=timezone.utc)
    )
    assert counts["linked_fenced"] == 0
    async with db.open_active_db(task.db_path) as conn:
        cur = await conn.execute(
            "SELECT status, lease_owner FROM pipelines WHERE id=?", (run_id,)
        )
        row = await cur.fetchone()
    assert row[0] == "running"
    assert row[1] == "worker-A"


@pytest.mark.asyncio
async def test_drain_with_recovery_returns_true_after_fencing(tmp_path: Path):
    task = await _v3_setup(tmp_path)
    async with db.open_active_db(task.db_path) as conn:
        await _seed_run(
            conn, lease_until_iso="2026-06-21T00:30:00+00:00", owner="dead"
        )
        await conn.commit()
    ok = await drain_running_pipelines(
        task.db_path, task.name, timeout_seconds=2.0, poll_interval_seconds=0.05
    )
    assert ok is True


@pytest.mark.asyncio
async def test_drain_without_recovery_times_out_on_zombie(tmp_path: Path):
    task = await _v3_setup(tmp_path)
    async with db.open_active_db(task.db_path) as conn:
        await _seed_run(
            conn, lease_until_iso="2026-06-21T00:30:00+00:00", owner="dead"
        )
        await conn.commit()
    ok = await drain_running_pipelines(
        task.db_path, task.name, timeout_seconds=0.5,
        poll_interval_seconds=0.1, recover_first=False,
    )
    assert ok is False


@pytest.mark.asyncio
async def test_fenced_finalize_after_recovery_returns_zero(tmp_path: Path):
    """After drain fences a row, finalize_v3 from the old owner returns 0."""
    task = await _v3_setup(tmp_path)
    async with db.open_active_db(task.db_path) as conn:
        run_id = await _seed_run(
            conn, owner="worker-A", lease_until_iso="2026-06-21T00:30:00+00:00"
        )
        await conn.commit()

    await recover_expired_leases(
        task.db_path, task.name, now=datetime(2026, 6, 21, 1, 0, tzinfo=timezone.utc)
    )

    async with db.open_active_db(task.db_path) as conn:
        store = PipelinesStore(conn)
        rowcount = await store.finalize_v3(
            run_id,
            finished_at="2026-06-21T01:00:00+00:00",
            status="success", items_in=1, items_out=1, error_msg=None,
            lease_owner="worker-A", lease_version=1,
        )
    assert rowcount == 0
