from __future__ import annotations

from pathlib import Path

import pytest

from sluice.dag import db
from sluice.dag.dynamic_tables import (
    migrate_task_dynamic_tables,
    upgrade_task_dynamic_tables,
)
from sluice.dag.stores.pipeline_state import PipelineStateStore
from sluice.dag.stores.pipelines import PipelinesStore
from sluice.dag.stores.stage_attempt_log import StageAttemptLogStore
from sluice.dag.stores.stage_state import StageStateStore


async def _v3_db(tmp_path: Path, *, pipeline_name: str = "blog", stage_name: str = "extract"):
    """Build a v3-upgraded active DB with one pipeline and one stage."""
    from dataclasses import dataclass

    @dataclass(frozen=True)
    class _P:
        name: str
        sinks: tuple = ()

    @dataclass(frozen=True)
    class _S:
        name: str

    @dataclass(frozen=True)
    class _T:
        pipelines: tuple
        stages: tuple

    db_path = tmp_path / "active.db"
    task = _T(pipelines=(_P(pipeline_name),), stages=(_S(stage_name),))
    async with db.open_active_db(str(db_path)) as conn:
        await migrate_task_dynamic_tables(conn, task)
        await conn.commit()
        await db.apply_v3_migrations(conn)
        await upgrade_task_dynamic_tables(conn, task)
    return db_path, task


async def _insert_run(conn, *, run_key: str = "rk", cycle_anchor: str = "2026-06-21") -> int:
    cur = await conn.execute(
        "INSERT INTO pipelines (run_key, task, pipeline_name, cycle_anchor, cycle_id, "
        "trigger, started_at, status) "
        "VALUES (?, 't', 'blog', ?, ?, 'cron', 'n', 'running')",
        (run_key, cycle_anchor, cycle_anchor),
    )
    return cur.lastrowid


@pytest.mark.asyncio
async def test_pipeline_state_upsert_v3_creates_run_scoped_row(tmp_path: Path):
    db_path, _ = await _v3_db(tmp_path)
    async with db.open_active_db(str(db_path)) as conn:
        await db.apply_v3_migrations(conn)
        run_id = await _insert_run(conn, run_key="rk1")
        store = PipelineStateStore(conn, "blog")
        await store.upsert_v3(
            run_id=run_id,
            item_id="i1",
            cycle_id="2026-06-21",
            status="success",
            processed_at="n",
            error_msg=None,
        )
        cur = await conn.execute(
            "SELECT run_id, item_id, cycle_id, status FROM pipeline_blog"
        )
        rows = await cur.fetchall()
    assert rows == [(run_id, "i1", "2026-06-21", "success")]


@pytest.mark.asyncio
async def test_pipeline_state_upsert_v3_two_runs_same_item_distinct_rows(tmp_path: Path):
    db_path, _ = await _v3_db(tmp_path)
    async with db.open_active_db(str(db_path)) as conn:
        await db.apply_v3_migrations(conn)
        run_a = await _insert_run(conn, run_key="rk_a", cycle_anchor="2026-06-21")
        run_b = await _insert_run(conn, run_key="rk_b", cycle_anchor="2026-06-22")
        store = PipelineStateStore(conn, "blog")
        await store.upsert_v3(
            run_id=run_a, item_id="i1", cycle_id="2026-06-21",
            status="success", processed_at="n", error_msg=None,
        )
        await store.upsert_v3(
            run_id=run_b, item_id="i1", cycle_id="2026-06-22",
            status="failed", processed_at="n", error_msg="boom",
        )
        cur = await conn.execute(
            "SELECT run_id, status FROM pipeline_blog ORDER BY run_id"
        )
        rows = await cur.fetchall()
    assert rows == [(run_a, "success"), (run_b, "failed")]


@pytest.mark.asyncio
async def test_stage_state_upsert_v3_creates_run_scoped_row(tmp_path: Path):
    db_path, _ = await _v3_db(tmp_path)
    async with db.open_active_db(str(db_path)) as conn:
        await db.apply_v3_migrations(conn)
        run_id = await _insert_run(conn, run_key="rk1")
        cur = await conn.execute(
            "INSERT INTO stages (pipeline_run_id, stage_name, started_at, status) "
            "VALUES (?, 'extract', 'n', 'success')",
            (run_id,),
        )
        stage_run_id = cur.lastrowid
        store = StageStateStore(conn, "extract")
        await store.upsert_v3(
            run_id=run_id,
            pipeline_name="blog",
            item_id="i1",
            cycle_id="2026-06-21",
            status="done",
            attempts=1,
            last_stage_run_id=stage_run_id,
            last_processed_at="n",
            last_error_class=None,
            last_error_msg=None,
        )
        cur = await conn.execute(
            "SELECT run_id, pipeline_name, item_id, status FROM stage_extract"
        )
        rows = await cur.fetchall()
    assert rows == [(run_id, "blog", "i1", "done")]


@pytest.mark.asyncio
async def test_stage_attempt_log_next_attempt_no_v3_scoped_by_run(tmp_path: Path):
    db_path, _ = await _v3_db(tmp_path)
    async with db.open_active_db(str(db_path)) as conn:
        await db.apply_v3_migrations(conn)
        run_a = await _insert_run(conn, run_key="rk_a")
        run_b = await _insert_run(conn, run_key="rk_b")
        cur = await conn.execute(
            "INSERT INTO stages (pipeline_run_id, stage_name, started_at, status) "
            "VALUES (?, 'extract', 'n', 'success')",
            (run_a,),
        )
        stage_run_a = cur.lastrowid
        cur = await conn.execute(
            "INSERT INTO stages (pipeline_run_id, stage_name, started_at, status) "
            "VALUES (?, 'extract', 'n', 'success')",
            (run_b,),
        )
        # stage_run_b intentionally unused; we only need the stage row to exist

        store = StageAttemptLogStore(conn)
        # Append two attempts for run_a, one for run_b, all on same item
        await store.append_v3(
            stage_run_id=stage_run_a, pipeline_run_id=run_a, run_id=run_a,
            cycle_id="c", pipeline_name="blog", stage_name="extract",
            item_id="i1", attempt_no=1, status="failed", processed_at="n",
            error_class=None, error_msg=None,
        )
        await store.append_v3(
            stage_run_id=stage_run_a, pipeline_run_id=run_a, run_id=run_a,
            cycle_id="c", pipeline_name="blog", stage_name="extract",
            item_id="i1", attempt_no=2, status="success", processed_at="n",
            error_class=None, error_msg=None,
        )
        n_a = await store.next_attempt_no_v3(
            run_id=run_a, pipeline_name="blog", stage_name="extract", item_id="i1"
        )
        n_b = await store.next_attempt_no_v3(
            run_id=run_b, pipeline_name="blog", stage_name="extract", item_id="i1"
        )
    assert n_a == 3
    assert n_b == 1


@pytest.mark.asyncio
async def test_pipelines_store_has_cycle_id_helper(tmp_path: Path):
    db_path, _ = await _v3_db(tmp_path)
    async with db.open_active_db(str(db_path)) as conn:
        await db.apply_v3_migrations(conn)
        run_id = await _insert_run(conn, run_key="rk1", cycle_anchor="2026-06-21")
        store = PipelinesStore(conn)
        cycle_id = await store.get_cycle_id(run_id)
        lease_state = await store.get_lease_state(run_id)
    assert cycle_id == "2026-06-21"
    assert lease_state is not None
    assert "lease_owner" in lease_state
    assert "lease_version" in lease_state


@pytest.mark.asyncio
async def test_pipelines_store_set_lease_records_owner_and_version(tmp_path: Path):
    db_path, _ = await _v3_db(tmp_path)
    async with db.open_active_db(str(db_path)) as conn:
        await db.apply_v3_migrations(conn)
        run_id = await _insert_run(conn, run_key="rk1")
        store = PipelinesStore(conn)
        await store.set_lease(
            run_id=run_id,
            lease_owner="worker-1",
            lease_until="2026-06-21T01:00:00+00:00",
            heartbeat_at="2026-06-21T00:00:00+00:00",
            lease_version=1,
        )
        state = await store.get_lease_state(run_id)
    assert state["lease_owner"] == "worker-1"
    assert state["lease_version"] == 1


@pytest.mark.asyncio
async def test_legacy_upsert_still_works_on_v2_schema(tmp_path: Path):
    """Smoke: a fresh v2 DB (no v3 migration) must still accept legacy upserts."""
    from dataclasses import dataclass

    @dataclass(frozen=True)
    class _P:
        name: str
        sinks: tuple = ()

    @dataclass(frozen=True)
    class _S:
        name: str

    @dataclass(frozen=True)
    class _T:
        pipelines: tuple
        stages: tuple

    db_path = tmp_path / "active.db"
    task = _T(pipelines=(_P("blog"),), stages=(_S("extract"),))
    async with db.open_active_db(str(db_path)) as conn:
        await migrate_task_dynamic_tables(conn, task)
        cur = await conn.execute(
            "INSERT INTO pipelines (run_key, task, pipeline_name, cycle_anchor, "
            "trigger, started_at, status) VALUES (?, 't', 'blog', 'c', 'cron', 'n', 'success')"
        , ("rk",))
        run_id = cur.lastrowid
        await conn.commit()
        store = PipelineStateStore(conn, "blog")
        await store.upsert(
            item_id="i1", last_run_id=run_id, status="success",
            processed_at="n", error_msg=None,
        )
        cur = await conn.execute("SELECT COUNT(*) FROM pipeline_blog")
        count = (await cur.fetchone())[0]
    assert count == 1
