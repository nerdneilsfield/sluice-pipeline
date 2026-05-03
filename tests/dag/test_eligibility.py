from __future__ import annotations

import pytest

from sluice.dag.config import PipelineDef
from sluice.dag.dynamic_tables import ensure_pipeline_table
from sluice.dag.eligibility import select_eligible_items, select_entry_carryover


def _pipeline(name, **kw):
    base = dict(
        name=name,
        cron="0 8 * * *",
        stages=["s"],
        sources=None,
        upstream_all=None,
        upstream_any=None,
        sinks=None,
        post_run_cleanup="none",
        max_attempts=3,
    )
    base.update(kw)
    return PipelineDef(**base)


async def _seed_item(conn, item_id, source_name="src", ingested_by="U"):
    await conn.execute(
        "INSERT INTO items(item_id, source_name, ingested_at, ingested_by, "
        "updated_at, raw_json, dropped) VALUES (?, ?, ?, ?, ?, ?, 0)",
        (
            item_id,
            source_name,
            "2026-05-04T00:00:00+00:00",
            ingested_by,
            "2026-05-04T00:00:00+00:00",
            "{}",
        ),
    )


async def _seed_pipeline_run(conn, run_key, pipeline_name, status="success"):
    await conn.execute(
        "INSERT INTO pipelines(run_key, task, pipeline_name, cycle_anchor, "
        "trigger, started_at, status, finished_at) VALUES "
        "(?, 't', ?, '2026-05-04T00:00:00+00:00', 'manual', "
        "'2026-05-04T00:00:00+00:00', ?, '2026-05-04T00:00:01+00:00')",
        (run_key, pipeline_name, status),
    )


@pytest.mark.asyncio
async def test_eligibility_excludes_terminal_p_rows(active_db):
    await ensure_pipeline_table(active_db, "P")
    await ensure_pipeline_table(active_db, "U")
    await _seed_item(active_db, "a")
    await _seed_item(active_db, "b")
    await _seed_pipeline_run(active_db, "rk_u", "U")
    await active_db.execute(
        "INSERT INTO pipeline_U(item_id, last_run_id, status, processed_at) "
        "SELECT 'a', id, 'success', '2026-05-04T00:00:01+00:00' "
        "FROM pipelines WHERE run_key='rk_u'"
    )
    await active_db.execute(
        "INSERT INTO pipeline_U(item_id, last_run_id, status, processed_at) "
        "SELECT 'b', id, 'success', '2026-05-04T00:00:01+00:00' "
        "FROM pipelines WHERE run_key='rk_u'"
    )
    await _seed_pipeline_run(active_db, "rk_p", "P")
    await active_db.execute(
        "INSERT INTO pipeline_P(item_id, last_run_id, status, processed_at) "
        "SELECT 'b', id, 'success', '2026-05-04T00:00:01+00:00' "
        "FROM pipelines WHERE run_key='rk_p'"
    )
    await active_db.commit()

    rows = await select_eligible_items(active_db, _pipeline("P", upstream_all=["U"]))
    assert {row["item_id"] for row in rows} == {"a"}


@pytest.mark.asyncio
async def test_eligibility_intersection_requires_all_upstreams(active_db):
    for name in ("P", "U1", "U2"):
        await ensure_pipeline_table(active_db, name)
    await _seed_item(active_db, "a")
    await _seed_item(active_db, "b")
    await _seed_pipeline_run(active_db, "rk_u1", "U1")
    await _seed_pipeline_run(active_db, "rk_u2", "U2")
    for item_id, pipeline_name, run_key in (
        ("a", "U1", "rk_u1"),
        ("a", "U2", "rk_u2"),
        ("b", "U1", "rk_u1"),
    ):
        await active_db.execute(
            f"INSERT INTO pipeline_{pipeline_name}(item_id, last_run_id, status, processed_at) "
            "SELECT ?, id, 'success', '2026-05-04T00:00:01+00:00' "
            "FROM pipelines WHERE run_key=?",
            (item_id, run_key),
        )
    await active_db.commit()
    rows = await select_eligible_items(
        active_db,
        _pipeline("P", upstream_all=["U1", "U2"], input_mode="intersection"),
    )
    assert {row["item_id"] for row in rows} == {"a"}


@pytest.mark.asyncio
async def test_entry_carryover_includes_pending_items_from_p_sources(active_db):
    await ensure_pipeline_table(active_db, "P")
    await _seed_item(active_db, "a", source_name="feed_a", ingested_by="P")
    await _seed_item(active_db, "b", source_name="feed_b", ingested_by="P")
    await _seed_item(active_db, "c", source_name="feed_a", ingested_by="P")
    await _seed_pipeline_run(active_db, "rk_p", "P")
    await active_db.execute(
        "INSERT INTO pipeline_P(item_id, last_run_id, status, processed_at) "
        "SELECT 'c', id, 'success', '2026-05-04T00:00:01+00:00' "
        "FROM pipelines WHERE run_key='rk_p'"
    )
    await active_db.commit()

    rows = await select_entry_carryover(active_db, _pipeline("P", sources=["feed_a"]))
    assert {row["item_id"] for row in rows} == {"a"}
