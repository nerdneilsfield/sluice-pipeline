from __future__ import annotations

import pytest

from sluice.dag.config import PipelineDef
from sluice.dag.eligibility import evaluate_run_gating


def _pipeline(**kw):
    base = dict(
        name="P",
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


async def _seed(conn, run_key, pipeline_name, status, anchor):
    await conn.execute(
        "INSERT INTO pipelines(run_key, task, pipeline_name, cycle_anchor, "
        "trigger, started_at, status, finished_at) VALUES "
        "(?, 't', ?, ?, 'manual', ?, ?, ?)",
        (run_key, pipeline_name, anchor, anchor, status, anchor),
    )


@pytest.mark.asyncio
async def test_gating_passes_when_no_upstream(active_db):
    decision = await evaluate_run_gating(active_db, _pipeline())
    assert decision.proceed


@pytest.mark.asyncio
async def test_gating_blocks_when_required_upstream_never_succeeded(active_db):
    decision = await evaluate_run_gating(active_db, _pipeline(upstream_all=["U"]))
    assert not decision.proceed
    assert "U" in decision.reason


@pytest.mark.asyncio
async def test_gating_passes_when_upstream_has_success(active_db):
    await _seed(active_db, "rk_u", "U", "success", "2026-05-04T00:00:00+00:00")
    await active_db.commit()
    decision = await evaluate_run_gating(active_db, _pipeline(upstream_all=["U"]))
    assert decision.proceed
