from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

import aiosqlite

from sluice.dag._name import safe_name


class _PipelineLike(Protocol):
    name: str
    sinks: object


class _StageLike(Protocol):
    name: str


class _TaskLike(Protocol):
    pipelines: Sequence[_PipelineLike]
    stages: Sequence[_StageLike]


async def ensure_pipeline_table(
    conn: aiosqlite.Connection, pipeline_name: str
) -> None:
    safe_pipeline = safe_name(pipeline_name)
    await conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS pipeline_{safe_pipeline} (
          item_id      TEXT PRIMARY KEY,
          last_run_id  INTEGER NOT NULL REFERENCES pipelines(id),
          status       TEXT NOT NULL CHECK (status IN ('success', 'dropped', 'failed')),
          processed_at TEXT NOT NULL,
          error_msg    TEXT
        )
        """
    )
    await conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_pipeline_{safe_pipeline}_status "
        f"ON pipeline_{safe_pipeline}(status)"
    )


async def ensure_pipeline_sink_pending_table(
    conn: aiosqlite.Connection, pipeline_name: str
) -> None:
    safe_pipeline = safe_name(pipeline_name)
    await conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS pipeline_{safe_pipeline}_sink_pending (
          item_id       TEXT PRIMARY KEY,
          run_key       TEXT NOT NULL,
          pending_since TEXT NOT NULL
        )
        """
    )
    await conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_pipeline_{safe_pipeline}_sink_pending_runkey "
        f"ON pipeline_{safe_pipeline}_sink_pending(run_key)"
    )


async def ensure_stage_table(conn: aiosqlite.Connection, stage_name: str) -> None:
    safe_stage = safe_name(stage_name)
    status_check = "CHECK (status IN ('done', 'failed', 'dropped', 'skipped'))"
    await conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS stage_{safe_stage} (
          pipeline_name        TEXT NOT NULL,
          item_id              TEXT NOT NULL,
          status               TEXT NOT NULL {status_check},
          attempts             INTEGER NOT NULL DEFAULT 0,
          last_stage_run_id    INTEGER NOT NULL REFERENCES stages(id),
          last_pipeline_run_id INTEGER NOT NULL REFERENCES pipelines(id),
          last_processed_at    TEXT NOT NULL,
          last_error_class     TEXT,
          last_error_msg       TEXT,
          PRIMARY KEY (pipeline_name, item_id)
        )
        """
    )
    await conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_stage_{safe_stage}_item "
        f"ON stage_{safe_stage}(item_id)"
    )
    await conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_stage_{safe_stage}_run "
        f"ON stage_{safe_stage}(last_pipeline_run_id)"
    )
    await conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_stage_{safe_stage}_status "
        f"ON stage_{safe_stage}(status)"
    )


async def migrate_task_dynamic_tables(
    conn: aiosqlite.Connection, task: _TaskLike
) -> None:
    for pipeline in task.pipelines:
        await ensure_pipeline_table(conn, pipeline.name)
        if pipeline.sinks:
            await ensure_pipeline_sink_pending_table(conn, pipeline.name)
    for stage in task.stages:
        await ensure_stage_table(conn, stage.name)
    await conn.commit()
