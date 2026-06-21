from __future__ import annotations

import contextlib
from collections.abc import Sequence
from typing import Protocol

import aiosqlite

from sluice.core.errors import UpgradeError
from sluice.dag._name import safe_name

_DYNAMIC_V3_VERSION = 3


class _PipelineLike(Protocol):
    name: str
    sinks: object


class _StageLike(Protocol):
    name: str


class _TaskLike(Protocol):
    pipelines: Sequence[_PipelineLike]
    stages: Sequence[_StageLike]


async def ensure_pipeline_table(conn: aiosqlite.Connection, pipeline_name: str) -> None:
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
        f"CREATE INDEX IF NOT EXISTS idx_stage_{safe_stage}_item ON stage_{safe_stage}(item_id)"
    )
    await conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_stage_{safe_stage}_run "
        f"ON stage_{safe_stage}(last_pipeline_run_id)"
    )
    await conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_stage_{safe_stage}_status ON stage_{safe_stage}(status)"
    )


async def migrate_task_dynamic_tables(conn: aiosqlite.Connection, task: _TaskLike) -> None:
    for pipeline in task.pipelines:
        await ensure_pipeline_table(conn, pipeline.name)
        if pipeline.sinks:
            await ensure_pipeline_sink_pending_table(conn, pipeline.name)
    for stage in task.stages:
        await ensure_stage_table(conn, stage.name)
    await conn.commit()


# ---------------------------------------------------------------------------
# v3 dynamic table upgrade
# ---------------------------------------------------------------------------


async def _table_exists(conn: aiosqlite.Connection, table: str) -> bool:
    cur = await conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    )
    return await cur.fetchone() is not None


async def _dynamic_version(conn: aiosqlite.Connection, table: str) -> int:
    cur = await conn.execute(
        "SELECT version FROM dynamic_schema_version WHERE table_name = ?", (table,)
    )
    row = await cur.fetchone()
    return int(row[0]) if row else 0


async def _set_dynamic_version(conn: aiosqlite.Connection, table: str, version: int) -> None:
    await conn.execute(
        "INSERT INTO dynamic_schema_version (table_name, version, migrated_at, checksum) "
        "VALUES (?, ?, CURRENT_TIMESTAMP, NULL) "
        "ON CONFLICT(table_name) DO UPDATE SET version=excluded.version, "
        "migrated_at=excluded.migrated_at",
        (table, version),
    )


async def _assert_sink_pending_empty(
    conn: aiosqlite.Connection, table: str
) -> None:
    if not await _table_exists(conn, table):
        return
    cur = await conn.execute(f"SELECT COUNT(*) FROM {table}")
    count = (await cur.fetchone())[0]
    if count:
        raise UpgradeError(
            f"{table} has {count} pending rows; drain or clear before v3 upgrade"
        )


async def _rebuild_pipeline_table(conn: aiosqlite.Connection, table: str) -> None:
    """Rebuild one pipeline_<safe> table to v3 run-scoped form. Atomic per table."""
    new_table = f"{table}__new"
    old_table = f"{table}__old"

    # Clean up any leftover temp tables from a prior failed attempt.
    for stale in (new_table, old_table):
        if await _table_exists(conn, stale):
            await conn.execute(f"DROP TABLE {stale}")

    await conn.execute(
        f"""
        CREATE TABLE {new_table} (
          run_id       INTEGER NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
          item_id      TEXT NOT NULL,
          cycle_id     TEXT NOT NULL,
          status       TEXT NOT NULL CHECK (status IN ('success', 'dropped', 'failed')),
          processed_at TEXT NOT NULL,
          error_msg    TEXT,
          PRIMARY KEY (run_id, item_id)
        )
        """
    )

    # Copy rows; fail closed if any last_run_id does not resolve to a pipelines row.
    cur = await conn.execute(
        f"SELECT item_id, last_run_id, status, processed_at, error_msg FROM {table}"
    )
    rows = await cur.fetchall()
    for item_id, last_run_id, status, processed_at, error_msg in rows:
        run_cur = await conn.execute(
            "SELECT id, cycle_anchor FROM pipelines WHERE id = ?", (last_run_id,)
        )
        run_row = await run_cur.fetchone()
        if run_row is None:
            raise UpgradeError(
                f"{table} item {item_id!r} references missing run row id={last_run_id}"
            )
        _run_id, cycle_anchor = run_row
        cycle_id = cycle_anchor or ""
        await conn.execute(
            f"INSERT INTO {new_table} (run_id, item_id, cycle_id, status, processed_at, error_msg) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (last_run_id, item_id, cycle_id, status, processed_at, error_msg),
        )

    await conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{new_table}_cycle ON {new_table}(cycle_id, item_id)"
    )

    cur = await conn.execute("PRAGMA foreign_key_check")
    violations = await cur.fetchall()
    if violations:
        raise UpgradeError(f"foreign_key_check failed after rebuilding {table}: {violations}")

    await conn.execute(f"ALTER TABLE {table} RENAME TO {old_table}")
    await conn.execute(f"ALTER TABLE {new_table} RENAME TO {table}")
    await _set_dynamic_version(conn, table, _DYNAMIC_V3_VERSION)
    await conn.execute(f"DROP TABLE {old_table}")


async def _rebuild_stage_table(conn: aiosqlite.Connection, table: str) -> None:
    """Rebuild one stage_<safe> table to v3 run-scoped form. Atomic per table."""
    new_table = f"{table}__new"
    old_table = f"{table}__old"

    for stale in (new_table, old_table):
        if await _table_exists(conn, stale):
            await conn.execute(f"DROP TABLE {stale}")

    await conn.execute(
        f"""
        CREATE TABLE {new_table} (
          run_id           INTEGER NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
          pipeline_name    TEXT NOT NULL,
          item_id          TEXT NOT NULL,
          cycle_id         TEXT NOT NULL,
          status           TEXT NOT NULL CHECK (status IN ('done', 'failed', 'dropped', 'skipped')),
          attempts         INTEGER NOT NULL DEFAULT 0,
          last_stage_run_id INTEGER NOT NULL REFERENCES stages(id),
          last_processed_at TEXT NOT NULL,
          last_error_class TEXT,
          last_error_msg   TEXT,
          PRIMARY KEY (run_id, pipeline_name, item_id)
        )
        """
    )

    cur = await conn.execute(
        f"SELECT pipeline_name, item_id, status, attempts, last_stage_run_id, "
        f"last_pipeline_run_id, last_processed_at, last_error_class, last_error_msg FROM {table}"
    )
    rows = await cur.fetchall()
    for (
        pipeline_name,
        item_id,
        status,
        attempts,
        last_stage_run_id,
        last_pipeline_run_id,
        last_processed_at,
        last_error_class,
        last_error_msg,
    ) in rows:
        run_cur = await conn.execute(
            "SELECT id, cycle_anchor FROM pipelines WHERE id = ?",
            (last_pipeline_run_id,),
        )
        run_row = await run_cur.fetchone()
        if run_row is None:
            raise UpgradeError(
                f"{table} {pipeline_name}:{item_id} references missing run row "
                f"id={last_pipeline_run_id}"
            )
        _run_id, cycle_anchor = run_row
        cycle_id = cycle_anchor or ""
        await conn.execute(
            f"INSERT INTO {new_table} (run_id, pipeline_name, item_id, cycle_id, status, "
            "attempts, last_stage_run_id, last_processed_at, last_error_class, last_error_msg) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                last_pipeline_run_id,
                pipeline_name,
                item_id,
                cycle_id,
                status,
                attempts,
                last_stage_run_id,
                last_processed_at,
                last_error_class,
                last_error_msg,
            ),
        )

    await conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{new_table}_cycle "
        f"ON {new_table}(pipeline_name, cycle_id, item_id)"
    )
    await conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{new_table}_item ON {new_table}(item_id)"
    )

    cur = await conn.execute("PRAGMA foreign_key_check")
    violations = await cur.fetchall()
    if violations:
        raise UpgradeError(f"foreign_key_check failed after rebuilding {table}: {violations}")

    await conn.execute(f"ALTER TABLE {table} RENAME TO {old_table}")
    await conn.execute(f"ALTER TABLE {new_table} RENAME TO {table}")
    await _set_dynamic_version(conn, table, _DYNAMIC_V3_VERSION)
    await conn.execute(f"DROP TABLE {old_table}")


async def _backfill_stage_attempt_log(
    conn: aiosqlite.Connection, task: _TaskLike
) -> None:
    """Populate run_id and cycle_id on stage_attempt_log rows added before v3.

    Rows already carrying a non-null run_id are skipped. Rows whose
    pipeline_run_id no longer exists in pipelines cannot be joined and cause
    a fail-closed UpgradeError.
    """
    cur = await conn.execute(
        "SELECT id, pipeline_run_id FROM stage_attempt_log WHERE run_id IS NULL"
    )
    pending = await cur.fetchall()
    for log_id, pipeline_run_id in pending:
        run_cur = await conn.execute(
            "SELECT id, cycle_anchor FROM pipelines WHERE id = ?", (pipeline_run_id,)
        )
        run_row = await run_cur.fetchone()
        if run_row is None:
            raise UpgradeError(
                f"stage_attempt_log id={log_id} references missing "
                f"pipeline_run_id={pipeline_run_id}"
            )
        _id, cycle_anchor = run_row
        await conn.execute(
            "UPDATE stage_attempt_log SET run_id = ?, cycle_id = ? WHERE id = ?",
            (pipeline_run_id, cycle_anchor or "", log_id),
        )


async def upgrade_task_dynamic_tables(
    conn: aiosqlite.Connection, task: _TaskLike
) -> None:
    """Rebuild per-pipeline and per-stage dynamic tables to v3 run-scoped form.

    Idempotent. Atomic per table. Fails closed on:
    - non-empty pipeline_<safe>_sink_pending tables
    - any dynamic row whose referenced run no longer exists in pipelines
    - any stage_attempt_log row whose pipeline_run_id no longer exists

    Must be called after apply_v3_migrations(conn). Normal startup never calls
    this; the upgrade service does.
    """
    for pipeline in task.pipelines:
        safe = safe_name(pipeline.name)
        if pipeline.sinks:
            await _assert_sink_pending_empty(conn, f"pipeline_{safe}_sink_pending")

    for pipeline in task.pipelines:
        safe = safe_name(pipeline.name)
        table = f"pipeline_{safe}"
        if not await _table_exists(conn, table):
            continue
        if await _dynamic_version(conn, table) >= _DYNAMIC_V3_VERSION:
            continue
        await conn.execute("BEGIN IMMEDIATE")
        try:
            await _rebuild_pipeline_table(conn, table)
            await conn.commit()
        except BaseException:
            with contextlib.suppress(Exception):
                await conn.execute("ROLLBACK")
            raise

    for stage in task.stages:
        safe = safe_name(stage.name)
        table = f"stage_{safe}"
        if not await _table_exists(conn, table):
            continue
        if await _dynamic_version(conn, table) >= _DYNAMIC_V3_VERSION:
            continue
        await conn.execute("BEGIN IMMEDIATE")
        try:
            await _rebuild_stage_table(conn, table)
            await conn.commit()
        except BaseException:
            with contextlib.suppress(Exception):
                await conn.execute("ROLLBACK")
            raise

    await conn.execute("BEGIN IMMEDIATE")
    try:
        await _backfill_stage_attempt_log(conn, task)
        await conn.commit()
    except BaseException:
        with contextlib.suppress(Exception):
            await conn.execute("ROLLBACK")
        raise
