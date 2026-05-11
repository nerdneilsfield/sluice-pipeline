from __future__ import annotations

import sqlite3
from collections.abc import Iterable

import aiosqlite

from sluice.dag._name import safe_name
from sluice.dag.config import PipelineDef, TaskConfig
from sluice.dag.rotation.partition import Partition
from sluice.dag.stage_driver import VOLATILE_TYPES

_CHUNK = 400


def _chunked(items: Iterable) -> Iterable[list]:
    buf: list = []
    for item in items:
        buf.append(item)
        if len(buf) >= _CHUNK:
            yield buf
            buf = []
    if buf:
        yield buf


async def copy_carryover(
    new_conn: aiosqlite.Connection,
    *,
    old_path: str,
    partition: Partition,
    task: TaskConfig,
) -> None:
    await new_conn.execute("ATTACH DATABASE ? AS old", (old_path,))
    # Do not DETACH here: copy_carryover intentionally leaves commit ownership to
    # the caller, and SQLite cannot DETACH while the write transaction is open.
    await new_conn.execute(
        "INSERT OR IGNORE INTO main.attachment_mirror "
        "SELECT * FROM old.attachment_mirror"
    )

    if not partition.carryover_ids:
        return

    items_per_pipeline = _items_per_pipeline(partition)
    stage_def_by_name = {stage.name: stage for stage in task.stages}

    await _copy_items(new_conn, partition.carryover_ids)

    pipeline_ids: set[int] = set()
    stage_ids: set[int] = set()
    for pipeline in task.pipelines:
        ids = items_per_pipeline.get(pipeline.name, set())
        if not ids:
            continue
        safe_pipeline = safe_name(pipeline.name)

        for chunk in _chunked(ids):
            placeholders = ",".join("?" for _ in chunk)
            rows = await _select_or_empty(
                new_conn,
                f"SELECT DISTINCT last_run_id FROM old.pipeline_{safe_pipeline} "
                f"WHERE item_id IN ({placeholders})",
                tuple(chunk),
            )
            pipeline_ids.update(row[0] for row in rows if row[0] is not None)

        if pipeline.sinks:
            for chunk in _chunked(ids):
                placeholders = ",".join("?" for _ in chunk)
                rows = await _select_or_empty(
                    new_conn,
                    "SELECT DISTINCT p.id "
                    f"FROM old.pipeline_{safe_pipeline}_sink_pending sp "
                    "JOIN old.pipelines p ON p.run_key = sp.run_key "
                    f"WHERE sp.item_id IN ({placeholders})",
                    tuple(chunk),
                )
                pipeline_ids.update(row[0] for row in rows if row[0] is not None)

        for stage_name in pipeline.stages:
            stage_def = stage_def_by_name[stage_name]
            if stage_def.type in VOLATILE_TYPES:
                continue
            safe_stage = safe_name(stage_name)
            for chunk in _chunked(ids):
                placeholders = ",".join("?" for _ in chunk)
                rows = await _select_or_empty(
                    new_conn,
                    "SELECT DISTINCT last_pipeline_run_id, last_stage_run_id "
                    f"FROM old.stage_{safe_stage} "
                    f"WHERE pipeline_name = ? AND item_id IN ({placeholders})",
                    (pipeline.name, *chunk),
                )
                for row in rows:
                    if row[0] is not None:
                        pipeline_ids.add(row[0])
                    if row[1] is not None:
                        stage_ids.add(row[1])

        for chunk in _chunked(ids):
            placeholders = ",".join("?" for _ in chunk)
            rows = await _select_or_empty(
                new_conn,
                "SELECT DISTINCT pipeline_run_id, stage_run_id "
                "FROM old.stage_attempt_log "
                f"WHERE pipeline_name = ? AND item_id IN ({placeholders})",
                (pipeline.name, *chunk),
            )
            for row in rows:
                if row[0] is not None:
                    pipeline_ids.add(row[0])
                if row[1] is not None:
                    stage_ids.add(row[1])

    for chunk in _chunked(stage_ids):
        placeholders = ",".join("?" for _ in chunk)
        cur = await new_conn.execute(
            f"SELECT DISTINCT pipeline_run_id FROM old.stages "
            f"WHERE id IN ({placeholders})",
            tuple(chunk),
        )
        pipeline_ids.update(row[0] for row in await cur.fetchall())

    await _copy_pipeline_masters(new_conn, pipeline_ids)
    await _copy_stage_masters(new_conn, stage_ids)

    for pipeline in task.pipelines:
        ids = items_per_pipeline.get(pipeline.name, set())
        if not ids:
            continue
        await _copy_pipeline_state(new_conn, pipeline, ids)
        if pipeline.sinks:
            await _copy_sink_pending(new_conn, pipeline, ids)
        for stage_name in pipeline.stages:
            stage_def = stage_def_by_name[stage_name]
            if stage_def.type in VOLATILE_TYPES:
                continue
            await _copy_stage_state(new_conn, stage_name, pipeline.name, ids)
            await _copy_stage_attempt_log(new_conn, stage_name, pipeline.name, ids)

    await _copy_referenced_sink_rows(new_conn)


def _items_per_pipeline(partition: Partition) -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    for item_id in partition.carryover_ids:
        for pipeline_name in partition.item_reach[item_id]:
            out.setdefault(pipeline_name, set()).add(item_id)
    return out


async def _select_or_empty(conn: aiosqlite.Connection, sql: str, params: tuple) -> list:
    try:
        cur = await conn.execute(sql, params)
        return await cur.fetchall()
    except sqlite3.OperationalError as exc:
        if "no such table" not in str(exc):
            raise
        return []


async def _copy_items(conn: aiosqlite.Connection, ids: set[str]) -> None:
    for chunk in _chunked(ids):
        placeholders = ",".join("?" for _ in chunk)
        await conn.execute(
            f"INSERT OR IGNORE INTO main.items "
            f"SELECT * FROM old.items WHERE item_id IN ({placeholders})",
            tuple(chunk),
        )


async def _copy_pipeline_masters(conn: aiosqlite.Connection, ids: set[int]) -> None:
    for chunk in _chunked(ids):
        placeholders = ",".join("?" for _ in chunk)
        await conn.execute(
            f"INSERT OR IGNORE INTO main.pipelines "
            f"SELECT * FROM old.pipelines WHERE id IN ({placeholders})",
            tuple(chunk),
        )


async def _copy_stage_masters(conn: aiosqlite.Connection, ids: set[int]) -> None:
    for chunk in _chunked(ids):
        placeholders = ",".join("?" for _ in chunk)
        await conn.execute(
            f"INSERT OR IGNORE INTO main.stages "
            f"SELECT * FROM old.stages WHERE id IN ({placeholders})",
            tuple(chunk),
        )


async def _copy_pipeline_state(
    conn: aiosqlite.Connection, pipeline: PipelineDef, ids: set[str]
) -> None:
    safe_pipeline = safe_name(pipeline.name)
    for chunk in _chunked(ids):
        placeholders = ",".join("?" for _ in chunk)
        try:
            await conn.execute(
                f"INSERT OR IGNORE INTO main.pipeline_{safe_pipeline} "
                f"SELECT * FROM old.pipeline_{safe_pipeline} "
                f"WHERE item_id IN ({placeholders})",
                tuple(chunk),
            )
        except sqlite3.OperationalError as exc:
            if "no such table" not in str(exc):
                raise


async def _copy_sink_pending(
    conn: aiosqlite.Connection, pipeline: PipelineDef, ids: set[str]
) -> None:
    safe_pipeline = safe_name(pipeline.name)
    for chunk in _chunked(ids):
        placeholders = ",".join("?" for _ in chunk)
        try:
            await conn.execute(
                f"INSERT OR IGNORE INTO main.pipeline_{safe_pipeline}_sink_pending "
                f"SELECT * FROM old.pipeline_{safe_pipeline}_sink_pending "
                f"WHERE item_id IN ({placeholders})",
                tuple(chunk),
            )
        except sqlite3.OperationalError as exc:
            if "no such table" not in str(exc):
                raise


async def _copy_stage_state(
    conn: aiosqlite.Connection, stage_name: str, pipeline_name: str, ids: set[str]
) -> None:
    safe_stage = safe_name(stage_name)
    for chunk in _chunked(ids):
        placeholders = ",".join("?" for _ in chunk)
        try:
            await conn.execute(
                f"INSERT OR IGNORE INTO main.stage_{safe_stage} "
                f"SELECT * FROM old.stage_{safe_stage} "
                f"WHERE pipeline_name = ? AND item_id IN ({placeholders})",
                (pipeline_name, *chunk),
            )
        except sqlite3.OperationalError as exc:
            if "no such table" not in str(exc):
                raise


async def _copy_stage_attempt_log(
    conn: aiosqlite.Connection, stage_name: str, pipeline_name: str, ids: set[str]
) -> None:
    for chunk in _chunked(ids):
        placeholders = ",".join("?" for _ in chunk)
        await conn.execute(
            "INSERT OR IGNORE INTO main.stage_attempt_log "
            "SELECT * FROM old.stage_attempt_log "
            "WHERE pipeline_name = ? AND stage_name = ? "
            f"AND item_id IN ({placeholders})",
            (pipeline_name, stage_name, *chunk),
        )


async def _copy_referenced_sink_rows(conn: aiosqlite.Connection) -> None:
    cur = await conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' "
        "AND name LIKE 'pipeline\\_%\\_sink\\_pending' ESCAPE '\\'"
    )
    pending_tables = [row[0] for row in await cur.fetchall()]
    run_keys: set[str] = set()
    # Plan 4 zombie cleanup may increase this set; batch per pending table if it
    # becomes high-cardinality in real deployments.
    for table in pending_tables:
        cur = await conn.execute(f"SELECT DISTINCT run_key FROM main.{table}")
        run_keys.update(row[0] for row in await cur.fetchall())

    for chunk in _chunked(run_keys):
        placeholders = ",".join("?" for _ in chunk)
        await conn.execute(
            f"INSERT OR IGNORE INTO main.sink_emissions "
            f"SELECT * FROM old.sink_emissions WHERE run_key IN ({placeholders})",
            tuple(chunk),
        )
        await conn.execute(
            f"INSERT OR IGNORE INTO main.sink_delivery_log "
            f"SELECT * FROM old.sink_delivery_log WHERE run_key IN ({placeholders})",
            tuple(chunk),
        )
