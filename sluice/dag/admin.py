from __future__ import annotations

import json
import tomllib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import aiosqlite

from sluice.core.errors import ConfigError
from sluice.dag._name import safe_name
from sluice.dag.config import TaskConfig
from sluice.dag.db import apply_migrations
from sluice.dag.dynamic_tables import migrate_task_dynamic_tables
from sluice.dag.validator import ValidationError, validate_task


def _validate_admin_task(task: TaskConfig) -> None:
    try:
        validate_task(task)
    except ValidationError as exc:
        raise ConfigError(f"invalid task {task.name!r}: {exc}") from exc


async def migrate_task(task: TaskConfig) -> None:
    """Create both DBs and apply schema plus dynamic per-pipeline/stage tables."""
    _validate_admin_task(task)
    Path(task.db_path).parent.mkdir(parents=True, exist_ok=True)
    Path(task.seen_db).parent.mkdir(parents=True, exist_ok=True)

    async with aiosqlite.connect(task.db_path) as active:
        await apply_migrations(active, "active")
        await migrate_task_dynamic_tables(active, task)
    async with aiosqlite.connect(task.seen_db) as seen:
        await apply_migrations(seen, "seen")


def _pipeline_safe_name(task: TaskConfig, pipeline: str) -> str:
    pipeline_names = {p.name for p in task.pipelines}
    if pipeline not in pipeline_names:
        raise ConfigError(f"pipeline {pipeline!r} not defined in task {task.name!r}")
    return safe_name(pipeline)


async def retry_pipeline(task: TaskConfig, *, pipeline: str, item_id: str | None) -> int:
    """Delete terminal pipeline/stage rows so a future run can retry them."""
    _validate_admin_task(task)
    pipeline_safe = _pipeline_safe_name(task, pipeline)
    stage_safe_names = [safe_name(stage.name) for stage in task.stages]
    pipeline_table = f"pipeline_{pipeline_safe}"

    async with aiosqlite.connect(task.db_path) as db:
        if item_id is None:
            cur = await db.execute(f"DELETE FROM {pipeline_table}")
            deleted = cur.rowcount or 0
            for stage_name in stage_safe_names:
                await db.execute(
                    f"DELETE FROM stage_{stage_name} WHERE pipeline_name = ?",
                    (pipeline,),
                )
        else:
            cur = await db.execute(
                f"DELETE FROM {pipeline_table} WHERE item_id = ?",
                (item_id,),
            )
            deleted = cur.rowcount or 0
            for stage_name in stage_safe_names:
                await db.execute(
                    f"DELETE FROM stage_{stage_name} WHERE pipeline_name = ? AND item_id = ?",
                    (pipeline, item_id),
                )
        await db.commit()
    return deleted


async def list_items(task: TaskConfig, *, limit: int = 50) -> list[dict[str, Any]]:
    """Return recent items joined with per-pipeline status."""
    _validate_admin_task(task)
    pipeline_pairs = [(pipeline.name, safe_name(pipeline.name)) for pipeline in task.pipelines]
    select_pipeline_cols = ", ".join(
        f"(SELECT status FROM pipeline_{safe} WHERE item_id = items.item_id) AS p_{safe}"
        for _name, safe in pipeline_pairs
    )
    query = (
        "SELECT item_id, source_name, title, ingested_at"
        + (", " + select_pipeline_cols if pipeline_pairs else "")
        + " FROM items ORDER BY ingested_at DESC LIMIT ?"
    )

    async with aiosqlite.connect(task.db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(query, (limit,))
        out = []
        async for row_data in cur:
            row = {
                "item_id": row_data["item_id"],
                "source_name": row_data["source_name"],
                "title": row_data["title"],
                "ingested_at": row_data["ingested_at"],
            }
            for name, safe in pipeline_pairs:
                row[name] = row_data[f"p_{safe}"]
            out.append(row)
        return out


async def show_item(task: TaskConfig, *, item_id: str) -> dict[str, Any]:
    _validate_admin_task(task)
    async with aiosqlite.connect(task.db_path) as db:
        db.row_factory = aiosqlite.Row
        item_row = await (
            await db.execute("SELECT * FROM items WHERE item_id = ?", (item_id,))
        ).fetchone()
        if item_row is None:
            raise KeyError(item_id)

        pipelines_section: dict[str, dict[str, Any]] = {}
        for pipeline in task.pipelines:
            safe = safe_name(pipeline.name)
            row = await (
                await db.execute(f"SELECT * FROM pipeline_{safe} WHERE item_id = ?", (item_id,))
            ).fetchone()
            if row is not None:
                pipelines_section[pipeline.name] = dict(row)

        stages_section: dict[str, list[dict[str, Any]]] = {}
        for stage in task.stages:
            safe = safe_name(stage.name)
            cur = await db.execute(
                f"SELECT * FROM stage_{safe} WHERE item_id = ? ORDER BY last_processed_at",
                (item_id,),
            )
            rows = [dict(row) async for row in cur]
            if rows:
                stages_section[stage.name] = rows

        return {"item": dict(item_row), "pipelines": pipelines_section, "stages": stages_section}


async def kill_item(
    task: TaskConfig,
    *,
    item_id: str,
    reason: str = "killed_by_operator",
) -> None:
    _validate_admin_task(task)
    async with aiosqlite.connect(task.db_path) as db:
        cur = await db.execute(
            "UPDATE items SET dropped=1, dropped_reason=?, dropped_at=? WHERE item_id=?",
            (reason, datetime.now(UTC).isoformat(timespec="seconds"), item_id),
        )
        if cur.rowcount == 0:
            raise KeyError(item_id)
        await db.commit()


def _toml_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return json.dumps(value)
    if value is None:
        raise ConfigError("unsupported TOML value: None")
    if isinstance(value, dict):
        raise ConfigError("unsupported TOML value: dict")
    if isinstance(value, list):
        return "[" + ", ".join(_toml_value(item) for item in value) + "]"
    if isinstance(value, int | float):
        return repr(value)
    raise ConfigError(f"unsupported TOML value: {type(value).__name__}")


def _toml_array(array_name: str, entries: list[dict[str, Any]]) -> str:
    chunks = []
    for entry in entries:
        chunks.append(f"[[{array_name}]]")
        for key, value in entry.items():
            chunks.append(f"{key} = {_toml_value(value)}")
        chunks.append("")
    return "\n".join(chunks)


def scaffold_from_v1(*, old_path: Path, out_dir: Path) -> None:
    """Emit starter v2 TOML files from a v1 single-file pipeline config."""
    raw = tomllib.loads(old_path.read_text())
    name = raw["id"]
    db_path = raw.get("db_path", f"data/{name}.db")
    timezone = raw.get("timezone", "UTC")
    window = raw.get("window", "24h")
    anchor = raw.get("cron", "0 8 * * *")
    sources_v1 = raw.get("sources", [])
    stages_v1 = raw.get("stages", [])
    sinks_v1 = raw.get("sinks", [])

    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "task.toml").write_text(
        f"""\
[task]
name = {_toml_value(name)}
db_path = {_toml_value(db_path)}
seen_db = {_toml_value(f"data/{name}_seen.db")}
enabled = true
timezone = {_toml_value(timezone)}

[task.window]
anchor = {_toml_value(anchor)}
length = {_toml_value(window)}

[task.deployments]
pipelines = "all"
"""
    )

    source_entries = []
    source_names = []
    for index, source in enumerate(sources_v1, start=1):
        source_name = source.get("name", f"src{index}")
        source_names.append(source_name)
        source_entries.append(
            {"name": source_name, **{key: value for key, value in source.items() if key != "name"}}
        )
    (out_dir / "sources.toml").write_text(_toml_array("sources", source_entries))

    sink_entries = []
    sink_names = []
    for sink in sinks_v1:
        sink_name = sink.get("name") or sink.get("id")
        sink_names.append(sink_name)
        sink_entries.append(
            {
                "name": sink_name,
                **{key: value for key, value in sink.items() if key not in {"id", "name"}},
            }
        )
    (out_dir / "sinks.toml").write_text(_toml_array("sinks", sink_entries))

    stage_entries = []
    stage_names = []
    for index, stage in enumerate(stages_v1, start=1):
        stage_name = stage.get("name", f"s{index}")
        stage_names.append(stage_name)
        stage_entries.append(
            {"name": stage_name, **{key: value for key, value in stage.items() if key != "name"}}
        )
    (out_dir / "stages.toml").write_text(_toml_array("stages", stage_entries))

    (out_dir / "pipelines.toml").write_text(
        f"""\
[[pipelines]]
name = {_toml_value(f"{name}_main")}
cron = {_toml_value(anchor)}
sources = {_toml_value(source_names)}
stages = {_toml_value(stage_names)}
sinks = {_toml_value(sink_names)}
"""
    )
