from __future__ import annotations

import sqlite3
import textwrap
from datetime import UTC, datetime
from pathlib import Path

import pytest

from sluice.dag.item import Item


def _row(**overrides):
    base = {
        "item_id": "i",
        "source_name": "feed",
        "guid": "g",
        "url": "https://example.test/i",
        "title": None,
        "author": None,
        "published_at": None,
        "ingested_at": "2026-05-04T00:00:00+00:00",
        "ingested_by": "P",
        "updated_at": "2026-05-04T00:00:00+00:00",
        "tag": None,
        "raw_summary": None,
        "raw_content": None,
        "raw_json": "{}",
        "fulltext": None,
        "attachments_json": None,
        "summary": None,
        "score": None,
        "tags_json": None,
        "extras_json": None,
        "dropped": 0,
        "dropped_reason": None,
        "dropped_at": None,
    }
    base.update(overrides)
    return base


def test_safe_name_accepts_identifier():
    from sluice.dag._name import safe_name

    assert safe_name("a_1") == "a_1"


@pytest.mark.parametrize("value", ["a;b", "a-b", "", "space name"])
def test_safe_name_rejects_unsafe_identifier(value):
    from sluice.dag._name import safe_name

    with pytest.raises(ValueError, match="unsafe identifier"):
        safe_name(value)


def test_source_hash_from_row_matches_seen_post_existing_formula():
    from sluice.dag._source_hash import source_hash_from_row
    from sluice.dag.phases.seen_post import _source_hash

    item = Item.from_row(_row(source_name="feed", guid="guid-1", url="https://x/u"))
    assert source_hash_from_row(item) == _source_hash(item)


def test_source_hash_from_row_falls_back_to_url_then_empty_string():
    from sluice.dag._source_hash import source_hash_from_row

    with_url = source_hash_from_row(_row(guid=None, url="https://x/u"))
    same_url = source_hash_from_row(_row(guid=None, url="https://x/u"))
    no_identity = source_hash_from_row(_row(guid=None, url=None))

    assert with_url == same_url
    assert with_url != no_identity


def make_task_dir(
    tmp_path: Path,
    *,
    name: str,
    pipeline_names: list[str],
    stage_names: list[str],
) -> Path:
    task_dir = tmp_path / "configs" / "tasks" / name
    task_dir.mkdir(parents=True, exist_ok=True)
    db_path = tmp_path / "data" / f"{name}.db"
    seen_db = tmp_path / "data" / f"{name}_seen.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)

    (task_dir / "task.toml").write_text(
        textwrap.dedent(
            f"""\
            [task]
            name = "{name}"
            db_path = "{db_path}"
            seen_db = "{seen_db}"
            enabled = true

            [task.window]
            anchor = "0 8 * * *"
            length = "24h"

            [task.deployments]
            pipelines = "all"
            """
        )
    )
    (task_dir / "sources.toml").write_text(
        textwrap.dedent(
            """\
            [[sources]]
            name = "src1"
            type = "rss"
            url = "https://example.test/rss"
            """
        )
    )
    (task_dir / "sinks.toml").write_text("")
    stage_blocks = []
    for i, stage_name in enumerate(stage_names):
        stage_type = "fetch_rss" if i == 0 else "filter"
        block = textwrap.dedent(
            f"""\
            [[stages]]
            name = "{stage_name}"
            type = "{stage_type}"
            """
        )
        if stage_type == "fetch_rss":
            block += 'sources = ["src1"]\n'
        stage_blocks.append(block)
    (task_dir / "stages.toml").write_text("\n".join(stage_blocks))
    pipeline_blocks = "\n".join(
        textwrap.dedent(
            f"""\
            [[pipelines]]
            name = "{pipeline_name}"
            cron = "0 */6 * * *"
            sources = ["src1"]
            stages = {stage_names!r}
            """
        )
        for pipeline_name in pipeline_names
    )
    (task_dir / "pipelines.toml").write_text(pipeline_blocks)
    return task_dir


def make_configs_dir(tmp_path: Path, *, tasks: dict[str, dict]) -> Path:
    for task_name, spec in tasks.items():
        make_task_dir(
            tmp_path,
            name=task_name,
            pipeline_names=spec["pipelines"],
            stage_names=spec["stages"],
        )
    return tmp_path / "configs"


async def seed_item(
    db_path: str | Path,
    *,
    item_id: str,
    source_name: str,
    title: str | None = None,
) -> None:
    import aiosqlite

    now = datetime.now(UTC).isoformat(timespec="seconds")
    async with aiosqlite.connect(str(db_path)) as db:
        await db.execute(
            "INSERT INTO items (item_id, source_name, ingested_at, ingested_by, "
            "updated_at, raw_json, title, dropped) VALUES (?, ?, ?, ?, ?, '{}', ?, 0)",
            (item_id, source_name, now, "P", now, title),
        )
        await db.commit()


def seed_item_sync(
    db_path: str | Path,
    *,
    item_id: str,
    source_name: str,
    title: str | None = None,
) -> None:
    now = datetime.now(UTC).isoformat(timespec="seconds")
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            "INSERT INTO items (item_id, source_name, ingested_at, ingested_by, "
            "updated_at, raw_json, title, dropped) VALUES (?, ?, ?, ?, ?, '{}', ?, 0)",
            (item_id, source_name, now, "P", now, title),
        )


async def seed_pipeline_state(
    db_path: str | Path,
    *,
    pipeline: str,
    item_id: str,
    status: str,
) -> None:
    import aiosqlite

    now = datetime.now(UTC).isoformat(timespec="seconds")
    async with aiosqlite.connect(str(db_path)) as db:
        await db.execute(
            f"INSERT INTO pipeline_{pipeline} (item_id, last_run_id, status, processed_at) "
            "VALUES (?, 1, ?, ?)",
            (item_id, status, now),
        )
        await db.commit()


def seed_pipeline_state_sync(
    db_path: str | Path,
    *,
    pipeline: str,
    item_id: str,
    status: str,
) -> None:
    now = datetime.now(UTC).isoformat(timespec="seconds")
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            f"INSERT INTO pipeline_{pipeline} (item_id, last_run_id, status, processed_at) "
            "VALUES (?, 1, ?, ?)",
            (item_id, status, now),
        )


async def seed_stage_state(
    db_path: str | Path,
    *,
    stage: str,
    pipeline: str,
    item_id: str,
    status: str,
    attempts: int = 1,
) -> None:
    import aiosqlite

    now = datetime.now(UTC).isoformat(timespec="seconds")
    async with aiosqlite.connect(str(db_path)) as db:
        await db.execute(
            f"INSERT INTO stage_{stage} (pipeline_name, item_id, status, attempts, "
            "last_stage_run_id, last_pipeline_run_id, last_processed_at) "
            "VALUES (?, ?, ?, ?, 1, 1, ?)",
            (pipeline, item_id, status, attempts, now),
        )
        await db.commit()


def test_make_task_dir_writes_five_files(tmp_path):
    task_dir = make_task_dir(tmp_path, name="x", pipeline_names=["p"], stage_names=["s"])
    for filename in ("task.toml", "sources.toml", "sinks.toml", "stages.toml", "pipelines.toml"):
        assert (task_dir / filename).exists()
