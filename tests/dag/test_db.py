from __future__ import annotations

from pathlib import Path

import pytest

from sluice.dag import db


async def _table_names(conn) -> set[str]:
    cur = await conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    return {row[0] for row in await cur.fetchall()}


@pytest.mark.asyncio
async def test_open_active_db_creates_base_schema(tmp_path: Path):
    db_path = tmp_path / "active.db"

    async with db.open_active_db(str(db_path)) as conn:
        names = await _table_names(conn)

    expected = {
        "items",
        "pipelines",
        "stages",
        "stage_attempt_log",
        "sink_emissions",
        "sink_delivery_log",
        "attachment_mirror",
        "gc_log",
        "rotation_gate",
        "pipeline_admissions",
        "schema_version",
    }
    assert expected <= names


@pytest.mark.asyncio
async def test_open_seen_db_creates_seen_schema(tmp_path: Path):
    db_path = tmp_path / "seen.db"

    async with db.open_seen_db(str(db_path)) as conn:
        names = await _table_names(conn)
        cur = await conn.execute("PRAGMA table_info(item_seen)")
        columns = {row[1] for row in await cur.fetchall()}

    assert {"item_seen", "schema_version"} <= names
    assert {"item_id", "source_hash", "archived_at"} <= columns


@pytest.mark.asyncio
async def test_open_db_is_idempotent(tmp_path: Path):
    db_path = tmp_path / "active.db"

    async with db.open_active_db(str(db_path)):
        pass

    async with db.open_active_db(str(db_path)) as conn:
        cur = await conn.execute("SELECT MAX(version) FROM schema_version")
        row = await cur.fetchone()

    assert row == (2,)


@pytest.mark.asyncio
async def test_open_db_sets_pragmas(tmp_path: Path):
    db_path = tmp_path / "active.db"

    async with db.open_active_db(str(db_path)) as conn:
        cur = await conn.execute("PRAGMA foreign_keys")
        foreign_keys = (await cur.fetchone())[0]
        cur = await conn.execute("PRAGMA journal_mode")
        journal_mode = (await cur.fetchone())[0]

    assert foreign_keys == 1
    assert journal_mode.lower() == "wal"


@pytest.mark.asyncio
async def test_migrations_are_filtered_by_kind_and_pick_up_future_versions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    fake_dir = tmp_path / "migrations"
    fake_dir.mkdir()
    real_active = (db._MIGRATIONS_DIR / "v2_0001_active_init.sql").read_text()
    real_seen = (db._MIGRATIONS_DIR / "v2_0001_seen_init.sql").read_text()
    (fake_dir / "v2_0001_active_init.sql").write_text(real_active)
    (fake_dir / "v2_0001_seen_init.sql").write_text(real_seen)
    (fake_dir / "v2_0002_active_future.sql").write_text(
        "ALTER TABLE items ADD COLUMN future_col TEXT;\n"
        "INSERT INTO schema_version (version, applied_at) VALUES (2, CURRENT_TIMESTAMP);\n"
    )
    (fake_dir / "v2_0002_seen_future.sql").write_text(
        "ALTER TABLE item_seen ADD COLUMN seen_future_col TEXT;\n"
        "INSERT INTO schema_version (version, applied_at) VALUES (2, CURRENT_TIMESTAMP);\n"
    )
    monkeypatch.setattr(db, "_MIGRATIONS_DIR", fake_dir)

    async with db.open_active_db(str(tmp_path / "active.db")) as conn:
        cur = await conn.execute("PRAGMA table_info(items)")
        active_cols = {row[1] for row in await cur.fetchall()}
        cur = await conn.execute("SELECT MAX(version) FROM schema_version")
        active_version = (await cur.fetchone())[0]

    async with db.open_seen_db(str(tmp_path / "seen.db")) as conn:
        cur = await conn.execute("PRAGMA table_info(item_seen)")
        seen_cols = {row[1] for row in await cur.fetchall()}
        cur = await conn.execute("SELECT MAX(version) FROM schema_version")
        seen_version = (await cur.fetchone())[0]

    assert "future_col" in active_cols
    assert "seen_future_col" not in active_cols
    assert active_version == 2
    assert "seen_future_col" in seen_cols
    assert "future_col" not in seen_cols
    assert seen_version == 2
