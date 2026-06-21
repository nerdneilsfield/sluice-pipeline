from __future__ import annotations

from pathlib import Path

import pytest

from sluice.dag import db


async def _table_columns(conn, table: str) -> set[str]:
    cur = await conn.execute(f"PRAGMA table_info({table})")
    return {row[1] for row in await cur.fetchall()}


async def _index_names(conn, table: str) -> set[str]:
    cur = await conn.execute(f"PRAGMA index_list({table})")
    return {row[1] for row in await cur.fetchall()}


async def _table_sql(conn, table: str) -> str:
    cur = await conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
    )
    row = await cur.fetchone()
    return row[0] if row else ""


@pytest.mark.asyncio
async def test_normal_startup_does_not_apply_v3_migration(tmp_path: Path):
    db_path = tmp_path / "active.db"
    async with db.open_active_db(str(db_path)) as conn:
        cur = await conn.execute("SELECT MAX(version) FROM schema_version")
        version = (await cur.fetchone())[0]
        cur = await conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name IN "
            "('sink_outbox', 'rotation_manifest', 'dynamic_schema_version')"
        )
        v3_tables = {row[0] for row in await cur.fetchall()}
    assert version == 2
    assert v3_tables == set()


@pytest.mark.asyncio
async def test_apply_v3_migrations_adds_pipelines_lease_columns(tmp_path: Path):
    async with db.open_active_db(str(tmp_path / "active.db")) as conn:
        await db.apply_v3_migrations(conn)
        columns = await _table_columns(conn, "pipelines")
    assert {"cycle_id", "lease_until", "heartbeat_at", "lease_owner", "lease_version"} <= columns


@pytest.mark.asyncio
async def test_apply_v3_migrations_adds_admissions_lease_columns(tmp_path: Path):
    async with db.open_active_db(str(tmp_path / "active.db")) as conn:
        await db.apply_v3_migrations(conn)
        columns = await _table_columns(conn, "pipeline_admissions")
    assert {"run_id", "lease_until", "heartbeat_at", "lease_owner", "lease_version"} <= columns


@pytest.mark.asyncio
async def test_apply_v3_migrations_backfills_cycle_id_from_cycle_anchor(tmp_path: Path):
    async with db.open_active_db(str(tmp_path / "active.db")) as conn:
        await conn.execute(
            "INSERT INTO pipelines(run_key, task, pipeline_name, cycle_anchor, trigger, "
            "started_at, status) VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("rk1", "t", "p", "2026-06-21T00:00:00+00:00", "cron", "now", "success"),
        )
        await conn.commit()
        await db.apply_v3_migrations(conn)
        cur = await conn.execute("SELECT cycle_id FROM pipelines WHERE run_key = ?", ("rk1",))
        row = await cur.fetchone()
    assert row is not None
    assert row[0] == "2026-06-21T00:00:00+00:00"


@pytest.mark.asyncio
async def test_apply_v3_migrations_adds_stage_attempt_log_run_scoping(tmp_path: Path):
    async with db.open_active_db(str(tmp_path / "active.db")) as conn:
        await db.apply_v3_migrations(conn)
        columns = await _table_columns(conn, "stage_attempt_log")
        sql = await _table_sql(conn, "stage_attempt_log")
        indexes = await _index_names(conn, "stage_attempt_log")
    assert {"cycle_id", "run_id"} <= columns
    assert "REFERENCES pipelines(id)" in sql
    assert "ON DELETE CASCADE" in sql
    assert "idx_attempt_log_run_scoped" in indexes


@pytest.mark.asyncio
async def test_apply_v3_migrations_creates_dynamic_schema_version(tmp_path: Path):
    async with db.open_active_db(str(tmp_path / "active.db")) as conn:
        await db.apply_v3_migrations(conn)
        sql = await _table_sql(conn, "dynamic_schema_version")
        cur = await conn.execute("PRAGMA table_info(dynamic_schema_version)")
        cols = {row[1]: row[2] for row in await cur.fetchall()}
    assert cols == {
        "table_name": "TEXT",
        "version": "INTEGER",
        "migrated_at": "TEXT",
        "checksum": "TEXT",
    }
    assert "PRIMARY KEY" in sql.upper()


@pytest.mark.asyncio
async def test_apply_v3_migrations_creates_sink_outbox(tmp_path: Path):
    async with db.open_active_db(str(tmp_path / "active.db")) as conn:
        await db.apply_v3_migrations(conn)
        cols = await _table_columns(conn, "sink_outbox")
        sql = await _table_sql(conn, "sink_outbox")
        indexes = await _index_names(conn, "sink_outbox")
    expected_cols = {
        "id", "run_id", "run_key", "sink_id", "sink_type", "ordinal",
        "idempotency_key", "message_kind", "payload_json", "status",
        "attempts", "next_attempt_at", "claim_owner", "claim_token",
        "claim_until", "sent_at", "last_error_class", "last_error_msg",
        "created_at", "updated_at",
    }
    assert expected_cols <= cols
    sql_compact = sql.replace(" ", "").replace("\n", "")
    assert "UNIQUE(run_id,sink_id,ordinal)" in sql_compact
    assert "UNIQUE(idempotency_key)" in sql_compact
    assert "REFERENCES pipelines(id)" in sql
    assert "ON DELETE CASCADE" in sql
    assert "CHECK(status IN ('pending','sent','retryable','failed'))" in sql or \
        "CHECK(status IN ('pending','sent','retryable','failed'))".replace(" ", "") in sql.replace(" ", "")
    assert len(indexes) >= 2


@pytest.mark.asyncio
async def test_apply_v3_migrations_creates_rotation_manifest_mirror(tmp_path: Path):
    async with db.open_active_db(str(tmp_path / "active.db")) as conn:
        await db.apply_v3_migrations(conn)
        cur = await conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='rotation_manifest'"
        )
        row = await cur.fetchone()
    assert row is not None


@pytest.mark.asyncio
async def test_apply_v3_migrations_inserts_schema_version_3_last(tmp_path: Path):
    async with db.open_active_db(str(tmp_path / "active.db")) as conn:
        await db.apply_v3_migrations(conn)
        cur = await conn.execute("SELECT MAX(version) FROM schema_version")
        max_version = (await cur.fetchone())[0]
        cur = await conn.execute("SELECT version FROM schema_version WHERE version = 3")
        v3_row = await cur.fetchone()
    assert max_version == 3
    assert v3_row is not None


@pytest.mark.asyncio
async def test_apply_v3_migrations_passes_foreign_key_check(tmp_path: Path):
    async with db.open_active_db(str(tmp_path / "active.db")) as conn:
        await db.apply_v3_migrations(conn)
        cur = await conn.execute("PRAGMA foreign_key_check")
        rows = await cur.fetchall()
    assert rows == []


@pytest.mark.asyncio
async def test_apply_v3_migrations_is_idempotent(tmp_path: Path):
    async with db.open_active_db(str(tmp_path / "active.db")) as conn:
        await db.apply_v3_migrations(conn)
        await db.apply_v3_migrations(conn)
        cur = await conn.execute("SELECT MAX(version) FROM schema_version")
        max_version = (await cur.fetchone())[0]
    assert max_version == 3


@pytest.mark.asyncio
async def test_apply_v3_migrations_is_transactional_on_partial_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """If a statement fails mid-migration, schema_version must not advance to 3."""
    async with db.open_active_db(str(tmp_path / "active.db")) as conn:
        bad_statements = list(db._V3_NEW_TABLE_STATEMENTS) + ["THIS IS NOT VALID SQL"]
        monkeypatch.setattr(db, "_V3_NEW_TABLE_STATEMENTS", bad_statements)
        with pytest.raises(Exception):
            await db.apply_v3_migrations(conn)
        cur = await conn.execute("SELECT MAX(version) FROM schema_version")
        max_version = (await cur.fetchone())[0]
        cur = await conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='sink_outbox'"
        )
        sink_outbox_exists = await cur.fetchone()
    assert max_version == 2
    assert sink_outbox_exists is None
