import pytest

from sluice.state.db import current_version, open_db


@pytest.mark.asyncio
async def test_migration_creates_v1_1_tables(tmp_path):
    db_path = tmp_path / "s.db"
    async with open_db(db_path) as db:
        assert await current_version(db) >= 2
        cur = await db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='attachment_mirror'"
        )
        assert await cur.fetchone() is not None
        cur = await db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='gc_log'"
        )
        assert await cur.fetchone() is not None
        cur = await db.execute("PRAGMA table_info(sink_delivery_log)")
        cols = {row[1] for row in await cur.fetchall()}
        assert {
            "id",
            "pipeline_id",
            "run_key",
            "sink_id",
            "sink_type",
            "attempt_at",
            "ordinal",
            "message_kind",
            "recipient",
            "external_id",
            "status",
            "error_class",
            "error_msg",
        } <= cols
        cur = await db.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_runlog_started'"
        )
        assert await cur.fetchone() is not None


@pytest.mark.asyncio
async def test_migration_idempotent(tmp_path):
    db_path = tmp_path / "s.db"
    async with open_db(db_path):
        pass
    async with open_db(db_path) as db:
        assert await current_version(db) >= 2
