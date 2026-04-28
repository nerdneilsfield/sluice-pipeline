import pytest, aiosqlite
from sluice.state.db import open_db, current_version

@pytest.mark.asyncio
async def test_initial_migration_creates_tables(tmp_path):
    db_path = tmp_path / "x.db"
    async with open_db(db_path) as db:
        assert await current_version(db) == 1
        names = {row[0] async for row in await db.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"seen_items", "failed_items", "sink_emissions",
            "url_cache", "run_log"} <= names

@pytest.mark.asyncio
async def test_idempotent_open(tmp_path):
    db_path = tmp_path / "y.db"
    async with open_db(db_path): pass
    async with open_db(db_path) as db:
        assert await current_version(db) == 1
