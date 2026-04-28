import pytest

from sluice.state.db import current_version, open_db


@pytest.mark.asyncio
async def test_initial_migration_creates_tables(tmp_path):
    db_path = tmp_path / "x.db"
    async with open_db(db_path) as db:
        assert await current_version(db) == 1
        names = {
            row[0]
            async for row in await db.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
    assert {"seen_items", "failed_items", "sink_emissions", "url_cache", "run_log"} <= names


@pytest.mark.asyncio
async def test_idempotent_open(tmp_path):
    db_path = tmp_path / "y.db"
    async with open_db(db_path):
        pass
    async with open_db(db_path) as db:
        assert await current_version(db) == 1


@pytest.mark.asyncio
async def test_migration_is_atomic(tmp_path):
    import aiosqlite

    from sluice.state import db as db_module

    orig_migrations = db_module.MIGRATIONS
    fake_migrations = tmp_path / "fake_migrations"
    fake_migrations.mkdir()
    (fake_migrations / "0001_init.sql").write_text("CREATE TABLE t1 (id INT);")
    (fake_migrations / "0002_broken.sql").write_text("SYNTAX ERROR HERE;")
    db_module.MIGRATIONS = fake_migrations
    try:
        with pytest.raises(Exception):
            async with open_db(tmp_path / "z.db") as db:
                pass
        # Verify 0001 was committed (user_version=1, table exists)
        # even though 0002 failed — use direct connection to avoid re-running migrations
        async with aiosqlite.connect(str(tmp_path / "z.db")) as db:
            async with db.execute("PRAGMA user_version") as cur:
                v = (await cur.fetchone())[0]
            async with db.execute("SELECT name FROM sqlite_master WHERE type='table'") as cur:
                tables = [r[0] for r in await cur.fetchall()]
        assert v == 1
        assert "t1" in tables
    finally:
        db_module.MIGRATIONS = orig_migrations


@pytest.mark.asyncio
async def test_migration_allows_semicolon_inside_string_literal(tmp_path):
    import aiosqlite

    from sluice.state import db as db_module

    orig_migrations = db_module.MIGRATIONS
    fake_migrations = tmp_path / "fake_migrations"
    fake_migrations.mkdir()
    (fake_migrations / "0001_init.sql").write_text(
        "CREATE TABLE t (value TEXT);\n"
        "INSERT INTO t(value) VALUES ('hello; world');\n"
    )
    db_module.MIGRATIONS = fake_migrations
    try:
        async with open_db(tmp_path / "semi.db"):
            pass
        async with aiosqlite.connect(str(tmp_path / "semi.db")) as db:
            async with db.execute("SELECT value FROM t") as cur:
                row = await cur.fetchone()
        assert row[0] == "hello; world"
    finally:
        db_module.MIGRATIONS = orig_migrations
