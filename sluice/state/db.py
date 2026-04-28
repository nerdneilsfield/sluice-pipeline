from contextlib import asynccontextmanager
from pathlib import Path

import aiosqlite

MIGRATIONS = Path(__file__).parent / "migrations"


async def current_version(db: aiosqlite.Connection) -> int:
    async with db.execute("PRAGMA user_version") as cur:
        row = await cur.fetchone()
    return row[0] if row else 0


async def _migrate(db: aiosqlite.Connection) -> None:
    files = sorted(MIGRATIONS.glob("*.sql"))
    have = await current_version(db)
    for f in files:
        v = int(f.stem.split("_", 1)[0])
        if v <= have:
            continue
        sql = f.read_text()
        try:
            await db.executescript(
                f"BEGIN;\n{sql}\nPRAGMA user_version = {int(v)};\nCOMMIT;"
            )
        except Exception:
            await db.rollback()
            raise


@asynccontextmanager
async def open_db(path):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    async with aiosqlite.connect(str(path)) as db:
        await db.execute("PRAGMA foreign_keys = ON")
        await db.execute("PRAGMA journal_mode = WAL")
        await db.execute("PRAGMA busy_timeout = 5000")
        await _migrate(db)
        yield db
