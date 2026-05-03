from __future__ import annotations

import re
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator, Literal

import aiosqlite

MigrationKind = Literal["active", "seen"]

_MIGRATIONS_DIR = Path(__file__).parent / "migrations"
_MIGRATION_RE = re.compile(r"^v(\d+)_(\d+)_(active|seen)_.+\.sql$")


async def apply_migrations(conn: aiosqlite.Connection, kind: MigrationKind) -> None:
    cur = await conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_version'"
    )
    has_schema_version = await cur.fetchone() is not None
    current = 0
    if has_schema_version:
        cur = await conn.execute("SELECT MAX(version) FROM schema_version")
        row = await cur.fetchone()
        if row and row[0] is not None:
            current = int(row[0])

    candidates: list[tuple[int, int, Path]] = []
    for path in _MIGRATIONS_DIR.glob("*.sql"):
        match = _MIGRATION_RE.match(path.name)
        if match is None:
            continue
        major = int(match.group(1))
        version = int(match.group(2))
        migration_kind = match.group(3)
        if migration_kind != kind:
            continue
        candidates.append((major, version, path))

    for _major, version, path in sorted(candidates):
        if version <= current:
            continue
        await conn.executescript(path.read_text())
        await conn.commit()
        current = version


async def _set_pragmas(conn: aiosqlite.Connection) -> None:
    await conn.execute("PRAGMA foreign_keys = ON")
    await conn.execute("PRAGMA journal_mode = WAL")
    await conn.execute("PRAGMA busy_timeout = 5000")


@asynccontextmanager
async def open_active_db(path: str | Path) -> AsyncIterator[aiosqlite.Connection]:
    db_path = Path(path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = await aiosqlite.connect(str(db_path))
    try:
        await _set_pragmas(conn)
        await apply_migrations(conn, "active")
        yield conn
    finally:
        await conn.close()


@asynccontextmanager
async def open_seen_db(path: str | Path) -> AsyncIterator[aiosqlite.Connection]:
    db_path = Path(path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = await aiosqlite.connect(str(db_path))
    try:
        await _set_pragmas(conn)
        await apply_migrations(conn, "seen")
        yield conn
    finally:
        await conn.close()
