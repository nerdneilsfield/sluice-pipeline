from __future__ import annotations

import contextlib
import re
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator, Literal

import aiosqlite

MigrationKind = Literal["active", "seen"]

_MIGRATIONS_DIR = Path(__file__).parent / "migrations"
_MIGRATION_RE = re.compile(r"^v(\d+)_(\d+)_(active|seen)_.+\.sql$")
_V3_MIGRATION_PATH = _MIGRATIONS_DIR / "v3_0003_active_v3_core.sql"
_V3_TARGET_VERSION = 3


def _parse_sql_script(text: str) -> list[str]:
    """Split a .sql file into individual statements.

    Strips full-line comments before splitting on semicolons. Comment text must
    not contain semicolons that should not be treated as statement boundaries.
    The v3 migration only uses simple DDL with no embedded semicolons inside
    string literals.
    """
    lines = [line for line in text.splitlines() if not line.lstrip().startswith("--")]
    cleaned = "\n".join(lines)
    statements: list[str] = []
    for raw in cleaned.split(";"):
        stmt = raw.strip()
        if stmt:
            statements.append(stmt)
    return statements


_V3_NEW_TABLE_STATEMENTS: list[str] = _parse_sql_script(_V3_MIGRATION_PATH.read_text())

# Cap auto-discovery at v2 so normal startup never applies the v3 migration.
# The upgrade service must call apply_v3_migrations() explicitly after backup.
_AUTO_APPLY_MAX_VERSION = 2

_PipelinesV3Columns: list[tuple[str, str]] = [
    ("cycle_id", "TEXT NOT NULL DEFAULT ''"),
    ("lease_until", "TEXT"),
    ("heartbeat_at", "TEXT"),
    ("lease_owner", "TEXT"),
    ("lease_version", "INTEGER NOT NULL DEFAULT 0"),
]
_AdmissionsV3Columns: list[tuple[str, str]] = [
    ("run_id", "INTEGER REFERENCES pipelines(id) ON DELETE SET NULL"),
    ("lease_until", "TEXT"),
    ("heartbeat_at", "TEXT"),
    ("lease_owner", "TEXT"),
    ("lease_version", "INTEGER NOT NULL DEFAULT 0"),
]
_StageAttemptLogV3Columns: list[tuple[str, str]] = [
    ("cycle_id", "TEXT"),
    (
        "run_id",
        "INTEGER REFERENCES pipelines(id) ON DELETE CASCADE",
    ),
]

_V3_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_pipelines_task_lease "
    "ON pipelines(task, status, lease_until)",
    "CREATE INDEX IF NOT EXISTS idx_pipelines_cycle "
    "ON pipelines(cycle_id, pipeline_name)",
    "CREATE INDEX IF NOT EXISTS idx_pipelines_lease_owner "
    "ON pipelines(lease_owner, lease_version)",
    "CREATE INDEX IF NOT EXISTS idx_attempt_log_run_scoped "
    "ON stage_attempt_log(run_id, pipeline_name, stage_name, item_id, attempt_no)",
]


async def apply_migrations(
    conn: aiosqlite.Connection,
    kind: MigrationKind,
    *,
    max_version: int = _AUTO_APPLY_MAX_VERSION,
) -> None:
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
        if major >= 3:
            # v3+ migrations are gated and applied only by the upgrade service
            # via apply_v3_migrations(). They must never run from normal startup.
            continue
        if version > max_version:
            continue
        candidates.append((major, version, path))

    for _major, version, path in sorted(candidates):
        if version <= current:
            continue
        await conn.executescript(path.read_text())
        await conn.commit()
        current = version


async def _existing_columns(conn: aiosqlite.Connection, table: str) -> set[str]:
    cur = await conn.execute(f"PRAGMA table_info({table})")
    return {row[1] for row in await cur.fetchall()}


async def _add_missing_columns(
    conn: aiosqlite.Connection, table: str, columns: list[tuple[str, str]]
) -> None:
    existing = await _existing_columns(conn, table)
    for name, decl in columns:
        if name not in existing:
            await conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {decl}")


async def apply_v3_migrations(conn: aiosqlite.Connection) -> None:
    """Apply v3 core runtime migration. Idempotent and atomic.

    Normal startup must NOT call this. The upgrade service calls it after
    backup and dynamic table migration. If schema_version is already >= 3 this
    is a no-op. If any statement fails the transaction rolls back and
    schema_version stays at its prior value.
    """
    cur = await conn.execute("SELECT MAX(version) FROM schema_version")
    row = await cur.fetchone()
    current = int(row[0]) if row and row[0] is not None else 0
    if current >= _V3_TARGET_VERSION:
        return

    await conn.execute("BEGIN IMMEDIATE")
    try:
        await _add_missing_columns(conn, "pipelines", _PipelinesV3Columns)
        # Backfill cycle_id from cycle_anchor for legacy rows
        await conn.execute(
            "UPDATE pipelines SET cycle_id = cycle_anchor WHERE cycle_id = ''"
        )
        await _add_missing_columns(conn, "pipeline_admissions", _AdmissionsV3Columns)
        # Expire legacy admissions so drain can collect them under v3 rules.
        await conn.execute(
            "UPDATE pipeline_admissions SET lease_until = '1970-01-01T00:00:00+00:00' "
            "WHERE lease_until IS NULL"
        )
        await _add_missing_columns(conn, "stage_attempt_log", _StageAttemptLogV3Columns)

        for stmt in _V3_INDEXES:
            await conn.execute(stmt)

        # Idempotent new-table DDL (CREATE TABLE IF NOT EXISTS ...).
        # Run each statement individually so the surrounding BEGIN IMMEDIATE
        # stays in effect (executescript would implicitly COMMIT first).
        for stmt in _V3_NEW_TABLE_STATEMENTS:
            await conn.execute(stmt)

        await conn.execute(
            "INSERT OR IGNORE INTO schema_version (version, applied_at) "
            "VALUES (?, CURRENT_TIMESTAMP)",
            (_V3_TARGET_VERSION,),
        )
        await conn.execute("COMMIT")
    except BaseException:
        with contextlib.suppress(Exception):
            await conn.execute("ROLLBACK")
        raise


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
