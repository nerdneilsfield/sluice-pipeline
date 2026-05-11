from __future__ import annotations

from sluice.dag.db import open_active_db


async def close_gate(active_path: str) -> None:
    async with open_active_db(active_path) as conn:
        await conn.execute("BEGIN IMMEDIATE")
        await conn.execute(
            "INSERT INTO rotation_gate(id, closed) VALUES (1, 1) "
            "ON CONFLICT(id) DO UPDATE SET closed = 1"
        )
        await conn.commit()


async def open_gate(active_path: str) -> None:
    async with open_active_db(active_path) as conn:
        await conn.execute("BEGIN IMMEDIATE")
        await conn.execute(
            "INSERT INTO rotation_gate(id, closed) VALUES (1, 0) "
            "ON CONFLICT(id) DO UPDATE SET closed = 0"
        )
        await conn.commit()


async def admissions_count(active_path: str, task_name: str) -> int:
    async with open_active_db(active_path) as conn:
        cur = await conn.execute(
            "SELECT COUNT(*) FROM pipeline_admissions WHERE task = ?",
            (task_name,),
        )
        row = await cur.fetchone()
        return int(row[0]) if row else 0
