from __future__ import annotations

from collections.abc import Iterable

import aiosqlite

from sluice.dag._name import safe_name


class SinkPendingStore:
    def __init__(self, conn: aiosqlite.Connection, pipeline_name: str):
        self.conn = conn
        self.pipeline = safe_name(pipeline_name)

    async def upsert(self, *, item_id: str, run_key: str, pending_since: str) -> None:
        await self.conn.execute(
            f"""
            INSERT INTO pipeline_{self.pipeline}_sink_pending (
              item_id, run_key, pending_since
            ) VALUES (?, ?, ?)
            ON CONFLICT(item_id) DO UPDATE SET
              run_key = excluded.run_key,
              pending_since = excluded.pending_since
            """,
            (item_id, run_key, pending_since),
        )

    async def delete_for_items(self, item_ids: Iterable[str]) -> None:
        ids = list(item_ids)
        if not ids:
            return
        placeholders = ",".join("?" for _ in ids)
        await self.conn.execute(
            f"DELETE FROM pipeline_{self.pipeline}_sink_pending WHERE item_id IN ({placeholders})",
            tuple(ids),
        )

    async def oldest_run_key(self) -> str | None:
        cur = await self.conn.execute(
            f"SELECT run_key FROM pipeline_{self.pipeline}_sink_pending "
            "ORDER BY pending_since ASC LIMIT 1"
        )
        row = await cur.fetchone()
        return row[0] if row else None

    async def items_for_run_key(self, run_key: str) -> set[str]:
        cur = await self.conn.execute(
            f"SELECT item_id FROM pipeline_{self.pipeline}_sink_pending WHERE run_key = ?",
            (run_key,),
        )
        return {row[0] for row in await cur.fetchall()}

    async def items_with_other_keys(self, run_key: str) -> set[str]:
        cur = await self.conn.execute(
            f"SELECT item_id FROM pipeline_{self.pipeline}_sink_pending WHERE run_key <> ?",
            (run_key,),
        )
        return {row[0] for row in await cur.fetchall()}
