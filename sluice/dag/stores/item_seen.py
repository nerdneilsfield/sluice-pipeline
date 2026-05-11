from __future__ import annotations

from collections.abc import Iterable

import aiosqlite


class ItemSeenStore:
    def __init__(self, conn: aiosqlite.Connection):
        self.conn = conn

    async def insert_many(self, items: Iterable[tuple[str, str]], *, archived_at: str) -> None:
        rows = [(item_id, source_hash, archived_at) for item_id, source_hash in items]
        if not rows:
            return
        await self.conn.executemany(
            """
            INSERT OR IGNORE INTO item_seen (item_id, source_hash, archived_at)
            VALUES (?, ?, ?)
            """,
            rows,
        )

    async def filter_seen(self, item_ids: set[str]) -> set[str]:
        if not item_ids:
            return set()
        placeholders = ",".join("?" for _ in item_ids)
        cur = await self.conn.execute(
            f"SELECT item_id FROM item_seen WHERE item_id IN ({placeholders})",
            tuple(item_ids),
        )
        return {row[0] for row in await cur.fetchall()}
