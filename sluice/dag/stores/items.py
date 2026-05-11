from __future__ import annotations

from typing import Any

import aiosqlite

_UPDATABLE = frozenset(
    {
        "title",
        "author",
        "tag",
        "raw_summary",
        "raw_content",
        "fulltext",
        "attachments_json",
        "summary",
        "score",
        "tags_json",
        "extras_json",
    }
)


class ItemsStore:
    def __init__(self, conn: aiosqlite.Connection):
        self.conn = conn

    async def insert_new(
        self,
        *,
        item_id: str,
        source_name: str,
        url: str | None,
        title: str | None,
        published_at: str | None,
        raw_json: str,
        ingested_at: str,
        ingested_by: str,
        guid: str | None = None,
        author: str | None = None,
        tag: str | None = None,
        raw_summary: str | None = None,
        raw_content: str | None = None,
    ) -> None:
        await self.conn.execute(
            """
            INSERT OR IGNORE INTO items (
              item_id, source_name, guid, url, title, author,
              published_at, ingested_at, ingested_by, updated_at,
              tag, raw_summary, raw_content, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                item_id,
                source_name,
                guid,
                url,
                title,
                author,
                published_at,
                ingested_at,
                ingested_by,
                ingested_at,
                tag,
                raw_summary,
                raw_content,
                raw_json,
            ),
        )

    async def update_columns(self, item_id: str, cols: dict[str, Any], *, updated_at: str) -> None:
        if not cols:
            return
        bad = set(cols) - _UPDATABLE
        if bad:
            raise ValueError(f"non-updatable columns: {bad}")
        sets = ", ".join(f"{col} = ?" for col in cols)
        params = [*cols.values(), updated_at, item_id]
        await self.conn.execute(
            f"UPDATE items SET {sets}, updated_at = ? WHERE item_id = ?",
            params,
        )

    async def mark_dropped(self, item_id: str, *, reason: str, at: str) -> None:
        await self.conn.execute(
            """
            UPDATE items
            SET dropped = 1, dropped_reason = ?, dropped_at = ?, updated_at = ?
            WHERE item_id = ?
            """,
            (reason, at, at, item_id),
        )

    async def exists(self, item_id: str) -> bool:
        cur = await self.conn.execute("SELECT 1 FROM items WHERE item_id = ?", (item_id,))
        return await cur.fetchone() is not None

    async def get(self, item_id: str) -> dict[str, Any] | None:
        cur = await self.conn.execute("SELECT * FROM items WHERE item_id = ?", (item_id,))
        row = await cur.fetchone()
        if row is None:
            return None
        return dict(zip([desc[0] for desc in cur.description], row, strict=True))
