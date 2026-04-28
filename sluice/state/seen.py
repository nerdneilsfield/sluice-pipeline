from datetime import datetime, timezone
import aiosqlite
from sluice.core.item import Item

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _iso(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt else None

class SeenStore:
    def __init__(self, db: aiosqlite.Connection):
        self.db = db

    async def is_seen(self, pipeline_id: str, item_key: str) -> bool:
        async with self.db.execute(
            "SELECT 1 FROM seen_items WHERE pipeline_id=? AND item_key=?",
            (pipeline_id, item_key),
        ) as cur:
            return await cur.fetchone() is not None

    async def filter_unseen(self, pipeline_id: str,
                            keys: list[str]) -> list[str]:
        if not keys:
            return []
        seen: set[str] = set()
        BATCH = 900
        for i in range(0, len(keys), BATCH):
            chunk = keys[i:i + BATCH]
            placeholders = ",".join("?" * len(chunk))
            async with self.db.execute(
                f"SELECT item_key FROM seen_items "
                f"WHERE pipeline_id=? AND item_key IN ({placeholders})",
                (pipeline_id, *chunk),
            ) as cur:
                seen.update(row[0] for row in await cur.fetchall())
        return [k for k in keys if k not in seen]

    async def mark_seen_batch(self, pipeline_id: str,
                              items_with_keys: list[tuple[Item, str]]) -> None:
        now = _now_iso()
        rows = [
            (pipeline_id, key, it.url, it.title, _iso(it.published_at),
             it.summary, now)
            for it, key in items_with_keys
        ]
        await self.db.executemany(
            "INSERT OR REPLACE INTO seen_items "
            "(pipeline_id, item_key, url, title, published_at, summary, seen_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        await self.db.commit()
