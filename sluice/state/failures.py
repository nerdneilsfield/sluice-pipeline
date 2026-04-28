import json
from datetime import datetime, timezone
from dataclasses import asdict
import aiosqlite
from sluice.core.item import Item, Attachment

def _now() -> str: return datetime.now(timezone.utc).isoformat()

def _to_json(it: Item) -> str:
    d = asdict(it)
    d["published_at"] = it.published_at.isoformat() if it.published_at else None
    return json.dumps(d, ensure_ascii=False)

def _from_json(s: str) -> Item:
    d = json.loads(s)
    if d.get("published_at"):
        d["published_at"] = datetime.fromisoformat(d["published_at"])
    d["attachments"] = [Attachment(**a) for a in d.get("attachments", [])]
    return Item(**d)

class FailureStore:
    def __init__(self, db: aiosqlite.Connection):
        self.db = db

    async def record(self, pipeline_id: str, item_key: str, item: Item, *,
                     stage: str, error_class: str, error_msg: str,
                     max_retries: int) -> None:
        async with self.db.execute(
            "SELECT attempts FROM failed_items "
            "WHERE pipeline_id=? AND item_key=?",
            (pipeline_id, item_key),
        ) as cur:
            row = await cur.fetchone()
        now = _now()
        if row is None:
            await self.db.execute(
                "INSERT INTO failed_items (pipeline_id, item_key, url, stage, "
                "error_class, error_msg, attempts, status, item_json, "
                "first_failed_at, last_failed_at) "
                "VALUES (?, ?, ?, ?, ?, ?, 1, 'failed', ?, ?, ?)",
                (pipeline_id, item_key, item.url, stage, error_class,
                 error_msg, _to_json(item), now, now),
            )
        else:
            new_attempts = row[0] + 1
            new_status = "dead_letter" if new_attempts >= max_retries else "failed"
            await self.db.execute(
                "UPDATE failed_items SET attempts=?, status=?, "
                "stage=?, error_class=?, error_msg=?, last_failed_at=? "
                "WHERE pipeline_id=? AND item_key=?",
                (new_attempts, new_status, stage, error_class, error_msg,
                 now, pipeline_id, item_key),
            )
        await self.db.commit()

    async def requeue(self, pipeline_id: str) -> list[Item]:
        async with self.db.execute(
            "SELECT item_json FROM failed_items "
            "WHERE pipeline_id=? AND status='failed'",
            (pipeline_id,),
        ) as cur:
            rows = await cur.fetchall()
        return [_from_json(r[0]) for r in rows]

    async def mark_resolved(self, pipeline_id: str, item_key: str) -> None:
        await self.db.execute(
            "UPDATE failed_items SET status='resolved' "
            "WHERE pipeline_id=? AND item_key=?",
            (pipeline_id, item_key),
        )
        await self.db.commit()

    async def list(self, pipeline_id: str, status: str | None = None) -> list[dict]:
        sql = "SELECT * FROM failed_items WHERE pipeline_id=?"
        args = [pipeline_id]
        if status:
            sql += " AND status=?"
            args.append(status)
        async with self.db.execute(sql, args) as cur:
            cur.row_factory = aiosqlite.Row
            return [dict(r) for r in await cur.fetchall()]

    async def excluded_keys(self, pipeline_id: str) -> set[str]:
        async with self.db.execute(
            "SELECT item_key FROM failed_items WHERE pipeline_id=? "
            "AND status IN ('failed','dead_letter')",
            (pipeline_id,),
        ) as cur:
            return {r[0] for r in await cur.fetchall()}
