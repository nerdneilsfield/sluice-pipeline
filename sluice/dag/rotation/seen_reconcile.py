from __future__ import annotations

from sluice.dag._source_hash import source_hash_from_row
from sluice.dag.db import open_active_db, open_seen_db
from sluice.dag.stores.item_seen import ItemSeenStore


async def reconcile_seen_for_archive(
    *,
    old_active_path: str,
    seen_path: str,
    archive_ids: set[str],
    archived_at: str,
) -> int:
    if not archive_ids:
        return 0

    pairs: list[tuple[str, str]] = []
    ids = list(archive_ids)
    async with open_active_db(old_active_path) as old_conn:
        for start in range(0, len(ids), 400):
            chunk = ids[start : start + 400]
            placeholders = ",".join("?" for _ in chunk)
            cur = await old_conn.execute(
                f"SELECT item_id, source_name, guid, url FROM items "
                f"WHERE item_id IN ({placeholders})",
                tuple(chunk),
            )
            for row in await cur.fetchall():
                item_row = {
                    "item_id": row[0],
                    "source_name": row[1],
                    "guid": row[2],
                    "url": row[3],
                }
                pairs.append((item_row["item_id"], source_hash_from_row(item_row)))

    async with open_seen_db(seen_path) as seen_conn:
        await ItemSeenStore(seen_conn).insert_many(pairs, archived_at=archived_at)
        await seen_conn.commit()
    return len(pairs)
