from __future__ import annotations

from sluice.dag._source_hash import source_hash_from_row
from sluice.dag.db import open_active_db, open_seen_db
from sluice.dag.stores.item_seen import ItemSeenStore


async def read_archive_item_rows(
    *, old_active_path: str, archive_ids: set[str]
) -> list[dict]:
    """Read item rows that will be archived, BEFORE the active DB is swapped.

    Returns a list of dicts with keys: item_id, source_name, guid, url.
    The caller passes this list to ``write_seen_for_archive`` after swap
    succeeds, so a swap failure cannot leave the seen DB mutated.
    """
    if not archive_ids:
        return []
    out: list[dict] = []
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
                out.append({
                    "item_id": row[0],
                    "source_name": row[1],
                    "guid": row[2],
                    "url": row[3],
                })
    return out


async def write_seen_for_archive(
    *, seen_path: str, item_rows: list[dict], archived_at: str
) -> int:
    """Mark the given item rows as seen. Idempotent via INSERT OR IGNORE."""
    if not item_rows:
        return 0
    pairs = [
        (row["item_id"], source_hash_from_row(row))
        for row in item_rows
    ]
    async with open_seen_db(seen_path) as seen_conn:
        await ItemSeenStore(seen_conn).insert_many(pairs, archived_at=archived_at)
        await seen_conn.commit()
    return len(pairs)


async def reconcile_seen_for_archive(
    *,
    old_active_path: str,
    seen_path: str,
    archive_ids: set[str],
    archived_at: str,
) -> int:
    """Legacy one-shot: read from old_active_path and write to seen.

    Kept for callers that have not been split into pre-swap read +
    post-swap write. The orchestrator now uses read_archive_item_rows +
    write_seen_for_archive so seen mutation happens strictly after swap.
    """
    item_rows = await read_archive_item_rows(
        old_active_path=old_active_path, archive_ids=archive_ids
    )
    return await write_seen_for_archive(
        seen_path=seen_path, item_rows=item_rows, archived_at=archived_at
    )
