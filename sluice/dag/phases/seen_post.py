from __future__ import annotations

import hashlib
from collections.abc import Iterable
from datetime import datetime, timezone

from sluice.dag.item import Item
from sluice.dag.stores.item_seen import ItemSeenStore
from sluice.dag.writer import WriteJob


def _source_hash(item: Item) -> str:
    base = f"{item['source_name']}::{item['guid'] or item['url'] or ''}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def build_seen_post_job(new_items: Iterable[Item]) -> WriteJob:
    pairs = [(item["item_id"], _source_hash(item)) for item in new_items]
    archived_at = datetime.now(timezone.utc).isoformat()

    async def job(active, seen) -> None:  # noqa: ARG001
        if pairs:
            await ItemSeenStore(seen).insert_many(pairs, archived_at=archived_at)

    return WriteJob(run=job)
