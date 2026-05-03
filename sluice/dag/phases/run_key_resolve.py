from __future__ import annotations

import sqlite3

from sluice.dag.stores.sink_pending import SinkPendingStore


async def resolve_run_key(
    active, pipeline_name: str, fresh_run_key: str
) -> tuple[str, list[str]]:
    pending = SinkPendingStore(active, pipeline_name)
    try:
        oldest = await pending.oldest_run_key()
    except sqlite3.OperationalError as exc:
        if "no such table" not in str(exc):
            raise
        return fresh_run_key, []
    if oldest is None:
        return fresh_run_key, []
    return oldest, sorted(await pending.items_with_other_keys(oldest))
