from __future__ import annotations

import aiosqlite

from sluice.dag._name import safe_name


class PipelineStateStore:
    def __init__(self, conn: aiosqlite.Connection, pipeline_name: str):
        self.conn = conn
        self.pipeline = safe_name(pipeline_name)

    async def upsert(
        self,
        *,
        item_id: str,
        last_run_id: int,
        status: str,
        processed_at: str,
        error_msg: str | None,
    ) -> None:
        await self.conn.execute(
            f"""
            INSERT INTO pipeline_{self.pipeline} (
              item_id, last_run_id, status, processed_at, error_msg
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(item_id) DO UPDATE SET
              last_run_id = excluded.last_run_id,
              status = excluded.status,
              processed_at = excluded.processed_at,
              error_msg = excluded.error_msg
            """,
            (item_id, last_run_id, status, processed_at, error_msg),
        )
