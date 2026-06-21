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
        """Legacy v2 upsert keyed by item_id only. Removed from v3 tables."""
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

    async def upsert_v3(
        self,
        *,
        run_id: int,
        item_id: str,
        cycle_id: str,
        status: str,
        processed_at: str,
        error_msg: str | None,
    ) -> None:
        """Run-scoped upsert. Used after v3 upgrade. PK is (run_id, item_id)."""
        await self.conn.execute(
            f"""
            INSERT INTO pipeline_{self.pipeline} (
              run_id, item_id, cycle_id, status, processed_at, error_msg
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id, item_id) DO UPDATE SET
              cycle_id = excluded.cycle_id,
              status = excluded.status,
              processed_at = excluded.processed_at,
              error_msg = excluded.error_msg
            """,
            (run_id, item_id, cycle_id, status, processed_at, error_msg),
        )

    async def latest_status_for_cycle(
        self, *, cycle_id: str, item_id: str
    ) -> str | None:
        """Cycle-aggregate read: status from the highest run_id for this item/cycle."""
        cur = await self.conn.execute(
            f"SELECT status FROM pipeline_{self.pipeline} "
            "WHERE cycle_id = ? AND item_id = ? ORDER BY run_id DESC LIMIT 1",
            (cycle_id, item_id),
        )
        row = await cur.fetchone()
        return row[0] if row else None
