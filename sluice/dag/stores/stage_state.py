from __future__ import annotations

import aiosqlite

from sluice.dag._name import safe_name


class StageStateStore:
    def __init__(self, conn: aiosqlite.Connection, stage_name: str):
        self.conn = conn
        self.stage = safe_name(stage_name)

    async def upsert(
        self,
        *,
        pipeline_name: str,
        item_id: str,
        status: str,
        attempts: int,
        last_stage_run_id: int,
        last_pipeline_run_id: int,
        last_processed_at: str,
        last_error_class: str | None,
        last_error_msg: str | None,
    ) -> None:
        await self.conn.execute(
            f"""
            INSERT INTO stage_{self.stage} (
              pipeline_name, item_id, status, attempts, last_stage_run_id,
              last_pipeline_run_id, last_processed_at, last_error_class, last_error_msg
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(pipeline_name, item_id) DO UPDATE SET
              status = excluded.status,
              attempts = excluded.attempts,
              last_stage_run_id = excluded.last_stage_run_id,
              last_pipeline_run_id = excluded.last_pipeline_run_id,
              last_processed_at = excluded.last_processed_at,
              last_error_class = excluded.last_error_class,
              last_error_msg = excluded.last_error_msg
            """,
            (
                pipeline_name,
                item_id,
                status,
                attempts,
                last_stage_run_id,
                last_pipeline_run_id,
                last_processed_at,
                last_error_class,
                last_error_msg,
            ),
        )

    async def attempts(self, *, pipeline_name: str, item_id: str) -> int:
        cur = await self.conn.execute(
            f"SELECT attempts FROM stage_{self.stage} WHERE pipeline_name = ? AND item_id = ?",
            (pipeline_name, item_id),
        )
        row = await cur.fetchone()
        return int(row[0]) if row else 0

    async def done_item_ids(self, *, pipeline_name: str, item_ids: list[str]) -> set[str]:
        if not item_ids:
            return set()
        placeholders = ",".join("?" for _ in item_ids)
        cur = await self.conn.execute(
            f"SELECT item_id FROM stage_{self.stage} "
            f"WHERE pipeline_name = ? AND item_id IN ({placeholders}) "
            "AND status = 'done'",
            (pipeline_name, *item_ids),
        )
        return {row[0] for row in await cur.fetchall()}
