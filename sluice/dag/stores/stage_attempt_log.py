from __future__ import annotations

import aiosqlite


class StageAttemptLogStore:
    def __init__(self, conn: aiosqlite.Connection):
        self.conn = conn

    async def next_attempt_no(self, *, pipeline_name: str, stage_name: str, item_id: str) -> int:
        cur = await self.conn.execute(
            "SELECT COALESCE(MAX(attempt_no), 0) FROM stage_attempt_log "
            "WHERE pipeline_name = ? AND stage_name = ? AND item_id = ?",
            (pipeline_name, stage_name, item_id),
        )
        row = await cur.fetchone()
        return int(row[0]) + 1

    async def append(
        self,
        *,
        stage_run_id: int,
        pipeline_run_id: int,
        pipeline_name: str,
        stage_name: str,
        item_id: str,
        attempt_no: int,
        status: str,
        processed_at: str,
        error_class: str | None,
        error_msg: str | None,
    ) -> None:
        await self.conn.execute(
            """
            INSERT INTO stage_attempt_log (
              stage_run_id, pipeline_run_id, pipeline_name, stage_name,
              item_id, attempt_no, status, processed_at, error_class, error_msg
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                stage_run_id,
                pipeline_run_id,
                pipeline_name,
                stage_name,
                item_id,
                attempt_no,
                status,
                processed_at,
                error_class,
                error_msg,
            ),
        )
