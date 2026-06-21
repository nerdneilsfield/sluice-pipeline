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
        """Legacy v2 upsert keyed by (pipeline_name, item_id). Removed from v3 tables."""
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

    async def upsert_v3(
        self,
        *,
        run_id: int,
        pipeline_name: str,
        item_id: str,
        cycle_id: str,
        status: str,
        attempts: int,
        last_stage_run_id: int,
        last_processed_at: str,
        last_error_class: str | None,
        last_error_msg: str | None,
    ) -> None:
        """Run-scoped upsert. PK is (run_id, pipeline_name, item_id)."""
        await self.conn.execute(
            f"""
            INSERT INTO stage_{self.stage} (
              run_id, pipeline_name, item_id, cycle_id, status, attempts,
              last_stage_run_id, last_processed_at, last_error_class, last_error_msg
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id, pipeline_name, item_id) DO UPDATE SET
              cycle_id = excluded.cycle_id,
              status = excluded.status,
              attempts = excluded.attempts,
              last_stage_run_id = excluded.last_stage_run_id,
              last_processed_at = excluded.last_processed_at,
              last_error_class = excluded.last_error_class,
              last_error_msg = excluded.last_error_msg
            """,
            (
                run_id,
                pipeline_name,
                item_id,
                cycle_id,
                status,
                attempts,
                last_stage_run_id,
                last_processed_at,
                last_error_class,
                last_error_msg,
            ),
        )

    async def attempts(self, *, pipeline_name: str, item_id: str) -> int:
        """Legacy v2 attempts reader. Cycle/run agnostic — avoids use in v3."""
        cur = await self.conn.execute(
            f"SELECT attempts FROM stage_{self.stage} WHERE pipeline_name = ? AND item_id = ?",
            (pipeline_name, item_id),
        )
        row = await cur.fetchone()
        return int(row[0]) if row else 0

    async def attempts_v3(
        self, *, run_id: int, pipeline_name: str, item_id: str
    ) -> int:
        cur = await self.conn.execute(
            f"SELECT attempts FROM stage_{self.stage} "
            "WHERE run_id = ? AND pipeline_name = ? AND item_id = ?",
            (run_id, pipeline_name, item_id),
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

    async def done_item_ids_v3(
        self, *, run_id: int, pipeline_name: str, item_ids: list[str]
    ) -> set[str]:
        if not item_ids:
            return set()
        placeholders = ",".join("?" for _ in item_ids)
        cur = await self.conn.execute(
            f"SELECT item_id FROM stage_{self.stage} "
            f"WHERE run_id = ? AND pipeline_name = ? AND item_id IN ({placeholders}) "
            "AND status = 'done'",
            (run_id, pipeline_name, *item_ids),
        )
        return {row[0] for row in await cur.fetchall()}
