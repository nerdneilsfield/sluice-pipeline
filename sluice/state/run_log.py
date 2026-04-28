from datetime import datetime, timezone
import aiosqlite


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class RunLog:
    def __init__(self, db: aiosqlite.Connection):
        self.db = db

    async def start(self, pipeline_id: str, run_key: str):
        await self.db.execute(
            "INSERT OR REPLACE INTO run_log "
            "(pipeline_id, run_key, started_at, status) "
            "VALUES (?, ?, ?, 'running')",
            (pipeline_id, run_key, _now()),
        )
        await self.db.commit()

    async def update_stats(
        self, pipeline_id, run_key, *, items_in, items_out, llm_calls, est_cost_usd
    ):
        await self.db.execute(
            "UPDATE run_log SET items_in=?, items_out=?, llm_calls=?, "
            "est_cost_usd=? WHERE pipeline_id=? AND run_key=?",
            (items_in, items_out, llm_calls, est_cost_usd, pipeline_id, run_key),
        )
        await self.db.commit()

    async def finish(self, pipeline_id, run_key, *, status: str, error_msg: str | None = None):
        await self.db.execute(
            "UPDATE run_log SET finished_at=?, status=?, error_msg=? "
            "WHERE pipeline_id=? AND run_key=?",
            (_now(), status, error_msg, pipeline_id, run_key),
        )
        await self.db.commit()

    async def list(self, pipeline_id, limit=20):
        async with self.db.execute(
            "SELECT * FROM run_log WHERE pipeline_id=? ORDER BY started_at DESC LIMIT ?",
            (pipeline_id, limit),
        ) as cur:
            cur.row_factory = aiosqlite.Row
            return [dict(r) for r in await cur.fetchall()]
