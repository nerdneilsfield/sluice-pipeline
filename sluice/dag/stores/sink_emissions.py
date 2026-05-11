from __future__ import annotations

import aiosqlite


class SinkEmissionsStore:
    def __init__(self, conn: aiosqlite.Connection):
        self.conn = conn

    async def exists(self, run_key: str, sink_id: str, ordinal: int) -> bool:
        cur = await self.conn.execute(
            "SELECT 1 FROM sink_emissions WHERE run_key = ? AND sink_id = ? AND ordinal = ?",
            (run_key, sink_id, ordinal),
        )
        return await cur.fetchone() is not None

    async def insert(
        self,
        *,
        run_key: str,
        sink_id: str,
        ordinal: int,
        sink_type: str,
        message_kind: str | None,
        external_id: str | None,
        emitted_at: str,
    ) -> None:
        await self.conn.execute(
            """
            INSERT INTO sink_emissions (
              run_key, sink_id, ordinal, sink_type,
              message_kind, external_id, emitted_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (run_key, sink_id, ordinal, sink_type, message_kind, external_id, emitted_at),
        )
