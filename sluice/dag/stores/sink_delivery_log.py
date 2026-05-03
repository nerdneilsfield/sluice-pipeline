from __future__ import annotations

import aiosqlite


class SinkDeliveryLogStore:
    def __init__(self, conn: aiosqlite.Connection):
        self.conn = conn

    async def append(
        self,
        *,
        run_key: str,
        sink_id: str,
        sink_type: str,
        attempt_at: str,
        ordinal: int,
        message_kind: str | None,
        recipient: str | None,
        external_id: str | None,
        status: str,
        error_class: str | None,
        error_msg: str | None,
    ) -> None:
        await self.conn.execute(
            """
            INSERT INTO sink_delivery_log (
              run_key, sink_id, sink_type, attempt_at, ordinal, message_kind,
              recipient, external_id, status, error_class, error_msg
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_key,
                sink_id,
                sink_type,
                attempt_at,
                ordinal,
                message_kind,
                recipient,
                external_id,
                status,
                error_class,
                error_msg,
            ),
        )
