from __future__ import annotations

import re

import aiosqlite

_NAME_RE = re.compile(r"^[a-zA-Z0-9_]+$")


def _safe(name: str) -> str:
    if _NAME_RE.fullmatch(name) is None:
        raise ValueError(f"unsafe pipeline name: {name!r}")
    return name


class PipelineStateStore:
    def __init__(self, conn: aiosqlite.Connection, pipeline_name: str):
        self.conn = conn
        self.pipeline = _safe(pipeline_name)

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
