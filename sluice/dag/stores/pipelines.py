from __future__ import annotations

import aiosqlite


class PipelinesStore:
    def __init__(self, conn: aiosqlite.Connection):
        self.conn = conn

    async def get_run_id(self, run_key: str) -> int | None:
        cur = await self.conn.execute(
            "SELECT id FROM pipelines WHERE run_key = ?",
            (run_key,),
        )
        row = await cur.fetchone()
        return None if row is None else int(row[0])

    async def get_cycle_id(self, run_id: int) -> str | None:
        cur = await self.conn.execute(
            "SELECT cycle_id FROM pipelines WHERE id = ?",
            (run_id,),
        )
        row = await cur.fetchone()
        return row[0] if row else None

    async def get_lease_state(self, run_id: int) -> dict | None:
        cur = await self.conn.execute(
            "SELECT lease_owner, lease_version, lease_until, heartbeat_at, status "
            "FROM pipelines WHERE id = ?",
            (run_id,),
        )
        row = await cur.fetchone()
        if row is None:
            return None
        return {
            "lease_owner": row[0],
            "lease_version": row[1],
            "lease_until": row[2],
            "heartbeat_at": row[3],
            "status": row[4],
        }

    async def set_lease(
        self,
        *,
        run_id: int,
        lease_owner: str,
        lease_until: str,
        heartbeat_at: str,
        lease_version: int,
    ) -> int:
        """Set lease fields. Fenced: only updates if lease_version matches.

        Returns the number of rows updated. Zero means the fencing token did
        not match (the row was already recovered or fenced by drain).
        """
        cur = await self.conn.execute(
            "UPDATE pipelines SET lease_owner = ?, lease_until = ?, "
            "heartbeat_at = ?, lease_version = ? WHERE id = ?",
            (lease_owner, lease_until, heartbeat_at, lease_version, run_id),
        )
        return cur.rowcount or 0

    async def upsert_running(
        self,
        *,
        run_key: str,
        task: str,
        pipeline_name: str,
        cycle_anchor: str,
        trigger: str,
        started_at: str,
    ) -> int:
        cur = await self.conn.execute(
            """
            INSERT INTO pipelines (
              run_key, task, pipeline_name, cycle_anchor,
              trigger, started_at, status
            ) VALUES (?, ?, ?, ?, ?, ?, 'running')
            ON CONFLICT(run_key) DO UPDATE SET
              status = 'running',
              started_at = excluded.started_at,
              finished_at = NULL,
              items_in = NULL,
              items_out = NULL,
              error_msg = NULL
            """,
            (run_key, task, pipeline_name, cycle_anchor, trigger, started_at),
        )
        await cur.close()
        row = await (
            await self.conn.execute("SELECT id FROM pipelines WHERE run_key = ?", (run_key,))
        ).fetchone()
        if row is None:
            raise RuntimeError("pipeline upsert did not return an id")
        return int(row[0])

    async def upsert_running_v3(
        self,
        *,
        run_key: str,
        task: str,
        pipeline_name: str,
        cycle_anchor: str,
        cycle_id: str,
        trigger: str,
        started_at: str,
    ) -> int:
        """Like upsert_running but also sets cycle_id. Used by tx0 under v3.

        Caller must ensure the pipelines table has the cycle_id column (i.e.
        apply_v3_migrations has run); otherwise this raises OperationalError.
        """
        cur = await self.conn.execute(
            """
            INSERT INTO pipelines (
              run_key, task, pipeline_name, cycle_anchor, cycle_id,
              trigger, started_at, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'running')
            ON CONFLICT(run_key) DO UPDATE SET
              status = 'running',
              cycle_id = excluded.cycle_id,
              started_at = excluded.started_at,
              finished_at = NULL,
              items_in = NULL,
              items_out = NULL,
              error_msg = NULL
            """,
            (run_key, task, pipeline_name, cycle_anchor, cycle_id, trigger, started_at),
        )
        await cur.close()
        row = await (
            await self.conn.execute("SELECT id FROM pipelines WHERE run_key = ?", (run_key,))
        ).fetchone()
        if row is None:
            raise RuntimeError("pipeline upsert did not return an id")
        return int(row[0])

    async def finalize(
        self,
        run_id: int,
        *,
        finished_at: str,
        status: str,
        items_in: int | None,
        items_out: int | None,
        error_msg: str | None,
    ) -> None:
        await self.conn.execute(
            """
            UPDATE pipelines
            SET finished_at = ?, status = ?, items_in = ?, items_out = ?, error_msg = ?
            WHERE id = ?
            """,
            (finished_at, status, items_in, items_out, error_msg, run_id),
        )

    async def finalize_v3(
        self,
        run_id: int,
        *,
        finished_at: str,
        status: str,
        items_in: int | None,
        items_out: int | None,
        error_msg: str | None,
        lease_owner: str,
        lease_version: int,
    ) -> int:
        """Fenced finalize: only updates if lease_owner/version match. Returns rowcount."""
        cur = await self.conn.execute(
            "UPDATE pipelines SET finished_at = ?, status = ?, items_in = ?, "
            "items_out = ?, error_msg = ? "
            "WHERE id = ? AND lease_owner = ? AND lease_version = ? AND status = 'running'",
            (finished_at, status, items_in, items_out, error_msg,
             run_id, lease_owner, lease_version),
        )
        return cur.rowcount or 0
