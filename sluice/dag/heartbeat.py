from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

from sluice.dag.stores.pipelines import PipelinesStore
from sluice.dag.writer import WriterThread

logger = logging.getLogger(__name__)


class HeartbeatTask:
    """Background task that refreshes a run's lease on a fixed interval.

    Lifecycle:
    - ``start()`` schedules the heartbeat loop as an asyncio Task.
    - ``stop()`` cancels the task and awaits cleanup.
    - Each tick submits a fenced ``set_lease`` writer job; if the lease
      has been fenced (rowcount == 0), the heartbeat stops and surfaces
      the failure via ``last_refresh_ok``.

    The runner ``finally`` block should call ``await stop()`` AFTER
    cancelling stage execution but BEFORE calling ``aclose()`` on other
    resources (ProviderPool, sinks), so the lease state is consistent.
    """

    def __init__(
        self,
        *,
        writer: WriterThread,
        run_id: int,
        lease_owner: str,
        lease_version: int,
        refresh_interval_seconds: float,
        lease_duration_seconds: float,
    ) -> None:
        self._writer = writer
        self._run_id = run_id
        self._lease_owner = lease_owner
        self._lease_version = lease_version
        self._refresh_interval = refresh_interval_seconds
        self._lease_duration = lease_duration_seconds
        self._task: asyncio.Task[None] | None = None
        self.last_refresh_ok: bool = True

    def start(self) -> None:
        if self._task is not None:
            return
        self._task = asyncio.create_task(self._loop())

    async def _loop(self) -> None:
        while True:
            await asyncio.sleep(self._refresh_interval)
            now = datetime.now(timezone.utc)
            ok = await self._refresh(now)
            self.last_refresh_ok = ok
            if not ok:
                logger.warning(
                    "heartbeat lost lease fence for run_id=%s owner=%s",
                    self._run_id,
                    self._lease_owner,
                )
                return

    async def _refresh(self, now: datetime) -> bool:
        from datetime import timedelta

        heartbeat_at = now.isoformat()
        lease_until = (now + timedelta(seconds=self._lease_duration)).isoformat()

        out: dict[str, Any] = {}

        async def _job(active, seen):  # noqa: ARG001
            store = PipelinesStore(active)
            rowcount = await store.set_lease(
                run_id=self._run_id,
                lease_owner=self._lease_owner,
                lease_until=lease_until,
                heartbeat_at=heartbeat_at,
                lease_version=self._lease_version,
                expected_lease_version=self._lease_version,
            )
            out["rowcount"] = rowcount

        try:
            from sluice.dag.writer import WriteJob

            await self._writer.submit(WriteJob(run=_job))
            # writer.submit returns the awaited future; the job has run by now.
            # We need to await the future too.
        except Exception as exc:  # noqa: BLE001
            logger.error("heartbeat writer job failed: %s", exc)
            return False
        return out.get("rowcount", 0) > 0

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None
