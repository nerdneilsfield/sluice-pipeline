from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Awaitable, Callable

import aiosqlite

from sluice.dag.db import open_active_db, open_seen_db
from sluice.dag.rotation.lock import RotationLock

WriteJobCallable = Callable[
    [aiosqlite.Connection, aiosqlite.Connection],
    Awaitable[None],
]


@dataclass(frozen=True)
class WriteJob:
    run: WriteJobCallable


class WriterThread:
    def __init__(
        self,
        *,
        active_path: str,
        seen_path: str,
        hold_rotation_lock: bool = True,
    ):
        self.active_path = active_path
        self.seen_path = seen_path
        self._rotation_lock = RotationLock(active_path) if hold_rotation_lock else None
        self._queue: asyncio.Queue[tuple[WriteJob, asyncio.Future[None]] | None] = (
            asyncio.Queue()
        )
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._task is not None:
            return
        if self._rotation_lock is not None:
            self._rotation_lock.acquire()
        try:
            self._task = asyncio.create_task(self._run(), name="dag-writer")
        except Exception:
            if self._rotation_lock is not None:
                self._rotation_lock.release()
            raise

    async def stop(self) -> None:
        task = self._task
        if task is None:
            return
        try:
            await self._queue.put(None)
            await task
        finally:
            self._task = None
            if self._rotation_lock is not None:
                self._rotation_lock.release()

    async def submit(self, job: WriteJob) -> asyncio.Future[None]:
        if self._task is None:
            raise RuntimeError("writer is not started")
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[None] = loop.create_future()
        await self._queue.put((job, fut))
        return fut

    async def _run(self) -> None:
        async with open_active_db(self.active_path) as active:
            async with open_seen_db(self.seen_path) as seen:
                while True:
                    item = await self._queue.get()
                    if item is None:
                        return

                    job, fut = item
                    await active.execute("BEGIN IMMEDIATE")
                    try:
                        await job.run(active, seen)
                        await active.commit()
                        try:
                            await seen.commit()
                        except Exception as exc:
                            # Active is already durable here. This is at-least-once
                            # on active and best-effort on seen: retrying the same
                            # job may repeat active-side effects. Do not report
                            # success; callers must see the divergence risk and
                            # keep active writes idempotent.
                            await self._rollback_best_effort(seen)
                            if not fut.done():
                                fut.set_exception(exc)
                            continue
                    except Exception as exc:  # noqa: BLE001
                        await self._rollback_best_effort(active)
                        await self._rollback_best_effort(seen)
                        if not fut.done():
                            fut.set_exception(exc)
                    else:
                        if not fut.done():
                            fut.set_result(None)

    @staticmethod
    async def _rollback_best_effort(conn: aiosqlite.Connection) -> None:
        try:
            await conn.rollback()
        except Exception:
            pass
