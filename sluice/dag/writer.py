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
        self._queue: asyncio.Queue[tuple[WriteJob, asyncio.Future[None]] | None] = asyncio.Queue()
        self._task: asyncio.Task[None] | None = None
        self._fatal: BaseException | None = None

    async def start(self) -> None:
        if self._task is not None:
            return
        if self._rotation_lock is not None:
            self._rotation_lock.acquire()
        try:
            self._fatal = None
            self._task = asyncio.create_task(self._run(), name="dag-writer")
            self._task.add_done_callback(self._on_task_done)
        except Exception:
            if self._rotation_lock is not None:
                self._rotation_lock.release()
            raise

    async def stop(self) -> None:
        task = self._task
        if task is None:
            return
        try:
            if not task.done():
                await self._queue.put(None)
                try:
                    await task
                except Exception as exc:  # noqa: BLE001
                    self._fatal = exc
                    self._fail_pending(exc)
            else:
                exc = self._task_exception(task)
                if exc is not None:
                    self._fatal = exc
                    self._fail_pending(exc)
        finally:
            self._task = None
            self._fatal = None
            if self._rotation_lock is not None:
                self._rotation_lock.release()

    async def submit(self, job: WriteJob) -> asyncio.Future[None]:
        if self._task is None:
            raise RuntimeError("writer is not started")
        if self._fatal is not None:
            raise RuntimeError(f"writer task failed: {self._fatal}") from self._fatal
        if self._task.done():
            exc = self._task_exception(self._task)
            if exc is not None:
                self._fatal = exc
                self._fail_pending(exc)
                raise RuntimeError(f"writer task failed: {exc}") from exc
            raise RuntimeError("writer is not running")
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[None] = loop.create_future()
        await self._queue.put((job, fut))
        return fut

    def _on_task_done(self, task: asyncio.Task[None]) -> None:
        exc = self._task_exception(task)
        if exc is None:
            return
        self._fatal = exc
        self._fail_pending(exc)

    @staticmethod
    def _task_exception(task: asyncio.Task[None]) -> BaseException | None:
        if task.cancelled():
            return RuntimeError("writer task was cancelled")
        return task.exception()

    def _fail_pending(self, exc: BaseException) -> None:
        while True:
            try:
                item = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            if item is None:
                continue
            _, fut = item
            if not fut.done():
                fut.set_exception(exc)

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
