from __future__ import annotations

import asyncio
import sqlite3

import pytest

from sluice.dag.rotation.lock import RotationLock
from sluice.dag.writer import WriteJob, WriterThread


@pytest.mark.asyncio
async def test_writer_processes_jobs_in_order(tmp_path):
    active = tmp_path / "active.db"
    seen = tmp_path / "seen.db"
    writer = WriterThread(active_path=str(active), seen_path=str(seen))
    await writer.start()
    try:
        order = []

        async def job_1(active_conn, seen_conn):
            order.append(1)
            await active_conn.execute("CREATE TABLE t (n INTEGER)")
            await active_conn.execute("INSERT INTO t (n) VALUES (1)")

        async def job_2(active_conn, seen_conn):
            order.append(2)
            await active_conn.execute("INSERT INTO t (n) VALUES (2)")

        f1 = await writer.submit(WriteJob(run=job_1))
        f2 = await writer.submit(WriteJob(run=job_2))
        await f1
        await f2
        assert order == [1, 2]
    finally:
        await writer.stop()


@pytest.mark.asyncio
async def test_writer_holds_rotation_lock_until_stopped(tmp_path):
    active = tmp_path / "active.db"
    seen = tmp_path / "seen.db"
    writer = WriterThread(active_path=str(active), seen_path=str(seen))
    await writer.start()
    try:
        with pytest.raises(RuntimeError, match="rotation lock already held"):
            RotationLock(active).acquire()
    finally:
        await writer.stop()

    lock = RotationLock(active)
    lock.acquire()
    lock.release()


@pytest.mark.asyncio
async def test_writer_propagates_errors(tmp_path):
    active = tmp_path / "active.db"
    seen = tmp_path / "seen.db"
    writer = WriterThread(active_path=str(active), seen_path=str(seen))
    await writer.start()
    try:

        async def bad(active_conn, seen_conn):
            raise RuntimeError("nope")

        f = await writer.submit(WriteJob(run=bad))
        with pytest.raises(RuntimeError, match="nope"):
            await f
    finally:
        await writer.stop()


@pytest.mark.asyncio
async def test_writer_rolls_back_on_exception(tmp_path):
    active = tmp_path / "active.db"
    seen = tmp_path / "seen.db"
    writer = WriterThread(active_path=str(active), seen_path=str(seen))
    await writer.start()
    try:

        async def setup(active_conn, seen_conn):
            await active_conn.execute("CREATE TABLE t (n INTEGER)")

        await (await writer.submit(WriteJob(run=setup)))

        async def write_then_raise(active_conn, seen_conn):
            await active_conn.execute("INSERT INTO t (n) VALUES (999)")
            raise RuntimeError("rollback me")

        f = await writer.submit(WriteJob(run=write_then_raise))
        with pytest.raises(RuntimeError, match="rollback me"):
            await f

        async def assert_clean(active_conn, seen_conn):
            cur = await active_conn.execute("SELECT count(*) FROM t WHERE n=999")
            assert (await cur.fetchone())[0] == 0

        await (await writer.submit(WriteJob(run=assert_clean)))
    finally:
        await writer.stop()


@pytest.mark.asyncio
async def test_writer_rolls_back_seen_on_exception(tmp_path):
    active = tmp_path / "active.db"
    seen = tmp_path / "seen.db"
    writer = WriterThread(active_path=str(active), seen_path=str(seen))
    await writer.start()
    try:

        async def setup(active_conn, seen_conn):
            await seen_conn.execute("CREATE TABLE seen_t (n INTEGER)")

        await (await writer.submit(WriteJob(run=setup)))

        async def write_seen_then_raise(active_conn, seen_conn):
            await seen_conn.execute("INSERT INTO seen_t (n) VALUES (999)")
            raise RuntimeError("rollback seen")

        f = await writer.submit(WriteJob(run=write_seen_then_raise))
        with pytest.raises(RuntimeError, match="rollback seen"):
            await f

        async def assert_clean(active_conn, seen_conn):
            cur = await seen_conn.execute("SELECT count(*) FROM seen_t WHERE n=999")
            assert (await cur.fetchone())[0] == 0

        await (await writer.submit(WriteJob(run=assert_clean)))
    finally:
        await writer.stop()


@pytest.mark.asyncio
async def test_writer_propagates_seen_commit_failure(tmp_path):
    active = tmp_path / "active.db"
    seen = tmp_path / "seen.db"
    writer = WriterThread(active_path=str(active), seen_path=str(seen))
    await writer.start()
    try:

        async def commit_fails(active_conn, seen_conn):
            await active_conn.execute("CREATE TABLE t (n INTEGER)")

            async def fail_commit():
                raise RuntimeError("seen commit failed")

            seen_conn.commit = fail_commit

        f = await writer.submit(WriteJob(run=commit_fails))
        with pytest.raises(RuntimeError, match="seen commit failed"):
            await f
    finally:
        await writer.stop()

    with sqlite3.connect(active) as conn:
        assert conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='t'"
        ).fetchone() == ("t",)


@pytest.mark.asyncio
async def test_writer_concurrent_submitters(tmp_path):
    active = tmp_path / "active.db"
    seen = tmp_path / "seen.db"
    writer = WriterThread(active_path=str(active), seen_path=str(seen))
    await writer.start()
    try:

        async def setup(active_conn, seen_conn):
            await active_conn.execute("CREATE TABLE t (n INTEGER)")

        await (await writer.submit(WriteJob(run=setup)))

        async def insert(i):
            async def job(active_conn, seen_conn):
                await active_conn.execute("INSERT INTO t (n) VALUES (?)", (i,))

            return await (await writer.submit(WriteJob(run=job)))

        await asyncio.gather(*(insert(i) for i in range(20)))
    finally:
        await writer.stop()
