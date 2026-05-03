from __future__ import annotations

from pathlib import Path

import pytest

from sluice.dag import db
from sluice.dag.emission import SinkEmitter
from sluice.dag.sinks.base import EmissionResult, Sink, SinkChunk
from sluice.dag.writer import WriterThread


class _Sink(Sink):
    def __init__(self, *, fail: bool = False):
        self.fail = fail

    async def chunks(self, rendered, params):
        if params.get("chunks_fail"):
            raise RuntimeError("chunks boom")
        return [SinkChunk(ordinal=0, payload=rendered, message_kind="markdown")]

    async def emit(self, chunk, params):
        if self.fail:
            raise RuntimeError("boom")
        return EmissionResult(
            sink_id=params["sink_id"],
            ordinal=chunk.ordinal,
            success=True,
            external_id="ext",
            recipient="rec",
        )


@pytest.mark.asyncio
async def test_sink_emitter_records_success_and_is_idempotent(tmp_path: Path):
    active = str(tmp_path / "active.db")
    seen = str(tmp_path / "seen.db")
    writer = WriterThread(active_path=active, seen_path=seen)
    await writer.start()
    try:
        emitter = SinkEmitter(writer=writer, run_key="rk", pipeline_name="P")
        result = await emitter.emit_all(
            sink_id="out",
            sink_type="test",
            sink=_Sink(),
            rendered="hello",
            params={"sink_id": "out"},
        )
        again = await emitter.emit_all(
            sink_id="out",
            sink_type="test",
            sink=_Sink(fail=True),
            rendered="hello",
            params={"sink_id": "out"},
        )
    finally:
        await writer.stop()

    assert result[0].success
    assert again[0].success and again[0].error_msg == "already_emitted"
    async with db.open_active_db(active) as conn:
        cur = await conn.execute("SELECT count(*) FROM sink_emissions")
        assert (await cur.fetchone())[0] == 1
        cur = await conn.execute("SELECT status FROM sink_delivery_log")
        assert [row[0] for row in await cur.fetchall()] == ["success"]


@pytest.mark.asyncio
async def test_sink_emitter_records_failure_without_emission(tmp_path: Path):
    writer = WriterThread(
        active_path=str(tmp_path / "active.db"),
        seen_path=str(tmp_path / "seen.db"),
    )
    await writer.start()
    try:
        emitter = SinkEmitter(writer=writer, run_key="rk", pipeline_name="P")
        result = await emitter.emit_all(
            sink_id="out",
            sink_type="test",
            sink=_Sink(fail=True),
            rendered="hello",
            params={"sink_id": "out"},
        )
    finally:
        await writer.stop()

    assert not result[0].success


@pytest.mark.asyncio
async def test_sink_emitter_records_chunk_failure_without_leaving_running(tmp_path: Path):
    writer = WriterThread(
        active_path=str(tmp_path / "active.db"),
        seen_path=str(tmp_path / "seen.db"),
    )
    await writer.start()
    try:
        emitter = SinkEmitter(writer=writer, run_key="rk", pipeline_name="P")
        result = await emitter.emit_all(
            sink_id="out",
            sink_type="test",
            sink=_Sink(),
            rendered="hello",
            params={"sink_id": "out", "chunks_fail": True},
        )
    finally:
        await writer.stop()

    assert not result[0].success
    async with db.open_active_db(str(tmp_path / "active.db")) as conn:
        cur = await conn.execute("SELECT status, error_class FROM sink_delivery_log")
        assert await cur.fetchone() == ("failed", "RuntimeError")
