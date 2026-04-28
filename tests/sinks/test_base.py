import pytest

from sluice.context import PipelineContext
from sluice.sinks.base import Sink
from sluice.state.db import open_db
from sluice.state.emissions import EmissionStore


class StubSink(Sink):
    type = "stub"

    def __init__(self, sid, mode="upsert"):
        super().__init__(id=sid, mode=mode, emit_on_empty=False)
        self.created = []
        self.updated = []

    async def create(self, ctx) -> str:
        self.created.append(ctx.run_key)
        return f"ext-{len(self.created)}"

    async def update(self, ext_id: str, ctx) -> str:
        self.updated.append((ext_id, ctx.run_key))
        return ext_id


def make_ctx(items=None):
    return PipelineContext(
        "p", "p/2026-04-28", "2026-04-28", items or [object()], {"markdown": "x"}
    )


@pytest.mark.asyncio
async def test_first_run_creates(tmp_path):
    async with open_db(tmp_path / "d.db") as db:
        e = EmissionStore(db)
        s = StubSink("sid1")
        res = await s.emit(make_ctx(), emissions=e)
        assert res.created and res.external_id == "ext-1"


@pytest.mark.asyncio
async def test_second_run_upserts(tmp_path):
    async with open_db(tmp_path / "d.db") as db:
        e = EmissionStore(db)
        s = StubSink("sid1", mode="upsert")
        await s.emit(make_ctx(), emissions=e)
        res = await s.emit(make_ctx(), emissions=e)
        assert not res.created and s.updated == [("ext-1", "p/2026-04-28")]


@pytest.mark.asyncio
async def test_create_once_noop_on_retry(tmp_path):
    async with open_db(tmp_path / "d.db") as db:
        e = EmissionStore(db)
        s = StubSink("sid1", mode="create_once")
        await s.emit(make_ctx(), emissions=e)
        res = await s.emit(make_ctx(), emissions=e)
        assert not res.created and s.updated == []


@pytest.mark.asyncio
async def test_create_new_always_creates(tmp_path):
    async with open_db(tmp_path / "d.db") as db:
        e = EmissionStore(db)
        s = StubSink("sid1", mode="create_new")
        r1 = await s.emit(make_ctx(), emissions=e)
        r2 = await s.emit(make_ctx(), emissions=e)
        assert r1.external_id != r2.external_id
        assert r1.created and r2.created


@pytest.mark.asyncio
async def test_emit_on_empty_skips(tmp_path):
    async with open_db(tmp_path / "d.db") as db:
        e = EmissionStore(db)
        s = StubSink("sid1")
        s.emit_on_empty = False
        res = await s.emit(PipelineContext("p", "p/r", "r", [], {}), emissions=e)
        assert res is None
