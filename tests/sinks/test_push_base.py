import pytest

from sluice.sinks._push_base import PushBatchItem, PushSinkBase
from sluice.state.db import open_db
from sluice.state.delivery_log import DeliveryLog
from sluice.state.emissions import EmissionStore
from tests.conftest import make_ctx


class _OkSink(PushSinkBase):
    type = "telegram"

    def build_batch(self, ctx):
        return [
            PushBatchItem(kind="brief", payload="msg1"),
            PushBatchItem(kind="item", payload="msg2"),
        ]

    async def send_one(self, payload, recipient=None):
        return f"id-{payload}"


@pytest.mark.asyncio
async def test_push_base_logs_each_send(tmp_path):
    db_path = tmp_path / "s.db"
    async with open_db(db_path) as db:
        log = DeliveryLog(db_path=str(db_path))
        emissions = EmissionStore(db)
        sink = _OkSink(sink_id="X", footer_template="", delivery_log=log)
        ctx = make_ctx()
        res = await sink.emit(ctx, emissions=emissions)
        assert res.external_id == "id-msg1"
        cur = await db.execute("SELECT message_kind FROM sink_delivery_log ORDER BY ordinal")
        kinds = [r[0] for r in await cur.fetchall()]
        assert kinds == ["brief", "item"]
        cur = await db.execute("SELECT external_id FROM sink_emissions WHERE sink_id='X'")
        row = await cur.fetchone()
        assert row[0] == "id-msg1"


@pytest.mark.asyncio
async def test_push_base_compatible_with_runner_emit_call_shape(tmp_path):
    db_path = tmp_path / "s.db"

    class _PartialFail(PushSinkBase):
        type = "telegram"

        def build_batch(self, ctx):
            return [
                PushBatchItem(kind="brief", payload="ok"),
                PushBatchItem(kind="item", payload="boom"),
            ]

        async def send_one(self, payload, recipient=None):
            if payload == "boom":
                raise RuntimeError("nope")
            return "id-ok"

    async with open_db(db_path) as db:
        log = DeliveryLog(db_path=str(db_path))
        emissions = EmissionStore(db)
        sink = _PartialFail(sink_id="X", footer_template="", delivery_log=log)
        with pytest.raises(RuntimeError):
            await sink.emit(make_ctx(), emissions=emissions)
        cur = await db.execute("SELECT status FROM sink_delivery_log ORDER BY ordinal")
        rows = [r[0] for r in await cur.fetchall()]
        assert rows == ["success", "failed"]
