import pytest

from sluice.state.db import open_db
from sluice.state.delivery_log import DeliveryLog


@pytest.mark.asyncio
async def test_log_attempt_writes_immediately(tmp_path):
    db_path = tmp_path / "s.db"
    async with open_db(db_path) as _db:
        log = DeliveryLog(db_path=str(db_path))
        await log.record(
            pipeline_id="p",
            run_key="p/x",
            sink_id="tg",
            sink_type="telegram",
            ordinal=1,
            message_kind="brief",
            recipient=None,
            external_id="100:200",
            status="success",
            error_class=None,
            error_msg=None,
        )
    async with open_db(db_path) as db2:
        cur = await db2.execute("SELECT COUNT(*) FROM sink_delivery_log")
        assert (await cur.fetchone())[0] == 1


@pytest.mark.asyncio
async def test_log_records_failure(tmp_path):
    db_path = tmp_path / "s.db"
    async with open_db(db_path):
        pass
    log = DeliveryLog(db_path=str(db_path))
    await log.record(
        pipeline_id="p",
        run_key="p/x",
        sink_id="em",
        sink_type="email",
        ordinal=2,
        message_kind="recipient",
        recipient="a@b.c",
        external_id=None,
        status="failed",
        error_class="SMTPException",
        error_msg="boom",
    )
    async with open_db(db_path) as db:
        cur = await db.execute(
            "SELECT status, recipient, error_class FROM sink_delivery_log WHERE recipient='a@b.c'"
        )
        row = await cur.fetchone()
        assert row == ("failed", "a@b.c", "SMTPException")
