from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from sluice.dag import db
from sluice.dag.dynamic_tables import (
    migrate_task_dynamic_tables,
    upgrade_task_dynamic_tables,
)
from sluice.dag.outbox_dispatcher import dispatch_outbox, finalize_delivery
from sluice.dag.stores.sink_outbox import SinkOutboxStore


class _FakeSink:
    """Minimal sink for testing: records all emit calls."""

    def __init__(self, *, fail_first: int = 0, fail_forever: bool = False):
        self.calls: list[dict] = []
        self.fail_first = fail_first
        self.fail_forever = fail_forever
        self.closed = False

    async def chunks(self, rendered, params):
        from sluice.dag.sinks.base import SinkChunk
        return [SinkChunk(ordinal=0, payload=rendered, message_kind="markdown")]

    async def emit(self, chunk, params):
        self.calls.append({"chunk": chunk, "params": params})
        if self.fail_forever or len(self.calls) <= self.fail_first:
            raise RuntimeError(f"sink fail #{len(self.calls)}")

    async def aclose(self):
        self.closed = True


async def _v3_db_with_outbox(tmp_path: Path, payloads):
    from dataclasses import dataclass

    @dataclass(frozen=True)
    class _P:
        name: str
        sinks: tuple = ()

    @dataclass(frozen=True)
    class _S:
        name: str

    @dataclass(frozen=True)
    class _T:
        pipelines: tuple
        stages: tuple

    db_path = tmp_path / "active.db"
    task = _T(pipelines=(_P("main"),), stages=(_S("extract"),))
    async with db.open_active_db(str(db_path)) as conn:
        await migrate_task_dynamic_tables(conn, task)
        await conn.commit()
        await db.apply_v3_migrations(conn)
        await upgrade_task_dynamic_tables(conn, task)
        cur = await conn.execute(
            "INSERT INTO pipelines(run_key, task, pipeline_name, cycle_anchor, "
            "cycle_id, trigger, started_at, status) VALUES "
            "('rk1', 't', 'main', 'c', 'c', 'cron', 'n', 'pending_delivery')"
        )
        run_id = cur.lastrowid
        store = SinkOutboxStore(conn)
        await store.enqueue_many(
            run_id=run_id, run_key="rk1", sink_id="tg",
            sink_type="telegram", payloads=payloads,
        )
        await conn.commit()
    return db_path, run_id


@pytest.mark.asyncio
async def test_dispatch_sends_all_pending_and_advances_pipeline(tmp_path: Path):
    db_path, run_id = await _v3_db_with_outbox(
        tmp_path,
        payloads=[
            (0, "idem-0", '{"body":"a"}', "markdown"),
            (1, "idem-1", '{"body":"b"}', "markdown"),
        ],
    )
    sink = _FakeSink()
    counts = await dispatch_outbox(
        str(db_path), sink_factory=lambda _type: sink, max_iterations=1,
    )
    assert counts["sent"] == 2
    assert len(sink.calls) == 2
    assert sink.closed is True

    async with db.open_active_db(str(db_path)) as conn:
        cur = await conn.execute("SELECT status FROM pipelines WHERE id=?", (run_id,))
        status = (await cur.fetchone())[0]
    assert status == "success"


@pytest.mark.asyncio
async def test_dispatch_marks_retryable_on_sink_error(tmp_path: Path):
    db_path, run_id = await _v3_db_with_outbox(
        tmp_path,
        payloads=[(0, "idem-0", '{"body":"a"}', "markdown")],
    )
    sink = _FakeSink(fail_forever=True)
    counts = await dispatch_outbox(
        str(db_path), sink_factory=lambda _type: sink, max_iterations=1,
    )
    assert counts["sent"] == 0
    assert counts["retryable"] == 1
    # Pipeline stays pending_delivery because retryable > 0
    async with db.open_active_db(str(db_path)) as conn:
        cur = await conn.execute("SELECT status FROM pipelines WHERE id=?", (run_id,))
        status = (await cur.fetchone())[0]
    assert status == "pending_delivery"


@pytest.mark.asyncio
async def test_finalize_delivery_no_op_when_retryable_remaining(tmp_path: Path):
    db_path, run_id = await _v3_db_with_outbox(
        tmp_path,
        payloads=[
            (0, "idem-0", '{"body":"a"}', "markdown"),
            (1, "idem-1", '{"body":"b"}', "markdown"),
        ],
    )
    sink = _FakeSink(fail_first=1)  # one fails, one succeeds
    counts = await dispatch_outbox(
        str(db_path), sink_factory=lambda _type: sink, max_iterations=1,
    )
    assert counts["sent"] == 1
    assert counts["retryable"] == 1
    async with db.open_active_db(str(db_path)) as conn:
        cur = await conn.execute("SELECT status FROM pipelines WHERE id=?", (run_id,))
        status = (await cur.fetchone())[0]
    assert status == "pending_delivery"


@pytest.mark.asyncio
async def test_finalize_delivery_advances_to_partial(tmp_path: Path):
    db_path, run_id = await _v3_db_with_outbox(
        tmp_path,
        payloads=[
            (0, "idem-0", '{"body":"a"}', "markdown"),
            (1, "idem-1", '{"body":"b"}', "markdown"),
        ],
    )
    # Manually mark one row as sent and one as failed
    async with db.open_active_db(str(db_path)) as conn:
        store = SinkOutboxStore(conn)
        rows = await store.claim_pending(
            claim_owner="test", claim_token="tok",
            claim_until="2099-01-01T00:00:00+00:00", batch_size=10,
            now=datetime(2026, 6, 21, 0, 0, tzinfo=timezone.utc),
        )
        await store.mark_sent(
            row_id=rows[0]["id"], claim_owner="test", claim_token="tok",
            sent_at="2026-06-21T00:00:00+00:00",
        )
        await store.mark_failed(
            row_id=rows[1]["id"], claim_owner="test", claim_token="tok",
            error_class="X", error_msg="bad",
        )
        new_status = await finalize_delivery(conn, run_id)
        await conn.commit()
    assert new_status == "partial"


@pytest.mark.asyncio
async def test_finalize_delivery_returns_none_when_no_outbox_rows(tmp_path: Path):
    from dataclasses import dataclass

    @dataclass(frozen=True)
    class _P:
        name: str
        sinks: tuple = ()

    @dataclass(frozen=True)
    class _S:
        name: str

    @dataclass(frozen=True)
    class _T:
        pipelines: tuple
        stages: tuple

    db_path = tmp_path / "active.db"
    task = _T(pipelines=(_P("main"),), stages=(_S("extract"),))
    async with db.open_active_db(str(db_path)) as conn:
        await migrate_task_dynamic_tables(conn, task)
        await conn.commit()
        await db.apply_v3_migrations(conn)
        await upgrade_task_dynamic_tables(conn, task)
        cur = await conn.execute(
            "INSERT INTO pipelines(run_key, task, pipeline_name, cycle_anchor, "
            "cycle_id, trigger, started_at, status) VALUES "
            "('rk1', 't', 'main', 'c', 'c', 'cron', 'n', 'pending_delivery')"
        )
        run_id = cur.lastrowid
        await conn.commit()
        result = await finalize_delivery(conn, run_id)
    assert result is None
