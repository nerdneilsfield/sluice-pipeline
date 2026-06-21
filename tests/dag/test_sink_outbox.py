from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from sluice.dag import db
from sluice.dag.dynamic_tables import (
    migrate_task_dynamic_tables,
    upgrade_task_dynamic_tables,
)
from sluice.dag.stores.sink_outbox import SinkOutboxStore


async def _v3_db(tmp_path: Path):
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
    task = _T(pipelines=(_P("main", sinks=("telegram",)),), stages=(_S("extract"),))
    async with db.open_active_db(str(db_path)) as conn:
        await migrate_task_dynamic_tables(conn, task)
        await conn.commit()
        await db.apply_v3_migrations(conn)
        await upgrade_task_dynamic_tables(conn, task)
        # Seed a pipeline run
        cur = await conn.execute(
            "INSERT INTO pipelines(run_key, task, pipeline_name, cycle_anchor, "
            "cycle_id, trigger, started_at, status) VALUES "
            "('rk1', 't', 'main', 'c', 'c', 'cron', 'n', 'running')"
        )
        run_id = cur.lastrowid
        await conn.commit()
    return db_path, run_id


@pytest.mark.asyncio
async def test_enqueue_many_inserts_pending_rows(tmp_path: Path):
    db_path, run_id = await _v3_db(tmp_path)
    async with db.open_active_db(str(db_path)) as conn:
        store = SinkOutboxStore(conn)
        inserted = await store.enqueue_many(
            run_id=run_id, run_key="rk1", sink_id="telegram",
            sink_type="telegram",
            payloads=[
                (0, "idem-0", '{"body":"a"}', "markdown"),
                (1, "idem-1", '{"body":"b"}', "markdown"),
            ],
        )
        rows = await store.pending_for_run(run_id)
    assert inserted == 2
    assert len(rows) == 2
    assert {r["status"] for r in rows} == {"pending"}
    assert {r["ordinal"] for r in rows} == {0, 1}


@pytest.mark.asyncio
async def test_enqueue_many_idempotent_on_replay(tmp_path: Path):
    db_path, run_id = await _v3_db(tmp_path)
    async with db.open_active_db(str(db_path)) as conn:
        store = SinkOutboxStore(conn)
        await store.enqueue_many(
            run_id=run_id, run_key="rk1", sink_id="tg", sink_type="telegram",
            payloads=[(0, "idem-0", '{"body":"a"}', "markdown")],
        )
        inserted = await store.enqueue_many(
            run_id=run_id, run_key="rk1", sink_id="tg", sink_type="telegram",
            payloads=[(0, "idem-0", '{"body":"a"}', "markdown")],
        )
        rows = await store.pending_for_run(run_id)
    assert inserted == 0  # second call did not insert
    assert len(rows) == 1


@pytest.mark.asyncio
async def test_enqueue_many_raises_on_payload_mismatch(tmp_path: Path):
    db_path, run_id = await _v3_db(tmp_path)
    async with db.open_active_db(str(db_path)) as conn:
        store = SinkOutboxStore(conn)
        await store.enqueue_many(
            run_id=run_id, run_key="rk1", sink_id="tg", sink_type="telegram",
            payloads=[(0, "idem-0", '{"body":"a"}', "markdown")],
        )
        with pytest.raises(RuntimeError, match="different"):
            await store.enqueue_many(
                run_id=run_id, run_key="rk1", sink_id="tg", sink_type="telegram",
                payloads=[(0, "idem-0", '{"body":"DIFFERENT"}', "markdown")],
            )


@pytest.mark.asyncio
async def test_claim_pending_returns_batch_with_token(tmp_path: Path):
    db_path, run_id = await _v3_db(tmp_path)
    async with db.open_active_db(str(db_path)) as conn:
        store = SinkOutboxStore(conn)
        await store.enqueue_many(
            run_id=run_id, run_key="rk1", sink_id="tg", sink_type="telegram",
            payloads=[
                (0, "idem-0", '{"body":"a"}', "markdown"),
                (1, "idem-1", '{"body":"b"}', "markdown"),
            ],
        )
        await conn.commit()
        claimed = await store.claim_pending(
            claim_owner="dispatcher-A",
            claim_token="tok-1",
            claim_until="2026-06-21T01:00:00+00:00",
            batch_size=10,
        )
    assert len(claimed) == 2
    assert all(c["idempotency_key"].startswith("idem-") for c in claimed)


@pytest.mark.asyncio
async def test_claim_pending_two_dispatchers_do_not_overlap(tmp_path: Path):
    db_path, run_id = await _v3_db(tmp_path)
    claim_now = datetime(2026, 6, 21, 0, 0, tzinfo=timezone.utc)
    async with db.open_active_db(str(db_path)) as conn:
        store = SinkOutboxStore(conn)
        await store.enqueue_many(
            run_id=run_id, run_key="rk1", sink_id="tg", sink_type="telegram",
            payloads=[
                (0, "idem-0", '{"body":"a"}', "markdown"),
                (1, "idem-1", '{"body":"b"}', "markdown"),
            ],
        )
        await conn.commit()
        first = await store.claim_pending(
            claim_owner="d-A", claim_token="t-A",
            claim_until="2026-06-21T01:00:00+00:00", batch_size=10,
            now=claim_now,
        )
        second = await store.claim_pending(
            claim_owner="d-B", claim_token="t-B",
            claim_until="2026-06-21T01:00:00+00:00", batch_size=10,
            now=claim_now,
        )
    assert len(first) == 2
    assert len(second) == 0


@pytest.mark.asyncio
async def test_claim_expired_can_be_reclaimed(tmp_path: Path):
    db_path, run_id = await _v3_db(tmp_path)
    async with db.open_active_db(str(db_path)) as conn:
        store = SinkOutboxStore(conn)
        await store.enqueue_many(
            run_id=run_id, run_key="rk1", sink_id="tg", sink_type="telegram",
            payloads=[(0, "idem-0", '{"body":"a"}', "markdown")],
        )
        await conn.commit()
        first = await store.claim_pending(
            claim_owner="d-A", claim_token="t-A",
            claim_until="2026-06-21T00:00:30+00:00", batch_size=10,
        )
        # Simulate expiry by claiming again with now > claim_until
        second = await store.claim_pending(
            claim_owner="d-B", claim_token="t-B",
            claim_until="2026-06-21T01:00:00+00:00",
            batch_size=10,
            now=datetime(2026, 6, 21, 0, 1, tzinfo=timezone.utc),
        )
    assert len(first) == 1
    assert len(second) == 1


@pytest.mark.asyncio
async def test_mark_sent_requires_matching_claim(tmp_path: Path):
    db_path, run_id = await _v3_db(tmp_path)
    async with db.open_active_db(str(db_path)) as conn:
        store = SinkOutboxStore(conn)
        await store.enqueue_many(
            run_id=run_id, run_key="rk1", sink_id="tg", sink_type="telegram",
            payloads=[(0, "idem-0", '{"body":"a"}', "markdown")],
        )
        await conn.commit()
        claimed = await store.claim_pending(
            claim_owner="d-A", claim_token="t-A",
            claim_until="2026-06-21T01:00:00+00:00", batch_size=10,
        )
        row_id = claimed[0]["id"]
        # Wrong token rejected
        rc_bad = await store.mark_sent(
            row_id=row_id, claim_owner="d-A", claim_token="WRONG",
            sent_at="2026-06-21T00:00:30+00:00",
        )
        # Correct token accepted
        rc_ok = await store.mark_sent(
            row_id=row_id, claim_owner="d-A", claim_token="t-A",
            sent_at="2026-06-21T00:00:30+00:00",
        )
        sent, _, _ = await store.count_terminal_for_run(run_id)
    assert rc_bad == 0
    assert rc_ok == 1
    assert sent == 1


@pytest.mark.asyncio
async def test_count_terminal_returns_zero_for_empty_run(tmp_path: Path):
    db_path, run_id = await _v3_db(tmp_path)
    async with db.open_active_db(str(db_path)) as conn:
        store = SinkOutboxStore(conn)
        sent, retryable, failed = await store.count_terminal_for_run(run_id)
    assert (sent, retryable, failed) == (0, 0, 0)
