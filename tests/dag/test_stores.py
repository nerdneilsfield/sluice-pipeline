from __future__ import annotations

from datetime import datetime, timezone

import pytest

from sluice.dag import db
from sluice.dag.stores.item_seen import ItemSeenStore
from sluice.dag.stores.items import ItemsStore
from sluice.dag.stores.pipelines import PipelinesStore
from sluice.dag.stores.stages import StagesStore


@pytest.mark.asyncio
async def test_insert_and_lookup(tmp_path):
    p = tmp_path / "active.db"
    async with db.open_active_db(str(p)) as conn:
        store = ItemsStore(conn)
        now = datetime(2026, 5, 3, 10, 0, tzinfo=timezone.utc).isoformat()
        await store.insert_new(
            item_id="abc",
            source_name="src",
            url="https://x",
            title="T",
            published_at="2026-05-01T00:00:00+00:00",
            raw_json='{"a":1}',
            ingested_at=now,
            ingested_by="entry",
        )
        await conn.commit()
        rec = await store.get("abc")
    assert rec is not None
    assert rec["url"] == "https://x"
    assert rec["dropped"] == 0


@pytest.mark.asyncio
async def test_insert_or_ignore_duplicate(tmp_path):
    p = tmp_path / "active.db"
    async with db.open_active_db(str(p)) as conn:
        store = ItemsStore(conn)
        now = "2026-05-03T00:00:00+00:00"
        await store.insert_new(
            item_id="abc",
            source_name="s",
            url="u",
            title="t",
            published_at=None,
            raw_json="{}",
            ingested_at=now,
            ingested_by="e",
        )
        await store.insert_new(
            item_id="abc",
            source_name="s",
            url="u",
            title="other",
            published_at=None,
            raw_json="{}",
            ingested_at=now,
            ingested_by="e",
        )
        await conn.commit()
        rec = await store.get("abc")
    assert rec["title"] == "t"


@pytest.mark.asyncio
async def test_update_columns(tmp_path):
    p = tmp_path / "active.db"
    async with db.open_active_db(str(p)) as conn:
        store = ItemsStore(conn)
        now = "2026-05-03T00:00:00+00:00"
        await store.insert_new(
            item_id="abc",
            source_name="s",
            url="u",
            title="t",
            published_at=None,
            raw_json="{}",
            ingested_at=now,
            ingested_by="e",
        )
        await store.update_columns("abc", {"summary": "S", "score": 0.9}, updated_at=now)
        await conn.commit()
        rec = await store.get("abc")
    assert rec["summary"] == "S"
    assert rec["score"] == 0.9


@pytest.mark.asyncio
async def test_update_columns_rejects_unknown_columns(tmp_path):
    p = tmp_path / "active.db"
    async with db.open_active_db(str(p)) as conn:
        store = ItemsStore(conn)
        with pytest.raises(ValueError, match="non-updatable columns"):
            await store.update_columns("abc", {"item_id": "def"}, updated_at="t")


@pytest.mark.asyncio
async def test_mark_dropped(tmp_path):
    p = tmp_path / "active.db"
    async with db.open_active_db(str(p)) as conn:
        store = ItemsStore(conn)
        now = "2026-05-03T00:00:00+00:00"
        await store.insert_new(
            item_id="abc",
            source_name="s",
            url="u",
            title="t",
            published_at=None,
            raw_json="{}",
            ingested_at=now,
            ingested_by="e",
        )
        await store.mark_dropped("abc", reason="manual_kill", at=now)
        await conn.commit()
        rec = await store.get("abc")
    assert rec["dropped"] == 1
    assert rec["dropped_reason"] == "manual_kill"


@pytest.mark.asyncio
async def test_exists_check(tmp_path):
    p = tmp_path / "active.db"
    async with db.open_active_db(str(p)) as conn:
        store = ItemsStore(conn)
        assert await store.exists("nope") is False
        now = "2026-05-03T00:00:00+00:00"
        await store.insert_new(
            item_id="abc",
            source_name="s",
            url="u",
            title="t",
            published_at=None,
            raw_json="{}",
            ingested_at=now,
            ingested_by="e",
        )
        assert await store.exists("abc") is True


@pytest.mark.asyncio
async def test_pipelines_insert_running(tmp_path):
    p = tmp_path / "active.db"
    async with db.open_active_db(str(p)) as conn:
        store = PipelinesStore(conn)
        run_id = await store.upsert_running(
            run_key="example__report__2026-05-03T08:00",
            task="example",
            pipeline_name="report",
            cycle_anchor="2026-05-03T08:00:00+00:00",
            trigger="cron",
            started_at="2026-05-03T08:01:00+00:00",
        )
        assert isinstance(run_id, int) and run_id > 0
        await conn.commit()
        cur = await conn.execute("SELECT status FROM pipelines WHERE id = ?", (run_id,))
        assert (await cur.fetchone())[0] == "running"


@pytest.mark.asyncio
async def test_pipelines_upsert_resets_on_retry(tmp_path):
    p = tmp_path / "active.db"
    async with db.open_active_db(str(p)) as conn:
        store = PipelinesStore(conn)
        rk = "rk1"
        run_id = await store.upsert_running(
            run_key=rk,
            task="t",
            pipeline_name="p",
            cycle_anchor="c",
            trigger="cron",
            started_at="2026-05-03T08:00:00+00:00",
        )
        await store.finalize(
            run_id,
            finished_at="2026-05-03T08:05:00+00:00",
            status="failed",
            items_in=10,
            items_out=0,
            error_msg="boom",
        )
        await conn.commit()
        run_id_again = await store.upsert_running(
            run_key=rk,
            task="t",
            pipeline_name="p",
            cycle_anchor="c",
            trigger="cron",
            started_at="2026-05-03T08:30:00+00:00",
        )
        await conn.commit()
        assert run_id_again == run_id
        cur = await conn.execute(
            "SELECT status, started_at, finished_at, items_in, items_out, error_msg "
            "FROM pipelines WHERE id = ?",
            (run_id,),
        )
        st, started, fin, items_in, items_out, err = await cur.fetchone()
        assert st == "running"
        assert started == "2026-05-03T08:30:00+00:00"
        assert fin is None
        assert items_in is None
        assert items_out is None
        assert err is None


@pytest.mark.asyncio
async def test_pipelines_finalize(tmp_path):
    p = tmp_path / "active.db"
    async with db.open_active_db(str(p)) as conn:
        store = PipelinesStore(conn)
        run_id = await store.upsert_running(
            run_key="rk2",
            task="t",
            pipeline_name="p",
            cycle_anchor="c",
            trigger="cron",
            started_at="2026-05-03T08:00:00+00:00",
        )
        await store.finalize(
            run_id,
            finished_at="2026-05-03T08:05:00+00:00",
            status="success",
            items_in=10,
            items_out=8,
            error_msg=None,
        )
        await conn.commit()
        cur = await conn.execute(
            "SELECT status, items_out FROM pipelines WHERE id = ?", (run_id,)
        )
        st, out = await cur.fetchone()
        assert st == "success" and out == 8


@pytest.mark.asyncio
async def test_stages_insert_run(tmp_path):
    p = tmp_path / "active.db"
    async with db.open_active_db(str(p)) as conn:
        ps = PipelinesStore(conn)
        ss = StagesStore(conn)
        run_id = await ps.upsert_running(
            run_key="rk",
            task="t",
            pipeline_name="p",
            cycle_anchor="c",
            trigger="cron",
            started_at="2026-05-03T00:00:00+00:00",
        )
        sid = await ss.start(
            pipeline_run_id=run_id,
            stage_name="dd",
            started_at="2026-05-03T00:01:00+00:00",
        )
        await ss.finalize(
            sid,
            finished_at="2026-05-03T00:02:00+00:00",
            status="success",
            items_in=10,
            items_out=9,
            error_msg=None,
        )
        await conn.commit()
        cur = await conn.execute("SELECT status, items_out FROM stages WHERE id = ?", (sid,))
        st, n = await cur.fetchone()
        assert st == "success" and n == 9


@pytest.mark.asyncio
async def test_item_seen_insert_and_filter_seen(tmp_path):
    p = tmp_path / "seen.db"
    async with db.open_seen_db(str(p)) as conn:
        store = ItemSeenStore(conn)
        await store.insert_many(
            [
                ("a", "url-a"),
                ("b", "url-b"),
            ],
            archived_at="2026-05-03T00:00:00+00:00",
        )
        await conn.commit()
        seen = await store.filter_seen({"a", "b", "c"})
    assert seen == {"a", "b"}


@pytest.mark.asyncio
async def test_item_seen_insert_or_ignore_duplicates(tmp_path):
    p = tmp_path / "seen.db"
    async with db.open_seen_db(str(p)) as conn:
        store = ItemSeenStore(conn)
        await store.insert_many([("a", "url-a")], archived_at="t1")
        await store.insert_many([("a", "url-a")], archived_at="t2")
        await conn.commit()
        cur = await conn.execute("SELECT archived_at FROM item_seen WHERE item_id='a'")
        assert (await cur.fetchone())[0] == "t1"


@pytest.mark.asyncio
async def test_item_seen_empty_filter(tmp_path):
    p = tmp_path / "seen.db"
    async with db.open_seen_db(str(p)) as conn:
        store = ItemSeenStore(conn)
        assert await store.filter_seen(set()) == set()
