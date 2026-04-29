from datetime import datetime, timedelta, timezone

import pytest

from sluice.gc import gc_attachments, gc_failed_items
from sluice.state.db import open_db


def _iso(dt):
    return dt.isoformat(timespec="seconds")


@pytest.mark.asyncio
async def test_gc_failed_items_deletes_old_resolved(tmp_path):
    async with open_db(tmp_path / "s.db") as db:
        old = _iso(datetime.now(timezone.utc) - timedelta(days=200))
        new = _iso(datetime.now(timezone.utc))
        await db.execute(
            "INSERT INTO failed_items (pipeline_id, item_key, url, stage, "
            " error_class, error_msg, attempts, status, item_json, "
            " first_failed_at, last_failed_at) VALUES "
            "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("p", "k1", "u", "s", "E", "m", 1, "resolved", "{}", old, old),
        )
        await db.execute(
            "INSERT INTO failed_items VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("p", "k2", "u", "s", "E", "m", 1, "failed", "{}", new, new),
        )
        await db.commit()
        n = await gc_failed_items(
            db, older_than_iso=_iso(datetime.now(timezone.utc) - timedelta(days=90))
        )
        assert n == 1
        cur = await db.execute("SELECT COUNT(*) FROM failed_items")
        assert (await cur.fetchone())[0] == 1


@pytest.mark.asyncio
async def test_gc_attachments_unlinks_files(tmp_path):
    base = tmp_path / "att"
    base.mkdir()
    f = base / "x.bin"
    f.write_bytes(b"X")
    async with open_db(tmp_path / "s.db") as db:
        old = _iso(datetime.now(timezone.utc) - timedelta(days=60))
        await db.execute(
            "INSERT INTO attachment_mirror VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("h1", "u", "x.bin", "image/png", 1, old, old, "p"),
        )
        await db.commit()
        n = await gc_attachments(
            db, base_dir=base, older_than_iso=_iso(datetime.now(timezone.utc) - timedelta(days=30))
        )
        assert n == 1
        assert not f.exists()
