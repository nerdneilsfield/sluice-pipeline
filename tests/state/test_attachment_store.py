import asyncio
import hashlib
from datetime import datetime, timezone

import pytest

from sluice.state.attachment_store import AttachmentStore
from sluice.state.db import open_db
from sluice.url_canon import canonical_url


@pytest.mark.asyncio
async def test_put_creates_row_and_file(tmp_path):
    base = tmp_path / "att"
    base.mkdir()
    async with open_db(tmp_path / "s.db") as db:
        store = AttachmentStore(db, base_dir=base)
        local_path = await store.put_bytes(
            url="https://example.com/a.jpg",
            mime_type="image/jpeg",
            data=b"\xff\xd8\xff",
            pipeline_id="p1",
        )
        assert local_path
        assert (base / local_path).is_file()
        cur = await db.execute("SELECT byte_size FROM attachment_mirror")
        row = await cur.fetchone()
        assert row[0] == 3


@pytest.mark.asyncio
async def test_put_same_url_dedupes(tmp_path):
    base = tmp_path / "att"
    base.mkdir()
    async with open_db(tmp_path / "s.db") as db:
        store = AttachmentStore(db, base_dir=base)
        p1 = await store.put_bytes(
            url="https://x/a", mime_type="image/png", data=b"AAA", pipeline_id="p"
        )
        p2 = await store.put_bytes(
            url="https://x/a", mime_type="image/png", data=b"BBB", pipeline_id="p"
        )
        assert p1 == p2
        assert (base / p1).read_bytes() == b"AAA"


@pytest.mark.asyncio
async def test_put_stale_row_missing_file_retries_once(tmp_path):
    base = tmp_path / "att"
    base.mkdir()
    async with open_db(tmp_path / "s.db") as db:
        url = "https://x/a"
        canon = canonical_url(url)
        h = hashlib.sha256(canon.encode("utf-8")).hexdigest()
        await db.execute(
            "INSERT INTO attachment_mirror "
            "(url_hash, url, local_path, mime_type, byte_size, fetched_at, "
            " last_referenced_at, pipeline_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                h,
                canon,
                "2099/01/ff/missing.png",
                "image/png",
                3,
                datetime.now(timezone.utc).isoformat(),
                datetime.now(timezone.utc).isoformat(),
                "p",
            ),
        )
        await db.commit()
        store = AttachmentStore(db, base_dir=base)
        local_path = await store.put_bytes(
            url=url, mime_type="image/png", data=b"AAA", pipeline_id="p"
        )
        assert (base / local_path).read_bytes() == b"AAA"
        assert local_path != "2099/01/ff/missing.png"


@pytest.mark.asyncio
async def test_put_updates_last_referenced_at(tmp_path):
    base = tmp_path / "att"
    base.mkdir()
    async with open_db(tmp_path / "s.db") as db:
        store = AttachmentStore(db, base_dir=base)
        await store.put_bytes(
            url="https://x/a", mime_type="image/png", data=b"AAA", pipeline_id="p"
        )
        cur = await db.execute("SELECT last_referenced_at FROM attachment_mirror")
        first = (await cur.fetchone())[0]
        await asyncio.sleep(0.01)
        await store.put_bytes(
            url="https://x/a", mime_type="image/png", data=b"AAA", pipeline_id="p"
        )
        cur = await db.execute("SELECT last_referenced_at FROM attachment_mirror")
        second = (await cur.fetchone())[0]
        assert second > first
