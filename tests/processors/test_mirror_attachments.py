from unittest.mock import patch

import pytest
import respx
from httpx import Response

from sluice.core.item import Attachment, Item
from sluice.processors.mirror_attachments import MirrorAttachmentsProcessor
from sluice.state.attachment_store import AttachmentStore
from sluice.state.db import open_db
from tests.conftest import make_ctx


def _item_with_attach(url, **extras):
    it = Item(
        source_id="s",
        pipeline_id="p",
        guid="g",
        url="https://item/x",
        title="t",
        published_at=None,
        raw_summary=None,
        attachments=[Attachment(url=url, mime_type="image/jpeg")],
    )
    it.extras.update(extras)
    return it


@pytest.mark.asyncio
@respx.mock
async def test_mirrors_attachment_url_relative_mode(tmp_path):
    respx.get("https://example.com/a.jpg").mock(
        return_value=Response(200, content=b"\xff\xd8\xff", headers={"content-type": "image/jpeg"})
    )
    base = tmp_path / "att"
    base.mkdir()
    async with open_db(tmp_path / "s.db") as db:
        store = AttachmentStore(db, base_dir=base)
        proc = MirrorAttachmentsProcessor(
            name="mi",
            store=store,
            pipeline_id="p",
            mime_prefixes=["image/"],
            max_bytes=10_000,
            on_failure="skip",
            rewrite_fields=[],
            attachment_url_prefix="",
        )
        item = _item_with_attach("https://example.com/a.jpg")
        with patch("sluice.processors.mirror_attachments.guard"):
            out = await proc.process(make_ctx(items=[item]))
        a = out.items[0].attachments[0]
        assert a.local_path is not None
        assert a.url == a.local_path
        assert (base / a.local_path).is_file()


@pytest.mark.asyncio
@respx.mock
async def test_rewrite_fields_extras(tmp_path):
    respx.get("https://x/cover.png").mock(
        return_value=Response(
            200, content=b"\x89PNG\r\n\x1a\n", headers={"content-type": "image/png"}
        )
    )
    base = tmp_path / "att"
    base.mkdir()
    async with open_db(tmp_path / "s.db") as db:
        store = AttachmentStore(db, base_dir=base)
        proc = MirrorAttachmentsProcessor(
            name="mi",
            store=store,
            pipeline_id="p",
            mime_prefixes=["image/"],
            max_bytes=10_000,
            on_failure="skip",
            rewrite_fields=["extras.cover_image"],
            attachment_url_prefix="https://cdn/x",
        )
        item = Item(
            source_id="s",
            pipeline_id="p",
            guid="g",
            url="u",
            title="t",
            published_at=None,
            raw_summary=None,
            attachments=[],
        )
        item.extras["cover_image"] = "https://x/cover.png"
        with patch("sluice.processors.mirror_attachments.guard"):
            out = await proc.process(make_ctx(items=[item]))
        assert out.items[0].extras["cover_image"].startswith("https://cdn/x/")


@pytest.mark.asyncio
@respx.mock
async def test_oversize_skipped_or_failed(tmp_path):
    respx.get("https://big/x.jpg").mock(
        return_value=Response(200, content=b"\x00" * 5000, headers={"content-type": "image/jpeg"})
    )
    base = tmp_path / "att"
    base.mkdir()
    async with open_db(tmp_path / "s.db") as db:
        store = AttachmentStore(db, base_dir=base)
        proc = MirrorAttachmentsProcessor(
            name="mi",
            store=store,
            pipeline_id="p",
            mime_prefixes=["image/"],
            max_bytes=1000,
            on_failure="drop_attachment",
            rewrite_fields=[],
            attachment_url_prefix="",
        )
        item = _item_with_attach("https://big/x.jpg")
        with patch("sluice.processors.mirror_attachments.guard"):
            out = await proc.process(make_ctx(items=[item]))
        assert out.items[0].attachments == []
