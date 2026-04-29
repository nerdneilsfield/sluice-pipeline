import pytest

from sluice.core.item import Item
from sluice.sinks._email_render import render_to_html
from sluice.sinks._markdown_ast import parse_markdown
from sluice.sinks.email import EmailSink
from sluice.state.db import open_db
from sluice.state.delivery_log import DeliveryLog
from sluice.state.emissions import EmissionStore
from tests.conftest import make_ctx


def test_render_html_paragraph():
    toks = parse_markdown("hello\n\n**bold**")
    html = render_to_html(toks)
    assert "<p>" in html
    assert "<strong>" in html


@pytest.mark.asyncio
async def test_email_fail_fast_raises_on_first_failure(tmp_path, monkeypatch):
    async with open_db(tmp_path / "s.db"):
        pass
    log = DeliveryLog(db_path=str(tmp_path / "s.db"))

    async def fake_send(*a, **kw):
        raise RuntimeError("smtp blew up")

    monkeypatch.setattr("sluice.sinks.email._aiosmtplib_send", fake_send)

    sink = EmailSink(
        sink_id="em",
        smtp_host="x",
        smtp_port=587,
        smtp_username="u",
        smtp_password="p",
        smtp_starttls=True,
        from_address="from@x",
        recipients=["a@x", "b@x"],
        subject_template="s",
        brief_input=None,
        items_input="items",
        items_template_str="{{ item.title }}",
        split="single",
        html_template_str="<html>{{ body_html }}</html>",
        style_block="",
        footer_template="",
        attach_run_log=False,
        recipient_failure_policy="fail_fast",
        delivery_log=log,
    )
    item = Item(
        source_id="s",
        pipeline_id="p",
        guid="g",
        url="u",
        title="t",
        published_at=None,
        raw_summary=None,
    )
    ctx = make_ctx(items=[item])
    async with open_db(tmp_path / "s.db") as db:
        emissions = EmissionStore(db)
        with pytest.raises(RuntimeError):
            await sink.emit(ctx, emissions=emissions)
    async with open_db(tmp_path / "s.db") as db:
        cur = await db.execute("SELECT status FROM sink_delivery_log")
        statuses = [r[0] for r in await cur.fetchall()]
        assert "failed" in statuses


@pytest.mark.asyncio
async def test_email_best_effort_logs_and_succeeds(tmp_path, monkeypatch):
    async with open_db(tmp_path / "s.db"):
        pass
    log = DeliveryLog(db_path=str(tmp_path / "s.db"))

    async def fake_send(*a, recipient=None, **kw):
        if recipient == "a@x":
            raise RuntimeError("nope")
        return "smtp-id-1"

    monkeypatch.setattr("sluice.sinks.email._aiosmtplib_send", fake_send)
    sink = EmailSink(
        sink_id="em",
        smtp_host="x",
        smtp_port=587,
        smtp_username="u",
        smtp_password="p",
        smtp_starttls=True,
        from_address="from@x",
        recipients=["a@x", "b@x"],
        subject_template="s",
        brief_input=None,
        items_input="items",
        items_template_str="{{ item.title }}",
        split="single",
        html_template_str="<html>{{ body_html }}</html>",
        style_block="",
        footer_template="",
        attach_run_log=False,
        recipient_failure_policy="best_effort",
        delivery_log=log,
    )
    item = Item(
        source_id="s",
        pipeline_id="p",
        guid="g",
        url="u",
        title="t",
        published_at=None,
        raw_summary=None,
    )
    ctx = make_ctx(items=[item])
    async with open_db(tmp_path / "s.db") as db:
        emissions = EmissionStore(db)
        res = await sink.emit(ctx, emissions=emissions)
    assert res.external_id == "smtp-id-1"
    async with open_db(tmp_path / "s.db") as db:
        cur = await db.execute("SELECT recipient, status FROM sink_delivery_log ORDER BY id")
        rows = await cur.fetchall()
        assert ("a@x", "failed") in rows
        assert ("b@x", "success") in rows
