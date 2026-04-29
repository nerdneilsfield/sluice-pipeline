import pytest
import respx
from httpx import Response

from sluice.core.item import Item
from sluice.sinks._markdown_ast import parse_markdown
from sluice.sinks._telegram_render import _escape as _escape_markdown_v2
from sluice.sinks._telegram_render import render_to_markdown_v2
from sluice.sinks.telegram import TelegramSink
from sluice.state.db import open_db
from sluice.state.delivery_log import DeliveryLog
from sluice.state.emissions import EmissionStore
from tests.conftest import make_ctx


def test_markdown_v2_escape():
    assert _escape_markdown_v2("hello.world!") == "hello\\.world\\!"
    assert _escape_markdown_v2("a*b_c") == "a\\*b\\_c"


def test_render_link_preserved():
    toks = parse_markdown("[text](http://x)")
    out = render_to_markdown_v2(toks)
    assert "[text](http://x)" in out


@pytest.mark.asyncio
@respx.mock
async def test_telegram_send_and_audit(tmp_path):
    respx.post("https://api.telegram.org/botT/sendMessage").mock(
        return_value=Response(200, json={"ok": True, "result": {"message_id": 99}})
    )
    async with open_db(tmp_path / "s.db"):
        pass
    log = DeliveryLog(db_path=str(tmp_path / "s.db"))
    sink = TelegramSink(
        sink_id="tg",
        bot_token="T",
        chat_id="@x",
        brief_input=None,
        items_input="items",
        items_template_str="{{ item.title }}",
        split="per_item",
        link_preview_disabled=True,
        footer_template="",
        on_message_too_long="truncate",
        between_messages_delay_seconds=0.0,
        delivery_log=log,
    )
    item = Item(
        source_id="s",
        pipeline_id="p",
        guid="g",
        url="u",
        title="hello",
        published_at=None,
        raw_summary=None,
    )
    ctx = make_ctx(items=[item])
    async with open_db(tmp_path / "s.db") as db:
        emissions = EmissionStore(db)
        res = await sink.emit(ctx, emissions=emissions)
    assert res.external_id == "@x:99"
    sent = respx.calls.last.request
    body = sent.read().decode()
    assert "link_preview_options" in body
