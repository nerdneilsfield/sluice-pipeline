import pytest
import respx
from httpx import Response

from sluice.core.errors import ConfigError
from sluice.core.item import Item
from sluice.sinks._feishu_render import render_to_post_array
from sluice.sinks._markdown_ast import parse_markdown
from sluice.sinks.feishu import FeishuSink, _sign
from sluice.state.db import open_db
from sluice.state.delivery_log import DeliveryLog
from sluice.state.emissions import EmissionStore
from tests.conftest import make_ctx


def test_render_post_array_paragraph():
    toks = parse_markdown("hello world")
    arr = render_to_post_array(toks)
    assert any(seg.get("tag") == "text" for line in arr for seg in line)


def test_card_v2_split_more_rejected():
    with pytest.raises(ConfigError):
        FeishuSink(
            sink_id="f",
            webhook_url="http://x",
            secret=None,
            brief_input=None,
            items_input="items",
            items_template_str="{{ item.title }}",
            split="per_item",
            message_type="interactive",
            on_message_too_long="split_more",
            card_template_str="{}",
            footer_template="",
            between_messages_delay_seconds=0.0,
            delivery_log=None,
        )


def test_sign_deterministic_and_nonempty():
    ts = 1700000000
    s1 = _sign("my_secret", ts)
    s2 = _sign("my_secret", ts)
    assert s1 == s2
    assert len(s1) > 0
    assert _sign("other", ts) != s1


@pytest.mark.asyncio
@respx.mock
async def test_feishu_post_send(tmp_path):
    respx.post("http://wh/x").mock(return_value=Response(200, json={"code": 0, "msg": "ok"}))
    async with open_db(tmp_path / "s.db"):
        pass
    log = DeliveryLog(db_path=str(tmp_path / "s.db"))
    sink = FeishuSink(
        sink_id="f",
        webhook_url="http://wh/x",
        secret=None,
        brief_input=None,
        items_input="items",
        items_template_str="{{ item.title }}",
        split="per_item",
        message_type="post",
        on_message_too_long="truncate",
        card_template_str="",
        footer_template="",
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
    assert res.external_id


@pytest.mark.asyncio
@respx.mock
async def test_feishu_send_with_secret(tmp_path):
    respx.post("http://wh/x").mock(return_value=Response(200, json={"code": 0, "msg": "ok"}))
    async with open_db(tmp_path / "s.db"):
        pass
    log = DeliveryLog(db_path=str(tmp_path / "s.db"))
    sink = FeishuSink(
        sink_id="f",
        webhook_url="http://wh/x",
        secret="test_secret",
        brief_input=None,
        items_input="items",
        items_template_str="{{ item.title }}",
        split="per_item",
        message_type="post",
        on_message_too_long="truncate",
        card_template_str="",
        footer_template="",
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
    assert res.external_id
    sent = respx.calls.last.request
    body = sent.read().decode()
    import json

    data = json.loads(body)
    assert "timestamp" in data
    assert "sign" in data
