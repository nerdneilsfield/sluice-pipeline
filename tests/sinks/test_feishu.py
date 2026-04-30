import json

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


def test_single_mode_combines_items():
    items = [
        Item(
            source_id="s",
            pipeline_id="p",
            guid="1",
            url="u1",
            title="one",
            published_at=None,
            raw_summary=None,
        ),
        Item(
            source_id="s",
            pipeline_id="p",
            guid="2",
            url="u2",
            title="two",
            published_at=None,
            raw_summary=None,
        ),
    ]
    ctx = make_ctx(items=items)
    sink = FeishuSink(
        sink_id="f",
        webhook_url="http://wh/x",
        secret=None,
        brief_input=None,
        items_input="items",
        items_template_str="{{ item.title }}",
        split="single",
        message_type="text",
        on_message_too_long="truncate",
        card_template_str="",
        footer_template="",
        between_messages_delay_seconds=0.0,
        delivery_log=None,
    )
    batch = sink.build_batch(ctx)
    assert len(batch) == 1
    payload = json.loads(json.dumps(batch[0].payload))
    assert "one" in payload["content"]["text"]
    assert "two" in payload["content"]["text"]


def test_per_item_mode_separates():
    items = [
        Item(
            source_id="s",
            pipeline_id="p",
            guid="1",
            url="u1",
            title="one",
            published_at=None,
            raw_summary=None,
        ),
        Item(
            source_id="s",
            pipeline_id="p",
            guid="2",
            url="u2",
            title="two",
            published_at=None,
            raw_summary=None,
        ),
    ]
    ctx = make_ctx(items=items)
    sink = FeishuSink(
        sink_id="f",
        webhook_url="http://wh/x",
        secret=None,
        brief_input=None,
        items_input="items",
        items_template_str="{{ item.title }}",
        split="per_item",
        message_type="text",
        on_message_too_long="truncate",
        card_template_str="",
        footer_template="",
        between_messages_delay_seconds=0.0,
        delivery_log=None,
    )
    batch = sink.build_batch(ctx)
    assert len(batch) == 2


def test_post_split_more_chunks():
    long_md = "\n\n".join(f"paragraph {i} " + "x" * 500 for i in range(20))
    ctx = make_ctx(items=[], context={"brief": long_md})
    sink = FeishuSink(
        sink_id="f",
        webhook_url="http://wh/x",
        secret=None,
        brief_input="context.brief",
        items_input="none",
        items_template_str="",
        split="per_item",
        message_type="post",
        on_message_too_long="split_more",
        card_template_str="",
        footer_template="",
        between_messages_delay_seconds=0.0,
        delivery_log=None,
    )
    batch = sink.build_batch(ctx)
    assert len(batch) > 1
    for item in batch:
        assert item.payload["msg_type"] == "post"


def test_post_split_more_splits_single_oversized_line():
    ctx = make_ctx(items=[], context={"brief": "x" * 6000})
    sink = FeishuSink(
        sink_id="f",
        webhook_url="http://wh/x",
        secret=None,
        brief_input="context.brief",
        items_input="none",
        items_template_str="",
        split="per_item",
        message_type="post",
        on_message_too_long="split_more",
        card_template_str="",
        footer_template="",
        between_messages_delay_seconds=0.0,
        delivery_log=None,
    )
    batch = sink.build_batch(ctx)
    assert len(batch) > 1
    for item in batch:
        arr = item.payload["content"]["post"]["zh_cn"]["content"]
        char_count = sum(len(seg.get("text", "")) for line in arr for seg in line)
        assert char_count <= 4000


def test_text_split_more_chunks():
    long_text = "word " * 3000
    ctx = make_ctx(items=[], context={"brief": long_text})
    sink = FeishuSink(
        sink_id="f",
        webhook_url="http://wh/x",
        secret=None,
        brief_input="context.brief",
        items_input="none",
        items_template_str="",
        split="per_item",
        message_type="text",
        on_message_too_long="split_more",
        card_template_str="",
        footer_template="",
        between_messages_delay_seconds=0.0,
        delivery_log=None,
    )
    batch = sink.build_batch(ctx)
    assert len(batch) > 1
    for item in batch:
        payload = item.payload
        assert payload["msg_type"] == "text"
        assert len(payload["content"]["text"]) <= 4001


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
    data = json.loads(body)
    assert "timestamp" in data
    assert "sign" in data
