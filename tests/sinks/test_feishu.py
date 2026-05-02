import json

import httpx
import pytest
import respx
from httpx import Response

from sluice.core.errors import ConfigError
from sluice.core.item import Item
from sluice.sinks._feishu_bot import _FEISHU_BASE, FeishuBotClient
from sluice.sinks._feishu_post_render import convert_to_feishu_post
from sluice.sinks._feishu_render import render_to_post_array
from sluice.sinks._markdown_ast import parse_markdown
from sluice.sinks.feishu import FeishuSink, _sign, _truncate_post_array
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


def test_truncate_post_array_truncates_first_oversized_line():
    arr = [[{"tag": "text", "text": "x" * 5000}]]
    truncated = _truncate_post_array(arr, 4000)
    assert len(truncated) == 1
    assert sum(len(seg.get("text", "")) for line in truncated for seg in line) == 4000


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


# ---------------------------------------------------------------------------
# Tests for _feishu_post_render.convert_to_feishu_post
# ---------------------------------------------------------------------------


def test_convert_to_feishu_post_paragraph():
    result = convert_to_feishu_post("hello world")
    zh_cn = result["zh_cn"]
    assert zh_cn["title"] == ""
    content = zh_cn["content"]
    assert len(content) == 1
    row = content[0]
    assert any(seg.get("tag") == "text" for seg in row)
    text = "".join(seg.get("text", "") for seg in row)
    assert "hello world" in text


def test_convert_to_feishu_post_h1_title():
    result = convert_to_feishu_post("# My Title\n\nSome paragraph.")
    zh_cn = result["zh_cn"]
    assert zh_cn["title"] == "My Title"
    # H1 is consumed as title — paragraph remains in content
    content = zh_cn["content"]
    assert len(content) == 1
    text = "".join(seg.get("text", "") for seg in content[0])
    assert "Some paragraph" in text


def test_convert_to_feishu_post_bold_italic():
    result = convert_to_feishu_post("**bold** and *italic*")
    zh_cn = result["zh_cn"]
    content = zh_cn["content"]
    assert len(content) == 1
    row = content[0]
    bold_nodes = [seg for seg in row if "bold" in seg.get("style", [])]
    italic_nodes = [seg for seg in row if "italic" in seg.get("style", [])]
    assert bold_nodes, "expected a bold node"
    assert italic_nodes, "expected an italic node"
    assert bold_nodes[0]["text"] == "bold"
    assert italic_nodes[0]["text"] == "italic"


def test_convert_to_feishu_post_link():
    result = convert_to_feishu_post("[click here](https://example.com)")
    zh_cn = result["zh_cn"]
    content = zh_cn["content"]
    assert len(content) == 1
    row = content[0]
    links = [seg for seg in row if seg.get("tag") == "a"]
    assert links, "expected a link node"
    link = links[0]
    assert link["text"] == "click here"
    assert link["href"] == "https://example.com"


def test_convert_to_feishu_post_code_block():
    md = "```python\nprint('hi')\n```"
    result = convert_to_feishu_post(md)
    zh_cn = result["zh_cn"]
    content = zh_cn["content"]
    assert len(content) == 1
    row = content[0]
    code_nodes = [seg for seg in row if seg.get("tag") == "code_block"]
    assert code_nodes, "expected a code_block node"
    node = code_nodes[0]
    assert node["language"] == "python"
    assert "print" in node["text"]


def test_convert_to_feishu_post_list_as_md():
    md = "- first item\n- second item"
    result = convert_to_feishu_post(md)
    zh_cn = result["zh_cn"]
    content = zh_cn["content"]
    assert len(content) == 1
    row = content[0]
    md_nodes = [seg for seg in row if seg.get("tag") == "md"]
    assert md_nodes, "expected an md node for list"
    text = md_nodes[0]["text"]
    assert "first item" in text
    assert "second item" in text


# ---------------------------------------------------------------------------
# Tests for FeishuBotClient
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_feishu_bot_api_send_post():
    """Mock httpx to verify token fetch + message send."""
    token_url = f"{_FEISHU_BASE}/auth/v3/tenant_access_token/internal"
    send_url = f"{_FEISHU_BASE}/im/v1/messages"

    with respx.mock(assert_all_called=True) as mock:
        mock.post(token_url).mock(
            return_value=Response(
                200,
                json={
                    "code": 0,
                    "tenant_access_token": "test_token_abc",
                    "expire": 7200,
                },
            )
        )
        mock.post(send_url).mock(
            return_value=Response(
                200,
                json={
                    "code": 0,
                    "data": {"message_id": "msg_123"},
                },
            )
        )

        async with httpx.AsyncClient() as http_client:
            bot = FeishuBotClient("app1", "secret1", client=http_client)
            post_content = {
                "zh_cn": {"title": "Test", "content": [[{"tag": "text", "text": "hello"}]]}
            }
            msg_id = await bot.send_post("chat_001", "chat_id", post_content)
            assert msg_id == "msg_123"

        # Verify Authorization header was sent
        send_request = mock.calls[1].request
        assert "Bearer test_token_abc" in send_request.headers.get("authorization", "")

        # Verify receive_id_type query param
        assert "receive_id_type=chat_id" in str(send_request.url)


# ---------------------------------------------------------------------------
# Test FeishuSink in bot_api mode
# ---------------------------------------------------------------------------


def test_feishu_sink_bot_api_mode_builds_batch():
    """FeishuSink with auth_mode=bot_api should build correct batch payloads."""
    items = [
        Item(
            source_id="s",
            pipeline_id="p",
            guid="1",
            url="http://example.com",
            title="Article Title",
            published_at=None,
            raw_summary=None,
        )
    ]
    ctx = make_ctx(items=items)

    sink = FeishuSink(
        sink_id="f",
        webhook_url="http://unused",
        secret=None,
        brief_input=None,
        items_input="items",
        items_template_str="# {{ item.title }}\n\n{{ item.url }}",
        split="per_item",
        message_type="post",
        on_message_too_long="truncate",
        card_template_str="",
        footer_template="",
        between_messages_delay_seconds=0.0,
        delivery_log=None,
        auth_mode="bot_api",
        app_id="test_app_id",
        app_secret="test_app_secret",
        receive_id="chat_abc",
        receive_id_type="chat_id",
    )

    batch = sink.build_batch(ctx)
    assert len(batch) >= 1

    # Each payload should be a post_content dict with zh_cn key
    for item in batch:
        payload = item.payload
        assert "zh_cn" in payload, f"expected 'zh_cn' key in payload, got {list(payload.keys())}"
        zh_cn = payload["zh_cn"]
        assert "title" in zh_cn
        assert "content" in zh_cn
        # Title should come from H1
        assert zh_cn["title"] == "Article Title"
