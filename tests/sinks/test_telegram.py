import re

import pytest
import respx
from httpx import Response

from sluice.core.item import Item
from sluice.sinks._markdown_ast import parse_markdown
from sluice.sinks._telegram_render import _escape as _escape_markdown_v2
from sluice.sinks._telegram_render import render_to_markdown_v2
from sluice.sinks.telegram import TelegramSink, _safe_truncate, _split_md_to_payloads
from sluice.state.db import open_db
from sluice.state.delivery_log import DeliveryLog
from sluice.state.emissions import EmissionStore
from tests.conftest import make_ctx


def _unescaped_exact_runs(text: str, marker: str, width: int) -> list[str]:
    import re

    escaped = re.escape(marker)
    return re.findall(rf"(?<!\\){escaped}{{{width}}}(?!{escaped})", text)


def test_markdown_v2_escape():
    assert _escape_markdown_v2("hello.world!") == "hello\\.world\\!"
    assert _escape_markdown_v2("a*b_c") == "a\\*b\\_c"


def test_safe_truncate_does_not_split_escape():
    text = "ab\\*cd"
    assert _safe_truncate(text, 4) == "ab…"
    assert _safe_truncate("plain", 10) == "plain"
    assert _safe_truncate("toolong_" * 100, 10).endswith("…")


def test_safe_truncate_balances_bold():
    text = "*bold " + "x" * 5000 + "*"
    truncated = _safe_truncate(text, 100)
    assert truncated.endswith("…")
    assert truncated.count("*") % 2 == 0


def test_safe_truncate_balances_mixed_star_runs():
    text = "**bold " + "x" * 200 + "** *italic"
    truncated = _safe_truncate(text, 80)
    assert truncated.endswith("…")
    assert len(_unescaped_exact_runs(truncated, "*", 1)) % 2 == 0
    assert len(_unescaped_exact_runs(truncated, "*", 2)) % 2 == 0


def test_safe_truncate_ignores_escaped_star_markers():
    text = r"\*literal\* *italic " + "x" * 200
    truncated = _safe_truncate(text, 80)
    assert r"\*literal\*" in truncated
    assert len(_unescaped_exact_runs(truncated, "*", 1)) % 2 == 0


def test_safe_truncate_removes_incomplete_link_url():
    text = "[label](https://example.com/" + "x" * 200 + ")"
    truncated = _safe_truncate(text, 40)
    assert not re.search(r"\[[^\]]*\]\([^)]*$", truncated)
    assert truncated == "…"


def test_safe_truncate_balances_italic_and_code():
    italic = _safe_truncate("_italic " + "x" * 200 + "_", 80)
    code = _safe_truncate("`code " + "x" * 200 + "`", 80)
    assert len(_unescaped_exact_runs(italic, "_", 1)) % 2 == 0
    assert len(_unescaped_exact_runs(code, "`", 1)) % 2 == 0


def test_render_link_preserved():
    toks = parse_markdown("[text](http://x)")
    out = render_to_markdown_v2(toks)
    assert "[text](http://x)" in out


def test_render_link_with_double_underscore():
    toks = parse_markdown("[label](https://example.com/path__double)")
    out = render_to_markdown_v2(toks)
    assert "path__double" in out
    assert "\\_\\_" not in out
    assert "[label](https://example.com/path__double)" in out


def test_split_md_single_mode_combines():
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
    log = DeliveryLog.__new__(DeliveryLog)
    sink = TelegramSink(
        sink_id="tg",
        bot_token="T",
        chat_id="@x",
        brief_input=None,
        items_input="items",
        items_template_str="{{ item.title }}",
        split="single",
        link_preview_disabled=True,
        footer_template="",
        on_message_too_long="truncate",
        between_messages_delay_seconds=0.0,
        delivery_log=log,
    )
    batch = sink.build_batch(ctx)
    assert len(batch) == 1
    assert "one" in batch[0].payload
    assert "two" in batch[0].payload


def test_split_md_per_item_mode_separates():
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
    log = DeliveryLog.__new__(DeliveryLog)
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
    batch = sink.build_batch(ctx)
    assert len(batch) == 2


def test_split_md_brief_split_more():
    paragraphs = [f"paragraph {i} " + "x" * 200 for i in range(40)]
    long_brief = "\n\n".join(paragraphs)
    ctx = make_ctx(items=[], context={"markdown": long_brief})
    log = DeliveryLog.__new__(DeliveryLog)
    sink = TelegramSink(
        sink_id="tg",
        bot_token="T",
        chat_id="@x",
        brief_input="context.markdown",
        items_input="none",
        items_template_str="",
        split="per_item",
        link_preview_disabled=True,
        footer_template="",
        on_message_too_long="split_more",
        between_messages_delay_seconds=0.0,
        delivery_log=log,
    )
    batch = sink.build_batch(ctx)
    assert len(batch) > 1
    for item in batch:
        assert len(item.payload) <= 4097


def test_split_md_oversized_block_still_under_limit():
    md = "*bold " + "x" * 5000 + "*"
    payloads = _split_md_to_payloads(md, "", "split_more")
    for p in payloads:
        assert len(p.payload) <= 4097


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
