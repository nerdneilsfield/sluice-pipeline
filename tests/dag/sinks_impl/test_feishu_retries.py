from __future__ import annotations

import pytest

from sluice.dag.sinks._impl import feishu
from sluice.dag.sinks._impl.feishu import FeishuSink


@pytest.mark.asyncio
async def test_bot_api_429_retry_after_ignores_error_code(monkeypatch):
    class FakeBot:
        async def send_text(self, receive_id, receive_id_type, text):
            raise RuntimeError("11232 Too Many Requests retry_after 0.5")

    monkeypatch.setattr(feishu.time, "monotonic", lambda: 100.0)
    sink = FeishuSink(
        sink_id="fs",
        webhook_url="",
        secret="",
        brief_input="",
        items_input="none",
        items_template_str="{{ item.title }}",
        split="single",
        message_type="text",
        on_message_too_long="truncate",
        card_template_str="",
        footer_template="",
        between_messages_delay_seconds=0,
        delivery_log=None,
        auth_mode="bot_api",
        app_id="app",
        app_secret="secret",
        receive_id="chat",
    )
    sink._bot = FakeBot()

    with pytest.raises(RuntimeError, match="Too Many Requests"):
        await sink.send_one({"content": {"text": "hello"}})

    assert sink._next_send_mono == 100.5
