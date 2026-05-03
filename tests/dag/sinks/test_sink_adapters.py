from __future__ import annotations

import subprocess
import sys

import pytest

import sluice.dag.sinks  # noqa: F401
from sluice.dag.sinks.base import get_sink_class
from sluice.dag.sinks.email_sink import EmailDagSink
from sluice.dag.sinks.feishu_sink import FeishuDagSink
from sluice.dag.sinks.file_md import FileMdSink
from sluice.dag.sinks.notion_sink import NotionDagSink
from sluice.dag.sinks.telegram_sink import TelegramDagSink


class _NotionAdapter:
    async def create_page(self, *, parent_id, parent_type, title, properties, markdown):
        assert parent_id == "parent"
        assert parent_type == "page"
        assert title == "P c"
        assert markdown == "# hello"
        return "page-1"

    async def replace_page_blocks(self, page_id, markdown):  # pragma: no cover - create path only
        raise AssertionError("not used")


@pytest.mark.asyncio
async def test_importing_sink_package_does_not_eagerly_import_optional_adapters():
    code = (
        "import sys; import sluice.dag.sinks; "
        "assert 'sluice.dag.sinks.email_sink' not in sys.modules; "
        "assert 'sluice.sinks.email' not in sys.modules"
    )
    result = subprocess.run([sys.executable, "-c", code], check=False)
    assert result.returncode == 0


@pytest.mark.asyncio
async def test_builtin_sink_adapters_are_bound():
    assert get_sink_class("email") is EmailDagSink
    assert get_sink_class("telegram") is TelegramDagSink
    assert get_sink_class("feishu") is FeishuDagSink
    assert get_sink_class("notion") is NotionDagSink


@pytest.mark.asyncio
async def test_telegram_adapter_chunks_rendered_markdown_and_emits(monkeypatch):
    async def fake_send(self, payload, recipient=None):
        assert "hello" in payload
        return "tg:1"

    monkeypatch.setattr("sluice.sinks.telegram.TelegramSink.send_one", fake_send)
    sink = TelegramDagSink()
    params = {
        "sink_id": "tg",
        "bot_token": "token",
        "chat_id": "chat",
        "pipeline": "P",
        "run_key": "rk",
        "cycle_anchor": "c",
    }
    chunks = await sink.chunks("# hello", params)
    result = await sink.emit(chunks[0], params)

    assert chunks[0].ordinal == 1
    assert chunks[0].message_kind in {"item", "chunk"}
    assert result.success and result.external_id == "tg:1"


@pytest.mark.asyncio
async def test_telegram_adapter_resolves_footer_template_path_and_run_key(tmp_path):
    (tmp_path / "footer.md").write_text("KEY {{ run_key }}", encoding="utf-8")
    sink = TelegramDagSink()
    params = {
        "sink_id": "tg",
        "bot_token": "token",
        "chat_id": "chat",
        "pipeline": "P",
        "run_key": "rk",
        "cycle_anchor": "c",
        "footer_template": "footer.md",
        "config_root": str(tmp_path),
    }
    chunks = await sink.chunks("# hello", params)

    assert "rk" in chunks[0].payload.payload


@pytest.mark.asyncio
async def test_email_adapter_chunks_one_message_per_recipient_and_emits(monkeypatch):
    async def fake_send(self, payload, recipient=None):
        assert recipient in {"a@example.com", "b@example.com"}
        assert payload["To"] == recipient
        return f"mail:{recipient}"

    monkeypatch.setattr("sluice.sinks.email.EmailSink.send_one", fake_send)
    sink = EmailDagSink()
    params = {
        "sink_id": "mail",
        "smtp_host": "smtp.example.com",
        "from_address": "from@example.com",
        "recipients": ["a@example.com", "b@example.com"],
        "pipeline": "P",
        "run_key": "rk",
        "cycle_anchor": "c",
    }
    chunks = await sink.chunks("# hello", params)
    result = await sink.emit(chunks[1], params)

    assert [chunk.ordinal for chunk in chunks] == [1, 2]
    assert result.external_id == "mail:b@example.com"
    assert result.recipient == "b@example.com"


@pytest.mark.asyncio
async def test_email_adapter_resolves_template_paths(tmp_path):
    (tmp_path / "body.html").write_text(
        "BODY {{ body_html }} STYLE {{ style_block }}",
        encoding="utf-8",
    )
    (tmp_path / "style.css").write_text(".x{}", encoding="utf-8")
    sink = EmailDagSink()
    params = {
        "sink_id": "mail",
        "smtp_host": "smtp.example.com",
        "from_address": "from@example.com",
        "recipients": ["a@example.com"],
        "html_template": "body.html",
        "style_block_file": "style.css",
        "pipeline": "P",
        "run_key": "rk",
        "cycle_anchor": "c",
        "config_root": str(tmp_path),
    }
    chunks = await sink.chunks("# hello", params)
    html = chunks[0].payload.payload.get_payload()[1].get_payload(decode=True).decode()

    assert "BODY" in html
    assert ".x{}" in html


@pytest.mark.asyncio
async def test_feishu_adapter_chunks_markdown_and_emits(monkeypatch):
    async def fake_send(self, payload, recipient=None):
        assert payload["msg_type"] == "text"
        assert "hello" in payload["content"]["text"]
        return "fs:1"

    monkeypatch.setattr("sluice.sinks.feishu.FeishuSink.send_one", fake_send)
    sink = FeishuDagSink()
    params = {
        "sink_id": "fs",
        "webhook_url": "https://example.com/hook",
        "message_type": "text",
        "pipeline": "P",
        "run_key": "rk",
        "cycle_anchor": "c",
    }
    chunks = await sink.chunks("# hello", params)
    result = await sink.emit(chunks[0], params)

    assert chunks[0].message_kind == "item"
    assert result.external_id == "fs:1"


@pytest.mark.asyncio
async def test_feishu_adapter_resolves_card_template_path(tmp_path):
    (tmp_path / "card.json").write_text('{"config":{},"elements":[]}', encoding="utf-8")
    sink = FeishuDagSink()
    params = {
        "sink_id": "fs",
        "webhook_url": "https://example.com/hook",
        "message_type": "interactive",
        "card_template": "card.json",
        "pipeline": "P",
        "run_key": "rk",
        "cycle_anchor": "c",
        "config_root": str(tmp_path),
    }
    chunks = await sink.chunks("# hello", params)

    assert chunks[0].payload.payload["msg_type"] == "interactive"


@pytest.mark.asyncio
async def test_notion_adapter_uses_rendered_markdown():
    sink = NotionDagSink()
    params = {
        "sink_id": "nt",
        "parent_id": "parent",
        "parent_type": "page",
        "title_template": "{pipeline_id} {run_date}",
        "pipeline": "P",
        "run_key": "rk",
        "cycle_anchor": "c",
        "adapter": _NotionAdapter(),
    }
    chunks = await sink.chunks("# hello", params)
    result = await sink.emit(chunks[0], params)

    assert result.success
    assert result.external_id == "page-1"


@pytest.mark.asyncio
async def test_file_md_adapter_accepts_v1_path_schema(tmp_path):
    sink = FileMdSink()
    chunks = await sink.chunks("# hello", {})
    result = await sink.emit(
        chunks[0],
        {
            "sink_id": "file",
            "path": str(tmp_path / "{pipeline}-{run_key}.md"),
            "pipeline": "P",
            "run_key": "rk",
        },
    )

    assert (tmp_path / "P-rk.md").read_text(encoding="utf-8") == "# hello"
    assert result.external_id.endswith("P-rk.md")
