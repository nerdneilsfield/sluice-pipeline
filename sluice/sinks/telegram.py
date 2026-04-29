import asyncio

import httpx
from jinja2 import Template

from sluice.context import PipelineContext
from sluice.sinks._markdown_ast import parse_markdown, split_tokens
from sluice.sinks._push_base import PushBatchItem, PushSinkBase
from sluice.sinks._telegram_render import (
    _escape as _escape_markdown_v2,
)
from sluice.sinks._telegram_render import (
    render_to_markdown_v2,
)

_TG_MAX = 4096


def _estimate_size(toks):
    return len(render_to_markdown_v2(toks))


class TelegramSink(PushSinkBase):
    type = "telegram"

    def __init__(
        self,
        *,
        sink_id: str,
        bot_token: str,
        chat_id: str,
        brief_input: str | None,
        items_input: str,
        items_template_str: str,
        split: str,
        link_preview_disabled: bool,
        footer_template: str,
        on_message_too_long: str,
        between_messages_delay_seconds: float,
        delivery_log,
        client: httpx.AsyncClient | None = None,
    ):
        super().__init__(
            sink_id=sink_id, footer_template=footer_template, delivery_log=delivery_log
        )
        self._token = bot_token
        self._chat = chat_id
        self._brief_input = brief_input
        self._items_input = items_input
        self._tmpl = Template(items_template_str)
        self._split = split
        self._lpd = link_preview_disabled
        self._too_long = on_message_too_long
        self._delay = between_messages_delay_seconds
        self._client = client or httpx.AsyncClient(timeout=30.0)

    def _render_payload(self, md: str, footer: str | None = None) -> str:
        toks = parse_markdown(md)
        text = render_to_markdown_v2(toks)
        if footer:
            text = text + "\n" + _escape_markdown_v2(footer)
        if len(text) <= _TG_MAX:
            return text
        if self._too_long == "truncate":
            return text[: _TG_MAX - len("…")] + "…"
        if self._too_long == "fail":
            raise RuntimeError(f"telegram message exceeds {_TG_MAX} chars")
        return text

    def build_batch(self, ctx: PipelineContext) -> list[PushBatchItem]:
        out: list[PushBatchItem] = []
        footer = self.render_footer(ctx)
        if self._brief_input:
            key = (
                self._brief_input.split(".", 1)[1]
                if self._brief_input.startswith("context.")
                else ""
            )
            md = ctx.context.get(key, "") if key else ""
            if md:
                out.append(PushBatchItem(kind="brief", payload=self._render_payload(md, footer)))
        items = ctx.items if self._items_input == "items" else []
        for it in items:
            md = self._tmpl.render(item=it, ctx=ctx)
            text = self._render_payload(md, footer)
            if len(text) > _TG_MAX and self._too_long == "split_more":
                toks = parse_markdown(md)
                chunks = split_tokens(toks, _TG_MAX - len(footer) - 8, _estimate_size)
                total = len(chunks)
                for i, c in enumerate(chunks, start=1):
                    body = render_to_markdown_v2(c)
                    chunk_footer = (
                        _escape_markdown_v2(f"{footer} ({i}/{total})")
                        if footer
                        else _escape_markdown_v2(f"({i}/{total})")
                    )
                    out.append(PushBatchItem(kind="chunk", payload=body + "\n" + chunk_footer))
            else:
                out.append(PushBatchItem(kind="item", payload=text))
        return out

    async def send_one(self, payload, recipient=None) -> str:
        url = f"https://api.telegram.org/bot{self._token}/sendMessage"
        body = {
            "chat_id": self._chat,
            "text": payload,
            "parse_mode": "MarkdownV2",
            "link_preview_options": {"is_disabled": self._lpd},
        }
        resp = await self._client.post(url, json=body)
        resp.raise_for_status()
        data = resp.json()
        if not data.get("ok"):
            raise RuntimeError(f"telegram error: {data}")
        if self._delay > 0:
            await asyncio.sleep(self._delay)
        return f"{self._chat}:{data['result']['message_id']}"
