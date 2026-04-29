import asyncio
import base64
import hashlib
import hmac
import json
import time

import httpx
from jinja2 import Template

from sluice.context import PipelineContext
from sluice.core.errors import ConfigError
from sluice.sinks._feishu_render import render_to_post_array
from sluice.sinks._markdown_ast import parse_markdown
from sluice.sinks._push_base import PushBatchItem, PushSinkBase

_FEISHU_MAX = 30000


def _sign(secret: str, ts: int) -> str:
    string_to_sign = f"{ts}\n{secret}"
    sig = hmac.new(string_to_sign.encode(), digestmod=hashlib.sha256).digest()
    return base64.b64encode(sig).decode()


class FeishuSink(PushSinkBase):
    type = "feishu"

    def __init__(
        self,
        *,
        sink_id,
        webhook_url,
        secret,
        brief_input,
        items_input,
        items_template_str,
        split,
        message_type,
        on_message_too_long,
        card_template_str,
        footer_template,
        between_messages_delay_seconds,
        delivery_log,
        client: httpx.AsyncClient | None = None,
    ):
        if message_type == "interactive" and on_message_too_long == "split_more":
            raise ConfigError(
                "feishu interactive (Card V2) mode does not support on_message_too_long=split_more"
            )
        super().__init__(
            sink_id=sink_id, footer_template=footer_template, delivery_log=delivery_log
        )
        self._url = webhook_url
        self._secret = secret
        self._brief_input = brief_input
        self._items_input = items_input
        self._tmpl = Template(items_template_str)
        self._split = split
        self._mtype = message_type
        self._too_long = on_message_too_long
        self._card_tmpl = Template(card_template_str) if card_template_str else None
        self._delay = between_messages_delay_seconds
        self._client = client or httpx.AsyncClient(timeout=30.0)

    def _build_payload_post(self, md: str) -> dict:
        toks = parse_markdown(md)
        arr = render_to_post_array(toks)
        return {"msg_type": "post", "content": {"post": {"zh_cn": {"title": "", "content": arr}}}}

    def _build_payload_text(self, md: str) -> dict:
        toks = parse_markdown(md)
        out = []
        for tok in toks:
            if tok.type == "text":
                out.append(tok.content)
            elif tok.type == "inline":
                for c in tok.children or []:
                    if c.type == "text":
                        out.append(c.content)
        return {"msg_type": "text", "content": {"text": "".join(out)}}

    def _build_payload_card(self, md: str, ctx: PipelineContext) -> dict:
        toks = parse_markdown(md)
        arr = render_to_post_array(toks)
        rendered = self._card_tmpl.render(
            pipeline_id=ctx.pipeline_id,
            run_date=ctx.run_date,
            body_post_array=arr,
        )
        card = json.loads(rendered)
        return {"msg_type": "interactive", "card": card}

    def build_batch(self, ctx: PipelineContext) -> list[PushBatchItem]:
        out: list[PushBatchItem] = []
        items = ctx.items if self._items_input == "items" else []
        for it in items:
            md = self._tmpl.render(item=it, ctx=ctx)
            if self._mtype == "post":
                payload = self._build_payload_post(md)
            elif self._mtype == "text":
                payload = self._build_payload_text(md)
            else:
                payload = self._build_payload_card(md, ctx)
            out.append(PushBatchItem(kind="item", payload=payload))
        return out

    async def send_one(self, payload, recipient=None) -> str:
        body = dict(payload)
        if self._secret:
            ts = int(time.time())
            body["timestamp"] = str(ts)
            body["sign"] = _sign(self._secret, ts)
        resp = await self._client.post(self._url, json=body)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code", 0) != 0:
            raise RuntimeError(f"feishu webhook error: {data}")
        if self._delay > 0:
            await asyncio.sleep(self._delay)
        return f"feishu:{int(time.time())}"
