import asyncio
import base64
import hashlib
import hmac
import json
import time
import uuid

import httpx
from jinja2 import Template

from sluice.context import PipelineContext
from sluice.core.errors import ConfigError
from sluice.sinks._feishu_bot import FeishuBotClient
from sluice.sinks._feishu_post_render import convert_to_feishu_post
from sluice.sinks._feishu_render import render_to_post_array
from sluice.sinks._markdown_ast import parse_markdown
from sluice.sinks._push_base import PushBatchItem, PushSinkBase

_FEISHU_MAX_TEXT = 4000
_FEISHU_MAX_POST_CONTENT = 4000


def _sign(secret: str, ts: int) -> str:
    string_to_sign = f"{ts}\n{secret}"
    sig = hmac.new(string_to_sign.encode(), digestmod=hashlib.sha256).digest()
    return base64.b64encode(sig).decode()


def _truncate_post_array(arr: list[list[dict]], max_chars: int) -> list[list[dict]]:
    total = 0
    result: list[list[dict]] = []
    for line in arr:
        line_len = sum(len(seg.get("text", "")) for seg in line)
        if total + line_len > max_chars:
            if not result:
                result.append(_split_post_line(line, max_chars)[0])
            break
        result.append(line)
        total += line_len
    return result


def _truncate_text(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 1] + "…"


def _split_post_line(line: list[dict], max_chars: int) -> list[list[dict]]:
    chunks: list[list[dict]] = []
    cur: list[dict] = []
    cur_chars = 0
    for seg in line:
        text = str(seg.get("text", ""))
        if not text:
            cur.append(seg)
            continue
        pos = 0
        while pos < len(text):
            if cur_chars >= max_chars:
                chunks.append(cur)
                cur = []
                cur_chars = 0
            room = max_chars - cur_chars
            part = text[pos : pos + room]
            next_seg = dict(seg)
            next_seg["text"] = part
            cur.append(next_seg)
            cur_chars += len(part)
            pos += len(part)
    if cur:
        chunks.append(cur)
    return chunks


def _post_content_size(content: list[list[dict]]) -> int:
    return sum(len(seg.get("text", "")) for line in content for seg in line)


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
        emit_on_empty: bool = False,
        client: httpx.AsyncClient | None = None,
        # bot_api params
        auth_mode: str = "webhook",
        app_id: str | None = None,
        app_secret: str | None = None,
        receive_id: str | None = None,
        receive_id_type: str = "chat_id",
    ):
        if message_type == "interactive" and on_message_too_long == "split_more":
            raise ConfigError(
                "feishu interactive (Card V2) mode does not support on_message_too_long=split_more"
            )
        super().__init__(
            sink_id=sink_id,
            footer_template=footer_template,
            delivery_log=delivery_log,
            emit_on_empty=emit_on_empty,
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
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(timeout=30.0)
        self._auth_mode = auth_mode
        self._receive_id = receive_id
        self._receive_id_type = receive_id_type
        self._bot: FeishuBotClient | None = None
        if auth_mode == "bot_api":
            if app_id is None or app_secret is None:
                raise ConfigError("feishu bot_api mode requires app_id and app_secret")
            self._bot = FeishuBotClient(app_id, app_secret, client=self._client)

    async def aclose(self):
        if self._bot is not None:
            await self._bot.aclose()
        if self._owns_client:
            await self._client.aclose()

    # ------------------------------------------------------------------
    # Webhook payload builders
    # ------------------------------------------------------------------

    def _build_payload_post(self, md: str) -> dict | list[dict]:
        toks = parse_markdown(md)
        arr = render_to_post_array(toks)
        total = sum(len(seg.get("text", "")) for line in arr for seg in line)
        if total > _FEISHU_MAX_POST_CONTENT:
            if self._too_long == "truncate":
                arr = _truncate_post_array(arr, _FEISHU_MAX_POST_CONTENT)
            elif self._too_long == "fail":
                raise RuntimeError(f"feishu post content exceeds {_FEISHU_MAX_POST_CONTENT} chars")
            elif self._too_long == "split_more":
                chunks: list[dict] = []
                cur_chars = 0
                cur_lines: list[list[dict]] = []
                for line in arr:
                    for split_line in _split_post_line(line, _FEISHU_MAX_POST_CONTENT):
                        line_len = sum(len(seg.get("text", "")) for seg in split_line)
                        if cur_lines and cur_chars + line_len > _FEISHU_MAX_POST_CONTENT:
                            chunks.append(
                                {
                                    "msg_type": "post",
                                    "content": {
                                        "post": {"zh_cn": {"title": "", "content": cur_lines}}
                                    },
                                }
                            )
                            cur_lines = []
                            cur_chars = 0
                        cur_lines.append(split_line)
                        cur_chars += line_len
                if cur_lines:
                    chunks.append(
                        {
                            "msg_type": "post",
                            "content": {"post": {"zh_cn": {"title": "", "content": cur_lines}}},
                        }
                    )
                return chunks
        return {"msg_type": "post", "content": {"post": {"zh_cn": {"title": "", "content": arr}}}}

    def _build_payload_text(self, md: str) -> dict | list[dict]:
        toks = parse_markdown(md)
        out = []
        for tok in toks:
            if tok.type == "text":
                out.append(tok.content)
            elif tok.type == "inline":
                for c in tok.children or []:
                    if c.type == "text":
                        out.append(c.content)
        text = "".join(out)
        if len(text) > _FEISHU_MAX_TEXT:
            if self._too_long == "truncate":
                text = _truncate_text(text, _FEISHU_MAX_TEXT)
            elif self._too_long == "fail":
                raise RuntimeError(f"feishu text message exceeds {_FEISHU_MAX_TEXT} chars")
            elif self._too_long == "split_more":
                chunks: list[dict] = []
                pos = 0
                while pos < len(text):
                    end = min(pos + _FEISHU_MAX_TEXT, len(text))
                    chunks.append({"msg_type": "text", "content": {"text": text[pos:end]}})
                    pos = end
                return chunks
        return {"msg_type": "text", "content": {"text": text}}

    def _build_payload_card(self, md: str, ctx: PipelineContext) -> dict:
        if self._card_tmpl is None:
            raise ConfigError("feishu interactive mode requires a non-empty card_template")
        toks = parse_markdown(md)
        arr = render_to_post_array(toks)
        rendered = self._card_tmpl.render(
            pipeline_id=ctx.pipeline_id,
            run_date=ctx.run_date,
            body_post_array=arr,
        )
        card = json.loads(rendered)
        return {"msg_type": "interactive", "card": card}

    # ------------------------------------------------------------------
    # Bot API payload builder
    # ------------------------------------------------------------------

    def _build_payload_post_for_bot(self, md: str) -> list[dict]:
        """Build one or more post_content dicts for the bot API.

        Each dict: {"zh_cn": {"title": str, "content": list[list[dict]]}}
        Title is only included in the first chunk.
        """
        post = convert_to_feishu_post(md)
        zh_cn = post["zh_cn"]
        title: str = zh_cn["title"]
        content: list[list[dict]] = zh_cn["content"]

        total = _post_content_size(content)

        if total <= _FEISHU_MAX_POST_CONTENT:
            return [{"zh_cn": {"title": title, "content": content}}]

        if self._too_long == "fail":
            raise RuntimeError(
                f"feishu bot_api post content exceeds {_FEISHU_MAX_POST_CONTENT} chars"
            )

        if self._too_long == "truncate":
            truncated = _truncate_post_array(content, _FEISHU_MAX_POST_CONTENT)
            return [{"zh_cn": {"title": title, "content": truncated}}]

        # split_more
        chunks: list[dict] = []
        cur_lines: list[list[dict]] = []
        cur_chars = 0
        first_chunk = True

        for line in content:
            for split_line in _split_post_line(line, _FEISHU_MAX_POST_CONTENT):
                line_len = sum(len(seg.get("text", "")) for seg in split_line)
                if cur_lines and cur_chars + line_len > _FEISHU_MAX_POST_CONTENT:
                    chunk_title = title if first_chunk else ""
                    chunks.append({"zh_cn": {"title": chunk_title, "content": cur_lines}})
                    cur_lines = []
                    cur_chars = 0
                    first_chunk = False
                cur_lines.append(split_line)
                cur_chars += line_len

        if cur_lines:
            chunk_title = title if first_chunk else ""
            chunks.append({"zh_cn": {"title": chunk_title, "content": cur_lines}})

        return chunks

    # ------------------------------------------------------------------
    # Emit helpers
    # ------------------------------------------------------------------

    def _emit_payload(self, md, ctx):
        if self._auth_mode == "bot_api":
            if self._mtype == "text":
                # Plain text: reuse webhook text builder, but wrap payload differently
                p = self._build_payload_text(md)
                if isinstance(p, list):
                    return [PushBatchItem(kind="chunk", payload=item) for item in p]
                return [PushBatchItem(kind="item", payload=p)]
            else:
                # post
                post_contents = self._build_payload_post_for_bot(md)
                return [PushBatchItem(kind="chunk", payload=pc) for pc in post_contents]

        # webhook mode (original logic)
        if self._mtype == "post":
            p = self._build_payload_post(md)
        elif self._mtype == "text":
            p = self._build_payload_text(md)
        else:
            p = self._build_payload_card(md, ctx)
            return [PushBatchItem(kind="item", payload=p)]
        if isinstance(p, list):
            return [PushBatchItem(kind="chunk", payload=item) for item in p]
        return [PushBatchItem(kind="item", payload=p)]

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
                if footer:
                    md = md + "\n\n" + footer
                out.extend(self._emit_payload(md, ctx))
        items = ctx.items if self._items_input == "items" else []
        total = len(items)
        if self._split == "single" and items:
            parts = [
                self._tmpl.render(item=it, ctx=ctx, index=i, total=total)
                for i, it in enumerate(items, 1)
            ]
            combined = "\n\n".join(parts)
            if footer:
                combined = combined + "\n\n" + footer
            out.extend(self._emit_payload(combined, ctx))
        else:
            for i, it in enumerate(items, 1):
                md = self._tmpl.render(item=it, ctx=ctx, index=i, total=total)
                if footer:
                    md = md + "\n\n" + footer
                out.extend(self._emit_payload(md, ctx))
        return out

    async def send_one(self, payload, recipient=None) -> str:
        if self._auth_mode == "bot_api":
            if self._bot is None or self._receive_id is None:
                raise RuntimeError("feishu bot_api: bot client or receive_id not configured")

            if self._mtype == "text":
                # payload is {"msg_type": "text", "content": {"text": "..."}}
                text = payload.get("content", {}).get("text", "")
                return await self._bot.send_text(
                    self._receive_id, self._receive_id_type, text
                )
            else:
                # payload is {"zh_cn": {"title": ..., "content": [...]}}
                return await self._bot.send_post(
                    self._receive_id, self._receive_id_type, payload
                )

        # Webhook mode
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
        return f"feishu:{uuid.uuid4().hex[:12]}"
