import asyncio
import re
import time

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
_TG_RATE_MSG_PER_MIN = 20
_TG_MIN_SEND_GAP = 60.0 / _TG_RATE_MSG_PER_MIN * 1.05
_MAX_429_RETRIES = 2


def _estimate_size(toks):
    return len(render_to_markdown_v2(toks))


def _safe_truncate(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    cut = limit - 1
    if cut > 0 and text[cut - 1] == "\\":
        cut -= 1
    result = text[:cut] + "…"
    result = _repair_truncated_markdown_v2(result)
    if result.endswith("\\…"):
        result = result[:-2] + "…"
    if len(result) > limit:
        result = _repair_truncated_markdown_v2(result[: limit - 1] + "…")
    return result


def _repair_truncated_markdown_v2(text: str) -> str:
    previous = None
    while text != previous:
        previous = text
        text = _remove_incomplete_link(text)
        text = _balance_markers(text)
    return text


def _remove_incomplete_link(text: str) -> str:
    for pattern in (r"\[[^\]]*\]\([^)]*$", r"\[[^\]]*$"):
        matches = list(re.finditer(pattern, text))
        if matches:
            return text[: matches[-1].start()] + "…"
    return text


def _mask_link_urls(text: str) -> str:
    chars = list(text)
    for match in re.finditer(r"\[[^\]]*\]\([^)]*\)", text):
        open_paren = text.find("(", match.start(), match.end())
        for i in range(open_paren + 1, match.end() - 1):
            chars[i] = " "
    return "".join(chars)


def _balance_markers(text: str) -> str:
    masked = _mask_link_urls(text)
    remove: list[tuple[int, int]] = []
    for marker in ("*", "_", "`"):
        escaped = re.escape(marker)
        runs = list(re.finditer(r"(?<!\\)" + escaped + "+", masked))
        for width in {len(run.group(0)) for run in runs}:
            exact_pattern = (
                r"(?<!\\)(?<!" + escaped + ")" + escaped + f"{{{width}}}(?!" + escaped + ")"
            )
            exact = list(
                re.finditer(
                    exact_pattern,
                    masked,
                )
            )
            if len(exact) % 2 != 0:
                remove.append(exact[-1].span())
    result = text
    for start, end in sorted(remove, reverse=True):
        result = result[:start] + "…" + result[end:]
    return result


def _split_md_to_payloads(md: str, footer: str, too_long: str) -> list[PushBatchItem]:
    toks = parse_markdown(md)
    text = render_to_markdown_v2(toks)
    if footer:
        text = text + "\n" + _escape_markdown_v2(footer)
    if len(text) <= _TG_MAX:
        return [PushBatchItem(kind="item", payload=text)]
    if too_long == "truncate":
        return [PushBatchItem(kind="item", payload=_safe_truncate(text, _TG_MAX))]
    if too_long == "fail":
        raise RuntimeError(f"telegram message exceeds {_TG_MAX} chars")
    footer_escaped = _escape_markdown_v2(footer) if footer else ""
    suffix_template = " ({1}/{999})"
    max_suffix_len = len(_escape_markdown_v2(suffix_template))
    chunk_budget = _TG_MAX - max(8, len(footer_escaped) + max_suffix_len + 2)
    if chunk_budget < 200:
        chunk_budget = _TG_MAX - max_suffix_len - 2
        footer_escaped = ""
    chunks = split_tokens(toks, chunk_budget, _estimate_size)
    total = len(chunks)
    out: list[PushBatchItem] = []
    for i, c in enumerate(chunks, start=1):
        body = render_to_markdown_v2(c)
        chunk_suffix = _escape_markdown_v2(f"({i}/{total})")
        if footer_escaped:
            chunk_suffix = _escape_markdown_v2(footer) + " " + chunk_suffix
        payload = body + "\n" + chunk_suffix
        if len(payload) > _TG_MAX:
            payload = _safe_truncate(payload, _TG_MAX)
        out.append(PushBatchItem(kind="chunk", payload=payload))
    return out


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
        emit_on_empty: bool = False,
        client: httpx.AsyncClient | None = None,
    ):
        super().__init__(
            sink_id=sink_id,
            footer_template=footer_template,
            delivery_log=delivery_log,
            emit_on_empty=emit_on_empty,
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
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(timeout=30.0)
        self._next_send_mono = 0.0

    async def aclose(self):
        if self._owns_client:
            await self._client.aclose()

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
                out.extend(_split_md_to_payloads(md, footer, self._too_long))
        items = ctx.items if self._items_input == "items" else []
        total = len(items)
        if self._split == "single" and items:
            parts = [
                self._tmpl.render(item=it, ctx=ctx, index=i, total=total)
                for i, it in enumerate(items, 1)
            ]
            combined = "\n\n".join(parts)
            out.extend(_split_md_to_payloads(combined, footer, self._too_long))
        else:
            for i, it in enumerate(items, 1):
                md = self._tmpl.render(item=it, ctx=ctx, index=i, total=total)
                out.extend(_split_md_to_payloads(md, footer, self._too_long))
        return out

    async def send_one(self, payload, recipient=None) -> str:
        url = f"https://api.telegram.org/bot{self._token}/sendMessage"
        body = {
            "chat_id": self._chat,
            "text": payload,
            "parse_mode": "MarkdownV2",
            "link_preview_options": {"is_disabled": self._lpd},
        }
        for attempt in range(1 + _MAX_429_RETRIES):
            now = time.monotonic()
            if self._next_send_mono > now:
                await asyncio.sleep(self._next_send_mono - now)
            resp = await self._client.post(url, json=body)
            if resp.status_code == 429:
                retry_after = _TG_MIN_SEND_GAP * 2
                try:
                    retry_after = float(
                        resp.json().get("parameters", {}).get("retry_after", retry_after)
                    )
                except Exception:
                    pass
                self._next_send_mono = time.monotonic() + retry_after
                if attempt < _MAX_429_RETRIES:
                    continue
                raise RuntimeError(f"telegram 429: rate limited after {_MAX_429_RETRIES} retries")
            if not resp.is_success:
                raise RuntimeError(f"telegram {resp.status_code}: {resp.text[:500]}")
            data = resp.json()
            if not data.get("ok"):
                raise RuntimeError(f"telegram error: {data}")
            gap = max(_TG_MIN_SEND_GAP, self._delay)
            self._next_send_mono = time.monotonic() + gap
            return f"{self._chat}:{data['result']['message_id']}"
        raise RuntimeError(f"telegram 429: rate limited after {_MAX_429_RETRIES} retries")
