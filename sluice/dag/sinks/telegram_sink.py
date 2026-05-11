from __future__ import annotations

from typing import Any

from sluice.dag.sinks._impl.telegram import TelegramSink as V1TelegramSink
from sluice.dag.sinks._v1_push import (
    PushPayload,
    V1PushSinkAdapter,
    bool_param,
    chunks_from_batch,
    env_param,
    float_param,
    rendered_context,
    template_param,
    text_param,
)
from sluice.dag.sinks.base import SinkChunk, bind_sink


class TelegramDagSink(V1PushSinkAdapter):
    type_name = "telegram"

    def _sink(self, params: dict) -> V1TelegramSink:
        return V1TelegramSink(
            sink_id=params["sink_id"],
            bot_token=env_param(params, "bot_token"),
            chat_id=env_param(params, "chat_id"),
            brief_input=text_param(params, "brief_input", "context.rendered"),
            items_input=text_param(params, "items_input", "none"),
            items_template_str=template_param(params, "items_template", "{{ item.title }}"),
            split=text_param(params, "split", "single"),
            link_preview_disabled=bool_param(params, "link_preview_disabled", True),
            footer_template=template_param(params, "footer_template", ""),
            on_message_too_long=text_param(params, "on_message_too_long", "truncate"),
            between_messages_delay_seconds=float_param(
                params, "between_messages_delay_seconds", 0.0
            ),
            delivery_log=None,
            emit_on_empty=bool_param(params, "emit_on_empty", False),
            client=params.get("client"),
        )

    async def _build_batch(self, rendered: Any, params: dict):
        return self._sink(params).build_batch(rendered_context(rendered, params))

    async def chunks(self, rendered: Any, params: dict) -> list[SinkChunk]:
        return chunks_from_batch(await self._build_batch(rendered, params))

    async def _send_one(self, chunk: SinkChunk, params: dict) -> str:
        payload = chunk.payload
        raw_payload = payload.payload if isinstance(payload, PushPayload) else payload
        recipient = payload.recipient if isinstance(payload, PushPayload) else None
        return await self._sink(params).send_one(raw_payload, recipient=recipient)


bind_sink("telegram", TelegramDagSink)
