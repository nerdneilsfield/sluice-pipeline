from __future__ import annotations

from typing import Any

from sluice.dag.sinks._impl.feishu import FeishuSink as V1FeishuSink
from sluice.dag.sinks._v1_push import (
    PushPayload,
    V1PushSinkAdapter,
    aclose_if_available,
    bool_param,
    chunks_from_batch,
    env_param,
    float_param,
    rendered_context,
    template_param,
    text_param,
)
from sluice.dag.sinks.base import SinkChunk, bind_sink


class FeishuDagSink(V1PushSinkAdapter):
    type_name = "feishu"

    def _sink(self, params: dict) -> V1FeishuSink:
        auth_mode = text_param(params, "auth_mode", "webhook")
        return V1FeishuSink(
            sink_id=params["sink_id"],
            webhook_url=env_param(params, "webhook_url") if auth_mode == "webhook" else "",
            secret=env_param(params, "secret") if params.get("secret") else None,
            brief_input=text_param(params, "brief_input", "context.rendered"),
            items_input=text_param(params, "items_input", "none"),
            items_template_str=template_param(params, "items_template", "{{ item.title }}"),
            split=text_param(params, "split", "single"),
            message_type=text_param(params, "message_type", "post"),
            on_message_too_long=text_param(params, "on_message_too_long", "truncate"),
            card_template_str=template_param(params, "card_template", ""),
            footer_template=template_param(params, "footer_template", ""),
            between_messages_delay_seconds=float_param(
                params, "between_messages_delay_seconds", 0.0
            ),
            delivery_log=None,
            emit_on_empty=bool_param(params, "emit_on_empty", False),
            client=params.get("client"),
            auth_mode=auth_mode,
            app_id=env_param(params, "app_id") if params.get("app_id") else None,
            app_secret=env_param(params, "app_secret") if params.get("app_secret") else None,
            receive_id=env_param(params, "receive_id") if params.get("receive_id") else None,
            receive_id_type=text_param(params, "receive_id_type", "chat_id"),
        )

    async def _build_batch(self, rendered: Any, params: dict):
        sink = self._sink(params)
        try:
            return sink.build_batch(rendered_context(rendered, params))
        finally:
            await aclose_if_available(sink)

    async def chunks(self, rendered: Any, params: dict) -> list[SinkChunk]:
        return chunks_from_batch(await self._build_batch(rendered, params))

    async def _send_one(self, chunk: SinkChunk, params: dict) -> str:
        payload = chunk.payload
        raw_payload = payload.payload if isinstance(payload, PushPayload) else payload
        recipient = payload.recipient if isinstance(payload, PushPayload) else None
        sink = self._sink(params)
        try:
            return await sink.send_one(raw_payload, recipient=recipient)
        finally:
            await aclose_if_available(sink)


bind_sink("feishu", FeishuDagSink)
