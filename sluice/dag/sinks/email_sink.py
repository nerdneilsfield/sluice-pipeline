from __future__ import annotations

from typing import Any

from sluice.dag.sinks._impl.email import EmailSink as V1EmailSink
from sluice.dag.sinks._v1_push import (
    PushPayload,
    V1PushSinkAdapter,
    bool_param,
    chunks_from_batch,
    env_param,
    int_param,
    list_param,
    rendered_context,
    template_param,
    text_param,
)
from sluice.dag.sinks.base import SinkChunk, bind_sink


class EmailDagSink(V1PushSinkAdapter):
    type_name = "email"

    def _sink(self, params: dict) -> V1EmailSink:
        return V1EmailSink(
            sink_id=params["sink_id"],
            smtp_host=env_param(params, "smtp_host"),
            smtp_port=int_param(params, "smtp_port", 587),
            smtp_username=env_param(params, "smtp_username", ""),
            smtp_password=env_param(params, "smtp_password", ""),
            smtp_starttls=bool_param(params, "smtp_starttls", True),
            from_address=env_param(params, "from_address"),
            recipients=list_param(params, "recipients"),
            subject_template=text_param(params, "subject_template", "{pipeline_id} {run_date}"),
            brief_input=text_param(params, "brief_input", "context.rendered"),
            items_input=text_param(params, "items_input", "none"),
            items_template_str=template_param(params, "items_template", "{{ item.title }}"),
            split=text_param(params, "split", "single"),
            html_template_str=template_param(params, "html_template", "{{ body_html }}"),
            style_block=template_param(params, "style_block", "", alt_key="style_block_file"),
            footer_template=template_param(params, "footer_template", ""),
            attach_run_log=bool_param(params, "attach_run_log", False),
            recipient_failure_policy=text_param(params, "recipient_failure_policy", "fail_fast"),
            delivery_log=None,
            emit_on_empty=bool_param(params, "emit_on_empty", False),
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


bind_sink("email", EmailDagSink)
