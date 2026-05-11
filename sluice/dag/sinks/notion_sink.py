from __future__ import annotations

from typing import Any

from sluice.dag.sinks._impl.notion import NotionSink as V1NotionSink
from sluice.dag.sinks._v1_push import bool_param, env_param, int_param, rendered_context, text_param
from sluice.dag.sinks.base import EmissionResult, Sink, SinkChunk, bind_sink


class NotionDagSink(Sink):
    type_name = "notion"

    async def chunks(self, rendered: Any, params: dict) -> list[SinkChunk]:
        text = "" if rendered is None else str(rendered)
        return [SinkChunk(ordinal=0, payload=text, message_kind="markdown")]

    def _sink(self, params: dict) -> V1NotionSink:
        return V1NotionSink(
            id=params["sink_id"],
            input=text_param(params, "input", "context.rendered"),
            parent_id=env_param(params, "parent_id"),
            parent_type=text_param(params, "parent_type", "page"),
            title_template=text_param(params, "title_template", "{pipeline_id} {run_date}"),
            token=text_param(params, "token", "env:NOTION_TOKEN"),
            properties=dict(params.get("properties") or {}),
            mode=text_param(params, "mode", "create_new"),
            max_block_chars=int_param(params, "max_block_chars", 1900),
            emit_on_empty=bool_param(params, "emit_on_empty", False),
            adapter=params.get("adapter"),
        )

    async def emit(self, chunk: SinkChunk, params: dict) -> EmissionResult:
        ctx = rendered_context(chunk.payload, params)
        external_id = await self._sink(params).create(ctx)
        return EmissionResult(
            sink_id=params["sink_id"],
            ordinal=chunk.ordinal,
            success=True,
            external_id=external_id,
            recipient=text_param(params, "parent_id", ""),
        )


bind_sink("notion", NotionDagSink)
