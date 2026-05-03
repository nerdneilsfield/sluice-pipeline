from __future__ import annotations

from pathlib import Path
from typing import Any

import aiofiles

from sluice.dag.sinks.base import EmissionResult, Sink, SinkChunk, bind_sink


class FileMdSink(Sink):
    type_name = "file_md"

    async def chunks(self, rendered: Any, params: dict) -> list[SinkChunk]:
        text = "" if rendered is None else str(rendered)
        return [SinkChunk(ordinal=0, payload=text, message_kind="markdown")]

    async def emit(self, chunk: SinkChunk, params: dict) -> EmissionResult:
        path = self._resolve_path(chunk, params)
        path.parent.mkdir(parents=True, exist_ok=True)
        async with aiofiles.open(path, "w", encoding="utf-8") as handle:
            await handle.write(str(chunk.payload))
        sink_id = params.get("sink_id", "file_md")
        return EmissionResult(
            sink_id=sink_id,
            ordinal=chunk.ordinal,
            success=True,
            external_id=str(path),
            recipient=str(path.parent),
        )

    def _resolve_path(self, chunk: SinkChunk, params: dict) -> Path:
        fields = {
            key: params[key]
            for key in ("task", "pipeline", "run_key", "cycle_anchor", "sink_id")
            if key in params
        }
        fields["ordinal"] = chunk.ordinal
        if "path" in params:
            return Path(str(params["path"]).format(**fields))
        out_dir = Path(params["out_dir"])
        filename = params.get("filename_template", "{ordinal}.md").format(**fields)
        return out_dir / filename


bind_sink("file_md", FileMdSink)
