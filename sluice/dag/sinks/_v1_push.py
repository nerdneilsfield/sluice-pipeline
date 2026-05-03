from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sluice.context import PipelineContext
from sluice.dag.sinks.base import EmissionResult, Sink, SinkChunk
from sluice.loader import resolve_env
from sluice.sinks._push_base import PushBatchItem


@dataclass(frozen=True)
class PushPayload:
    payload: object
    recipient: str | None = None


def rendered_context(rendered: Any, params: dict) -> PipelineContext:
    text = "" if rendered is None else str(rendered)
    return PipelineContext(
        pipeline_id=params.get("pipeline", ""),
        run_key=params.get("run_key", ""),
        run_date=params.get("cycle_anchor", ""),
        items=[],
        context={
            "rendered": text,
            "markdown": text,
            "body": text,
        },
    )


def text_param(params: dict, key: str, default: str = "") -> str:
    value = params.get(key, default)
    return "" if value is None else str(value)


def template_param(params: dict, key: str, default: str = "", *, alt_key: str | None = None) -> str:
    raw = params.get(key)
    if raw is None and alt_key is not None:
        raw = params.get(alt_key)
    value = default if raw is None else str(raw)
    if not value:
        return ""
    root = params.get("config_root")
    candidates = [Path(value)]
    if root:
        candidates.insert(0, Path(str(root)) / value)
    for path in candidates:
        if path.is_file():
            return path.read_text(encoding="utf-8")
    return value


def env_param(params: dict, key: str, default: str = "") -> str:
    return resolve_env(text_param(params, key, default))


def bool_param(params: dict, key: str, default: bool = False) -> bool:
    return bool(params.get(key, default))


def int_param(params: dict, key: str, default: int) -> int:
    return int(params.get(key, default))


def float_param(params: dict, key: str, default: float) -> float:
    return float(params.get(key, default))


def list_param(params: dict, key: str) -> list[str]:
    value = params.get(key, [])
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    return [str(item) for item in value]


def chunks_from_batch(batch: list[PushBatchItem]) -> list[SinkChunk]:
    return [
        SinkChunk(
            ordinal=ordinal,
            payload=PushPayload(item.payload, item.recipient),
            message_kind=item.kind,
        )
        for ordinal, item in enumerate(batch, start=1)
    ]


class V1PushSinkAdapter(Sink):
    async def _build_batch(self, rendered: Any, params: dict) -> list[PushBatchItem]:
        raise NotImplementedError

    async def _send_one(self, chunk: SinkChunk, params: dict) -> str:
        raise NotImplementedError

    async def chunks(self, rendered: Any, params: dict) -> list[SinkChunk]:
        return chunks_from_batch(await self._build_batch(rendered, params))

    async def emit(self, chunk: SinkChunk, params: dict) -> EmissionResult:
        external_id = await self._send_one(chunk, params)
        payload = chunk.payload
        recipient = payload.recipient if isinstance(payload, PushPayload) else None
        return EmissionResult(
            sink_id=params["sink_id"],
            ordinal=chunk.ordinal,
            success=True,
            external_id=external_id,
            recipient=recipient,
        )
