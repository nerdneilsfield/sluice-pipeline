from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sluice.dag.sinks.base import EmissionResult, Sink
from sluice.dag.stores.sink_delivery_log import SinkDeliveryLogStore
from sluice.dag.stores.sink_emissions import SinkEmissionsStore
from sluice.dag.writer import WriteJob, WriterThread


class SinkEmitter:
    def __init__(self, *, writer: WriterThread, run_key: str, pipeline_name: str):
        self._writer = writer
        self._run_key = run_key
        self._pipeline_name = pipeline_name

    async def emit_all(
        self,
        *,
        sink_id: str,
        sink_type: str,
        sink: Sink,
        rendered: Any,
        params: dict,
    ) -> list[EmissionResult]:
        try:
            chunks = await sink.chunks(rendered, params)
        except Exception as exc:  # noqa: BLE001
            result = EmissionResult(
                sink_id=sink_id,
                ordinal=0,
                success=False,
                error_class=type(exc).__name__,
                error_msg=str(exc)[:500],
            )
            await self._record(
                sink_id=sink_id,
                sink_type=sink_type,
                ordinal=0,
                message_kind=None,
                result=result,
            )
            return [result]
        results: list[EmissionResult] = []
        for chunk in chunks:
            if await self._already_emitted(sink_id, chunk.ordinal):
                results.append(
                    EmissionResult(
                        sink_id=sink_id,
                        ordinal=chunk.ordinal,
                        success=True,
                        error_msg="already_emitted",
                    )
                )
                continue

            try:
                result = await sink.emit(chunk, params)
            except Exception as exc:  # noqa: BLE001
                result = EmissionResult(
                    sink_id=sink_id,
                    ordinal=chunk.ordinal,
                    success=False,
                    error_class=type(exc).__name__,
                    error_msg=str(exc)[:500],
                )
            await self._record(
                sink_id=sink_id,
                sink_type=sink_type,
                ordinal=chunk.ordinal,
                message_kind=chunk.message_kind,
                result=result,
            )
            results.append(result)
        return results

    async def _already_emitted(self, sink_id: str, ordinal: int) -> bool:
        out = {"hit": False}

        async def job(active, seen) -> None:  # noqa: ARG001
            out["hit"] = await SinkEmissionsStore(active).exists(
                self._run_key, sink_id, ordinal
            )

        await (await self._writer.submit(WriteJob(run=job)))
        return out["hit"]

    async def _record(
        self,
        *,
        sink_id: str,
        sink_type: str,
        ordinal: int,
        message_kind: str | None,
        result: EmissionResult,
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        run_key = self._run_key

        async def job(active, seen) -> None:  # noqa: ARG001
            log = SinkDeliveryLogStore(active)
            await log.append(
                run_key=run_key,
                sink_id=sink_id,
                sink_type=sink_type,
                attempt_at=now,
                ordinal=ordinal,
                message_kind=message_kind,
                recipient=result.recipient,
                external_id=result.external_id,
                status="success" if result.success else "failed",
                error_class=result.error_class,
                error_msg=result.error_msg,
            )
            if result.success:
                await SinkEmissionsStore(active).insert(
                    run_key=run_key,
                    sink_id=sink_id,
                    ordinal=ordinal,
                    sink_type=sink_type,
                    message_kind=message_kind,
                    external_id=result.external_id,
                    emitted_at=now,
                )

        await (await self._writer.submit(WriteJob(run=job)))
