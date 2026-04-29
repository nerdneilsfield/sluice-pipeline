from dataclasses import dataclass

from sluice.context import PipelineContext
from sluice.core.protocols import SinkResult
from sluice.sinks.base import Sink
from sluice.state.delivery_log import DeliveryLog
from sluice.state.emissions import EmissionStore


@dataclass
class PushBatchItem:
    kind: str
    payload: object
    recipient: str | None = None


class PushSinkBase(Sink):
    type: str = "push"

    def __init__(
        self,
        *,
        sink_id: str,
        footer_template: str,
        delivery_log: DeliveryLog,
        emit_on_empty: bool = False,
    ):
        super().__init__(id=sink_id, mode="create_new", emit_on_empty=emit_on_empty)
        self._footer = footer_template
        self._log = delivery_log

    async def create(self, ctx: PipelineContext) -> str:
        raise NotImplementedError(f"{self.type} sink uses emit() override, not create()")

    def render_footer(self, ctx: PipelineContext) -> str:
        return (
            self._footer.format(
                pipeline_id=ctx.pipeline_id,
                run_date=ctx.run_date,
            )
            if self._footer
            else ""
        )

    async def send_one(self, payload, recipient=None) -> str:
        raise NotImplementedError

    def build_batch(self, ctx: PipelineContext) -> list[PushBatchItem]:
        raise NotImplementedError

    @property
    def fail_fast(self) -> bool:
        return True

    async def emit(self, ctx: PipelineContext, *, emissions: EmissionStore) -> SinkResult | None:
        batch = self.build_batch(ctx)
        if not batch:
            if not self.emit_on_empty:
                return None
            await emissions.insert(
                ctx.pipeline_id,
                ctx.run_key,
                self.id,
                self.type,
                None,
            )
            return SinkResult(self.id, self.type, None, created=False)
        first_external_id: str | None = None
        any_success = False
        last_exc: Exception | None = None
        for ord_, item in enumerate(batch, start=1):
            try:
                ext = await self.send_one(item.payload, recipient=item.recipient)
                await self._log.record(
                    pipeline_id=ctx.pipeline_id,
                    run_key=ctx.run_key,
                    sink_id=self.id,
                    sink_type=self.type,
                    ordinal=ord_,
                    message_kind=item.kind,
                    recipient=item.recipient,
                    external_id=ext,
                    status="success",
                    error_class=None,
                    error_msg=None,
                )
                if first_external_id is None:
                    first_external_id = ext
                any_success = True
            except Exception as exc:
                last_exc = exc
                await self._log.record(
                    pipeline_id=ctx.pipeline_id,
                    run_key=ctx.run_key,
                    sink_id=self.id,
                    sink_type=self.type,
                    ordinal=ord_,
                    message_kind=item.kind,
                    recipient=item.recipient,
                    external_id=None,
                    status="failed",
                    error_class=type(exc).__name__,
                    error_msg=str(exc),
                )
                if self.fail_fast:
                    if any_success:
                        await emissions.insert(
                            ctx.pipeline_id,
                            ctx.run_key,
                            self.id,
                            self.type,
                            first_external_id,
                        )
                    raise
        if not any_success:
            raise RuntimeError(f"{self.type} sink {self.id}: all sends failed") from last_exc
        await emissions.insert(
            ctx.pipeline_id,
            ctx.run_key,
            self.id,
            self.type,
            first_external_id,
        )
        return SinkResult(self.id, self.type, first_external_id, created=True)
