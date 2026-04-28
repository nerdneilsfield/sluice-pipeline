from abc import ABC, abstractmethod
from datetime import datetime, timezone
from sluice.context import PipelineContext
from sluice.core.protocols import SinkResult
from sluice.state.emissions import EmissionStore
from sluice.registry import register_sink as _reg

register_sink = _reg

class Sink(ABC):
    type: str = "abstract"

    def __init__(self, *, id: str, mode: str = "upsert",
                 emit_on_empty: bool = False):
        self.id = id
        self.mode = mode
        self.emit_on_empty = emit_on_empty

    @abstractmethod
    async def create(self, ctx: PipelineContext) -> str: ...

    async def update(self, external_id: str, ctx: PipelineContext) -> str:
        return await self.create(ctx)

    async def emit(self, ctx: PipelineContext, *,
                   emissions: EmissionStore) -> SinkResult | None:
        if not ctx.items and not self.emit_on_empty:
            return None
        if self.mode == "create_new":
            ext = await self.create(ctx)
            await emissions.insert(ctx.pipeline_id, ctx.run_key,
                                    self.id, self.type, ext)
            return SinkResult(self.id, self.type, ext, created=True)
        prior = await emissions.lookup(ctx.pipeline_id, ctx.run_key, self.id)
        if prior:
            if self.mode == "create_once":
                return SinkResult(self.id, self.type, prior.external_id,
                                  created=False)
            ext = await self.update(prior.external_id, ctx)
            return SinkResult(self.id, self.type, ext, created=False)
        ext = await self.create(ctx)
        await emissions.insert(ctx.pipeline_id, ctx.run_key,
                                self.id, self.type, ext)
        return SinkResult(self.id, self.type, ext, created=True)
