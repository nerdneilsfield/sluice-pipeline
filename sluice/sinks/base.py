from abc import ABC, abstractmethod

from sluice.context import PipelineContext
from sluice.core.protocols import SinkResult
from sluice.registry import register_sink as _reg
from sluice.state.emissions import EmissionStore

register_sink = _reg


class Sink(ABC):
    type: str = "abstract"

    def __init__(self, *, id: str, mode: str = "upsert", emit_on_empty: bool = False):
        self.id = id
        self.mode = mode
        self.emit_on_empty = emit_on_empty

    @abstractmethod
    async def create(self, ctx: PipelineContext) -> str: ...

    async def update(self, external_id: str, ctx: PipelineContext) -> str:
        return await self.create(ctx)

    def _input_payload(self, ctx: PipelineContext):
        path = getattr(self, "input", None)
        if not path:
            return None
        cur = ctx
        for part in path.split("."):
            if isinstance(cur, dict):
                cur = cur.get(part)
            else:
                cur = getattr(cur, part, None)
            if cur is None:
                return None
        return cur

    def _has_input_payload(self, ctx: PipelineContext) -> bool:
        value = self._input_payload(ctx)
        if value is None:
            return False
        if isinstance(value, str):
            return value != ""
        try:
            return len(value) > 0
        except TypeError:
            return True

    async def emit(self, ctx: PipelineContext, *, emissions: EmissionStore) -> SinkResult | None:
        if not ctx.items and not self.emit_on_empty and not self._has_input_payload(ctx):
            return None
        if self.mode == "create_new":
            ext = await self.create(ctx)
            await emissions.insert(ctx.pipeline_id, ctx.run_key, self.id, self.type, ext)
            return SinkResult(self.id, self.type, ext, created=True)
        prior = await emissions.lookup(ctx.pipeline_id, ctx.run_key, self.id)
        if prior:
            if self.mode == "create_once":
                return SinkResult(self.id, self.type, prior.external_id, created=False)
            ext = await self.update(prior.external_id, ctx)
            return SinkResult(self.id, self.type, ext, created=False)
        ext = await self.create(ctx)
        await emissions.insert(ctx.pipeline_id, ctx.run_key, self.id, self.type, ext)
        return SinkResult(self.id, self.type, ext, created=True)
