from sluice.context import PipelineContext
from sluice.core.item import Item, compute_item_key as item_key
from sluice.state.seen import SeenStore
from sluice.state.failures import FailureStore


class DedupeProcessor:
    name = "dedupe"

    def __init__(self, *, name: str, pipeline_id: str, seen: SeenStore, failures: FailureStore):
        self.name = name
        self.pipeline_id = pipeline_id
        self.seen = seen
        self.failures = failures

    async def process(self, ctx: PipelineContext) -> PipelineContext:
        keys = [item_key(it) for it in ctx.items]
        seen_keys = set(keys) - set(await self.seen.filter_unseen(self.pipeline_id, keys))
        excluded = await self.failures.excluded_keys(self.pipeline_id)
        kept = [it for it, k in zip(ctx.items, keys) if k not in seen_keys and k not in excluded]
        ctx.items = kept
        return ctx
