from sluice.context import PipelineContext
from sluice.processors.dedupe import item_key

class FetcherApplyProcessor:
    name = "fetcher_apply"

    def __init__(self, *, name, chain, write_field, skip_if_field_longer_than,
                 failures, max_retries):
        self.name = name
        self.chain = chain
        self.write_field = write_field
        self.skip_if_field_longer_than = skip_if_field_longer_than
        self.failures = failures
        self.max_retries = max_retries

    async def process(self, ctx: PipelineContext) -> PipelineContext:
        survivors = []
        for it in ctx.items:
            existing = it.raw_summary or ""
            if (self.skip_if_field_longer_than
                    and len(existing) >= self.skip_if_field_longer_than):
                setattr(it, self.write_field, existing)
                survivors.append(it)
                continue
            try:
                md = await self.chain.fetch(it.url)
            except Exception as e:
                if self.failures is not None:
                    await self.failures.record(
                        it.pipeline_id, item_key(it), it,
                        stage=self.name, error_class=type(e).__name__,
                        error_msg=str(e), max_retries=self.max_retries)
                continue
            setattr(it, self.write_field, md)
            survivors.append(it)
        ctx.items = survivors
        return ctx
