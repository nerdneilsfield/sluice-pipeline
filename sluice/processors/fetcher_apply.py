from sluice.context import PipelineContext
from sluice.logging_setup import get_logger
from sluice.processors.dedupe import item_key

log = get_logger(__name__)


class FetcherApplyProcessor:
    name = "fetcher_apply"

    def __init__(
        self,
        *,
        name,
        chain,
        write_field,
        skip_if_field_longer_than,
        failures,
        max_retries,
        on_all_failed="skip",
    ):
        self.name = name
        self.chain = chain
        self.write_field = write_field
        self.skip_if_field_longer_than = skip_if_field_longer_than
        self.failures = failures
        self.max_retries = max_retries
        self.on_all_failed = on_all_failed

    async def process(self, ctx: PipelineContext) -> PipelineContext:
        survivors = []
        stats = {
            "items_in": len(ctx.items),
            "items_out": 0,
            "fetched": 0,
            "used_existing": 0,
            "empty": 0,
            "failed": 0,
            "errors": {},
        }
        for it in ctx.items:
            existing = it.raw_summary or ""
            if self.skip_if_field_longer_than and len(existing) >= self.skip_if_field_longer_than:
                setattr(it, self.write_field, existing)
                survivors.append(it)
                stats["used_existing"] += 1
                continue
            try:
                md = await self.chain.fetch(it.url)
            except Exception as e:
                stats["failed"] += 1
                errors = stats["errors"]
                errors[type(e).__name__] = errors.get(type(e).__name__, 0) + 1
                if self.on_all_failed == "use_raw_summary" and it.raw_summary:
                    setattr(it, self.write_field, it.raw_summary)
                    survivors.append(it)
                    stats["fetched"] += 1
                    continue
                if self.failures is not None:
                    error_class = type(e).__name__
                    error_msg = str(e)
                    if self.on_all_failed == "use_raw_summary" and not it.raw_summary:
                        error_class = "FetcherChainExhaustedNoFallback"
                        error_msg = "all fetchers failed and raw_summary is empty"
                    await self.failures.record(
                        it.pipeline_id,
                        item_key(it),
                        it,
                        stage=self.name,
                        error_class=error_class,
                        error_msg=error_msg,
                        max_retries=self.max_retries,
                    )
                log.bind(
                    stage=self.name,
                    item_key=item_key(it),
                    url=it.url,
                    error_class=type(e).__name__,
                    error=str(e),
                ).debug("fetcher_apply.item_failed")
                continue
            if md is None:
                stats["empty"] += 1
            else:
                stats["fetched"] += 1
            setattr(it, self.write_field, md)
            survivors.append(it)
        ctx.items = survivors
        stats["items_out"] = len(survivors)
        ctx.context.setdefault("_stage_stats", {})[self.name] = stats
        log.bind(stage=self.name, **stats).info("fetcher_apply.done")
        return ctx
