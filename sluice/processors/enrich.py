import asyncio
from typing import Literal

from sluice.context import PipelineContext
from sluice.core.dotpath import set_dotpath
from sluice.core.item import Item
from sluice.logging_setup import get_logger
from sluice.processors.base import register_processor

log = get_logger(__name__)


def _truncate_head_tail(s: str, max_chars: int) -> str:
    if len(s) <= max_chars:
        return s
    half = max_chars // 2
    return s[:half] + s[-(max_chars - half) :]


class EnrichProcessor:
    name: str
    type = "enrich"

    def __init__(
        self,
        *,
        name,
        enricher,
        output_field,
        on_failure: Literal["skip", "fail"],
        max_chars: int,
        concurrency: int,
        failures=None,
        max_retries: int = 3,
    ):
        self.name = name
        self._enricher = enricher
        self._output_field = output_field
        self._on_failure = on_failure
        self._max = max_chars
        self._sem = asyncio.Semaphore(concurrency)
        self._failures = failures
        self._max_retries = max_retries

    async def aclose(self):
        if hasattr(self._enricher, "close") and asyncio.iscoroutinefunction(self._enricher.close):
            await self._enricher.close()

    async def process(self, ctx: PipelineContext) -> PipelineContext:
        from sluice.processors.dedupe import item_key

        async def one(item: Item):
            async with self._sem:
                try:
                    res = await self._enricher.enrich(item)
                except Exception as exc:
                    if self._on_failure == "fail":
                        raise
                    log.bind(
                        stage=self.name,
                        enricher=self._enricher.name,
                        item_key=item_key(item),
                        error_class=type(exc).__name__,
                        error=str(exc),
                    ).warning("enrich.item_skipped")
                    if self._failures is not None:
                        try:
                            await self._failures.record(
                                item.pipeline_id,
                                item_key(item),
                                item,
                                stage=f"enrich:{self._enricher.name}",
                                error_class=type(exc).__name__,
                                error_msg=str(exc),
                                max_retries=self._max_retries,
                            )
                        except Exception as audit_exc:
                            log.bind(
                                stage=self.name,
                                enricher=self._enricher.name,
                                item_key=item_key(item),
                                error_class=type(audit_exc).__name__,
                                error=str(audit_exc),
                            ).warning("enrich.failure_record_failed")
                    return
            if res is None:
                return
            res = _truncate_head_tail(res, self._max)
            set_dotpath(item, self._output_field, res)

        await asyncio.gather(*(one(it) for it in ctx.items))
        return ctx


register_processor("enrich")(EnrichProcessor)
