from sluice.context import PipelineContext
from sluice.core.item import Item as _Item


def make_ctx(
    *, items=None, context=None, pipeline_id="p", run_key="p/2026-04-29", run_date="2026-04-29"
) -> PipelineContext:
    return PipelineContext(
        pipeline_id=pipeline_id,
        run_key=run_key,
        run_date=run_date,
        items=list(items or []),
        context=dict(context or {}),
    )


def make_item(
    url: str = "https://example.com/a",
    title: str = "Test Article",
    source_id: str = "rss_0",
    guid: str | None = None,
    tags: list | None = None,
    extras: dict | None = None,
    raw_summary: str | None = None,
    fulltext: str | None = None,
) -> _Item:
    return _Item(
        source_id=source_id,
        pipeline_id="p",
        guid=guid,
        url=url,
        title=title,
        published_at=None,
        raw_summary=raw_summary,
        fulltext=fulltext,
        tags=list(tags or []),
        extras=dict(extras or {}),
    )
