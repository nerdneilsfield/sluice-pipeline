from sluice.context import PipelineContext


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
