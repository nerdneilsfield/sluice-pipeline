from sluice.context import PipelineContext


def test_context_holds_items_and_dict():
    ctx = PipelineContext(
        pipeline_id="p",
        run_key="p/2026-04-28",
        run_date="2026-04-28",
        items=[],
        context={},
    )
    ctx.context["daily_brief"] = "..."
    assert ctx.context["daily_brief"] == "..."
    assert ctx.items == []


def test_context_stats_init():
    ctx = PipelineContext(
        pipeline_id="p",
        run_key="p/2026-04-28",
        run_date="2026-04-28",
        items=[],
        context={},
    )
    assert ctx.stats.items_in == 0
    assert ctx.stats.llm_calls == 0
    assert ctx.stats.est_cost_usd == 0.0
