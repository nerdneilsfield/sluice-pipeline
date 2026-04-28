import pytest
from datetime import datetime, timezone
from sluice.context import PipelineContext, RunStats
from sluice.core.item import Item
from sluice.processors.render import RenderProcessor

def mk(s):
    return Item(source_id="s", pipeline_id="p", guid=s, url=f"https://x/{s}",
                title=f"T-{s}", published_at=datetime(2026,4,28,tzinfo=timezone.utc),
                raw_summary=None, summary=f"sum-{s}")

@pytest.mark.asyncio
async def test_render_full_context(tmp_path):
    tpl = tmp_path / "t.j2"
    tpl.write_text(
        "# {{ pipeline_id }} {{ run_date }}\n\n"
        "{{ context.daily_brief }}\n\n"
        "{% for it in items %}- {{ it.summary }}\n{% endfor %}\n"
        "stats: {{ stats.items_out }}/{{ stats.llm_calls }}"
    )
    items = [mk("a"), mk("b")]
    ctx = PipelineContext("p1", "p1/2026-04-28", "2026-04-28", items,
                          {"daily_brief": "BRIEF"},
                          stats=RunStats(items_in=2, items_out=2,
                                         llm_calls=3, est_cost_usd=0.01))
    p = RenderProcessor(name="r", template=str(tpl),
                        output_field="context.markdown")
    ctx = await p.process(ctx)
    md = ctx.context["markdown"]
    assert "# p1 2026-04-28" in md
    assert "BRIEF" in md
    assert "- sum-a" in md
    assert "stats: 2/3" in md
