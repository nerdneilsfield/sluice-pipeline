from datetime import datetime, timezone

import pytest

from sluice.context import PipelineContext
from sluice.core.item import Item
from sluice.processors.llm_stage import LLMStageProcessor


class FakeLLM:
    def __init__(self, replies):
        self.replies = list(replies)
        self.calls = 0

    async def chat(self, messages):
        self.calls += 1
        return self.replies.pop(0)


def mk(i):
    return Item(
        source_id="s",
        pipeline_id="p",
        guid=f"g{i}",
        url=f"https://x/{i}",
        title=f"t{i}",
        published_at=datetime(2026, 4, 28, tzinfo=timezone.utc),
        raw_summary=None,
        fulltext=f"body {i}",
    )


@pytest.mark.asyncio
async def test_per_item_writes_summary(tmp_path):
    prompt = tmp_path / "p.md"
    prompt.write_text("Summarize: {{ item.fulltext }}")
    llm = FakeLLM(replies=["sum0", "sum1"])
    p = LLMStageProcessor(
        name="summarize",
        mode="per_item",
        input_field="fulltext",
        output_field="summary",
        prompt_file=str(prompt),
        llm_factory=lambda: llm,
        output_parser="text",
        max_input_chars=1000,
        truncate_strategy="head_tail",
        workers=2,
    )
    ctx = PipelineContext("p", "p/r", "2026-04-28", [mk(0), mk(1)], {})
    ctx = await p.process(ctx)
    assert [it.summary for it in ctx.items] == ["sum0", "sum1"]
    assert llm.calls == 2


@pytest.mark.asyncio
async def test_aggregate_writes_context(tmp_path):
    prompt = tmp_path / "p.md"
    prompt.write_text("Items: {{ items|length }}")
    llm = FakeLLM(replies=["BRIEF"])
    p = LLMStageProcessor(
        name="daily",
        mode="aggregate",
        input_field="summary",
        output_target="context.daily_brief",
        prompt_file=str(prompt),
        llm_factory=lambda: llm,
        output_parser="text",
        max_input_chars=10000,
        truncate_strategy="head_tail",
        workers=1,
    )
    ctx = PipelineContext("p", "p/r", "2026-04-28", [mk(0), mk(1)], {})
    ctx = await p.process(ctx)
    assert ctx.context["daily_brief"] == "BRIEF"


@pytest.mark.asyncio
async def test_json_parser_writes_dict(tmp_path):
    prompt = tmp_path / "p.md"
    prompt.write_text("rate: {{ item.summary }}")
    llm = FakeLLM(replies=['{"score": 7}'])
    p = LLMStageProcessor(
        name="rate",
        mode="per_item",
        input_field="summary",
        output_field="extras.relevance",
        prompt_file=str(prompt),
        llm_factory=lambda: llm,
        output_parser="json",
        max_input_chars=1000,
        truncate_strategy="head_tail",
        workers=1,
    )
    it = mk(0)
    it.summary = "abc"
    ctx = await p.process(PipelineContext("p", "p/r", "2026-04-28", [it], {}))
    assert ctx.items[0].extras["relevance"] == {"score": 7}


@pytest.mark.asyncio
async def test_truncate_head_tail(tmp_path):
    prompt = tmp_path / "p.md"
    prompt.write_text("{{ item.fulltext }}")
    seen = []

    class CaptureLLM:
        async def chat(self, messages):
            seen.append(messages[-1]["content"])
            return "ok"

    p = LLMStageProcessor(
        name="x",
        mode="per_item",
        input_field="fulltext",
        output_field="summary",
        prompt_file=str(prompt),
        llm_factory=lambda: CaptureLLM(),
        output_parser="text",
        max_input_chars=20,
        truncate_strategy="head_tail",
        workers=1,
    )
    it = mk(0)
    it.fulltext = "A" * 50 + "B" * 50
    await p.process(PipelineContext("p", "p/r", "2026-04-28", [it], {}))
    assert len(seen[0]) <= 23


@pytest.mark.asyncio
async def test_per_item_llm_failure_records_and_drops(tmp_path):
    prompt = tmp_path / "p.md"
    prompt.write_text("S: {{ item.fulltext }}")

    class FlakyLLM:
        def __init__(self):
            self.n = 0

        async def chat(self, messages):
            self.n += 1
            if self.n == 2:
                raise RuntimeError("transient")
            return f"ok-{self.n}"

    llm = FlakyLLM()

    class FakeFailures:
        def __init__(self):
            self.records = []

        async def record(self, pid, key, item, *, stage, error_class, error_msg, max_retries):
            self.records.append((key, error_class))

    fr = FakeFailures()
    p = LLMStageProcessor(
        name="summarize",
        mode="per_item",
        input_field="fulltext",
        output_field="summary",
        prompt_file=str(prompt),
        llm_factory=lambda: llm,
        output_parser="text",
        max_input_chars=1000,
        truncate_strategy="head_tail",
        workers=1,
        failures=fr,
        pipeline_id="p",
    )
    ctx = PipelineContext("p", "p/r", "2026-04-28", [mk(0), mk(1), mk(2)], {})
    ctx = await p.process(ctx)
    assert len(ctx.items) == 2
    assert len(fr.records) == 1 and fr.records[0][1] == "RuntimeError"


@pytest.mark.asyncio
async def test_per_item_failure_record_error_still_drops_item(tmp_path):
    prompt = tmp_path / "p.md"
    prompt.write_text("S: {{ item.fulltext }}")

    class FlakyLLM:
        def __init__(self):
            self.n = 0

        async def chat(self, messages):
            self.n += 1
            if self.n == 2:
                raise RuntimeError("transient")
            return f"ok-{self.n}"

    class BrokenFailures:
        async def record(self, pid, key, item, *, stage, error_class, error_msg, max_retries):
            raise RuntimeError("db locked")

    llm = FlakyLLM()
    p = LLMStageProcessor(
        name="summarize",
        mode="per_item",
        input_field="fulltext",
        output_field="summary",
        prompt_file=str(prompt),
        llm_factory=lambda: llm,
        output_parser="text",
        max_input_chars=1000,
        truncate_strategy="head_tail",
        workers=1,
        failures=BrokenFailures(),
        pipeline_id="p",
    )
    ctx = await p.process(PipelineContext("p", "p/r", "2026-04-28", [mk(0), mk(1), mk(2)], {}))
    assert [it.guid for it in ctx.items] == ["g0", "g2"]
    assert [it.summary for it in ctx.items] == ["ok-1", "ok-3"]


@pytest.mark.asyncio
async def test_render_one_preserves_item_get_method(tmp_path):
    prompt = tmp_path / "p.md"
    prompt.write_text("{{ item.fulltext }} / {{ item.get('extras.foo') }}")
    seen = []

    class CaptureLLM:
        async def chat(self, messages):
            seen.append(messages[-1]["content"])
            return "ok"

    it = mk(0)
    it.fulltext = "A" * 50
    it.extras["foo"] = "bar"
    p = LLMStageProcessor(
        name="summarize",
        mode="per_item",
        input_field="fulltext",
        output_field="summary",
        prompt_file=str(prompt),
        llm_factory=lambda: CaptureLLM(),
        output_parser="text",
        max_input_chars=5,
        truncate_strategy="head",
        workers=1,
    )
    ctx = await p.process(PipelineContext("p", "p/r", "2026-04-28", [it], {}))
    assert seen == ["AAAAA / bar"]
    assert ctx.items[0].fulltext == "A" * 50


@pytest.mark.asyncio
async def test_budget_preflight_blocks_on_call_count(tmp_path):
    from sluice.core.errors import BudgetExceeded
    from sluice.llm.budget import RunBudget

    prompt = tmp_path / "p.md"
    prompt.write_text("x")

    class CountLLM:
        def __init__(self):
            self.calls = 0

        async def chat(self, m):
            self.calls += 1
            return "ok"

    llm = CountLLM()
    budget = RunBudget(max_calls=1, max_usd=10.0)
    p = LLMStageProcessor(
        name="s",
        mode="per_item",
        input_field="fulltext",
        output_field="summary",
        prompt_file=str(prompt),
        llm_factory=lambda: llm,
        output_parser="text",
        max_input_chars=1000,
        truncate_strategy="head_tail",
        workers=1,
        budget=budget,
    )
    with pytest.raises(BudgetExceeded):
        await p.process(PipelineContext("p", "p/r", "r", [mk(0), mk(1), mk(2)], {}))
    assert llm.calls == 0


@pytest.mark.asyncio
async def test_budget_preflight_blocks_on_usd(tmp_path):
    from sluice.core.errors import BudgetExceeded
    from sluice.llm.budget import RunBudget

    prompt = tmp_path / "p.md"
    prompt.write_text("{{ item.fulltext }}")

    class CountLLM:
        def __init__(self):
            self.calls = 0

        async def chat(self, m):
            self.calls += 1
            return "ok"

    llm = CountLLM()
    budget = RunBudget(max_calls=999, max_usd=0.01)
    items = [mk(i) for i in range(3)]
    for it in items:
        it.fulltext = "X" * 1000
    p = LLMStageProcessor(
        name="s",
        mode="per_item",
        input_field="fulltext",
        output_field="summary",
        prompt_file=str(prompt),
        llm_factory=lambda: llm,
        output_parser="text",
        max_input_chars=10000,
        truncate_strategy="head_tail",
        workers=1,
        budget=budget,
        model_spec="n/m",
        price_lookup=lambda spec: (0.01, 0.01),
    )
    with pytest.raises(BudgetExceeded):
        await p.process(PipelineContext("p", "p/r", "r", items, {}))
    assert llm.calls == 0


@pytest.mark.asyncio
async def test_parse_error_skip(tmp_path):
    prompt = tmp_path / "p.md"
    prompt.write_text("x")
    llm = FakeLLM(replies=["{bad json"])
    p = LLMStageProcessor(
        name="x",
        mode="per_item",
        input_field="fulltext",
        output_field="summary",
        prompt_file=str(prompt),
        llm_factory=lambda: llm,
        output_parser="json",
        on_parse_error="skip",
        max_input_chars=1000,
        truncate_strategy="head_tail",
        workers=1,
    )
    it = mk(0)
    it.fulltext = "body"
    ctx = await p.process(PipelineContext("p", "p/r", "2026-04-28", [it], {}))
    assert ctx.items == []


@pytest.mark.asyncio
async def test_parse_error_default(tmp_path):
    prompt = tmp_path / "p.md"
    prompt.write_text("x")
    llm = FakeLLM(replies=["{bad json"])
    p = LLMStageProcessor(
        name="x",
        mode="per_item",
        input_field="fulltext",
        output_field="summary",
        prompt_file=str(prompt),
        llm_factory=lambda: llm,
        output_parser="json",
        on_parse_error="default",
        on_parse_error_default={"score": 0},
        max_input_chars=1000,
        truncate_strategy="head_tail",
        workers=1,
    )
    it = mk(0)
    it.fulltext = "body"
    ctx = await p.process(PipelineContext("p", "p/r", "2026-04-28", [it], {}))
    assert ctx.items[0].summary == {"score": 0}


@pytest.mark.asyncio
async def test_parse_error_fail_raises_stage_error(tmp_path):
    """on_parse_error='fail' must propagate StageError, not drop the item."""
    from sluice.core.errors import StageError

    prompt = tmp_path / "p.md"
    prompt.write_text("x")
    llm = FakeLLM(replies=["{bad json"])
    p = LLMStageProcessor(
        name="x",
        mode="per_item",
        input_field="fulltext",
        output_field="summary",
        prompt_file=str(prompt),
        llm_factory=lambda: llm,
        output_parser="json",
        on_parse_error="fail",
        max_input_chars=1000,
        truncate_strategy="head_tail",
        workers=1,
    )
    it = mk(0)
    it.fulltext = "body"
    with pytest.raises(StageError, match="json parse failed"):
        await p.process(PipelineContext("p", "p/r", "2026-04-28", [it], {}))
