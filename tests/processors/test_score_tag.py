from unittest.mock import AsyncMock, MagicMock

import pytest

from sluice.processors.score_tag import ScoreTagProcessor, _parse_score_tag, _strip_fence
from tests.conftest import make_ctx, make_item


def test_strip_fence_removes_json_fence():
    raw = '```json\n{"score": 7, "tags": ["AI"]}\n```'
    assert _strip_fence(raw) == '{"score": 7, "tags": ["AI"]}'


def test_strip_fence_removes_plain_fence():
    raw = '```\n{"score": 5}\n```'
    assert _strip_fence(raw) == '{"score": 5}'


def test_strip_fence_leaves_unfenced_alone():
    raw = '{"score": 7}'
    assert _strip_fence(raw) == raw


def test_strip_fence_no_strip_if_text_outside_fence():
    raw = 'Here is JSON:\n```json\n{"score": 7}\n```\nThat was it.'
    assert _strip_fence(raw) == raw


def test_parse_integer_score():
    score, tags = _parse_score_tag('{"score": 7, "tags": ["AI"]}')
    assert score == 7
    assert tags == ["AI"]


def test_parse_float_score_rounds_to_nearest():
    score, _ = _parse_score_tag('{"score": 7.4}')
    assert score == 7
    score, _ = _parse_score_tag('{"score": 7.6}')
    assert score == 8
    score, _ = _parse_score_tag('{"score": 8.5}')
    assert score == 8


def test_parse_numeric_string_score():
    score, _ = _parse_score_tag('{"score": "7.2"}')
    assert score == 7


def test_parse_boolean_score_raises():
    with pytest.raises(ValueError, match="score"):
        _parse_score_tag('{"score": true}')


def test_parse_clamps_score_below_1():
    score, _ = _parse_score_tag('{"score": -3}')
    assert score == 1


def test_parse_clamps_score_above_10():
    score, _ = _parse_score_tag('{"score": 99}')
    assert score == 10


@pytest.mark.parametrize("raw", ['{"score": NaN}', '{"score": Infinity}'])
def test_parse_rejects_non_finite_score(raw):
    with pytest.raises(ValueError, match="finite"):
        _parse_score_tag(raw)


def test_parse_missing_score_raises():
    with pytest.raises(ValueError, match="score"):
        _parse_score_tag('{"tags": ["AI"]}')


def test_parse_non_numeric_score_raises():
    with pytest.raises(ValueError, match="score"):
        _parse_score_tag('{"score": "high"}')


def test_parse_requires_json_object():
    with pytest.raises(ValueError, match="object"):
        _parse_score_tag('[{"score": 7}]')


def test_parse_missing_tags_returns_empty():
    _, tags = _parse_score_tag('{"score": 7}')
    assert tags == []


@pytest.mark.parametrize("raw", ['{"score": 7, "tags": "AI"}', '{"score": 7, "tags": null}'])
def test_parse_non_list_tags_raises(raw):
    with pytest.raises(ValueError, match="tags"):
        _parse_score_tag(raw)


def test_parse_non_string_tags_dropped():
    _, tags = _parse_score_tag('{"score": 7, "tags": ["AI", 42, null, "ML"]}')
    assert tags == ["AI", "ML"]


def test_parse_strips_and_drops_empty_tags():
    _, tags = _parse_score_tag('{"score": 7, "tags": ["  AI  ", "", "  "]}')
    assert tags == ["AI"]


def test_head_tail_truncate_respects_limit():
    from sluice.processors.score_tag import _truncate

    truncated = _truncate("abcdefghijklmnop", limit=10, strategy="head_tail")

    assert len(truncated) <= 10
    assert truncated.startswith("ab")
    assert truncated.endswith("nop")


def test_parse_fence_stripped_before_parse():
    score, tags = _parse_score_tag('```json\n{"score": 6, "tags": ["Rust"]}\n```')
    assert score == 6
    assert tags == ["Rust"]


def _make_proc(llm_output='{"score": 7}', on_parse_error="skip", **kw) -> ScoreTagProcessor:
    mock_llm = MagicMock()
    mock_llm.chat = AsyncMock(return_value=llm_output)
    defaults = dict(
        name="st",
        input_field="fulltext",
        prompt_file="prompts/score_tag.md",
        llm_factory=lambda: mock_llm,
        workers=1,
        score_field="score",
        tags_merge="append",
        on_parse_error=on_parse_error,
        default_score=5,
        default_tags=[],
        max_input_chars=8000,
        truncate_strategy="head_tail",
        budget=None,
        failures=None,
        pipeline_id="p",
        max_retries=3,
        model_spec="test/model",
        price_lookup=lambda _: (0.0, 0.0),
    )
    defaults.update(kw)
    return ScoreTagProcessor(**defaults)


@pytest.mark.asyncio
async def test_writes_score_to_extras():
    it = make_item(fulltext="article text")
    proc = _make_proc('{"score": 8, "tags": ["AI"]}')
    ctx = await proc.process(make_ctx(items=[it]))
    assert ctx.items[0].extras["score"] == 8


@pytest.mark.asyncio
async def test_appends_tags_case_sensitive_dedup_preserves_existing_order():
    it = make_item(fulltext="text", tags=["AI", "existing"])
    proc = _make_proc('{"score": 7, "tags": ["new", "AI", "ai", "new"]}')
    ctx = await proc.process(make_ctx(items=[it]))
    assert ctx.items[0].tags == ["AI", "existing", "new", "ai"]


@pytest.mark.asyncio
async def test_replaces_tags_with_received_order_dedup():
    it = make_item(fulltext="text", tags=["old"])
    proc = _make_proc('{"score": 7, "tags": ["new", "AI", "new"]}', tags_merge="replace")
    ctx = await proc.process(make_ctx(items=[it]))
    assert ctx.items[0].tags == ["new", "AI"]


@pytest.mark.asyncio
async def test_on_parse_error_skip_leaves_item_unchanged():
    it = make_item(fulltext="text", tags=["kept"], extras={"old": 1})
    proc = _make_proc("not json", on_parse_error="skip")
    ctx = await proc.process(make_ctx(items=[it]))
    assert ctx.items == [it]
    assert ctx.items[0].extras == {"old": 1}
    assert ctx.items[0].tags == ["kept"]


@pytest.mark.asyncio
async def test_on_parse_error_default_applies_defaults_with_merge_semantics():
    it = make_item(fulltext="text", tags=["existing"])
    proc = _make_proc(
        "not json",
        on_parse_error="default",
        default_score=3,
        default_tags=["fallback", "existing"],
    )
    ctx = await proc.process(make_ctx(items=[it]))
    assert ctx.items[0].extras["score"] == 3
    assert ctx.items[0].tags == ["existing", "fallback"]


@pytest.mark.asyncio
async def test_on_parse_error_default_cleans_default_tags():
    it = make_item(fulltext="text")
    proc = _make_proc(
        "not json",
        on_parse_error="default",
        default_tags=["  fallback  ", "", "  "],
    )
    ctx = await proc.process(make_ctx(items=[it]))
    assert ctx.items[0].tags == ["fallback"]


@pytest.mark.asyncio
async def test_on_parse_error_fail_records_and_drops_item():
    class FakeFailures:
        def __init__(self):
            self.records = []

        async def record(self, pid, key, item, *, stage, error_class, error_msg, max_retries):
            self.records.append((pid, key, item.url, stage, error_class, error_msg, max_retries))

    failures = FakeFailures()
    it = make_item(fulltext="text", url="https://bad.com")
    proc = _make_proc("not json", on_parse_error="fail", failures=failures, max_retries=4)
    ctx = await proc.process(make_ctx(items=[it]))
    assert ctx.items == []
    assert len(failures.records) == 1
    assert failures.records[0][3] == "st"
    assert failures.records[0][4] == "ValueError"
    assert failures.records[0][6] == 4


@pytest.mark.asyncio
async def test_on_parse_error_fail_drops_without_recording_when_failures_none():
    it = make_item(fulltext="text")
    proc = _make_proc("not json", on_parse_error="fail", failures=None)
    ctx = await proc.process(make_ctx(items=[it]))
    assert ctx.items == []


@pytest.mark.asyncio
async def test_one_item_failure_does_not_affect_others():
    good = make_item(fulltext="good text", url="https://good.com")
    bad = make_item(fulltext="bad text", url="https://bad.com")

    async def chat_side_effect(msgs):
        if "bad" in msgs[0]["content"]:
            return "invalid json"
        return '{"score": 9, "tags": ["AI"]}'

    mock_llm = MagicMock()
    mock_llm.chat = AsyncMock(side_effect=chat_side_effect)
    proc = _make_proc(llm_factory=lambda: mock_llm, on_parse_error="skip", workers=2)
    ctx = await proc.process(make_ctx(items=[good, bad]))
    assert len(ctx.items) == 2
    scored = next(it for it in ctx.items if it.url == "https://good.com")
    unscored = next(it for it in ctx.items if it.url == "https://bad.com")
    assert scored.extras["score"] == 9
    assert "score" not in unscored.extras


@pytest.mark.asyncio
async def test_renders_top_level_field_with_truncated_input(tmp_path):
    prompt = tmp_path / "prompt.md"
    prompt.write_text("{{ item.fulltext }} / {{ item.title }}")
    seen = []

    async def chat(messages):
        seen.append(messages[0]["content"])
        return '{"score": 7}'

    mock_llm = MagicMock()
    mock_llm.chat = AsyncMock(side_effect=chat)
    it = make_item(title="Title", fulltext="abcdef")
    proc = _make_proc(
        prompt_file=str(prompt),
        llm_factory=lambda: mock_llm,
        max_input_chars=3,
        truncate_strategy="head",
    )
    ctx = await proc.process(make_ctx(items=[it]))
    assert seen == ["abc / Title"]
    assert ctx.items[0].fulltext == "abcdef"


@pytest.mark.asyncio
async def test_renders_extras_field_view_without_nested_extras_key(tmp_path):
    prompt = tmp_path / "prompt.md"
    prompt.write_text("{{ item.extras.body }} / {{ item.extras.get('extras.body', 'missing') }}")
    seen = []

    async def chat(messages):
        seen.append(messages[0]["content"])
        return '{"score": 7}'

    mock_llm = MagicMock()
    mock_llm.chat = AsyncMock(side_effect=chat)
    it = make_item(extras={"body": "abcdef"}, fulltext="original")
    proc = _make_proc(
        input_field="extras.body",
        prompt_file=str(prompt),
        llm_factory=lambda: mock_llm,
        max_input_chars=3,
        truncate_strategy="head",
    )
    ctx = await proc.process(make_ctx(items=[it]))
    assert seen == ["abc / missing"]
    assert ctx.items[0].extras["body"] == "abcdef"
    assert "extras.body" not in ctx.items[0].extras


@pytest.mark.asyncio
async def test_truncate_strategy_error_uses_parse_error_policy_without_llm_call():
    chat_mock = AsyncMock(return_value='{"score": 7}')
    mock_llm = MagicMock()
    mock_llm.chat = chat_mock
    it = make_item(fulltext="abcdef")
    proc = _make_proc(
        llm_factory=lambda: mock_llm,
        truncate_strategy="error",
        max_input_chars=3,
        on_parse_error="default",
        default_score=4,
        default_tags=["too-long"],
    )
    ctx = await proc.process(make_ctx(items=[it]))
    chat_mock.assert_not_called()
    assert ctx.items[0].extras["score"] == 4
    assert ctx.items[0].tags == ["too-long"]


@pytest.mark.asyncio
async def test_budget_max_calls_blocks_llm_using_injected_mock():
    from sluice.core.errors import BudgetExceeded

    budget = MagicMock()
    budget.project.return_value = False
    chat_mock = AsyncMock(return_value='{"score": 7}')
    mock_llm = MagicMock()
    mock_llm.chat = chat_mock
    proc = _make_proc(budget=budget, llm_factory=lambda: mock_llm)

    with pytest.raises(BudgetExceeded):
        await proc.process(make_ctx(items=[make_item(fulltext="text")]))

    budget.project.assert_called_once()
    chat_mock.assert_not_called()


def test_score_field_with_dot_raises():
    from sluice.config import ScoreTagConfig
    from sluice.core.errors import ConfigError

    with pytest.raises(ConfigError):
        ScoreTagConfig(
            type="score_tag",
            name="x",
            input_field="fulltext",
            prompt_file="p.md",
            model="m",
            score_field="extras.score",
        )
