import json
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from sluice.processors.summarize_score_tag import SummarizeScoreTagProcessor, _parse_result
from tests.conftest import make_ctx, make_item


# ── parser tests ────────────────────────────────────────────────────────────

def test_parse_returns_score_tags_summary():
    payload = '{"score": 8, "tags": ["AI", "LLM"], "summary": "Great article."}'
    score, tags, summary = _parse_result(payload)
    assert score == 8
    assert tags == ["AI", "LLM"]
    assert summary == "Great article."


def test_parse_strips_fence():
    raw = '```json\n{"score": 7, "tags": [], "summary": "Neat."}\n```'
    score, tags, summary = _parse_result(raw)
    assert score == 7
    assert summary == "Neat."


def test_parse_clamps_score_to_1_10():
    score, _, _ = _parse_result('{"score": 99, "tags": [], "summary": "x"}')
    assert score == 10
    score, _, _ = _parse_result('{"score": -5, "tags": [], "summary": "x"}')
    assert score == 1


def test_parse_raises_if_score_missing():
    with pytest.raises(ValueError, match="score field missing"):
        _parse_result('{"tags": [], "summary": "x"}')


def test_parse_raises_if_summary_missing():
    with pytest.raises(ValueError, match="summary field missing"):
        _parse_result('{"score": 5, "tags": []}')


def test_parse_raises_if_summary_not_string():
    with pytest.raises(ValueError, match="summary must be a string"):
        _parse_result('{"score": 5, "tags": [], "summary": 42}')


def test_parse_raises_if_tags_not_list():
    with pytest.raises(ValueError, match="tags must be a list"):
        _parse_result('{"score": 5, "tags": "bad", "summary": "x"}')


def test_parse_float_score_rounds():
    score, _, _ = _parse_result('{"score": 7.6, "tags": [], "summary": "x"}')
    assert score == 8


def test_parse_empty_tags_allowed():
    _, tags, _ = _parse_result('{"score": 5, "summary": "x"}')
    assert tags == []


def test_parse_strips_whitespace_from_summary():
    _, _, summary = _parse_result('{"score": 5, "tags": [], "summary": "  hi  "}')
    assert summary == "hi"


# ── processor integration tests ─────────────────────────────────────────────

def _make_processor(tmp_path: Path, llm_response: str, **kwargs) -> SummarizeScoreTagProcessor:
    prompt = tmp_path / "prompt.md"
    prompt.write_text("Analyze: {{ item.title }}")

    from unittest.mock import MagicMock
    mock_llm = MagicMock()
    mock_llm.chat = AsyncMock(return_value=llm_response)

    return SummarizeScoreTagProcessor(
        name="test_sst",
        input_field="title",
        prompt_file=str(prompt),
        llm_factory=lambda: mock_llm,
        **kwargs,
    )


@pytest.mark.asyncio
async def test_process_writes_score_summary_tags(tmp_path):
    llm_out = '{"score": 9, "tags": ["ML"], "summary": "Important paper."}'
    proc = _make_processor(tmp_path, llm_out)
    ctx = make_ctx(items=[make_item(title="GPT-4")])
    ctx = await proc.process(ctx)
    assert len(ctx.items) == 1
    item = ctx.items[0]
    assert item.extras["score"] == 9
    assert item.extras["summary"] == "Important paper."
    assert "ML" in item.tags


@pytest.mark.asyncio
async def test_custom_score_and_summary_fields(tmp_path):
    llm_out = '{"score": 6, "tags": [], "summary": "Decent."}'
    proc = _make_processor(tmp_path, llm_out, score_field="relevance", summary_field="tldr")
    ctx = make_ctx(items=[make_item()])
    ctx = await proc.process(ctx)
    item = ctx.items[0]
    assert item.extras["relevance"] == 6
    assert item.extras["tldr"] == "Decent."


@pytest.mark.asyncio
async def test_on_parse_error_skip_keeps_item(tmp_path):
    proc = _make_processor(tmp_path, "not json", on_parse_error="skip")
    ctx = make_ctx(items=[make_item()])
    ctx = await proc.process(ctx)
    assert len(ctx.items) == 1
    assert "score" not in ctx.items[0].extras


@pytest.mark.asyncio
async def test_on_parse_error_default_applies_defaults(tmp_path):
    proc = _make_processor(
        tmp_path,
        "not json",
        on_parse_error="default",
        default_score=3,
        default_tags=["fallback"],
        default_summary="no summary",
    )
    ctx = make_ctx(items=[make_item()])
    ctx = await proc.process(ctx)
    item = ctx.items[0]
    assert item.extras["score"] == 3
    assert item.extras["summary"] == "no summary"
    assert "fallback" in item.tags


@pytest.mark.asyncio
async def test_tags_merge_replace(tmp_path):
    llm_out = '{"score": 7, "tags": ["new"], "summary": "x"}'
    proc = _make_processor(tmp_path, llm_out, tags_merge="replace")
    item = make_item(tags=["old"])
    ctx = make_ctx(items=[item])
    ctx = await proc.process(ctx)
    assert ctx.items[0].tags == ["new"]


@pytest.mark.asyncio
async def test_tags_merge_append_deduplicates(tmp_path):
    llm_out = '{"score": 7, "tags": ["existing", "new"], "summary": "x"}'
    proc = _make_processor(tmp_path, llm_out, tags_merge="append")
    item = make_item(tags=["existing"])
    ctx = make_ctx(items=[item])
    ctx = await proc.process(ctx)
    assert ctx.items[0].tags.count("existing") == 1
    assert "new" in ctx.items[0].tags


@pytest.mark.asyncio
async def test_multiple_items_processed_concurrently(tmp_path):
    llm_out = '{"score": 5, "tags": [], "summary": "ok"}'
    proc = _make_processor(tmp_path, llm_out, workers=2)
    items = [make_item(url=f"https://x.com/{i}", guid=str(i)) for i in range(4)]
    ctx = make_ctx(items=items)
    ctx = await proc.process(ctx)
    assert len(ctx.items) == 4
    assert all(it.extras["score"] == 5 for it in ctx.items)


@pytest.mark.asyncio
async def test_empty_batch_passes_through(tmp_path):
    proc = _make_processor(tmp_path, '{"score": 5, "tags": [], "summary": "x"}')
    ctx = make_ctx(items=[])
    ctx = await proc.process(ctx)
    assert ctx.items == []
