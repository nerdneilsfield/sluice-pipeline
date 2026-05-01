from sluice.config import (
    LLMStageConfig,
    ScoreTagConfig,
    SummarizeScoreTagConfig,
)


def _llm_stage(**extra):
    return LLMStageConfig(
        type="llm_stage",
        name="t",
        mode="aggregate",
        input_field="x",
        output_target="y",
        prompt_file="p.md",
        model="p/m",
        **extra,
    )


def test_llm_stage_long_context_default_none():
    assert _llm_stage().long_context_model is None


def test_llm_stage_long_context_set():
    assert _llm_stage(long_context_model="prov/lc").long_context_model == "prov/lc"


def test_llm_stage_middleware_knobs():
    cfg = _llm_stage(
        same_model_retries=4,
        same_model_retry_backoff=0.25,
        overflow_trim_step_tokens=12_345,
        long_context_threshold_ratio=0.6,
    )
    assert cfg.same_model_retries == 4
    assert cfg.same_model_retry_backoff == 0.25
    assert cfg.overflow_trim_step_tokens == 12_345
    assert cfg.long_context_threshold_ratio == 0.6


def test_score_tag_long_context_field():
    cfg = ScoreTagConfig(
        type="score_tag",
        name="s",
        input_field="x",
        prompt_file="p.md",
        model="p/m",
        long_context_model="prov/lc",
        same_model_retries=3,
        overflow_trim_step_tokens=50_000,
    )
    assert cfg.long_context_model == "prov/lc"
    assert cfg.same_model_retries == 3
    assert cfg.overflow_trim_step_tokens == 50_000


def test_summarize_score_tag_long_context_field():
    cfg = SummarizeScoreTagConfig(
        type="summarize_score_tag",
        name="s",
        input_field="x",
        prompt_file="p.md",
        model="p/m",
        long_context_model="prov/lc",
        long_context_threshold_ratio=0.7,
    )
    assert cfg.long_context_model == "prov/lc"
    assert cfg.long_context_threshold_ratio == 0.7
