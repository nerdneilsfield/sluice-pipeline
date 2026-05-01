import pytest

from sluice.builders import build_processors
from sluice.config import (
    BaseEndpoint,
    GlobalConfig,
    KeyConfig,
    LLMStageConfig,
    ModelEntry,
    PipelineConfig,
    Provider,
    ProvidersConfig,
    ScoreTagConfig,
    SummarizeScoreTagConfig,
)
from sluice.llm.middleware import LLMMiddleware
from sluice.llm.pool import ProviderPool


def _pool(monkeypatch) -> ProviderPool:
    monkeypatch.setenv("K", "v")
    return ProviderPool(
        ProvidersConfig(
            providers=[
                Provider(
                    name="p",
                    type="openai_compatible",
                    base=[
                        BaseEndpoint(
                            url="https://llm",
                            weight=1,
                            key=[KeyConfig(value="env:K", weight=1)],
                        )
                    ],
                    models=[
                        ModelEntry(model_name="m"),
                        ModelEntry(model_name="lc", max_input_tokens=1_000_000),
                    ],
                )
            ]
        )
    )


@pytest.mark.parametrize(
    "stage",
    [
        LLMStageConfig(
            type="llm_stage",
            name="llm",
            mode="per_item",
            input_field="fulltext",
            output_field="summary",
            prompt_file="{prompt}",
            model="p/m",
            long_context_model="p/lc",
            same_model_retries=4,
            same_model_retry_backoff=0.25,
            overflow_trim_step_tokens=12_345,
            long_context_threshold_ratio=0.6,
        ),
        ScoreTagConfig(
            type="score_tag",
            name="score",
            input_field="fulltext",
            prompt_file="{prompt}",
            model="p/m",
            long_context_model="p/lc",
            same_model_retries=4,
            same_model_retry_backoff=0.25,
            overflow_trim_step_tokens=12_345,
            long_context_threshold_ratio=0.6,
        ),
        SummarizeScoreTagConfig(
            type="summarize_score_tag",
            name="summarize_score",
            input_field="fulltext",
            prompt_file="{prompt}",
            model="p/m",
            long_context_model="p/lc",
            same_model_retries=4,
            same_model_retry_backoff=0.25,
            overflow_trim_step_tokens=12_345,
            long_context_threshold_ratio=0.6,
        ),
    ],
)
def test_build_processors_threads_long_context_model(stage, tmp_path, monkeypatch):
    prompt = tmp_path / "prompt.md"
    prompt.write_text("{{ item.fulltext }}")
    stage.prompt_file = str(prompt)
    pipe = PipelineConfig(id="p", sources=[], stages=[stage], sinks=[])

    procs = build_processors(
        pipe=pipe,
        global_cfg=GlobalConfig(),
        seen=None,
        failures=None,
        fetcher_chain=None,
        llm_pool=_pool(monkeypatch),
        budget=None,
        dry_run=True,
    )

    middleware = procs[0].llm_factory()
    assert isinstance(middleware, LLMMiddleware)
    assert middleware.cfg.model == "p/m"
    assert middleware.cfg.long_context_model == "p/lc"
    assert middleware.cfg.same_model_retries == 4
    assert middleware.cfg.same_model_retry_backoff == 0.25
    assert middleware.cfg.overflow_trim_step_tokens == 12_345
    assert middleware.cfg.long_context_threshold_ratio == 0.6
