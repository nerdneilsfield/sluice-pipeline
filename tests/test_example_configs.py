import tomllib
from pathlib import Path

from sluice.config import PipelineConfig, ProvidersConfig


def test_ai_news_example_model_refs_resolve_to_provider_models():
    providers = ProvidersConfig.model_validate(
        tomllib.loads(Path("configs/providers.toml.example").read_text())
    )
    pipeline = PipelineConfig.model_validate(
        tomllib.loads(Path("configs/pipelines/ai_news.toml.example").read_text())
    )
    model_entries = {
        f"{provider.name}/{model.model_name}": model
        for provider in providers.providers
        for model in provider.models
    }

    refs = []
    for stage in pipeline.stages:
        for field in (
            "model",
            "retry_model",
            "fallback_model",
            "fallback_model_2",
            "long_context_model",
        ):
            value = getattr(stage, field, None)
            if value:
                refs.append((stage.name, field, value))

    missing = [(stage, field, value) for stage, field, value in refs if value not in model_entries]
    assert missing == []

    summarize = next(stage for stage in pipeline.stages if stage.name == "summarize")
    assert summarize.long_context_model == "glm/glm-4-plus"
    assert summarize.max_input_chars >= 400_000
    assert (
        model_entries[summarize.long_context_model].max_input_tokens
        > model_entries[summarize.model].max_input_tokens
    )
