"""Retained web snapshot decoding and private invocation boundaries."""

from __future__ import annotations

import json
from datetime import timedelta

import pytest

from sluice.canonical import canonical_json
from sluice.config import decode_stage_snapshot
from sluice.contracts import TaskDef
from sluice.errors import ExecutionError
from sluice.resolution import private_web_invocation
from sluice.web.contracts import (
    ExtractorBinding,
    PrivateWebInvocation,
    ProviderBinding,
    WebBinding,
)


def _stage(tasks: list[dict[str, object]]) -> dict[str, object]:
    return {
        "name": "article",
        "input": "each",
        "cache_version": 1,
        "tasks": tasks,
        "output_schema": None,
    }


def test_snapshot_decoder_preserves_canonical_bytes_for_both_task_shapes():
    binding = WebBinding(output="markdown")
    legacy = canonical_json(
        _stage(
            [
                {
                    "use": "sluice.tasks.fetch_webpage",
                    "with": {"url_field": "url"},
                }
            ]
        )
    )
    current = canonical_json(
        _stage(
            [
                {
                    "use": "sluice.tasks.to_markdown",
                    "with": {},
                    "web_binding": binding.semantic_projection(),
                }
            ]
        )
    )

    legacy_stage = decode_stage_snapshot(json.loads(legacy))
    current_stage = decode_stage_snapshot(json.loads(current))

    assert legacy_stage.tasks[0].web_binding is None
    assert legacy_stage.canonical_config_json == legacy
    assert current_stage.tasks[0].web_binding is not None
    assert current_stage.canonical_config_json == current


def _current_web_snapshot(
    projection: dict[str, object], use: str = "sluice.tasks.to_markdown"
) -> dict[str, object]:
    return _stage(
        [
            {
                "use": use,
                "with": {},
                "web_binding": projection,
            }
        ]
    )


def _projection(binding: WebBinding | None = None) -> dict[str, object]:
    value = binding or WebBinding(output="markdown")
    return json.loads(canonical_json(value.semantic_projection()))


def _provider_projection() -> dict[str, object]:
    return _projection(
        WebBinding(
            fetch_chain=(
                ProviderBinding(
                    name="provider",
                    type="httpx",
                    timeout=timedelta(seconds=30),
                    connect_timeout=timedelta(seconds=10),
                ),
            ),
            fetch_chain_name="chain",
            extract_chain=(ExtractorBinding(name="identity", type="identity"),),
            extract_chain_name="extract",
        )
    )


def test_snapshot_decoder_rejects_missing_web_binding_projection_field():
    projection = _projection()
    projection.pop("output")

    with pytest.raises(ExecutionError):
        decode_stage_snapshot(_current_web_snapshot(projection))


def test_snapshot_decoder_rejects_missing_web_binding_quality_field():
    projection = _projection()
    quality = projection["quality"]
    assert isinstance(quality, dict)
    quality.pop("min_chars")

    with pytest.raises(ExecutionError):
        decode_stage_snapshot(_current_web_snapshot(projection))


def test_snapshot_decoder_rejects_missing_web_binding_chain_field():
    binding = WebBinding(
        fetch_chain=(ProviderBinding(name="provider", type="httpx"),),
        fetch_chain_name="chain",
    )
    projection = _projection(binding)
    fetch_chain = projection["fetch_chain"]
    assert isinstance(fetch_chain, dict)
    fetch_chain.pop("providers")

    with pytest.raises(ExecutionError):
        decode_stage_snapshot(_current_web_snapshot(projection))


def test_snapshot_decoder_rejects_missing_web_binding_provider_field():
    binding = WebBinding(
        fetch_chain=(ProviderBinding(name="provider", type="httpx"),),
        fetch_chain_name="chain",
    )
    projection = _projection(binding)
    fetch_chain = projection["fetch_chain"]
    assert isinstance(fetch_chain, dict)
    providers = fetch_chain["providers"]
    assert isinstance(providers, list)
    provider = providers[0]
    assert isinstance(provider, dict)
    provider.pop("type")

    with pytest.raises(ExecutionError):
        decode_stage_snapshot(_current_web_snapshot(projection))


def test_snapshot_decoder_rejects_unknown_web_binding_projection_field():
    projection = _projection()
    projection["unexpected"] = True

    with pytest.raises(ExecutionError):
        decode_stage_snapshot(_current_web_snapshot(projection))


def test_snapshot_decoder_rejects_disabled_web_binding_challenge_safety():
    projection = _projection()
    quality = projection["quality"]
    assert isinstance(quality, dict)
    quality["reject_challenges"] = False

    with pytest.raises(ExecutionError):
        decode_stage_snapshot(_current_web_snapshot(projection))


def test_snapshot_decoder_rejects_web_binding_on_custom_task():
    with pytest.raises(ExecutionError):
        decode_stage_snapshot(_current_web_snapshot(_projection(), use="custom.task"))


def test_snapshot_decoder_rejects_source_only_cache_version_on_stage_task():
    projection = _projection()
    projection["web_cache_version"] = 1

    with pytest.raises(ExecutionError):
        decode_stage_snapshot(_current_web_snapshot(projection))


@pytest.mark.parametrize(
    "use",
    ["sluice.tasks.fetch_webpage", "sluice.tasks.extract_content"],
)
def test_snapshot_decoder_rejects_empty_web_task_chain(use):
    projection = _projection(
        WebBinding(
            fetch_chain=() if use.endswith("fetch_webpage") else None,
            extract_chain=() if use.endswith("extract_content") else None,
            fetch_chain_name="chain" if use.endswith("fetch_webpage") else None,
            extract_chain_name="chain" if use.endswith("extract_content") else None,
        )
    )

    with pytest.raises(ExecutionError):
        decode_stage_snapshot(_current_web_snapshot(projection, use=use))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("timeout", None),
        ("connect_timeout", None),
        ("timeout", 30.5),
        ("connect_timeout", 10.5),
        ("timeout_seconds", 31),
    ],
)
def test_snapshot_decoder_rejects_invalid_retained_provider_timing(field, value):
    projection = _provider_projection()
    fetch_chain = projection["fetch_chain"]
    assert isinstance(fetch_chain, dict)
    providers = fetch_chain["providers"]
    assert isinstance(providers, list)
    provider = providers[0]
    assert isinstance(provider, dict)
    provider[field] = value

    with pytest.raises(ExecutionError):
        decode_stage_snapshot(_current_web_snapshot(projection))


def test_snapshot_decoder_rejects_provider_connection_timeout_longer_than_timeout():
    projection = _provider_projection()
    fetch_chain = projection["fetch_chain"]
    assert isinstance(fetch_chain, dict)
    providers = fetch_chain["providers"]
    assert isinstance(providers, list)
    provider = providers[0]
    assert isinstance(provider, dict)
    provider["connect_timeout"] = 31

    with pytest.raises(ExecutionError):
        decode_stage_snapshot(_current_web_snapshot(projection))


def test_snapshot_decoder_normalizes_whole_float_timeout_seconds():
    projection = _provider_projection()
    fetch_chain = projection["fetch_chain"]
    assert isinstance(fetch_chain, dict)
    providers = fetch_chain["providers"]
    assert isinstance(providers, list)
    provider = providers[0]
    assert isinstance(provider, dict)
    provider["timeout_seconds"] = 30.0

    stage = decode_stage_snapshot(
        _current_web_snapshot(projection, use="sluice.tasks.fetch_webpage")
    )
    timeout_seconds = stage.tasks[0].web_binding.providers[0].timeout_seconds  # type: ignore[union-attr]
    assert timeout_seconds == 30
    assert isinstance(timeout_seconds, int)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("base_url", "http://["),
        ("base_url", "https://user:secret@example.test"),
        ("proxy", "http://["),
        ("proxy", "http://user:secret@example.test"),
    ],
)
def test_snapshot_decoder_rejects_malformed_provider_urls(field, value):
    projection = _provider_projection()
    fetch_chain = projection["fetch_chain"]
    assert isinstance(fetch_chain, dict)
    providers = fetch_chain["providers"]
    assert isinstance(providers, list)
    provider = providers[0]
    assert isinstance(provider, dict)
    provider[field] = value

    with pytest.raises(ExecutionError):
        decode_stage_snapshot(_current_web_snapshot(projection))


def test_private_invocation_is_impossible_for_a_custom_task():
    custom_task_def = TaskDef(use="custom.task", with_config={"value": 1})
    assert private_web_invocation(custom_task_def) is None


def test_private_invocation_is_typed_and_frozen_for_a_web_builtin():
    binding = WebBinding(output="markdown")
    task_def = TaskDef(
        use="sluice.tasks.to_markdown",
        with_config={"input_format": "auto"},
        web_binding=binding,
    )

    invocation = private_web_invocation(task_def)

    assert isinstance(invocation, PrivateWebInvocation)
    assert invocation.web_binding is binding
    assert invocation.with_config["input_format"] == "auto"
    assert not isinstance(invocation, dict)
