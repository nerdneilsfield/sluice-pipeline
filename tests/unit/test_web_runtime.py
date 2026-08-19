"""Retained web snapshot decoding and private invocation boundaries."""

from __future__ import annotations

import json

import pytest

from sluice.canonical import canonical_json
from sluice.config import decode_stage_snapshot
from sluice.contracts import TaskDef
from sluice.errors import ExecutionError
from sluice.resolution import private_web_invocation
from sluice.web.contracts import (
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


def _current_web_snapshot(projection: dict[str, object]) -> dict[str, object]:
    return _stage(
        [
            {
                "use": "sluice.tasks.to_markdown",
                "with": {},
                "web_binding": projection,
            }
        ]
    )


def _projection(binding: WebBinding | None = None) -> dict[str, object]:
    value = binding or WebBinding(output="markdown")
    return json.loads(canonical_json(value.semantic_projection()))


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
