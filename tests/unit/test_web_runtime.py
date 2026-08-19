"""Retained web snapshot decoding and private invocation boundaries."""

from __future__ import annotations

import json

from sluice.canonical import canonical_json
from sluice.config import decode_stage_snapshot
from sluice.contracts import TaskDef
from sluice.resolution import private_web_invocation
from sluice.web.contracts import PrivateWebInvocation, WebBinding


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
