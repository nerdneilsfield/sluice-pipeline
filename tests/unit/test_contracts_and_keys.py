from dataclasses import replace

import pytest

from sluice.canonical import (
    canonical_json,
    derived_batch_logical_id,
    derived_each_logical_id,
    derived_observed_at,
    digest,
    make_item_id,
    materialized_item_id,
    stage_lookup_key,
)
from sluice.contracts import (
    DROP,
    FORWARD,
    KEEP_CHECKPOINT,
    NEW,
    BatchInput,
    Drop,
    Item,
    KeepCheckpoint,
    SourceRecord,
    StageDef,
    TaskDef,
)
from sluice.errors import ExecutionError
from sluice.source_runtime import _log_fields
from sluice.stage_runtime import run_stage_tasks, validate_task_outputs


current = Item(
    "current",
    ("source", "x"),
    "x",
    {"title": "A"},
    {},
    "h1",
    "2026-08-14T00:00:00Z",
    1,
)
foreign = Item(
    "foreign",
    ("source", "y"),
    "y",
    {"title": "B"},
    {},
    "h2",
    "2026-08-14T00:00:00Z",
    1,
)
stage = StageDef(
    name="prepare",
    input_mode="each",
    tasks=(TaskDef("sluice.tasks.to_markdown", {}),),
    output_schema=None,
    cache_version=1,
    canonical_config_json='{"tasks":["sluice.tasks.to_markdown"]}',
)


class ResultTask:
    def __init__(self, input_mode, result):
        self.input_mode = input_mode
        self.result = result

    def run(self, config, input):
        return self.result


def test_stage_output_markers_cardinality_and_empty_batch():
    for bad in (None, object(), FORWARD(foreign)):
        with pytest.raises(ExecutionError):
            validate_task_outputs(
                "each", bad, current_items=(current,), history_items=()
            )
    for bad_batch in ((NEW("a"), NEW("b")), (FORWARD(foreign),)):
        with pytest.raises(ExecutionError):
            validate_task_outputs(
                "batch",
                bad_batch,
                current_items=(current,),
                history_items=(foreign,),
            )
    assert validate_task_outputs(
        "batch", (), current_items=(current,), history_items=()
    ) == ()
    aggregate = validate_task_outputs(
        "batch", (NEW([]),), current_items=(current,), history_items=()
    )
    assert len(aggregate) == 1 and aggregate[0].value == []


@pytest.mark.parametrize("input_mode", ["each", "batch"])
def test_task_chain_cannot_forward_an_item_dropped_by_an_intermediate_new(input_mode):
    chain = StageDef(
        name="chain",
        input_mode=input_mode,
        tasks=(TaskDef("first", {}), TaskDef("second", {})),
        output_schema=None,
        cache_version=1,
        canonical_config_json='{"tasks":["first","second"]}',
    )
    first_result = NEW("replacement") if input_mode == "each" else (NEW("replacement"),)
    tasks = (
        ResultTask(input_mode, first_result),
        ResultTask(
            input_mode,
            FORWARD(current) if input_mode == "each" else (FORWARD(current),),
        ),
    )
    with pytest.raises(ExecutionError, match="current Stage Item"):
        run_stage_tasks(chain, (current,), (), tasks)


def test_stage_key_changes_with_item_value():
    same = stage_lookup_key(stage, (current,), ())
    changed = stage_lookup_key(
        stage, (replace(current, payload={"title": "B"}),), ()
    )
    assert same != changed


def test_derived_identity_and_observed_time_depend_only_on_stage_inputs():
    source_a = Item(
        "a",
        ("source", "one"),
        "same",
        {"x": 1},
        {},
        "h1",
        "2026-08-13T00:00:00Z",
        1,
    )
    source_b = Item(
        "b",
        ("source", "two"),
        "same",
        {"x": 1},
        {},
        "h2",
        "2026-08-14T00:00:00Z",
        1,
    )
    assert derived_each_logical_id(source_a) != derived_each_logical_id(source_b)
    assert derived_batch_logical_id((source_a, source_b)) != derived_batch_logical_id(
        (source_b, source_a)
    )
    assert derived_observed_at((source_a, source_b)) == "2026-08-14T00:00:00Z"
    logical_id = derived_batch_logical_id((source_a, source_b))
    assert materialized_item_id(
        "summary", logical_id, {"summary": "ok"}, {}
    ) != materialized_item_id(
        "summary", logical_id, {"summary": "changed"}, {}
    )


def test_marker_values_distinguish_drop_forward_new_none_and_checkpoint_keep():
    assert isinstance(DROP, Drop)
    assert isinstance(KEEP_CHECKPOINT, KeepCheckpoint)
    item = Item(
        id="i",
        producer_scope=("source", "s"),
        logical_id="l",
        payload={"x": 1},
        metadata={},
        content_hash="h",
        observed_at="2026-08-14T00:00:00Z",
        produced_by=1,
    )
    assert FORWARD(item).item is item
    assert NEW(None).value is None


def test_canonical_json_ignores_object_key_order_but_not_string_or_number_value():
    assert canonical_json({"b": 2, "a": 1}) == '{"a":1,"b":2}'
    assert digest({"a": 1, "b": 2}) == digest({"b": 2, "a": 1})
    assert digest({"a": 1}) != digest({"a": "1"})


def test_item_id_includes_typed_producer_scope_and_immutable_metadata():
    left = make_item_id(("source", "news"), "same", {"url": "u"}, {})
    right = make_item_id(("stage", "news"), "same", {"url": "u"}, {})
    assert left != right


def test_item_payload_and_metadata_are_deeply_immutable():
    item = Item(
        id="i",
        producer_scope=("source", "s"),
        logical_id="l",
        payload={"nested": {"x": 1}},
        metadata={"tags": ["a"]},
        content_hash="h",
        observed_at="2026-08-14T00:00:00Z",
        produced_by=1,
    )
    with pytest.raises(TypeError):
        item.payload["nested"]["x"] = 2
    with pytest.raises(AttributeError):
        item.metadata["tags"].append("b")


def test_source_record_freezes_nested_payload_and_metadata_at_construction():
    payload = {"nested": {"values": [1]}}
    metadata = {"nested": {"tags": ["news"]}}
    record = SourceRecord("logical", payload, metadata)

    payload["nested"]["values"].append(2)
    metadata["nested"]["tags"].append("changed")

    assert record.payload["nested"]["values"] == (1,)
    assert record.metadata["nested"]["tags"] == ("news",)
    with pytest.raises(TypeError):
        record.payload["nested"]["values"] = (2,)
    with pytest.raises(TypeError):
        record.metadata["nested"]["tags"] = ("changed",)


def test_new_freezes_nested_metadata_at_construction():
    metadata = {"nested": {"tags": ["news"]}}
    marker = NEW(None, metadata)

    metadata["nested"]["tags"].append("changed")

    assert marker.metadata["nested"]["tags"] == ("news",)
    with pytest.raises(AttributeError):
        marker.metadata["nested"]["tags"].append("changed")


def test_structured_logs_distinguish_keep_checkpoint_from_explicit_null():
    keep = _log_fields("source", "binding", 1, KEEP_CHECKPOINT, None, "running")
    explicit_null = _log_fields("source", "binding", 1, None, None, "running")

    assert keep["checkpoint"] != explicit_null["checkpoint"]
    assert explicit_null["checkpoint"] is None
