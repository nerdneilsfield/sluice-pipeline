import pytest

from sluice.canonical import canonical_json, digest, make_item_id
from sluice.contracts import (
    DROP,
    FORWARD,
    KEEP_CHECKPOINT,
    NEW,
    Drop,
    Item,
    KeepCheckpoint,
    SourceRecord,
)
from sluice.source_runtime import _log_fields


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
