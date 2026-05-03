from __future__ import annotations

import pytest

from sluice.dag.item import ITEM_COLUMNS, Item


def _row(**over):
    base = {
        "item_id": "x",
        "source_name": "s",
        "guid": None,
        "url": None,
        "title": None,
        "author": None,
        "published_at": None,
        "ingested_at": "2026-05-04T00:00:00+00:00",
        "ingested_by": "P",
        "updated_at": "2026-05-04T00:00:00+00:00",
        "tag": None,
        "raw_summary": None,
        "raw_content": None,
        "raw_json": "{}",
        "fulltext": None,
        "attachments_json": None,
        "summary": None,
        "score": None,
        "tags_json": None,
        "extras_json": None,
        "dropped": 0,
        "dropped_reason": None,
        "dropped_at": None,
    }
    base.update(over)
    return base


def test_item_columns_match_active_schema():
    must_have = {
        "item_id",
        "source_name",
        "ingested_at",
        "ingested_by",
        "updated_at",
        "raw_json",
        "dropped",
        "dropped_at",
    }
    assert must_have.issubset(ITEM_COLUMNS)


def test_item_construct_from_full_row_and_dirty_tracking():
    item = Item.from_row(_row())
    assert item["item_id"] == "x"
    assert item.dirty_columns() == frozenset()
    item["title"] = "new"
    assert item["title"] == "new"
    assert item.dirty_columns() == frozenset({"title"})
    item["title"] = "new"
    assert item.dirty_columns() == frozenset({"title"})


def test_item_unknown_column_rejects_get_and_set():
    item = Item.from_row(_row())
    with pytest.raises(KeyError):
        _ = item["nope"]
    with pytest.raises(KeyError):
        item["nope"] = "v"
    with pytest.raises(KeyError):
        Item.from_row(_row(nope="v"))


def test_item_drop_sets_flag_and_reason():
    item = Item.from_row(_row())
    item.mark_dropped("filter_rule")
    assert item.is_dropped()
    assert item.drop_reason() == "filter_rule"


def test_item_extras_and_volatile_values():
    item = Item.from_row(_row())
    item.set_extra("hn_score", 42)
    assert item.get_extra("hn_score") == 42
    assert "extras_json" in item.dirty_columns()
    item.set_volatile("rendered_text", "hi")
    assert item.get_volatile("rendered_text") == "hi"
    assert "rendered_text" not in item.dirty_columns()
