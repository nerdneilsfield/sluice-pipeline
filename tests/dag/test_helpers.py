from __future__ import annotations

import pytest

from sluice.dag.item import Item


def _row(**overrides):
    base = {
        "item_id": "i",
        "source_name": "feed",
        "guid": "g",
        "url": "https://example.test/i",
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
    base.update(overrides)
    return base


def test_safe_name_accepts_identifier():
    from sluice.dag._name import safe_name

    assert safe_name("a_1") == "a_1"


@pytest.mark.parametrize("value", ["a;b", "a-b", "", "space name"])
def test_safe_name_rejects_unsafe_identifier(value):
    from sluice.dag._name import safe_name

    with pytest.raises(ValueError, match="unsafe identifier"):
        safe_name(value)


def test_source_hash_from_row_matches_seen_post_existing_formula():
    from sluice.dag._source_hash import source_hash_from_row
    from sluice.dag.phases.seen_post import _source_hash

    item = Item.from_row(_row(source_name="feed", guid="guid-1", url="https://x/u"))
    assert source_hash_from_row(item) == _source_hash(item)


def test_source_hash_from_row_falls_back_to_url_then_empty_string():
    from sluice.dag._source_hash import source_hash_from_row

    with_url = source_hash_from_row(_row(guid=None, url="https://x/u"))
    same_url = source_hash_from_row(_row(guid=None, url="https://x/u"))
    no_identity = source_hash_from_row(_row(guid=None, url=None))

    assert with_url == same_url
    assert with_url != no_identity
