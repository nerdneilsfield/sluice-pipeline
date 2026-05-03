from __future__ import annotations

import pytest

from sluice.dag.registry import REGISTRY, StageType, register_stage_type


def test_all_15_types_registered():
    expected = {
        "fetch_rss",
        "dedupe",
        "cross_dedupe",
        "filter",
        "field_filter",
        "html_strip",
        "fetcher_apply",
        "mirror_attachments",
        "enrich",
        "llm_stage",
        "score_tag",
        "summarize_score_tag",
        "sort",
        "limit",
        "render",
    }
    assert set(REGISTRY) == expected


def test_volatile_classification():
    assert REGISTRY["sort"].volatile is True
    assert REGISTRY["render"].volatile is True
    assert REGISTRY["filter"].volatile is False
    assert REGISTRY["summarize_score_tag"].volatile is False


def test_drop_semantic():
    assert REGISTRY["filter"].drops is True
    assert REGISTRY["limit"].drops is True
    assert REGISTRY["dedupe"].drops is True
    assert REGISTRY["cross_dedupe"].drops is True
    assert REGISTRY["fetcher_apply"].drops is False


def test_writes_columns_are_subset_of_known_items_columns():
    known = {
        "source_name",
        "guid",
        "url",
        "title",
        "author",
        "published_at",
        "tag",
        "raw_summary",
        "raw_content",
        "raw_json",
        "fulltext",
        "attachments_json",
        "summary",
        "score",
        "tags_json",
        "extras_json",
    }
    for name, st in REGISTRY.items():
        for col in st.writes:
            assert col in known, f"stage {name!r} writes unknown column {col!r}"


def test_register_duplicate_raises():
    with pytest.raises(ValueError):
        register_stage_type(
            StageType(
                name="dedupe",
                reads=(),
                writes=(),
                drops=False,
                volatile=False,
            )
        )


def test_fetch_rss_writes_identity_columns():
    st = REGISTRY["fetch_rss"]
    assert "url" in st.writes
    assert "title" in st.writes
    assert "raw_json" in st.writes


def test_summarize_score_tag_writes():
    st = REGISTRY["summarize_score_tag"]
    assert set(st.writes) >= {"summary", "score", "tags_json"}
