from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class StageType:
    name: str
    reads: tuple[str, ...]
    writes: tuple[str, ...]
    drops: bool
    volatile: bool


REGISTRY: dict[str, StageType] = {}


def register_stage_type(stage_type: StageType) -> None:
    if stage_type.name in REGISTRY:
        raise ValueError(f"stage type already registered: {stage_type.name!r}")
    REGISTRY[stage_type.name] = stage_type


def _bootstrap() -> None:
    stage_types = [
        StageType(
            name="fetch_rss",
            reads=(),
            writes=(
                "source_name",
                "guid",
                "url",
                "title",
                "author",
                "published_at",
                "raw_summary",
                "raw_content",
                "raw_json",
                "tag",
            ),
            drops=False,
            volatile=False,
        ),
        StageType(
            name="dedupe",
            reads=(),
            writes=(),
            drops=True,
            volatile=False,
        ),
        StageType(
            name="cross_dedupe",
            reads=("url", "tags_json", "source_name"),
            writes=(),
            drops=True,
            volatile=False,
        ),
        StageType(
            name="filter",
            reads=(),
            writes=(),
            drops=True,
            volatile=False,
        ),
        StageType(
            name="field_filter",
            reads=(),
            writes=(),
            drops=False,
            volatile=False,
        ),
        StageType(
            name="html_strip",
            reads=("raw_summary", "raw_content", "fulltext"),
            writes=("raw_summary", "raw_content", "fulltext"),
            drops=False,
            volatile=False,
        ),
        StageType(
            name="fetcher_apply",
            reads=("url",),
            writes=("fulltext",),
            drops=False,
            volatile=False,
        ),
        StageType(
            name="mirror_attachments",
            reads=("raw_json", "fulltext"),
            writes=("attachments_json",),
            drops=False,
            volatile=False,
        ),
        StageType(
            name="enrich",
            reads=(),
            writes=("extras_json",),
            drops=False,
            volatile=False,
        ),
        StageType(
            name="llm_stage",
            reads=(),
            writes=("extras_json",),
            drops=False,
            volatile=False,
        ),
        StageType(
            name="score_tag",
            reads=(),
            writes=("score", "tags_json"),
            drops=False,
            volatile=False,
        ),
        StageType(
            name="summarize_score_tag",
            reads=(),
            writes=("summary", "score", "tags_json"),
            drops=False,
            volatile=False,
        ),
        StageType(
            name="sort",
            reads=(),
            writes=(),
            drops=False,
            volatile=True,
        ),
        StageType(
            name="limit",
            reads=(),
            writes=(),
            drops=True,
            volatile=False,
        ),
        StageType(
            name="render",
            reads=(),
            writes=(),
            drops=False,
            volatile=True,
        ),
    ]
    for stage_type in stage_types:
        register_stage_type(stage_type)


_bootstrap()
