from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from sluice.dag.config import (
    DeploymentsDef,
    PipelineDef,
    SinkDef,
    SourceDef,
    StageDef,
    TaskConfig,
    WindowDef,
)
from sluice.dag.loader import load_task
from sluice.dag.validator import ValidationError, compute_reach, validate_task

FIXTURE = Path(__file__).parent / "fixtures" / "example_task"


def _empty_task(**overrides) -> TaskConfig:
    base = dict(
        name="t",
        db_path="a.db",
        seen_db="b.db",
        enabled=True,
        timezone=None,
        window=WindowDef(anchor="0 8 * * *", length="24h"),
        deployments=DeploymentsDef(pipelines="all"),
        sources=[SourceDef(name="s", type="rss")],
        sinks=[],
        stages=[
            StageDef(name="fr", type="fetch_rss", params={"sources": ["s"]}),
            StageDef(name="dd", type="dedupe"),
            StageDef(name="sm", type="summarize_score_tag"),
            StageDef(name="srt", type="sort"),
            StageDef(name="rdr", type="render"),
        ],
        pipelines=[],
    )
    base.update(overrides)
    return TaskConfig(**base)


def test_fixture_validates_clean():
    t = load_task(FIXTURE)
    validate_task(t)


@pytest.mark.parametrize(
    ("field", "value", "match"),
    [
        ("task", "bad/task", "task name"),
        ("source", "rss-feed", "source name"),
        ("sink", "file-md", "sink name"),
        ("stage", "fetch.rss", "stage name"),
        ("pipeline", "daily-report", "pipeline name"),
    ],
)
def test_names_must_be_safe_sql_identifiers(field: str, value: str, match: str):
    t = _empty_task(
        sinks=[SinkDef(name="k", type="telegram")],
        pipelines=[
            PipelineDef(
                name="p",
                cron="* * * * *",
                stages=["fr"],
                sources=["s"],
                upstream_all=None,
                upstream_any=None,
                sinks=["k"],
                post_run_cleanup=None,
                input_mode="union",
            )
        ],
    )
    if field == "task":
        t = _empty_task(
            name=value,
            sinks=t.sinks,
            pipelines=t.pipelines,
        )
    elif field == "source":
        t = replace(
            t,
            sources=[SourceDef(name=value, type="rss")],
            stages=[
                replace(
                    t.stages[0],
                    params={"sources": [value]},
                ),
                *t.stages[1:],
            ],
            pipelines=[replace(t.pipelines[0], sources=[value])],
        )
    elif field == "sink":
        t = replace(
            t,
            sinks=[SinkDef(name=value, type="telegram")],
            pipelines=[replace(t.pipelines[0], sinks=[value])],
        )
    elif field == "stage":
        t = replace(
            t,
            stages=[replace(t.stages[0], name=value), *t.stages[1:]],
            pipelines=[replace(t.pipelines[0], stages=[value])],
        )
    elif field == "pipeline":
        t = replace(t, pipelines=[replace(t.pipelines[0], name=value)])

    with pytest.raises(ValidationError, match=match):
        validate_task(t)


def test_pipeline_with_both_sources_and_upstream_rejected():
    t = _empty_task(
        pipelines=[
            PipelineDef(
                name="p",
                cron="* * * * *",
                stages=["fr"],
                sources=["s"],
                upstream_all=["other"],
                upstream_any=None,
                sinks=None,
                post_run_cleanup=None,
                input_mode="union",
            ),
        ]
    )
    with pytest.raises(ValidationError, match="exactly one"):
        validate_task(t)


def test_pipeline_with_neither_rejected():
    t = _empty_task(
        pipelines=[
            PipelineDef(
                name="p",
                cron="* * * * *",
                stages=["fr"],
                sources=None,
                upstream_all=None,
                upstream_any=None,
                sinks=None,
                post_run_cleanup=None,
                input_mode="union",
            ),
        ]
    )
    with pytest.raises(ValidationError, match="exactly one"):
        validate_task(t)


def test_fetch_rss_in_upstream_pipeline_rejected():
    t = _empty_task(
        pipelines=[
            PipelineDef(
                name="entry",
                cron="* * * * *",
                stages=["fr"],
                sources=["s"],
                upstream_all=None,
                upstream_any=None,
                sinks=None,
                post_run_cleanup=None,
                input_mode="union",
            ),
            PipelineDef(
                name="bad",
                cron="* * * * *",
                stages=["fr", "dd"],
                sources=None,
                upstream_all=["entry"],
                upstream_any=None,
                sinks=None,
                post_run_cleanup=None,
                input_mode="union",
            ),
        ]
    )
    with pytest.raises(ValidationError, match="fetch_"):
        validate_task(t)


def test_entry_pipeline_must_contain_fetch():
    t = _empty_task(
        pipelines=[
            PipelineDef(
                name="p",
                cron="* * * * *",
                stages=["dd"],
                sources=["s"],
                upstream_all=None,
                upstream_any=None,
                sinks=None,
                post_run_cleanup=None,
                input_mode="union",
            ),
        ]
    )
    with pytest.raises(ValidationError, match="fetch_"):
        validate_task(t)


def test_last_stage_volatile_without_sinks_rejected():
    t = _empty_task(
        pipelines=[
            PipelineDef(
                name="entry",
                cron="* * * * *",
                stages=["fr"],
                sources=["s"],
                upstream_all=None,
                upstream_any=None,
                sinks=None,
                post_run_cleanup=None,
                input_mode="union",
            ),
            PipelineDef(
                name="bad",
                cron="* * * * *",
                stages=["sm", "srt"],
                sources=None,
                upstream_all=["entry"],
                upstream_any=None,
                sinks=None,
                post_run_cleanup=None,
                input_mode="union",
            ),
        ]
    )
    with pytest.raises(ValidationError, match="last stage"):
        validate_task(t)


def test_last_stage_volatile_with_sinks_ok():
    t = _empty_task(
        sinks=[SinkDef(name="k", type="telegram")],
        pipelines=[
            PipelineDef(
                name="entry",
                cron="* * * * *",
                stages=["fr"],
                sources=["s"],
                upstream_all=None,
                upstream_any=None,
                sinks=None,
                post_run_cleanup=None,
                input_mode="union",
            ),
            PipelineDef(
                name="rep",
                cron="* * * * *",
                stages=["sm", "srt", "rdr"],
                sources=None,
                upstream_all=["entry"],
                upstream_any=None,
                sinks=["k"],
                post_run_cleanup=None,
                input_mode="union",
            ),
        ],
    )
    validate_task(t)


def test_unknown_source_reference():
    t = _empty_task(
        pipelines=[
            PipelineDef(
                name="p",
                cron="* * * * *",
                stages=["fr"],
                sources=["nope"],
                upstream_all=None,
                upstream_any=None,
                sinks=None,
                post_run_cleanup=None,
                input_mode="union",
            ),
        ]
    )
    with pytest.raises(ValidationError, match="source"):
        validate_task(t)


def test_unknown_stage_type_rejected():
    t = _empty_task(stages=[StageDef(name="x", type="not_a_real_type")])
    with pytest.raises(ValidationError, match="unknown stage type"):
        validate_task(t)


def test_dag_cycle_rejected():
    t = _empty_task(
        pipelines=[
            PipelineDef(
                name="entry",
                cron="* * * * *",
                stages=["fr"],
                sources=["s"],
                upstream_all=None,
                upstream_any=None,
                sinks=None,
                post_run_cleanup=None,
                input_mode="union",
            ),
            PipelineDef(
                name="a",
                cron="* * * * *",
                stages=["sm"],
                sources=None,
                upstream_all=["entry", "b"],
                upstream_any=None,
                sinks=None,
                post_run_cleanup=None,
                input_mode="union",
            ),
            PipelineDef(
                name="b",
                cron="* * * * *",
                stages=["sm"],
                sources=None,
                upstream_all=["a"],
                upstream_any=None,
                sinks=None,
                post_run_cleanup=None,
                input_mode="union",
            ),
        ]
    )
    with pytest.raises(ValidationError, match="cycle"):
        validate_task(t)


def test_reach_from_fixture():
    t = load_task(FIXTURE)
    r = compute_reach(t, "fetch_blog")
    assert r == {"fetch_blog", "summarize_nightly", "report_telegram"}
    r2 = compute_reach(t, "fetch_releases")
    assert r2 == {"fetch_releases", "summarize_nightly", "report_telegram"}


def test_reach_excludes_disabled(tmp_path: Path):
    import shutil

    d = tmp_path / "task"
    shutil.copytree(FIXTURE, d)
    (d / "task.toml").write_text(
        (FIXTURE / "task.toml").read_text().replace(
            'pipelines = "all"',
            'pipelines = ["fetch_blog", "summarize_nightly"]',
        )
    )
    t = load_task(d)
    r = compute_reach(t, "fetch_blog")
    assert "report_telegram" not in r
    assert r == {"fetch_blog", "summarize_nightly"}
