from __future__ import annotations

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


def test_source_def_basic():
    s = SourceDef(name="rss1", type="rss", params={"url": "https://x"})
    assert s.name == "rss1"
    assert s.type == "rss"
    assert s.params["url"] == "https://x"


def test_pipeline_def_validates_input_mode():
    with pytest.raises(ValueError):
        PipelineDef(
            name="p",
            cron="* * * * *",
            stages=["s"],
            sources=None,
            upstream_all=None,
            upstream_any=None,
            sinks=None,
            post_run_cleanup=None,
            input_mode="weird",
        )


def test_pipeline_def_input_mode_default():
    p = PipelineDef(
        name="p",
        cron="* * * * *",
        stages=["s"],
        sources=["src1"],
        upstream_all=None,
        upstream_any=None,
        sinks=None,
        post_run_cleanup=None,
        input_mode=None,
    )
    assert p.input_mode == "union"


def test_pipeline_def_max_attempts_default_and_custom():
    p = PipelineDef(
        name="p",
        cron="* * * * *",
        stages=["s"],
        sources=["src1"],
        upstream_all=None,
        upstream_any=None,
        sinks=None,
        post_run_cleanup=None,
    )
    assert p.max_attempts == 3

    custom = PipelineDef(
        name="p",
        cron="* * * * *",
        stages=["s"],
        sources=["src1"],
        upstream_all=None,
        upstream_any=None,
        sinks=None,
        post_run_cleanup=None,
        max_attempts=5,
    )
    assert custom.max_attempts == 5


def test_pipeline_def_cron_defaults_to_none():
    p = PipelineDef(
        name="p",
        stages=["s"],
        sources=["src1"],
        upstream_all=None,
        upstream_any=None,
        sinks=None,
        post_run_cleanup=None,
    )
    assert p.cron is None


def test_task_config_round_trip():
    t = TaskConfig(
        name="airflow",
        db_path="data/airflow.db",
        seen_db="data/airflow_seen.db",
        enabled=True,
        timezone="Asia/Shanghai",
        window=WindowDef(anchor="0 8 * * *", length="24h"),
        deployments=DeploymentsDef(pipelines="all"),
        sources=[SourceDef(name="s", type="rss", params={"url": "x"})],
        sinks=[SinkDef(name="k", type="telegram", params={})],
        stages=[StageDef(name="st", type="dedupe", params={})],
        pipelines=[
            PipelineDef(
                name="p",
                cron="0 8 * * *",
                stages=["st"],
                sources=["s"],
                upstream_all=None,
                upstream_any=None,
                sinks=None,
                post_run_cleanup=None,
                input_mode="union",
            )
        ],
    )
    assert t.name == "airflow"
    assert t.pipelines[0].input_mode == "union"


def test_task_config_rotation_cron_default():
    t = TaskConfig(
        name="x",
        db_path="a.db",
        seen_db="s.db",
        enabled=True,
        timezone=None,
        window=WindowDef(anchor="* * * * *", length="1h"),
        deployments=DeploymentsDef(pipelines="all"),
        sources=[SourceDef(name="s", type="rss")],
        sinks=[],
        stages=[],
        pipelines=[],
    )
    assert t.rotation_cron is None


def test_task_config_rotation_cron_explicit():
    t = TaskConfig(
        name="x",
        db_path="a.db",
        seen_db="s.db",
        enabled=True,
        timezone=None,
        window=WindowDef(anchor="* * * * *", length="1h"),
        deployments=DeploymentsDef(pipelines="all"),
        sources=[SourceDef(name="s", type="rss")],
        sinks=[],
        stages=[],
        pipelines=[],
        rotation_cron="0 6 * * *",
    )
    assert t.rotation_cron == "0 6 * * *"
