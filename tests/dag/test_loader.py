from __future__ import annotations

from pathlib import Path

import pytest

from sluice.dag.loader import load_task

FIXTURE = Path(__file__).parent / "fixtures" / "example_task"


def test_load_task_basic():
    t = load_task(FIXTURE)
    assert t.name == "example"
    assert t.db_path == "data/example.db"
    assert t.window.anchor == "0 8 * * *"
    assert t.window.length == "24h"
    assert t.deployments.pipelines == "all"
    assert len(t.sources) == 2
    assert {s.name for s in t.sources} == {"blog", "releases"}
    assert len(t.sinks) == 1 and t.sinks[0].type == "telegram"
    assert len(t.stages) == 7
    assert len(t.pipelines) == 4
    p = next(p for p in t.pipelines if p.name == "summarize_nightly")
    assert p.upstream_all == ["fetch_blog", "fetch_releases"]
    assert p.input_mode == "union"


def test_load_task_missing_directory(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        load_task(tmp_path / "nope")


def test_load_task_missing_required_file(tmp_path: Path):
    d = tmp_path / "incomplete"
    d.mkdir()
    (d / "task.toml").write_text(
        "[task]\nname='x'\ndb_path='a.db'\nseen_db='b.db'\nenabled=true\n"
        "[task.window]\nanchor='0 8 * * *'\nlength='24h'\n"
        "[task.deployments]\npipelines='all'\n"
    )
    with pytest.raises(FileNotFoundError, match="sources.toml"):
        load_task(d)


def test_load_task_explicit_deployments_list(tmp_path: Path):
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
    assert t.deployments.pipelines == ["fetch_blog", "summarize_nightly"]


def test_load_task_pipeline_max_attempts(tmp_path: Path):
    import shutil

    d = tmp_path / "task"
    shutil.copytree(FIXTURE, d)
    text = (d / "pipelines.toml").read_text()
    text = text.replace(
        'name    = "fetch_blog"',
        'name    = "fetch_blog"\nmax_attempts = 7',
        1,
    )
    (d / "pipelines.toml").write_text(text)
    t = load_task(d)
    assert next(p for p in t.pipelines if p.name == "fetch_blog").max_attempts == 7
    assert next(p for p in t.pipelines if p.name == "fetch_releases").max_attempts == 3


def test_load_task_rotation_cron(tmp_path: Path):
    import shutil

    d = tmp_path / "task"
    shutil.copytree(FIXTURE, d)
    text = (d / "task.toml").read_text()
    text = text.replace('enabled  = true', 'enabled  = true\nrotation_cron = "0 6 * * *"')
    (d / "task.toml").write_text(text)
    t = load_task(d)
    assert t.rotation_cron == "0 6 * * *"
