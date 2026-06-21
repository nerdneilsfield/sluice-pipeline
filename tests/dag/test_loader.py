from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from sluice.core.errors import ConfigError
from sluice.dag.loader import load_all_tasks, load_task

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
        (FIXTURE / "task.toml")
        .read_text()
        .replace(
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


def test_load_task_pipeline_cron_missing_defaults_to_none(tmp_path: Path):
    import shutil

    d = tmp_path / "task"
    shutil.copytree(FIXTURE, d)
    lines = [
        line
        for line in (d / "pipelines.toml").read_text().splitlines()
        if 'cron    = "0 */6 * * *"' not in line
    ]
    (d / "pipelines.toml").write_text("\n".join(lines) + "\n")
    t = load_task(d)
    assert next(p for p in t.pipelines if p.name == "fetch_blog").cron is None


def test_load_task_invalid_pipeline_concurrency_strategy_raises(tmp_path: Path):
    d = tmp_path / "task"
    shutil.copytree(FIXTURE, d)
    text = (d / "pipelines.toml").read_text()
    text = text.replace(
        'name    = "fetch_blog"',
        'name    = "fetch_blog"\nconcurrency_strategy = "bogus"',
        1,
    )
    (d / "pipelines.toml").write_text(text)

    with pytest.raises(ConfigError, match="concurrency_strategy"):
        load_task(d)


def test_load_task_invalid_toml_is_config_error(tmp_path: Path):
    d = tmp_path / "task"
    shutil.copytree(FIXTURE, d)
    (d / "sources.toml").write_text("[[sources]\nname = 'bad'\n")

    with pytest.raises(ConfigError) as excinfo:
        load_task(d)

    message = str(excinfo.value)
    assert "sources.toml" in message
    assert "parse" in message or "TOML" in message


@pytest.mark.parametrize(
    ("filename", "old", "expected"),
    [
        (
            "task.toml",
            'anchor = "0 8 * * *"\n',
            "missing 'task.window.anchor'",
        ),
        (
            "task.toml",
            "[task]\n",
            "'task' expected table",
        ),
        (
            "task.toml",
            "[task.window]\nanchor = \"0 8 * * *\"\nlength = \"24h\"\n",
            "'task.window' expected table",
        ),
        (
            "sources.toml",
            'type = "rss"\n',
            "sources[0]: missing 'type'",
        ),
        (
            "sources.toml",
            "[[sources]]\n",
            "'sources' expected list",
        ),
        (
            "sources.toml",
            "[[sources]]\n",
            "sources[0]: expected table",
        ),
        (
            "sinks.toml",
            "[[sinks]]\n",
            "'sinks' expected list",
        ),
        (
            "sinks.toml",
            "[[sinks]]\n",
            "sinks[0]: expected table",
        ),
        (
            "stages.toml",
            "[[stages]]\n",
            "'stages' expected list",
        ),
        (
            "stages.toml",
            "[[stages]]\n",
            "stages[0]: expected table",
        ),
        (
            "pipelines.toml",
            "[[pipelines]]\n",
            "'pipelines' expected list",
        ),
        (
            "pipelines.toml",
            "[[pipelines]]\n",
            "pipelines[0]: expected table",
        ),
    ],
)
def test_load_task_structural_errors_are_config_errors(
    tmp_path: Path,
    filename: str,
    old: str,
    expected: str,
):
    d = tmp_path / "task"
    shutil.copytree(FIXTURE, d)
    text = (d / filename).read_text()
    if expected == "'task' expected table":
        (d / filename).write_text('task = "bad"\n')
    elif expected == "'task.window' expected table":
        (d / filename).write_text(
            text.replace(
                old,
                'window = "24h"\n',
            )
        )
    elif expected == "'sources' expected list":
        (d / filename).write_text('sources = "bad"\n')
    elif expected == "sources[0]: expected table":
        (d / filename).write_text('sources = ["bad"]\n')
    elif expected == "'sinks' expected list":
        (d / filename).write_text('sinks = "bad"\n')
    elif expected == "sinks[0]: expected table":
        (d / filename).write_text('sinks = ["bad"]\n')
    elif expected == "'stages' expected list":
        (d / filename).write_text('stages = "bad"\n')
    elif expected == "stages[0]: expected table":
        (d / filename).write_text('stages = ["bad"]\n')
    elif expected == "'pipelines' expected list":
        (d / filename).write_text('pipelines = "bad"\n')
    elif expected == "pipelines[0]: expected table":
        (d / filename).write_text('pipelines = ["bad"]\n')
    else:
        (d / filename).write_text(text.replace(old, ""))

    with pytest.raises(ConfigError) as excinfo:
        load_task(d)

    message = str(excinfo.value)
    assert filename in message
    assert expected in message


def test_load_task_aggregates_multiple_structural_errors(tmp_path: Path):
    d = tmp_path / "task"
    shutil.copytree(FIXTURE, d)
    task_text = (d / "task.toml").read_text()
    task_text = task_text.replace('name     = "example"\n', "")
    task_text = task_text.replace('anchor = "0 8 * * *"\n', "")
    (d / "task.toml").write_text(task_text)
    (d / "sources.toml").write_text(
        """\
[[sources]]
name = "blog"
"""
    )
    (d / "pipelines.toml").write_text(
        """\
[[pipelines]]
name = "p1"
concurrency_strategy = "bogus"
input_mode = "zip"
max_attempts = "many"
"""
    )

    with pytest.raises(ConfigError) as excinfo:
        load_task(d)

    message = str(excinfo.value)
    assert "task.toml" in message
    assert "sources.toml" in message
    assert "pipelines.toml" in message
    assert "missing 'task.name'" in message
    assert "missing 'task.window.anchor'" in message
    assert "sources[0]: missing 'type'" in message
    assert "pipelines[0]: missing 'stages'" in message
    assert "invalid concurrency_strategy 'bogus'" in message
    assert "invalid input_mode 'zip'" in message
    assert "max_attempts" in message


@pytest.mark.parametrize(
    ("replacement", "expected"),
    [
        ('concurrency_strategy = "bogus"', "invalid concurrency_strategy 'bogus'"),
        ('input_mode = "zip"', "invalid input_mode 'zip'"),
        ('max_attempts = "many"', "max_attempts"),
        ("max_attempts = inf", "max_attempts"),
        ('stages = "fetch"', "pipelines[0].stages"),
    ],
)
def test_load_task_pipeline_value_errors_are_config_errors(
    tmp_path: Path,
    replacement: str,
    expected: str,
):
    d = tmp_path / "task"
    shutil.copytree(FIXTURE, d)
    text = (d / "pipelines.toml").read_text()
    if replacement.startswith("stages"):
        text = text.replace('stages  = ["fetch_blog_rss", "dd"]', replacement, 1)
    else:
        text = text.replace('name    = "fetch_blog"', f'name    = "fetch_blog"\n{replacement}', 1)
    (d / "pipelines.toml").write_text(text)

    with pytest.raises(ConfigError) as excinfo:
        load_task(d)

    message = str(excinfo.value)
    assert "pipelines.toml" in message
    assert expected in message


def test_load_task_wraps_pipeline_constructor_errors(tmp_path: Path, monkeypatch):
    d = tmp_path / "task"
    shutil.copytree(FIXTURE, d)

    def broken_pipeline(*args, **kwargs):
        raise ValueError("pipeline constructor failed")

    monkeypatch.setattr("sluice.dag.loader.PipelineDef", broken_pipeline)

    with pytest.raises(ConfigError) as excinfo:
        load_task(d)

    assert "pipeline constructor failed" in str(excinfo.value)


def test_load_task_wraps_task_config_constructor_errors(tmp_path: Path, monkeypatch):
    d = tmp_path / "task"
    shutil.copytree(FIXTURE, d)

    def broken_task_config(*args, **kwargs):
        raise TypeError("task constructor failed")

    monkeypatch.setattr("sluice.dag.loader.TaskConfig", broken_task_config)

    with pytest.raises(ConfigError) as excinfo:
        load_task(d)

    assert "task constructor failed" in str(excinfo.value)


def test_load_task_rotation_cron(tmp_path: Path):
    import shutil

    d = tmp_path / "task"
    shutil.copytree(FIXTURE, d)
    text = (d / "task.toml").read_text()
    text = text.replace("enabled  = true", 'enabled  = true\nrotation_cron = "0 6 * * *"')
    (d / "task.toml").write_text(text)
    t = load_task(d)
    assert t.rotation_cron == "0 6 * * *"


def test_load_all_tasks_discovers_all(tmp_path: Path):
    import shutil

    src = FIXTURE
    for name in ("a", "b", "c"):
        dst = tmp_path / "tasks" / name
        dst.mkdir(parents=True)
        shutil.copytree(src, dst, dirs_exist_ok=True)
        (dst / "task.toml").write_text(
            (src / "task.toml")
            .read_text()
            .replace('name     = "example"', f'name     = "{name}"')
            .replace('db_path  = "data/example.db"', f'db_path  = "data/{name}.db"')
            .replace(
                'seen_db  = "data/example_seen.db"',
                f'seen_db  = "data/{name}_seen.db"',
            )
        )
    tasks = load_all_tasks(tmp_path)
    names = {t.name for t in tasks}
    assert names == {"a", "b", "c"}


def test_load_all_tasks_empty_when_no_task_dirs(tmp_path: Path):
    (tmp_path / "tasks").mkdir()
    tasks = load_all_tasks(tmp_path)
    assert tasks == []


def test_load_all_tasks_skips_missing_toml(tmp_path: Path):
    import shutil

    src = FIXTURE
    good = tmp_path / "tasks" / "good"
    good.mkdir(parents=True)
    shutil.copytree(src, good, dirs_exist_ok=True)
    (tmp_path / "tasks" / "empty").mkdir(parents=True)
    tasks = load_all_tasks(tmp_path)
    assert len(tasks) == 1
    assert tasks[0].name == "example"
