from datetime import timedelta

import pytest
import yaml

from sluice.app import open_app
from sluice.config import load_definition
from sluice.contracts import Definition
from sluice.errors import DefinitionError, GraphError


def sink_callable(config, payload, delivery_key):
    return None


@pytest.mark.parametrize(
    "fragment",
    [
        {
            "pipelines": {
                "p": {
                    "nodes": {
                        "a": {"stage": "missing", "from": "source.x"},
                        "out": {
                            "from": "a",
                            "deliver": {
                                "use": "tests.unit.test_collection_and_graph.sink_callable",
                                "with": {},
                            },
                        },
                    }
                }
            }
        },
        {"pipelines": {"p": {"nodes": {"a": {"from": "b"}, "b": {"from": "a"}}}}},
        {
            "stages": {"s": {"input": "each", "tasks": [{"use": "sluice.tasks.dedup"}]}},
            "pipelines": {
                "p": {
                    "nodes": {
                        "a": {"stage": "s", "from": "source.x", "batch": {"count": 2}},
                        "out": {
                            "from": "a",
                            "deliver": {
                                "use": "tests.unit.test_collection_and_graph.sink_callable",
                                "with": {},
                            },
                        },
                    }
                }
            },
        },
        {
            "sources": {
                "x": {
                    "use": "tests.unit.test_collection_and_graph.source_callable",
                    "with": {},
                }
            },
            "stages": {
                "s": {
                    "input": "each",
                    "tasks": [
                        {
                            "use": "sluice.tasks.to_markdown",
                            "with": {"invalid": "drop"},
                        }
                    ],
                }
            },
            "pipelines": {
                "p": {
                    "nodes": {
                        "a": {"stage": "s", "from": "source.x"},
                        "out": {
                            "from": "a",
                            "deliver": {
                                "use": "tests.unit.test_collection_and_graph.sink_callable",
                                "with": {},
                            },
                        },
                    }
                }
            },
        },
    ],
)
def test_invalid_graph_or_stage_policy_is_rejected(tmp_path, fragment):
    path = tmp_path / "bad.yml"
    path.write_text("version: 1\n" + yaml.safe_dump(fragment), encoding="utf-8")
    with pytest.raises((DefinitionError, GraphError)):
        load_definition(path)


def test_loader_is_safe_and_preserves_yaml_with_mapping(tmp_path):
    path = tmp_path / "definition.yml"
    path.write_text(
        """
version: 1
database: db.sqlite
sources:
  s:
    use: sluice.sources.rss
    with: {url: https://example.test/feed.xml}
stages:
  pass:
    input: each
    tasks: [{use: sluice.tasks.to_markdown}]
pipelines:
  p:
    nodes:
      out: {stage: pass, from: source.s, timeout: 120s}
      email: {from: out, deliver: {use: tests.unit.test_collection_and_graph.sink_callable, with: {target: a@example.test}}}
""",
        encoding="utf-8",
    )
    definition = load_definition(path)
    assert definition.sources["s"].with_config["url"] == "https://example.test/feed.xml"
    assert definition.pipelines["p"].nodes["out"].from_refs == ("source.s",)
    assert definition.pipelines["p"].nodes["out"].policy.timeout == timedelta(seconds=120)


@pytest.mark.parametrize("version", ["true", "1.0"])
def test_definition_version_requires_exact_integer_one(tmp_path, version):
    path = tmp_path / "bad-version.yml"
    path.write_text(f"version: {version}\n", encoding="utf-8")

    with pytest.raises(DefinitionError):
        load_definition(path)


def test_loader_rejects_recursive_yaml_aliases(tmp_path):
    path = tmp_path / "recursive.yml"
    path.write_text(
        """
version: 1
sources: &sources
  feed:
    use: sluice.sources.rss
    with:
      recursive: *sources
""",
        encoding="utf-8",
    )

    with pytest.raises(DefinitionError):
        load_definition(path)


def test_open_app_requires_a_definition_path(tmp_path):
    definition = Definition(version=1, database=str(tmp_path / "state.db"), sources={}, stages={}, pipelines={})

    with pytest.raises(DefinitionError):
        open_app(definition, tmp_path / "state.db")
