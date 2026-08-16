from datetime import datetime, timedelta, timezone
from dataclasses import replace

import pytest
import yaml

from sluice.app import open_app
from sluice.config import load_definition
from sluice.contracts import BatchInput, Definition, DROP, FORWARD, Item
from sluice.contracts import BatchPolicy
from sluice.errors import DefinitionError, ExecutionError, GraphError
from sluice.tasks import collection
from sluice.reconcile import freeze_eligible


def sink_callable(config, payload, delivery_key):
    return None


def item(item_id, payload, metadata=None):
    return Item(
        item_id,
        ("source", "feed"),
        item_id,
        payload,
        metadata or {},
        item_id,
        "2026-08-14T00:00:00Z",
        1,
    )


def test_item_field_prefix_cannot_be_shadowed_by_payload():
    value = item("i", {"observed_at": "payload", "$item": {"observed_at": "shadow"}})
    assert collection.field_value(value, "observed_at") == "payload"
    assert collection.field_value(value, "$item.observed_at") == "2026-08-14T00:00:00Z"
    assert collection.field_value(value, "$item.content_hash") == "i"


def test_dedup_change_and_metadata_boundaries():
    prior = item("old", {"url": "u", "content": "abcd"})
    changed = item("new", {"url": "u", "content": "abce"})
    assert collection.dedup(
        {"keys": ["url"], "change": {"field": "content", "min_rate": 0.25}},
        BatchInput((changed,), (prior,)),
    ) == (FORWARD(changed),)
    assert collection.dedup(
        {"keys": ["url"], "change": {"field": "content", "min_rate": 0.26}},
        BatchInput((changed,), (prior,)),
    ) == (DROP,)
    metadata_changed = item("meta", prior.payload, {"tag": "new"})
    assert collection.dedup(
        {"keys": ["url"]}, BatchInput((metadata_changed,), (prior,))
    ) == (FORWARD(metadata_changed),)


def test_filter_sort_and_limit_boundaries():
    low = item("b", {})
    explicit_null = item("n", {"score": None})
    high = item("a", {"score": 2})
    assert collection.filter(
        {"all": [{"field": "score", "op": "exists"}]},
        BatchInput((low, high), ()),
    ) == (DROP, FORWARD(high))
    assert collection.filter(
        {"all": [{"field": "score", "op": "exists"}]},
        BatchInput((explicit_null,), ()),
    ) == (FORWARD(explicit_null),)
    with pytest.raises(ExecutionError):
        collection.filter(
            {"all": [{"field": "score", "op": "gte", "value": 1}]},
            BatchInput((item("bad", {"score": "2"}),), ()),
        )
    ordered = collection.sort(
        {"by": [{"field": "score", "order": "asc", "nulls": "last"}]},
        BatchInput((low, high), ()),
    )
    assert ordered == (FORWARD(high), FORWARD(low))
    assert collection.limit(
        {"count": 1}, BatchInput((high, low), ())
    ) == (FORWARD(high),)


def test_dedup_uses_only_explicit_eligible_history_in_identity_order():
    candidate = Item(
        "new",
        ("source", "feed"),
        "n",
        {"url": "u", "content": "same"},
        {},
        "h-same",
        "2026-08-14T00:00:00Z",
        2,
    )
    prior = Item(
        "old",
        ("stage", "select"),
        "n",
        {"url": "u", "content": "same"},
        {},
        "h-same",
        "2026-08-13T00:00:00Z",
        1,
    )
    assert collection.dedup(
        {"keys": ["url"]}, BatchInput((candidate,), ())
    ) == (FORWARD(candidate),)
    assert collection.dedup(
        {"keys": ["url"]}, BatchInput((candidate,), (prior,))
    ) == (DROP,)
    assert collection.select_dedup_history((candidate,), ((prior, ("new",)),)) == ()
    assert collection.select_dedup_history(
        (candidate,), ((prior, ("other-input",)),)
    ) == (prior,)


def test_select_dedup_history_uses_configured_keys_and_strict_prior_order():
    candidate = item("candidate", {"url": "u", "title": "new"})
    same_key = item("prior", {"url": "u", "title": "old"})
    unrelated_key = item("other", {"url": "other", "title": "old"})
    future = item("future", {"url": "u", "title": "future"})
    # Keep the candidate strictly later than the prior and strictly earlier
    # than the future by replacing their immutable observed timestamps.
    candidate = replace(candidate, observed_at="2026-08-14T00:00:00Z")
    same_key = replace(same_key, observed_at="2026-08-13T00:00:00Z")
    unrelated_key = replace(unrelated_key, observed_at="2026-08-12T00:00:00Z")
    future = replace(future, observed_at="2026-08-15T00:00:00Z")

    accepted = (
        (same_key, ("old-input",)),
        (unrelated_key, ("other-input",)),
        (future, ("future-input",)),
    )
    assert collection.select_dedup_history(
        (candidate,), accepted, keys=("url",)
    ) == (same_key,)


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


def instant(seconds: int) -> datetime:
    return datetime(2026, 8, 14, tzinfo=timezone.utc) + timedelta(seconds=seconds)


@pytest.mark.parametrize(
    ("policy", "count", "first", "now", "due", "expected"),
    [
        (BatchPolicy(count=2), 1, instant(0), instant(1), False, False),
        (BatchPolicy(count=2), 2, instant(0), instant(1), False, True),
        (BatchPolicy(max_wait=timedelta(hours=1)), 1, instant(0), instant(3600), False, True),
        (BatchPolicy(on_schedule=True), 99, instant(0), instant(1), False, False),
        (BatchPolicy(on_schedule=True), 99, instant(0), instant(1), True, True),
    ],
)
def test_freeze_eligibility_is_pure(policy, count, first, now, due, expected):
    assert freeze_eligible(policy, count, first, now, due) is expected
