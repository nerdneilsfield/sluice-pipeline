from datetime import datetime, timezone
from dataclasses import replace
import json
import logging
from types import MappingProxyType

import pytest

from sluice.app import open_app
from sluice.canonical import content_hash
from sluice.config import freeze_json
from sluice.contracts import KEEP_CHECKPOINT, SourceRecord, SourceResult
from sluice.errors import ExecutionError
import sluice.source_runtime as source_runtime
from sluice.source_runtime import collect_source


def source_collect(config, checkpoint):
    return SourceResult(
        records=(SourceRecord("a", {"url": "u", "title": "A"}),),
        next_checkpoint="cursor-2",
    )


def malformed_collect(config, checkpoint):
    return SourceResult(records=(SourceRecord("", {"bad": True}),), next_checkpoint=None)


def checkpoint_collect(config, checkpoint):
    if config["mode"] == "keep":
        return SourceResult((), KEEP_CHECKPOINT)
    return SourceResult((), None)


def raising_collect(config, checkpoint):
    raise RuntimeError("source unavailable")


def canonical_collect(config, checkpoint):
    return SourceResult(
        records=(SourceRecord("canonical", {"b": 2, "a": 1}),),
        next_checkpoint=None,
    )


def configurable_collect(config, checkpoint):
    return SourceResult(records=(), next_checkpoint=KEEP_CHECKPOINT)


def source_only_app(tmp_path, use, with_config="{}"):
    definition = tmp_path / "source.yml"
    definition.write_text(
        f"version: 1\ndatabase: state.db\nsources:\n  feed: {{use: {use}, with: {with_config}}}\n",
        encoding="utf-8",
    )
    return open_app(definition, tmp_path / "state.db")


def test_source_success_inserts_items_delta_and_checkpoint_atomically(tmp_path):
    definition = tmp_path / "definition.yml"
    definition.write_text(
        """
version: 1
database: state.db
sources:
  feed:
    use: tests.integration.test_source_lifecycle.source_collect
    with: {url: https://example.test/feed.xml}
""",
        encoding="utf-8",
    )
    app = open_app(definition, tmp_path / "state.db")
    execution_id = collect_source(
        app, "feed", datetime(2026, 8, 14, tzinfo=timezone.utc)
    )
    row = app.conn.execute(
        "SELECT state FROM executions WHERE id=?", (execution_id,)
    ).fetchone()
    assert row[0] == "succeeded"
    assert app.conn.execute("SELECT checkpoint_json FROM source_state").fetchone()[0] == '"cursor-2"'
    assert app.conn.execute("SELECT kind FROM publications").fetchone()[0] == "delta"


@pytest.mark.parametrize(("mode", "stored"), [("keep", None), ("null", "null")])
def test_checkpoint_sentinel_and_explicit_json_null_are_distinct(tmp_path, mode, stored):
    app = source_only_app(
        tmp_path,
        "tests.integration.test_source_lifecycle.checkpoint_collect",
        f"{{mode: {mode}}}",
    )
    collect_source(app, "feed", datetime(2026, 8, 14, tzinfo=timezone.utc))
    assert app.conn.execute("SELECT checkpoint_json FROM source_state").fetchone()[0] == stored


def test_raised_source_error_rolls_back_outputs_and_records_failed_attempt(tmp_path):
    app = source_only_app(
        tmp_path, "tests.integration.test_source_lifecycle.raising_collect"
    )
    with pytest.raises(ExecutionError):
        collect_source(app, "feed", datetime(2026, 8, 14, tzinfo=timezone.utc))
    assert app.conn.execute("SELECT COUNT(*) FROM items").fetchone()[0] == 0
    assert app.conn.execute("SELECT COUNT(*) FROM publications").fetchone()[0] == 0
    assert (
        app.conn.execute(
            "SELECT state FROM executions ORDER BY id DESC LIMIT 1"
        ).fetchone()[0]
        == "failed"
    )


def test_identical_source_replay_reuses_item_identity(tmp_path):
    app = source_only_app(
        tmp_path, "tests.integration.test_source_lifecycle.source_collect"
    )
    collect_source(app, "feed", datetime(2026, 8, 14, tzinfo=timezone.utc))
    collect_source(
        app, "feed", datetime(2026, 8, 14, 0, 1, tzinfo=timezone.utc)
    )
    assert app.conn.execute("SELECT COUNT(*) FROM items").fetchone()[0] == 1
    assert app.conn.execute("SELECT COUNT(*) FROM publication_members").fetchone()[0] == 1
    assert (
        app.conn.execute(
            "SELECT COUNT(*) FROM executions WHERE state='succeeded'"
        ).fetchone()[0]
        == 2
    )


class FakeHttp:
    def __init__(self, responses):
        self.responses = responses

    def get(self, url):
        return self.responses[url]


RSS_XML = """<rss version='2.0'><channel><item><guid>a</guid><title>A</title><link>https://example.test/a</link></item></channel></rss>"""
HTML_PAGE = "<html><body><article><h1>A</h1><p>Body</p></article></body></html>"


@pytest.mark.parametrize(
    ("source_use", "url"),
    [
        ("sluice.sources.rss", "https://example.test/feed"),
        ("sluice.sources.webpage", "https://example.test/page"),
    ],
)
def test_builtin_sources_finalize_ordered_records_through_the_runtime(
    tmp_path, source_use, url
):
    definition = tmp_path / "builtin.yml"
    definition.write_text(
        f"version: 1\ndatabase: state.db\nsources:\n  feed: {{use: {source_use}, with: {{url: {url}}}}}\n",
        encoding="utf-8",
    )
    app = open_app(
        definition,
        tmp_path / "state.db",
        http_client=FakeHttp(
            {"https://example.test/feed": RSS_XML, "https://example.test/page": HTML_PAGE}
        ),
    )
    execution_id = collect_source(
        app, "feed", datetime(2026, 8, 14, tzinfo=timezone.utc)
    )
    assert (
        app.conn.execute(
            "SELECT state FROM executions WHERE id=?", (execution_id,)
        ).fetchone()[0]
        == "succeeded"
    )
    assert app.conn.execute("SELECT COUNT(*) FROM items").fetchone()[0] >= 1
    assert (
        app.conn.execute(
            "SELECT kind FROM publications ORDER BY id DESC LIMIT 1"
        ).fetchone()[0]
        == "delta"
    )


def test_malformed_source_result_publishes_nothing_and_keeps_checkpoint(tmp_path):
    definition = tmp_path / "definition.yml"
    definition.write_text(
        """
version: 1
database: state.db
sources:
  feed: {use: tests.integration.test_source_lifecycle.malformed_collect, with: {}}
""",
        encoding="utf-8",
    )
    app = open_app(definition, tmp_path / "state.db")
    with pytest.raises(ExecutionError):
        collect_source(app, "feed", datetime(2026, 8, 14, tzinfo=timezone.utc))
    assert app.conn.execute("SELECT COUNT(*) FROM items").fetchone()[0] == 0
    assert app.conn.execute("SELECT COUNT(*) FROM publications").fetchone()[0] == 0
    assert app.conn.execute("SELECT checkpoint_json FROM source_state").fetchone()[0] is None


def test_source_item_persists_canonical_content_hash_and_utc_observed_at(tmp_path):
    app = source_only_app(
        tmp_path,
        "tests.integration.test_source_lifecycle.canonical_collect",
    )

    collect_source(
        app,
        "feed",
        datetime(2026, 8, 14, 12, 30, tzinfo=timezone.utc),
    )
    row = app.conn.execute(
        "SELECT payload_json,content_hash,observed_at FROM items"
    ).fetchone()

    assert row[0] == '{"a":1,"b":2}'
    assert row[1] == content_hash({"a": 1, "b": 2})
    assert row[2] == "2026-08-14T12:30:00Z"


def test_source_supersession_log_uses_superseded_binding_digest(tmp_path, caplog):
    app = source_only_app(
        tmp_path,
        "tests.integration.test_source_lifecycle.configurable_collect",
        "{mode: old}",
    )
    old_source = app.definition.sources["feed"]
    old_binding = source_runtime._binding_digest("feed", old_source)
    old_execution_id, _, _, _ = source_runtime._activate_binding(
        app,
        "feed",
        old_source,
        old_binding,
        "2026-08-14T12:00:00Z",
        "manual",
    )

    new_source = replace(old_source, with_config=freeze_json({"mode": "new"}))
    new_definition = replace(
        app.definition,
        sources=MappingProxyType({"feed": new_source}),
    )
    object.__setattr__(app, "definition", new_definition)

    caplog.set_level(logging.INFO, logger="sluice.source_runtime")
    collect_source(app, "feed", datetime(2026, 8, 14, 12, 1, tzinfo=timezone.utc))

    events = [
        json.loads(record.message)
        for record in caplog.records
        if json.loads(record.message).get("event") == "source_supersession"
    ]
    assert events
    assert events[-1]["execution_id"] == old_execution_id
    assert events[-1]["binding_digest"] == old_binding
