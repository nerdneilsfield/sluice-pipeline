import json
import logging
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from threading import Event, Thread
from types import MappingProxyType

import pytest

from sluice import source_runtime
from sluice.app import open_app
from sluice.canonical import content_hash
from sluice.config import freeze_json
from sluice.contracts import KEEP_CHECKPOINT, SourceRecord, SourceResult
from sluice.errors import ExecutionError
from sluice.source_runtime import collect_source
from sluice.web.contracts import DocumentArtifact


def source_collect(config, checkpoint):
    return SourceResult(
        records=(SourceRecord("a", {"url": "u", "title": "A"}),),
        next_checkpoint="cursor-2",
    )


def malformed_collect(config, checkpoint):
    return SourceResult(
        records=(SourceRecord("", {"bad": True}),), next_checkpoint=None
    )


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
    execution_id = collect_source(app, "feed", datetime(2026, 8, 14, tzinfo=UTC))
    row = app.conn.execute(
        "SELECT state FROM executions WHERE id=?", (execution_id,)
    ).fetchone()
    assert row[0] == "succeeded"
    assert (
        app.conn.execute("SELECT checkpoint_json FROM source_state").fetchone()[0]
        == '"cursor-2"'
    )
    assert app.conn.execute("SELECT kind FROM publications").fetchone()[0] == "delta"


@pytest.mark.parametrize(("mode", "stored"), [("keep", None), ("null", "null")])
def test_checkpoint_sentinel_and_explicit_json_null_are_distinct(
    tmp_path, mode, stored
):
    app = source_only_app(
        tmp_path,
        "tests.integration.test_source_lifecycle.checkpoint_collect",
        f"{{mode: {mode}}}",
    )
    collect_source(app, "feed", datetime(2026, 8, 14, tzinfo=UTC))
    assert (
        app.conn.execute("SELECT checkpoint_json FROM source_state").fetchone()[0]
        == stored
    )


def test_raised_source_error_rolls_back_outputs_and_records_failed_attempt(tmp_path):
    app = source_only_app(
        tmp_path, "tests.integration.test_source_lifecycle.raising_collect"
    )
    with pytest.raises(ExecutionError):
        collect_source(app, "feed", datetime(2026, 8, 14, tzinfo=UTC))
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
    collect_source(app, "feed", datetime(2026, 8, 14, tzinfo=UTC))
    collect_source(app, "feed", datetime(2026, 8, 14, 0, 1, tzinfo=UTC))
    assert app.conn.execute("SELECT COUNT(*) FROM items").fetchone()[0] == 1
    assert (
        app.conn.execute("SELECT COUNT(*) FROM publication_members").fetchone()[0] == 1
    )
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


class SourceWebClient:
    def __init__(self, responses):
        self.responses = responses

    def resolve(self, request, fetch_chain, extract_chain):
        del fetch_chain, extract_chain
        body = self.responses[request.url]
        if request.mode == "raw_source":
            return DocumentArtifact(
                source_url=request.url,
                final_url=request.url,
                content_type="application/rss+xml",
                raw_source=body.encode(),
                capabilities=frozenset({"raw_source"}),
                provider_instance="__rss_direct__",
                provider_type="startup_http",
            )
        return DocumentArtifact(
            source_url=request.url,
            final_url=request.url,
            content_type="text/html",
            raw_html=body,
            text="A Body with enough visible content for quality.",
            selected_format="text",
            capabilities=frozenset({"raw_html", "text"}),
            provider_instance="trusted",
            provider_type="startup_http",
            extraction="identity",
        )


RSS_XML = """<rss version='2.0'><channel><item><guid>a</guid><title>A</title><link>https://example.test/a</link></item></channel></rss>"""
HTML_PAGE = "<html><body><article><h1>A</h1><p>Body</p></article></body></html>"


@pytest.mark.parametrize(
    ("source_use", "url", "with_config"),
    [
        (
            "sluice.sources.rss",
            "https://example.test/feed",
            "{url: https://example.test/feed}",
        ),
        (
            "sluice.sources.webpage",
            "https://example.test/page",
            "{url: https://example.test/page, fetch_chain: trusted, extract_chain: identity, output: text, min_chars: 1}",
        ),
    ],
)
def test_builtin_sources_finalize_ordered_records_through_the_runtime(
    tmp_path, source_use, url, with_config
):
    definition = tmp_path / "builtin.yml"
    definition.write_text(
        f"""version: 1
database: state.db
web:
  providers: {{trusted: {{type: startup_http, trusted: true}}}}
  fetch_chains: {{trusted: {{providers: [trusted]}}}}
  extractors: {{identity: {{type: identity}}}}
  extract_chains: {{identity: {{extractors: [identity]}}}}
sources:
  feed: {{use: {source_use}, with: {with_config}}}
""",
        encoding="utf-8",
    )
    app = open_app(
        definition,
        tmp_path / "state.db",
        http_client=FakeHttp(
            {
                "https://example.test/feed": RSS_XML,
                "https://example.test/page": HTML_PAGE,
            }
        ),
        web_content_client=SourceWebClient(
            {
                "https://example.test/feed": RSS_XML,
                "https://example.test/page": HTML_PAGE,
            }
        ),
    )
    execution_id = collect_source(app, "feed", datetime(2026, 8, 14, tzinfo=UTC))
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
        collect_source(app, "feed", datetime(2026, 8, 14, tzinfo=UTC))
    assert app.conn.execute("SELECT COUNT(*) FROM items").fetchone()[0] == 0
    assert app.conn.execute("SELECT COUNT(*) FROM publications").fetchone()[0] == 0
    assert (
        app.conn.execute("SELECT checkpoint_json FROM source_state").fetchone()[0]
        is None
    )


def test_source_item_persists_canonical_content_hash_and_utc_observed_at(tmp_path):
    app = source_only_app(
        tmp_path,
        "tests.integration.test_source_lifecycle.canonical_collect",
    )

    collect_source(
        app,
        "feed",
        datetime(2026, 8, 14, 12, 30, tzinfo=UTC),
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
    collect_source(app, "feed", datetime(2026, 8, 14, 12, 1, tzinfo=UTC))

    events = [
        json.loads(record.message)
        for record in caplog.records
        if json.loads(record.message).get("event") == "source_supersession"
    ]
    assert events
    assert events[-1]["execution_id"] == old_execution_id
    assert events[-1]["binding_digest"] == old_binding


def source_recovery_instant(seconds: int) -> datetime:
    return datetime(2026, 8, 14, tzinfo=UTC) + timedelta(seconds=seconds)


class BlockingSource:
    def __init__(self):
        self.started = Event()
        self.release = Event()

    def collect(self, config, checkpoint):
        self.started.set()
        if not self.release.wait(timeout=5):
            raise RuntimeError("test barrier was not released")
        return SourceResult((SourceRecord("late", {"title": "Late"}),), "done")


blocking_source = BlockingSource()


def test_startup_recovery_transfers_source_claim_and_rejects_late_finalize(tmp_path):
    definition = tmp_path / "source-recovery.yml"
    definition.write_text(
        """
version: 1
database: state.db
sources: {feed: {use: test_source_lifecycle.blocking_source, retries: 1, with: {}}}
""",
        encoding="utf-8",
    )
    app = open_app(definition, tmp_path / "state.db")
    worker = Thread(
        target=collect_source, args=(app, "feed", source_recovery_instant(0))
    )
    worker.start()
    assert blocking_source.started.wait(timeout=5)
    old_id = app.conn.execute(
        "SELECT id FROM executions WHERE kind='source' AND state='running'"
    ).fetchone()[0]
    from sluice.reconcile import recover_startup

    recover_startup(app, source_recovery_instant(1))
    blocking_source.release.set()
    worker.join(timeout=5)
    assert not worker.is_alive()
    assert (
        app.conn.execute(
            "SELECT state FROM executions WHERE id=?", (old_id,)
        ).fetchone()[0]
        == "failed"
    )
    assert app.conn.execute("SELECT COUNT(*) FROM publications").fetchone()[0] == 0
    assert (
        app.conn.execute(
            "SELECT COUNT(*) FROM executions WHERE retry_parent_id=? AND state='queued'",
            (old_id,),
        ).fetchone()[0]
        == 1
    )


def test_queued_retry_from_old_source_binding_is_superseded_before_new_callable(
    tmp_path,
):
    definition = tmp_path / "source-switch.yml"
    database = tmp_path / "state.db"
    definition.write_text(
        """
version: 1
database: state.db
sources:
  feed: {use: tests.integration.test_source_lifecycle.raising_collect, retries: 1, with: {}}
""",
        encoding="utf-8",
    )
    old = open_app(definition, database)
    with pytest.raises(ExecutionError):
        collect_source(old, "feed", source_recovery_instant(0))
    retry_id = old.conn.execute(
        "SELECT id FROM executions WHERE state='queued' ORDER BY id DESC LIMIT 1"
    ).fetchone()[0]
    old.conn.close()

    definition.write_text(
        """
version: 1
database: state.db
sources:
  feed: {use: tests.integration.test_source_lifecycle.source_collect, with: {}}
""",
        encoding="utf-8",
    )
    changed = open_app(definition, database)
    source_runtime._execute_queued_source(changed, retry_id, source_recovery_instant(1))
    assert (
        changed.conn.execute(
            "SELECT state FROM executions WHERE id=?", (retry_id,)
        ).fetchone()[0]
        == "failed"
    )
    assert changed.conn.execute("SELECT COUNT(*) FROM items").fetchone()[0] == 0


def test_queued_retry_whose_finalization_raises_is_failed_not_left_running(
    tmp_path, monkeypatch
):
    import sqlite3

    definition = tmp_path / "source-finalize.yml"
    definition.write_text(
        """
version: 1
database: state.db
sources:
  feed: {use: tests.integration.test_source_lifecycle.source_collect, retries: 1, with: {}}
""",
        encoding="utf-8",
    )
    app = open_app(definition, tmp_path / "state.db")

    def broken(*args):
        raise sqlite3.OperationalError("disk I/O error")

    monkeypatch.setattr(source_runtime, "_finalize_source", broken)
    with pytest.raises(ExecutionError):
        collect_source(app, "feed", source_recovery_instant(0))
    retry_id = app.conn.execute(
        "SELECT id FROM executions WHERE state='queued'"
    ).fetchone()[0]

    with pytest.raises(ExecutionError, match="finalization failed"):
        source_runtime._execute_queued_source(app, retry_id, source_recovery_instant(1))

    assert (
        app.conn.execute(
            "SELECT state FROM executions WHERE id=?", (retry_id,)
        ).fetchone()[0]
        == "failed"
    )
    assert (
        app.conn.execute(
            "SELECT COUNT(*) FROM executions WHERE state='running'"
        ).fetchone()[0]
        == 0
    )
