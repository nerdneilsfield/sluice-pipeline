import time
from datetime import UTC, datetime, timedelta
from threading import Event, Lock, Thread

import pytest

from sluice.app import open_app
from sluice.contracts import SourceRecord, SourceResult, StageEnvelope
from sluice.reconcile import reconcile, reconcile_node, tick
from sluice.source_runtime import collect_source
from sluice.stage_runtime import (
    _execute_claimed_stage,
    execute_stage,
    rebuild_stage,
    resolve_dedup_history_ids,
)


def sink_callable(config, payload, delivery_key):
    return None


def instant(seconds: int) -> datetime:
    return datetime(2026, 8, 14, tzinfo=UTC) + timedelta(seconds=seconds)


def reconcile_source_collect(config, checkpoint):
    if checkpoint is None:
        return SourceResult(
            (
                SourceRecord("item-a", {"title": "A"}),
                SourceRecord("item-b", {"title": "B"}),
            ),
            "two-1",
        )
    return SourceResult((SourceRecord("item-c", {"title": "C"}),), "two-2")


def same_identity_source(config, checkpoint):
    if checkpoint is None:
        return SourceResult((SourceRecord("same", {"title": "old"}),), "v1")
    return SourceResult((SourceRecord("same", {"title": "new"}),), "v2")


def downstream_source(config, checkpoint):
    return SourceResult((SourceRecord("downstream", {"content": "message"}),), "done")


def one_item_source(config, checkpoint):
    return SourceResult((SourceRecord("only", {"title": "Only"}),), "done")


def shared_dedup_source(config, checkpoint):
    if checkpoint is None:
        return SourceResult(
            (SourceRecord("first", {"title": "same", "body": "old"}),),
            "first-done",
        )
    return SourceResult(
        (SourceRecord("second", {"title": "same", "body": "new"}),),
        "second-done",
    )


def test_count_freeze_keeps_exact_order_and_post_freeze_arrival_pending(tmp_path):
    definition = tmp_path / "definition.yml"
    definition.write_text(
        """
version: 1
database: state.db
sources: {feed: {use: tests.integration.test_shared_materialization.reconcile_source_collect, with: {}}}
stages: {summary: {input: batch, tasks: [{use: sluice.tasks.dedup, with: {keys: [title]}}]}}
pipelines:
  p:
    nodes:
      summary: {stage: summary, from: source.feed, batch: {count: 2}}
      email: {from: summary, deliver: {use: tests.integration.test_shared_materialization.sink_callable, with: {target: test@example.test}}}
""",
        encoding="utf-8",
    )
    app = open_app(definition, tmp_path / "state.db")
    collect_source(app, "feed", instant(0))
    reconcile_node(app, "p", "summary", instant(0))
    batch_id = app.conn.execute(
        "SELECT current_frozen_batch_id FROM node_state WHERE pipeline_name='p' AND node_name='summary'"
    ).fetchone()[0]
    collect_source(app, "feed", instant(1))
    reconcile_node(app, "p", "summary", instant(1))
    frozen = app.conn.execute(
        "SELECT i.logical_id FROM batch_items bi JOIN items i ON i.id=bi.item_id WHERE bi.batch_id=? ORDER BY bi.ordinal",
        (batch_id,),
    ).fetchall()
    pending = app.conn.execute(
        "SELECT i.logical_id FROM node_members nm JOIN items i ON i.id=nm.item_id WHERE nm.pipeline_name='p' AND nm.node_name='summary' AND nm.state='pending' ORDER BY nm.ordinal"
    ).fetchall()
    assert [value[0] for value in frozen] == ["item-a", "item-b"]
    assert [value[0] for value in pending] == ["item-c"]


def test_reconcile_keeps_latest_pending_generation_stable_when_delta_repeats(tmp_path):
    definition = tmp_path / "generations.yml"
    definition.write_text(
        """
version: 1
database: state.db
sources: {feed: {use: tests.integration.test_shared_materialization.same_identity_source, with: {}}}
stages: {documents: {input: batch, tasks: [{use: sluice.tasks.filter, with: {all: [{field: title, op: exists}]}}]}}
pipelines:
  p:
    nodes:
      documents: {stage: documents, from: source.feed, batch: {max_wait: 1h}}
      email: {from: documents, deliver: {use: tests.integration.test_shared_materialization.sink_callable, with: {}}}
""",
        encoding="utf-8",
    )
    app = open_app(definition, tmp_path / "state.db")
    collect_source(app, "feed", instant(0))
    reconcile_node(app, "p", "documents", instant(0))
    old_id, old_generation = app.conn.execute(
        "SELECT item_id,generation FROM node_members "
        "WHERE pipeline_name='p' AND node_name='documents' AND state='pending'"
    ).fetchone()
    collect_source(app, "feed", instant(1))
    reconcile_node(app, "p", "documents", instant(1))
    latest = tuple(
        app.conn.execute(
            "SELECT item_id,generation,state FROM node_members "
            "WHERE pipeline_name='p' AND node_name='documents' AND state!='stale'"
        ).fetchone()
    )
    assert latest == (
        app.conn.execute(
            "SELECT id FROM items WHERE producer_name='feed' AND logical_id='same' "
            "AND payload_json=?",
            ('{"title":"new"}',),
        ).fetchone()[0],
        old_generation + 1,
        "pending",
    )

    reconcile_node(app, "p", "documents", instant(2))
    repeated = tuple(
        app.conn.execute(
            "SELECT item_id,generation,state FROM node_members "
            "WHERE pipeline_name='p' AND node_name='documents' AND state!='stale'"
        ).fetchone()
    )
    assert repeated == latest
    assert old_id != latest[0]


def test_batch_update_preserves_completed_visible_generation(tmp_path):
    definition = tmp_path / "completed-generation.yml"
    definition.write_text(
        """
version: 1
database: state.db
sources: {feed: {use: tests.integration.test_shared_materialization.same_identity_source, with: {}}}
stages: {documents: {input: batch, tasks: [{use: sluice.tasks.filter, with: {all: [{field: title, op: exists}]}}]}}
pipelines:
  p:
    nodes:
      documents: {stage: documents, from: source.feed, batch: {count: 1}}
      email: {from: documents, deliver: {use: tests.integration.test_shared_materialization.sink_callable, with: {}}}
""",
        encoding="utf-8",
    )
    app = open_app(definition, tmp_path / "state.db")
    collect_source(app, "feed", instant(0))
    reconcile_node(app, "p", "documents", instant(0))
    batch_id = app.conn.execute(
        "SELECT current_frozen_batch_id FROM node_state "
        "WHERE pipeline_name='p' AND node_name='documents'"
    ).fetchone()[0]
    old_id = app.conn.execute(
        "SELECT item_id FROM batch_items WHERE batch_id=?", (batch_id,)
    ).fetchone()[0]
    execute_stage(
        app,
        "documents",
        StageEnvelope((old_id,)),
        pipeline_name="p",
        node_name="documents",
        generation=1,
        batch_id=batch_id,
        at=instant(0),
    )
    old_member = tuple(
        app.conn.execute(
            "SELECT item_id,state,visible_result_id FROM node_members "
            "WHERE pipeline_name='p' AND node_name='documents' AND item_id=?",
            (old_id,),
        ).fetchone()
    )
    assert old_member[1] == "terminal"
    assert old_member[2] is not None

    collect_source(app, "feed", instant(1))
    reconcile_node(app, "p", "documents", instant(1))

    assert (
        tuple(
            app.conn.execute(
                "SELECT item_id,state,visible_result_id FROM node_members "
                "WHERE pipeline_name='p' AND node_name='documents' AND item_id=?",
                (old_id,),
            ).fetchone()
        )
        == old_member
    )
    publication_id = app.conn.execute(
        "SELECT current_publication_id FROM node_state "
        "WHERE pipeline_name='p' AND node_name='documents'"
    ).fetchone()[0]
    assert [
        row[0]
        for row in app.conn.execute(
            "SELECT item_id FROM publication_members WHERE publication_id=?",
            (publication_id,),
        )
    ] == [old_id]
    assert (
        app.conn.execute(
            "SELECT COUNT(*) FROM node_members WHERE pipeline_name='p' AND node_name='documents' "
            "AND state!='stale' AND item_id!=?",
            (old_id,),
        ).fetchone()[0]
        == 0
    )


def test_each_stage_admits_new_source_generation_after_prior_terminal(tmp_path):
    definition = tmp_path / "each-generations.yml"
    definition.write_text(
        """
version: 1
database: state.db
sources: {feed: {use: tests.integration.test_shared_materialization.same_identity_source, with: {}}}
stages: {documents: {input: each, tasks: [{use: sluice.tasks.to_markdown}]}}
pipelines:
  p:
    nodes:
      documents: {stage: documents, from: source.feed}
      email: {from: documents, deliver: {use: tests.integration.test_shared_materialization.sink_callable, with: {}}}
""",
        encoding="utf-8",
    )
    app = open_app(definition, tmp_path / "state.db")
    collect_source(app, "feed", instant(0))
    reconcile_node(app, "p", "documents", instant(0))
    old_id, old_generation = app.conn.execute(
        "SELECT item_id,generation FROM node_members "
        "WHERE pipeline_name='p' AND node_name='documents'"
    ).fetchone()
    execute_stage(
        app,
        "documents",
        StageEnvelope((old_id,)),
        pipeline_name="p",
        node_name="documents",
        generation=old_generation,
        batch_id=None,
        at=instant(0),
    )
    assert (
        app.conn.execute(
            "SELECT state FROM node_members WHERE item_id=?", (old_id,)
        ).fetchone()[0]
        == "terminal"
    )

    collect_source(app, "feed", instant(1))
    reconcile_node(app, "p", "documents", instant(1))
    members = [
        tuple(row)
        for row in app.conn.execute(
            "SELECT item_id,generation,state FROM node_members "
            "WHERE pipeline_name='p' AND node_name='documents' AND state!='stale' "
            "ORDER BY generation"
        )
    ]
    assert len(members) == 2
    assert members[0] == (old_id, old_generation, "terminal")
    assert members[1][1:] == (old_generation + 1, "pending")
    assert members[1][0] != old_id


def test_targeted_reconcile_drains_downstream_stages_and_delivery(tmp_path):
    definition = tmp_path / "downstream.yml"
    definition.write_text(
        """
version: 1
database: state.db
sources: {feed: {use: tests.integration.test_shared_materialization.downstream_source, with: {}}}
stages:
  first: {input: each, tasks: [{use: sluice.tasks.to_markdown}]}
  second: {input: each, tasks: [{use: sluice.tasks.render_email}]}
pipelines:
  p:
    nodes:
      first: {stage: first, from: source.feed}
      second: {stage: second, from: first}
      email: {from: second, deliver: {use: tests.integration.test_shared_materialization.sink_callable, with: {}}}
""",
        encoding="utf-8",
    )
    app = open_app(definition, tmp_path / "state.db")
    collect_source(app, "feed", instant(0))

    reconcile(app, "p", "first", instant(0))

    assert [
        row[0]
        for row in app.conn.execute(
            "SELECT state FROM executions WHERE kind='stage' "
            "AND pipeline_name='p' ORDER BY id"
        )
    ] == ["succeeded", "succeeded"]
    assert (
        app.conn.execute(
            "SELECT COUNT(*) FROM deliveries WHERE pipeline_name='p' AND node_name='email' "
            "AND state='succeeded'"
        ).fetchone()[0]
        == 1
    )


def test_ordinary_batch_drop_is_a_succeeded_empty_materialization(tmp_path):
    definition = tmp_path / "drop.yml"
    definition.write_text(
        """
version: 1
database: state.db
sources: {feed: {use: tests.integration.test_shared_materialization.source_collect, with: {}}}
stages:
  discard: {input: batch, tasks: [{use: sluice.tasks.filter, with: {all: [{field: missing, op: exists}]}}]}
pipelines:
  p:
    nodes:
      discard: {stage: discard, from: source.feed, batch: {count: 1}}
      email: {from: discard, deliver: {use: tests.integration.test_shared_materialization.sink_callable, with: {}}}
""",
        encoding="utf-8",
    )
    app = open_app(definition, tmp_path / "state.db")
    collect_source(app, "feed", instant(0))
    reconcile_node(app, "p", "discard", instant(0))
    batch_id = app.conn.execute(
        "SELECT current_frozen_batch_id FROM node_state "
        "WHERE pipeline_name='p' AND node_name='discard'"
    ).fetchone()[0]
    item_ids = tuple(
        row[0]
        for row in app.conn.execute(
            "SELECT item_id FROM batch_items WHERE batch_id=? ORDER BY ordinal",
            (batch_id,),
        )
    )
    execution_id = execute_stage(
        app,
        "discard",
        StageEnvelope(item_ids),
        pipeline_name="p",
        node_name="discard",
        generation=1,
        batch_id=batch_id,
        at=instant(0),
    )
    assert (
        app.conn.execute(
            "SELECT state FROM executions WHERE id=?", (execution_id,)
        ).fetchone()[0]
        == "succeeded"
    )
    assert (
        app.conn.execute(
            "SELECT COUNT(*) FROM stage_result_items sri "
            "JOIN stage_results sr ON sr.id=sri.result_id WHERE sr.producer_execution_id=?",
            (execution_id,),
        ).fetchone()[0]
        == 0
    )


@pytest.mark.parametrize(
    "node_policy",
    [
        "batch: {max_wait: 1h}",
        "schedule: 1h\n        batch: {on_schedule: true}",
    ],
)
def test_max_wait_and_on_schedule_freeze_only_at_persisted_due_time(
    tmp_path, node_policy
):
    definition = tmp_path / "due.yml"
    definition.write_text(
        f"""
version: 1
database: state.db
sources: {{feed: {{use: tests.integration.test_shared_materialization.one_item_source, with: {{}}}}}}
stages: {{summary: {{input: batch, tasks: [{{use: sluice.tasks.dedup, with: {{keys: [title]}}}}]}}}}
pipelines:
  p:
    nodes:
      summary:
        stage: summary
        from: source.feed
        {node_policy}
      email: {{from: summary, deliver: {{use: tests.integration.test_shared_materialization.sink_callable, with: {{}}}}}}
""",
        encoding="utf-8",
    )
    app = open_app(definition, tmp_path / "state.db")
    collect_source(app, "feed", instant(0))
    reconcile_node(app, "p", "summary", instant(0))
    reconcile_node(app, "p", "summary", instant(3599))
    assert app.conn.execute("SELECT COUNT(*) FROM batches").fetchone()[0] == 0
    reconcile_node(app, "p", "summary", instant(3600))
    assert app.conn.execute("SELECT COUNT(*) FROM batches").fetchone()[0] == 1


def source_collect(config, checkpoint):
    count = int(config.get("count", 1))
    return SourceResult(
        tuple(
            SourceRecord(f"i-{index}", {"url": f"https://example.test/{index}"})
            for index in range(count)
        ),
        "done",
    )


def first_binding_source(config, checkpoint):
    return SourceResult(
        (SourceRecord("old", {"title": "Old", "content": "Old"}),), "old-done"
    )


def second_binding_source(config, checkpoint):
    return SourceResult(
        (SourceRecord("new", {"title": "New", "content": "New"}),), "new-done"
    )


def write_binding_definition(path, source_path):
    path.write_text(
        """
version: 1
database: state.db
sources: {feed: {use: __SOURCE__, with: {}}}
stages: {documents: {input: each, tasks: [{use: sluice.tasks.to_markdown}]}}
pipelines:
  p:
    nodes:
      documents: {stage: documents, from: source.feed}
      email: {from: documents, deliver: {use: tests.integration.test_shared_materialization.sink_callable, with: {}}}
""".replace("__SOURCE__", source_path),
        encoding="utf-8",
    )


def test_tick_repairs_missed_callback_and_source_binding_switch_resets_edge(tmp_path):
    definition = tmp_path / "binding.yml"
    database = tmp_path / "state.db"
    write_binding_definition(
        definition, "tests.integration.test_shared_materialization.first_binding_source"
    )
    first = open_app(definition, database)
    collect_source(first, "feed", instant(0))
    first.conn.close()

    replay = open_app(definition, database)
    tick(replay, instant(1))
    assert (
        replay.conn.execute(
            "SELECT COUNT(*) FROM stage_results WHERE stage_name='documents'"
        ).fetchone()[0]
        == 1
    )
    replay.conn.close()

    write_binding_definition(
        definition,
        "tests.integration.test_shared_materialization.second_binding_source",
    )
    changed = open_app(definition, database)
    collect_source(changed, "feed", instant(2))
    reconcile(changed, "p", "documents", instant(2))
    active = changed.conn.execute(
        "SELECT i.logical_id FROM node_members nm JOIN items i ON i.id=nm.item_id "
        "WHERE nm.pipeline_name='p' AND nm.node_name='documents' AND nm.state!='stale' ORDER BY nm.ordinal"
    ).fetchall()
    assert [row[0] for row in active] == ["new"]


class BarrierHttp:
    def __init__(self, blocked_urls):
        self.started = {url: Event() for url in blocked_urls}
        self.release = {url: Event() for url in blocked_urls}
        self._first_calls = set(blocked_urls)
        self._lock = Lock()

    def get(self, url):
        with self._lock:
            blocks = url in self._first_calls
            self._first_calls.discard(url)
        if blocks:
            self.started[url].set()
            if not self.release[url].wait(timeout=5):
                raise RuntimeError("test barrier was not released")
        return (
            f"<html><body><article>{url} "
            + ("shared materialized content " * 8)
            + "</article></body></html>"
        )


class VersionedHttp:
    def __init__(self):
        self.calls = 0

    def get(self, url):
        self.calls += 1
        return (
            f"<html><body><article>{url}-version-{self.calls} "
            + ("versioned shared content " * 8)
            + "</article></body></html>"
        )


class VersionedLLM:
    def __init__(self):
        self.calls = 0

    def complete(self, model, prompt, input_payload):
        self.calls += 1
        return {"title": "same", "body": f"version-{self.calls}"}


class FailingBarrierHttp:
    def __init__(self, blocked_url):
        self.started = Event()
        self.release = Event()
        self.blocked_url = blocked_url

    def get(self, url):
        assert url == self.blocked_url
        self.started.set()
        if not self.release.wait(timeout=5):
            raise RuntimeError("test barrier was not released")
        raise RuntimeError("forced owner failure")


def shared_app(tmp_path, http_client, *, count=1, concurrency=1):
    definition = tmp_path / "definition.yml"
    definition.write_text(
        f"""
version: 1
database: state.db
web:
  providers: {{direct: {{type: startup_http, trusted: true, concurrency: {concurrency}}}}}
  fetch_chains: {{robust: {{providers: [direct]}}}}
  extractors: {{identity: {{type: identity}}, article: {{type: beautifulsoup}}}}
  extract_chains: {{article: {{extractors: [identity, article]}}}}
sources: {{feed: {{use: tests.integration.test_shared_materialization.source_collect, with: {{count: {count}}}}}}}
stages: {{prepare: {{input: each, tasks: [{{use: sluice.tasks.fetch_webpage, with: {{url_field: url, fetch_chain: robust, extract_chain: article, min_chars: 1}}}}, {{use: sluice.tasks.to_text}}]}}}}
pipelines:
  p1:
    nodes:
      prepare: {{stage: prepare, from: source.feed, timeout: 1s, retries: 1, concurrency: {concurrency}}}
      email: {{from: prepare, deliver: {{use: tests.integration.test_shared_materialization.sink_callable, with: {{target: one@example.test}}}}}}
  p2:
    nodes:
      prepare: {{stage: prepare, from: source.feed, timeout: 1s, retries: 1, concurrency: {concurrency}}}
      email: {{from: prepare, deliver: {{use: tests.integration.test_shared_materialization.sink_callable, with: {{target: two@example.test}}}}}}
""",
        encoding="utf-8",
    )
    app = open_app(definition, tmp_path / "state.db", http_client=http_client)
    collect_source(app, "feed", instant(0))
    return app


def test_shared_owner_waiter_survives_restart_and_waiter_uses_cached_result(tmp_path):
    url = "https://example.test/0"
    http = BarrierHttp((url,))
    app = shared_app(tmp_path, http)
    worker = Thread(target=tick, args=(app, instant(0)))
    worker.start()
    assert http.started[url].wait(timeout=5)
    old_id = app.conn.execute(
        "SELECT owner_execution_id FROM materialization_claims"
    ).fetchone()[0]
    assert (
        app.conn.execute("SELECT COUNT(*) FROM materialization_waiters").fetchone()[0]
        == 1
    )
    from sluice.reconcile import recover_startup

    recover_startup(app, instant(1))
    http.release[url].set()
    worker.join(timeout=5)
    assert not worker.is_alive()
    tick(app, instant(2))
    assert (
        app.conn.execute(
            "SELECT state FROM executions WHERE id=?", (old_id,)
        ).fetchone()[0]
        == "failed"
    )
    assert (
        app.conn.execute(
            "SELECT state FROM executions WHERE pipeline_name='p2' AND node_name='prepare' ORDER BY id DESC LIMIT 1"
        ).fetchone()[0]
        == "cached"
    )
    assert (
        app.conn.execute(
            "SELECT COUNT(*) FROM stage_results WHERE stage_name='prepare'"
        ).fetchone()[0]
        == 1
    )


def test_late_completion_after_timeout_cannot_publish(tmp_path):
    url = "https://example.test/0"
    http = BarrierHttp((url,))
    app = shared_app(tmp_path, http)
    worker = Thread(target=tick, args=(app, instant(0)))
    worker.start()
    assert http.started[url].wait(timeout=5)
    old_id = app.conn.execute(
        "SELECT owner_execution_id FROM materialization_claims"
    ).fetchone()[0]
    tick(app, instant(1))
    http.release[url].set()
    worker.join(timeout=5)
    assert not worker.is_alive()
    assert (
        app.conn.execute(
            "SELECT COUNT(*) FROM stage_results WHERE producer_execution_id=?",
            (old_id,),
        ).fetchone()[0]
        == 0
    )
    assert (
        app.conn.execute(
            "SELECT COUNT(*) FROM executions WHERE retry_parent_id=?", (old_id,)
        ).fetchone()[0]
        == 1
    )


def test_each_completion_publishes_only_longest_resolved_prefix(tmp_path):
    first_url, second_url = "https://example.test/0", "https://example.test/1"
    http = BarrierHttp((first_url, second_url))
    app = shared_app(tmp_path, http, count=2, concurrency=2)
    reconcile_node(app, "p1", "prepare", instant(0))
    members = app.conn.execute(
        "SELECT item_id,generation FROM node_members WHERE pipeline_name='p1' AND node_name='prepare' ORDER BY ordinal"
    ).fetchall()
    workers = [
        Thread(
            target=execute_stage,
            args=(app, "prepare", StageEnvelope((item_id,))),
            kwargs={
                "pipeline_name": "p1",
                "node_name": "prepare",
                "generation": generation,
                "batch_id": None,
                "at": instant(0),
            },
        )
        for item_id, generation in members
    ]
    for worker in workers:
        worker.start()
    assert http.started[first_url].wait(timeout=5)
    assert http.started[second_url].wait(timeout=5)
    http.release[second_url].set()
    workers[1].join(timeout=5)
    assert not workers[1].is_alive()
    current = app.conn.execute(
        "SELECT ns.current_publication_id FROM node_current_bindings nb JOIN node_state ns "
        "ON ns.pipeline_name=nb.pipeline_name AND ns.node_name=nb.node_name AND ns.binding_digest=nb.binding_digest "
        "WHERE nb.pipeline_name='p1' AND nb.node_name='prepare'"
    ).fetchone()[0]
    assert (
        app.conn.execute(
            "SELECT COUNT(*) FROM publication_members WHERE publication_id=?",
            (current,),
        ).fetchone()[0]
        == 0
    )
    http.release[first_url].set()
    workers[0].join(timeout=5)
    assert not workers[0].is_alive()
    logical_ids = [
        row[0]
        for row in app.conn.execute(
            "SELECT i.logical_id FROM node_current_bindings nb JOIN node_state ns "
            "ON ns.pipeline_name=nb.pipeline_name AND ns.node_name=nb.node_name AND ns.binding_digest=nb.binding_digest "
            "JOIN publication_members pm ON pm.publication_id=ns.current_publication_id "
            "JOIN items i ON i.id=pm.item_id WHERE nb.pipeline_name='p1' AND nb.node_name='prepare' ORDER BY pm.ordinal"
        )
    ]
    assert len(logical_ids) == 2


def sequential_source(config, checkpoint):
    ordinal = 1 if checkpoint is None else 2
    return SourceResult(
        (SourceRecord(f"item-{ordinal}", {"title": f"Item {ordinal}"}),), str(ordinal)
    )


def test_tick_resets_max_wait_from_first_post_freeze_arrival(tmp_path):
    definition = tmp_path / "two-waits.yml"
    definition.write_text(
        """
version: 1
database: state.db
sources: {feed: {use: tests.integration.test_shared_materialization.sequential_source, with: {}}}
stages: {summary: {input: batch, tasks: [{use: sluice.tasks.dedup, with: {keys: [title]}}]}}
pipelines:
  p:
    nodes:
      summary: {stage: summary, from: source.feed, batch: {max_wait: 1h}}
      email: {from: summary, deliver: {use: tests.integration.test_shared_materialization.sink_callable, with: {}}}
""",
        encoding="utf-8",
    )
    app = open_app(definition, tmp_path / "state.db")
    collect_source(app, "feed", instant(0))
    reconcile_node(app, "p", "summary", instant(0))
    tick(app, instant(3600))
    collect_source(app, "feed", instant(3700))
    reconcile_node(app, "p", "summary", instant(3700))
    tick(app, instant(7299))
    assert app.conn.execute("SELECT COUNT(*) FROM batches").fetchone()[0] == 1
    tick(app, instant(7300))
    assert app.conn.execute("SELECT COUNT(*) FROM batches").fetchone()[0] == 2


def test_tick_advances_source_and_stage_intervals_independently(tmp_path):
    definition = tmp_path / "cadence.yml"
    definition.write_text(
        """
version: 1
database: state.db
sources:
  feed: {use: tests.integration.test_shared_materialization.one_item_source, schedule: 10m, with: {}}
stages: {documents: {input: each, tasks: [{use: sluice.tasks.to_markdown}]}}
pipelines:
  p:
    nodes:
      documents: {stage: documents, from: source.feed, schedule: 1h}
      email: {from: documents, deliver: {use: tests.integration.test_shared_materialization.sink_callable, with: {}}}
""",
        encoding="utf-8",
    )
    app = open_app(definition, tmp_path / "state.db")
    tick(app, instant(0))
    tick(app, instant(600))
    assert (
        app.conn.execute(
            "SELECT COUNT(*) FROM executions WHERE kind='source'"
        ).fetchone()[0]
        == 2
    )
    assert (
        app.conn.execute(
            "SELECT COUNT(*) FROM executions WHERE kind='stage'"
        ).fetchone()[0]
        == 0
    )
    tick(app, instant(3600))
    assert (
        app.conn.execute(
            "SELECT COUNT(*) FROM executions WHERE kind='stage'"
        ).fetchone()[0]
        >= 1
    )


def test_stage_attachment_keeps_computed_lookup_key_on_node_member(tmp_path):
    http = BarrierHttp(("https://example.test/0",))
    app = shared_app(tmp_path, http)
    tick(app, instant(0))
    row = app.conn.execute(
        "SELECT e.lookup_key, nm.lookup_key FROM executions e "
        "JOIN node_members nm ON nm.pipeline_name=e.pipeline_name AND nm.node_name=e.node_name "
        "AND nm.binding_digest=e.binding_digest AND nm.item_id IN "
        "(SELECT item_id FROM execution_inputs WHERE execution_id=e.id AND slot='items') "
        "WHERE e.kind='stage' AND e.state IN ('succeeded','cached') ORDER BY e.id DESC LIMIT 1"
    ).fetchone()
    assert row is not None
    assert row[0] == row[1]


def test_execute_stage_waiter_returns_its_cached_terminal_attempt(tmp_path):
    url = "https://example.test/0"
    http = BarrierHttp((url,))
    app = shared_app(tmp_path, http)
    reconcile_node(app, "p1", "prepare", instant(0))
    reconcile_node(app, "p2", "prepare", instant(0))
    item_id, generation = app.conn.execute(
        "SELECT item_id,generation FROM node_members WHERE pipeline_name='p1' AND node_name='prepare'"
    ).fetchone()
    results: dict[str, int] = {}

    def owner_call():
        results["owner"] = execute_stage(
            app,
            "prepare",
            StageEnvelope((item_id,)),
            pipeline_name="p1",
            node_name="prepare",
            generation=generation,
            batch_id=None,
            at=instant(0),
        )

    def waiter_call():
        results["waiter"] = execute_stage(
            app,
            "prepare",
            StageEnvelope((item_id,)),
            pipeline_name="p2",
            node_name="prepare",
            generation=generation,
            batch_id=None,
            at=instant(0),
        )

    owner = Thread(target=owner_call)
    owner.start()
    assert http.started[url].wait(timeout=5)
    waiter = Thread(target=waiter_call)
    waiter.start()
    # The public waiter call must remain pending until the owner terminally
    # resolves, rather than returning the owner's running ID immediately.
    assert waiter.is_alive()
    http.release[url].set()
    owner.join(timeout=5)
    waiter.join(timeout=5)
    assert not owner.is_alive() and not waiter.is_alive()
    assert results["waiter"] != results["owner"]
    assert (
        app.conn.execute(
            "SELECT state FROM executions WHERE id=?", (results["waiter"],)
        ).fetchone()[0]
        == "cached"
    )


def test_forced_rebuild_heals_all_shared_terminal_consumers_without_rerunning_tasks(
    tmp_path,
):
    http = VersionedHttp()
    app = shared_app(tmp_path, http)
    reconcile_node(app, "p1", "prepare", instant(0))
    reconcile_node(app, "p2", "prepare", instant(0))
    item_id, generation = app.conn.execute(
        "SELECT item_id,generation FROM node_members "
        "WHERE pipeline_name='p1' AND node_name='prepare'"
    ).fetchone()

    owner_id = execute_stage(
        app,
        "prepare",
        StageEnvelope((item_id,)),
        pipeline_name="p1",
        node_name="prepare",
        generation=generation,
        batch_id=None,
        at=instant(0),
    )
    waiter_id = execute_stage(
        app,
        "prepare",
        StageEnvelope((item_id,)),
        pipeline_name="p2",
        node_name="prepare",
        generation=generation,
        batch_id=None,
        at=instant(0),
    )
    assert http.calls == 1
    old = [
        tuple(row)
        for row in app.conn.execute(
            "SELECT consumed_sequence,visible_result_id FROM node_members "
            "WHERE node_name='prepare' ORDER BY pipeline_name"
        )
    ]
    assert old[0] == old[1]

    rebuilt = rebuild_stage(app, owner_id, instant(1))
    assert rebuilt.status == "succeeded"
    assert rebuilt.execution_id is not None
    assert http.calls == 2

    current = app.conn.execute(
        "SELECT sr.id,sr.sequence FROM stage_result_current src "
        "JOIN stage_results sr ON sr.id=src.result_id "
        "WHERE src.stage_name='prepare'"
    ).fetchone()
    members = [
        tuple(row)
        for row in app.conn.execute(
            "SELECT consumed_sequence,visible_result_id FROM node_members "
            "WHERE node_name='prepare' ORDER BY pipeline_name"
        )
    ]
    assert members == [(current[1], current[0]), (current[1], current[0])]
    assert (
        app.conn.execute(
            "SELECT state FROM executions WHERE id=?", (waiter_id,)
        ).fetchone()[0]
        == "cached"
    )


def test_batch_healing_preserves_empty_dedup_history_when_live_history_changes(
    tmp_path,
):
    llm = VersionedLLM()
    definition = tmp_path / "dedup-heal.yml"
    definition.write_text(
        """
version: 1
database: state.db
sources: {feed: {use: tests.integration.test_shared_materialization.shared_dedup_source, with: {}}}
stages:
  summary:
    input: batch
    tasks:
      - {use: sluice.tasks.dedup, with: {keys: [title]}}
      - use: sluice.tasks.llm_summary
        with: {model: test, prompt: prompt.md}
pipelines:
  p1:
    nodes:
      summary: {stage: summary, from: source.feed, batch: {count: 1}}
      email: {from: summary, schedule: 1h, deliver: {use: tests.integration.test_shared_materialization.sink_callable, with: {}}}
  p2:
    nodes:
      summary: {stage: summary, from: source.feed, batch: {count: 1}}
      email: {from: summary, schedule: 1h, deliver: {use: tests.integration.test_shared_materialization.sink_callable, with: {}}}
""",
        encoding="utf-8",
    )
    (tmp_path / "prompt.md").write_text("Return a JSON summary.", encoding="utf-8")
    app = open_app(definition, tmp_path / "state.db", llm_client=llm)

    def frozen_batch(pipeline_name):
        batch_id = app.conn.execute(
            "SELECT current_frozen_batch_id FROM node_state "
            "WHERE pipeline_name=? AND node_name='summary'",
            (pipeline_name,),
        ).fetchone()[0]
        item_id, generation = app.conn.execute(
            "SELECT item_id,generation FROM node_members "
            "WHERE pipeline_name=? AND node_name='summary' AND state='frozen'",
            (pipeline_name,),
        ).fetchone()
        return int(batch_id), (str(item_id),), int(generation)

    collect_source(app, "feed", instant(0))
    reconcile_node(app, "p1", "summary", instant(0))
    reconcile_node(app, "p2", "summary", instant(0))
    first_batch_1, first_items, first_generation = frozen_batch("p1")
    first_batch_2, first_items_2, _ = frozen_batch("p2")
    assert first_items_2 == first_items

    first_owner = execute_stage(
        app,
        "summary",
        StageEnvelope(first_items),
        pipeline_name="p1",
        node_name="summary",
        generation=first_generation,
        batch_id=first_batch_1,
        at=instant(0),
    )
    execute_stage(
        app,
        "summary",
        StageEnvelope(first_items_2),
        pipeline_name="p2",
        node_name="summary",
        generation=first_generation,
        batch_id=first_batch_2,
        at=instant(0),
    )
    first_lookup, first_output = app.conn.execute(
        "SELECT e.lookup_key, sri.item_id FROM executions e "
        "JOIN stage_results sr ON sr.id=e.result_id "
        "JOIN stage_result_items sri ON sri.result_id=sr.id "
        "WHERE e.id=? ORDER BY sri.ordinal",
        (first_owner,),
    ).fetchone()
    first_result = app.conn.execute(
        "SELECT result_id FROM executions WHERE id=?", (first_owner,)
    ).fetchone()[0]
    assert (
        app.conn.execute(
            "SELECT COUNT(*) FROM execution_inputs WHERE execution_id=? AND slot='history'",
            (first_owner,),
        ).fetchone()[0]
        == 0
    )
    assert (
        app.conn.execute(
            "SELECT history_persisted FROM executions WHERE id=?", (first_owner,)
        ).fetchone()[0]
        == 1
    )

    collect_source(app, "feed", instant(-1))
    reconcile_node(app, "p1", "summary", instant(1))
    reconcile_node(app, "p2", "summary", instant(1))
    second_batch_1, second_items, second_generation = frozen_batch("p1")
    second_batch_2, second_items_2, _ = frozen_batch("p2")
    assert second_items_2 != first_items
    second_owner = execute_stage(
        app,
        "summary",
        StageEnvelope(second_items),
        pipeline_name="p1",
        node_name="summary",
        generation=second_generation,
        batch_id=second_batch_1,
        at=instant(1),
    )
    execute_stage(
        app,
        "summary",
        StageEnvelope(second_items_2),
        pipeline_name="p2",
        node_name="summary",
        generation=second_generation,
        batch_id=second_batch_2,
        at=instant(1),
    )
    assert llm.calls == 2
    second_result = app.conn.execute(
        "SELECT result_id FROM executions WHERE id=?", (second_owner,)
    ).fetchone()[0]
    assert resolve_dedup_history_ids(
        app, app.definition.stages["summary"], first_items
    ) == tuple(
        row[0]
        for row in app.conn.execute(
            "SELECT item_id FROM stage_result_items WHERE result_id=? ORDER BY ordinal",
            (second_result,),
        )
    )

    rebuilt = rebuild_stage(app, first_owner, instant(2))
    assert rebuilt.status == "succeeded"
    assert rebuilt.execution_id is not None
    assert llm.calls == 3
    rebuilt_result, rebuilt_lookup = app.conn.execute(
        "SELECT result_id,lookup_key FROM executions WHERE id=?",
        (rebuilt.execution_id,),
    ).fetchone()
    assert rebuilt_lookup == first_lookup
    rebuilt_output = app.conn.execute(
        "SELECT sri.item_id FROM stage_result_items sri WHERE sri.result_id=?",
        (rebuilt_result,),
    ).fetchone()[0]
    assert rebuilt_output != first_output

    healed = [
        tuple(row)
        for row in app.conn.execute(
            "SELECT pipeline_name,state,lookup_key,result_id FROM executions "
            "WHERE kind='stage' AND id>? ORDER BY id",
            (rebuilt.execution_id,),
        )
    ]
    assert healed == [("p2", "cached", first_lookup, rebuilt_result)]
    assert llm.calls == 3
    healed_execution = app.conn.execute(
        "SELECT id FROM executions WHERE pipeline_name='p2' AND id>? ORDER BY id DESC LIMIT 1",
        (rebuilt.execution_id,),
    ).fetchone()[0]
    assert (
        app.conn.execute(
            "SELECT COUNT(*) FROM execution_inputs WHERE execution_id=? AND slot='history'",
            (healed_execution,),
        ).fetchone()[0]
        == 0
    )
    assert (
        app.conn.execute(
            "SELECT history_persisted FROM executions WHERE id=?", (healed_execution,)
        ).fetchone()[0]
        == 1
    )
    rebuilt_sequence = app.conn.execute(
        "SELECT sequence FROM stage_results WHERE id=?", (rebuilt_result,)
    ).fetchone()[0]
    second_result = app.conn.execute(
        "SELECT result_id FROM executions WHERE id=?", (second_owner,)
    ).fetchone()[0]
    for pipeline_name in ("p1", "p2"):
        first_member = app.conn.execute(
            "SELECT consumed_sequence,visible_result_id,state FROM node_members "
            "WHERE pipeline_name=? AND node_name='summary' AND item_id=?",
            (pipeline_name, first_items[0]),
        ).fetchone()
        second_member = app.conn.execute(
            "SELECT consumed_sequence,visible_result_id,state FROM node_members "
            "WHERE pipeline_name=? AND node_name='summary' AND item_id=?",
            (pipeline_name, second_items[0]),
        ).fetchone()
        expected_result = first_result if pipeline_name == "p1" else rebuilt_result
        assert tuple(first_member) == (rebuilt_sequence, expected_result, "terminal")
        assert tuple(second_member) == (2, second_result, "terminal")


def test_forced_rebuild_keeps_a_distinct_force_waiter_ahead_of_normal_waiter(tmp_path):
    url = "https://example.test/0"
    http = BarrierHttp((url,))
    app = shared_app(tmp_path, http)
    reconcile_node(app, "p1", "prepare", instant(0))
    item_id, generation = app.conn.execute(
        "SELECT item_id,generation FROM node_members WHERE pipeline_name='p1' AND node_name='prepare'"
    ).fetchone()
    results: dict[str, int] = {}

    def owner_call():
        results["owner"] = execute_stage(
            app,
            "prepare",
            StageEnvelope((item_id,)),
            pipeline_name="p1",
            node_name="prepare",
            generation=generation,
            batch_id=None,
            at=instant(0),
        )

    def waiter_call():
        results["waiter"] = execute_stage(
            app,
            "prepare",
            StageEnvelope((item_id,)),
            pipeline_name="p1",
            node_name="prepare",
            generation=generation,
            batch_id=None,
            at=instant(0),
        )

    owner = Thread(target=owner_call)
    owner.start()
    assert http.started[url].wait(timeout=5)
    owner_id = app.conn.execute(
        "SELECT id FROM executions WHERE pipeline_name='p1' AND node_name='prepare' "
        "ORDER BY id DESC LIMIT 1"
    ).fetchone()[0]
    waiter = Thread(target=waiter_call)
    waiter.start()
    assert waiter.is_alive()
    for _ in range(1000):
        with app.conn.transaction_lock:
            count = app.conn.execute(
                "SELECT COUNT(*) FROM materialization_waiters WHERE force=0"
            ).fetchone()[0]
        if count == 1:
            break
        time.sleep(0.001)
    with app.conn.transaction_lock:
        assert (
            app.conn.execute(
                "SELECT COUNT(*) FROM materialization_waiters WHERE force=0"
            ).fetchone()[0]
            == 1
        )

    rebuild = rebuild_stage(app, owner_id, instant(1))
    assert rebuild.execution_id is None
    assert rebuild.status == "queued"
    waiters = [
        tuple(row)
        for row in app.conn.execute(
            "SELECT force,rebuild_target_id FROM materialization_waiters "
            "ORDER BY force DESC,waiting_sequence"
        )
    ]
    assert waiters == [(1, owner_id), (0, None)]

    http.release[url].set()
    owner.join(timeout=5)
    assert not owner.is_alive()
    forced_id = app.conn.execute(
        "SELECT id FROM executions WHERE rebuild_target_id=? ORDER BY id DESC LIMIT 1",
        (owner_id,),
    ).fetchone()[0]
    _execute_claimed_stage(app, forced_id, instant(2))
    waiter.join(timeout=5)
    assert not waiter.is_alive()
    assert (
        app.conn.execute(
            "SELECT state FROM executions WHERE id=?", (forced_id,)
        ).fetchone()[0]
        == "succeeded"
    )
    assert (
        app.conn.execute(
            "SELECT state FROM executions WHERE id=?", (results["waiter"],)
        ).fetchone()[0]
        == "cached"
    )


def test_stale_waiter_after_failed_owner_terminates_deterministically(tmp_path):
    url = "https://example.test/0"
    http = FailingBarrierHttp(url)
    app = shared_app(tmp_path, http)
    reconcile_node(app, "p1", "prepare", instant(0))
    reconcile_node(app, "p2", "prepare", instant(0))
    item_id, generation = app.conn.execute(
        "SELECT item_id,generation FROM node_members "
        "WHERE pipeline_name='p1' AND node_name='prepare'"
    ).fetchone()
    errors: dict[str, BaseException] = {}

    def invoke(name, pipeline_name):
        try:
            execute_stage(
                app,
                "prepare",
                StageEnvelope((item_id,)),
                pipeline_name=pipeline_name,
                node_name="prepare",
                generation=generation,
                batch_id=None,
                at=instant(0),
            )
        except BaseException as error:  # noqa: BLE001 - propagate worker failures to the test thread
            errors[name] = error

    owner = Thread(target=invoke, args=("owner", "p1"), daemon=True)
    owner.start()
    assert http.started.wait(timeout=5)
    waiter = Thread(target=invoke, args=("waiter", "p2"), daemon=True)
    waiter.start()
    assert waiter.is_alive()

    app.conn.execute(
        "UPDATE node_edge_members SET active=0 "
        "WHERE pipeline_name='p2' AND node_name='prepare'"
    )
    reconcile_node(app, "p2", "prepare", instant(1))
    http.release.set()
    owner.join(timeout=5)
    waiter.join(timeout=5)

    assert not owner.is_alive()
    assert not waiter.is_alive()
    assert not errors


def test_reconcile_marks_omitted_claimed_member_stale_and_rejects_late_result(tmp_path):
    url = "https://example.test/0"
    http = BarrierHttp((url,))
    app = shared_app(tmp_path, http)
    reconcile_node(app, "p1", "prepare", instant(0))
    item_id, generation = app.conn.execute(
        "SELECT item_id,generation FROM node_members WHERE pipeline_name='p1' AND node_name='prepare'"
    ).fetchone()
    worker = Thread(
        target=execute_stage,
        args=(app, "prepare", StageEnvelope((item_id,))),
        kwargs={
            "pipeline_name": "p1",
            "node_name": "prepare",
            "generation": generation,
            "batch_id": None,
            "at": instant(0),
        },
    )
    worker.start()
    assert http.started[url].wait(timeout=5)
    app.conn.execute(
        "UPDATE node_edge_members SET active=0 WHERE pipeline_name='p1' AND node_name='prepare'"
    )
    reconcile_node(app, "p1", "prepare", instant(1))
    http.release[url].set()
    worker.join(timeout=5)
    assert not worker.is_alive()
    member = app.conn.execute(
        "SELECT state,visible_result_id FROM node_members WHERE pipeline_name='p1' AND node_name='prepare' AND item_id=?",
        (item_id,),
    ).fetchone()
    assert tuple(member) == ("stale", None)
    current = app.conn.execute(
        "SELECT ns.current_publication_id FROM node_current_bindings nb JOIN node_state ns "
        "ON ns.pipeline_name=nb.pipeline_name AND ns.node_name=nb.node_name AND ns.binding_digest=nb.binding_digest "
        "WHERE nb.pipeline_name='p1' AND nb.node_name='prepare'"
    ).fetchone()[0]
    assert (
        app.conn.execute(
            "SELECT COUNT(*) FROM publication_members WHERE publication_id=?",
            (current,),
        ).fetchone()[0]
        == 0
    )


def test_targeted_reconcile_does_not_timeout_unrelated_running_owner(tmp_path):
    url = "https://example.test/0"
    http = BarrierHttp((url,))
    app = shared_app(tmp_path, http)
    reconcile_node(app, "p1", "prepare", instant(0))
    reconcile_node(app, "p2", "prepare", instant(0))
    item_id, generation = app.conn.execute(
        "SELECT item_id,generation FROM node_members WHERE pipeline_name='p2' AND node_name='prepare'"
    ).fetchone()
    worker = Thread(
        target=execute_stage,
        args=(app, "prepare", StageEnvelope((item_id,))),
        kwargs={
            "pipeline_name": "p2",
            "node_name": "prepare",
            "generation": generation,
            "batch_id": None,
            "at": instant(0),
        },
    )
    worker.start()
    assert http.started[url].wait(timeout=5)
    old_id = app.conn.execute(
        "SELECT id FROM executions WHERE pipeline_name='p2' AND node_name='prepare' "
        "ORDER BY id DESC LIMIT 1"
    ).fetchone()[0]
    reconcile(app, "p1", "prepare", instant(1))
    state = app.conn.execute(
        "SELECT state FROM executions WHERE id=?",
        (old_id,),
    ).fetchone()[0]
    assert state == "running"
    http.release[url].set()
    worker.join(timeout=5)
    assert not worker.is_alive()


def ordered_source(config, checkpoint):
    return SourceResult(
        (
            SourceRecord("a", {"title": "A", "content": "A"}),
            SourceRecord("b", {"title": "B", "content": "B"}),
        ),
        "done",
    )


def write_union_definition(path, left_stage):
    path.write_text(
        f"""
version: 1
database: state.db
sources: {{feed: {{use: tests.integration.test_shared_materialization.ordered_source, with: {{}}}}}}
stages:
  asc: {{input: batch, tasks: [{{use: sluice.tasks.sort, with: {{by: [{{field: title, order: asc}}]}}}}]}}
  desc: {{input: batch, tasks: [{{use: sluice.tasks.sort, with: {{by: [{{field: title, order: desc}}]}}}}]}}
  empty: {{input: batch, tasks: [{{use: sluice.tasks.filter, with: {{all: [{{field: absent, op: exists}}]}}}}]}}
  documents: {{input: each, tasks: [{{use: sluice.tasks.to_markdown}}]}}
pipelines:
  p:
    nodes:
      left: {{stage: {left_stage}, from: source.feed, batch: {{count: 2}}}}
      right: {{stage: desc, from: source.feed, batch: {{count: 2}}}}
      documents: {{stage: documents, from: [left, right]}}
      email: {{from: documents, deliver: {{use: tests.integration.test_shared_materialization.sink_callable, with: {{}}}}}}
""",
        encoding="utf-8",
    )


def test_reorder_duplicate_fallback_and_snapshot_omission_preserve_current_union(
    tmp_path,
):
    definition = tmp_path / "union.yml"
    write_union_definition(definition, "asc")
    first = open_app(definition, tmp_path / "state.db")
    tick(first, instant(0))
    assert [
        row[0]
        for row in first.conn.execute(
            "SELECT i.logical_id FROM node_members nm JOIN items i ON i.id=nm.item_id "
            "WHERE nm.pipeline_name='p' AND nm.node_name='documents' AND nm.state!='stale' ORDER BY nm.ordinal"
        )
    ] == ["a", "b"]
    right_results = first.conn.execute(
        "SELECT COUNT(*) FROM stage_results WHERE stage_name='desc'"
    ).fetchone()[0]
    first.conn.close()

    write_union_definition(definition, "empty")
    second = open_app(definition, tmp_path / "state.db")
    tick(second, instant(1))
    assert [
        row[0]
        for row in second.conn.execute(
            "SELECT i.logical_id FROM node_members nm JOIN items i ON i.id=nm.item_id "
            "WHERE nm.pipeline_name='p' AND nm.node_name='documents' AND nm.state!='stale' ORDER BY nm.ordinal"
        )
    ] == ["b", "a"]
    empty_publication = second.conn.execute(
        "SELECT ns.current_publication_id FROM node_current_bindings nb JOIN node_state ns "
        "ON ns.pipeline_name=nb.pipeline_name AND ns.node_name=nb.node_name AND ns.binding_digest=nb.binding_digest "
        "WHERE nb.pipeline_name='p' AND nb.node_name='left'"
    ).fetchone()[0]
    assert (
        second.conn.execute(
            "SELECT COUNT(*) FROM publication_members WHERE publication_id=?",
            (empty_publication,),
        ).fetchone()[0]
        == 0
    )
    assert (
        second.conn.execute(
            "SELECT COUNT(*) FROM stage_results WHERE stage_name='desc'"
        ).fetchone()[0]
        == right_results
    )


def test_admission_reorder_publishes_snapshot_without_new_stage_completion(tmp_path):
    definition = tmp_path / "union.yml"
    write_union_definition(definition, "asc")
    first = open_app(definition, tmp_path / "state.db")
    tick(first, instant(0))
    current_before = first.conn.execute(
        "SELECT ns.current_publication_id FROM node_current_bindings nb JOIN node_state ns "
        "ON ns.pipeline_name=nb.pipeline_name AND ns.node_name=nb.node_name AND ns.binding_digest=nb.binding_digest "
        "WHERE nb.pipeline_name='p' AND nb.node_name='documents'"
    ).fetchone()[0]
    members_before = [
        row[0]
        for row in first.conn.execute(
            "SELECT item_id FROM publication_members WHERE publication_id=? ORDER BY ordinal",
            (current_before,),
        )
    ]
    first.conn.close()

    write_union_definition(definition, "empty")
    second = open_app(definition, tmp_path / "state.db")
    before_results = second.conn.execute(
        "SELECT COUNT(*) FROM stage_results WHERE stage_name='documents'"
    ).fetchone()[0]
    reconcile_node(second, "p", "documents", instant(1))
    after_results = second.conn.execute(
        "SELECT COUNT(*) FROM stage_results WHERE stage_name='documents'"
    ).fetchone()[0]
    assert after_results == before_results
    current_after = second.conn.execute(
        "SELECT ns.current_publication_id FROM node_current_bindings nb JOIN node_state ns "
        "ON ns.pipeline_name=nb.pipeline_name AND ns.node_name=nb.node_name AND ns.binding_digest=nb.binding_digest "
        "WHERE nb.pipeline_name='p' AND nb.node_name='documents'"
    ).fetchone()[0]
    assert current_after != current_before
    members_after = [
        row[0]
        for row in second.conn.execute(
            "SELECT pm.item_id FROM publication_members pm "
            "WHERE pm.publication_id=? ORDER BY pm.ordinal",
            (current_after,),
        )
    ]
    assert members_after == list(reversed(members_before))


def test_targeted_reconcile_activates_changed_source_binding_without_collection(
    tmp_path,
):
    definition = tmp_path / "binding.yml"
    database = tmp_path / "state.db"
    write_binding_definition(
        definition,
        "tests.integration.test_shared_materialization.first_binding_source",
    )
    first = open_app(definition, database)
    collect_source(first, "feed", instant(0))
    reconcile_node(first, "p", "documents", instant(0))
    old_binding = first.conn.execute(
        "SELECT binding_digest FROM source_current_bindings WHERE source_name='feed'"
    ).fetchone()[0]
    assert (
        first.conn.execute(
            "SELECT COUNT(*) FROM publications WHERE producer_key='feed' AND binding_digest=?",
            (old_binding,),
        ).fetchone()[0]
        >= 1
    )
    first.conn.close()

    write_binding_definition(
        definition,
        "tests.integration.test_shared_materialization.second_binding_source",
    )
    changed = open_app(definition, database)
    reconcile(changed, "p", "documents", instant(1))
    new_binding = changed.conn.execute(
        "SELECT binding_digest FROM source_current_bindings WHERE source_name='feed'"
    ).fetchone()[0]
    assert new_binding != old_binding
    assert (
        changed.conn.execute(
            "SELECT COUNT(*) FROM source_state WHERE source_name='feed' AND binding_digest=?",
            (new_binding,),
        ).fetchone()[0]
        == 1
    )
    assert (
        changed.conn.execute(
            "SELECT COUNT(*) FROM publications WHERE producer_key='feed' AND binding_digest=?",
            (old_binding,),
        ).fetchone()[0]
        >= 1
    )
    new_publications = changed.conn.execute(
        "SELECT revision,kind,digest FROM publications WHERE producer_key='feed' "
        "AND binding_digest=? ORDER BY revision",
        (new_binding,),
    ).fetchall()
    assert [(row[0], row[1]) for row in new_publications] == [(0, "delta")]
    edge = changed.conn.execute(
        "SELECT producer_binding_digest,last_revision,applied_digest "
        "FROM node_edge_state WHERE pipeline_name='p' AND node_name='documents' "
        "AND edge_index=0"
    ).fetchone()
    assert edge[0] == new_binding
    assert edge[1] == 0
    assert edge[2] == new_publications[0][2]
    assert (
        changed.conn.execute(
            "SELECT COUNT(*) FROM node_edge_members WHERE pipeline_name='p' "
            "AND node_name='documents' AND edge_index=0 AND active=1"
        ).fetchone()[0]
        == 0
    )
    assert (
        changed.conn.execute(
            "SELECT COUNT(*) FROM node_members WHERE pipeline_name='p' AND node_name='documents' "
            "AND state!='stale'"
        ).fetchone()[0]
        == 0
    )


def test_targeted_source_activation_is_due_immediately_and_advances_schedule(tmp_path):
    definition = tmp_path / "scheduled-binding.yml"
    definition.write_text(
        """
version: 1
database: state.db
sources:
  feed:
    use: tests.integration.test_shared_materialization.first_binding_source
    schedule: 1h
    with: {}
stages: {documents: {input: each, tasks: [{use: sluice.tasks.to_markdown}]}}
pipelines:
  p:
    nodes:
      documents: {stage: documents, from: source.feed}
      email: {from: documents, deliver: {use: tests.integration.test_shared_materialization.sink_callable, with: {}}}
""",
        encoding="utf-8",
    )
    app = open_app(definition, tmp_path / "state.db")
    reconcile_node(app, "p", "documents", instant(0))
    binding = app.conn.execute(
        "SELECT binding_digest FROM source_current_bindings WHERE source_name='feed'"
    ).fetchone()[0]
    due = app.conn.execute(
        "SELECT next_due_at FROM source_state WHERE source_name='feed' AND binding_digest=?",
        (binding,),
    ).fetchone()[0]
    assert due == "2026-08-14T00:00:00Z"
    tick(app, instant(1))
    assert (
        app.conn.execute(
            "SELECT COUNT(*) FROM executions WHERE kind='source' AND state='succeeded'"
        ).fetchone()[0]
        == 1
    )
    assert (
        app.conn.execute(
            "SELECT next_due_at FROM source_state WHERE source_name='feed' AND binding_digest=?",
            (binding,),
        ).fetchone()[0]
        == "2026-08-14T01:00:01Z"
    )
