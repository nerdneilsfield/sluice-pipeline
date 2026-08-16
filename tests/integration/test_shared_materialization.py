from datetime import datetime, timedelta, timezone
from threading import Event, Lock, Thread

import pytest

from sluice.app import open_app
from sluice.contracts import SourceRecord, SourceResult, StageEnvelope
from sluice.errors import ExecutionError
from sluice.reconcile import reconcile, reconcile_node, tick
from sluice.source_runtime import collect_source
from sluice.stage_runtime import execute_stage


def sink_callable(config, payload, delivery_key):
    return None


def instant(seconds: int) -> datetime:
    return datetime(2026, 8, 14, tzinfo=timezone.utc) + timedelta(seconds=seconds)


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


def one_item_source(config, checkpoint):
    return SourceResult((SourceRecord("only", {"title": "Only"}),), "done")


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


@pytest.mark.parametrize(
    "node_policy",
    [
        "batch: {max_wait: 1h}",
        "schedule: 1h\n        batch: {on_schedule: true}",
    ],
)
def test_max_wait_and_on_schedule_freeze_only_at_persisted_due_time(tmp_path, node_policy):
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
        tuple(SourceRecord(f"i-{index}", {"url": f"https://example.test/{index}"}) for index in range(count)),
        "done",
    )


def first_binding_source(config, checkpoint):
    return SourceResult((SourceRecord("old", {"title": "Old", "content": "Old"}),), "old-done")


def second_binding_source(config, checkpoint):
    return SourceResult((SourceRecord("new", {"title": "New", "content": "New"}),), "new-done")


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
    write_binding_definition(definition, "tests.integration.test_shared_materialization.first_binding_source")
    first = open_app(definition, database)
    collect_source(first, "feed", instant(0))
    first.conn.close()

    replay = open_app(definition, database)
    tick(replay, instant(1))
    assert replay.conn.execute("SELECT COUNT(*) FROM stage_results WHERE stage_name='documents'").fetchone()[0] == 1
    replay.conn.close()

    write_binding_definition(definition, "tests.integration.test_shared_materialization.second_binding_source")
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
        return f"<html><body><article>{url}</article></body></html>"


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
sources: {{feed: {{use: tests.integration.test_shared_materialization.source_collect, with: {{count: {count}}}}}}}
stages: {{prepare: {{input: each, tasks: [{{use: sluice.tasks.fetch_webpage, with: {{url_field: url}}}}]}}}}
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
    old_id = app.conn.execute("SELECT owner_execution_id FROM materialization_claims").fetchone()[0]
    assert app.conn.execute("SELECT COUNT(*) FROM materialization_waiters").fetchone()[0] == 1
    from sluice.reconcile import recover_startup
    recover_startup(app, instant(1))
    http.release[url].set()
    worker.join(timeout=5)
    assert not worker.is_alive()
    tick(app, instant(2))
    assert app.conn.execute("SELECT state FROM executions WHERE id=?", (old_id,)).fetchone()[0] == "failed"
    assert app.conn.execute(
        "SELECT state FROM executions WHERE pipeline_name='p2' AND node_name='prepare' ORDER BY id DESC LIMIT 1"
    ).fetchone()[0] == "cached"
    assert app.conn.execute("SELECT COUNT(*) FROM stage_results WHERE stage_name='prepare'").fetchone()[0] == 1


def test_late_completion_after_timeout_cannot_publish(tmp_path):
    url = "https://example.test/0"
    http = BarrierHttp((url,))
    app = shared_app(tmp_path, http)
    worker = Thread(target=tick, args=(app, instant(0)))
    worker.start()
    assert http.started[url].wait(timeout=5)
    old_id = app.conn.execute("SELECT owner_execution_id FROM materialization_claims").fetchone()[0]
    tick(app, instant(1))
    http.release[url].set()
    worker.join(timeout=5)
    assert not worker.is_alive()
    assert app.conn.execute(
        "SELECT COUNT(*) FROM stage_results WHERE producer_execution_id=?", (old_id,)
    ).fetchone()[0] == 0
    assert app.conn.execute("SELECT COUNT(*) FROM executions WHERE retry_parent_id=?", (old_id,)).fetchone()[0] == 1


def test_each_completion_publishes_only_longest_resolved_prefix(tmp_path):
    first_url, second_url = "https://example.test/0", "https://example.test/1"
    http = BarrierHttp((first_url, second_url))
    app = shared_app(tmp_path, http, count=2, concurrency=2)
    reconcile_node(app, "p1", "prepare", instant(0))
    members = app.conn.execute(
        "SELECT item_id,generation FROM node_members WHERE pipeline_name='p1' AND node_name='prepare' ORDER BY ordinal"
    ).fetchall()
    workers = [Thread(
        target=execute_stage,
        args=(app, "prepare", StageEnvelope((item_id,))),
        kwargs={"pipeline_name": "p1", "node_name": "prepare", "generation": generation,
                "batch_id": None, "at": instant(0)},
    ) for item_id, generation in members]
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
    assert app.conn.execute("SELECT COUNT(*) FROM publication_members WHERE publication_id=?", (current,)).fetchone()[0] == 0
    http.release[first_url].set()
    workers[0].join(timeout=5)
    assert not workers[0].is_alive()
    logical_ids = [row[0] for row in app.conn.execute(
        "SELECT i.logical_id FROM node_current_bindings nb JOIN node_state ns "
        "ON ns.pipeline_name=nb.pipeline_name AND ns.node_name=nb.node_name AND ns.binding_digest=nb.binding_digest "
        "JOIN publication_members pm ON pm.publication_id=ns.current_publication_id "
        "JOIN items i ON i.id=pm.item_id WHERE nb.pipeline_name='p1' AND nb.node_name='prepare' ORDER BY pm.ordinal"
    )]
    assert len(logical_ids) == 2


def sequential_source(config, checkpoint):
    ordinal = 1 if checkpoint is None else 2
    return SourceResult((SourceRecord(f"item-{ordinal}", {"title": f"Item {ordinal}"}),), str(ordinal))


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
    collect_source(app, "feed", instant(0)); reconcile_node(app, "p", "summary", instant(0))
    tick(app, instant(3600))
    collect_source(app, "feed", instant(3700)); reconcile_node(app, "p", "summary", instant(3700))
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
    tick(app, instant(0)); tick(app, instant(600))
    assert app.conn.execute("SELECT COUNT(*) FROM executions WHERE kind='source'").fetchone()[0] == 2
    assert app.conn.execute("SELECT COUNT(*) FROM executions WHERE kind='stage'").fetchone()[0] == 0
    tick(app, instant(3600))
    assert app.conn.execute("SELECT COUNT(*) FROM executions WHERE kind='stage'").fetchone()[0] >= 1


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
    assert app.conn.execute(
        "SELECT state FROM executions WHERE id=?", (results["waiter"],)
    ).fetchone()[0] == "cached"


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
        except BaseException as error:
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
    assert isinstance(errors.get("waiter"), ExecutionError)


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
    assert app.conn.execute(
        "SELECT COUNT(*) FROM publication_members WHERE publication_id=?", (current,)
    ).fetchone()[0] == 0


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
        (SourceRecord("a", {"title": "A", "content": "A"}), SourceRecord("b", {"title": "B", "content": "B"})),
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


def test_reorder_duplicate_fallback_and_snapshot_omission_preserve_current_union(tmp_path):
    definition = tmp_path / "union.yml"
    write_union_definition(definition, "asc")
    first = open_app(definition, tmp_path / "state.db")
    tick(first, instant(0))
    assert [row[0] for row in first.conn.execute(
        "SELECT i.logical_id FROM node_members nm JOIN items i ON i.id=nm.item_id "
        "WHERE nm.pipeline_name='p' AND nm.node_name='documents' AND nm.state!='stale' ORDER BY nm.ordinal"
    )] == ["a", "b"]
    right_results = first.conn.execute("SELECT COUNT(*) FROM stage_results WHERE stage_name='desc'").fetchone()[0]
    first.conn.close()

    write_union_definition(definition, "empty")
    second = open_app(definition, tmp_path / "state.db")
    tick(second, instant(1))
    assert [row[0] for row in second.conn.execute(
        "SELECT i.logical_id FROM node_members nm JOIN items i ON i.id=nm.item_id "
        "WHERE nm.pipeline_name='p' AND nm.node_name='documents' AND nm.state!='stale' ORDER BY nm.ordinal"
    )] == ["b", "a"]
    empty_publication = second.conn.execute(
        "SELECT ns.current_publication_id FROM node_current_bindings nb JOIN node_state ns "
        "ON ns.pipeline_name=nb.pipeline_name AND ns.node_name=nb.node_name AND ns.binding_digest=nb.binding_digest "
        "WHERE nb.pipeline_name='p' AND nb.node_name='left'"
    ).fetchone()[0]
    assert second.conn.execute(
        "SELECT COUNT(*) FROM publication_members WHERE publication_id=?", (empty_publication,)
    ).fetchone()[0] == 0
    assert second.conn.execute("SELECT COUNT(*) FROM stage_results WHERE stage_name='desc'").fetchone()[0] == right_results


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
    members_before = [row[0] for row in first.conn.execute(
        "SELECT item_id FROM publication_members WHERE publication_id=? ORDER BY ordinal",
        (current_before,),
    )]
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
    members_after = [row[0] for row in second.conn.execute(
        "SELECT pm.item_id FROM publication_members pm "
        "WHERE pm.publication_id=? ORDER BY pm.ordinal", (current_after,)
    )]
    assert members_after == list(reversed(members_before))


def test_targeted_reconcile_activates_changed_source_binding_without_collection(tmp_path):
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
    assert first.conn.execute(
        "SELECT COUNT(*) FROM publications WHERE producer_key='feed' AND binding_digest=?",
        (old_binding,),
    ).fetchone()[0] >= 1
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
    assert changed.conn.execute(
        "SELECT COUNT(*) FROM source_state WHERE source_name='feed' AND binding_digest=?",
        (new_binding,),
    ).fetchone()[0] == 1
    assert changed.conn.execute(
        "SELECT COUNT(*) FROM publications WHERE producer_key='feed' AND binding_digest=?",
        (old_binding,),
    ).fetchone()[0] >= 1
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
    assert changed.conn.execute(
        "SELECT COUNT(*) FROM node_edge_members WHERE pipeline_name='p' "
        "AND node_name='documents' AND edge_index=0 AND active=1"
    ).fetchone()[0] == 0
    assert changed.conn.execute(
        "SELECT COUNT(*) FROM node_members WHERE pipeline_name='p' AND node_name='documents' "
        "AND state!='stale'"
    ).fetchone()[0] == 0


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
    assert app.conn.execute(
        "SELECT COUNT(*) FROM executions WHERE kind='source' AND state='succeeded'"
    ).fetchone()[0] == 1
    assert app.conn.execute(
        "SELECT next_due_at FROM source_state WHERE source_name='feed' AND binding_digest=?",
        (binding,),
    ).fetchone()[0] == "2026-08-14T01:00:01Z"
