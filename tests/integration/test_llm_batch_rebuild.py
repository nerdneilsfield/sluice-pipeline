from datetime import datetime, timedelta, timezone
import json

import pytest

from sluice.app import open_app
from sluice.contracts import SourceRecord, SourceResult, StageEnvelope
from sluice.errors import ExecutionError
from sluice.reconcile import reconcile_node, tick
from sluice.source_runtime import collect_source
from sluice.stage_runtime import execute_stage, rebuild_stage


class InvalidLLM:
    def __init__(self, responses):
        self.responses = list(responses)

    def complete(self, model, prompt, input_payload):
        return self.responses.pop(0)


def source_collect(config, checkpoint):
    if checkpoint is None:
        return SourceResult(
            (
                SourceRecord("item-a", {"content": "A"}),
                SourceRecord("item-b", {"content": "B"}),
            ),
            "done",
        )
    return SourceResult((SourceRecord("item-c", {"content": "C"}),), "done-2")


def sink_callable(config, payload, delivery_key):
    return None


def versioned_source(config, checkpoint):
    old = checkpoint is None
    score = 1 if old else "not-a-number"
    suffix = "old" if old else "new"
    records = tuple(
        SourceRecord(
            name,
            {
                "url": f"https://example.test/{name}",
                "content": suffix,
                "score": score,
            },
        )
        for name in ("a", "b")
    )
    return SourceResult(records, "v1" if old else "v2")


def instant(seconds: int) -> datetime:
    return datetime(2026, 8, 14, tzinfo=timezone.utc) + timedelta(seconds=seconds)


def llm_app(tmp_path, responses):
    definition = tmp_path / "definition.yml"
    definition.write_text(
        """
version: 1
database: state.db
sources: {feed: {use: tests.integration.test_llm_batch_rebuild.source_collect, with: {}}}
stages:
  extract:
    input: each
    tasks:
      - use: sluice.tasks.llm_extract
        with: {model: test, prompt: prompt.md, output: {title: string}, invalid: drop}
  summary:
    input: batch
    tasks: [{use: sluice.tasks.llm_summary, with: {model: test, prompt: prompt.md, output: {summary: string}}}]
pipelines:
  p:
    nodes:
      extract: {stage: extract, from: source.feed, retries: 1}
      summary: {stage: summary, from: source.feed, batch: {count: 2}}
      email: {from: summary, deliver: {use: tests.integration.test_llm_batch_rebuild.sink_callable, with: {target: test@example.test}}}
""",
        encoding="utf-8",
    )
    (tmp_path / "prompt.md").write_text("Return JSON.", encoding="utf-8")
    return open_app(definition, tmp_path / "state.db", llm_client=InvalidLLM(responses))


def failed_batch_app(tmp_path):
    app = llm_app(
        tmp_path,
        [{"bad": True}, {"summary": "rebuild-1"}, {"summary": "rebuild-2"}],
    )
    collect_source(app, "feed", instant(0))
    reconcile_node(app, "p", "summary", instant(0))
    batch_id = app.conn.execute(
        "SELECT current_frozen_batch_id FROM node_state "
        "WHERE pipeline_name='p' AND node_name='summary'"
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
        "summary",
        StageEnvelope(item_ids),
        pipeline_name="p",
        node_name="summary",
        generation=1,
        batch_id=batch_id,
        at=instant(0),
    )
    collect_source(app, "feed", instant(1))
    reconcile_node(app, "p", "summary", instant(1))
    return app, execution_id, batch_id, item_ids


def test_valid_structured_item_materializes_one_derived_output(tmp_path):
    app = llm_app(tmp_path, [{"title": "A"}])
    collect_source(app, "feed", instant(0))
    reconcile_node(app, "p", "extract", instant(0))
    item_id = app.conn.execute(
        "SELECT id FROM items WHERE logical_id='item-a'"
    ).fetchone()[0]
    execution_id = execute_stage(
        app,
        "extract",
        StageEnvelope((item_id,)),
        pipeline_name="p",
        node_name="extract",
        generation=1,
        batch_id=None,
        at=instant(0),
    )
    row = app.conn.execute(
        "SELECT e.state, i.payload_json FROM executions e "
        "JOIN stage_results sr ON sr.id=e.result_id "
        "JOIN stage_result_items sri ON sri.result_id=sr.id "
        "JOIN items i ON i.id=sri.item_id WHERE e.id=?",
        (execution_id,),
    ).fetchone()
    assert row[0] == "succeeded"
    assert json.loads(row[1]) == {"title": "A"}


def test_invalid_item_llm_exhaustion_is_cached_empty_dropped_result(tmp_path):
    app = llm_app(tmp_path, [{"wrong": True}, {"wrong": True}])
    collect_source(app, "feed", instant(0))
    reconcile_node(app, "p", "extract", instant(0))
    item_id = app.conn.execute(
        "SELECT id FROM items WHERE logical_id='item-a'"
    ).fetchone()[0]
    execution_id = execute_stage(
        app,
        "extract",
        StageEnvelope((item_id,)),
        pipeline_name="p",
        node_name="extract",
        generation=1,
        batch_id=None,
        at=instant(0),
    )
    assert app.conn.execute(
        "SELECT state FROM executions WHERE id=?", (execution_id,)
    ).fetchone()[0] == "dropped"
    assert app.conn.execute(
        "SELECT COUNT(*) FROM stage_result_items sri JOIN stage_results sr ON sr.id=sri.result_id "
        "WHERE sr.producer_execution_id=?",
        (execution_id,),
    ).fetchone()[0] == 0
    cached = execute_stage(
        app,
        "extract",
        StageEnvelope((item_id,)),
        pipeline_name="p",
        node_name="extract",
        generation=1,
        batch_id=None,
        at=instant(3),
    )
    assert app.conn.execute(
        "SELECT state FROM executions WHERE id=?", (cached,)
    ).fetchone()[0] == "cached"
    assert app.conn.execute(
        "SELECT COUNT(*) FROM stage_results WHERE stage_name='extract'"
    ).fetchone()[0] == 1


def test_invalid_batch_llm_output_stays_failed_and_frozen(tmp_path):
    app = llm_app(tmp_path, [{"bad": True}])
    collect_source(app, "feed", instant(0))
    reconcile_node(app, "p", "summary", instant(0))
    batch_id = app.conn.execute(
        "SELECT current_frozen_batch_id FROM node_state "
        "WHERE pipeline_name='p' AND node_name='summary'"
    ).fetchone()[0]
    item_ids = [
        row[0]
        for row in app.conn.execute(
            "SELECT item_id FROM batch_items WHERE batch_id=? ORDER BY ordinal",
            (batch_id,),
        )
    ]
    execution_id = execute_stage(
        app,
        "summary",
        StageEnvelope(tuple(item_ids)),
        pipeline_name="p",
        node_name="summary",
        generation=1,
        batch_id=batch_id,
        at=instant(0),
    )
    assert app.conn.execute(
        "SELECT state FROM executions WHERE id=?", (execution_id,)
    ).fetchone()[0] == "failed"
    assert app.conn.execute(
        "SELECT state FROM batches WHERE id=?", (batch_id,)
    ).fetchone()[0] == "failed"
    assert app.conn.execute(
        "SELECT COUNT(*) FROM stage_results WHERE producer_execution_id=?",
        (execution_id,),
    ).fetchone()[0] == 0
    assert [
        row[0]
        for row in app.conn.execute(
            "SELECT item_id FROM batch_items WHERE batch_id=? ORDER BY ordinal",
            (batch_id,),
        )
    ] == item_ids


def test_rebuild_current_failed_batch_uses_exact_history_and_clears_only_current_freeze(tmp_path):
    app, target, batch_id, item_ids = failed_batch_app(tmp_path)
    result = rebuild_stage(app, target, instant(10))
    assert result.forced is True
    assert result.execution_id is not None
    rebuilt = result.execution_id
    assert [
        row[0]
        for row in app.conn.execute(
            "SELECT item_id FROM execution_inputs "
            "WHERE execution_id=? AND slot='items' ORDER BY ordinal",
            (rebuilt,),
        )
    ] == list(item_ids)
    assert (
        app.conn.execute(
            "SELECT COUNT(*) FROM execution_inputs "
            "WHERE execution_id=? AND slot='history'",
            (rebuilt,),
        ).fetchone()[0]
        == 0
    )
    assert (
        app.conn.execute(
            "SELECT current_frozen_batch_id FROM node_state "
            "WHERE pipeline_name='p' AND node_name='summary'"
        ).fetchone()[0]
        is None
    )
    assert [
        row[0]
        for row in app.conn.execute(
            "SELECT i.logical_id FROM node_members nm JOIN items i ON i.id=nm.item_id "
            "WHERE nm.pipeline_name='p' AND nm.node_name='summary' AND nm.state='pending' "
            "ORDER BY nm.ordinal"
        )
    ] == ["item-c"]


def test_historical_rebuild_publishes_shared_history_but_does_not_touch_current_scope(tmp_path):
    app, target, _original_batch_id, _ = failed_batch_app(tmp_path)
    rebuild_stage(app, target, instant(1))
    collect_source(app, "feed", instant(2))
    reconcile_node(app, "p", "summary", instant(2))
    current_before = app.conn.execute(
        "SELECT current_frozen_batch_id FROM node_state "
        "WHERE pipeline_name='p' AND node_name='summary'"
    ).fetchone()[0]
    rebuilt = rebuild_stage(app, target, instant(10)).execution_id
    assert rebuilt is not None
    assert (
        app.conn.execute("SELECT state FROM executions WHERE id=?", (rebuilt,)).fetchone()[0]
        in {"succeeded", "cached"}
    )
    assert (
        app.conn.execute(
            "SELECT target_scope_excluded FROM executions WHERE id=?", (rebuilt,)
        ).fetchone()[0]
        == 1
    )
    assert (
        app.conn.execute(
            "SELECT current_frozen_batch_id FROM node_state "
            "WHERE pipeline_name='p' AND node_name='summary'"
        ).fetchone()[0]
        == current_before
    )
    assert [
        row[0]
        for row in app.conn.execute(
            "SELECT i.logical_id FROM node_members nm JOIN items i ON i.id=nm.item_id "
            "WHERE nm.pipeline_name='p' AND nm.node_name='summary' AND nm.state='pending' "
            "ORDER BY nm.ordinal"
        )
    ] == ["item-c"]
    tick(app, instant(11))
    assert (
        app.conn.execute(
            "SELECT current_frozen_batch_id FROM node_state "
            "WHERE pipeline_name='p' AND node_name='summary'"
        ).fetchone()[0]
        == current_before
    )


def test_rebuild_rejects_changed_retained_prompt_file(tmp_path):
    app, target, _batch_id, _item_ids = failed_batch_app(tmp_path)
    (tmp_path / "prompt.md").write_text(
        "Changed after the retained snapshot.", encoding="utf-8"
    )
    with pytest.raises(ExecutionError):
        rebuild_stage(app, target, instant(10))


def test_retry_and_rebuild_preserve_non_empty_dedup_history_ids(tmp_path):
    definition = tmp_path / "history.yml"
    definition.write_text(
        """
version: 1
database: state.db
sources: {feed: {use: tests.integration.test_llm_batch_rebuild.versioned_source, with: {}}}
stages:
  select:
    input: batch
    tasks:
      - {use: sluice.tasks.dedup, with: {keys: [url]}}
      - use: sluice.tasks.filter
        with: {all: [{field: score, op: gte, value: 0}]}
pipelines:
  p:
    nodes:
      select: {stage: select, from: source.feed, retries: 1, batch: {count: 2, include_updates: true}}
      email: {from: select, deliver: {use: tests.integration.test_llm_batch_rebuild.sink_callable, with: {}}}
""",
        encoding="utf-8",
    )
    app = open_app(definition, tmp_path / "state.db")
    collect_source(app, "feed", instant(0))
    reconcile_node(app, "p", "select", instant(0))
    first_batch = app.conn.execute(
        "SELECT current_frozen_batch_id FROM node_state "
        "WHERE pipeline_name='p' AND node_name='select'"
    ).fetchone()[0]
    first_items = tuple(
        row[0]
        for row in app.conn.execute(
            "SELECT item_id FROM batch_items WHERE batch_id=? ORDER BY ordinal",
            (first_batch,),
        )
    )
    first = execute_stage(
        app,
        "select",
        StageEnvelope(first_items),
        pipeline_name="p",
        node_name="select",
        generation=1,
        batch_id=first_batch,
        at=instant(0),
    )
    assert app.conn.execute("SELECT state FROM executions WHERE id=?", (first,)).fetchone()[0] == "succeeded"

    collect_source(app, "feed", instant(1))
    reconcile_node(app, "p", "select", instant(1))
    second_batch = app.conn.execute(
        "SELECT current_frozen_batch_id FROM node_state "
        "WHERE pipeline_name='p' AND node_name='select'"
    ).fetchone()[0]
    second_items = tuple(
        row[0]
        for row in app.conn.execute(
            "SELECT item_id FROM batch_items WHERE batch_id=? ORDER BY ordinal",
            (second_batch,),
        )
    )
    failed = execute_stage(
        app,
        "select",
        StageEnvelope(second_items),
        pipeline_name="p",
        node_name="select",
        generation=2,
        batch_id=second_batch,
        at=instant(1),
    )
    history_ids = tuple(
        row[0]
        for row in app.conn.execute(
            "SELECT item_id FROM execution_inputs WHERE execution_id=? "
            "AND slot='history' ORDER BY ordinal",
            (failed,),
        )
    )
    assert history_ids
    parent = app.conn.execute(
        "SELECT retry_parent_id FROM executions WHERE id=?", (failed,)
    ).fetchone()[0]
    assert parent is not None
    assert tuple(
        row[0]
        for row in app.conn.execute(
            "SELECT item_id FROM execution_inputs WHERE execution_id=? "
            "AND slot='history' ORDER BY ordinal",
            (parent,),
        )
    ) == history_ids
    rebuilt = rebuild_stage(app, failed, instant(2)).execution_id
    assert rebuilt is not None
    assert tuple(
        row[0]
        for row in app.conn.execute(
            "SELECT item_id FROM execution_inputs WHERE execution_id=? "
            "AND slot='history' ORDER BY ordinal",
            (rebuilt,),
        )
    ) == history_ids
