from datetime import datetime, timezone

from sluice.app import open_app
from sluice.contracts import SourceRecord, SourceResult
from sluice.reconcile import tick


def rss_source(config, checkpoint):
    return SourceResult((SourceRecord("rss-a", {"title": "RSS A"}),), "rss-done")


def site_source(config, checkpoint):
    return SourceResult((SourceRecord("site-b", {"title": "Site B"}),), "site-done")


class AcceptanceLLM:
    def complete(self, model, prompt, input_payload):
        return {"summary": "RSS A; Site B"}


def sink_ok(config, payload, delivery_key):
    return "sent"


def acceptance_app(tmp_path):
    definition = tmp_path / "definition.yml"
    definition.write_text(
        """
version: 1
database: state.db
sources:
  rss: {use: tests.smoke.test_acceptance.rss_source, with: {}}
  site: {use: tests.smoke.test_acceptance.site_source, with: {}}
stages:
  candidates: {input: batch, tasks: [{use: sluice.tasks.dedup, with: {keys: [title]}}]}
  summary:
    input: batch
    tasks: [{use: sluice.tasks.llm_summary, with: {model: test, prompt: prompt.md, output: {summary: string}}}]
  email_render: {input: each, tasks: [{use: sluice.tasks.render_email}]}
  telegram_render: {input: each, tasks: [{use: sluice.tasks.render_telegram}]}
pipelines:
  daily_digest:
    nodes:
      candidates: {stage: candidates, from: [source.rss, source.site], batch: {count: 2}}
      summary: {stage: summary, from: candidates, batch: {count: 2}}
      email_message: {stage: email_render, from: summary}
      telegram_message: {stage: telegram_render, from: summary}
      email: {from: email_message, deliver: {use: tests.smoke.test_acceptance.sink_ok, with: {target: email}}}
      telegram: {from: telegram_message, deliver: {use: tests.smoke.test_acceptance.sink_ok, with: {target: telegram}}}
""",
        encoding="utf-8",
    )
    (tmp_path / "prompt.md").write_text("Summarize the ordered items.", encoding="utf-8")
    return open_app(definition, tmp_path / "state.db", llm_client=AcceptanceLLM())


def test_branching_merging_sources_shared_summary_and_two_deliveries(tmp_path):
    app = acceptance_app(tmp_path)
    tick(app, datetime(2026, 8, 14, tzinfo=timezone.utc))
    tick(app, datetime(2026, 8, 14, 1, tzinfo=timezone.utc))
    logical_ids = [
        row[0]
        for row in app.conn.execute(
            "SELECT i.logical_id FROM node_members nm JOIN items i ON i.id=nm.item_id "
            "WHERE nm.pipeline_name='daily_digest' AND nm.node_name='candidates' ORDER BY nm.ordinal"
        )
    ]
    assert logical_ids == ["rss-a", "site-b"]
    assert (
        app.conn.execute("SELECT COUNT(*) FROM stage_results WHERE stage_name='summary'").fetchone()[0]
        == 1
    )
    assert {
        row[0]: row[1]
        for row in app.conn.execute("SELECT node_name,state FROM deliveries")
    } == {"email": "succeeded", "telegram": "succeeded"}
