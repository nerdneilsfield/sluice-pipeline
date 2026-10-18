from datetime import UTC, datetime
from threading import Event, Thread
from urllib.parse import quote

import pytest

from sluice.app import open_app
from sluice.contracts import SourceRecord, SourceResult
from sluice.read_model import default_rebuild_target, pipeline_detail, pipeline_list
from sluice.reconcile import tick
from sluice.ui import create_ui


def management_source(config, checkpoint):
    # ``to_markdown`` consumes the canonical ``content`` field; ``title`` is
    # retained so the fixture remains representative of a document payload.
    return SourceResult(
        (SourceRecord("logical-a", {"title": "A", "content": "<p>A</p>"}),),
        "done",
    )


class ManagementLLM:
    def complete(self, model, prompt, input_payload):
        return {"invalid": True}


def management_sink(config, payload, delivery_key):
    raise RuntimeError("delivery unavailable")


def management_app(tmp_path):
    definition = tmp_path / "definition.yml"
    definition.write_text(
        """
version: 1
database: state.db
sources: {feed: {use: tests.smoke.test_management.management_source, with: {}}}
stages:
  pass: {input: each, tasks: [{use: sluice.tasks.to_markdown}]}
  drop:
    input: each
    tasks:
      - {use: sluice.tasks.llm_extract, with: {model: test, prompt: prompt.md, output: {title: string}, invalid: drop}}
  waiting: {input: each, tasks: [{use: sluice.tasks.to_markdown}]}
  failed_summary:
    input: batch
    tasks: [{use: sluice.tasks.llm_summary, with: {model: test, prompt: prompt.md, output: {summary: string}}}]
pipelines:
  daily_digest:
    nodes:
      pass: {stage: pass, from: source.feed}
      dropped: {stage: drop, from: source.feed}
      waiting: {stage: waiting, from: source.feed, schedule: 1h}
      failed_summary: {stage: failed_summary, from: source.feed, batch: {count: 1}}
      email: {from: pass, retries: 1, deliver: {use: tests.smoke.test_management.management_sink, with: {target: test@example.test}}}
""",
        encoding="utf-8",
    )
    (tmp_path / "prompt.md").write_text("Return a title object.", encoding="utf-8")
    app = open_app(definition, tmp_path / "state.db", llm_client=ManagementLLM())
    tick(app, datetime(2026, 8, 14, tzinfo=UTC))
    return app


def test_management_read_model_states_counts_and_actions(tmp_path):
    app = management_app(tmp_path)
    summaries = pipeline_list(app)
    summary = next(value for value in summaries if value.name == "daily_digest")
    assert summary.state == "failed"
    # The per-item invalid result is an authorized ``dropped`` materialization;
    # summary and the exhausted Delivery retry are the two current failures.
    assert summary.failed_count == 2
    assert summary.pending_count == 1
    detail = pipeline_detail(app, "daily_digest")
    pass_node = next(node for node in detail.nodes if node.name == "pass")
    client = create_ui(app, port=80).test_client()
    listing = client.get("/pipelines")
    detail_page = client.get("/pipelines/daily_digest")
    assert listing.status_code == detail_page.status_code == 200
    assert listing.content_type.startswith("text/html")
    assert detail_page.content_type.startswith("text/html")
    failed_target = default_rebuild_target(app, "daily_digest", "failed_summary")
    assert failed_target is not None
    assert client.post(
        "/pipelines/daily_digest/nodes/failed_summary/rebuild"
    ).status_code in {
        202,
        303,
    }
    execution_id = pass_node.executions[0]
    delivery_key = next(iter(detail.deliveries))
    assert client.post(f"/executions/{execution_id}/rebuild").status_code in {202, 303}
    assert client.post(
        f"/deliveries/{quote(delivery_key, safe='')}/retry"
    ).status_code in {202, 303}


@pytest.mark.parametrize(
    "path",
    [
        "/pipelines/daily_digest/nodes/failed_summary/rebuild",
        "/executions/1/rebuild",
        "/deliveries/missing/retry",
    ],
)
def test_ui_actions_are_refused_after_app_close(tmp_path, path):
    app = management_app(tmp_path)
    client = create_ui(app, port=80).test_client()
    app.close()

    assert client.post(path).status_code == 404


def test_ui_action_holds_app_admission_so_close_waits_for_it(tmp_path, monkeypatch):
    app = management_app(tmp_path)
    client = create_ui(app, port=80).test_client()
    admitted = []
    entered = Event()
    release = Event()

    def blocked_retry(app_, delivery_key, at):
        admitted.append(app._admission.active)
        entered.set()
        assert release.wait(timeout=5)
        return delivery_key

    monkeypatch.setattr("sluice.ui.retry_delivery_action", blocked_retry)
    request = Thread(target=lambda: client.post("/deliveries/k/retry"))
    request.start()
    try:
        assert entered.wait(timeout=5)
        assert admitted == [1]
    finally:
        release.set()
        request.join(timeout=5)
    app.close()


def test_ui_refuses_cross_site_and_foreign_host_requests(tmp_path):
    app = management_app(tmp_path)
    client = create_ui(app, port=80).test_client()
    path = "/executions/1/rebuild"

    for headers in (
        {"Sec-Fetch-Site": "cross-site"},
        {"Sec-Fetch-Site": "same-site"},
        {"Origin": "http://evil.example"},
        {"Origin": "null"},
    ):
        assert client.post(path, headers=headers).status_code == 403
    assert client.get("/pipelines", headers={"Host": "evil.example"}).status_code == 403
    assert client.get("/pipelines", headers={"Host": "localhost:9"}).status_code == 403

    for headers in (
        {},
        {"Sec-Fetch-Site": "same-origin", "Origin": "http://localhost"},
        {"Sec-Fetch-Site": "none"},
    ):
        assert client.post(path, headers=headers).status_code != 403
    assert client.get("/pipelines", headers={"Host": "[::1]"}).status_code == 200


def test_ui_bound_to_non_loopback_host_allows_only_that_host(tmp_path):
    app = management_app(tmp_path)
    client = create_ui(app, host="ui.internal", port=8000).test_client()

    assert (
        client.get("/pipelines", headers={"Host": "ui.internal:8000"}).status_code
        == 200
    )
    assert (
        client.get("/pipelines", headers={"Host": "localhost:8000"}).status_code == 403
    )
