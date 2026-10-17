"""YAML-configured LLM client: startup wiring, availability rules, secrecy."""

import json
import sqlite3
import threading
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from sluice import cli
from sluice.app import open_app
from sluice.canonical import node_binding_digest, stage_lookup_key
from sluice.config import load_definition
from sluice.contracts import Item, SourceRecord, SourceResult
from sluice.errors import DefinitionError
from sluice.reconcile import tick
from sluice.stage_runtime import rebuild_stage

SINK = "tests.integration.test_llm_runtime_wiring.sink_noop"
SOURCE = "tests.integration.test_llm_runtime_wiring.source_collect"
SENTINEL = "SENTINEL-vendor-body-7f3a"
AT = "2026-08-14T00:00:00Z"
KEY_ENV = "LLM_TEST_KEY"


def source_collect(config, checkpoint):
    if checkpoint is None:
        return SourceResult((SourceRecord("item-a", {"content": "A"}),), "done")
    return SourceResult((), "done")


def sink_noop(config, payload, delivery_key):
    return None


LLM_STAGES = f"""
sources: {{feed: {{use: {SOURCE}, with: {{}}}}}}
stages:
  extract:
    input: each
    tasks:
      - use: sluice.tasks.llm_extract
        with: {{model: m-extract, prompt: prompt.md, output: {{summary: string}}, invalid: drop}}
  summary:
    input: batch
    tasks:
      - use: sluice.tasks.llm_summary
        with: {{model: m-summary, prompt: prompt.md, output: {{summary: string}}}}
  message:
    input: each
    tasks: [{{use: sluice.tasks.render_telegram, with: {{template: "{{summary}}"}}}}]
pipelines:
  p:
    nodes:
      extract: {{stage: extract, from: source.feed, retries: 1}}
      summary: {{stage: summary, from: extract, batch: {{count: 1}}}}
      message: {{stage: message, from: summary}}
      out: {{from: message, deliver: {{use: {SINK}, with: {{}}}}}}
"""

PLAIN_STAGES = f"""
sources: {{feed: {{use: {SOURCE}, with: {{}}}}}}
stages:
  message:
    input: each
    tasks: [{{use: sluice.tasks.render_telegram, with: {{template: "{{content}}"}}}}]
pipelines:
  p:
    nodes:
      message: {{stage: message, from: source.feed}}
      out: {{from: message, deliver: {{use: {SINK}, with: {{}}}}}}
"""


def write_definition(
    tmp_path, stages=LLM_STAGES, llm_lines=None, name="definition.yml"
):
    text = "version: 1\ndatabase: state.db\n"
    if llm_lines is not None:
        text += "llm:\n" + "".join(f"  {line}\n" for line in llm_lines)
    path = tmp_path / name
    path.write_text(text + stages, encoding="utf-8")
    (tmp_path / "prompt.md").write_text("Return bare JSON.", encoding="utf-8")
    return path


class FakeLLM:
    """Injected client used to build retained history without HTTP."""

    def complete(self, model, prompt, input_payload):
        return {"summary": "final digest" if model == "m-summary" else "extracted"}


class FakeServer:
    def __init__(self):
        self.requests = []
        self.status = 200
        self.error_body = b""
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                outer.requests.append(
                    (self.path, self.headers.get("Authorization"), body)
                )
                if outer.status != 200:
                    data = outer.error_body
                else:
                    summary = "final digest" if body["model"] == "m-summary" else "x"
                    data = json.dumps(
                        {
                            "choices": [
                                {
                                    "message": {
                                        "content": json.dumps({"summary": summary})
                                    }
                                }
                            ]
                        }
                    ).encode()
                self.send_response(outer.status)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, *args):
                return None

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.httpd.server_address[1]}/v1"
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()


@pytest.fixture
def server(monkeypatch):
    monkeypatch.setenv("NO_PROXY", "*")
    monkeypatch.setenv("no_proxy", "*")
    fake = FakeServer()
    yield fake
    fake.httpd.shutdown()
    fake.httpd.server_close()


def llm_lines(url):
    return [f"base_url: {url}", f"api_key: env:{KEY_ENV}", "max_attempts: 1"]


def query(tmp_path, sql, params=()):
    conn = sqlite3.connect(tmp_path / "state.db")
    try:
        return conn.execute(sql, params).fetchall()
    finally:
        conn.close()


def test_tick_runs_llm_stages_from_yaml_against_configured_endpoint(
    tmp_path, server, monkeypatch
):
    monkeypatch.setenv(KEY_ENV, "key-from-env")
    definition = write_definition(tmp_path, llm_lines=llm_lines(server.url))

    assert cli.main(["tick", str(definition), "--at", AT]) == 0

    states = query(
        tmp_path,
        "SELECT stage_name, state FROM executions WHERE kind='stage' "
        "AND stage_name IN ('extract','summary')",
    )
    assert sorted(states) == [("extract", "succeeded"), ("summary", "succeeded")]
    payloads = query(
        tmp_path,
        "SELECT i.payload_json FROM deliveries d "
        "JOIN items i ON i.id=d.rendered_item_id WHERE d.state='succeeded'",
    )
    assert [json.loads(row[0]) for row in payloads] == ["final digest"]
    assert [(path, auth, body["model"]) for path, auth, body in server.requests] == [
        ("/v1/chat/completions", "Bearer key-from-env", "m-extract"),
        ("/v1/chat/completions", "Bearer key-from-env", "m-summary"),
    ]


def test_vendor_error_body_is_not_persisted_in_execution_error(
    tmp_path, server, monkeypatch
):
    monkeypatch.setenv(KEY_ENV, "key-from-env")
    server.status = 400
    server.error_body = SENTINEL.encode()
    definition = write_definition(tmp_path, llm_lines=llm_lines(server.url))

    assert cli.main(["tick", str(definition), "--at", AT]) == 0

    rows = query(
        tmp_path,
        "SELECT state, error FROM executions WHERE kind='stage' AND stage_name='extract'",
    )
    assert rows
    assert {state for state, _ in rows} == {"failed"}
    assert all("400" in error and SENTINEL not in error for _, error in rows)


def test_current_llm_stage_without_client_fails_before_database_is_created(tmp_path):
    definition = write_definition(tmp_path)
    with pytest.raises(DefinitionError):
        open_app(definition, tmp_path / "state.db")
    assert not (tmp_path / "state.db").exists()


@pytest.mark.parametrize(
    "lines",
    [
        ["base_url: http://h/v1", "api_key: sk-literal"],
        ["base_url: http://h/v1", "model: x"],
        ["base_url: http://user:pw@h/v1"],
        ["base_url: http://h/v1?token=1"],
        ["base_url: http://h/v1", "timeout_seconds: true"],
        ["base_url: http://h/v1", "timeout_seconds: 0"],
        ["base_url: http://h/v1", "max_attempts: 0"],
    ],
)
def test_load_definition_rejects_invalid_llm_block(tmp_path, lines):
    definition = write_definition(tmp_path, llm_lines=lines)
    with pytest.raises(DefinitionError):
        load_definition(definition)


def test_unset_api_key_env_fails_open_app_even_without_llm_stage(tmp_path, monkeypatch):
    monkeypatch.delenv(KEY_ENV, raising=False)
    definition = write_definition(
        tmp_path, PLAIN_STAGES, llm_lines=llm_lines("http://127.0.0.1:1/v1")
    )
    with pytest.raises(DefinitionError):
        open_app(definition, tmp_path / "state.db")


def test_injected_client_wins_over_unresolvable_llm_block(tmp_path, monkeypatch):
    monkeypatch.delenv(KEY_ENV, raising=False)
    definition = write_definition(
        tmp_path, llm_lines=llm_lines("http://127.0.0.1:1/v1")
    )
    app = open_app(definition, tmp_path / "state.db", llm_client=FakeLLM())
    try:
        assert app.llm_available
        tick(app, datetime(2026, 8, 14, tzinfo=UTC))
        states = app.conn.execute(
            "SELECT state FROM executions WHERE kind='stage' "
            "AND stage_name IN ('extract','summary')"
        ).fetchall()
        assert [row[0] for row in states] == ["succeeded", "succeeded"]
    finally:
        app.close()


def retained_history(tmp_path):
    """Run the LLM definition with an injected client, then close it."""

    definition = write_definition(tmp_path)
    app = open_app(definition, tmp_path / "state.db", llm_client=FakeLLM())
    try:
        tick(app, datetime(2026, 8, 14, tzinfo=UTC))
    finally:
        app.close()
    return write_definition(tmp_path, PLAIN_STAGES, name="plain.yml")


def test_terminal_llm_history_opens_without_client_but_rebuild_is_rejected(tmp_path):
    plain = retained_history(tmp_path)
    app = open_app(plain, tmp_path / "state.db")
    try:
        assert not app.llm_available
        target = app.conn.execute(
            "SELECT id FROM executions WHERE kind='stage' AND stage_name='extract'"
        ).fetchone()[0]
        before = app.conn.execute("SELECT COUNT(*) FROM executions").fetchone()[0]
        with pytest.raises(DefinitionError):
            rebuild_stage(app, target, datetime(2026, 8, 15, tzinfo=UTC))
        after = app.conn.execute("SELECT COUNT(*) FROM executions").fetchone()[0]
        assert after == before
    finally:
        app.close()


@pytest.mark.parametrize("state", ["queued", "running"])
def test_executable_llm_snapshot_without_client_fails_at_open(tmp_path, state):
    plain = retained_history(tmp_path)
    conn = sqlite3.connect(tmp_path / "state.db")
    conn.execute(
        "UPDATE executions SET state=? WHERE kind='stage' AND stage_name='extract'",
        (state,),
    )
    conn.commit()
    conn.close()
    with pytest.raises(DefinitionError):
        open_app(plain, tmp_path / "state.db")


def test_waiter_only_llm_snapshot_without_client_fails_at_open(tmp_path):
    plain = retained_history(tmp_path)
    conn = sqlite3.connect(tmp_path / "state.db")
    config_json = conn.execute(
        "SELECT config_json FROM executions WHERE kind='stage' AND stage_name='extract'"
    ).fetchone()[0]
    claim = conn.execute(
        "INSERT INTO materialization_claims(stage_name, lookup_key) VALUES('extract','k')"
    ).lastrowid
    conn.execute(
        "INSERT INTO materialization_waiters(stage_name, lookup_key, claim_id, "
        "waiting_sequence, force, envelope_json, policy_json, config_json) "
        "VALUES('extract','k',?,1,0,'{}','{}',?)",
        (claim, config_json),
    )
    conn.commit()
    conn.close()
    with pytest.raises(DefinitionError):
        open_app(plain, tmp_path / "state.db")


def test_endpoint_is_not_part_of_stage_identity(tmp_path, monkeypatch):
    item = Item(
        id="i1",
        producer_scope=("source", "feed"),
        logical_id="l1",
        payload={"content": "A"},
        metadata={},
        content_hash="h",
        observed_at="2026-08-14T00:00:00Z",
        produced_by=1,
    )

    def identity(name, url, secret):
        monkeypatch.setenv(KEY_ENV, secret)
        definition = load_definition(
            write_definition(tmp_path, llm_lines=llm_lines(url), name=name)
        )
        result = {}
        for pipeline in definition.pipelines.values():
            for node in pipeline.nodes.values():
                if node.stage is None:
                    continue
                stage = definition.stages[node.stage]
                result[node.name] = (
                    stage.canonical_config_json,
                    node_binding_digest(
                        pipeline.name,
                        node.name,
                        stage,
                        node.from_refs,
                        node.schedule,
                        node.batch,
                    ),
                    stage_lookup_key(stage, [item], []),
                )
        return result

    assert identity("a.yml", "http://one.test/v1", "k1") == identity(
        "b.yml", "https://two.test/v2", "k2"
    )
