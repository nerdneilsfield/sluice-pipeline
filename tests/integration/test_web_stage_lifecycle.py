"""Durable lifecycle tests for web Stage attempts."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from sluice.app import open_app
from sluice.contracts import SourceRecord, SourceResult
from sluice.reconcile import tick
from sluice.stage_runtime import terminal_fetch_drop_eligible
from sluice.web.contracts import FetchChainExhausted


def instant(seconds: int) -> datetime:
    return datetime(2026, 8, 19, tzinfo=timezone.utc) + timedelta(seconds=seconds)


def web_source(config, checkpoint):
    del config, checkpoint
    return SourceResult((SourceRecord("article", {"url": "https://example.test/article"}),), "done")


def web_sink(config, payload, delivery_key):
    del config, payload, delivery_key
    return None


class FailingClient:
    def __init__(self, failure: BaseException) -> None:
        self.failure = failure
        self.calls = 0

    def resolve(self, request, fetch_chain, extract_chain):
        del request, fetch_chain, extract_chain
        self.calls += 1
        raise self.failure

    def close(self) -> None:
        pass


def write_definition(path: Path, *, retries: int, tasks: str) -> None:
    path.write_text(
        f"""version: 1
database: state.db
web:
  providers: {{direct: {{type: startup_http, trusted: true}}}}
  fetch_chains: {{robust: {{providers: [direct]}}}}
  extractors: {{identity: {{type: identity}}}}
  extract_chains: {{article: {{extractors: [identity]}}}}
sources:
  feed: {{use: tests.integration.test_web_stage_lifecycle.web_source, with: {{}}}}
stages:
  web:
    input: each
    tasks: {tasks}
pipelines:
  p:
    nodes:
      web: {{stage: web, from: source.feed, retries: {retries}}}
      sink: {{from: web, deliver: {{use: tests.integration.test_web_stage_lifecycle.web_sink, with: {{}}}}}}
""",
        encoding="utf-8",
    )


def normal_tasks() -> str:
    return """\n      - use: sluice.tasks.fetch_webpage
        with: {fetch_chain: robust, extract_chain: article, min_chars: 1}
      - use: sluice.tasks.to_text"""


def test_fetch_exhaustion_retries_then_publishes_empty_success(tmp_path: Path) -> None:
    definition = tmp_path / "definition.yml"
    write_definition(definition, retries=1, tasks=normal_tasks())
    client = FailingClient(FetchChainExhausted(()))
    app = open_app(
        definition,
        tmp_path / "state.db",
        http_client=type("HTTP", (), {"get": lambda self, url: "unused"})(),
        web_content_client=client,
    )
    try:
        tick(app, instant(0))
        rows = app.conn.execute(
            "SELECT state,error,retry_count FROM executions WHERE kind='stage' ORDER BY id"
        ).fetchall()
        assert [(row[0], row[2]) for row in rows] == [("failed", 0), ("succeeded", 1)]
        assert rows[-1][1] is None
        assert app.conn.execute("SELECT COUNT(*) FROM stage_result_current").fetchone()[0] == 1
        assert client.calls == 2
    finally:
        app.close()


def test_terminal_fetch_drop_eligibility_rejects_failed_index(tmp_path: Path) -> None:
    definition = tmp_path / "definition.yml"
    write_definition(definition, retries=0, tasks=normal_tasks())
    from sluice.config import load_definition

    stage = load_definition(definition).stages["web"]
    assert terminal_fetch_drop_eligible(stage, failed_task_index=1) is False


def test_web_error_is_sanitized_before_persistence(tmp_path: Path) -> None:
    definition = tmp_path / "definition.yml"
    write_definition(definition, retries=0, tasks=normal_tasks())
    client = FailingClient(RuntimeError("Authorization: env:KEY body=private"))
    app = open_app(
        definition,
        tmp_path / "state.db",
        http_client=type("HTTP", (), {"get": lambda self, url: "unused"})(),
        web_content_client=client,
    )
    try:
        tick(app, instant(0))
        error = app.conn.execute(
            "SELECT error FROM executions WHERE kind='stage' ORDER BY id DESC LIMIT 1"
        ).fetchone()[0]
        assert error == "provider_error"
        assert "private" not in error and "KEY" not in error
    finally:
        app.close()
