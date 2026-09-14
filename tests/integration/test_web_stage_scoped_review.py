"""Scoped regressions for durable web Stage behavior."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from sluice.app import open_app
from sluice.contracts import SourceRecord, SourceResult, StageDef, TaskDef
from sluice.read_model import pipeline_list
from sluice.reconcile import tick
from sluice.stage_runtime import terminal_fetch_drop_eligible
from sluice.web.client import WebContentClient
from sluice.web.providers import FirecrawlProvider
from sluice.web.registry import resolve_provider_binding


def instant(seconds: int = 0) -> datetime:
    return datetime(2026, 8, 19, 0, 0, seconds, tzinfo=UTC)


def source(config, checkpoint):
    del config, checkpoint
    return SourceResult(
        (SourceRecord("article", {"url": "https://example.test/article"}),), "done"
    )


def sink(config, payload, delivery_key):
    del config, payload, delivery_key


def write_web(
    path: Path,
    *,
    timeout: str | None = None,
    retries: int = 0,
    providers: str = "direct: {type: startup_http, trusted: true}",
    chain: str = "[direct]",
) -> None:
    timeout_field = "" if timeout is None else f", timeout: {timeout}"
    path.write_text(
        f"""version: 1
database: state.db
web:
  providers: {{{providers}}}
  fetch_chains: {{robust: {{providers: {chain}}}}}
  extractors: {{identity: {{type: identity}}}}
  extract_chains: {{article: {{extractors: [identity]}}}}
sources: {{feed: {{use: tests.integration.test_web_stage_scoped_review.source, with: {{}}}}}}
stages:
  web:
    input: each
    tasks:
      - use: sluice.tasks.fetch_webpage
        with: {{fetch_chain: robust, extract_chain: article, min_chars: 1}}
      - use: sluice.tasks.to_text
pipelines:
  p:
    nodes:
      web: {{stage: web, from: source.feed, retries: {retries}{timeout_field}}}
      out: {{from: web, deliver: {{use: tests.integration.test_web_stage_scoped_review.sink, with: {{}}}}}}
""",
        encoding="utf-8",
    )


def _stage(*uses: str, input_mode: str = "each") -> StageDef:
    return StageDef(
        "shape", input_mode, tuple(TaskDef(use, {}) for use in uses), None, 1, "{}"
    )


@pytest.mark.parametrize(
    ("stage", "failed_task_index"),
    [
        (
            _stage(
                "sluice.tasks.fetch_webpage", "sluice.tasks.to_text", input_mode="batch"
            ),
            0,
        ),
        (
            _stage(
                "custom.before", "sluice.tasks.fetch_webpage", "sluice.tasks.to_text"
            ),
            1,
        ),
        (
            _stage(
                "sluice.tasks.fetch_webpage", "custom.after", "sluice.tasks.to_text"
            ),
            0,
        ),
        (
            _stage(
                "sluice.tasks.fetch_webpage",
                "sluice.tasks.extract_content",
                "sluice.tasks.extract_content",
                "sluice.tasks.to_text",
            ),
            0,
        ),
        (_stage("sluice.tasks.fetch_webpage"), 0),
        (
            _stage(
                "sluice.tasks.fetch_webpage",
                "sluice.tasks.to_text",
                "sluice.tasks.to_markdown",
            ),
            0,
        ),
        (_stage("custom.failure"), 0),
        (_stage("sluice.tasks.extract_content", "sluice.tasks.to_text"), 0),
    ],
)
def test_terminal_drop_helper_rejects_negative_shapes(
    stage: StageDef, failed_task_index: int
) -> None:
    assert terminal_fetch_drop_eligible(stage, failed_task_index) is False


def test_production_chain_deadline_stops_second_provider_and_fails(
    tmp_path: Path, monkeypatch
) -> None:
    definition = tmp_path / "definition.yml"
    write_web(
        definition,
        timeout="1s",
        providers="first: {type: startup_http, trusted: true}, second: {type: startup_http, trusted: true}",
        chain="[first, second]",
    )
    calls: list[str] = []

    def transport(url: str) -> str:
        calls.append(url)
        monkeypatch.setattr("sluice.web.client.time.monotonic", lambda: 2.0)
        return "short body"

    monkeypatch.setattr("sluice.stage_runtime.time.monotonic", lambda: 1.0)
    app = open_app(definition, tmp_path / "state.db", http_client=transport)
    try:
        tick(app, instant())
        assert calls == ["https://example.test/article"]
        rows = app.conn.execute(
            "SELECT state,retry_count FROM executions WHERE kind='stage' ORDER BY id"
        ).fetchall()
        assert [(row[0], row[1]) for row in rows] == [("failed", 0)]
        assert (
            app.conn.execute("SELECT COUNT(*) FROM stage_result_current").fetchone()[0]
            == 0
        )
    finally:
        app.close()


class _BatchFailure:
    input_mode = "batch"

    def __call__(self, config, input):
        del config, input
        raise RuntimeError("custom batch failure")


batch_failure = _BatchFailure()


class _EachFailure:
    input_mode = "each"

    def __call__(self, config, input):
        del config, input
        raise RuntimeError("custom each failure")


each_failure = _EachFailure()


@pytest.mark.parametrize(
    "definition_text",
    [
        """sources: {feed: {use: tests.integration.test_web_stage_scoped_review.source, with: {}}}
stages: {custom: {input: batch, tasks: [{use: tests.integration.test_web_stage_scoped_review.batch_failure, with: {}}]}}
pipelines: {p: {nodes: {custom: {stage: custom, from: source.feed, batch: {count: 1}}, out: {from: custom, deliver: {use: tests.integration.test_web_stage_scoped_review.sink, with: {}}}}}}
""",
        """sources: {feed: {use: tests.integration.test_web_stage_scoped_review.source, with: {}}}
stages: {custom: {input: each, tasks: [{use: tests.integration.test_web_stage_scoped_review.each_failure, with: {}}]}}
pipelines: {p: {nodes: {custom: {stage: custom, from: source.feed}, out: {from: custom, deliver: {use: tests.integration.test_web_stage_scoped_review.sink, with: {}}}}}}
""",
        """web:
  extractors: {identity: {type: identity}}
  extract_chains: {article: {extractors: [identity]}}
sources: {feed: {use: tests.integration.test_web_stage_scoped_review.source, with: {}}}
stages: {extract: {input: each, tasks: [{use: sluice.tasks.extract_content, with: {extract_chain: article, min_chars: 1}}, {use: sluice.tasks.to_text}]}}
pipelines: {p: {nodes: {extract: {stage: extract, from: source.feed}, out: {from: extract, deliver: {use: tests.integration.test_web_stage_scoped_review.sink, with: {}}}}}}
""",
    ],
)
def test_noneligible_real_failures_never_publish_empty_result(
    tmp_path: Path, definition_text: str
) -> None:
    definition = tmp_path / "definition.yml"
    definition.write_text(
        "version: 1\ndatabase: state.db\n" + definition_text, encoding="utf-8"
    )
    app = open_app(definition, tmp_path / "state.db")
    try:
        tick(app, instant())
        assert (
            app.conn.execute(
                "SELECT state FROM executions WHERE kind='stage'"
            ).fetchone()[0]
            == "failed"
        )
        assert (
            app.conn.execute("SELECT COUNT(*) FROM stage_result_current").fetchone()[0]
            == 0
        )
    finally:
        app.close()


def test_resolved_secret_and_vendor_body_never_reach_durable_or_log_views(
    tmp_path: Path, monkeypatch, caplog
) -> None:
    secret = "actual-secret-value"
    monkeypatch.setenv("WEB_SECRET", secret)
    definition = tmp_path / "definition.yml"
    write_web(
        definition,
        providers="vendor: {type: firecrawl, base_url: https://vendor.test, api_key: env:WEB_SECRET}",
        chain="[vendor]",
    )
    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["93.184.216.34"])
    seen: list[dict[str, object]] = []

    class Transport:
        def post(self, url, json=None, headers=None, **kwargs):
            del url, json, kwargs
            seen.append(dict(headers or {}))
            raise RuntimeError(f"vendor body contains {secret}")

    from sluice.config import load_definition

    binding = (
        load_definition(definition).stages["web"].tasks[0].web_binding.providers[0]
    )
    resolved = resolve_provider_binding(binding)
    assert resolved.options["api_key"] == secret
    provider = FirecrawlProvider(resolved, SimpleNamespace(service_client=Transport()))

    class Catalog:
        def provider(self, received):
            assert received.name == "vendor"
            return provider

        def limiter(self, received):
            del received

    client = WebContentClient(Catalog())
    app = open_app(
        definition,
        tmp_path / "state.db",
        http_client=Transport(),
        web_content_client=client,
    )
    try:
        tick(app, instant())
        errors = [
            str(row[0] or "")
            for row in app.conn.execute(
                "SELECT error FROM executions WHERE kind='stage'"
            )
        ]
        assert seen and secret in str(seen[0]), errors
        assert all(
            secret not in error and "vendor body" not in error for error in errors
        )
        assert secret not in caplog.text and "vendor body" not in caplog.text
        management_view = pipeline_list(app)
        assert secret not in str(management_view)
        assert "vendor body" not in str(management_view)
    finally:
        app.close()
