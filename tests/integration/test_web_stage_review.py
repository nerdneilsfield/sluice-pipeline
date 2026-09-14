"""Adversarial durable web Stage lifecycle regressions."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Event, Thread

import pytest

from sluice.app import open_app
from sluice.contracts import SourceRecord, SourceResult, StageEnvelope
from sluice.read_model import pipeline_list
from sluice.reconcile import tick
from sluice.source_runtime import collect_source
from sluice.stage_runtime import (
    _execute_claimed_stage,
    _request_materialization,
    _StageRequest,
)
from sluice.web.client import WebContentClient
from sluice.web.contracts import (
    DocumentArtifact,
    FetchChainExhausted,
    FetchRequest,
    ProviderBinding,
)


def instant(seconds: int) -> datetime:
    return datetime(2026, 8, 19, tzinfo=UTC) + timedelta(seconds=seconds)


def source(config, checkpoint):
    del config, checkpoint
    return SourceResult(
        (SourceRecord("article", {"url": "https://example.test/article"}),), "done"
    )


def sink(config, payload, delivery_key):
    del config, payload, delivery_key


def write_definition(
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
sources: {{feed: {{use: tests.integration.test_web_stage_review.source, with: {{}}}}}}
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
      out: {{from: web, deliver: {{use: tests.integration.test_web_stage_review.sink, with: {{}}}}}}
""",
        encoding="utf-8",
    )


def request_execution(app, at: datetime) -> int:
    collect_source(app, "feed", at)
    item_id = app.conn.execute(
        "SELECT id FROM items WHERE producer_kind='source'"
    ).fetchone()[0]
    request = _StageRequest(
        stage_name="web",
        envelope=StageEnvelope((item_id,)),
        owner=None,
        generation=None,
        batch_id=None,
        policy=app.definition.pipelines["p"].nodes["web"].policy,
        binding_digest="review-test",
    )
    execution_id = _request_materialization(app, request, at).execution_id
    assert execution_id is not None
    return execution_id


class ExhaustingClient:
    def __init__(self) -> None:
        self.requests: list[FetchRequest] = []

    def resolve(self, request, fetch_chain, extract_chain):
        del fetch_chain, extract_chain
        self.requests.append(request)
        raise FetchChainExhausted(())

    def close(self) -> None:
        pass


def test_persisted_started_at_limits_provider_budget(
    tmp_path: Path, monkeypatch
) -> None:
    definition = tmp_path / "definition.yml"
    write_definition(definition, timeout="10s")
    client = ExhaustingClient()
    app = open_app(
        definition,
        tmp_path / "state.db",
        http_client=lambda url: "unused",
        web_content_client=client,
    )
    try:
        execution_id = request_execution(app, instant(0))
        app.conn.execute(
            "UPDATE executions SET state='running',started_at=? WHERE id=?",
            ("2026-08-19T00:00:00Z", execution_id),
        )
        monkeypatch.setattr(
            "sluice.stage_runtime._start_claimed_execution", lambda *args: True
        )
        monkeypatch.setattr("sluice.stage_runtime.time.monotonic", lambda: 1000.0)
        _execute_claimed_stage(app, execution_id, instant(4))
        assert client.requests[0].stage_deadline == pytest.approx(1006.0)
    finally:
        app.close()


def test_expired_persisted_started_at_never_starts_provider(
    tmp_path: Path, monkeypatch
) -> None:
    definition = tmp_path / "definition.yml"
    write_definition(definition, timeout="10s")
    client = ExhaustingClient()
    app = open_app(
        definition,
        tmp_path / "state.db",
        http_client=lambda url: "unused",
        web_content_client=client,
    )
    try:
        execution_id = request_execution(app, instant(0))
        app.conn.execute(
            "UPDATE executions SET state='running',started_at=? WHERE id=?",
            ("2026-08-19T00:00:00Z", execution_id),
        )
        monkeypatch.setattr(
            "sluice.stage_runtime._start_claimed_execution", lambda *args: True
        )
        monkeypatch.setattr("sluice.stage_runtime.time.monotonic", lambda: 1000.0)
        _execute_claimed_stage(app, execution_id, instant(11))
        assert client.requests == []
        assert app.conn.execute("SELECT COUNT(*) FROM stage_results").fetchone()[0] == 0
    finally:
        app.close()


def _provider(name: str) -> ProviderBinding:
    return ProviderBinding(
        name=name,
        type="startup_http",
        options={"trusted": True},
        capabilities=frozenset({"markdown"}),
    )


def test_lost_claim_after_response_cannot_start_paid_fallback_or_retry(
    tmp_path: Path,
) -> None:
    first_started, release = Event(), Event()
    calls: list[str] = []
    first, second = _provider("first"), _provider("second")

    class Catalog:
        def provider(self, binding: ProviderBinding):
            class Provider:
                def fetch(self, request: FetchRequest) -> DocumentArtifact:
                    del request
                    calls.append(binding.name)
                    if binding.name == "first":
                        first_started.set()
                        assert release.wait(5)
                        return DocumentArtifact(
                            final_url="https://example.test/article",
                            content_type="text/markdown",
                            markdown="first content",
                            capabilities=frozenset({"markdown"}),
                        )
                    raise AssertionError("stale execution started paid fallback")

            return Provider()

    definition = tmp_path / "definition.yml"
    write_definition(
        definition,
        retries=1,
        providers="first: {type: startup_http, trusted: true}, second: {type: startup_http, trusted: true}",
        chain="[first, second]",
    )
    app = open_app(
        definition,
        tmp_path / "state.db",
        http_client=lambda url: "unused",
        web_content_client=WebContentClient(Catalog()),
    )
    try:
        execution_id = request_execution(app, instant(0))
        worker = Thread(
            target=_execute_claimed_stage, args=(app, execution_id, instant(0))
        )
        worker.start()
        assert first_started.wait(5)
        app.conn.execute(
            "UPDATE materialization_claims SET owner_execution_id=NULL WHERE owner_execution_id=?",
            (execution_id,),
        )
        release.set()
        worker.join(5)
        assert not worker.is_alive()
        assert calls == ["first"]
        assert app.conn.execute("SELECT COUNT(*) FROM stage_results").fetchone()[0] == 0
        assert (
            app.conn.execute(
                "SELECT COUNT(*) FROM executions WHERE retry_parent_id=?",
                (execution_id,),
            ).fetchone()[0]
            == 0
        )
    finally:
        app.close()


def test_web_error_never_reaches_log_or_management_projection(
    tmp_path: Path, caplog
) -> None:
    class UnsafeClient:
        def resolve(self, request, fetch_chain, extract_chain):
            del request, fetch_chain, extract_chain
            raise RuntimeError("Authorization: env:KEY vendor-body-private")

        def close(self) -> None:
            pass

    definition = tmp_path / "definition.yml"
    write_definition(definition)
    app = open_app(
        definition,
        tmp_path / "state.db",
        http_client=lambda url: "unused",
        web_content_client=UnsafeClient(),
    )
    try:
        collect_source(app, "feed", instant(0))
        tick(app, instant(0))
        execution_id = app.conn.execute(
            "SELECT id FROM executions WHERE kind='stage'"
        ).fetchone()[0]
        error = app.conn.execute(
            "SELECT error FROM executions WHERE id=?", (execution_id,)
        ).fetchone()[0]
        assert error == "provider_error"
        assert "KEY" not in caplog.text and "vendor-body-private" not in caplog.text
        assert pipeline_list(app)[0].latest_error == "provider_error"
    finally:
        app.close()
