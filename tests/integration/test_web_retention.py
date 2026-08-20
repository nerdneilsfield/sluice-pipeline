"""Task 6 startup lifecycle and retained web snapshot contracts."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Event, Thread

import pytest

from sluice.app import open_app
from sluice.canonical import canonical_json, content_hash, stage_lookup_key
from sluice.config import canonical_stage_config
from sluice.contracts import Item, StageDef, TaskDef
from sluice.errors import DefinitionError
from sluice.web.contracts import ExtractorBinding, ProviderBinding, WebBinding


def _empty_definition(path: Path) -> Path:
    path.write_text("version: 1\nsources: {}\nstages: {}\npipelines: {}\n", encoding="utf-8")
    return path


class _CloseSpy:
    def __init__(self) -> None:
        self.close_calls = 0

    def close(self) -> None:
        self.close_calls += 1


def test_app_close_waits_for_admitted_drain_and_is_idempotent(tmp_path: Path) -> None:
    spy = _CloseSpy()
    app = open_app(_empty_definition(tmp_path / "definition.yml"), tmp_path / "state.db", web_content_client=spy)
    entered = Event()
    release = Event()

    def drain() -> None:
        with app.admit():
            entered.set()
            release.wait(5)

    worker = Thread(target=drain)
    worker.start()
    assert entered.wait(2)
    closer = Thread(target=app.close)
    closer.start()
    closer.join(0.1)
    assert closer.is_alive()
    assert spy.close_calls == 0
    release.set()
    worker.join(2)
    closer.join(2)
    assert not closer.is_alive()
    assert spy.close_calls == 1
    app.close()
    assert spy.close_calls == 1


def _retained_stage(*, tmp_path: Path, binding: WebBinding | None, state: str = "succeeded") -> tuple[Path, int]:
    definition_path = _empty_definition(tmp_path / "definition.yml")
    first = open_app(definition_path, tmp_path / "state.db")
    payload = {"url": "https://example.test/article", "content": "source"}
    item = Item(
        id="item-1",
        producer_scope=("source", "feed"),
        logical_id="one",
        payload=payload,
        metadata={},
        content_hash=content_hash(payload),
        observed_at="2026-01-01T00:00:00Z",
        produced_by=1,
    )
    first.conn.execute("INSERT INTO executions (kind,state,binding_digest,config_json,policy_json) VALUES ('source','succeeded','source-binding','{}','{}')")
    producer = int(first.conn.execute("SELECT last_insert_rowid()").fetchone()[0])
    first.conn.execute(
        "INSERT INTO items (id,producer_kind,producer_name,logical_id,payload_json,metadata_json,content_hash,observed_at,produced_by) VALUES (?,?,?,?,?,?,?,?,?)",
        (item.id, "source", "feed", item.logical_id, canonical_json(item.payload), canonical_json(item.metadata), item.content_hash, item.observed_at, producer),
    )
    task = TaskDef(use="sluice.tasks.fetch_webpage", with_config={"url_field": "url"}, web_binding=binding)
    stage = StageDef(
        name="article",
        input_mode="each",
        tasks=(task,),
        output_schema=None,
        cache_version=1,
        canonical_config_json=canonical_stage_config("article", "each", (task,), None, 1, tmp_path),
    )
    if binding is None:
        legacy_snapshot = __import__("json").loads(stage.canonical_config_json)
        legacy_snapshot["tasks"][0].pop("web_binding", None)
        stage = StageDef(
            name=stage.name,
            input_mode=stage.input_mode,
            tasks=stage.tasks,
            output_schema=stage.output_schema,
            cache_version=stage.cache_version,
            canonical_config_json=canonical_json(legacy_snapshot),
        )
    lookup = stage_lookup_key(stage, (item,), ())
    execution = first.conn.execute(
        "INSERT INTO executions (kind,stage_name,state,binding_digest,config_json,lookup_key,policy_json) VALUES ('stage','article',?,'stage-binding',?,?,?)",
        (state, stage.canonical_config_json, lookup, canonical_json({"timeout_seconds": None, "retries": 0, "concurrency": 1})),
    )
    execution_id = int(execution.lastrowid)
    first.conn.execute("INSERT INTO execution_inputs (execution_id,slot,ordinal,item_id) VALUES (?,?,?,?)", (execution_id, "items", 0, item.id))
    first.close()
    return definition_path, execution_id


def test_terminal_missing_secret_does_not_block_startup_but_rebuild_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("RETIRED_KEY", raising=False)
    binding = WebBinding(
        fetch_chain=(ProviderBinding(name="compat", type="startup_http", options={"trusted": True}, target_headers={"X-Token": "env:RETIRED_KEY"}, capabilities=frozenset({"text"}), timeout=timedelta(seconds=30), connect_timeout=timedelta(seconds=10), timeout_seconds=30),),
        fetch_chain_name="retired",
        extract_chain=(ExtractorBinding(name="identity", type="identity"),),
        extract_chain_name="article",
        output="artifact",
    )
    definition_path, execution_id = _retained_stage(tmp_path=tmp_path, binding=binding)
    app = open_app(definition_path, tmp_path / "state.db")
    with pytest.raises(DefinitionError, match="RETIRED_KEY"):
        from sluice.stage_runtime import rebuild_stage

        rebuild_stage(app, execution_id, datetime.now(timezone.utc))
    app.close()


def test_queued_missing_secret_blocks_startup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("QUEUED_KEY", raising=False)
    binding = WebBinding(
        fetch_chain=(ProviderBinding(name="compat", type="startup_http", options={"trusted": True}, target_headers={"X-Token": "env:QUEUED_KEY"}, capabilities=frozenset({"text"}), timeout=timedelta(seconds=30), connect_timeout=timedelta(seconds=10), timeout_seconds=30),),
        fetch_chain_name="queued",
        extract_chain=(ExtractorBinding(name="identity", type="identity"),),
        extract_chain_name="article",
        output="artifact",
    )
    definition_path, _ = _retained_stage(tmp_path=tmp_path, binding=binding, state="queued")
    with pytest.raises(DefinitionError, match="QUEUED_KEY"):
        open_app(definition_path, tmp_path / "state.db")


def test_terminal_legacy_web_snapshot_is_readable_but_never_rebuildable(tmp_path: Path) -> None:
    definition_path, execution_id = _retained_stage(tmp_path=tmp_path, binding=None)
    app = open_app(definition_path, tmp_path / "state.db")
    with pytest.raises(DefinitionError, match="legacy web snapshot"):
        from sluice.stage_runtime import rebuild_stage

        rebuild_stage(app, execution_id, datetime.now(timezone.utc))
    app.close()
