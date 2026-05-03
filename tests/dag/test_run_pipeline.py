from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from sluice.dag import db
from sluice.dag.config import (
    DeploymentsDef,
    PipelineDef,
    SinkDef,
    SourceDef,
    StageDef,
    TaskConfig,
    WindowDef,
)
from sluice.dag.dynamic_tables import ensure_pipeline_table
from sluice.dag.run_pipeline import ensure_dynamic_tables, run_pipeline
from sluice.dag.sinks.base import EmissionResult, Sink, SinkChunk, bind_sink
from sluice.dag.stages.executors import FetchRssExecutor
from sluice.dag.writer import WriterThread

RSS = """<?xml version="1.0"?>
<rss version="2.0"><channel><title>F</title>
  <item><guid>g1</guid><title>X</title><link>https://x/1</link>
        <pubDate>Mon, 04 May 2026 07:00:00 +0000</pubDate>
        <description>d</description></item>
</channel></rss>"""


def _task(tmp_path: Path, *, stages=None) -> TaskConfig:
    return TaskConfig(
        name="t",
        db_path=str(tmp_path / "active.db"),
        seen_db=str(tmp_path / "seen.db"),
        enabled=True,
        timezone="UTC",
        window=WindowDef(anchor="0 8 * * *", length="24h"),
        deployments=DeploymentsDef(pipelines="all"),
        sources=[SourceDef(name="feed", type="rss", params={"url": "https://feed"})],
        sinks=[
            SinkDef(
                name="out",
                type="file_md",
                params={
                    "out_dir": str(tmp_path / "out"),
                    "filename_template": "digest.md",
                },
            )
        ],
        stages=stages
        or [
            StageDef("fetch", "fetch_rss", {}),
            StageDef("dedupe", "dedupe", {}),
            StageDef("render", "render", {"template": "{{ items|length }} {{ items[0].title }}"}),
        ],
        pipelines=[
            PipelineDef(
                name="P",
                cron="0 8 * * *",
                stages=["fetch", "dedupe", "render"],
                sources=["feed"],
                upstream_all=None,
                upstream_any=None,
                sinks=["out"],
                post_run_cleanup="none",
            )
        ],
    )


def _retry_task(tmp_path: Path) -> TaskConfig:
    task = _task(tmp_path)
    return TaskConfig(
        name=task.name,
        db_path=task.db_path,
        seen_db=task.seen_db,
        enabled=task.enabled,
        timezone=task.timezone,
        window=task.window,
        deployments=task.deployments,
        sources=task.sources,
        sinks=[SinkDef(name="out", type="flaky_retry_test", params={"sink_id": "out"})],
        stages=task.stages,
        pipelines=[
            PipelineDef(
                name="P",
                cron="0 8 * * *",
                stages=["fetch", "dedupe", "render"],
                sources=["feed"],
                upstream_all=None,
                upstream_any=None,
                sinks=["out"],
                post_run_cleanup="none",
            )
        ],
    )


class _FlakyRetrySink(Sink):
    fail = True

    async def chunks(self, rendered, params):
        return [SinkChunk(ordinal=0, payload=rendered, message_kind="markdown")]

    async def emit(self, chunk, params):
        if self.fail:
            raise RuntimeError("sink down")
        return EmissionResult(
            sink_id=params["sink_id"],
            ordinal=chunk.ordinal,
            success=True,
            external_id="ok",
        )


try:
    bind_sink("flaky_retry_test", _FlakyRetrySink)
except ValueError:
    pass


@pytest.mark.asyncio
async def test_run_pipeline_fetches_renders_sinks_and_marks_success(tmp_path, monkeypatch):
    async def fake_fetch(self, url):
        return RSS

    monkeypatch.setattr(FetchRssExecutor, "_default_fetch_text", fake_fetch)
    task = _task(tmp_path)
    await ensure_dynamic_tables(task)
    writer = WriterThread(active_path=task.db_path, seen_path=task.seen_db)
    await writer.start()
    try:
        result = await run_pipeline(
            task,
            "P",
            writer=writer,
            now=datetime(2026, 5, 4, 8, 0, tzinfo=timezone.utc),
        )
    finally:
        await writer.stop()

    assert result.status == "success"
    assert result.items_in == 1
    assert result.items_out == 1
    assert (tmp_path / "out" / "digest.md").read_text() == "1 X"
    async with db.open_active_db(task.db_path) as active:
        cur = await active.execute("SELECT status FROM pipeline_P WHERE item_id='feed:g1'")
        assert (await cur.fetchone())[0] == "success"
        cur = await active.execute("SELECT count(*) FROM sink_emissions")
        assert (await cur.fetchone())[0] == 1
    async with db.open_seen_db(task.seen_db) as seen:
        cur = await seen.execute("SELECT item_id FROM item_seen")
        assert (await cur.fetchone())[0] == "feed:g1"


@pytest.mark.asyncio
async def test_entry_carryover_skips_duplicate_fetch_item(tmp_path, monkeypatch):
    async def fake_fetch(self, url):
        return RSS

    monkeypatch.setattr(FetchRssExecutor, "_default_fetch_text", fake_fetch)
    task = _task(tmp_path)
    await ensure_dynamic_tables(task)
    async with db.open_active_db(task.db_path) as active:
        await ensure_pipeline_table(active, "P")
        await active.execute(
            "INSERT INTO items(item_id, source_name, guid, url, title, ingested_at, "
            "ingested_by, updated_at, raw_json, dropped) VALUES "
            "('feed:g1', 'feed', 'g1', 'https://x/1', 'carry', 't', 'P', 't', '{}', 0)"
        )
        await active.commit()

    writer = WriterThread(active_path=task.db_path, seen_path=task.seen_db)
    await writer.start()
    try:
        result = await run_pipeline(
            task,
            "P",
            writer=writer,
            now=datetime(2026, 5, 4, 8, 0, tzinfo=timezone.utc),
        )
    finally:
        await writer.stop()

    assert result.status == "success"
    assert result.items_in == 1
    assert (tmp_path / "out" / "digest.md").read_text() == "1 carry"


@pytest.mark.asyncio
async def test_sink_pending_retry_is_not_dropped_by_seen_dedupe(tmp_path, monkeypatch):
    async def fake_fetch(self, url):
        return RSS

    monkeypatch.setattr(FetchRssExecutor, "_default_fetch_text", fake_fetch)
    task = _retry_task(tmp_path)
    await ensure_dynamic_tables(task)
    writer = WriterThread(active_path=task.db_path, seen_path=task.seen_db)
    await writer.start()
    try:
        _FlakyRetrySink.fail = True
        first = await run_pipeline(
            task,
            "P",
            writer=writer,
            now=datetime(2026, 5, 4, 8, 0, tzinfo=timezone.utc),
        )
        _FlakyRetrySink.fail = False
        second = await run_pipeline(
            task,
            "P",
            writer=writer,
            now=datetime(2026, 5, 4, 8, 30, tzinfo=timezone.utc),
        )
    finally:
        await writer.stop()
        _FlakyRetrySink.fail = True

    assert first.status == "failed"
    assert second.status == "success"
    async with db.open_active_db(task.db_path) as active:
        cur = await active.execute("SELECT status FROM pipeline_P WHERE item_id='feed:g1'")
        assert (await cur.fetchone())[0] == "success"
        cur = await active.execute("SELECT count(*) FROM pipeline_P_sink_pending")
        assert (await cur.fetchone())[0] == 0
