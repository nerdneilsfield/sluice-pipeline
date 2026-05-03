from __future__ import annotations

import pytest

from sluice.dag import db
from sluice.dag.dynamic_tables import (
    ensure_pipeline_sink_pending_table,
    ensure_pipeline_table,
    ensure_stage_table,
)
from sluice.dag.stores.pipeline_state import PipelineStateStore
from sluice.dag.stores.pipelines import PipelinesStore
from sluice.dag.stores.sink_delivery_log import SinkDeliveryLogStore
from sluice.dag.stores.sink_emissions import SinkEmissionsStore
from sluice.dag.stores.sink_pending import SinkPendingStore
from sluice.dag.stores.stage_attempt_log import StageAttemptLogStore
from sluice.dag.stores.stage_state import StageStateStore
from sluice.dag.stores.stages import StagesStore


@pytest.mark.asyncio
async def test_sink_emission_and_delivery_stores_match_schema(tmp_path):
    async with db.open_active_db(str(tmp_path / "active.db")) as conn:
        emissions = SinkEmissionsStore(conn)
        log = SinkDeliveryLogStore(conn)
        assert not await emissions.exists("rk", "s1", 0)
        await log.append(
            run_key="rk",
            sink_id="s1",
            sink_type="file_md",
            attempt_at="now",
            ordinal=0,
            message_kind="markdown",
            recipient="out",
            external_id="file",
            status="success",
            error_class=None,
            error_msg=None,
        )
        await emissions.insert(
            run_key="rk",
            sink_id="s1",
            ordinal=0,
            sink_type="file_md",
            message_kind="markdown",
            external_id="file",
            emitted_at="now",
        )
        assert await emissions.exists("rk", "s1", 0)


@pytest.mark.asyncio
async def test_sink_pending_oldest_and_defer_helpers(tmp_path):
    async with db.open_active_db(str(tmp_path / "active.db")) as conn:
        await ensure_pipeline_sink_pending_table(conn, "P")
        store = SinkPendingStore(conn, "P")
        await store.upsert(item_id="a", run_key="rk2", pending_since="2026-05-04T02:00:00")
        await store.upsert(item_id="b", run_key="rk1", pending_since="2026-05-04T01:00:00")
        assert await store.oldest_run_key() == "rk1"
        assert await store.items_for_run_key("rk1") == {"b"}
        assert await store.items_with_other_keys("rk1") == {"a"}
        await store.delete_for_items(["b"])
        assert await store.oldest_run_key() == "rk2"


@pytest.mark.asyncio
async def test_pipeline_and_stage_state_stores(tmp_path):
    async with db.open_active_db(str(tmp_path / "active.db")) as conn:
        await ensure_pipeline_table(conn, "P")
        await ensure_stage_table(conn, "S")
        run_id = await PipelinesStore(conn).upsert_running(
            run_key="rk",
            task="t",
            pipeline_name="P",
            cycle_anchor="c",
            trigger="manual",
            started_at="start",
        )
        stage_run_id = await StagesStore(conn).start(
            pipeline_run_id=run_id,
            stage_name="S",
            started_at="start",
        )
        await PipelineStateStore(conn, "P").upsert(
            item_id="i",
            last_run_id=run_id,
            status="success",
            processed_at="now",
            error_msg=None,
        )
        stage_state = StageStateStore(conn, "S")
        await stage_state.upsert(
            pipeline_name="P",
            item_id="i",
            status="done",
            attempts=1,
            last_stage_run_id=stage_run_id,
            last_pipeline_run_id=run_id,
            last_processed_at="now",
            last_error_class=None,
            last_error_msg=None,
        )
        assert await stage_state.attempts(pipeline_name="P", item_id="i") == 1
        assert await stage_state.done_item_ids(pipeline_name="P", item_ids=["i", "x"]) == {"i"}
        log = StageAttemptLogStore(conn)
        assert await log.next_attempt_no(pipeline_name="P", stage_name="S", item_id="i") == 1
        await log.append(
            stage_run_id=stage_run_id,
            pipeline_run_id=run_id,
            pipeline_name="P",
            stage_name="S",
            item_id="i",
            attempt_no=1,
            status="done",
            processed_at="now",
            error_class=None,
            error_msg=None,
        )
        assert await log.next_attempt_no(pipeline_name="P", stage_name="S", item_id="i") == 2
