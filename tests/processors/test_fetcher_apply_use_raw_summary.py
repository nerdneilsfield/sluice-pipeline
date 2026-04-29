import pytest

from sluice.core.item import Item
from sluice.processors.fetcher_apply import FetcherApplyProcessor
from tests.conftest import make_ctx


class _AllFailChain:
    async def fetch(self, url: str):
        raise RuntimeError("all fetchers exhausted")


class _RecordingFailures:
    def __init__(self):
        self.rows = []

    async def record(
        self, pipeline_id, item_key, item, *, stage, error_class, error_msg, max_retries
    ):
        self.rows.append(
            {
                "pipeline_id": pipeline_id,
                "item_key": item_key,
                "stage": stage,
                "error_class": error_class,
                "error_msg": error_msg,
                "max_retries": max_retries,
            }
        )


@pytest.mark.asyncio
async def test_use_raw_summary_fallback():
    item = Item(
        source_id="s",
        pipeline_id="p",
        guid="g1",
        url="http://x",
        title="t",
        published_at=None,
        raw_summary="this is the rss-shipped fulltext",
    )
    ctx = make_ctx(items=[item])
    proc = FetcherApplyProcessor(
        name="fa",
        chain=_AllFailChain(),
        write_field="fulltext",
        skip_if_field_longer_than=None,
        failures=None,
        max_retries=3,
        on_all_failed="use_raw_summary",
    )
    out = await proc.process(ctx)
    assert out.items[0].fulltext == "this is the rss-shipped fulltext"


@pytest.mark.asyncio
async def test_use_raw_summary_with_empty_raw_records_failure():
    item = Item(
        source_id="s",
        pipeline_id="p",
        guid="g2",
        url="http://x",
        title="t",
        published_at=None,
        raw_summary=None,
    )
    ctx = make_ctx(items=[item])
    failures = _RecordingFailures()
    proc = FetcherApplyProcessor(
        name="fa",
        chain=_AllFailChain(),
        write_field="fulltext",
        skip_if_field_longer_than=None,
        failures=failures,
        max_retries=3,
        on_all_failed="use_raw_summary",
    )
    out = await proc.process(ctx)
    assert out.items == []
    assert failures.rows
    assert failures.rows[0]["error_class"] == "FetcherChainExhaustedNoFallback"


@pytest.mark.asyncio
async def test_use_raw_summary_with_whitespace_only_raw_records_failure():
    item = Item(
        source_id="s",
        pipeline_id="p",
        guid="g3",
        url="http://x",
        title="t",
        published_at=None,
        raw_summary="   \n  \t  ",  # Only whitespace
    )
    ctx = make_ctx(items=[item])
    failures = _RecordingFailures()
    proc = FetcherApplyProcessor(
        name="fa",
        chain=_AllFailChain(),
        write_field="fulltext",
        skip_if_field_longer_than=None,
        failures=failures,
        max_retries=3,
        on_all_failed="use_raw_summary",
    )
    out = await proc.process(ctx)
    assert out.items == []
    assert failures.rows
    assert failures.rows[0]["error_class"] == "FetcherChainExhaustedNoFallback"
