import pytest
from datetime import datetime, timezone
from sluice.context import PipelineContext
from sluice.core.item import Item
from sluice.processors.fetcher_apply import FetcherApplyProcessor
from sluice.core.errors import AllFetchersFailed

class StubChain:
    def __init__(self, by_url): self.by_url = by_url
    async def fetch(self, url):
        v = self.by_url.get(url)
        if isinstance(v, Exception): raise v
        return v

def mk(url, **kw):
    return Item(source_id="s", pipeline_id="p", guid=None, url=url,
                title="t", published_at=datetime(2026,4,28,tzinfo=timezone.utc),
                raw_summary=kw.get("raw_summary"))

class FailRecorder:
    def __init__(self): self.records = []
    async def record(self, pid, key, item, *, stage, error_class,
                     error_msg, max_retries):
        self.records.append((key, error_class))

@pytest.mark.asyncio
async def test_writes_fulltext():
    chain = StubChain({"https://a": "x" * 1000})
    fr = FailRecorder()
    p = FetcherApplyProcessor(name="fa", chain=chain, write_field="fulltext",
                              skip_if_field_longer_than=None,
                              failures=fr, max_retries=3)
    ctx = PipelineContext("p","p/r","2026-04-28",[mk("https://a")],{})
    ctx = await p.process(ctx)
    assert ctx.items[0].fulltext.startswith("x")

@pytest.mark.asyncio
async def test_skips_if_raw_summary_long_enough():
    chain = StubChain({"https://a": "fetched"})
    p = FetcherApplyProcessor(name="fa", chain=chain, write_field="fulltext",
                              skip_if_field_longer_than=10,
                              failures=FailRecorder(), max_retries=3)
    item = mk("https://a", raw_summary="long enough raw summary here")
    ctx = PipelineContext("p","p/r","2026-04-28",[item],{})
    ctx = await p.process(ctx)
    assert ctx.items[0].fulltext == "long enough raw summary here"

@pytest.mark.asyncio
async def test_records_failure_drops_item():
    chain = StubChain({"https://a": AllFetchersFailed("https://a", ["x"])})
    fr = FailRecorder()
    p = FetcherApplyProcessor(name="fa", chain=chain, write_field="fulltext",
                              skip_if_field_longer_than=None,
                              failures=fr, max_retries=3)
    ctx = PipelineContext("p","p/r","2026-04-28",[mk("https://a")],{})
    ctx = await p.process(ctx)
    assert ctx.items == []
    assert fr.records and fr.records[0][1] == "AllFetchersFailed"
