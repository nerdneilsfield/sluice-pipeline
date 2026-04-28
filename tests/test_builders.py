import pytest
from sluice.config import (
    GlobalConfig, FetcherImplConfig, GlobalFetcherConfig,
    PipelineConfig, RssSourceConfig, DedupeConfig, FetcherApplyConfig,
    FilterConfig, FilterRule, FieldFilterConfig, FieldOp, LLMStageConfig,
    RenderConfig, FileMdSinkConfig,
)
from sluice.builders import build_sources, build_fetcher_chain, build_processors

import sluice.sources.rss     # noqa
import sluice.fetchers.trafilatura_fetcher  # noqa
import sluice.fetchers.firecrawl  # noqa
import sluice.processors.dedupe  # noqa
import sluice.processors.filter  # noqa


def test_build_rss_source():
    cfg = PipelineConfig(
        id="p", window="24h",
        sources=[RssSourceConfig(type="rss", url="https://x/feed", tag="ai")],
        stages=[DedupeConfig(type="dedupe", name="d")],
        sinks=[FileMdSinkConfig(id="x", type="file_md",
                                input="context.markdown", path="./{run_date}.md")],
    )
    sources = build_sources(cfg)
    assert len(sources) == 1 and sources[0].url == "https://x/feed"
    assert sources[0].tags == ["ai"]


@pytest.mark.asyncio
async def test_build_fetcher_chain():
    from sluice.state.cache import UrlCacheStore
    from sluice.state.db import open_db
    g = GlobalConfig(fetchers={"trafilatura": FetcherImplConfig(type="trafilatura", timeout=5)})
    p = PipelineConfig(
        id="p", window="24h",
        sources=[RssSourceConfig(type="rss", url="https://x")],
        stages=[DedupeConfig(type="dedupe", name="d")],
        sinks=[FileMdSinkConfig(id="x", type="file_md",
                                input="context.markdown", path="./x.md")],
        fetcher={"chain": ["trafilatura"], "min_chars": 100},
    )
    chain = build_fetcher_chain(g, p, cache=None)
    assert chain.min_chars == 100
    assert len(chain.fetchers) == 1


@pytest.mark.asyncio
async def test_build_processors():
    from sluice.state.db import open_db
    from sluice.state.seen import SeenStore
    from sluice.state.failures import FailureStore
    g = GlobalConfig()
    p = PipelineConfig(
        id="p", window="24h",
        sources=[RssSourceConfig(type="rss", url="https://x")],
        stages=[FilterConfig(type="filter", name="f", mode="keep_if_all",
                             rules=[FilterRule(field="title", op="eq", value="x")])],
        sinks=[FileMdSinkConfig(id="x", type="file_md",
                                input="context.markdown", path="./x.md")],
    )
    async with open_db(":memory:") as db:
        seen = SeenStore(db); failures = FailureStore(db)
        procs = build_processors(
            pipe=p, global_cfg=g, seen=seen, failures=failures,
            fetcher_chain=None, llm_pool=None, budget=None)
        assert len(procs) == 1
        assert procs[0].name == "f"
