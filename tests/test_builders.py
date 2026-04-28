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
