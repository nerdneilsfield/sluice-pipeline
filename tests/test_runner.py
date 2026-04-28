import pytest
from datetime import datetime, timezone
from unittest.mock import patch
from sluice.config import (
    GlobalConfig, PipelineConfig, RssSourceConfig,
    DedupeConfig, FilterConfig, FilterRule, RenderConfig,
    FileMdSinkConfig, StateConfig, RuntimeConfig,
    GlobalFetcherConfig, FetcherImplConfig,
    ProvidersConfig,
)
from sluice.core.item import Item
from sluice.loader import ConfigBundle
from sluice.runner import run_pipeline

import sluice.sources.rss           # noqa
import sluice.fetchers.trafilatura_fetcher  # noqa
import sluice.processors.dedupe     # noqa
import sluice.processors.filter     # noqa
import sluice.processors.render     # noqa
import sluice.sinks.file_md         # noqa


def mk_item(guid, url="https://x/a", title="t", published_at=None):
    return Item(
        source_id="s", pipeline_id="p", guid=guid, url=url,
        title=title, published_at=published_at or datetime.now(timezone.utc),
        raw_summary=None,
    )


class FakeSource:
    def __init__(self, items):
        self.items = items

    async def fetch(self, window_start, window_end):
        for it in self.items:
            yield it


@pytest.mark.asyncio
async def test_end_to_end_no_llm(tmp_path):
    # Create template file for render stage
    template_path = tmp_path / "template.md"
    template_path.write_text("{% for it in items %}{{ it.title }}\n{% endfor %}")

    global_cfg = GlobalConfig(
        state=StateConfig(db_path=str(tmp_path / "test.db")),
        runtime=RuntimeConfig(timezone="UTC"),
        fetcher=GlobalFetcherConfig(chain=["trafilatura"]),
        fetchers={
            "trafilatura": FetcherImplConfig(type="trafilatura"),
        },
    )
    pipe = PipelineConfig(
        id="p", window="24h",
        sources=[RssSourceConfig(type="rss", url="https://x/feed")],
        stages=[
            DedupeConfig(type="dedupe", name="dedupe"),
            FilterConfig(type="filter", name="filter", mode="keep_if_all",
                         rules=[FilterRule(field="title", op="eq", value="keep")]),
            RenderConfig(type="render", name="render", template=str(template_path),
                         output_field="context.markdown"),
        ],
        sinks=[FileMdSinkConfig(id="out", type="file_md",
                                input="context.markdown", path=str(tmp_path / "{run_date}.md"))],
    )
    bundle = ConfigBundle(
        global_cfg=global_cfg,
        providers=ProvidersConfig(providers=[]),
        pipelines={"p": pipe},
        root=tmp_path,
    )

    items = [
        mk_item(guid="g1", title="keep"),
        mk_item(guid="g2", title="drop"),
    ]
    fake_source = FakeSource(items)

    with patch("sluice.runner.build_sources", return_value=[fake_source]):
        result = await run_pipeline(bundle, pipeline_id="p",
                                     now=datetime(2026, 4, 28, 12, 0, 0, tzinfo=timezone.utc))

    assert result.status == "success"
    assert result.items_in == 2
    assert result.items_out == 1

    # Verify output file was written
    out_file = tmp_path / "2026-04-28.md"
    assert out_file.exists()
    assert "keep" in out_file.read_text()
