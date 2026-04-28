import pytest
import textwrap
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
from sluice.loader import ConfigBundle, load_all
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


@pytest.mark.asyncio
async def test_requeue_resolved_after_success(tmp_path):
    from sluice.state.db import open_db
    from sluice.state.failures import FailureStore
    from sluice.state.seen import SeenStore

    db_path = tmp_path / "test.db"
    # Pre-seed a failed item
    async with open_db(db_path) as db:
        failures = FailureStore(db)
        item = mk_item(guid="requeue1", title="requeue1")
        await failures.record("p", "requeue1", item, stage="x",
                              error_class="E", error_msg="m", max_retries=3)

    template_path = tmp_path / "template.md"
    template_path.write_text("{{ items | length }}")

    global_cfg = GlobalConfig(
        state=StateConfig(db_path=str(db_path)),
        runtime=RuntimeConfig(timezone="UTC"),
        fetcher=GlobalFetcherConfig(chain=["trafilatura"]),
        fetchers={"trafilatura": FetcherImplConfig(type="trafilatura")},
    )
    pipe = PipelineConfig(
        id="p", window="24h",
        sources=[RssSourceConfig(type="rss", url="https://x/feed")],
        stages=[
            DedupeConfig(type="dedupe", name="dedupe"),
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

    # Source returns the same item that was failed
    fake_source = FakeSource([mk_item(guid="requeue1", title="requeue1")])

    with patch("sluice.runner.build_sources", return_value=[fake_source]):
        result = await run_pipeline(bundle, pipeline_id="p",
                                     now=datetime(2026, 4, 28, 12, 0, 0, tzinfo=timezone.utc))

    assert result.status == "success"

    # Verify failure is now resolved
    async with open_db(db_path) as db:
        failures = FailureStore(db)
        resolved = await failures.list("p", status="resolved")
        assert len(resolved) == 1
        assert resolved[0]["item_key"] == "requeue1"


@pytest.mark.asyncio
async def test_backpressure_fires_after_dedupe(tmp_path):
    template_path = tmp_path / "template.md"
    template_path.write_text("{{ items | length }}")

    global_cfg = GlobalConfig(
        state=StateConfig(db_path=str(tmp_path / "test.db")),
        runtime=RuntimeConfig(timezone="UTC"),
        fetcher=GlobalFetcherConfig(chain=["trafilatura"]),
        fetchers={"trafilatura": FetcherImplConfig(type="trafilatura")},
    )
    from sluice.config import PipelineLimits
    pipe = PipelineConfig(
        id="p", window="24h",
        sources=[RssSourceConfig(type="rss", url="https://x/feed")],
        stages=[
            DedupeConfig(type="dedupe", name="dedupe"),
            RenderConfig(type="render", name="render", template=str(template_path),
                         output_field="context.markdown"),
        ],
        sinks=[FileMdSinkConfig(id="out", type="file_md",
                                input="context.markdown", path=str(tmp_path / "{run_date}.md"))],
        limits=PipelineLimits(max_items_per_run=1),
    )
    bundle = ConfigBundle(
        global_cfg=global_cfg,
        providers=ProvidersConfig(providers=[]),
        pipelines={"p": pipe},
        root=tmp_path,
    )

    # 3 items, but cap is 1
    items = [
        mk_item(guid="g1", title="a"),
        mk_item(guid="g2", title="b"),
        mk_item(guid="g3", title="c"),
    ]
    fake_source = FakeSource(items)

    with patch("sluice.runner.build_sources", return_value=[fake_source]):
        result = await run_pipeline(bundle, pipeline_id="p",
                                     now=datetime(2026, 4, 28, 12, 0, 0, tzinfo=timezone.utc))

    assert result.status == "success"
    assert result.items_out == 1


@pytest.mark.asyncio
async def test_dry_run_writes_nothing_external(tmp_path):
    from sluice.state.db import open_db
    from sluice.state.seen import SeenStore

    template_path = tmp_path / "template.md"
    template_path.write_text("{{ items | length }}")

    db_path = tmp_path / "test.db"
    global_cfg = GlobalConfig(
        state=StateConfig(db_path=str(db_path)),
        runtime=RuntimeConfig(timezone="UTC"),
        fetcher=GlobalFetcherConfig(chain=["trafilatura"]),
        fetchers={"trafilatura": FetcherImplConfig(type="trafilatura")},
    )
    pipe = PipelineConfig(
        id="p", window="24h",
        sources=[RssSourceConfig(type="rss", url="https://x/feed")],
        stages=[
            DedupeConfig(type="dedupe", name="dedupe"),
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

    items = [mk_item(guid="g1", title="keep")]
    fake_source = FakeSource(items)

    with patch("sluice.runner.build_sources", return_value=[fake_source]):
        result = await run_pipeline(bundle, pipeline_id="p", dry_run=True,
                                     now=datetime(2026, 4, 28, 12, 0, 0, tzinfo=timezone.utc))

    assert result.status == "success"
    assert result.items_out == 1

    # No output file should be written
    out_file = tmp_path / "2026-04-28.md"
    assert not out_file.exists()

    # No seen records should be written
    async with open_db(db_path) as db:
        seen = SeenStore(db)
        assert not await seen.is_seen("p", "g1")


def _bootstrap_minimal(tmp_path, *, rss_text: str, cap: int = 30,
                       extra_pipe_toml: str = ""):
    """Helper used by regression tests. Builds a configs/ tree with:
    state DB at tmp_path/d.db, one openai-compat provider (env K),
    one RSS source (mocked), dedupe + render + file_md sink. No LLM stages
    so we don't need to mock the chat endpoint."""
    cfg = tmp_path / "configs"; (cfg / "pipelines").mkdir(parents=True)
    (cfg / "sluice.toml").write_text(textwrap.dedent(f"""
        [state]
        db_path = "{tmp_path}/d.db"
        [runtime]
        timezone="UTC"
        default_cron="0 8 * * *"
        prefect_api_url="http://x"
        [fetcher]
        chain = ["trafilatura"]
        min_chars = 50
        on_all_failed = "skip"
        [fetchers.trafilatura]
        type = "trafilatura"
        timeout = 5
        [fetcher.cache]
        enabled = false
        ttl = "1d"
    """))
    (cfg / "providers.toml").write_text(textwrap.dedent("""
        [[providers]]
        name = "n"
        type = "openai_compatible"
        [[providers.base]]
        url = "https://llm.example"
        weight = 1
        key = [{ value = "env:K", weight = 1 }]
        [[providers.models]]
        model_name = "m"
    """))
    tpl = tmp_path / "tpl.j2"
    tpl.write_text("count={{ items|length }}")
    (cfg / "pipelines" / "p.toml").write_text(textwrap.dedent(f"""
        id = "p"
        window = "24h"
        timezone = "UTC"
        [limits]
        max_items_per_run = {cap}
        item_overflow_policy = "drop_oldest"
        [[sources]]
        type = "rss"
        url = "https://feed.example/rss"
        [[stages]]
        name = "d"
        type = "dedupe"
        [[stages]]
        name = "r"
        type = "render"
        template = "{tpl}"
        output_field = "context.markdown"
        [[sinks]]
        id = "out"
        type = "file_md"
        input = "context.markdown"
        path  = "{tmp_path}/out_{{{{ run_date }}}}.md"
        {extra_pipe_toml}
    """))
    return cfg


@pytest.mark.asyncio
async def test_requeue_works_without_dedupe_stage(tmp_path, monkeypatch):
    """Pipeline without dedupe stage must still merge requeued items."""
    import respx, httpx
    from datetime import datetime, timezone
    from sluice.state.db import open_db
    from sluice.state.failures import FailureStore
    from sluice.core.item import Item, compute_item_key

    monkeypatch.setenv("K", "v")
    cfg = _bootstrap_minimal(tmp_path, rss_text="", cap=30)
    # Remove dedupe stage from the config
    pipe_toml = (cfg / "pipelines" / "p.toml").read_text()
    pipe_toml = pipe_toml.replace("[[stages]]\nname = \"d\"\ntype = \"dedupe\"\n", "")
    (cfg / "pipelines" / "p.toml").write_text(pipe_toml)

    failed_item = Item(source_id="s", pipeline_id="p", guid="reseed",
                       url="https://x/r", title="t",
                       published_at=datetime(2026, 4, 28, tzinfo=timezone.utc),
                       raw_summary=None, fulltext="x" * 200)
    async with open_db(tmp_path / "d.db") as conn:
        await FailureStore(conn).record(
            "p", compute_item_key(failed_item), failed_item,
            stage="summarize", error_class="X", error_msg="m",
            max_retries=3,
        )

    bundle = load_all(cfg)
    fake_now = datetime(2026, 4, 28, 12, tzinfo=timezone.utc)
    empty_rss = '<?xml version="1.0"?><rss><channel></channel></rss>'
    with respx.mock() as r:
        r.get("https://feed.example/rss").mock(
            return_value=httpx.Response(200, text=empty_rss))
        result = await run_pipeline(bundle, pipeline_id="p", now=fake_now)

    assert result.status == "success"
    assert result.items_out == 1  # requeued item must survive
