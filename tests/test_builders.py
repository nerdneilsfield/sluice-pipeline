import pytest

import sluice.fetchers.firecrawl  # noqa
import sluice.fetchers.trafilatura_fetcher  # noqa
import sluice.processors.dedupe  # noqa
import sluice.processors.filter  # noqa
import sluice.sources.rss  # noqa
from sluice.builders import (
    _resolve_template,
    build_fetcher_chain,
    build_processors,
    build_sinks,
    build_sources,
)
from sluice.config import (
    DedupeConfig,
    EmailSinkConfig,
    FetcherImplConfig,
    FileMdSinkConfig,
    FilterConfig,
    FilterRule,
    GlobalConfig,
    PipelineConfig,
    RssSourceConfig,
)
from sluice.core.errors import ConfigError
from tests.conftest import make_ctx


def test_build_rss_source():
    cfg = PipelineConfig(
        id="p",
        window="24h",
        sources=[RssSourceConfig(type="rss", url="https://x/feed", tag="ai")],
        stages=[DedupeConfig(type="dedupe", name="d")],
        sinks=[
            FileMdSinkConfig(
                id="x", type="file_md", input="context.markdown", path="./{run_date}.md"
            )
        ],
    )
    sources = build_sources(cfg)
    assert len(sources) == 1 and sources[0].url == "https://x/feed"
    assert sources[0].tags == ["ai"]


@pytest.mark.asyncio
async def test_build_fetcher_chain():

    g = GlobalConfig(fetchers={"trafilatura": FetcherImplConfig(type="trafilatura", timeout=5)})
    p = PipelineConfig(
        id="p",
        window="24h",
        sources=[RssSourceConfig(type="rss", url="https://x")],
        stages=[DedupeConfig(type="dedupe", name="d")],
        sinks=[FileMdSinkConfig(id="x", type="file_md", input="context.markdown", path="./x.md")],
        fetcher={"chain": ["trafilatura"], "min_chars": 100},
    )
    chain = build_fetcher_chain(g, p, cache=None)
    assert chain.min_chars == 100
    assert len(chain.fetchers) == 1


@pytest.mark.asyncio
async def test_build_processors():
    from sluice.state.db import open_db
    from sluice.state.failures import FailureStore
    from sluice.state.seen import SeenStore

    g = GlobalConfig()
    p = PipelineConfig(
        id="p",
        window="24h",
        sources=[RssSourceConfig(type="rss", url="https://x")],
        stages=[
            FilterConfig(
                type="filter",
                name="f",
                mode="keep_if_all",
                rules=[FilterRule(field="title", op="eq", value="x")],
            )
        ],
        sinks=[FileMdSinkConfig(id="x", type="file_md", input="context.markdown", path="./x.md")],
    )
    async with open_db(":memory:") as db:
        seen = SeenStore(db)
        failures = FailureStore(db)
        procs = build_processors(
            pipe=p,
            global_cfg=g,
            seen=seen,
            failures=failures,
            fetcher_chain=None,
            llm_pool=None,
            budget=None,
        )
        assert len(procs) == 1
        assert procs[0].name == "f"


def test_resolve_template_reads_file(tmp_path):
    f = tmp_path / "prompts" / "item.md"
    f.parent.mkdir(parents=True)
    f.write_text("Hello {{ item.title }}")
    result = _resolve_template(tmp_path, "prompts/item.md")
    assert result == "Hello {{ item.title }}"


def test_resolve_template_literal_string_passthrough():
    result = _resolve_template(None, "{{ item.title }}\n{{ item.url }}")
    assert result == "{{ item.title }}\n{{ item.url }}"


def test_resolve_template_missing_file_falls_back_to_value(tmp_path):
    # Missing template file: warn and return the path string as literal template.
    result = _resolve_template(tmp_path, "prompts/missing.md")
    assert result == "prompts/missing.md"


def test_resolve_template_empty_string():
    assert _resolve_template(None, "") == ""


def test_email_style_block_file_injected_end_to_end(tmp_path):
    css = tmp_path / "prompts" / "email.css"
    css.parent.mkdir(parents=True)
    css.write_text("body { color: red; }")
    cfg = PipelineConfig(
        id="p",
        window="24h",
        sources=[RssSourceConfig(type="rss", url="https://x/feed")],
        stages=[DedupeConfig(type="dedupe", name="d")],
        sinks=[
            EmailSinkConfig(
                id="em",
                type="email",
                smtp_host="smtp.example",
                smtp_username="u",
                smtp_password="p",
                from_address="from@example.com",
                recipients=["to@example.com"],
                items_input="none",
                style_block_file="prompts/email.css",
            )
        ],
    )
    sink = build_sinks(cfg, delivery_log=None, root=tmp_path)[0]
    batch = sink.build_batch(make_ctx(items=[]))
    html = batch[0].payload.get_payload()[-1].get_content()
    assert "body { color: red; }" in html


def test_resolve_template_missing_file_without_root_falls_back_to_value():
    result = _resolve_template(None, "missing.md")
    assert result == "missing.md"
