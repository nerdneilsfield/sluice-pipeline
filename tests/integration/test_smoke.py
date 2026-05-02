import textwrap
from datetime import datetime, timezone

import httpx
import pytest
import respx

import sluice.fetchers.trafilatura_fetcher  # noqa
import sluice.processors.dedupe  # noqa
import sluice.processors.fetcher_apply  # noqa
import sluice.processors.filter  # noqa
import sluice.processors.llm_stage  # noqa
import sluice.processors.render  # noqa
import sluice.sinks.file_md  # noqa
import sluice.sources.rss  # noqa
from sluice.loader import load_all
from sluice.runner import run_pipeline

RSS = """<?xml version='1.0'?><rss><channel>
<item><title>OpenAI GPT-X</title><link>https://o.example/x</link>
<guid>g-x</guid><pubDate>Mon, 28 Apr 2026 06:00:00 +0000</pubDate></item>
<item><title>Anthropic Claude-Y</title><link>https://a.example/y</link>
<guid>g-y</guid><pubDate>Mon, 28 Apr 2026 04:00:00 +0000</pubDate></item>
</channel></rss>"""


@pytest.mark.asyncio
async def test_full_pipeline_with_mocked_llm(tmp_path, monkeypatch):
    import socket

    monkeypatch.setenv("K", "v")

    def fake_getaddrinfo(host, port, *args, **kwargs):
        return [(socket.AF_INET, socket.SOCK_STREAM, 0, "", ("93.184.216.34", 0))]

    monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)

    async def fake_extract(self, url):
        return f"Fetched article from {url}. " + ("This is a long article about AI. " * 40)

    monkeypatch.setattr(
        "sluice.fetchers.trafilatura_fetcher.TrafilaturaFetcher.extract",
        fake_extract,
    )

    cfg = tmp_path / "configs"
    (cfg / "pipelines").mkdir(parents=True)
    (cfg / "sluice.toml").write_text(
        textwrap.dedent(f"""
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
        timeout = 10
        [fetcher.cache]
        enabled = false
        ttl = "1d"
    """)
    )
    (cfg / "providers.toml").write_text(
        textwrap.dedent("""
        [[providers]]
        name = "n"
        type = "openai_compatible"
        [[providers.base]]
        url = "https://llm.example"
        weight = 1
        key = [{ value = "env:K", weight = 1 }]
        [[providers.models]]
        model_name = "m"
        input_price_per_1k = 0.0001
        output_price_per_1k = 0.0001
    """)
    )
    sp = tmp_path / "sum.md"
    sp.write_text("S: {{ item.fulltext }}")
    dp = tmp_path / "daily.md"
    dp.write_text("Brief({{ items|length }})")
    tp = tmp_path / "tpl.j2"
    tp.write_text(
        "# {{ run_date }}\\n{{ context.daily_brief }}\\n"
        "{% for it in items %}- {{ it.summary }}\\n{% endfor %}"
        "stats={{ stats.items_out }}/{{ stats.llm_calls }}/"
        "{{ '%.6f'|format(stats.est_cost_usd) }}\\n"
    )
    (cfg / "pipelines" / "p.toml").write_text(
        textwrap.dedent(f"""
        id = "p"
        window = "24h"
        timezone = "UTC"
        [[sources]]
        type = "rss"
        url = "https://feed.example/rss"
        [[stages]]
        name = "d"
        type = "dedupe"
        [[stages]]
        name = "f"
        type = "fetcher_apply"
        write_field = "fulltext"
        [[stages]]
        name = "s"
        type = "llm_stage"
        mode = "per_item"
        input_field = "fulltext"
        output_field = "summary"
        prompt_file = "{sp}"
        model = "n/m"
        workers = 2
        [[stages]]
        name = "a"
        type = "llm_stage"
        mode = "aggregate"
        input_field = "summary"
        output_target = "context.daily_brief"
        prompt_file = "{dp}"
        model = "n/m"
        [[stages]]
        name = "r"
        type = "render"
        template = "{tp}"
        output_field = "context.markdown"
        [[sinks]]
        id = "out"
        type = "file_md"
        input = "context.markdown"
        path = "{tmp_path}/out_{{run_date}}.md"
    """)
    )

    bundle = load_all(cfg)
    fake_now = datetime(2026, 4, 28, 12, tzinfo=timezone.utc)
    with respx.mock(assert_all_called=False) as r:
        r.get("https://feed.example/rss").mock(return_value=httpx.Response(200, text=RSS))
        # 2 per_item summarize calls + 1 aggregate brief
        llm_route = r.post("https://llm.example/chat/completions").mock(
            side_effect=[
                httpx.Response(
                    200,
                    json={
                        "choices": [{"message": {"content": "Summary X"}}],
                        "usage": {"prompt_tokens": 100, "completion_tokens": 10},
                    },
                ),
                httpx.Response(
                    200,
                    json={
                        "choices": [{"message": {"content": "Summary Y"}}],
                        "usage": {"prompt_tokens": 100, "completion_tokens": 10},
                    },
                ),
                httpx.Response(
                    200,
                    json={
                        "choices": [{"message": {"content": "TODAY'S BRIEF"}}],
                        "usage": {"prompt_tokens": 200, "completion_tokens": 20},
                    },
                ),
            ]
        )
        result = await run_pipeline(bundle, pipeline_id="p", now=fake_now)
        assert llm_route.call_count == 3

    assert result.status == "success" and result.items_out == 2
    out = (tmp_path / "out_2026-04-28.md").read_text()
    assert "TODAY'S BRIEF" in out
    assert "Summary X" in out and "Summary Y" in out
    assert "stats=2/3/0.000044" in out
