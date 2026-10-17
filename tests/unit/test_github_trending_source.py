"""github_trending Source: no network; the pinned sender is faked.

ROWS is two rows trimmed from the real https://github.com/trending page
(fetched 2026-10-17); the second row has no description.
"""

import socket

import pytest

from sluice.errors import ExecutionError
from sluice.resolution import freeze_adapters, resolve_source
from sluice.sources import github_trending
from sluice.web.http import DirectResponse

ROWS = """<article class="Box-row">
<h2 class="h3 lh-condensed">
<a class="Link" href="/anthropics/claude-plugins-official">
<span class="text-normal">
        anthropics /
</span>
      claude-plugins-official</a> </h2>
<p class="col-9 color-fg-muted my-1 tmp-pr-4">
      Official, Anthropic-managed directory of high quality Claude Code Plugins.
    </p>
<div class="f6 color-fg-muted mt-2">
<span class="tmp-mr-3 d-inline-block ml-0 tmp-ml-0">
<span class="repo-language-color"></span>
<span itemprop="programmingLanguage">Python</span>
</span>
<a class="tmp-mr-3 Link Link--muted d-inline-block" href="/anthropics/claude-plugins-official/stargazers">
        41,252</a>
<a class="tmp-mr-3 Link Link--muted d-inline-block" href="/anthropics/claude-plugins-official/forks">
        4,440</a>
<span class="tmp-mr-3 d-inline-block">
        Built by
</span>
<span class="d-inline-block float-sm-right">
        1,383 stars today
</span> </div>
</article>
<article class="Box-row">
<h2 class="h3 lh-condensed">
<a class="Link" href="/android/skills">
<span class="text-normal">
        android /
</span>
      skills</a> </h2>
<div class="f6 color-fg-muted mt-2">
<span class="tmp-mr-3 d-inline-block ml-0 tmp-ml-0">
<span class="repo-language-color"></span>
<span itemprop="programmingLanguage">Kotlin</span>
</span>
<a class="tmp-mr-3 Link Link--muted d-inline-block" href="/android/skills/stargazers">
        8,293</a>
<a class="tmp-mr-3 Link Link--muted d-inline-block" href="/android/skills/forks">
        548</a>
<span class="tmp-mr-3 d-inline-block">
        Built by
</span>
<span class="d-inline-block float-sm-right">
        368 stars today
</span> </div>
</article>
"""
BROKEN_ROW = '<article class="Box-row"><h2><a href="/login">x</a></h2></article>'


def page(rows):
    return f"<html><body>{rows}</body></html>".encode()


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def refuse(*args, **kwargs):
        raise AssertionError("real network use")

    monkeypatch.setattr(socket, "getaddrinfo", refuse)
    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["93.184.216.34"])


@pytest.fixture
def site(monkeypatch):
    """Fake github.com; returns requested URLs, `state` sets the response."""

    class Urls(list):
        state: dict

    urls = Urls()
    state = {"status": 200, "type": "text/html; charset=utf-8", "body": page(ROWS)}

    def fake(target, addresses, request=None, binding=None, *, headers=None, **_):
        urls.append(str(target))
        return DirectResponse(
            status_code=state["status"],
            headers={"content-type": state["type"]},
            body=state["body"],
        )

    monkeypatch.setattr("sluice.sources._http.send_pinned_httpx_request", fake)
    urls.state = state
    return urls


def run(**config):
    return github_trending.collect(config, None).records


def test_default_request_and_payload_without_stats(site):
    first, second = run()
    assert site == ["https://github.com/trending?since=daily"]
    assert first.logical_id == "anthropics/claude-plugins-official"
    assert dict(first.payload) == {
        "repo": "anthropics/claude-plugins-official",
        "url": "https://github.com/anthropics/claude-plugins-official",
        "description": (
            "Official, Anthropic-managed directory of high quality Claude Code Plugins."
        ),
        "language": "Python",
    }
    assert second.logical_id == "android/skills"
    assert "description" not in second.payload


def test_include_stats_adds_integers(site):
    first, second = run(include_stats=True)
    assert (first.payload["stars_total"], first.payload["forks"]) == (41252, 4440)
    assert first.payload["stars_period"] == 1383
    assert second.payload["stars_period"] == 368


def test_since_language_and_limit(site):
    records = run(since="weekly", language="C++", limit=1)
    assert site == ["https://github.com/trending/C%2B%2B?since=weekly"]
    assert len(records) == 1


def test_row_without_valid_repo_is_skipped(site):
    site.state["body"] = page(BROKEN_ROW + ROWS)
    assert len(run()) == 2


def test_duplicate_repo_keeps_first(site):
    site.state["body"] = page(ROWS + ROWS)
    assert len(run()) == 2


@pytest.mark.parametrize("body", [b"<html></html>", page(BROKEN_ROW)])
def test_page_without_valid_rows_fails_instead_of_empty_success(site, body):
    site.state["body"] = body
    with pytest.raises(ExecutionError, match="layout not recognized"):
        run()


def test_non_html_content_type_fails(site):
    site.state["type"] = "application/json"
    with pytest.raises(ExecutionError, match="not text/html"):
        run()


def test_http_error_reports_status_only(site):
    site.state["status"] = 429
    with pytest.raises(ExecutionError) as caught:
        run()
    assert str(caught.value) == "github_trending HTTP 429"


@pytest.mark.parametrize(
    "config",
    [
        {"since": "hourly"},
        {"language": "../x"},
        {"language": ""},
        {"language": 3},
        {"limit": 0},
        {"limit": 51},
        {"include_stats": "yes"},
        {"url": "https://evil.test"},
        {"timeout_seconds": 301},
    ],
)
def test_bad_config_raises_before_any_request(site, config):
    with pytest.raises(ExecutionError):
        run(**config)
    assert site == []


def test_registry_binds_github_trending():
    source = resolve_source("sluice.sources.github_trending", freeze_adapters())
    assert callable(source.collect)
