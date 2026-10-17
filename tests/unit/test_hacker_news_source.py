"""hacker_news Source: no network; the pinned sender is faked."""

import json
import socket

import pytest

from sluice.errors import ExecutionError
from sluice.resolution import freeze_adapters, resolve_source
from sluice.sources import hacker_news
from sluice.web.http import DirectResponse

BASE = "https://hacker-news.firebaseio.com/v0/"
ITEMS = {
    1: {
        "id": 1,
        "type": "story",
        "by": "a",
        "time": 10,
        "title": "One",
        "url": "https://example.test/1",
        "score": 5,
        "descendants": 2,
    },
    2: {
        "id": 2,
        "type": "story",
        "by": "b",
        "time": 20,
        "title": "Ask",
        "text": "<p>hi</p>",
        "score": 9,
    },
    3: {"id": 3, "deleted": True},
    4: {"id": 4, "dead": True, "title": "Dead"},
}


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def refuse(*args, **kwargs):
        raise AssertionError("real network use")

    monkeypatch.setattr(socket, "getaddrinfo", refuse)
    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["93.184.216.34"])


@pytest.fixture
def api(monkeypatch):
    """Fake Firebase; returns the list of requested URLs."""

    class Urls(list):
        state: dict

    urls = Urls()
    state = {"ids": [1, 2, 3, 4, 5], "fail": None}

    def fake(target, addresses, request=None, binding=None, *, headers=None, **_):
        url = str(target)
        urls.append(url)
        name = url.removeprefix(BASE).removesuffix(".json")
        if name == state["fail"]:
            return DirectResponse(status_code=500, body=b"SECRET")
        if name.endswith("stories"):
            body = state["ids"]
        else:
            body = ITEMS.get(int(name.removeprefix("item/")))
        return DirectResponse(status_code=200, body=json.dumps(body).encode())

    monkeypatch.setattr("sluice.sources._http.send_pinned_httpx_request", fake)
    urls.state = state
    return urls


def run(**config):
    return hacker_news.collect(config, None).records


@pytest.mark.parametrize("feed", ["top", "new", "best", "ask", "show", "job"])
def test_each_feed_requests_its_list(api, feed):
    run(feed=feed, limit=1)
    assert api[0] == f"{BASE}{feed}stories.json"


def test_default_feed_limit_skip_and_urls(api):
    records = run(limit=4)
    # ids 1-4 requested; deleted and dead skipped; id 5 never requested
    assert [r.logical_id for r in records] == ["hn:1", "hn:2"]
    assert api[1:] == [f"{BASE}item/{i}.json" for i in (1, 2, 3, 4)]
    assert all(u.startswith(BASE) for u in api)


def test_null_item_is_skipped(api):
    api.state["ids"] = [1, 99]
    assert [r.logical_id for r in run()] == ["hn:1"]


def test_payload_is_stable_without_stats(api):
    first, ask = run(limit=2)
    assert dict(first.payload) == {
        "id": 1,
        "title": "One",
        "url": "https://example.test/1",
        "hn_url": "https://news.ycombinator.com/item?id=1",
        "by": "a",
        "time": 10,
        "type": "story",
    }
    assert ask.payload["url"] == "https://news.ycombinator.com/item?id=2"
    assert ask.payload["text"] == "<p>hi</p>"
    assert "text" not in first.payload


def test_include_stats_adds_score_and_descendants(api):
    first, ask = run(limit=2, include_stats=True)
    assert (first.payload["score"], first.payload["descendants"]) == (5, 2)
    assert ask.payload["score"] == 9 and "descendants" not in ask.payload


def test_failing_item_request_fails_run_without_body(api):
    api.state["fail"] = "item/2"
    with pytest.raises(ExecutionError) as caught:
        run(limit=3)
    assert str(caught.value) == "hacker_news HTTP 500"


@pytest.mark.parametrize(
    "config",
    [
        {"feed": "x"},
        {"limit": 0},
        {"limit": 101},
        {"include_stats": 1},
        {"url": "https://evil.test"},
        {"timeout_seconds": 0},
    ],
)
def test_bad_config_raises(api, config):
    with pytest.raises(ExecutionError):
        run(**config)
    assert api == []


def test_registry_binds_hacker_news():
    source = resolve_source("sluice.sources.hacker_news", freeze_adapters())
    assert callable(source.collect)
