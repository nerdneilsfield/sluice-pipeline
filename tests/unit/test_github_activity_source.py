"""github_activity Source: no network; the pinned sender is faked."""

import json
import socket
from datetime import UTC, datetime

import pytest

from sluice.app import open_app
from sluice.errors import ExecutionError
from sluice.resolution import freeze_adapters, resolve_source
from sluice.source_runtime import collect_source
from sluice.sources import github_activity
from sluice.web.http import DirectResponse

REPO = "python/cpython"
API = f"https://api.github.com/repos/{REPO}/"
TOKEN = "ghp_SECRET_TOKEN_VALUE"
RELEASES = [
    {
        "id": 11,
        "tag_name": "v1.0",
        "name": "One",
        "html_url": "https://github.com/python/cpython/releases/tag/v1.0",
        "body": "notes",
        "prerelease": False,
        "published_at": "2026-01-01T00:00:00Z",
        "author": {"login": "bob"},
    },
    {"id": 12, "draft": True, "tag_name": "v2.0-draft"},
    {"id": 13, "tag_name": "v1.1rc1", "prerelease": True},
]
COMMITS = [
    {
        "sha": "abc123",
        "html_url": "https://github.com/python/cpython/commit/abc123",
        "commit": {
            "message": "Fix bug\n\nLong explanation",
            "author": {"name": "Ann", "date": "2026-02-02T00:00:00Z"},
        },
    }
]


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def refuse(*args, **kwargs):
        raise AssertionError("real network use")

    monkeypatch.setattr(socket, "getaddrinfo", refuse)
    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["93.184.216.34"])


@pytest.fixture
def api(monkeypatch):
    """Fake api.github.com; returns calls as (url, headers); `state` sets replies."""

    class Calls(list):
        state: dict

    calls = Calls()
    state = {"status": {}, "releases": RELEASES, "commits": COMMITS}

    def fake(target, addresses, request=None, binding=None, *, headers=None, **_):
        url = str(target)
        calls.append((url, dict(headers or {})))
        kind = url.removeprefix(API).split("?")[0]
        status = state["status"].get(kind, 200)
        return DirectResponse(status_code=status, body=json.dumps(state[kind]).encode())

    monkeypatch.setattr("sluice.sources._http.send_pinned_httpx_request", fake)
    calls.state = state
    return calls


def run(**config):
    config.setdefault("repo", REPO)
    return github_activity.collect(config, None).records


def test_releases_and_commits_are_mapped(api):
    records = run()
    assert [r.logical_id for r in records] == [
        f"github:{REPO}:release:11",
        f"github:{REPO}:release:13",
        f"github:{REPO}:commit:abc123",
    ]
    assert dict(records[0].payload) == {
        "kind": "release",
        "repo": REPO,
        "tag": "v1.0",
        "name": "One",
        "url": "https://github.com/python/cpython/releases/tag/v1.0",
        "body": "notes",
        "prerelease": False,
        "published_at": "2026-01-01T00:00:00Z",
        "author": "bob",
    }
    assert records[1].payload["prerelease"] is True
    assert dict(records[2].payload) == {
        "kind": "commit",
        "repo": REPO,
        "sha": "abc123",
        "title": "Fix bug",
        "message": "Fix bug\n\nLong explanation",
        "author": "Ann",
        "date": "2026-02-02T00:00:00Z",
        "url": "https://github.com/python/cpython/commit/abc123",
    }
    assert [url for url, _ in api] == [
        f"{API}releases?per_page=30",
        f"{API}commits?per_page=30",
    ]


@pytest.mark.parametrize("events", [["releases"], ["commits"]])
def test_events_subset_requests_only_that_kind(api, events):
    run(events=events, limit=5)
    assert [url for url, _ in api] == [f"{API}{events[0]}?per_page=5"]


def test_branch_is_sent_as_sha_for_commits_only(api):
    run(branch="release/3.12")
    assert api[0][0] == f"{API}releases?per_page=30"
    assert api[1][0] == f"{API}commits?per_page=30&sha=release%2F3.12"


def test_no_token_sends_no_authorization(api):
    run()
    for _, headers in api:
        assert headers == {
            "Accept": "application/vnd.github+json",
            "User-Agent": "sluice",
        }


def test_token_is_sent_as_bearer_to_api_host_only(api, monkeypatch):
    monkeypatch.setenv("GH_TOKEN", TOKEN)
    run(token="env:GH_TOKEN")
    assert all(url.startswith("https://api.github.com/") for url, _ in api)
    assert all(h["Authorization"] == f"Bearer {TOKEN}" for _, h in api)


@pytest.mark.parametrize("token", [TOKEN, "env:1BAD", "env:A B", "env:", "", 5])
def test_literal_or_malformed_token_is_rejected_before_any_request(api, token):
    with pytest.raises(ExecutionError) as caught:
        run(token=token)
    assert TOKEN not in str(caught.value)
    assert api == []


def test_missing_token_env_fails_without_unauthenticated_fallback(api, monkeypatch):
    monkeypatch.delenv("GH_TOKEN", raising=False)
    with pytest.raises(ExecutionError, match="GH_TOKEN"):
        run(token="env:GH_TOKEN")
    assert api == []


@pytest.mark.parametrize("status", [301, 403, 404, 409, 429])
def test_http_failure_reports_status_only(api, monkeypatch, status):
    monkeypatch.setenv("GH_TOKEN", TOKEN)
    api.state["status"]["releases"] = status
    with pytest.raises(ExecutionError) as caught:
        run(token="env:GH_TOKEN")
    assert str(caught.value) == f"GitHub API HTTP {status}"
    assert len(api) == 1


def test_second_request_failure_fails_whole_run(api):
    api.state["status"]["commits"] = 500
    with pytest.raises(ExecutionError, match="HTTP 500"):
        run()


@pytest.mark.parametrize(
    "config",
    [
        {"repo": "nope"},
        {"repo": "a/b/c"},
        {"repo": "../x"},
        {"repo": "a/.."},
        {"repo": None},
        {"events": []},
        {"events": ["issues"]},
        {"events": ["commits", "commits"]},
        {"events": "commits"},
        {"branch": ""},
        {"limit": 0},
        {"limit": 101},
        {"timeout_seconds": 0},
        {"url": "https://evil.test"},
    ],
)
def test_bad_config_raises_before_any_request(api, config):
    with pytest.raises(ExecutionError):
        run(**config)
    assert api == []


def test_token_never_reaches_sqlite_or_error_text(api, monkeypatch, tmp_path):
    monkeypatch.setenv("GH_TOKEN", TOKEN)
    definition = tmp_path / "d.yml"
    definition.write_text(
        "version: 1\ndatabase: state.db\nsources:\n"
        "  gh:\n    use: sluice.sources.github_activity\n"
        f"    with: {{repo: {REPO}, token: 'env:GH_TOKEN'}}\n",
        encoding="utf-8",
    )
    app = open_app(definition, tmp_path / "state.db")
    collect_source(app, "gh", datetime(2026, 8, 14, tzinfo=UTC))
    assert app.conn.execute("SELECT COUNT(*) FROM items").fetchone()[0] == 3
    stored = app.conn.execute("SELECT config_json FROM executions").fetchone()[0]
    assert "env:GH_TOKEN" in stored
    app.conn.commit()
    dump = "\n".join(app.conn.iterdump())
    assert TOKEN not in dump
    # a failing run records an error that must not carry the token either
    api.state["status"]["releases"] = 403
    with pytest.raises(ExecutionError):
        collect_source(app, "gh", datetime(2026, 8, 14, 0, 1, tzinfo=UTC))
    errors = app.conn.execute("SELECT error FROM executions").fetchall()
    assert any("HTTP 403" in (e[0] or "") for e in errors)
    assert TOKEN not in "\n".join(app.conn.iterdump())


def test_registry_binds_github_activity():
    source = resolve_source("sluice.sources.github_activity", freeze_adapters())
    assert callable(source.collect)
