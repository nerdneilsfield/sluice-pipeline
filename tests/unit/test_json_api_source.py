"""json_api Source: no network; the pinned sender and DNS are faked."""

import json
import socket

import pytest

from sluice.errors import ExecutionError
from sluice.resolution import freeze_adapters, resolve_source
from sluice.sources import json_api
from sluice.web.http import DirectResponse

URL = "https://api.example.test/v1/posts?token=SECRET-QUERY"


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def refuse(*args, **kwargs):
        raise AssertionError("real network use")

    monkeypatch.setattr(socket, "getaddrinfo", refuse)
    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["93.184.216.34"])


@pytest.fixture
def respond(monkeypatch):
    """Install a fake sender; returns the list of (url, addresses, headers) calls."""

    calls = []

    def install(body=b"[]", status=200):
        def fake(target, addresses, request=None, binding=None, *, headers=None, **_):
            calls.append((str(target), addresses, dict(headers or {}), binding))
            data = body if isinstance(body, bytes) else json.dumps(body).encode()
            return DirectResponse(status_code=status, body=data)

        monkeypatch.setattr("sluice.sources._http.send_pinned_httpx_request", fake)
        return calls

    return install


def run(**config):
    config.setdefault("url", URL)
    config.setdefault("id_field", "id")
    return json_api.collect(config, None).records


def test_list_at_root_with_int_and_str_ids(respond):
    respond([{"id": 1, "title": "a"}, {"id": "b", "author": {"name": "n"}}])
    records = run()
    assert [r.logical_id for r in records] == ["1", "b"]
    assert records[1].payload["author"]["name"] == "n"
    assert dict(records[0].payload) == {"id": 1, "title": "a"}


def test_items_path_and_dotted_id_field(respond):
    respond({"data": {"items": [{"meta": {"key": "k1"}}, {"meta": {"key": "k2"}}]}})
    records = run(items_path="data.items", id_field="meta.key")
    assert [r.logical_id for r in records] == ["k1", "k2"]


def test_limit_keeps_first_n_and_duplicates_first_wins(respond):
    respond([{"id": 1, "v": "first"}, {"id": 1, "v": "dup"}, {"id": 2}, {"id": 3}])
    records = run(limit=3)
    assert [r.logical_id for r in records] == ["1", "2"]
    assert records[0].payload["v"] == "first"


def test_empty_list_is_valid(respond):
    respond([])
    assert run() == ()


@pytest.mark.parametrize(
    "body, config",
    [
        ([1], {}),
        ([{"id": True}], {}),
        ([{"id": ""}], {}),
        ([{"id": 1.5}], {}),
        ([{"x": 1}], {}),
        ({"a": 1}, {}),
        ({"a": 1}, {"items_path": "missing"}),
        ({"a": {"b": 1}}, {"items_path": "a.b"}),
        (b"not json", {}),
        (b"[NaN]", {}),
    ],
)
def test_bad_shapes_raise(respond, body, config):
    respond(body)
    with pytest.raises(ExecutionError):
        run(**config)


@pytest.mark.parametrize(
    "config",
    [
        {"bogus": 1},
        {"limit": 0},
        {"limit": 1001},
        {"limit": True},
        {"timeout_seconds": 0},
        {"timeout_seconds": 301},
        {"headers": {"X": 1}},
        {"target_allow_hosts": "n8n"},
        {"target_allow_cidrs": ["not-a-cidr"]},
    ],
)
def test_bad_config_raises(respond, config):
    respond([])
    with pytest.raises(ExecutionError):
        run(**config)


def test_env_header_is_resolved_and_sent(respond, monkeypatch):
    calls = respond([])
    monkeypatch.setenv("API_TOKEN", "t0ken")
    run(headers={"Authorization": "env:API_TOKEN", "X-Plain": "plain"})
    assert calls[0][2] == {"Authorization": "t0ken", "X-Plain": "plain"}


def test_missing_env_header_raises(respond, monkeypatch):
    respond([])
    monkeypatch.delenv("API_TOKEN", raising=False)
    with pytest.raises(ExecutionError, match="API_TOKEN"):
        run(headers={"Authorization": "env:API_TOKEN"})


def test_forbidden_header_is_rejected(respond):
    respond([])
    with pytest.raises(ExecutionError):
        run(headers={"Cookie": "a=b"})


@pytest.mark.parametrize("status", [302, 404, 500])
def test_non_200_raises_status_only(respond, status):
    respond(b"SECRET-BODY", status=status)
    with pytest.raises(ExecutionError) as caught:
        run()
    assert str(caught.value) == f"json_api HTTP {status}"


def test_oversize_body_limit_is_passed_to_sender(respond):
    calls = respond([])
    run()
    assert calls[0][3].max_body_bytes == 5 * 1024 * 1024


def test_default_policy_blocks_private_resolution(respond, monkeypatch):
    calls = respond([])
    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["10.0.0.5"])
    with pytest.raises(ExecutionError) as caught:
        run(url="http://n8n/hook")
    assert "n8n" not in str(caught.value)
    assert calls == []


def test_default_policy_blocks_literal_private_address(respond):
    calls = respond([])
    with pytest.raises(ExecutionError):
        run(url="http://10.0.0.5/")
    assert calls == []


def test_target_allow_hosts_permits_private_resolution(respond, monkeypatch):
    calls = respond([{"id": 1}])
    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["10.0.0.5"])
    records = run(url="http://n8n/hook", target_allow_hosts=["n8n"])
    assert [r.logical_id for r in records] == ["1"]
    assert calls[0][1] == ("10.0.0.5",)


def test_target_allow_hosts_is_exact_host_only(respond, monkeypatch):
    respond([])
    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["10.0.0.5"])
    with pytest.raises(ExecutionError):
        run(url="http://other/hook", target_allow_hosts=["n8n"])


def test_target_allow_cidrs_permits_private_address(respond, monkeypatch):
    calls = respond([{"id": 1}])
    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["10.0.0.5"])
    run(url="http://10.0.0.5/", target_allow_cidrs=["10.0.0.0/8"])
    run(url="http://svc.lan/", target_allow_cidrs=["10.0.0.0/8"])
    assert [c[1] for c in calls] == [("10.0.0.5",), ("10.0.0.5",)]


def test_target_allow_cidrs_does_not_cover_other_ranges(respond, monkeypatch):
    respond([])
    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["192.168.1.9"])
    with pytest.raises(ExecutionError):
        run(url="http://svc.lan/", target_allow_cidrs=["10.0.0.0/8"])


def test_registry_binds_json_api():
    source = resolve_source("sluice.sources.json_api", freeze_adapters())
    assert callable(source.collect)
