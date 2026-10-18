"""Webhook Sink: no network; the pinned POST sender and DNS are faked."""

import socket
from datetime import UTC, datetime

import httpx
import pytest

from sluice.app import open_app
from sluice.contracts import SourceRecord, SourceResult
from sluice.errors import ExecutionError
from sluice.reconcile import tick
from sluice.sinks import webhook
from sluice.web.http import DirectResponse

URL = "https://hooks.example.test/in?token=SECRET-TOKEN"
KEY = "ab" * 32


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def refuse(*args, **kwargs):
        raise AssertionError("real network use")

    monkeypatch.setattr(socket, "getaddrinfo", refuse)
    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["93.184.216.34"])


@pytest.fixture
def respond(monkeypatch):
    """Install a fake sender; returns the list of (url, addresses, body, headers)."""

    calls = []

    def install(status=200):
        def fake(target, addresses, payload, request, binding, **_):
            calls.append(
                (str(target), addresses, payload, dict(request.target_headers), binding)
            )
            return DirectResponse(status_code=status, body=b"SECRET-BODY")

        monkeypatch.setattr("sluice.sinks.webhook.send_pinned_httpx_post", fake)
        return calls

    return install


def send(payload=None, **config):
    config.setdefault("url", URL)
    return webhook.send(config, {"content": "hi"} if payload is None else payload, KEY)


def test_posts_body_and_idempotency_key(respond):
    calls = respond()
    assert send() is None
    url, addresses, body, headers, binding = calls[0]
    assert url == "https://hooks.example.test/in?token=SECRET-TOKEN"
    assert addresses == ("93.184.216.34",)
    assert body == {"delivery_key": KEY, "payload": {"content": "hi"}}
    assert headers == {"Idempotency-Key": KEY}
    assert binding.max_body_bytes == 64 * 1024


def test_lowercase_idempotency_key_is_replaced_on_the_wire(monkeypatch):
    seen = []

    def handler(request):
        seen.append(request.headers.get_list("idempotency-key"))
        return httpx.Response(200)

    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["93.184.216.34"])
    monkeypatch.setattr(
        "sluice.web.http._PinnedHTTPTransport",
        lambda addresses: httpx.MockTransport(handler),
    )
    webhook.send(
        {
            "url": "https://hooks.example.test/in",
            "headers": {"idempotency-key": "attacker", "X-Source": "sluice"},
        },
        {"content": "hi"},
        "real-key",
    )
    assert seen == [["real-key"]]


def test_header_env_is_resolved_and_sent(respond, monkeypatch):
    calls = respond()
    monkeypatch.setenv("HOOK_TOKEN", "t0ken")
    send(headers={"Authorization": "env:HOOK_TOKEN", "X-Plain": "p"})
    assert calls[0][3] == {
        "Authorization": "t0ken",
        "X-Plain": "p",
        "Idempotency-Key": KEY,
    }


def test_missing_header_env_and_missing_url_env_raise(respond, monkeypatch):
    calls = respond()
    monkeypatch.delenv("HOOK_TOKEN", raising=False)
    with pytest.raises(ExecutionError, match="HOOK_TOKEN"):
        send(headers={"Authorization": "env:HOOK_TOKEN"})
    with pytest.raises(ExecutionError):
        send(url="env:HOOK_TOKEN")
    assert calls == []


def test_http_url_and_unknown_key_are_rejected(respond):
    calls = respond()
    with pytest.raises(ExecutionError, match="https"):
        send(url="http://hooks.example.test/in")
    with pytest.raises(ExecutionError, match="bogus"):
        send(bogus=1)
    assert calls == []


@pytest.mark.parametrize("status", [301, 302, 400, 500])
def test_non_2xx_raises_status_only(respond, status):
    respond(status)
    with pytest.raises(ExecutionError) as caught:
        send(headers={"X-Key": "HEADER-VALUE"})
    assert str(caught.value) == f"Webhook HTTP {status}"


def test_2xx_other_than_200_succeeds(respond):
    respond(204)
    assert send() is None


def test_default_policy_blocks_private_resolution(respond, monkeypatch):
    calls = respond()
    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["10.0.0.5"])
    with pytest.raises(ExecutionError) as caught:
        send(url="https://n8n/hook?token=SECRET-TOKEN")
    assert "n8n" not in str(caught.value) and "SECRET-TOKEN" not in str(caught.value)
    assert calls == []


def test_default_policy_blocks_literal_private_address(respond):
    calls = respond()
    with pytest.raises(ExecutionError):
        send(url="https://10.0.0.5/hook")
    assert calls == []


def test_target_allow_hosts_permits_private_resolution(respond, monkeypatch):
    calls = respond()
    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["10.0.0.5"])
    send(url="https://n8n/hook", target_allow_hosts=["n8n"])
    assert calls[0][1] == ("10.0.0.5",)


def test_target_allow_cidrs_permits_private_address(respond, monkeypatch):
    calls = respond()
    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: [host])
    send(url="https://10.0.0.5/hook", target_allow_cidrs=["10.0.0.0/8"])
    assert calls[0][1] == ("10.0.0.5",)


def test_invalid_allow_lists_are_rejected(respond):
    respond()
    with pytest.raises(ExecutionError):
        send(target_allow_hosts="n8n")
    with pytest.raises(ExecutionError, match="CIDR"):
        send(target_allow_cidrs=["not-a-cidr"])


def test_oversize_body_fails_before_any_request(respond):
    calls = respond()
    with pytest.raises(ExecutionError, match="1 MiB"):
        send(payload="x" * (1024 * 1024))
    assert calls == []


def source(config, checkpoint):
    return SourceResult((SourceRecord("one", {"content": "Hello 世界"}),), "done")


def test_tick_delivers_once_and_retry_reuses_the_key(tmp_path, respond, monkeypatch):
    calls = respond()
    monkeypatch.setenv("HOOK_URL", URL)
    definition = tmp_path / "d.yml"
    definition.write_text("""version: 1
sources:
  feed: {use: tests.unit.test_webhook_sink.source}
pipelines:
  p:
    nodes:
      hook:
        from: source.feed
        deliver: {use: sluice.sinks.webhook, with: {url: 'env:HOOK_URL'}}
""")
    app = open_app(definition, tmp_path / "state.db")
    try:
        tick(app, datetime(2026, 1, 1, tzinfo=UTC))
        rows = app.conn.execute(
            "SELECT delivery_key,state,config_json FROM deliveries"
        ).fetchall()
        assert [r["state"] for r in rows] == ["succeeded"]
        assert "SECRET-TOKEN" not in "\n".join(app.conn.iterdump())
        assert calls[0][2]["delivery_key"] == rows[0]["delivery_key"]
        assert calls[0][2]["payload"] == {"content": "Hello 世界"}
        assert calls[0][3]["Idempotency-Key"] == rows[0]["delivery_key"]

        tick(app, datetime(2026, 1, 1, 0, 1, tzinfo=UTC))
        assert len(calls) == 1
    finally:
        app.close()


def test_failed_attempt_resends_with_the_same_key(tmp_path, monkeypatch):
    from sluice.delivery import retry_delivery

    statuses = iter([500, 200])
    keys = []

    def fake(target, addresses, payload, request, binding, **_):
        keys.append(request.target_headers["Idempotency-Key"])
        return DirectResponse(status_code=next(statuses), body=b"")

    monkeypatch.setattr("sluice.sinks.webhook.send_pinned_httpx_post", fake)
    monkeypatch.setenv("HOOK_URL", URL)
    definition = tmp_path / "d.yml"
    definition.write_text("""version: 1
sources:
  feed: {use: tests.unit.test_webhook_sink.source}
pipelines:
  p:
    nodes:
      hook:
        from: source.feed
        deliver: {use: sluice.sinks.webhook, with: {url: 'env:HOOK_URL'}}
""")
    app = open_app(definition, tmp_path / "state.db")
    try:
        tick(app, datetime(2026, 1, 1, tzinfo=UTC))
        key, state = app.conn.execute(
            "SELECT delivery_key,state FROM deliveries"
        ).fetchone()
        assert state == "failed"
        retry_delivery(app, key, datetime(2026, 1, 1, 0, 1, tzinfo=UTC))
        assert (
            app.conn.execute("SELECT state FROM deliveries").fetchone()[0]
            == "succeeded"
        )
        assert keys == [key, key]
    finally:
        app.close()
