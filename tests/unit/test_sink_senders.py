"""Built-in sender wire contracts; no external messages are sent."""

import base64
import hashlib
import hmac
from datetime import UTC, datetime

import httpx
import pytest

from sluice.app import open_app
from sluice.errors import ExecutionError
from sluice.reconcile import tick
from sluice.sinks import _common, email, feishu, telegram


def source(config, checkpoint):
    from sluice.contracts import SourceRecord, SourceResult

    return SourceResult((SourceRecord("one", {"content": "Hello 世界"}),), "done")


def test_three_builtin_sinks_work_without_startup_injection(tmp_path, monkeypatch):
    requests = []
    mails = []
    client_type = httpx.Client

    def transport(request):
        import json

        body = json.loads(request.content)
        requests.append((str(request.url), body))
        if request.url.host == "api.telegram.org":
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 42}})
        return httpx.Response(200, json={"code": 0})

    monkeypatch.setattr(
        _common.httpx,
        "Client",
        lambda **kw: client_type(transport=httpx.MockTransport(transport), **kw),
    )
    monkeypatch.setenv("BOT_TOKEN", "123:secret")
    monkeypatch.setenv(
        "FEISHU_HOOK", "https://open.feishu.cn/open-apis/bot/v2/hook/test"
    )
    monkeypatch.setenv("FEISHU_SECRET", "signing-secret")
    monkeypatch.setenv("SMTP_PASSWORD", "mail-secret")
    monkeypatch.setattr(feishu.time, "time", lambda: 1700000000)

    class SMTP:
        def __init__(self, host, port, **kwargs):
            assert (host, port) == ("smtp.example.test", 587)
            self.secure = False

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def starttls(self, **kwargs):
            self.secure = True

        def login(self, username, password):
            assert self.secure
            assert (username, password) == ("user", "mail-secret")

        def send_message(self, message):
            assert self.secure
            mails.append(message)
            return {}

    monkeypatch.setattr("smtplib.SMTP", SMTP)
    definition = tmp_path / "channels.yml"
    definition.write_text("""version: 1
sources:
  feed: {use: tests.unit.test_sink_senders.source}
pipelines:
  notifications:
    nodes:
      telegram:
        from: source.feed
        deliver:
          use: sluice.sinks.telegram
          with: {bot_token: 'env:BOT_TOKEN', chat_id: '-100123'}
      email:
        from: source.feed
        deliver:
          use: sluice.sinks.email
          with: {smtp_host: smtp.example.test, username: user, password: 'env:SMTP_PASSWORD', from: sender@example.test, to: reader@example.test}
      feishu:
        from: source.feed
        deliver:
          use: sluice.sinks.feishu
          with: {webhook: 'env:FEISHU_HOOK', secret: 'env:FEISHU_SECRET'}
""")
    app = open_app(definition, tmp_path / "state.db")
    try:
        tick(app, datetime(2026, 1, 1, tzinfo=UTC))
        rows = app.conn.execute(
            "SELECT node_name,state,external_id FROM deliveries ORDER BY node_name"
        ).fetchall()
        assert [(r[0], r[1]) for r in rows] == [
            ("email", "succeeded"),
            ("feishu", "succeeded"),
            ("telegram", "succeeded"),
        ]
        assert rows[2][2] == "42"
        assert mails[0]["Message-ID"] == rows[0][2]
        assert mails[0].get_content().strip() == "Hello 世界"
        dump = "\n".join(app.conn.iterdump())
        assert (
            "mail-secret" not in dump
            and "signing-secret" not in dump
            and "123:secret" not in dump
        )
    finally:
        app.close()
    telegram_body = next(body for url, body in requests if "telegram.org" in url)
    assert telegram_body == {"chat_id": "-100123", "text": "Hello 世界"}
    feishu_body = next(body for url, body in requests if "feishu.cn" in url)
    expected = base64.b64encode(
        hmac.new(b"1700000000\nsigning-secret", b"", hashlib.sha256).digest()
    ).decode()
    assert feishu_body["sign"] == expected
    assert feishu_body["content"] == {"text": "Hello 世界"}


@pytest.mark.parametrize(
    "sender,config,result",
    [
        (
            telegram.send,
            {"bot_token": "123:secret", "chat_id": "1"},
            {"ok": False, "description": "secret"},
        ),
        (
            feishu.send,
            {"webhook": "https://open.feishu.cn/open-apis/bot/v2/hook/secret"},
            {"code": 19021, "msg": "secret"},
        ),
    ],
)
def test_vendor_failure_is_not_success_or_secret_leak(
    monkeypatch, sender, config, result
):
    monkeypatch.setattr(_common, "post_json", lambda *args: result)
    # Feishu imports the helper at module load; Telegram resolves it on send.
    monkeypatch.setattr(feishu, "post_json", lambda *args: result)
    with pytest.raises(ExecutionError, match="rejected") as error:
        sender(config, "Hello", "key")
    assert "secret" not in str(error.value)


def test_http_failure_hides_token_and_does_not_follow_redirects(monkeypatch):
    client_type = httpx.Client

    def transport(request):
        return httpx.Response(302, headers={"location": "https://evil.test"})

    monkeypatch.setattr(
        _common.httpx,
        "Client",
        lambda **kw: client_type(transport=httpx.MockTransport(transport), **kw),
    )
    with pytest.raises(ExecutionError, match="HTTP request failed") as error:
        telegram.send({"bot_token": "123:secret", "chat_id": "1"}, "Hello", "key")
    assert "secret" not in str(error.value)


def test_sender_limits_and_secret_validation(monkeypatch):
    monkeypatch.delenv("UNSET_BOT", raising=False)
    with pytest.raises(ExecutionError, match="bot_token"):
        telegram.send({"bot_token": "env:UNSET_BOT"}, "Hello", "key")
    with pytest.raises(ExecutionError, match="4096"):
        telegram.send({"bot_token": "123:secret", "chat_id": "1"}, "😀" * 2049, "key")
    with pytest.raises(ExecutionError, match="webhook"):
        feishu.send({"webhook": "http://127.0.0.1/secret"}, "Hello", "key")
    with pytest.raises(ExecutionError, match="20000"):
        feishu.send(
            {"webhook": "https://open.feishu.cn/open-apis/bot/v2/hook/test"},
            "大" * 7000,
            "key",
        )
    with pytest.raises(ExecutionError, match="headers"):
        email.send(
            {
                "smtp_host": "smtp.example.test",
                "from": "a@example.test",
                "to": "b@example.test",
                "subject": "bad\nBcc: evil",
            },
            "Hello",
            "key",
        )


def test_smtp_ssl_failure_is_sanitized(monkeypatch):
    import smtplib

    def fail(host, port, *, timeout, context):
        assert port == 465
        assert context.check_hostname
        raise smtplib.SMTPAuthenticationError(535, b"mail-secret")

    monkeypatch.setattr(smtplib, "SMTP_SSL", fail)
    with pytest.raises(ExecutionError, match="SMTP request failed") as error:
        email.send(
            {
                "smtp_host": "smtp.example.test",
                "security": "ssl",
                "from": "a@example.test",
                "to": "b@example.test",
            },
            "Hello",
            "key",
        )
    assert "mail-secret" not in str(error.value)


def test_email_sets_a_parseable_date_header(monkeypatch):
    import smtplib
    from email.utils import parsedate_to_datetime

    sent = []

    class SMTP:
        def __init__(self, host, port, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def starttls(self, **kwargs):
            pass

        def send_message(self, message):
            sent.append(message)
            return {}

    monkeypatch.setattr(smtplib, "SMTP", SMTP)
    email.send(
        {"smtp_host": "smtp.example.test", "from": "[邮箱]", "to": "[邮箱]"},
        "Hello",
        "key",
    )

    assert parsedate_to_datetime(sent[0]["Date"]).tzinfo is not None
