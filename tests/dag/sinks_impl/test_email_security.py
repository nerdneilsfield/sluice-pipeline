from __future__ import annotations

from email.message import EmailMessage

import pytest

from sluice.dag.sinks._impl import _email_render
from sluice.dag.sinks._impl._markdown_ast import parse_markdown
from sluice.dag.sinks._impl.email import _aiosmtplib_send


def test_render_to_html_sanitizes_when_lxml_cleaner_is_unavailable(monkeypatch):
    monkeypatch.setattr(_email_render, "Cleaner", None)

    html = _email_render.render_to_html(
        parse_markdown(
            'trusted\n\n<script>alert(1)</script>\n\n<img src="x" onerror="alert(2)">'
            '\n\n<a href="javascript:alert(3)">bad</a>'
        )
    )

    assert "trusted" in html
    assert "<script" not in html.lower()
    assert "onerror" not in html.lower()
    assert "javascript:" not in html.lower()


def _message() -> EmailMessage:
    msg = EmailMessage()
    msg["From"] = "from@example.com"
    msg["To"] = "to@example.com"
    msg["Message-ID"] = "<msg@example.com>"
    msg.set_content("body")
    return msg


@pytest.mark.asyncio
async def test_smtp_auth_refuses_non_tls_transport(monkeypatch):
    instances = []

    class FakeSMTP:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
            self.login_called = False
            instances.append(self)

        async def connect(self, **kwargs):
            self.connect_kwargs = kwargs

        async def login(self, username, password):
            self.login_called = True

        async def sendmail(self, sender, recipients, raw, **kwargs):
            self.sendmail_kwargs = kwargs

        async def quit(self):
            pass

    monkeypatch.setattr("sluice.dag.sinks._impl.email.aiosmtplib.SMTP", FakeSMTP)

    with pytest.raises(RuntimeError, match="TLS"):
        await _aiosmtplib_send(
            _message(),
            host="smtp.example.com",
            port=25,
            username="user",
            password="password",
            starttls=False,
            recipient="to@example.com",
        )

    assert not instances or not instances[0].login_called


@pytest.mark.asyncio
async def test_smtp_connect_and_sendmail_use_explicit_timeout(monkeypatch):
    instances = []

    class FakeSMTP:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
            instances.append(self)

        async def connect(self, **kwargs):
            self.connect_kwargs = kwargs

        async def sendmail(self, sender, recipients, raw, **kwargs):
            self.sendmail_kwargs = kwargs

        async def quit(self):
            pass

    monkeypatch.setattr("sluice.dag.sinks._impl.email.aiosmtplib.SMTP", FakeSMTP)

    await _aiosmtplib_send(
        _message(),
        host="smtp.example.com",
        port=587,
        username="",
        password="",
        starttls=True,
        recipient="to@example.com",
    )

    smtp = instances[0]
    assert smtp.kwargs["timeout"] == 30.0
    assert smtp.connect_kwargs["timeout"] == 30.0
    assert smtp.sendmail_kwargs["timeout"] == 30.0
