"""Email Sink startup adapter."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass

from ..contracts import JSONValue


@dataclass(frozen=True, slots=True)
class _EmailSink:
    client: Callable[[Mapping[str, JSONValue], JSONValue, str], str | None]

    def send(
        self,
        config: Mapping[str, JSONValue],
        rendered_payload: JSONValue,
        delivery_key: str,
    ) -> str | None:
        return self.client(config, rendered_payload, delivery_key)


def bind(
    client: Callable[[Mapping[str, JSONValue], JSONValue, str], str | None]
    | None = None,
) -> _EmailSink:
    return _EmailSink(send if client is None else client)


def send(config: Mapping[str, JSONValue], payload: JSONValue, delivery_key: str) -> str:
    """Send a UTF-8 plain-text message over authenticated TLS SMTP."""
    import smtplib
    import ssl
    from email.message import EmailMessage
    from hashlib import sha256

    from ..errors import ExecutionError
    from ._common import message_text, setting, timeout_seconds

    host = setting(config, "smtp_host")
    security = setting(config, "security", "starttls")
    if security not in {"starttls", "ssl"}:
        raise ExecutionError("Email security must be starttls or ssl")
    port = config.get("smtp_port", 465 if security == "ssl" else 587)
    if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
        raise ExecutionError("Email smtp_port is invalid")
    sender = setting(config, "from")
    recipient = setting(config, "to", config.get("target"))
    subject = setting(config, "subject", "Sluice notification")
    username = setting(config, "username") if "username" in config else None
    password = setting(config, "password") if username is not None else None
    if username is None and "password" in config:
        raise ExecutionError("Email password requires username")
    message_id = f"<{sha256(delivery_key.encode()).hexdigest()}@sluice.local>"
    message = EmailMessage()
    try:
        message["From"] = sender
        message["To"] = recipient
        message["Subject"] = subject
        message["Message-ID"] = message_id
        message.set_content(message_text(payload))
    except ValueError:
        raise ExecutionError("Email headers are invalid") from None
    timeout = timeout_seconds(config)
    context = ssl.create_default_context()
    try:
        if security == "ssl":
            client = smtplib.SMTP_SSL(host, port, timeout=timeout, context=context)
        else:
            client = smtplib.SMTP(host, port, timeout=timeout)
        with client:
            if security == "starttls":
                client.starttls(context=context)
            if username is not None:
                client.login(username, password)
            refused = client.send_message(message)
            if refused:
                raise ExecutionError("Email recipients were refused")
    except (OSError, smtplib.SMTPException):
        raise ExecutionError("Email SMTP request failed") from None
    return message_id
