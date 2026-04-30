from email.message import EmailMessage

import aiosmtplib
from jinja2 import Template

from sluice.context import PipelineContext
from sluice.sinks._email_render import render_to_html
from sluice.sinks._markdown_ast import parse_markdown
from sluice.sinks._push_base import PushBatchItem, PushSinkBase


async def _aiosmtplib_send(
    message: EmailMessage,
    *,
    host: str,
    port: int,
    username: str,
    password: str,
    starttls: bool,
    recipient: str,
) -> str:
    smtp = aiosmtplib.SMTP(hostname=host, port=port, start_tls=starttls)
    try:
        await smtp.connect()
        if username:
            await smtp.login(username, password)
        await smtp.send_message(message, recipients=[recipient])
        await smtp.quit()
    except Exception:
        try:
            await smtp.quit()
        except Exception:
            pass
        raise
    return message["Message-ID"] or "no-id"


class EmailSink(PushSinkBase):
    type = "email"

    def __init__(
        self,
        *,
        sink_id,
        smtp_host,
        smtp_port,
        smtp_username,
        smtp_password,
        smtp_starttls,
        from_address,
        recipients,
        subject_template,
        brief_input,
        items_input,
        items_template_str,
        split,
        html_template_str,
        style_block,
        footer_template,
        attach_run_log,
        recipient_failure_policy,
        delivery_log,
        emit_on_empty: bool = False,
    ):
        super().__init__(
            sink_id=sink_id,
            footer_template=footer_template,
            delivery_log=delivery_log,
            emit_on_empty=emit_on_empty,
        )
        self._host = smtp_host
        self._port = smtp_port
        self._user = smtp_username
        self._pass = smtp_password
        self._starttls = smtp_starttls
        self._from = from_address
        self._recipients = list(recipients)
        self._subject_tmpl = Template(subject_template)
        self._brief_input = brief_input
        self._items_input = items_input
        self._items_tmpl = Template(items_template_str)
        self._split = split
        self._html_tmpl = Template(html_template_str)
        self._style_block = style_block
        self._policy = recipient_failure_policy

    @property
    def fail_fast(self) -> bool:
        return self._policy == "fail_fast"

    def _compose_body(self, ctx: PipelineContext) -> str:
        items = ctx.items if self._items_input == "items" else []
        parts: list[str] = []
        if self._brief_input and self._brief_input.startswith("context."):
            key = self._brief_input.split(".", 1)[1]
            brief = ctx.context.get(key)
            if brief:
                parts.append(brief)
        for it in items:
            parts.append(self._items_tmpl.render(item=it, ctx=ctx))
        md = "\n\n".join(parts)
        body_html = render_to_html(parse_markdown(md))
        return self._html_tmpl.render(
            body_html=body_html,
            style_block=self._style_block,
            pipeline_id=ctx.pipeline_id,
            run_date=ctx.run_date,
            footer=self.render_footer(ctx),
        )

    def build_batch(self, ctx: PipelineContext) -> list[PushBatchItem]:
        subject = self._subject_tmpl.render(pipeline_id=ctx.pipeline_id, run_date=ctx.run_date)
        out = []
        items = ctx.items if self._items_input == "items" else []
        if self._split == "per_item" and items:
            for it in items:
                parts: list[str] = []
                if self._brief_input and self._brief_input.startswith("context."):
                    key = self._brief_input.split(".", 1)[1]
                    brief = ctx.context.get(key)
                    if brief:
                        parts.append(brief)
                parts.append(self._items_tmpl.render(item=it, ctx=ctx))
                md = "\n\n".join(parts)
                body_html = render_to_html(parse_markdown(md))
                html = self._html_tmpl.render(
                    body_html=body_html,
                    style_block=self._style_block,
                    pipeline_id=ctx.pipeline_id,
                    run_date=ctx.run_date,
                    footer=self.render_footer(ctx),
                )
                for r in self._recipients:
                    msg = EmailMessage()
                    msg["From"] = self._from
                    msg["To"] = r
                    msg["Subject"] = subject
                    msg.set_content("HTML email — please view in an HTML-capable client.")
                    msg.add_alternative(html, subtype="html")
                    out.append(PushBatchItem(kind="recipient", payload=msg, recipient=r))
        else:
            html = self._compose_body(ctx)
            for r in self._recipients:
                msg = EmailMessage()
                msg["From"] = self._from
                msg["To"] = r
                msg["Subject"] = subject
                msg.set_content("HTML email — please view in an HTML-capable client.")
                msg.add_alternative(html, subtype="html")
                out.append(PushBatchItem(kind="recipient", payload=msg, recipient=r))
        return out

    async def send_one(self, payload, recipient=None) -> str:
        return await _aiosmtplib_send(
            payload,
            host=self._host,
            port=self._port,
            username=self._user,
            password=self._pass,
            starttls=self._starttls,
            recipient=recipient,
        )
