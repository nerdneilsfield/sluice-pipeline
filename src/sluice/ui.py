"""Small optional Flask management UI over :mod:`sluice.read_model`."""

from __future__ import annotations

import ipaddress
from datetime import UTC, datetime

from .errors import ExecutionError
from .read_model import (
    default_rebuild_target,
    pipeline_detail,
    pipeline_list,
    rebuild_action,
    retry_delivery_action,
)


def _is_loopback(host: str) -> bool:
    try:
        return ipaddress.ip_address(host.strip("[]")).is_loopback
    except ValueError:
        return host == "localhost"


def _is_wildcard(host: str) -> bool:
    try:
        return ipaddress.ip_address(host.strip("[]")).is_unspecified
    except ValueError:
        return False


def create_ui(app, host: str = "127.0.0.1", port: int = 8000):
    """Create the HTML-only management application for one open ``App``.

    ``host`` and ``port`` are the address the server binds.  Requests are
    refused unless their ``Host`` header names that address (or, for a
    loopback bind, any loopback name), which defeats DNS rebinding, and state
    changing requests must come from the same origin.  A wildcard bind
    (``0.0.0.0`` or ``::``) has no single name to match, so the ``Host`` check
    is skipped and DNS-rebinding protection is given up.

    Flask is imported here, rather than by the core package or CLI parser, so
    installations without the optional ``ui`` extra retain all local runtime
    commands.
    """

    from flask import Flask, abort, redirect, render_template_string, request, url_for

    web = Flask(__name__)

    bound = host.strip("[]").lower()
    if ":" in bound:
        bound = f"[{bound}]"
    names = {bound}
    if _is_loopback(bound):
        names |= {"localhost", "127.0.0.1", "[::1]"}
    # A browser omits the port when it is the HTTP default.
    allowed_hosts = {f"{name}:{port}" for name in names}
    if port == 80:
        allowed_hosts |= names
    wildcard = _is_wildcard(bound)

    @web.before_request
    def refuse_foreign_requests():
        if not wildcard and request.host.lower() not in allowed_hosts:
            abort(403)
        if request.method == "POST":
            if request.headers.get("Sec-Fetch-Site", "same-origin") not in {
                "same-origin",
                "none",
            }:
                abort(403)
            origin = request.headers.get("Origin")
            if origin is not None and origin != f"{request.scheme}://{request.host}":
                abort(403)

    @web.get("/")
    def home():
        return redirect(url_for("pipelines"), code=303)

    @web.get("/pipelines")
    def pipelines():
        values = pipeline_list(app)
        return render_template_string(
            """
            <!doctype html>
            <title>Sluice pipelines</title>
            <h1>Pipelines</h1>
            <table>
              <thead><tr><th>Name</th><th>State</th><th>Pending</th><th>Failed</th><th>Latest delivery</th><th>Latest error</th></tr></thead>
              <tbody>
              {% for value in values %}
                <tr>
                  <td><a href="{{ url_for('pipeline', pipeline_name=value.name) }}">{{ value.name }}</a></td>
                  <td>{{ value.state }}</td><td>{{ value.pending_count }}</td><td>{{ value.failed_count }}</td>
                  <td>{{ value.latest_delivery or '' }}</td><td>{{ value.latest_error or '' }}</td>
                </tr>
              {% endfor %}
              </tbody>
            </table>
            """,
            values=values,
        )

    @web.get("/pipelines/<pipeline_name>")
    def pipeline(pipeline_name: str):
        try:
            detail = pipeline_detail(app, pipeline_name)
        except ExecutionError:
            abort(404)
        return render_template_string(
            """
            <!doctype html>
            <title>{{ detail.name }}</title>
            <h1>{{ detail.name }}</h1>
            <p><a href="{{ url_for('pipelines') }}">All pipelines</a></p>
            <h2>Nodes</h2>
            {% for node in detail.nodes %}
              <section>
                <h3>{{ node.name }}{% if node.stage %} ({{ node.stage }}){% endif %}</h3>
                {% if node.executions %}<p>Executions: {{ node.executions|join(', ') }}</p>{% endif %}
                {% if node.frozen_items %}<p>Frozen items: {{ node.frozen_items|join(', ') }}</p>{% endif %}
                {% if node.stage %}
                  <form method="post" action="{{ url_for('rebuild_node', pipeline_name=detail.name, node_name=node.name) }}">
                    <button type="submit">Rebuild latest failed execution</button>
                  </form>
                {% endif %}
                {% if node.deliveries %}
                  <ul>
                  {% for key in node.deliveries %}
                    <li>{{ key }} — {{ detail.deliveries[key] }}
                      <form method="post" action="{{ url_for('retry_delivery', delivery_key=key) }}"><button type="submit">Retry</button></form>
                    </li>
                  {% endfor %}
                  </ul>
                {% endif %}
              </section>
            {% endfor %}
            {% if detail.frozen_batches %}
              <h2>Frozen batches</h2>
              <ul>{% for node, items in detail.frozen_batches.items() %}<li>{{ node }}: {{ items|join(', ') }}</li>{% endfor %}</ul>
            {% endif %}
            """,
            detail=detail,
        )

    def _action_page(message: str, status: int = 202):
        return (
            render_template_string(
                "<!doctype html><title>Sluice action</title><p>{{ message }}</p>",
                message=message,
            ),
            status,
        )

    @web.post("/pipelines/<pipeline_name>/nodes/<node_name>/rebuild")
    def rebuild_node(pipeline_name: str, node_name: str):
        # admit() refuses during App.close(); that surfaces as the same 404.
        try:
            with app.admit():
                target = default_rebuild_target(app, pipeline_name, node_name)
                if target is None:
                    abort(404)
                result = rebuild_action(app, target, datetime.now(UTC))
        except ExecutionError:
            abort(404)
        return _action_page(
            f"Rebuild requested for execution {target}: {result.status}"
        )

    @web.post("/executions/<int:execution_id>/rebuild")
    def rebuild_execution(execution_id: int):
        try:
            with app.admit():
                result = rebuild_action(app, execution_id, datetime.now(UTC))
        except ExecutionError:
            abort(404)
        return _action_page(
            f"Rebuild requested for execution {execution_id}: {result.status}"
        )

    @web.post("/deliveries/<path:delivery_key>/retry")
    def retry_delivery(delivery_key: str):
        try:
            with app.admit():
                key = retry_delivery_action(app, delivery_key, datetime.now(UTC))
        except ExecutionError:
            abort(404)
        return _action_page(f"Delivery retry requested: {key}")

    return web


__all__ = ["create_ui"]
