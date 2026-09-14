"""Small optional Flask management UI over :mod:`sluice.read_model`."""

from __future__ import annotations

from datetime import UTC, datetime

from .errors import ExecutionError
from .read_model import (
    default_rebuild_target,
    pipeline_detail,
    pipeline_list,
    rebuild_action,
    retry_delivery_action,
)


def create_ui(app):
    """Create the HTML-only management application for one open ``App``.

    Flask is imported here, rather than by the core package or CLI parser, so
    installations without the optional ``ui`` extra retain all local runtime
    commands.
    """

    from flask import Flask, abort, redirect, render_template_string, url_for

    web = Flask(__name__)

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
        try:
            target = default_rebuild_target(app, pipeline_name, node_name)
        except ExecutionError:
            abort(404)
        if target is None:
            abort(404)
        try:
            result = rebuild_action(app, target, datetime.now(UTC))
        except ExecutionError:
            abort(404)
        return _action_page(
            f"Rebuild requested for execution {target}: {result.status}"
        )

    @web.post("/executions/<int:execution_id>/rebuild")
    def rebuild_execution(execution_id: int):
        try:
            result = rebuild_action(app, execution_id, datetime.now(UTC))
        except ExecutionError:
            abort(404)
        return _action_page(
            f"Rebuild requested for execution {execution_id}: {result.status}"
        )

    @web.post("/deliveries/<path:delivery_key>/retry")
    def retry_delivery(delivery_key: str):
        try:
            key = retry_delivery_action(app, delivery_key, datetime.now(UTC))
        except ExecutionError:
            abort(404)
        return _action_page(f"Delivery retry requested: {key}")

    return web


__all__ = ["create_ui"]
