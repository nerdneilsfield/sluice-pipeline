"""json_api Source -> render Stage -> file Sink through ``tick``; no network."""

import json
import socket
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from sluice.app import open_app
from sluice.reconcile import tick
from sluice.web.http import DirectResponse


@pytest.fixture
def api(monkeypatch):
    """The JSON the fake API returns; mutate ``api.body`` between ticks."""

    def refuse(*args, **kwargs):
        raise AssertionError("real network use")

    monkeypatch.setattr(socket, "getaddrinfo", refuse)
    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["93.184.216.34"])

    state = SimpleNamespace(body=[{"id": 1, "title": "first", "views": 1}])

    def fake(target, addresses, request=None, binding=None, *, headers=None, **_):
        return DirectResponse(status_code=200, body=json.dumps(state.body).encode())

    monkeypatch.setattr("sluice.sources._http.send_pinned_httpx_request", fake)
    return state


@pytest.fixture
def app(tmp_path, api):
    (tmp_path / "data").mkdir()
    definition = tmp_path / "d.yml"
    definition.write_text("""version: 1
file_root: data
sources:
  posts:
    use: sluice.sources.json_api
    schedule: 1m
    with: {url: 'https://api.example.test/posts', id_field: id}
stages:
  render: {input: each, tasks: [{use: sluice.tasks.render_telegram, with: {template: "{title}"}}]}
pipelines:
  p:
    nodes:
      text: {stage: render, from: source.posts}
      save: {from: text, deliver: {use: sluice.sinks.file, with: {directory: posts}}}
""")
    app = open_app(definition, tmp_path / "state.db")
    yield app
    app.close()


def run_tick(app, minute):
    tick(app, datetime(2026, 1, 1, tzinfo=UTC) + timedelta(minutes=minute))


def written(tmp_path):
    return sorted(p.read_text() for p in (tmp_path / "data" / "posts").iterdir())


def count(app, table):
    return app.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def test_json_api_to_file_sink(tmp_path, app, api):
    run_tick(app, 0)
    assert written(tmp_path) == ["first"]
    assert [r[0] for r in app.conn.execute("SELECT state FROM deliveries")] == [
        "succeeded"
    ]
    external = app.conn.execute("SELECT delivery_key,external_id FROM deliveries")
    key, name = external.fetchone()
    assert name == f"{key}.txt"

    run_tick(app, 1)  # nothing changed: no duplicate Item, Delivery or file
    assert (count(app, "items"), count(app, "deliveries")) == (2, 1)
    assert written(tmp_path) == ["first"]

    # Same id, changed rendered field: a new Item, a new Delivery, a new file.
    api.body = [{"id": 1, "title": "renamed", "views": 1}]
    run_tick(app, 2)
    assert count(app, "deliveries") == 2
    assert written(tmp_path) == ["first", "renamed"]

    # Same id, only an unrendered field changed: new source Item, same rendered
    # text, so no third Delivery.
    api.body = [{"id": 1, "title": "renamed", "views": 99}]
    run_tick(app, 3)
    assert count(app, "deliveries") == 2
    assert written(tmp_path) == ["first", "renamed"]
