import sqlite3
from datetime import UTC, datetime, timedelta
from threading import Event, Thread, current_thread, main_thread

import pytest

from sluice.app import open_app
from sluice.contracts import SourceRecord, SourceResult
from sluice.delivery import (
    _claim_delivery,
    _ensure_delivery_row_for_item,
    _ensure_delivery_rows,
    _send_attempt,
    deliver,
    retry_delivery,
)
from sluice.errors import ExecutionError
from sluice.reconcile import reconcile_node, recover_startup, tick
from sluice.source_runtime import collect_source


def instant(seconds: int) -> datetime:
    return datetime(2026, 8, 14, tzinfo=UTC) + timedelta(seconds=seconds)


def source_collect(config, checkpoint):
    return SourceResult((SourceRecord("rendered", {"content": "message"}),), "done")


def source_collect_two(config, checkpoint):
    return SourceResult(
        (
            SourceRecord("first", {"content": "first"}),
            SourceRecord("second", {"content": "second"}),
        ),
        "done",
    )


def source_collect_empty(config, checkpoint):
    return SourceResult((), "done")


def delivery_app(tmp_path, sink_clients, email_target="a@example.test"):
    definition = tmp_path / "definition.yml"
    definition.write_text(
        f"""
version: 1
database: state.db
sources: {{feed: {{use: tests.integration.test_delivery.source_collect, with: {{}}}}}}
pipelines:
  p:
    nodes:
      email: {{from: source.feed, retries: 1, concurrency: 1, deliver: {{use: sluice.sinks.email, with: {{target: {email_target}}}}}}}
      telegram: {{from: source.feed, retries: 1, concurrency: 1, deliver: {{use: sluice.sinks.telegram, with: {{target: primary}}}}}}
""",
        encoding="utf-8",
    )
    app = open_app(definition, tmp_path / "state.db", sink_clients=sink_clients)
    collect_source(app, "feed", instant(0))
    item_id = app.conn.execute("SELECT id FROM items").fetchone()[0]
    return app, item_id


def test_sibling_deliveries_retry_independently_and_null_is_success(tmp_path):
    sent = []

    def email(config, payload, key):
        sent.append(("email", key))

    def telegram(config, payload, key):
        raise RuntimeError("down")

    app, _ = delivery_app(
        tmp_path,
        {"sluice.sinks.email": email, "sluice.sinks.telegram": telegram},
    )
    tick(
        app, instant(0)
    )  # generic node admission creates and drains both Delivery rows
    email_key = app.conn.execute(
        "SELECT delivery_key FROM deliveries WHERE node_name='email'"
    ).fetchone()[0]
    telegram_key = app.conn.execute(
        "SELECT delivery_key FROM deliveries WHERE node_name='telegram'"
    ).fetchone()[0]
    assert (
        app.conn.execute(
            "SELECT state FROM deliveries WHERE delivery_key=?", (email_key,)
        ).fetchone()[0]
        == "succeeded"
    )
    assert (
        app.conn.execute(
            "SELECT external_id FROM deliveries WHERE delivery_key=?", (email_key,)
        ).fetchone()[0]
        is None
    )
    assert (
        app.conn.execute(
            "SELECT state FROM deliveries WHERE delivery_key=?", (telegram_key,)
        ).fetchone()[0]
        == "failed"
    )
    upstream_before = app.conn.execute("SELECT COUNT(*) FROM executions").fetchone()[0]
    retry_delivery(app, telegram_key, instant(1))
    assert (
        app.conn.execute("SELECT COUNT(*) FROM executions").fetchone()[0]
        == upstream_before
    )
    assert (
        app.conn.execute(
            "SELECT state FROM deliveries WHERE delivery_key=?", (telegram_key,)
        ).fetchone()[0]
        == "failed"
    )
    assert (
        app.conn.execute(
            "SELECT state FROM deliveries WHERE delivery_key=?", (email_key,)
        ).fetchone()[0]
        == "succeeded"
    )


def test_delivery_binding_digest_and_attempt_token_reject_stale_completion(tmp_path):
    started, release = Event(), Event()
    first_call = True

    def email(config, payload, key):
        nonlocal first_call
        if first_call:
            first_call = False
            started.set()
            if not release.wait(timeout=5):
                raise RuntimeError("test barrier was not released")
            return "old"
        return "new"

    app, item_id = delivery_app(
        tmp_path,
        {
            "sluice.sinks.email": email,
            "sluice.sinks.telegram": lambda config, payload, key: "ok",
        },
    )
    worker = Thread(
        target=deliver,
        args=(app, "p", "email", item_id, instant(0)),
    )
    worker.start()
    assert started.wait(timeout=5)
    key, old_token = app.conn.execute(
        "SELECT delivery_key,attempt_token FROM deliveries WHERE node_name='email'"
    ).fetchone()
    recover_startup(app, instant(1))
    retry_delivery(app, key, instant(1))
    new_token = app.conn.execute(
        "SELECT attempt_token FROM deliveries WHERE delivery_key=?", (key,)
    ).fetchone()[0]
    assert new_token != old_token
    release.set()
    worker.join(timeout=5)
    assert not worker.is_alive()
    state, external_id = app.conn.execute(
        "SELECT state,external_id FROM deliveries WHERE delivery_key=?", (key,)
    ).fetchone()
    assert (state, external_id) == ("succeeded", "new")


def test_stale_delivery_attempt_is_not_sent_or_allowed_to_mutate_current_row(tmp_path):
    calls = []

    def email(config, payload, key):
        calls.append(key)
        return "stale"

    app, item_id = delivery_app(
        tmp_path,
        {
            "sluice.sinks.email": email,
            "sluice.sinks.telegram": lambda config, payload, key: "ok",
        },
    )
    reconcile_node(app, "p", "email", instant(0))
    key = _ensure_delivery_row_for_item(app, "p", "email", item_id, instant(0))
    stale = _claim_delivery(app, key, instant(0))
    assert stale is not None
    recover_startup(app, instant(1))
    current = _claim_delivery(app, key, instant(1))
    assert current is not None
    assert current.attempt_token != stale.attempt_token
    before = tuple(
        app.conn.execute(
            "SELECT state,attempt_token,error,external_id FROM deliveries WHERE delivery_key=?",
            (key,),
        ).fetchone()
    )

    _send_attempt(app, stale, instant(2))

    after = tuple(
        app.conn.execute(
            "SELECT state,attempt_token,error,external_id FROM deliveries WHERE delivery_key=?",
            (key,),
        ).fetchone()
    )
    assert calls == []
    assert after == before


def test_delivery_config_change_creates_a_distinct_local_success_key(tmp_path):
    clients = {
        "sluice.sinks.email": lambda config, payload, key: "ok",
        "sluice.sinks.telegram": lambda config, payload, key: "ok",
    }
    first_app, item_id = delivery_app(tmp_path, clients, "one@example.test")
    first_key = deliver(first_app, "p", "email", item_id, instant(0))
    first_app.conn.close()
    second_app, item_id = delivery_app(tmp_path, clients, "two@example.test")
    second_key = deliver(second_app, "p", "email", item_id, instant(1))
    assert first_key != second_key
    assert (
        second_app.conn.execute(
            "SELECT COUNT(*) FROM deliveries WHERE state='succeeded'"
        ).fetchone()[0]
        == 2
    )


def test_retry_rejects_a_delivery_when_the_declared_sink_binding_changed(tmp_path):
    definition = tmp_path / "definition.yml"
    database = tmp_path / "state.db"

    def write_definition(sink_use):
        definition.write_text(
            f"""
version: 1
database: state.db
sources: {{feed: {{use: tests.integration.test_delivery.source_collect, with: {{}}}}}}
pipelines:
  p:
    nodes:
      out: {{from: source.feed, retries: 1, concurrency: 1, deliver: {{use: {sink_use}, with: {{target: old}}}}}}
""",
            encoding="utf-8",
        )

    old_calls = []
    new_calls = []

    def old_sink(config, payload, key):
        old_calls.append(key)
        raise RuntimeError("old sink is down")

    def new_sink(config, payload, key):
        new_calls.append(key)
        return "new"

    write_definition("sluice.sinks.email")
    first = open_app(
        definition,
        database,
        sink_clients={
            "sluice.sinks.email": old_sink,
            "sluice.sinks.telegram": new_sink,
        },
    )
    collect_source(first, "feed", instant(0))
    item_id = first.conn.execute("SELECT id FROM items").fetchone()[0]
    key = deliver(first, "p", "out", item_id, instant(0))
    old_binding, old_config = first.conn.execute(
        "SELECT binding_digest,config_json FROM deliveries WHERE delivery_key=?", (key,)
    ).fetchone()
    assert old_calls == [key]
    first.conn.close()

    write_definition("sluice.sinks.telegram")
    second = open_app(
        definition,
        database,
        sink_clients={
            "sluice.sinks.email": old_sink,
            "sluice.sinks.telegram": new_sink,
        },
    )
    with pytest.raises(ExecutionError, match="binding"):
        retry_delivery(second, key, instant(1))

    assert new_calls == []
    row = second.conn.execute(
        "SELECT state,binding_digest,config_json FROM deliveries WHERE delivery_key=?",
        (key,),
    ).fetchone()
    assert tuple(row) == ("failed", old_binding, old_config)


def test_manual_retry_waits_for_the_persisted_node_concurrency_slot(tmp_path):
    definition = tmp_path / "definition.yml"
    definition.write_text(
        """
version: 1
database: state.db
sources: {feed: {use: tests.integration.test_delivery.source_collect_two, with: {}}}
pipelines:
  p:
    nodes:
      out: {from: source.feed, retries: 1, concurrency: 1, deliver: {use: sluice.sinks.email, with: {target: test}}}
""",
        encoding="utf-8",
    )
    calls = []
    second_started = Event()
    release_second = Event()
    call_count = 0

    def sink(config, payload, key):
        nonlocal call_count
        call_count += 1
        calls.append(key)
        if call_count == 1:
            raise RuntimeError("first attempt failed")
        if call_count == 2:
            second_started.set()
            if not release_second.wait(timeout=5):
                raise RuntimeError("test barrier was not released")
            return "second"
        return "retry"

    app = open_app(
        definition,
        tmp_path / "state.db",
        sink_clients={"sluice.sinks.email": sink},
    )
    collect_source(app, "feed", instant(0))
    item_ids = [
        row[0] for row in app.conn.execute("SELECT id FROM items ORDER BY rowid")
    ]
    first_key = deliver(app, "p", "out", item_ids[0], instant(0))
    second_worker = Thread(
        target=deliver,
        args=(app, "p", "out", item_ids[1], instant(1)),
    )
    second_worker.start()
    assert second_started.wait(timeout=5)

    retry_delivery(app, first_key, instant(2))
    assert calls == [
        first_key,
        app.conn.execute(
            "SELECT delivery_key FROM deliveries WHERE rendered_item_id=?",
            (item_ids[1],),
        ).fetchone()[0],
    ]
    assert (
        app.conn.execute(
            "SELECT state FROM deliveries WHERE delivery_key=?", (first_key,)
        ).fetchone()[0]
        == "failed"
    )

    release_second.set()
    second_worker.join(timeout=5)
    assert not second_worker.is_alive()
    assert (
        app.conn.execute(
            "SELECT state FROM deliveries WHERE rendered_item_id=?", (item_ids[1],)
        ).fetchone()[0]
        == "succeeded"
    )

    retry_delivery(app, first_key, instant(3))
    assert (
        app.conn.execute(
            "SELECT state FROM deliveries WHERE delivery_key=?", (first_key,)
        ).fetchone()[0]
        == "succeeded"
    )
    assert calls[-1] == first_key


def test_tick_skips_failed_delivery_rows_for_stale_members(tmp_path):
    definition = tmp_path / "definition.yml"
    database = tmp_path / "state.db"

    def write_definition(source_use):
        definition.write_text(
            f"""
version: 1
database: state.db
sources: {{feed: {{use: {source_use}, with: {{}}}}}}
pipelines:
  p:
    nodes:
      out: {{from: source.feed, retries: 1, concurrency: 1, deliver: {{use: sluice.sinks.email, with: {{target: test}}}}}}
""",
            encoding="utf-8",
        )

    old_calls = []

    def old_sink(config, payload, key):
        old_calls.append(key)
        raise RuntimeError("old sink is down")

    write_definition("tests.integration.test_delivery.source_collect")
    first = open_app(
        definition,
        database,
        sink_clients={"sluice.sinks.email": old_sink},
    )
    collect_source(first, "feed", instant(0))
    item_id = first.conn.execute("SELECT id FROM items").fetchone()[0]
    key = deliver(first, "p", "out", item_id, instant(0))
    assert (
        first.conn.execute(
            "SELECT state FROM deliveries WHERE delivery_key=?", (key,)
        ).fetchone()[0]
        == "failed"
    )
    first.conn.close()

    write_definition("tests.integration.test_delivery.source_collect_empty")
    second = open_app(
        definition,
        database,
        sink_clients={"sluice.sinks.email": old_sink},
    )
    tick(second, instant(1))

    assert old_calls == [key]
    assert (
        second.conn.execute(
            "SELECT state FROM deliveries WHERE delivery_key=?", (key,)
        ).fetchone()[0]
        == "failed"
    )
    assert (
        second.conn.execute(
            "SELECT state FROM node_members WHERE item_id=?", (item_id,)
        ).fetchone()[0]
        == "stale"
    )


def test_delivery_worker_reads_hold_the_connection_lock_but_sink_send_does_not(
    tmp_path, monkeypatch
):
    held_during_read = []
    held_during_send = []
    app, _ = delivery_app(
        tmp_path,
        {
            "sluice.sinks.email": lambda config, payload, key: held_during_send.append(
                probe_lock_held(app)
            ),
            "sluice.sinks.telegram": lambda config, payload, key: None,
        },
    )
    execute = app.conn.execute

    def probed_execute(sql, parameters=()):
        # Only pool threads share the connection with the main reconcile loop.
        if current_thread() is not main_thread():
            held_during_read.append(probe_lock_held(app))
        return execute(sql, parameters)

    monkeypatch.setattr(app.conn, "execute", probed_execute)
    tick(app, instant(0))

    assert held_during_read and all(held_during_read)
    assert held_during_send == [False]


def probe_lock_held(app):
    """Report from a fresh thread whether the connection lock is currently held."""

    result = []

    def probe():
        acquired = app.conn.transaction_lock.acquire(blocking=False)
        if acquired:
            app.conn.transaction_lock.release()
        result.append(not acquired)

    thread = Thread(target=probe)
    thread.start()
    thread.join()
    return result[0]


def test_failed_completion_write_fails_the_attempt_instead_of_leaving_it_running(
    tmp_path, monkeypatch
):
    from sluice import delivery

    app, item_id = delivery_app(
        tmp_path,
        {
            "sluice.sinks.email": lambda config, payload, key: "sent",
            "sluice.sinks.telegram": lambda config, payload, key: "ok",
        },
    )

    def broken_complete(*args):
        raise sqlite3.OperationalError("disk I/O error")

    monkeypatch.setattr(delivery, "_complete_delivery", broken_complete)
    with pytest.raises(sqlite3.OperationalError):
        deliver(app, "p", "email", item_id, instant(0))

    state, error = app.conn.execute(
        "SELECT state,error FROM deliveries WHERE node_name='email'"
    ).fetchone()
    assert (state, error) == ("failed", "delivery completion failed")


def test_unfinalizable_attempt_is_logged_and_raised(tmp_path, monkeypatch, caplog):
    from sluice import delivery

    app, item_id = delivery_app(
        tmp_path,
        {
            "sluice.sinks.email": lambda config, payload, key: "sent",
            "sluice.sinks.telegram": lambda config, payload, key: "ok",
        },
    )

    def broken(*args):
        raise sqlite3.OperationalError("disk I/O error")

    monkeypatch.setattr(delivery, "_complete_delivery", broken)
    monkeypatch.setattr(delivery, "_fail_delivery_attempt", broken)
    with (
        caplog.at_level("INFO", logger="sluice.delivery"),
        pytest.raises(sqlite3.OperationalError),
    ):
        deliver(app, "p", "email", item_id, instant(0))

    assert "delivery_finalize_failed" in caplog.text


def test_non_string_sink_result_is_coerced_and_not_retried(tmp_path):
    calls = []

    def email(config, payload, key):
        calls.append(key)
        return 123

    app, item_id = delivery_app(
        tmp_path,
        {
            "sluice.sinks.email": email,
            "sluice.sinks.telegram": lambda config, payload, key: "ok",
        },
    )
    key = deliver(app, "p", "email", item_id, instant(0))

    assert app.conn.execute(
        "SELECT state,external_id FROM deliveries WHERE delivery_key=?", (key,)
    ).fetchone()[:] == ("succeeded", "123")
    assert calls == [key]


def test_changed_retry_policy_applies_to_failed_and_queued_rows(tmp_path):
    definition = tmp_path / "definition.yml"
    database = tmp_path / "state.db"
    calls = []

    def flaky(config, payload, key):
        calls.append(key)
        if len(calls) == 1:
            raise RuntimeError("down")
        return "ok"

    def write_definition(retries):
        definition.write_text(
            f"""
version: 1
database: state.db
sources: {{feed: {{use: tests.integration.test_delivery.source_collect, with: {{}}}}}}
pipelines:
  p:
    nodes:
      out: {{from: source.feed, retries: {retries}, concurrency: 1, deliver: {{use: sluice.sinks.email, with: {{target: x}}}}}}
""",
            encoding="utf-8",
        )

    write_definition(0)
    first = open_app(definition, database, sink_clients={"sluice.sinks.email": flaky})
    collect_source(first, "feed", instant(0))
    tick(first, instant(1))
    tick(first, instant(2))
    assert first.conn.execute("SELECT state FROM deliveries").fetchone()[0] == "failed"
    assert len(calls) == 1
    first.close()

    write_definition(2)
    second = open_app(definition, database, sink_clients={"sluice.sinks.email": flaky})
    tick(second, instant(3))
    assert len(calls) == 2
    assert second.conn.execute("SELECT state,retry_count FROM deliveries").fetchone()[
        :
    ] == ("succeeded", 1)


def _replace_sink_scenario(tmp_path, first_sink):
    """Run one tick with ``first_sink`` on email, then switch the node to telegram."""
    definition = tmp_path / "definition.yml"
    database = tmp_path / "state.db"

    def write_definition(sink_use):
        definition.write_text(
            f"""
version: 1
database: state.db
sources: {{feed: {{use: tests.integration.test_delivery.source_collect, with: {{}}}}}}
pipelines:
  p:
    nodes:
      out: {{from: source.feed, retries: 0, concurrency: 1, deliver: {{use: {sink_use}, with: {{target: x}}}}}}
""",
            encoding="utf-8",
        )

    def reopen(sink_use):
        write_definition(sink_use)
        return open_app(
            definition,
            database,
            sink_clients={
                "sluice.sinks.email": first_sink,
                "sluice.sinks.telegram": lambda config, payload, key: "ok",
            },
        )

    first = reopen("sluice.sinks.email")
    collect_source(first, "feed", instant(0))
    return first, reopen


def test_never_attempted_rows_of_a_replaced_binding_are_dropped(tmp_path):
    first, reopen = _replace_sink_scenario(tmp_path, lambda config, payload, key: "ok")
    reconcile_node(first, "p", "out", instant(0))
    old_keys = _ensure_delivery_rows(first, "p", "out", instant(0))
    assert (
        first.conn.execute("SELECT state FROM deliveries").fetchall()[0][0] == "queued"
    )
    first.close()

    second = reopen("sluice.sinks.telegram")
    tick(second, instant(1))
    rows = second.conn.execute("SELECT delivery_key,state FROM deliveries").fetchall()
    assert [tuple(row) for row in rows] != []
    assert all(row[0] not in old_keys for row in rows)
    assert [row[1] for row in rows] == ["succeeded"]


def test_attempted_rows_of_a_replaced_binding_are_kept(tmp_path):
    def down(config, payload, key):
        raise RuntimeError("down")

    first, reopen = _replace_sink_scenario(tmp_path, down)
    tick(first, instant(1))
    old = first.conn.execute("SELECT delivery_key,state FROM deliveries").fetchall()
    assert [row[1] for row in old] == ["failed"]
    first.close()

    second = reopen("sluice.sinks.telegram")
    tick(second, instant(2))
    states = dict(second.conn.execute("SELECT delivery_key,state FROM deliveries"))
    assert states[old[0][0]] == "failed"
    assert sorted(states.values()) == ["failed", "succeeded"]
