import sqlite3
from datetime import UTC, datetime, timedelta

from sluice.app import open_app
from sluice.contracts import SourceRecord, SourceResult
from sluice.prefect_flow import prefect_tick
from sluice.reconcile import tick


def instant(seconds: int) -> datetime:
    return datetime(2026, 8, 14, tzinfo=UTC) + timedelta(seconds=seconds)


def acceptance_source(config, checkpoint):
    return SourceResult((SourceRecord("a", {"title": "A"}),), "done")


def acceptance_sink(config, payload, delivery_key):
    return "sent"


def write_acceptance_definition(tmp_path):
    definition = tmp_path / "definition.yml"
    definition.write_text(
        """
version: 1
database: state.db
sources: {feed: {use: tests.smoke.test_prefect_parity.acceptance_source, with: {}}}
stages: {documents: {input: each, tasks: [{use: sluice.tasks.to_markdown}]}}
pipelines:
  p:
    nodes:
      documents: {stage: documents, from: source.feed}
      email: {from: documents, deliver: {use: tests.smoke.test_prefect_parity.acceptance_sink, with: {target: test@example.test}}}
""",
        encoding="utf-8",
    )
    return definition


def runtime_summary(connection):
    connection.row_factory = sqlite3.Row
    execution_states = dict(
        connection.execute(
            "SELECT state, COUNT(*) FROM executions GROUP BY state"
        ).fetchall()
    )
    delivery_states = dict(
        connection.execute(
            "SELECT state, COUNT(*) FROM deliveries GROUP BY state"
        ).fetchall()
    )
    item_logical_ids = tuple(
        row[0]
        for row in connection.execute(
            "SELECT logical_id FROM items ORDER BY logical_id"
        )
    )
    return execution_states, delivery_states, item_logical_ids


def test_prefect_flow_and_local_tick_produce_identical_sqlite_state(tmp_path):
    definition = write_acceptance_definition(tmp_path)
    local = open_app(definition, tmp_path / "local.db")
    tick(local, instant(0))
    prefect_tick(str(definition), str(tmp_path / "prefect.db"), instant(0))
    assert runtime_summary(local.conn) == runtime_summary(
        sqlite3.connect(tmp_path / "prefect.db")
    )


def test_prefect_flow_without_db_path_uses_the_definition_database(tmp_path):
    definition = write_acceptance_definition(tmp_path)
    prefect_tick(str(definition), at=instant(0))
    assert runtime_summary(sqlite3.connect(tmp_path / "state.db"))[2] == ("a",)
