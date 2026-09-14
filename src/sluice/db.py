from __future__ import annotations

import fcntl
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from threading import RLock
from typing import IO


class LockedConnection(sqlite3.Connection):
    writer_lock: IO[str]
    transaction_lock: RLock

    def close(self) -> None:
        if hasattr(self, "writer_lock") and not self.writer_lock.closed:
            fcntl.flock(self.writer_lock.fileno(), fcntl.LOCK_UN)
            self.writer_lock.close()
        super().close()


def open_database(path: str | Path) -> LockedConnection:
    database_path = Path(path)
    database_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(
        database_path,
        isolation_level=None,
        check_same_thread=False,
        factory=LockedConnection,
    )
    lock_file = open(
        database_path.with_suffix(database_path.suffix + ".lock"),
        "a+",
        encoding="utf-8",
    )
    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BaseException:
        lock_file.close()
        connection.close()
        raise
    connection.writer_lock = lock_file
    connection.transaction_lock = RLock()
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    return connection


@contextmanager
def write_transaction(connection: sqlite3.Connection) -> Iterator[None]:
    if not isinstance(connection, LockedConnection):
        raise TypeError("connection must be opened by open_database")
    with connection.transaction_lock:
        connection.execute("BEGIN IMMEDIATE")
        try:
            yield
        except BaseException:
            connection.rollback()
            raise
        else:
            connection.commit()


_SCHEMA = """
CREATE TABLE IF NOT EXISTS items(
  id TEXT PRIMARY KEY, producer_kind TEXT NOT NULL, producer_name TEXT NOT NULL,
  logical_id TEXT NOT NULL, payload_json TEXT NOT NULL, metadata_json TEXT NOT NULL,
  content_hash TEXT NOT NULL, observed_at TEXT NOT NULL, produced_by INTEGER NOT NULL,
  FOREIGN KEY(produced_by) REFERENCES executions(id)
);
CREATE TABLE IF NOT EXISTS executions(
  id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL CHECK(kind IN ('source','stage')),
  pipeline_name TEXT, node_name TEXT, source_name TEXT, stage_name TEXT,
  state TEXT NOT NULL CHECK(state IN ('queued','running','succeeded','failed','cached','dropped')),
  binding_digest TEXT NOT NULL, config_json TEXT NOT NULL,
  lookup_key TEXT, admitted_generation INTEGER, claim_ordinal INTEGER, batch_id INTEGER,
  history_persisted INTEGER NOT NULL DEFAULT 0,
  expected_checkpoint_json TEXT, policy_json TEXT NOT NULL, retry_count INTEGER NOT NULL DEFAULT 0,
  retry_parent_id INTEGER, result_id INTEGER, error TEXT, started_at TEXT, finished_at TEXT,
  rebuild_target_id INTEGER, target_scope_excluded INTEGER NOT NULL DEFAULT 0,
  FOREIGN KEY(retry_parent_id) REFERENCES executions(id)
);
CREATE TABLE IF NOT EXISTS execution_inputs(
  execution_id INTEGER NOT NULL, slot TEXT NOT NULL CHECK(slot IN ('items','history')),
  ordinal INTEGER NOT NULL, item_id TEXT NOT NULL,
  PRIMARY KEY(execution_id, slot, ordinal), FOREIGN KEY(execution_id) REFERENCES executions(id),
  FOREIGN KEY(item_id) REFERENCES items(id)
);
CREATE TABLE IF NOT EXISTS publications(
  id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL CHECK(kind IN ('delta','snapshot')),
  producer_key TEXT NOT NULL, binding_digest TEXT NOT NULL, revision INTEGER NOT NULL,
  digest TEXT NOT NULL,
  UNIQUE(producer_key, binding_digest, revision)
);
CREATE TABLE IF NOT EXISTS publication_members(
  publication_id INTEGER NOT NULL, ordinal INTEGER NOT NULL, item_id TEXT NOT NULL,
  visible_result_id INTEGER, materialization_sequence INTEGER,
  PRIMARY KEY(publication_id, ordinal), FOREIGN KEY(publication_id) REFERENCES publications(id),
  FOREIGN KEY(item_id) REFERENCES items(id)
);
CREATE TABLE IF NOT EXISTS source_state(
  source_name TEXT NOT NULL, binding_digest TEXT NOT NULL, checkpoint_json TEXT,
  next_due_at TEXT, active_execution_id INTEGER,
  PRIMARY KEY(source_name, binding_digest), FOREIGN KEY(active_execution_id) REFERENCES executions(id)
);
CREATE TABLE IF NOT EXISTS source_current_bindings(
  source_name TEXT PRIMARY KEY, binding_digest TEXT NOT NULL,
  FOREIGN KEY(source_name, binding_digest) REFERENCES source_state(source_name, binding_digest)
);
CREATE UNIQUE INDEX IF NOT EXISTS source_one_active_execution
  ON executions(binding_digest) WHERE kind='source' AND state IN ('queued','running');
CREATE INDEX IF NOT EXISTS source_expected_checkpoint
  ON executions(binding_digest,expected_checkpoint_json) WHERE kind='source';
"""


# These tables are deliberately appended after the shared Item/Execution and
# Publication substrate.  StageResult rows are the first consumers that carry
# a foreign key to Executions, and node state points at immutable Publications;
# creating them in this order keeps a fresh database FK-safe without a
# migration layer.
_RECONCILE_SCHEMA = """
CREATE TABLE IF NOT EXISTS stage_results(
  id INTEGER PRIMARY KEY AUTOINCREMENT, stage_name TEXT NOT NULL, lookup_key TEXT NOT NULL,
  sequence INTEGER NOT NULL, created_at TEXT NOT NULL, producer_execution_id INTEGER NOT NULL,
  UNIQUE(stage_name, sequence), FOREIGN KEY(producer_execution_id) REFERENCES executions(id)
);
CREATE TABLE IF NOT EXISTS stage_result_items(
  result_id INTEGER NOT NULL, ordinal INTEGER NOT NULL, item_id TEXT NOT NULL,
  PRIMARY KEY(result_id, ordinal), FOREIGN KEY(result_id) REFERENCES stage_results(id),
  FOREIGN KEY(item_id) REFERENCES items(id)
);
CREATE TABLE IF NOT EXISTS stage_result_current(
  stage_name TEXT NOT NULL, lookup_key TEXT NOT NULL, result_id INTEGER NOT NULL,
  PRIMARY KEY(stage_name, lookup_key), UNIQUE(result_id),
  FOREIGN KEY(result_id) REFERENCES stage_results(id)
);
CREATE TABLE IF NOT EXISTS node_state(
  pipeline_name TEXT NOT NULL, node_name TEXT NOT NULL, binding_digest TEXT NOT NULL,
  next_due_at TEXT, first_pending_at TEXT, current_frozen_batch_id INTEGER,
  current_publication_id INTEGER NOT NULL,
  schedule_json TEXT NOT NULL, batch_policy_json TEXT,
  PRIMARY KEY(pipeline_name, node_name, binding_digest),
  FOREIGN KEY(current_publication_id) REFERENCES publications(id)
);
CREATE TABLE IF NOT EXISTS node_current_bindings(
  pipeline_name TEXT NOT NULL, node_name TEXT NOT NULL, binding_digest TEXT NOT NULL,
  PRIMARY KEY(pipeline_name,node_name),
  FOREIGN KEY(pipeline_name,node_name,binding_digest) REFERENCES node_state(pipeline_name,node_name,binding_digest)
);
CREATE TABLE IF NOT EXISTS node_edge_state(
  pipeline_name TEXT NOT NULL, node_name TEXT NOT NULL, binding_digest TEXT NOT NULL,
  edge_index INTEGER NOT NULL, producer_key TEXT NOT NULL, producer_binding_digest TEXT NOT NULL,
  last_revision INTEGER NOT NULL DEFAULT 0, applied_digest TEXT,
  PRIMARY KEY(pipeline_name,node_name,binding_digest,edge_index),
  FOREIGN KEY(pipeline_name,node_name,binding_digest) REFERENCES node_state(pipeline_name,node_name,binding_digest)
);
CREATE TABLE IF NOT EXISTS node_edge_members(
  pipeline_name TEXT NOT NULL, node_name TEXT NOT NULL, binding_digest TEXT NOT NULL,
  edge_index INTEGER NOT NULL, item_id TEXT NOT NULL, upstream_ordinal INTEGER NOT NULL,
  active INTEGER NOT NULL CHECK(active IN (0,1)),
  PRIMARY KEY(pipeline_name,node_name,binding_digest,edge_index,item_id),
  FOREIGN KEY(item_id) REFERENCES items(id)
);
CREATE TABLE IF NOT EXISTS node_members(
  pipeline_name TEXT NOT NULL, node_name TEXT NOT NULL, binding_digest TEXT NOT NULL,
  item_id TEXT NOT NULL, producer_kind TEXT NOT NULL, producer_name TEXT NOT NULL,
  logical_id TEXT NOT NULL, generation INTEGER NOT NULL, ordinal INTEGER NOT NULL,
  state TEXT NOT NULL CHECK(state IN ('pending','claimed','frozen','terminal','stale')),
  lookup_key TEXT, consumed_sequence INTEGER, visible_result_id INTEGER,
  PRIMARY KEY(pipeline_name, node_name, binding_digest, item_id),
  FOREIGN KEY(item_id) REFERENCES items(id),
  FOREIGN KEY(visible_result_id) REFERENCES stage_results(id)
);
CREATE TABLE IF NOT EXISTS batches(
  id INTEGER PRIMARY KEY AUTOINCREMENT, pipeline_name TEXT NOT NULL, node_name TEXT NOT NULL,
  binding_digest TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('frozen','succeeded','failed')),
  frozen_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS batch_items(
  batch_id INTEGER NOT NULL, ordinal INTEGER NOT NULL, item_id TEXT NOT NULL,
  PRIMARY KEY(batch_id, ordinal), FOREIGN KEY(batch_id) REFERENCES batches(id),
  FOREIGN KEY(item_id) REFERENCES items(id)
);
CREATE INDEX IF NOT EXISTS node_pending
  ON node_members(pipeline_name, node_name, binding_digest, state, ordinal);
CREATE UNIQUE INDEX IF NOT EXISTS node_one_pending_identity
  ON node_members(pipeline_name,node_name,binding_digest,producer_kind,producer_name,logical_id)
  WHERE state='pending';
CREATE INDEX IF NOT EXISTS node_visible
  ON node_members(pipeline_name, node_name, binding_digest, visible_result_id);
CREATE INDEX IF NOT EXISTS node_edge_active
  ON node_edge_members(pipeline_name,node_name,binding_digest,edge_index,active,upstream_ordinal);
CREATE INDEX IF NOT EXISTS batch_current
  ON node_state(pipeline_name, node_name, current_frozen_batch_id);
CREATE INDEX IF NOT EXISTS node_current_binding
  ON node_current_bindings(pipeline_name,node_name,binding_digest);

CREATE TABLE IF NOT EXISTS materialization_claims(
  stage_name TEXT NOT NULL, lookup_key TEXT NOT NULL, claim_id INTEGER PRIMARY KEY AUTOINCREMENT,
  owner_execution_id INTEGER, next_waiting_sequence INTEGER NOT NULL DEFAULT 1,
  UNIQUE(stage_name, lookup_key), FOREIGN KEY(owner_execution_id) REFERENCES executions(id)
);
CREATE TABLE IF NOT EXISTS materialization_waiters(
  stage_name TEXT NOT NULL, lookup_key TEXT NOT NULL, claim_id INTEGER NOT NULL,
  waiting_sequence INTEGER NOT NULL, pipeline_name TEXT, node_name TEXT, force INTEGER NOT NULL,
  history_persisted INTEGER NOT NULL DEFAULT 0,
  generation INTEGER, batch_id INTEGER, envelope_json TEXT NOT NULL, policy_json TEXT NOT NULL,
  config_json TEXT NOT NULL, binding_digest TEXT,
  rebuild_target_id INTEGER,
  PRIMARY KEY(stage_name, lookup_key, waiting_sequence), FOREIGN KEY(claim_id) REFERENCES materialization_claims(claim_id)
);
CREATE INDEX IF NOT EXISTS materialization_owner ON materialization_claims(stage_name,lookup_key,owner_execution_id);
CREATE INDEX IF NOT EXISTS materialization_waiting ON materialization_waiters(stage_name,lookup_key,force,waiting_sequence);
CREATE INDEX IF NOT EXISTS stage_result_lookup ON stage_results(stage_name,lookup_key,sequence);
CREATE INDEX IF NOT EXISTS stage_result_producer ON stage_results(producer_execution_id);

CREATE TABLE IF NOT EXISTS deliveries(
  delivery_key TEXT PRIMARY KEY, pipeline_name TEXT NOT NULL, node_name TEXT NOT NULL,
  rendered_item_id TEXT NOT NULL, binding_digest TEXT NOT NULL, config_json TEXT NOT NULL,
  policy_json TEXT NOT NULL,
  state TEXT NOT NULL CHECK(state IN ('queued','running','succeeded','failed')),
  retry_count INTEGER NOT NULL DEFAULT 0, attempt_token INTEGER, error TEXT, external_id TEXT,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS deliveries_node_state ON deliveries(pipeline_name,node_name,state);
CREATE INDEX IF NOT EXISTS deliveries_retry ON deliveries(state,retry_count,updated_at);
"""


def init_schema(connection: sqlite3.Connection) -> None:
    connection.execute("PRAGMA foreign_keys=ON")
    connection.executescript(_SCHEMA)
    connection.executescript(_RECONCILE_SCHEMA)
    waiter_columns = {
        str(row[1])
        for row in connection.execute("PRAGMA table_info(materialization_waiters)")
    }
    if "history_persisted" not in waiter_columns:
        connection.execute(
            "ALTER TABLE materialization_waiters ADD COLUMN "
            "history_persisted INTEGER NOT NULL DEFAULT 0"
        )
    execution_columns = {
        str(row[1]) for row in connection.execute("PRAGMA table_info(executions)")
    }
    if "history_persisted" not in execution_columns:
        connection.execute(
            "ALTER TABLE executions ADD COLUMN "
            "history_persisted INTEGER NOT NULL DEFAULT 0"
        )
