from __future__ import annotations

from contextlib import contextmanager
import fcntl
import sqlite3
from pathlib import Path
from threading import RLock
from typing import IO, Iterator


class LockedConnection(sqlite3.Connection):
    writer_lock: IO[str]
    transaction_lock: RLock

    def close(self) -> None:
        if hasattr(self, "writer_lock") and not self.writer_lock.closed:
            fcntl.flock(self.writer_lock.fileno(), fcntl.LOCK_UN)
            self.writer_lock.close()
        super().close()


def open_database(path: str | Path) -> sqlite3.Connection:
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


def init_schema(connection: sqlite3.Connection) -> None:
    connection.execute("PRAGMA foreign_keys=ON")
    connection.executescript(_SCHEMA)
