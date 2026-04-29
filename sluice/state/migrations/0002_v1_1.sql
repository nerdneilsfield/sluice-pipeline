CREATE TABLE attachment_mirror (
  url_hash           TEXT PRIMARY KEY,
  url                TEXT NOT NULL,
  local_path         TEXT NOT NULL,
  mime_type          TEXT,
  byte_size          INTEGER,
  fetched_at         TEXT NOT NULL,
  last_referenced_at TEXT NOT NULL,
  pipeline_id        TEXT NOT NULL
);
CREATE INDEX idx_attachment_pipeline    ON attachment_mirror(pipeline_id);
CREATE INDEX idx_attachment_referenced  ON attachment_mirror(last_referenced_at);

CREATE TABLE gc_log (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  ran_at        TEXT NOT NULL,
  table_name    TEXT NOT NULL,
  rows_deleted  INTEGER NOT NULL,
  criteria      TEXT NOT NULL
);

CREATE TABLE sink_delivery_log (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  pipeline_id   TEXT NOT NULL,
  run_key       TEXT NOT NULL,
  sink_id       TEXT NOT NULL,
  sink_type     TEXT NOT NULL,
  attempt_at    TEXT NOT NULL,
  ordinal       INTEGER NOT NULL,
  message_kind  TEXT NOT NULL,
  recipient     TEXT,
  external_id   TEXT,
  status        TEXT NOT NULL,
  error_class   TEXT,
  error_msg     TEXT
);
CREATE INDEX idx_delivery_run    ON sink_delivery_log(pipeline_id, run_key, sink_id);
CREATE INDEX idx_delivery_status ON sink_delivery_log(pipeline_id, status);

CREATE INDEX idx_runlog_started ON run_log(started_at);
