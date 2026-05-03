-- items master content
CREATE TABLE items (
  item_id      TEXT PRIMARY KEY,
  source_name  TEXT NOT NULL,
  guid         TEXT,
  url          TEXT,
  title        TEXT,
  author       TEXT,
  published_at TEXT,
  ingested_at  TEXT NOT NULL,
  ingested_by  TEXT NOT NULL,
  updated_at   TEXT NOT NULL,
  tag          TEXT,
  raw_summary  TEXT,
  raw_content  TEXT,
  raw_json     TEXT NOT NULL,
  fulltext         TEXT,
  attachments_json TEXT,
  summary          TEXT,
  score            REAL,
  tags_json        TEXT,
  extras_json      TEXT,
  dropped          INTEGER NOT NULL DEFAULT 0,
  dropped_reason   TEXT,
  dropped_at       TEXT
);
CREATE INDEX idx_items_source     ON items(source_name);
CREATE INDEX idx_items_published  ON items(published_at);
CREATE INDEX idx_items_dropped    ON items(dropped);

-- pipelines run master
CREATE TABLE pipelines (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  run_key       TEXT NOT NULL UNIQUE,
  task          TEXT NOT NULL,
  pipeline_name TEXT NOT NULL,
  cycle_anchor  TEXT NOT NULL,
  trigger       TEXT NOT NULL,
  started_at    TEXT NOT NULL,
  finished_at   TEXT,
  status        TEXT NOT NULL,
  items_in      INTEGER,
  items_out     INTEGER,
  error_msg     TEXT
);
CREATE INDEX idx_pipelines_task     ON pipelines(task, pipeline_name);
CREATE INDEX idx_pipelines_started  ON pipelines(started_at);
CREATE INDEX idx_pipelines_run_key  ON pipelines(run_key);

-- stages run master
CREATE TABLE stages (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  pipeline_run_id INTEGER NOT NULL REFERENCES pipelines(id),
  stage_name      TEXT NOT NULL,
  started_at      TEXT NOT NULL,
  finished_at     TEXT,
  status          TEXT NOT NULL,
  items_in        INTEGER,
  items_out       INTEGER,
  error_msg       TEXT
);
CREATE INDEX idx_stages_pipeline ON stages(pipeline_run_id);
CREATE INDEX idx_stages_name     ON stages(stage_name);

-- stage attempt log (single shared table, append-only)
CREATE TABLE stage_attempt_log (
  id                INTEGER PRIMARY KEY AUTOINCREMENT,
  stage_run_id      INTEGER NOT NULL REFERENCES stages(id),
  pipeline_run_id   INTEGER NOT NULL REFERENCES pipelines(id),
  pipeline_name     TEXT NOT NULL,
  stage_name        TEXT NOT NULL,
  item_id           TEXT NOT NULL,
  attempt_no        INTEGER NOT NULL,
  status            TEXT NOT NULL,
  processed_at      TEXT NOT NULL,
  error_class       TEXT,
  error_msg         TEXT
);
CREATE INDEX idx_attempt_log_lookup ON stage_attempt_log(pipeline_name, stage_name, item_id);
CREATE INDEX idx_attempt_log_run    ON stage_attempt_log(stage_run_id);

-- sink emission idempotency (PK = run_key, sink_id, ordinal)
CREATE TABLE sink_emissions (
  run_key      TEXT NOT NULL,
  sink_id      TEXT NOT NULL,
  ordinal      INTEGER NOT NULL,
  sink_type    TEXT NOT NULL,
  message_kind TEXT,
  external_id  TEXT,
  emitted_at   TEXT NOT NULL,
  PRIMARY KEY (run_key, sink_id, ordinal)
);

-- sink delivery log
CREATE TABLE sink_delivery_log (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  run_key       TEXT NOT NULL,
  sink_id       TEXT NOT NULL,
  sink_type     TEXT NOT NULL,
  attempt_at    TEXT NOT NULL,
  ordinal       INTEGER NOT NULL,
  message_kind  TEXT,
  recipient     TEXT,
  external_id   TEXT,
  status        TEXT NOT NULL,
  error_class   TEXT,
  error_msg     TEXT
);
CREATE INDEX idx_delivery_run    ON sink_delivery_log(run_key, sink_id);
CREATE INDEX idx_delivery_status ON sink_delivery_log(status);

-- attachment mirror
CREATE TABLE attachment_mirror (
  url_hash           TEXT PRIMARY KEY,
  url                TEXT NOT NULL,
  local_path         TEXT NOT NULL,
  mime_type          TEXT,
  byte_size          INTEGER,
  fetched_at         TEXT NOT NULL,
  last_referenced_at TEXT NOT NULL,
  task               TEXT NOT NULL
);
CREATE INDEX idx_attachment_task       ON attachment_mirror(task);
CREATE INDEX idx_attachment_referenced ON attachment_mirror(last_referenced_at);

-- gc log
CREATE TABLE gc_log (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  ran_at       TEXT NOT NULL,
  table_name   TEXT NOT NULL,
  rows_deleted INTEGER NOT NULL,
  criteria     TEXT NOT NULL
);

-- schema version sentinel
CREATE TABLE schema_version (
  version    INTEGER PRIMARY KEY,
  applied_at TEXT NOT NULL
);
INSERT INTO schema_version (version, applied_at) VALUES (1, CURRENT_TIMESTAMP);
