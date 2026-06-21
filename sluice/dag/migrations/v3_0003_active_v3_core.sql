-- v3 core runtime schema (idempotent; applied by db.apply_v3_migrations).
-- ALTER TABLE ADD COLUMN statements are issued from Python with existence
-- checks because SQLite has no ADD COLUMN IF NOT EXISTS. Only the new-table
-- and new-index DDL lives here. Comments must not contain semicolons because
-- the runner splits on semicolons.

-- Run-scoped tracking for dynamic table migration
CREATE TABLE IF NOT EXISTS dynamic_schema_version (
  table_name  TEXT PRIMARY KEY,
  version     INTEGER NOT NULL,
  migrated_at TEXT NOT NULL,
  checksum    TEXT
);

-- Durable sink outbox (at-least-once delivery)
CREATE TABLE IF NOT EXISTS sink_outbox (
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id           INTEGER NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
  run_key          TEXT NOT NULL,
  sink_id          TEXT NOT NULL,
  sink_type        TEXT NOT NULL,
  ordinal          INTEGER NOT NULL,
  idempotency_key  TEXT NOT NULL,
  message_kind     TEXT,
  payload_json     TEXT NOT NULL,
  status           TEXT NOT NULL CHECK(status IN ('pending','sent','retryable','failed')),
  attempts         INTEGER NOT NULL DEFAULT 0,
  next_attempt_at  TEXT,
  claim_owner      TEXT,
  claim_token      TEXT,
  claim_until      TEXT,
  sent_at          TEXT,
  last_error_class TEXT,
  last_error_msg   TEXT,
  created_at       TEXT NOT NULL,
  updated_at       TEXT NOT NULL,
  UNIQUE(run_id, sink_id, ordinal),
  UNIQUE(idempotency_key)
);
CREATE INDEX IF NOT EXISTS idx_sink_outbox_claim ON sink_outbox(status, next_attempt_at, id);
CREATE INDEX IF NOT EXISTS idx_sink_outbox_run   ON sink_outbox(run_id, status);

-- Rotation manifest history mirror (sidecar file is source of truth)
CREATE TABLE IF NOT EXISTS rotation_manifest (
  task         TEXT NOT NULL,
  phase        TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  recorded_at  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_rotation_manifest_task ON rotation_manifest(task, recorded_at);
