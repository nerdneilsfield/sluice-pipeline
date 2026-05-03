CREATE TABLE item_seen (
  item_id     TEXT PRIMARY KEY,
  source_hash TEXT NOT NULL,
  archived_at TEXT NOT NULL
);

CREATE TABLE schema_version (
  version    INTEGER PRIMARY KEY,
  applied_at TEXT NOT NULL
);
INSERT INTO schema_version (version, applied_at) VALUES (1, CURRENT_TIMESTAMP);
