CREATE TABLE seen_items (
  pipeline_id TEXT NOT NULL,
  item_key    TEXT NOT NULL,
  url         TEXT,
  title       TEXT,
  published_at TEXT,
  summary     TEXT,
  seen_at     TEXT NOT NULL,
  PRIMARY KEY (pipeline_id, item_key)
);
CREATE INDEX idx_seen_published ON seen_items(pipeline_id, published_at);

CREATE TABLE failed_items (
  pipeline_id TEXT NOT NULL,
  item_key    TEXT NOT NULL,
  url         TEXT,
  stage       TEXT NOT NULL,
  error_class TEXT NOT NULL,
  error_msg   TEXT NOT NULL,
  attempts    INTEGER NOT NULL DEFAULT 1,
  status      TEXT NOT NULL,
  item_json   TEXT NOT NULL,
  first_failed_at TEXT NOT NULL,
  last_failed_at  TEXT NOT NULL,
  PRIMARY KEY (pipeline_id, item_key)
);
CREATE INDEX idx_failed_status ON failed_items(pipeline_id, status);

CREATE TABLE sink_emissions (
  pipeline_id TEXT NOT NULL,
  run_key     TEXT NOT NULL,
  sink_id     TEXT NOT NULL,
  sink_type   TEXT NOT NULL,
  external_id TEXT,
  emitted_at  TEXT NOT NULL,
  PRIMARY KEY (pipeline_id, run_key, sink_id)
);

CREATE TABLE url_cache (
  url_hash    TEXT PRIMARY KEY,
  url         TEXT NOT NULL,
  fetcher     TEXT NOT NULL,
  markdown    TEXT NOT NULL,
  fetched_at  TEXT NOT NULL,
  expires_at  TEXT NOT NULL
);

CREATE TABLE run_log (
  pipeline_id TEXT NOT NULL,
  run_key     TEXT NOT NULL,
  started_at  TEXT NOT NULL,
  finished_at TEXT,
  status      TEXT NOT NULL,
  items_in    INTEGER,
  items_out   INTEGER,
  llm_calls   INTEGER,
  est_cost_usd REAL,
  error_msg   TEXT,
  PRIMARY KEY (pipeline_id, run_key)
);
