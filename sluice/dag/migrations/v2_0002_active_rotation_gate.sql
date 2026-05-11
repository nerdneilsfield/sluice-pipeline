CREATE TABLE rotation_gate (
  id     INTEGER PRIMARY KEY CHECK (id = 1),
  closed INTEGER NOT NULL DEFAULT 0
);
INSERT INTO rotation_gate(id, closed) VALUES (1, 0);

CREATE TABLE pipeline_admissions (
  admission_id   TEXT PRIMARY KEY,
  task           TEXT NOT NULL,
  pipeline_name  TEXT NOT NULL,
  registered_at  TEXT NOT NULL
);
CREATE INDEX idx_admissions_task ON pipeline_admissions(task);
CREATE INDEX idx_admissions_task_registered_at
  ON pipeline_admissions(task, registered_at);

INSERT INTO schema_version(version, applied_at) VALUES (2, CURRENT_TIMESTAMP);
