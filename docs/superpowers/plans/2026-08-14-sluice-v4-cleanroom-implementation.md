# Sluice v4 Clean-room Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the approved Sluice v4 clean-room runtime: safe YAML definitions drive immutable SQLite-backed Sources, reusable Stage materializations, deterministic Pipeline reconciliation and batching, independent Deliveries, local/Prefect entrypoints, and the required management read model/UI.

**Architecture:** A small Python package calls ordinary application functions (`tick`, `collect_source`, `reconcile`, `reconcile_node`, `execute_stage`, `rebuild_stage`, and `deliver`) against one SQLite connection. `db.py` owns schema creation and transactions, while the runtime modules issue SQL directly; there is deliberately no repository layer, event bus, reconciler service, runner-backend abstraction, or test-only orchestration API. Immutable Items, Publications, StageResults, Executions, node membership, claims, batches, and Deliveries are the durable source of truth; Prefect, the CLI, and the UI invoke the same functions.

**Tech Stack:** Python 3.12; `sqlite3`, `hashlib`, `json`, `datetime`, `urllib`, `xml.etree.ElementTree`, and `html` from the standard library; `PyYAML` with `yaml.safe_load`; optional Flask for the small management UI; optional Prefect 3 for the flow entrypoint; pytest for tests. Core installation does not pull UI/Prefect dependencies. No ORM or migration framework is introduced.

## Global Constraints

- SQLite is the only state store; one process owns and writes one database at a time. Prefect and the local CLI are alternate entrypoints and must not share a database concurrently.
- YAML is the only human-authored definition format. Load it with `yaml.safe_load` into ordinary configuration objects; do not add a compiler IR, Python DSL, schema registry, static type inference, discovery, entry points, registry, marketplace, or hot loading.
- A Stage is the only materialization/cache boundary. Task intermediates remain transient; an expensive reusable operation must be its own Stage.
- Items, StageResults, Publications, and Execution input/configuration snapshots are immutable. Use canonical JSON (sorted object keys, JSON number semantics, unchanged strings, UTC-normalized timestamps) for IDs, hashes, and cache keys.
- Source, Task, and Sink callables resolve through fixed built-in path maps or local Python dotted paths exactly once during `open_app` startup; the validated bindings are then immutable for the process. No Task may read hidden mutable process state, wall-clock time, or randomness to affect a result.
- An `each` Task returns exactly one `DROP`, `FORWARD(item)`, or `NEW(value, metadata={})`. A `batch` Task returns an ordered tuple of those markers. Bare `None`, foreign `FORWARD`, multiple `each` outputs, and more than one batch `NEW` are execution errors. `KEEP_CHECKPOINT` is an identity singleton and is never persisted; JSON `null` remains an explicit checkpoint.
- Execution states are exactly `queued`, `running`, `succeeded`, `failed`, `cached`, and `dropped`. Failed Executions publish no StageResult; only exhausted per-item `llm_extract` with `invalid: drop` may publish an empty `dropped` StageResult.
- Stage cache identity includes Stage name, `cache_version`, canonical result-affecting Task configuration (including prompt/schema file digests), and the complete ordered immutable input envelope plus dedup history. Pipeline name, schedule, batch trigger, retries, timeout, concurrency, and executor are excluded.
- Source binding serialization is `(name, use path, canonical with)`, Stage node binding is fully-qualified `(Pipeline, node)` plus Stage definition, ordered `from`, schedule, and batch policy; binding changes activate a new current binding without live reload or historical deletion.
- Schedules are persisted UTC interval durations such as `10m`, `3h`, or `6h`; no cron, timezone, sleep-based test clock, misfire DSL, sliding window, session window, watermark, or “all available” implicit batch policy.
- Count/max-wait/on-schedule batching freezes an immutable ordered envelope and monotonic `batch_id` in one SQLite transaction. A frozen batch never changes; retries and rebuilds retain its `batch_id`; arrivals after a freeze remain pending.
- Reconciliation is an ordinary idempotent function invoked by `tick(at)`, completion callbacks, Stage completion, Prefect, the local CLI, or the UI. There is no persistent reconciliation queue, event bus, control-plane protocol, or separate Reconciler service.
- Guard Source/Stage/Delivery completion and failure with the current Execution ID or Delivery attempt token. A late completion that loses the compare-and-swap discards its proposed output and cannot overwrite the winner.
- A shared `(Stage, lookup key)` has one stable claim with a replaceable owner Execution. Waiters consume no attempt budget or concurrency; forced rebuild waiters are promoted before normal waiters after owner exhaustion.
- Deliveries are side-effecting terminal records with independent status/retry/concurrency. Local duplicate suppression is best effort and cannot eliminate the external-send crash window.
- The tests must follow spec §15: behavior-level TDD at the YAML → production functions/CLI → real temporary SQLite boundary; only small table-driven unit tests for marker validation, cache semantic equality, collection decisions, graph validation, and pure freeze eligibility/order; four focused integration scenario families; one deterministic end-to-end smoke, one Prefect parity smoke, and one SQLite-backed management/UI smoke.
- Do not create repository/test-DSL/fake-orchestrator/Clock layers, mock internal application functions or SQLite, persist Task internals for seams, write a browser test matrix, use live external services, or add compatibility/migration/adapters for v1–v3.

## File Map (created by the tasks below)

- `pyproject.toml` — package metadata, runtime/test dependencies, and the `sluice` CLI entry point.
- `src/sluice/contracts.py` — JSON aliases, immutable runtime values, validated Definition/policy dataclasses, marker singletons, adapter protocols, and approved Source/Task/Sink contracts.
- `src/sluice/canonical.py` — canonical JSON, UTC timestamp normalization, content/item IDs, configuration/file digests, and stage/delivery binding-key primitives.
- `src/sluice/db.py` — direct `sqlite3` connection setup, incrementally assembled static schema DDL, foreign keys, indexes, and transaction helpers; no repository objects or migration subsystem.
- `src/sluice/logging.py` — one ordinary structured JSON log helper over Python `logging`; no service, exporter, or monitoring platform.
- `src/sluice/config.py` — safe YAML decoding into the contract dataclasses, duration/policy parsing, file-digest capture, and reference/cycle/mode/batch validation.
- `src/sluice/resolution.py` — fixed built-in Source/Task/Sink path resolution plus startup loading of configured dotted paths and mode checks; it is not a plugin registry.
- `src/sluice/app.py` — immutable process-start `App` containing the loaded Definition, SQLite connection, and already-bound Source/Task/Sink closures (not a second raw client bundle).
- `src/sluice/sources/rss.py` and `src/sluice/sources/webpage.py` — initial Source implementations returning production `SourceResult` values.
- `src/sluice/source_runtime.py` — serialized Source claim, collection validation, and all-or-none Item/delta/checkpoint finalization; the single recovery dispatcher lives with `tick`.
- `src/sluice/tasks/collection.py` — `dedup`, `filter`, `sort`, and `limit` Tasks plus deterministic field/rule lookup and Levenshtein change-rate decisions.
- `src/sluice/tasks/content.py` — HTTP fetch, HTML extraction, and Markdown conversion; channel renderers are added only with Delivery.
- `src/sluice/tasks/llm.py` — structured per-item extraction and batch summary Tasks with prompt/schema digest handling.
- `src/sluice/stage_runtime.py` — Stage envelope/key construction, each/batch Task execution, materialization claims, guarded finalization, cache hits, retries, and exact-envelope rebuild.
- `src/sluice/reconcile.py` — publication cursor/snapshot admission, ordered union and duplicate fallback, stale generations, batch freeze decisions, schedule advancement, `tick`, and `reconcile_node`.
- `src/sluice/delivery.py` — Delivery binding/key construction, token-guarded send/retry/recovery, and independent endpoint state.
- `src/sluice/read_model.py` — deterministic Pipeline list/detail projections and action functions shared by CLI/UI.
- `src/sluice/ui.py` — minimal Flask HTML routes/forms for list/detail, rebuild, and Delivery retry actions (no JSON API).
- `src/sluice/prefect_flow.py` — thin Prefect Flow calling `open_app` and `tick` only.
- `src/sluice/cli.py` — local one-shot reconciliation, rebuild, Delivery retry, and foreground tick commands.
- `tests/unit/test_contracts_and_keys.py` — the permitted table-driven marker and canonical-key decisions.
- `tests/unit/test_collection_and_graph.py` — the permitted table-driven collection, validation, and freeze pure decisions.
- `tests/integration/test_source_lifecycle.py` — Source/publication/checkpoint scenario family.
- `tests/integration/test_shared_materialization.py` — shared Stage owner/waiter, restart, reconciliation/order, and stale-finalization scenario family.
- `tests/integration/test_llm_batch_rebuild.py` — invalid LLM, frozen batch, retry, and rebuild scenario family.
- `tests/integration/test_delivery.py` — independent Delivery/retry/token scenario family.
- `tests/smoke/test_acceptance.py` — one deterministic branching/merging end-to-end smoke.
- `tests/smoke/test_prefect_parity.py` — one Prefect-vs-local entrypoint parity smoke.
- `tests/smoke/test_management.py` — one SQLite-backed read-model/UI/action smoke using Flask’s test client, not a browser matrix.

---

### Task 1: Bootstrap the clean-room package, production contracts, canonical values, and SQLite schema

**Files:**
- Create: `pyproject.toml`
- Create: `src/sluice/__init__.py`
- Create: `src/sluice/contracts.py`
- Create: `src/sluice/canonical.py`
- Create: `src/sluice/db.py`
- Create: `src/sluice/errors.py`
- Test: `tests/unit/test_contracts_and_keys.py`

**Interfaces:**
- Produces `JSONValue`, deeply immutable `Item`, `SourceRecord`, `SourceResult`, `Drop`/`DROP`, `KeepCheckpoint`/`KEEP_CHECKPOINT`, `FORWARD(item)`, and `NEW(value, metadata={})` in `sluice.contracts`.
- Produces only the base `ExecutionError` in `sluice.errors`; definition-, graph-, and LLM-specific classifications are added by the slices that first consume them.
- Produces `canonical_json(value) -> str`, `utc_datetime(value) -> datetime`, `digest(value) -> str`, `content_hash(payload) -> str`, and `make_item_id(producer_scope, logical_id, payload, metadata) -> str` in `sluice.canonical`.
- Produces `open_database(path: str | Path) -> sqlite3.Connection`, `init_schema(conn) -> None`, and `write_transaction(conn) -> contextmanager` in `sluice.db`; all later runtime code executes SQL through this connection rather than a repository abstraction.

- [ ] **Step 1: Write the permitted table-driven red tests for contracts and canonical semantics.**

```python
# tests/unit/test_contracts_and_keys.py
import pytest
from sluice.canonical import canonical_json, digest, make_item_id
from sluice.contracts import DROP, FORWARD, KEEP_CHECKPOINT, NEW, Drop, Item, KeepCheckpoint


def test_marker_values_distinguish_drop_forward_new_none_and_checkpoint_keep():
    assert isinstance(DROP, Drop)
    assert isinstance(KEEP_CHECKPOINT, KeepCheckpoint)
    item = Item(
        id="i", producer_scope=("source", "s"), logical_id="l",
        payload={"x": 1}, metadata={}, content_hash="h",
        observed_at="2026-08-14T00:00:00Z", produced_by=1,
    )
    assert FORWARD(item).item is item
    assert NEW(None).value is None


def test_canonical_json_ignores_object_key_order_but_not_string_or_number_value():
    assert canonical_json({"b": 2, "a": 1}) == '{"a":1,"b":2}'
    assert digest({"a": 1, "b": 2}) == digest({"b": 2, "a": 1})
    assert digest({"a": 1}) != digest({"a": "1"})


def test_item_id_includes_typed_producer_scope_and_immutable_metadata():
    left = make_item_id(("source", "news"), "same", {"url": "u"}, {})
    right = make_item_id(("stage", "news"), "same", {"url": "u"}, {})
    assert left != right


def test_item_payload_and_metadata_are_deeply_immutable():
    item = Item(
        id="i", producer_scope=("source", "s"), logical_id="l",
        payload={"nested": {"x": 1}}, metadata={"tags": ["a"]}, content_hash="h",
        observed_at="2026-08-14T00:00:00Z", produced_by=1,
    )
    with pytest.raises(TypeError):
        item.payload["nested"]["x"] = 2
    with pytest.raises(AttributeError):
        item.metadata["tags"].append("b")
```

- [ ] **Step 2: Run the focused unit test to verify the clean-room package is absent.**

Run: `pytest -q tests/unit/test_contracts_and_keys.py`

Expected: FAIL during collection with `ModuleNotFoundError: No module named 'sluice'`.

- [ ] **Step 3: Add the minimal package metadata and immutable production contracts.**

```toml
# pyproject.toml
[build-system]
requires = ["setuptools>=75"]
build-backend = "setuptools.build_meta"

[project]
name = "sluice"
version = "4.0.0"
requires-python = ">=3.12"
dependencies = ["PyYAML>=6.0,<7"]

[project.optional-dependencies]
ui = ["Flask>=3.0,<4"]
prefect = ["prefect>=3.0,<4"]
test = ["pytest>=8.0,<9"]

[project.scripts]
sluice = "sluice.cli:main"

[tool.pytest.ini_options]
pythonpath = ["src"]
testpaths = ["tests"]
```

`contracts.py` must use frozen dataclasses and expose these exact shapes:

```python
from __future__ import annotations
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Final, Literal, Mapping, TypeAlias

JSONValue: TypeAlias = None | bool | int | float | str | list["JSONValue"] | dict[str, "JSONValue"]
FrozenJSON: TypeAlias = None | bool | int | float | str | tuple["FrozenJSON", ...] | Mapping[str, "FrozenJSON"]

def freeze_item_json(value: JSONValue) -> FrozenJSON:
    if isinstance(value, dict):
        return MappingProxyType({key: freeze_item_json(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(freeze_item_json(item) for item in value)
    return value

@dataclass(frozen=True)
class Item:
    id: str
    producer_scope: tuple[Literal["source", "stage"], str]
    logical_id: str
    payload: FrozenJSON
    metadata: Mapping[str, FrozenJSON]
    content_hash: str
    observed_at: str                 # canonical UTC RFC3339 text at the public boundary
    produced_by: int
    def __post_init__(self) -> None:
        object.__setattr__(self, "payload", freeze_item_json(self.payload))
        frozen_metadata = freeze_item_json(dict(self.metadata))
        object.__setattr__(self, "metadata", frozen_metadata)

@dataclass(frozen=True)
class SourceRecord:
    logical_id: str
    payload: JSONValue
    metadata: Mapping[str, JSONValue] = field(default_factory=dict)

@dataclass(frozen=True)
class SourceResult:
    records: tuple[SourceRecord, ...]
    next_checkpoint: JSONValue | KeepCheckpoint

@dataclass(frozen=True)
class KeepCheckpoint:
    """Identity-only marker that preserves the previous Source checkpoint."""
    __slots__ = ()
KEEP_CHECKPOINT: Final = KeepCheckpoint()

@dataclass(frozen=True)
class Drop:
    """Explicit Task output that produces no value."""
    __slots__ = ()
DROP: Final = Drop()

@dataclass(frozen=True)
class FORWARD:
    item: Item

@dataclass(frozen=True)
class NEW:
    value: Any
    metadata: Mapping[str, JSONValue] = field(default_factory=dict)
```

- [ ] **Step 4: Implement canonical JSON/hash functions and the one shared Item/Execution/Publication substrate.**

`canonical.py` must recursively normalize any immutable `Mapping`/tuple Item view back to ordinary JSON objects/arrays, normalize aware/naive timestamps to UTC RFC3339 with `Z`, call `json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)`, and hash UTF-8 bytes with SHA-256. `make_item_id` hashes `{producer_scope, logical_id, payload, metadata}`; it rejects an empty logical ID, non-object metadata, NaN/Infinity, or non-JSON values with `ExecutionError`. `load_item` always constructs this deep-frozen `Item`, so no Task can mutate a persisted or later-chain input through a nested reference.

`errors.py` defines `class ExecutionError(RuntimeError): """A claimed Source, Stage, or Delivery attempt failed."""`. Later tasks extend this file only when their first red test needs a narrower classification.

`db.py` must enable foreign keys, set `conn.row_factory = sqlite3.Row`, acquire and retain one non-blocking process-local writer lock for the adjacent database lock file, and implement `write_transaction` as an explicit `BEGIN IMMEDIATE`/commit/rollback context manager. It creates the shared Item/Execution/input/Publication substrate below once; nullable Execution columns are the final superset used by Source, Stage, retry, and rebuild attempts, not a temporary Source schema. Later tasks add only their owned Stage-result, node/batch, claim, and Delivery tables to this same fresh-database `init_schema`; there is no migration framework.

```python
import fcntl
import sqlite3
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

def open_database(path: str | Path) -> sqlite3.Connection:
    database_path = Path(path)
    database_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(
        database_path, isolation_level=None, check_same_thread=False, factory=LockedConnection
    )
    connection.writer_lock = open(database_path.with_suffix(database_path.suffix + ".lock"), "a+", encoding="utf-8")
    connection.transaction_lock = RLock()
    fcntl.flock(connection.writer_lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    return connection

@contextmanager
def write_transaction(connection: sqlite3.Connection):
    with connection.transaction_lock:
        connection.execute("BEGIN IMMEDIATE")
        try:
            yield
        except BaseException:
            connection.rollback()
            raise
        else:
            connection.commit()
```

```sql
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
```

```sql
CREATE UNIQUE INDEX IF NOT EXISTS source_one_active_execution
  ON executions(binding_digest) WHERE kind='source' AND state IN ('queued','running');
CREATE INDEX IF NOT EXISTS source_expected_checkpoint
  ON executions(binding_digest,expected_checkpoint_json) WHERE kind='source';
```

Binding activation inserts the new `source_state`, then upserts exactly one `source_current_bindings` row in the same `BEGIN IMMEDIATE` transaction; readers join this pointer and never infer current state from a mutable flag. A Source finalization allocates its next immutable Publication revision with `COALESCE(MAX(revision), 0) + 1` for that producer/binding inside the same writer transaction; `source_state` does not duplicate the revision counter. Later schema steps add indexes for current StageResults, active node members, pending/frozen states, materialization waiters, and Delivery retries; do not introduce another storage abstraction.

- [ ] **Step 5: Run the focused unit test to verify the bootstrap implementation.**

Run: `python -m pip install -e '.[test]' && pytest -q tests/unit/test_contracts_and_keys.py`

Expected: PASS (all table-driven marker and canonical-key cases).

- [ ] **Step 6: Commit the independently usable foundation.**

```bash
git add pyproject.toml src/sluice tests/unit/test_contracts_and_keys.py
git commit -m "feat: bootstrap v4 contracts canonical values and sqlite schema"
```

### Task 2: Load YAML definitions, resolve startup callables, and validate the Pipeline DAG

**Files:**
- Create: `src/sluice/config.py`
- Create: `src/sluice/resolution.py`
- Create: `src/sluice/app.py`
- Create: `tests/unit/test_collection_and_graph.py`
- Modify: `src/sluice/contracts.py` (add frozen Definition/policy values plus the Source/Task/Sink and HTTP/LLM boundary protocols)
- Modify: `src/sluice/errors.py` (add `DefinitionError` and `GraphError`)

**Interfaces:**
- Produces `load_definition(path: str | Path) -> Definition`, `validate_definition(definition: Definition) -> None`, `parse_interval(text: str) -> timedelta` for `m|h` schedules/batches, `parse_timeout(text: str) -> timedelta` for `s|m|h` execution bounds, and `canonical_stage_config(name, input_mode, tasks, output_schema, cache_version, base_dir) -> str` in `sluice.config`.
- Produces `resolve_source(use: str, adapters: BoundaryAdapters) -> SourceContract`, `resolve_task(use: str, expected_mode: Literal["each","batch"], adapters: BoundaryAdapters) -> TaskContract`, and `resolve_sink(use: str, adapters: BoundaryAdapters) -> SinkContract` in `sluice.resolution`; built-in names are a fixed map and configured paths are imported once at startup.
- Produces `open_app(definition_path: str | Path, db_path: str | Path | None = None, *, llm_client=None, http_client=None, sink_clients=None) -> App` in `sluice.app`; adapters are immutable startup inputs for the built-in HTTP/LLM/Sink boundaries, not mutable globals or test orchestration.

- [ ] **Step 1: Write table-driven red tests for safe loading and graph rules.**

```python
# append to tests/unit/test_collection_and_graph.py
from datetime import timedelta
import pytest
import yaml
from sluice.config import load_definition
from sluice.errors import DefinitionError, GraphError

def sink_callable(config, payload, delivery_key):
    return None


@pytest.mark.parametrize("fragment", [
    {"pipelines": {"p": {"nodes": {"a": {"stage": "missing", "from": "source.x"},
                                      "out": {"from": "a", "deliver": {"use": "tests.unit.test_collection_and_graph.sink_callable", "with": {}}}}}}},
    {"pipelines": {"p": {"nodes": {"a": {"from": "b"}, "b": {"from": "a"}}}}},
    {"stages": {"s": {"input": "each", "tasks": [{"use": "sluice.tasks.dedup"}]}},
     "pipelines": {"p": {"nodes": {"a": {"stage": "s", "from": "source.x", "batch": {"count": 2}},
                                      "out": {"from": "a", "deliver": {"use": "tests.unit.test_collection_and_graph.sink_callable", "with": {}}}}}}},
    {"sources": {"x": {"use": "tests.unit.test_collection_and_graph.source_callable", "with": {}}},
     "stages": {"s": {"input": "each", "tasks": [{"use": "sluice.tasks.to_markdown", "with": {"invalid": "drop"}}]}},
     "pipelines": {"p": {"nodes": {"a": {"stage": "s", "from": "source.x"},
                                      "out": {"from": "a", "deliver": {"use": "tests.unit.test_collection_and_graph.sink_callable", "with": {}}}}}}},
])
def test_invalid_graph_or_stage_policy_is_rejected(tmp_path, fragment):
    path = tmp_path / "bad.yml"
    path.write_text("version: 1\n" + yaml.safe_dump(fragment), encoding="utf-8")
    with pytest.raises((DefinitionError, GraphError)):
        load_definition(path)


def test_loader_is_safe_and_preserves_yaml_with_mapping(tmp_path):
    path = tmp_path / "definition.yml"
    path.write_text("""
version: 1
database: db.sqlite
sources:
  s:
    use: sluice.sources.rss
    with: {url: https://example.test/feed.xml}
stages:
  pass:
    input: each
    tasks: [{use: sluice.tasks.to_markdown}]
pipelines:
  p:
    nodes:
      out: {stage: pass, from: source.s, timeout: 120s}
      email: {from: out, deliver: {use: tests.unit.test_collection_and_graph.sink_callable, with: {target: a@example.test}}}
""", encoding="utf-8")
    definition = load_definition(path)
    assert definition.sources["s"].with_config["url"] == "https://example.test/feed.xml"
    assert definition.pipelines["p"].nodes["out"].from_refs == ("source.s",)
    assert definition.pipelines["p"].nodes["out"].policy.timeout == timedelta(seconds=120)
```

- [ ] **Step 2: Run the focused unit test to verify that no loader exists.**

Run: `pytest -q tests/unit/test_collection_and_graph.py`

Expected: FAIL with `ModuleNotFoundError: No module named 'sluice.config'` (and a missing `yaml` import until the test dependency is installed).

- [ ] **Step 3: Implement frozen configuration objects, safe YAML loading, and exact validation.**

Use these public fields (no hidden compiler IR): `ExecutionPolicy(timeout: timedelta | None, retries: int, concurrency: int)`, `BatchPolicy(count: int | None, max_wait: timedelta | None, on_schedule: bool, include_updates: bool)`, `SourceDef(name, use, schedule, with_config, policy)`, `TaskDef(use, with_config)`, `StageDef(name, input_mode, tasks, output_schema, cache_version, canonical_config_json)`, `NodeDef(name, stage, from_refs, schedule, batch, policy, deliver)`, `PipelineDef(name, nodes)`, and `Definition(version, database, sources, stages, pipelines)`. `canonical_config_json` is a complete, decodable Stage snapshot: name, input mode, cache version, task paths/configuration, output schema, and normalized absolute prompt/schema paths plus their digests; it contains no Pipeline policy. Normalize `from` strings to one-element tuples and lists to declaration-order tuples. Reject unknown top-level keys, non-mapping `with`, missing `version: 1`, empty task lists, duplicate names, unknown references, cycles, any Pipeline without a Delivery endpoint, `batch` on an `each` Stage, a batch Stage without positive `count`/`max_wait`/`on_schedule`, `on_schedule` without a node schedule, invalid intervals, negative retries, non-positive concurrency, Source `concurrency`, and a built-in Task in the wrong mode. Reserve `invalid: drop` at load time exclusively for built-in `sluice.tasks.llm_extract` in an `each` Stage; reject it on every other Task/mode before the LLM implementation exists. A definition with Sources but no Pipelines is valid for collection-only operation; the Delivery rule applies per declared Pipeline.

The dataclass defaults are `ExecutionPolicy(timeout=None, retries=0, concurrency=1)` and `BatchPolicy(count=None, max_wait=None, on_schedule=False, include_updates=False)`, so the table-driven constructors below are complete production values rather than test shortcuts. Recursively freeze validated `with` and output-schema JSON mappings before storing them; Task/Source/Sink calls receive a fresh deep JSON copy, so user callables cannot mutate the Definition or alter a later cache/binding digest.

This slice is the single owner of callable contracts and configuration errors; later Source/Stage/LLM tasks implement them but do not redefine their class identity:

```python
# append to src/sluice/contracts.py
from typing import Protocol

TaskMarker: TypeAlias = Drop | FORWARD | NEW
EachTaskResult: TypeAlias = TaskMarker
BatchTaskResult: TypeAlias = tuple[TaskMarker, ...]

class SourceContract(Protocol):
    def collect(self, config: Mapping[str, JSONValue], checkpoint: JSONValue) -> SourceResult:
        raise NotImplementedError

class TaskContract(Protocol):
    input_mode: Literal["each", "batch"]
    def run(self, config: Mapping[str, JSONValue], input: Any) -> EachTaskResult | BatchTaskResult:
        raise NotImplementedError

class SinkContract(Protocol):
    def send(self, config: Mapping[str, JSONValue], rendered_payload: JSONValue, delivery_key: str) -> str | None:
        raise NotImplementedError

class HTTPClient(Protocol):
    def get(self, url: str) -> str:
        raise NotImplementedError

class LLMClient(Protocol):
    def complete(self, model: str, prompt: str, input_payload: JSONValue) -> JSONValue:
        raise NotImplementedError
```

Define the frozen authoring values once in the same module; `config.py` decodes into these classes and later slices import them rather than creating parallel types:

```python
# append to src/sluice/contracts.py
from datetime import timedelta

@dataclass(frozen=True, slots=True)
class ExecutionPolicy:
    timeout: timedelta | None = None
    retries: int = 0
    concurrency: int = 1

@dataclass(frozen=True, slots=True)
class BatchPolicy:
    count: int | None = None
    max_wait: timedelta | None = None
    on_schedule: bool = False
    include_updates: bool = False

@dataclass(frozen=True, slots=True)
class SourceDef:
    name: str
    use: str
    schedule: timedelta | None
    with_config: Mapping[str, FrozenJSON]
    policy: ExecutionPolicy

@dataclass(frozen=True, slots=True)
class TaskDef:
    use: str
    with_config: Mapping[str, FrozenJSON]

@dataclass(frozen=True, slots=True)
class StageDef:
    name: str
    input_mode: Literal["each", "batch"]
    tasks: tuple[TaskDef, ...]
    output_schema: Mapping[str, FrozenJSON] | None
    cache_version: int
    canonical_config_json: str

@dataclass(frozen=True, slots=True)
class DeliveryDef:
    use: str
    with_config: Mapping[str, FrozenJSON]

@dataclass(frozen=True, slots=True)
class NodeDef:
    name: str
    stage: str | None
    from_refs: tuple[str, ...]
    schedule: timedelta | None
    batch: BatchPolicy | None
    policy: ExecutionPolicy
    deliver: DeliveryDef | None

@dataclass(frozen=True, slots=True)
class PipelineDef:
    name: str
    nodes: Mapping[str, NodeDef]

@dataclass(frozen=True, slots=True)
class Definition:
    version: int
    database: str
    sources: Mapping[str, SourceDef]
    stages: Mapping[str, StageDef]
    pipelines: Mapping[str, PipelineDef]
```

`errors.py` adds `DefinitionError(ValueError)` and `GraphError(DefinitionError)` here. `InvalidStructuredOutput` remains owned by the LLM slice that first uses it.

```python
# src/sluice/config.py (core shape)
import hashlib
import json
import re
from collections.abc import Mapping
from datetime import timedelta
from pathlib import Path
from types import MappingProxyType

import yaml

from sluice.canonical import canonical_json
from sluice.contracts import (
    BatchPolicy, Definition, DeliveryDef, ExecutionPolicy, JSONValue,
    NodeDef, PipelineDef, SourceDef, StageDef, TaskDef,
)
from sluice.errors import DefinitionError, GraphError

BUILTIN_TASK_MODES = {
    "sluice.tasks.dedup": "batch",
    "sluice.tasks.filter": "batch",
    "sluice.tasks.sort": "batch",
    "sluice.tasks.limit": "batch",
    "sluice.tasks.fetch_webpage": "each",
    "sluice.tasks.extract_content": "each",
    "sluice.tasks.to_markdown": "each",
    "sluice.tasks.llm_extract": "each",
    "sluice.tasks.llm_summary": "batch",
    "sluice.tasks.render_email": "each",
    "sluice.tasks.render_telegram": "each",
}

def _mapping(value: object, label: str) -> dict[str, object]:
    if value is None:
        return {}
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise DefinitionError(f"{label} must be a mapping")
    return value

def freeze_json(value: object) -> object:
    if isinstance(value, dict):
        return MappingProxyType({key: freeze_json(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(freeze_json(item) for item in value)
    return value

def thaw_json(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [thaw_json(item) for item in value]
    return value


def _decode_definition(raw: dict[str, object], base_dir: Path) -> Definition:
    allowed = {"version", "database", "sources", "stages", "pipelines"}
    unknown = set(raw) - allowed
    if unknown:
        raise DefinitionError(f"unknown top-level keys: {sorted(unknown)}")
    return Definition(
        version=1,
        database=str(raw.get("database", ".sluice/sluice.db")),
        sources=MappingProxyType(_decode_sources(_mapping(raw.get("sources"), "sources"))),
        stages=MappingProxyType(_decode_stages(_mapping(raw.get("stages"), "stages"), base_dir)),
        pipelines=MappingProxyType(_decode_pipelines(_mapping(raw.get("pipelines"), "pipelines"))),
    )


def _decode_sources(raw: dict[str, object]) -> dict[str, SourceDef]:
    decoded: dict[str, SourceDef] = {}
    for name, value in raw.items():
        spec = _mapping(value, f"sources.{name}")
        if "concurrency" in spec:
            raise DefinitionError(f"sources.{name} cannot set concurrency")
        decoded[name] = SourceDef(name=name, use=str(spec["use"]), schedule=_optional_duration(spec.get("schedule")),
                                   with_config=freeze_json(_mapping(spec.get("with"), f"sources.{name}.with")),
                                   policy=_decode_source_policy(spec))
    return decoded


def _decode_stages(raw: dict[str, object], base_dir: Path) -> dict[str, StageDef]:
    decoded: dict[str, StageDef] = {}
    for name, value in raw.items():
        spec = _mapping(value, f"stages.{name}")
        tasks = tuple(_decode_task(_mapping(task, f"stages.{name}.tasks[{index}]"), base_dir) for index, task in enumerate(spec.get("tasks", [])))
        raw_output_schema = _mapping(spec.get("output"), f"stages.{name}.output") or None
        output_schema = None if raw_output_schema is None else freeze_json(raw_output_schema)
        input_mode = str(spec["input"])
        for task in tasks:
            expected_mode = BUILTIN_TASK_MODES.get(task.use)
            if expected_mode is not None and expected_mode != input_mode:
                raise DefinitionError(f"{task.use} requires {expected_mode} input")
            invalid = task.with_config.get("invalid")
            if invalid is not None and not (
                invalid == "drop" and input_mode == "each" and task.use == "sluice.tasks.llm_extract"
            ):
                raise DefinitionError("invalid: drop is only valid for each llm_extract")
        cache_version = int(spec.get("cache_version", 1))
        decoded[name] = StageDef(name=name, input_mode=input_mode, tasks=tasks,
                                 output_schema=output_schema, cache_version=cache_version,
                                 canonical_config_json=canonical_stage_config(
                                     name, input_mode, tasks, output_schema, cache_version, base_dir
                                 ))
    return decoded


def _decode_task(spec: dict[str, object], base_dir: Path) -> TaskDef:
    config = _mapping(spec.get("with"), "task.with")
    for key in ("prompt", "schema", "output_schema"):
        if key in config:
            config[key] = str((base_dir / str(config[key])).resolve())
    return TaskDef(use=str(spec["use"]), with_config=freeze_json(config))


def _decode_pipelines(raw: dict[str, object]) -> dict[str, PipelineDef]:
    return {name: _decode_pipeline(name, _mapping(value, f"pipelines.{name}")) for name, value in raw.items()}


def _decode_pipeline(name: str, spec: dict[str, object]) -> PipelineDef:
    nodes = {node_name: _decode_node(node_name, _mapping(node, f"pipelines.{name}.nodes.{node_name}"))
             for node_name, node in _mapping(spec.get("nodes"), f"pipelines.{name}.nodes").items()}
    return PipelineDef(name=name, nodes=MappingProxyType(nodes))


def _decode_node(name: str, spec: dict[str, object]) -> NodeDef:
    refs = spec.get("from", ())
    from_refs = (str(refs),) if isinstance(refs, str) else tuple(str(ref) for ref in refs)
    delivery = _decode_delivery(_mapping(spec["deliver"], f"node.{name}.deliver")) if "deliver" in spec else None
    return NodeDef(name=name, stage=str(spec["stage"]) if "stage" in spec else None, from_refs=from_refs,
                   schedule=_optional_duration(spec.get("schedule")), batch=_decode_batch(spec.get("batch")),
                   policy=_decode_policy(spec), deliver=delivery)


def _decode_delivery(spec: dict[str, object]) -> DeliveryDef:
    return DeliveryDef(use=str(spec["use"]), with_config=freeze_json(_mapping(spec.get("with"), "deliver.with")))


def _optional_duration(value: object) -> timedelta | None:
    return None if value is None else parse_interval(str(value))


def _decode_batch(value: object) -> BatchPolicy | None:
    if value is None:
        return None
    spec = _mapping(value, "batch")
    return BatchPolicy(count=int(spec["count"]) if "count" in spec else None,
                       max_wait=_optional_duration(spec.get("max_wait")),
                       on_schedule=bool(spec.get("on_schedule", False)),
                       include_updates=bool(spec.get("include_updates", False)))


def _decode_policy(spec: dict[str, object]) -> ExecutionPolicy:
    timeout = None if spec.get("timeout") is None else parse_timeout(str(spec["timeout"]))
    return ExecutionPolicy(timeout=timeout, retries=int(spec.get("retries", 0)), concurrency=int(spec.get("concurrency", 1)))

def _decode_source_policy(spec: dict[str, object]) -> ExecutionPolicy:
    timeout = None if spec.get("timeout") is None else parse_timeout(str(spec["timeout"]))
    return ExecutionPolicy(timeout=timeout, retries=int(spec.get("retries", 0)), concurrency=1)


def canonical_stage_config(
    name: str, input_mode: str, tasks: tuple[TaskDef, ...],
    output_schema: Mapping[str, JSONValue] | None, cache_version: int, base_dir: Path
) -> str:
    values = []
    for task in tasks:
        config = thaw_json(task.with_config)
        assert isinstance(config, dict)
        for key in ("prompt", "schema", "output_schema"):
            if key in config:
                file_path = (base_dir / str(config[key])).resolve()
                config[key] = str(file_path)
                config[f"{key}_digest"] = hashlib.sha256(file_path.read_bytes()).hexdigest()
        values.append({"use": task.use, "with": config})
    return canonical_json({"name": name, "input": input_mode, "cache_version": cache_version,
                           "tasks": values, "output_schema": thaw_json(output_schema)})


def load_definition(path: str | Path) -> Definition:
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, dict) or raw.get("version") != 1:
        raise DefinitionError("version: 1 is required")
    definition = _decode_definition(raw, base_dir=Path(path).parent)
    validate_definition(definition)
    return definition


def parse_interval(text: str) -> timedelta:
    match = re.fullmatch(r"([1-9][0-9]*)(m|h)", text)
    if not match:
        raise DefinitionError("duration must be a positive integer followed by m or h")
    amount, unit = int(match.group(1)), match.group(2)
    return timedelta(minutes=amount) if unit == "m" else timedelta(hours=amount)

def parse_timeout(text: str) -> timedelta:
    match = re.fullmatch(r"([1-9][0-9]*)(s|m|h)", text)
    if not match:
        raise DefinitionError("timeout must be a positive integer followed by s, m, or h")
    amount, unit = int(match.group(1)), match.group(2)
    return {"s": timedelta(seconds=amount), "m": timedelta(minutes=amount), "h": timedelta(hours=amount)}[unit]

def verify_stage_files(stage: StageDef) -> None:
    snapshot = json.loads(stage.canonical_config_json)
    for task in snapshot["tasks"]:
        config = task["with"]
        for key in ("prompt", "schema", "output_schema"):
            digest_key = f"{key}_digest"
            if key not in config:
                continue
            path = Path(config[key])
            if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != config[digest_key]:
                raise DefinitionError(f"stage file changed or is missing: {path}")
```

`validate_definition` must perform DFS cycle detection on node references, verify `source.<name>` and `<node>` edge forms, enforce YAML edge order, and validate built-in mode declarations without importing arbitrary dotted paths. It must include prompt/output-schema file digests in the Stage’s canonical configuration. `verify_stage_files(stage)` reads the normalized paths retained in `stage.canonical_config_json` and rejects a missing or digest-mismatched file before cache lookup or Task execution, so a file edit cannot execute new behavior under an old lookup key; v4 does not hot-reload it.

- [ ] **Step 4: Implement fixed callable resolution and the process-start App.**

`resolution.py` stores the fixed built-in path-to-module map and resolves every configured Source, Task, and Sink exactly once during `open_app` startup. A configured dotted path is imported, wrapped, mode-checked, and cached before `open_app` returns; an unknown or malformed callable fails startup, not a later run. If a built-in module exposes legacy `collect`, `run`, or `send`, resolution wraps it immediately in an immutable adapter object whose public method matches the approved contract; runtime code never mixes attribute-based and plain-function invocation. `open_app` binds immutable `http_client`, `llm_client`, and `sink_clients` adapters into built-in closures, opens/initializes SQLite, and stores a frozen `App` containing resolved callables. It must not reload definitions or use module globals.

`BoundaryAdapters` exists only while `open_app` resolves immutable closures. `App` is a frozen dataclass with exactly `definition`, `conn`, `sources`, `tasks`, and `sinks`; it does not retain a second mutable/raw client bundle after resolution. All configured callables are resolved before the object is returned, and a later call cannot replace them.

```python
# src/sluice/resolution.py: the only dotted-path adaptation boundary
import importlib
import inspect
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Literal

from sluice.contracts import (
    HTTPClient, JSONValue, LLMClient, SinkContract, SourceContract,
    SourceResult, TaskContract,
)
from sluice.errors import DefinitionError

@dataclass(frozen=True)
class BoundaryAdapters:
    http_client: HTTPClient | None
    llm_client: LLMClient | None
    sink_clients: Mapping[str, Callable[[Mapping[str, JSONValue], JSONValue, str], str | None]]

BUILTIN_SOURCE_BINDERS: dict[str, str] = {}
BUILTIN_TASK_BINDERS: dict[str, str] = {}
BUILTIN_SINK_BINDERS: dict[str, str] = {}

@dataclass(frozen=True)
class FunctionSource:
    function: Callable[[Mapping[str, JSONValue], JSONValue], SourceResult]
    def collect(self, config, checkpoint):
        return self.function(config, checkpoint)

@dataclass(frozen=True)
class FunctionTask:
    function: Callable[[Mapping[str, JSONValue], object], object]
    input_mode: Literal["each", "batch"]
    def run(self, config, input):
        return self.function(config, input)

@dataclass(frozen=True)
class FunctionSink:
    function: Callable[[Mapping[str, JSONValue], JSONValue, str], str | None]
    def send(self, config, rendered_payload, delivery_key):
        return self.function(config, rendered_payload, delivery_key)

def import_value(path: str) -> object:
    try:
        module_name, attribute = path.rsplit(".", 1)
        return getattr(importlib.import_module(module_name), attribute)
    except (ValueError, ImportError, AttributeError) as error:
        raise DefinitionError(f"cannot resolve {path}") from error

def bind_builtin_source(path: str, http_client: HTTPClient | None) -> SourceContract:
    try:
        binder = import_value(BUILTIN_SOURCE_BINDERS[path])
        return binder(http_client)
    except (KeyError, TypeError, AttributeError) as error:
        raise DefinitionError(f"cannot bind built-in Source {path}") from error

def bind_builtin_task(path: str, adapters: BoundaryAdapters) -> TaskContract:
    try:
        binder = import_value(BUILTIN_TASK_BINDERS[path])
        return binder(adapters)
    except (KeyError, TypeError, AttributeError) as error:
        raise DefinitionError(f"cannot bind built-in Task {path}") from error

def bind_builtin_sink(path: str, sink_clients) -> SinkContract:
    try:
        binder = import_value(BUILTIN_SINK_BINDERS[path])
        return binder(sink_clients[path])
    except (KeyError, TypeError, AttributeError) as error:
        raise DefinitionError(f"cannot bind built-in Sink {path}") from error

def require_call_shape(path: str, function: Callable[..., object], *sample_args: object) -> None:
    if not callable(function):
        raise DefinitionError(f"{path} is not callable")
    try:
        inspect.signature(function).bind(*sample_args)
    except (TypeError, ValueError) as error:
        raise DefinitionError(f"{path} has the wrong callable signature") from error

def resolve_source(path: str, adapters: BoundaryAdapters) -> SourceContract:
    value = bind_builtin_source(path, adapters.http_client) if path in BUILTIN_SOURCE_BINDERS else import_value(path)
    method = getattr(value, "collect", None)
    if callable(method):
        require_call_shape(path, method, {}, None)
        return value
    if callable(value):
        require_call_shape(path, value, {}, None)
        return FunctionSource(value)
    raise DefinitionError(f"{path} is not a Source callable")

def resolve_task(path: str, expected_mode: Literal["each", "batch"], adapters: BoundaryAdapters) -> TaskContract:
    value = bind_builtin_task(path, adapters) if path in BUILTIN_TASK_BINDERS else import_value(path)
    method = getattr(value, "run", None)
    declared_mode = getattr(value, "input_mode", None)
    if declared_mode not in {"each", "batch"}:
        raise DefinitionError(f"{path} must declare input_mode")
    if callable(method):
        require_call_shape(path, method, {}, object())
        task = value
    elif callable(value):
        require_call_shape(path, value, {}, object())
        task = FunctionTask(value, declared_mode)
    else:
        raise DefinitionError(f"{path} is not a Task callable with input_mode")
    if task.input_mode != expected_mode:
        raise DefinitionError(f"{path} declares {task.input_mode}, expected {expected_mode}")
    return task

def resolve_sink(path: str, adapters: BoundaryAdapters) -> SinkContract:
    value = bind_builtin_sink(path, adapters.sink_clients) if path in BUILTIN_SINK_BINDERS else import_value(path)
    method = getattr(value, "send", None)
    if callable(method):
        require_call_shape(path, method, {}, None, "key")
        return value
    if callable(value):
        require_call_shape(path, value, {}, None, "key")
        return FunctionSink(value)
    raise DefinitionError(f"{path} is not a Sink callable")
```

The three maps start empty in this infrastructure slice. Tasks 3, 4, 7, and 9 add only the literal built-ins they implement in that slice; future modules are not predeclared. These are fixed source constants, not discovery or a plugin registry.

`open_app` walks `Definition.sources`, every Stage Task, and every Delivery node, calls the appropriate resolver for each distinct path, wraps the three completed dictionaries in `MappingProxyType`, and returns only after all immutable mappings have been built successfully. No resolver is called from `collect_source`, Stage execution, or `deliver`.

- [ ] **Step 5: Run tests and commit the definition boundary.**

Run: `pytest -q tests/unit/test_collection_and_graph.py`

Expected: PASS, including safe parsing, missing reference, cycle, mode, batch policy, and Delivery presence failures.

```bash
git add src/sluice/contracts.py src/sluice/config.py src/sluice/resolution.py src/sluice/app.py src/sluice/errors.py tests/unit/test_collection_and_graph.py
git commit -m "feat: load and validate v4 yaml definitions"
```

### Task 3: Implement Sources, immutable Items, delta Publications, and all-or-none checkpoint finalization

**Files:**
- Create: `src/sluice/sources/__init__.py`
- Create: `src/sluice/sources/rss.py`
- Create: `src/sluice/sources/webpage.py`
- Create: `src/sluice/source_runtime.py`
- Create: `tests/integration/test_source_lifecycle.py`
- Modify: `src/sluice/resolution.py` (register Source callables)
- Modify: `src/sluice/db.py` (add the unique active-Source-claim index and expected-checkpoint lookup)
- Create: `src/sluice/logging.py`

**Interfaces:**
- `collect_source(app: App, source_name: str, at: datetime, *, trigger: str = "manual") -> int` claims a serialized Source binding and returns the successful Source Execution ID. A collection/validation failure first persists that attempt as `failed`, then raises `ExecutionError`; callers never receive a failed ID as if it succeeded.
- `rss.collect(config, checkpoint) -> SourceResult` and `webpage.collect(config, checkpoint) -> SourceResult` are wrapped at startup into immutable `SourceContract` objects; they parse RSS/Atom or fetch one page/configured list through the startup HTTP adapter.

- [ ] **Step 1: Write the first Source integration red test against YAML, a temporary SQLite file, and one inline boundary Source fake.**

```python
# tests/integration/test_source_lifecycle.py
from datetime import datetime, timezone
import yaml
import pytest
from sluice.app import open_app
from sluice.contracts import SourceRecord, SourceResult
from sluice.errors import ExecutionError
from sluice.source_runtime import collect_source

def source_collect(config, checkpoint):
    return SourceResult(
        records=(SourceRecord("a", {"url": "u", "title": "A"}),),
        next_checkpoint="cursor-2",
    )

def malformed_collect(config, checkpoint):
    return SourceResult(records=(SourceRecord("", {"bad": True}),), next_checkpoint=None)

def test_source_success_inserts_items_delta_and_checkpoint_atomically(tmp_path):
    definition = tmp_path / "definition.yml"
    definition.write_text("""
version: 1
database: state.db
sources:
  feed:
    use: tests.integration.test_source_lifecycle.source_collect
    with: {url: https://example.test/feed.xml}
""", encoding="utf-8")
    app = open_app(definition, tmp_path / "state.db")
    execution_id = collect_source(app, "feed", datetime(2026, 8, 14, tzinfo=timezone.utc))
    row = app.conn.execute("SELECT state FROM executions WHERE id=?", (execution_id,)).fetchone()
    assert row[0] == "succeeded"
    assert app.conn.execute("SELECT checkpoint_json FROM source_state").fetchone()[0] == '"cursor-2"'
    assert app.conn.execute("SELECT kind FROM publications").fetchone()[0] == "delta"
```

Add the following cases in the same file. They use only the approved Source boundary and the same production entrypoint:

```python
from sluice.contracts import KEEP_CHECKPOINT

def checkpoint_collect(config, checkpoint):
    if config["mode"] == "keep":
        return SourceResult((), KEEP_CHECKPOINT)
    return SourceResult((), None)

def raising_collect(config, checkpoint):
    raise RuntimeError("source unavailable")

def source_only_app(tmp_path, use, with_config="{}"):
    definition = tmp_path / "source.yml"
    definition.write_text(
        f"version: 1\ndatabase: state.db\nsources:\n  feed: {{use: {use}, with: {with_config}}}\n",
        encoding="utf-8",
    )
    return open_app(definition, tmp_path / "state.db")

@pytest.mark.parametrize(("mode", "stored"), [("keep", None), ("null", "null")])
def test_checkpoint_sentinel_and_explicit_json_null_are_distinct(tmp_path, mode, stored):
    app = source_only_app(
        tmp_path, "tests.integration.test_source_lifecycle.checkpoint_collect", f"{{mode: {mode}}}"
    )
    collect_source(app, "feed", datetime(2026, 8, 14, tzinfo=timezone.utc))
    assert app.conn.execute("SELECT checkpoint_json FROM source_state").fetchone()[0] == stored

def test_raised_source_error_rolls_back_outputs_and_records_failed_attempt(tmp_path):
    app = source_only_app(tmp_path, "tests.integration.test_source_lifecycle.raising_collect")
    with pytest.raises(ExecutionError):
        collect_source(app, "feed", datetime(2026, 8, 14, tzinfo=timezone.utc))
    assert app.conn.execute("SELECT COUNT(*) FROM items").fetchone()[0] == 0
    assert app.conn.execute("SELECT COUNT(*) FROM publications").fetchone()[0] == 0
    assert app.conn.execute("SELECT state FROM executions ORDER BY id DESC LIMIT 1").fetchone()[0] == "failed"

def test_identical_source_replay_reuses_item_identity(tmp_path):
    app = source_only_app(tmp_path, "tests.integration.test_source_lifecycle.source_collect")
    collect_source(app, "feed", datetime(2026, 8, 14, tzinfo=timezone.utc))
    collect_source(app, "feed", datetime(2026, 8, 14, 0, 1, tzinfo=timezone.utc))
    assert app.conn.execute("SELECT COUNT(*) FROM items").fetchone()[0] == 1
    assert app.conn.execute("SELECT COUNT(*) FROM publication_members").fetchone()[0] == 1
    assert app.conn.execute("SELECT COUNT(*) FROM executions WHERE state='succeeded'").fetchone()[0] == 2
```

A definition with no Pipeline is intentionally valid for these collection-only tests. The fixture writes ordinary YAML and uses the production dotted-path resolver; it does not create a fake runner or repository.

- [ ] **Step 2: Run the Source test to observe the missing runtime.**

Run: `pytest -q tests/integration/test_source_lifecycle.py::test_source_success_inserts_items_delta_and_checkpoint_atomically`

Expected: FAIL with `ImportError: cannot import name 'collect_source' from 'sluice.source_runtime'`.

- [ ] **Step 3: Implement RSS and webpage Sources using the production contract.**

`rss.collect` must parse RSS 2.0 and Atom title/link/content/published/update fields with `xml.etree.ElementTree`, preserve returned order, use the configured checkpoint only as an explicit feed cursor, and return `KEEP_CHECKPOINT` when the source cannot advance. `webpage.collect` must call the startup HTTP adapter with the configured URL, return one `SourceRecord` for a page, or parse a configured list selector into ordered records; no network is performed in tests. Validate that every logical ID is a non-empty string, payload is JSON-compatible, metadata is an object, and `next_checkpoint` is JSON-compatible or the identity sentinel.

This slice adds exactly `{"sluice.sources.rss": "sluice.sources.rss.bind", "sluice.sources.webpage": "sluice.sources.webpage.bind"}` to `BUILTIN_SOURCE_BINDERS` before the built-in YAML test runs.

Add one parameterized case in `test_source_lifecycle.py`: each built-in is resolved from real YAML by `open_app`, collected through `collect_source`, and verified through the durable Execution/Item/delta rows. The fake HTTP boundary is one callable with `get(url) -> str` defined in the test module; it is not a runner or repository seam.

```python
class FakeHttp:
    def __init__(self, responses):
        self.responses = responses
    def get(self, url):
        return self.responses[url]

RSS_XML = """<rss version='2.0'><channel><item><guid>a</guid><title>A</title><link>https://example.test/a</link></item></channel></rss>"""
HTML_PAGE = "<html><body><article><h1>A</h1><p>Body</p></article></body></html>"

@pytest.mark.parametrize(
    ("source_use", "url"),
    [("sluice.sources.rss", "https://example.test/feed"), ("sluice.sources.webpage", "https://example.test/page")],
)
def test_builtin_sources_finalize_ordered_records_through_the_runtime(tmp_path, source_use, url):
    definition = tmp_path / "builtin.yml"
    definition.write_text(
        f"version: 1\ndatabase: state.db\nsources:\n  feed: {{use: {source_use}, with: {{url: {url}}}}}\n",
        encoding="utf-8",
    )
    app = open_app(
        definition,
        tmp_path / "state.db",
        http_client=FakeHttp({"https://example.test/feed": RSS_XML, "https://example.test/page": HTML_PAGE}),
    )
    execution_id = collect_source(app, "feed", datetime(2026, 8, 14, tzinfo=timezone.utc))
    assert app.conn.execute("SELECT state FROM executions WHERE id=?", (execution_id,)).fetchone()[0] == "succeeded"
    assert app.conn.execute("SELECT COUNT(*) FROM items").fetchone()[0] >= 1
    assert app.conn.execute("SELECT kind FROM publications ORDER BY id DESC LIMIT 1").fetchone()[0] == "delta"
```

`FakeHttp`, `RSS_XML`, and `HTML_PAGE` are the complete boundary fixture. `sources/__init__.py` is only a package marker; Source binding remains exclusively in the startup resolver and no test-only production API is added.

- [ ] **Step 4: Implement serialized Source claim and guarded finalization in direct SQL.**

`collect_source` computes the binding digest from source name, `use`, and canonical `with`, claims `(binding, expected checkpoint)` in one `BEGIN IMMEDIATE`, advances `next_due_at` before the external call, inserts a `source` Execution with canonical config/expected checkpoint/policy, invokes the resolved callable, validates the complete result before writing anything, then finalizes in one transaction guarded by `(binding_digest, execution_id, expected_checkpoint)`: insert/reuse immutable Items, append an ordered delta Publication containing only newly inserted/changed Item versions, compare-and-swap checkpoint, mark Execution `succeeded`, and release the active claim. Records resolving to an already-retained identical Item ID do not become delta members; an empty delta may still advance a checkpoint and revision. A newly inserted Source Item stores canonical trigger `at` as its first `observed_at`; an identical replay reuses the existing Item and never updates that timestamp or `produced_by`. Any exception rolls back Item/publication/checkpoint writes, records a retryable failed Execution, and is then raised to the caller as `ExecutionError`. `KEEP_CHECKPOINT` leaves the prior JSON value unchanged; an explicit null writes SQL JSON `null`.

When a Source `use` or canonical `with` changes, activation must atomically create a new binding digest/current pointer with an empty initial delta and leave old revisions historical; downstream `source.<name>` edges read only the current pointer. In that activation transaction, any queued/running Execution referenced by the superseded `source_state.active_execution_id` is guardedly marked failed with a superseded-binding error and the old pointer is cleared; inactive bindings never spawn retries or require their old callable to remain resolvable. Emit one structured log event per claim, external completion, rollback, stale-finalization, and supersession outcome with `source`, `binding_digest`, `execution_id`, `checkpoint`, `revision`, and `state` fields; no OpenTelemetry or custom log service is introduced.

```python
# src/sluice/logging.py
import logging

from sluice.canonical import canonical_json
from sluice.contracts import JSONValue

def log_event(logger: logging.Logger, event: str, **fields: JSONValue) -> None:
    logger.info(canonical_json({"event": event, **fields}))
```

Runtime modules call only this helper and pass the identifiers they own; it writes one JSON object through standard `logging` and has no exporter, global buffer, or persistence behavior.

- [ ] **Step 5: Add Source failure/recovery cases and verify the all-or-none invariant.**

```python
def test_malformed_source_result_publishes_nothing_and_keeps_checkpoint(tmp_path):
    definition = tmp_path / "definition.yml"
    definition.write_text("""
version: 1
database: state.db
sources:
  feed: {use: tests.integration.test_source_lifecycle.malformed_collect, with: {}}
""", encoding="utf-8")
    app = open_app(definition, tmp_path / "state.db")
    with pytest.raises(ExecutionError):
        collect_source(app, "feed", datetime(2026, 8, 14, tzinfo=timezone.utc))
    assert app.conn.execute("SELECT COUNT(*) FROM items").fetchone()[0] == 0
    assert app.conn.execute("SELECT COUNT(*) FROM publications").fetchone()[0] == 0
    assert app.conn.execute("SELECT checkpoint_json FROM source_state").fetchone()[0] is None
```

Run: `pytest -q tests/integration/test_source_lifecycle.py`

Expected: PASS for success, sentinel/null, idempotent replay, malformed/raised rollback, and serialized claim cases. Add startup interruption/retry and stale-finalization cases after Task 6’s shared recovery transition is available; keep them in this same Source scenario family.

- [ ] **Step 6: Commit the Source vertical slice.**

```bash
git add src/sluice/sources src/sluice/source_runtime.py src/sluice/logging.py src/sluice/resolution.py src/sluice/db.py tests/integration/test_source_lifecycle.py
git commit -m "feat: persist source items publications and checkpoints"
```

### Task 4: Implement Task chains, collection behavior, and Stage materialization primitives

**Files:**
- Create: `src/sluice/tasks/__init__.py`
- Create: `src/sluice/tasks/collection.py`
- Create: `src/sluice/tasks/content.py`
- Create: `src/sluice/stage_runtime.py`
- Modify: `src/sluice/contracts.py` (add frozen `BatchInput`, `StageEnvelope`, and `StageOutcome` values)
- Modify: `src/sluice/canonical.py` (add only Stage lookup/derived-identity primitives)
- Modify: `src/sluice/resolution.py` (register collection/content Tasks)
- Modify: `tests/unit/test_contracts_and_keys.py`
- Modify: `tests/unit/test_collection_and_graph.py`

**Interfaces:**
- `stage_lookup_key(stage: StageDef, items: Sequence[Item], history: Sequence[Item]) -> str` hashes Stage name, cache version, canonical task/file configuration, and complete ordered Item values.
- `validate_task_outputs(input_mode, result, *, current_items, history_items)` and `run_stage_tasks(stage, items, history, tasks) -> StageOutcome` are pure in-process Task-chain primitives. They know nothing about SQLite claims, retries, Pipelines, or rebuild scope.
- `StageEnvelope(items: tuple[str, ...], history: tuple[str, ...] = ())` is the later persisted exact input reference; `BatchInput` and `StageOutcome` are the only additional contract values introduced here.
- `derived_each_logical_id(input_item: Item) -> str`, `derived_batch_logical_id(ordered_items: Sequence[Item]) -> str`, `derived_observed_at(ordered_items: Sequence[Item]) -> str`, and `materialized_item_id(stage_name: str, logical_id: str, payload: JSONValue, metadata: Mapping[str, JSONValue]) -> str` implement typed identity/time rules and are reused by retries.
- Built-in Task callables expose `.input_mode` and `run(config, input)`: `dedup`, `filter`, `sort`, `limit` (batch), `fetch_webpage`, `extract_content`, `to_markdown` (each).

- [ ] **Step 1: Add red table-driven tests for marker/cardinality validation, cache semantic equality, and collection decisions.**

```python
# tests/unit/test_contracts_and_keys.py
from dataclasses import replace
import pytest
from sluice.contracts import FORWARD, NEW, Item, StageDef, TaskDef
from sluice.canonical import stage_lookup_key
from sluice.errors import ExecutionError
from sluice.stage_runtime import validate_task_outputs

current = Item("current", ("source", "x"), "x", {"title": "A"}, {}, "h1", "2026-08-14T00:00:00Z", 1)
foreign = Item("foreign", ("source", "y"), "y", {"title": "B"}, {}, "h2", "2026-08-14T00:00:00Z", 1)
stage = StageDef(
    name="prepare", input_mode="each", tasks=(TaskDef("sluice.tasks.to_markdown", {}),),
    output_schema=None, cache_version=1, canonical_config_json='{"tasks":["sluice.tasks.to_markdown"]}',
)

def test_stage_output_markers_cardinality_and_empty_batch():
    for bad in (None, object(), FORWARD(foreign)):
        with pytest.raises(ExecutionError):
            validate_task_outputs("each", bad, current_items=(current,), history_items=())
    for bad_batch in ((NEW("a"), NEW("b")), (FORWARD(foreign),)):
        with pytest.raises(ExecutionError):
            validate_task_outputs("batch", bad_batch, current_items=(current,), history_items=(foreign,))
    assert validate_task_outputs("batch", (), current_items=(current,), history_items=()) == ()
    aggregate = validate_task_outputs("batch", (NEW([]),), current_items=(current,), history_items=())
    assert len(aggregate) == 1 and aggregate[0].value == []


def test_stage_key_changes_with_item_value():
    same = stage_lookup_key(stage, (current,), ())
    changed = stage_lookup_key(stage, (replace(current, payload={"title": "B"}),), ())
    assert same != changed
```

The remaining collection branches stay in this one compact unit file rather than becoming more integration matrices:

```python
from sluice.tasks import collection

def item(item_id, payload, metadata=None):
    return Item(item_id, ("source", "feed"), item_id, payload, metadata or {}, item_id,
                "2026-08-14T00:00:00Z", 1)

def test_item_field_prefix_cannot_be_shadowed_by_payload():
    value = item("i", {"observed_at": "payload", "$item": {"observed_at": "shadow"}})
    assert collection.field_value(value, "observed_at") == "payload"
    assert collection.field_value(value, "$item.observed_at") == "2026-08-14T00:00:00Z"
    assert collection.field_value(value, "$item.content_hash") == "i"

def test_dedup_change_and_metadata_boundaries():
    prior = item("old", {"url": "u", "content": "abcd"})
    changed = item("new", {"url": "u", "content": "abce"})
    assert collection.dedup({"keys": ["url"], "change": {"field": "content", "min_rate": 0.25}}, BatchInput((changed,), (prior,))) == (FORWARD(changed),)
    assert collection.dedup({"keys": ["url"], "change": {"field": "content", "min_rate": 0.26}}, BatchInput((changed,), (prior,))) == (DROP,)
    metadata_changed = item("meta", prior.payload, {"tag": "new"})
    assert collection.dedup({"keys": ["url"]}, BatchInput((metadata_changed,), (prior,))) == (FORWARD(metadata_changed),)

def test_filter_sort_and_limit_boundaries():
    low = item("b", {})
    explicit_null = item("n", {"score": None})
    high = item("a", {"score": 2})
    assert collection.filter({"all": [{"field": "score", "op": "exists"}]}, BatchInput((low, high), ())) == (DROP, FORWARD(high))
    assert collection.filter({"all": [{"field": "score", "op": "exists"}]}, BatchInput((explicit_null,), ())) == (FORWARD(explicit_null),)
    with pytest.raises(ExecutionError):
        collection.filter({"all": [{"field": "score", "op": "gte", "value": 1}]}, BatchInput((item("bad", {"score": "2"}),), ()))
    ordered = collection.sort({"by": [{"field": "score", "order": "asc", "nulls": "last"}]}, BatchInput((low, high), ()))
    assert ordered == (FORWARD(high), FORWARD(low))
    assert collection.limit({"count": 1}, BatchInput((high, low), ())) == (FORWARD(high),)
```

The empty-history and prior-history cases must be executable, not only described:

```python
from sluice.contracts import BatchInput, DROP, FORWARD, Item
from sluice.tasks import collection

def test_dedup_uses_only_explicit_eligible_history_in_identity_order():
    candidate = Item("new", ("source", "feed"), "n", {"url": "u", "content": "same"}, {}, "h-same", "2026-08-14T00:00:00Z", 2)
    prior = Item("old", ("stage", "select"), "n", {"url": "u", "content": "same"}, {}, "h-same", "2026-08-13T00:00:00Z", 1)
    assert collection.dedup({"keys": ["url"]}, BatchInput((candidate,), ())) == (FORWARD(candidate),)
    assert collection.dedup({"keys": ["url"]}, BatchInput((candidate,), (prior,))) == (DROP,)
    assert collection.select_dedup_history((candidate,), ((prior, ("new",)),)) == ()
    assert collection.select_dedup_history((candidate,), ((prior, ("other-input",)),)) == (prior,)
```

The Stage executor constructs the `BatchInput.history` tuple by querying only current complete reusable StageResults for this named Stage. It excludes any StageResult whose immutable input envelope contains the current candidate Item ID, then admits only matching Items whose `(observed_at, Item ID)` is strictly less than the current candidate's tuple. It orders identities by first appearance in current input, chooses the greatest eligible prior tuple per identity, and excludes outputs removed by later `filter`/`limit` because they were never published by the complete Stage. History is persisted in the Execution envelope/cache key and is visible only to the first `dedup` Task; no module-global or persisted Task “seen” set is allowed.

`select_dedup_history(candidates, accepted_rows) -> tuple[Item, ...]` is a production pure collection decision: `accepted_rows` contains Item plus immutable input IDs and is table-tested above. The Stage executor itself performs the direct SQL join through `stage_result_current`, `stage_results.producer_execution_id`, `stage_result_items`, and `execution_inputs`, then passes those rows to this helper. The SQL join is exercised only through real YAML/SQLite integration; no injectable repository or history-resolver seam is added.

- [ ] **Step 2: Run the permitted unit cases before implementing the Stage runtime.**

Run: `pytest -q tests/unit/test_contracts_and_keys.py tests/unit/test_collection_and_graph.py`

Expected: FAIL with missing `stage_lookup_key`, `validate_task_outputs`, and built-in Task imports.

- [ ] **Step 3: Implement canonical Stage envelope/key and output validation.**

The persisted envelope stores ordered Item IDs in `items` and `history`; `stage_lookup_key` resolves those IDs and hashes complete immutable values (`id`, typed producer scope, logical ID, payload, metadata, content hash, observed time, producer Execution) in order. Include prompt/schema file content digests in the Stage definition’s canonical result-affecting configuration. Exclude Pipeline name, schedule, batch policy, timeout, retries, concurrency, and executor. `validate_task_outputs(input_mode, result, *, current_items, history_items) -> tuple[FORWARD | NEW, ...]` enforces current `items` membership for `FORWARD`, rejects forwarding `history`, bare Python `None`, foreign Items, and excess outputs, accepts `NEW(None)`, JSON-null payloads, and `NEW([])`, and normalizes one `each` marker or an ordered batch-marker tuple. A batch permits any number of existing-Item forwards but at most one aggregate `NEW`; an empty tuple short-circuits remaining Tasks.

Add only the contract values used by the pure chain in this slice. They are defined once here and consumed unchanged by materialization, retry, and rebuild:

```python
# append to src/sluice/contracts.py
@dataclass(frozen=True)
class BatchInput:
    items: tuple[Item, ...]
    history: tuple[Item, ...] = ()

@dataclass(frozen=True)
class StageEnvelope:
    items: tuple[str, ...]
    history: tuple[str, ...] = ()

@dataclass(frozen=True)
class StageOutcome:
    outputs: tuple[FORWARD | NEW, ...]
    status: Literal["succeeded", "dropped"]
```

StageResult and claim persistence deliberately wait for Task 6, where the final shared-owner transaction is implemented once. Task 4 does not create an Execution, queue, retry chain, cache pointer, or test-only runtime facade.

- [ ] **Step 4: Implement deterministic collection/content Tasks without persisted Task state.**

`dedup` computes configured-key identity in the named Stage scope, chooses latest `observed_at` (Item ID tie-break), consults only `BatchInput.history`, applies optional Unicode-code-point Levenshtein `change_rate`, and returns ordered explicit markers. `filter` supports only nested `all`/`any`/`not` and `eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `in`, `not_in`, `contains`, `exists`; `exists` is true whenever the path is present, including JSON null, while missing paths are false and every other comparison treats missing/null as false. Incompatible non-null types raise `ExecutionError`. Unprefixed paths read payload only; `$item.id`, `$item.logical_id`, `$item.observed_at`, and `$item.content_hash` read immutable Item fields, even if a payload contains a `$item` key; the preceding table fixes those cases. `sort` honors ordered field/order/null-position rules and Item ID final tie-break; `limit` truncates existing order. `fetch_webpage` uses the startup HTTP adapter, `extract_content` extracts HTML text, and `to_markdown` produces deterministic text. Every resolved Task is a `TaskContract.run(config, input)` over explicit arguments.

Add only these literal entries to `BUILTIN_TASK_BINDERS`: `dedup`, `filter`, `sort`, and `limit` from `sluice.tasks.collection`, plus `fetch_webpage`, `extract_content`, and `to_markdown` from `sluice.tasks.content`. LLM and renderer paths remain absent until their owning slices.

- [ ] **Step 5: Implement the pure Task chain and deterministic derived identity/time rules.**

`run_stage_tasks` loads no database state. For `each`, it threads one current value through the declared Tasks and validates every marker. For `batch`, it gives only the first Task `BatchInput(items, history)`, threads the resulting ordered current values through later Tasks, and stops immediately when a Task returns the empty tuple. `NEW([])` remains one aggregate JSON value. Existing Items forwarded at the Stage boundary retain identity; every final non-Item value is counted as `NEW` and is materialized later by Task 6.

The derived helpers are deterministic and typed: an `each` logical ID hashes the input producer scope plus logical ID; a batch logical ID hashes ordered input Item IDs. `derived_observed_at` is the maximum original input timestamp, never execution `at`. The materialized Item ID changes when payload or metadata changes.

```python
from sluice.canonical import (
    derived_batch_logical_id, derived_each_logical_id,
    derived_observed_at, materialized_item_id,
)

def test_derived_identity_and_observed_time_depend_only_on_stage_inputs():
    source_a = Item("a", ("source", "one"), "same", {"x": 1}, {}, "h1", "2026-08-13T00:00:00Z", 1)
    source_b = Item("b", ("source", "two"), "same", {"x": 1}, {}, "h2", "2026-08-14T00:00:00Z", 1)
    assert derived_each_logical_id(source_a) != derived_each_logical_id(source_b)
    assert derived_batch_logical_id((source_a, source_b)) != derived_batch_logical_id((source_b, source_a))
    assert derived_observed_at((source_a, source_b)) == "2026-08-14T00:00:00Z"
    logical_id = derived_batch_logical_id((source_a, source_b))
    assert materialized_item_id("summary", logical_id, {"summary": "ok"}, {}) != materialized_item_id("summary", logical_id, {"summary": "changed"}, {})
```

- [ ] **Step 6: Run the pure Stage/collection unit tests.**

Run: `pytest -q tests/unit/test_contracts_and_keys.py tests/unit/test_collection_and_graph.py`

Expected: PASS for marker/cardinality, empty-batch short circuit, cache-key semantics, collection/history decisions, and deterministic derived identity/time. No Execution or cache state machine exists yet.

- [ ] **Step 7: Commit the Stage/task vertical slice.**

```bash
git add src/sluice/tasks src/sluice/stage_runtime.py src/sluice/canonical.py src/sluice/contracts.py src/sluice/resolution.py tests/unit
git commit -m "feat: add deterministic task and stage primitives"
```

### Task 5: Reconcile Pipeline publications, ordered admission, schedules, and frozen batch membership

**Files:**
- Create: `src/sluice/reconcile.py`
- Create: `tests/integration/test_shared_materialization.py` (publication admission/schedule/batch cases; Task 6 extends the same family)
- Modify: `src/sluice/canonical.py` (add the Stage/Delivery-node binding digest used by admission partitions)
- Modify: `src/sluice/db.py` (add the StageResult referential substrate plus `node_state`, `node_members`, `batches`, and `batch_items`; extend publication members for snapshots)
- Modify: `tests/unit/test_collection_and_graph.py`

**Interfaces:**
- `reconcile_node(app: App, pipeline_name: str, node_name: str, at: datetime) -> None` applies one idempotent publication-cursor/union/membership/freeze transaction and never executes external code. It is the durable admission primitive used by callbacks and tests in this slice.
- `_ensure_node_binding(app, pipeline_name, node_name, at) -> str` is a private initializer for `node_state`, edge cursors, and the empty initial snapshot. `reconcile_node` calls it idempotently; Task 6's final `reconcile`/`tick` drain paths call the same function rather than replacing it.
- The public targeted `reconcile(...)` orchestration entrypoint is introduced only in Task 6 when cache/claim/execution exists; Task 5 does not publish a temporary admission-only alias under that name.
- `freeze_eligible(policy: BatchPolicy, pending_count: int, first_pending_at: datetime | None, at: datetime, schedule_due: bool) -> bool` is the only pure reconciliation decision unit-tested here. Ordered union/duplicate fallback is covered once at the real SQLite boundary in Task 6.

- [ ] **Step 1: Write red tests for ordered union, duplicate fallback, schedule due state, and count/max-wait/on-schedule freezing.**

```python
# tests/unit/test_collection_and_graph.py
from datetime import datetime, timedelta, timezone
import pytest
from sluice.contracts import BatchPolicy
from sluice.reconcile import freeze_eligible

def instant(seconds: int) -> datetime:
    return datetime(2026, 8, 14, tzinfo=timezone.utc) + timedelta(seconds=seconds)

@pytest.mark.parametrize(
    ("policy", "count", "first", "now", "due", "expected"),
    [
        (BatchPolicy(count=2), 1, instant(0), instant(1), False, False),
        (BatchPolicy(count=2), 2, instant(0), instant(1), False, True),
        (BatchPolicy(max_wait=timedelta(hours=1)), 1, instant(0), instant(3600), False, True),
        (BatchPolicy(on_schedule=True), 99, instant(0), instant(1), False, False),
        (BatchPolicy(on_schedule=True), 99, instant(0), instant(1), True, True),
    ],
)
def test_freeze_eligibility_is_pure(policy, count, first, now, due, expected):
    assert freeze_eligible(policy, count, first, now, due) is expected

```

- [ ] **Step 2: Run the focused unit test before reconciliation exists.**

Run: `pytest -q tests/unit/test_collection_and_graph.py -k freeze`

Expected: FAIL with `ImportError: cannot import name 'freeze_eligible' from 'sluice.reconcile'`.

- [ ] **Step 3: Implement publication cursors, binding activation, and deterministic union admission.**

For a Stage node, the binding digest uses Stage identity/definition, ordered `from`, schedule, and batch policy. For a terminal Delivery node, the initializer instead uses `stage=None` plus ordered `from` and the Delivery `use`/canonical `with` binding; it never indexes `definition.stages[None]`. Timeout/retry/concurrency are excluded in both cases.

At startup, compute each fully-qualified node binding digest from Stage identity/definition, ordered `from`, schedule, and batch policy (excluding timeout/retries/concurrency), insert one immutable empty snapshot, store it in `node_state.current_publication_id`, and atomically activate `node_current_bindings`. For each declared edge, `reconcile_node` reads `node_edge_state`, applies only revisions after `last_revision` (or a changed snapshot digest), updates `node_edge_members`, and advances the cursor/digest in the same `BEGIN IMMEDIATE` transaction. When `source_current_bindings` selects a different producer binding for an existing edge, that same transaction deletes the edge's old `node_edge_members`, replaces `producer_binding_digest`, and resets `last_revision=0`/`applied_digest=NULL` before applying the new binding; old Source publications remain historical and never mix into the active contribution. The cursor row is the idempotency record; there is no second per-revision audit table. It then rebuilds the desired union by `(edge_index, upstream_ordinal)`, keeps the first Item ID, and diffs `node_members`; retaining every edge contribution allows a later duplicate to become visible if the first edge omits it. Append new generations, replace an unclaimed newer version in place for the same `(producer scope, logical_id)`, apply `include_updates` after freeze/terminal consumption, mark omitted snapshot members stale, and persist cursors/membership/ordinals/schedule changes together. Each binding activation, admission diff, freeze, and snapshot publication calls `log_event` with `pipeline`, `node`, `binding_digest`, `publication_id`, ordered `item_ids`, `batch_id`, and `state`; SQLite remains the authoritative read model.

Source edges use `source_current_bindings` to select one binding and scan its retained delta revisions through the per-edge cursor. Stage-node edges join `node_current_bindings` to the matching `node_state.current_publication_id`. Source deltas never read or update a node snapshot pointer.

For an `each` node, completion rebuilds the current output snapshot by stable current `ordinal`, never callback order. A new result is exposed only through the longest prefix whose members are terminal/stale or retain a prior `visible_result_id`; a replacement in progress keeps its prior visible result, and a later completion waits behind an earlier unresolved member. A newly inserted earlier ordinal does not retract already published outputs merely to enforce the prefix. Every completion or supersession transaction recomputes this prefix from member state plus `visible_result_id` and publishes a new snapshot only when the resulting ordered Item-ID envelope changes; no duplicate `published` flag or admission ordinal is stored.

Extend `init_schema` here with the complete node/snapshot/batch tables and indexes:

```sql
-- Installed with the first table that references a StageResult. Task 6 is the
-- first slice that writes these immutable rows or moves their current pointer.
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
  PRIMARY KEY(pipeline_name, node_name, binding_digest, item_id), FOREIGN KEY(item_id) REFERENCES items(id),
  FOREIGN KEY(visible_result_id) REFERENCES stage_results(id)
);
CREATE TABLE IF NOT EXISTS batches(
  id INTEGER PRIMARY KEY AUTOINCREMENT, pipeline_name TEXT NOT NULL, node_name TEXT NOT NULL,
  binding_digest TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('frozen','succeeded','failed')),
  frozen_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS batch_items(
  batch_id INTEGER NOT NULL, ordinal INTEGER NOT NULL, item_id TEXT NOT NULL,
  PRIMARY KEY(batch_id, ordinal), FOREIGN KEY(batch_id) REFERENCES batches(id), FOREIGN KEY(item_id) REFERENCES items(id)
);
CREATE INDEX IF NOT EXISTS node_pending ON node_members(pipeline_name, node_name, binding_digest, state, ordinal);
CREATE UNIQUE INDEX IF NOT EXISTS node_one_pending_identity
  ON node_members(pipeline_name,node_name,binding_digest,producer_kind,producer_name,logical_id)
  WHERE state='pending';
CREATE INDEX IF NOT EXISTS node_visible ON node_members(pipeline_name, node_name, binding_digest, visible_result_id);
CREATE INDEX IF NOT EXISTS node_edge_active ON node_edge_members(pipeline_name,node_name,binding_digest,edge_index,active,upstream_ordinal);
CREATE INDEX IF NOT EXISTS batch_current ON node_state(pipeline_name, node_name, current_frozen_batch_id);
CREATE INDEX IF NOT EXISTS node_current_binding ON node_current_bindings(pipeline_name,node_name,binding_digest);
```

The shared `publication_members` schema already carries the snapshot's optional result/sequence provenance, so this task needs no migration or `ALTER TABLE`. The StageResult tables are installed now only because `node_members.visible_result_id` is their first foreign-key consumer; this slice never writes or advances them. `batch_items` is the sole immutable frozen-envelope source; no JSON copy is stored on `batches`. Binding activation and every changed snapshot insert the immutable Publication/members and update `node_state.current_publication_id` in the same transaction. Old binding partitions remain historical through `node_current_bindings` plus their retained `node_state` rows.

- [ ] **Step 4: Implement schedule advancement and batch freeze as one transaction.**

Sources are due immediately on first enable; scheduled `each` nodes begin an interval when their first pending input arrives; scheduled `batch` nodes freeze at due time unless positive count/max-wait wins first. `reconcile_node(at)` uses only persisted UTC timestamps. Count/max-wait/on-schedule selects pending members in current ordinal order, replaces a newer pending version in place, allocates monotonic `batch_id`, writes immutable `batches`/`batch_items`, moves those members to `frozen`, and records `current_frozen_batch_id` atomically. The freeze transaction clears `first_pending_at` after moving the selected set; the first post-freeze arrival sets a new value, and successful clearing of the frozen batch never overwrites that newer timestamp. The node never freezes a second batch while one is frozen and leaves post-freeze arrivals pending.

This task stops at durable admission/freeze. It does not introduce a partial `tick`, Execution queue, cache-hit path, or completion callback. Task 6 composes these final transactions with the shared claim state machine in the one production scan/drain loop.

- [ ] **Step 5: Add integration cases for retained admission and durable freeze boundaries.**

```python
from datetime import datetime, timedelta, timezone
import pytest
from sluice.app import open_app
from sluice.contracts import SourceRecord, SourceResult
from sluice.reconcile import reconcile_node
from sluice.source_runtime import collect_source

def sink_callable(config, payload, delivery_key):
    return None

def instant(seconds: int) -> datetime:
    return datetime(2026, 8, 14, tzinfo=timezone.utc) + timedelta(seconds=seconds)

def reconcile_source_collect(config, checkpoint):
    if checkpoint is None:
        return SourceResult((SourceRecord("item-a", {"title": "A"}), SourceRecord("item-b", {"title": "B"})), "two-1")
    return SourceResult((SourceRecord("item-c", {"title": "C"}),), "two-2")


def test_count_freeze_keeps_exact_order_and_post_freeze_arrival_pending(tmp_path):
    definition = tmp_path / "definition.yml"
    definition.write_text("""
version: 1
database: state.db
sources: {feed: {use: tests.integration.test_shared_materialization.reconcile_source_collect, with: {}}}
stages: {summary: {input: batch, tasks: [{use: sluice.tasks.dedup, with: {keys: [title]}}]}}
pipelines:
  p:
    nodes:
      summary: {stage: summary, from: source.feed, batch: {count: 2}}
      email: {from: summary, deliver: {use: tests.integration.test_shared_materialization.sink_callable, with: {target: test@example.test}}}
""", encoding="utf-8")
    app = open_app(definition, tmp_path / "state.db")
    collect_source(app, "feed", instant(0))
    reconcile_node(app, "p", "summary", instant(0))
    batch_id = app.conn.execute(
        "SELECT current_frozen_batch_id FROM node_state WHERE pipeline_name='p' AND node_name='summary'"
    ).fetchone()[0]
    collect_source(app, "feed", instant(1))
    reconcile_node(app, "p", "summary", instant(1))
    frozen = app.conn.execute("SELECT i.logical_id FROM batch_items bi JOIN items i ON i.id=bi.item_id WHERE bi.batch_id=? ORDER BY bi.ordinal", (batch_id,)).fetchall()
    pending = app.conn.execute(
        "SELECT i.logical_id FROM node_members nm JOIN items i ON i.id=nm.item_id WHERE nm.pipeline_name='p' AND nm.node_name='summary' AND nm.state='pending' ORDER BY nm.ordinal"
    ).fetchall()
    assert [value[0] for value in frozen] == ["item-a", "item-b"]
    assert [value[0] for value in pending] == ["item-c"]
```

Add these cases to the same integration file; they exercise production `reconcile_node(at)` and never sleep:

```python
def one_item_source(config, checkpoint):
    return SourceResult((SourceRecord("only", {"title": "Only"}),), "done")

@pytest.mark.parametrize("node_policy", [
    "batch: {max_wait: 1h}",
    "schedule: 1h\n        batch: {on_schedule: true}",
])
def test_max_wait_and_on_schedule_freeze_only_at_persisted_due_time(tmp_path, node_policy):
    definition = tmp_path / "due.yml"
    definition.write_text(f"""
version: 1
database: state.db
sources: {{feed: {{use: tests.integration.test_shared_materialization.one_item_source, with: {{}}}}}}
stages: {{summary: {{input: batch, tasks: [{{use: sluice.tasks.dedup, with: {{keys: [title]}}}}]}}}}
pipelines:
  p:
    nodes:
      summary:
        stage: summary
        from: source.feed
        {node_policy}
      email: {{from: summary, deliver: {{use: tests.integration.test_shared_materialization.sink_callable, with: {{}}}}}}
""", encoding="utf-8")
    app = open_app(definition, tmp_path / "state.db")
    collect_source(app, "feed", instant(0))
    reconcile_node(app, "p", "summary", instant(0))
    reconcile_node(app, "p", "summary", instant(3599))
    assert app.conn.execute("SELECT COUNT(*) FROM batches").fetchone()[0] == 0
    reconcile_node(app, "p", "summary", instant(3600))
    assert app.conn.execute("SELECT COUNT(*) FROM batches").fetchone()[0] == 1

```

The second max-wait deadline, independent Source/Stage schedules, and binding replacement are exercised once in Task 6 after the final `tick`/claim loop exists. They are not duplicated as a provisional scheduler or a separate binding-only integration here.

Run: `pytest -q tests/integration/test_shared_materialization.py`

Expected: PASS for applying retained Source publications into count-frozen membership with post-freeze arrivals retained, and max-wait/on-schedule eligibility at the persisted deadline. No external Task is executed in this slice; Task 6 gates lost-callback repair through the final `tick` loop.

- [ ] **Step 6: Commit the reconciliation/batch admission slice.**

```bash
git add src/sluice/reconcile.py src/sluice/canonical.py src/sluice/db.py tests/unit/test_collection_and_graph.py tests/integration/test_shared_materialization.py
git commit -m "feat: reconcile publications and freeze deterministic batches"
```

### Task 6: Complete shared materialization ownership, guarded retries, and crash recovery

**Files:**
- Modify: `tests/integration/test_shared_materialization.py` (cache/owner/waiter/restart/order cases)
- Modify: `tests/integration/test_source_lifecycle.py` (append interrupted-Source recovery/stale-finalization case)
- Modify: `src/sluice/stage_runtime.py`
- Modify: `src/sluice/source_runtime.py`
- Modify: `src/sluice/reconcile.py`
- Modify: `src/sluice/app.py` (run recovery exactly once during startup)
- Modify: `src/sluice/db.py` (add materialization claim/waiter tables and StageResult indexes to `init_schema`)

**Interfaces:**
- `tick(app: App, at: datetime) -> None` is the single callback-independent scan/drain loop. It reconciles durable admissions, requests materialization, starts queued owners subject to node concurrency, executes persisted attempts, processes timeouts/retries, and repeats to quiescence.
- `reconcile(app: App, pipeline_name: str, node_name: str, at: datetime) -> None` is the public targeted drain: it computes that fully-qualified node plus its downstream descendants in YAML DAG order, invokes `reconcile_node`, and reuses the same private claim/execute/heal loop as `tick` until the closure has no immediately eligible work. It is not an alias for admission and does not duplicate the state machine.
- `execute_stage(app: App, stage_name: str, envelope: StageEnvelope, *, pipeline_name: str, node_name: str, generation: int | None, batch_id: int | None, at: datetime) -> int` is the public synchronous node entrypoint. It derives policy/binding from the frozen node definition, persists `_StageRequest`, and returns the final terminal attempt ID. It never accepts policy, retained YAML, force flags, or rebuild scope as call-time parameters; the private `_execute_claimed_stage(app, execution_id, at)` runs only the stored snapshot.
- `recover_startup(app: App, at: datetime) -> None` is the one startup recovery entrypoint. It dispatches private Source/Stage recovery transitions here; Task 9 adds a private Delivery branch without creating a second recovery API.
- `_StageRequest`, `_RequestOutcome`, `_request_materialization`, `_start_claimed_execution`, `_finalize_stage`, `_finalize_source`, and `_fail_execution` are private state-machine helpers. Integration tests drive them through `tick`, `execute_stage`, `recover_startup`, and explicit Source/HTTP/LLM boundary barriers rather than treating these transitions as a stable test API.
- `resolve_dedup_history_ids(app, stage, candidate_ids)` performs the fixed SQL join through current StageResult/input rows only for a fresh normal request. The resulting IDs are persisted in `execution_inputs`; retry/cache/rebuild never reselect history.

The private request value is exact rather than a ten-argument public function:

```python
from dataclasses import dataclass

from sluice.contracts import ExecutionPolicy, StageDef, StageEnvelope

@dataclass(frozen=True, slots=True)
class _StageRequest:
    stage_name: str
    envelope: StageEnvelope
    owner: tuple[str, str] | None
    generation: int | None
    batch_id: int | None
    policy: ExecutionPolicy
    binding_digest: str
    force: bool = False
    retained_stage: StageDef | None = None
    rebuild_target_id: int | None = None
```

The immutable StageResult tables were installed beside their first `node_members` foreign key in Task 5 but remain empty there. Add only the shared-claim tables and the indexes first used by materialization here:

```sql
CREATE TABLE IF NOT EXISTS materialization_claims(
  stage_name TEXT NOT NULL, lookup_key TEXT NOT NULL, claim_id INTEGER PRIMARY KEY AUTOINCREMENT,
  owner_execution_id INTEGER, next_waiting_sequence INTEGER NOT NULL DEFAULT 1,
  UNIQUE(stage_name, lookup_key), FOREIGN KEY(owner_execution_id) REFERENCES executions(id)
);
CREATE TABLE IF NOT EXISTS materialization_waiters(
  stage_name TEXT NOT NULL, lookup_key TEXT NOT NULL, claim_id INTEGER NOT NULL,
  waiting_sequence INTEGER NOT NULL, pipeline_name TEXT, node_name TEXT, force INTEGER NOT NULL,
  generation INTEGER, batch_id INTEGER, envelope_json TEXT NOT NULL, policy_json TEXT NOT NULL,
  config_json TEXT NOT NULL, binding_digest TEXT,
  rebuild_target_id INTEGER,
  PRIMARY KEY(stage_name, lookup_key, waiting_sequence), FOREIGN KEY(claim_id) REFERENCES materialization_claims(claim_id)
);
CREATE INDEX IF NOT EXISTS materialization_owner ON materialization_claims(stage_name,lookup_key,owner_execution_id);
CREATE INDEX IF NOT EXISTS materialization_waiting ON materialization_waiters(stage_name,lookup_key,force,waiting_sequence);
CREATE INDEX IF NOT EXISTS stage_result_lookup ON stage_results(stage_name,lookup_key,sequence);
CREATE INDEX IF NOT EXISTS stage_result_producer ON stage_results(producer_execution_id);
```

- [ ] **Step 1: Write red integration tests for owner/waiter recovery, stale completion, and prefix ordering.**

```python
from datetime import datetime, timedelta, timezone
from threading import Event, Lock, Thread
from sluice.app import open_app
from sluice.contracts import SourceRecord, SourceResult, StageEnvelope
from sluice.reconcile import reconcile, reconcile_node, recover_startup, tick
from sluice.source_runtime import collect_source
from sluice.stage_runtime import execute_stage

def instant(seconds: int) -> datetime:
    return datetime(2026, 8, 14, tzinfo=timezone.utc) + timedelta(seconds=seconds)

def source_collect(config, checkpoint):
    count = int(config.get("count", 1))
    return SourceResult(tuple(
        SourceRecord(f"i-{index}", {"url": f"https://example.test/{index}"})
        for index in range(count)
    ), "done")

def first_binding_source(config, checkpoint):
    return SourceResult((SourceRecord("old", {"title": "Old"}),), "old-done")

def second_binding_source(config, checkpoint):
    return SourceResult((SourceRecord("new", {"title": "New"}),), "new-done")

def write_binding_definition(path, source_path):
    path.write_text("""
version: 1
database: state.db
sources: {feed: {use: __SOURCE__, with: {}}}
stages: {documents: {input: each, tasks: [{use: sluice.tasks.to_markdown}]}}
pipelines:
  p:
    nodes:
      documents: {stage: documents, from: source.feed}
      email: {from: documents, deliver: {use: tests.integration.test_shared_materialization.sink_callable, with: {}}}
""".replace("__SOURCE__", source_path), encoding="utf-8")

def test_tick_repairs_missed_callback_and_source_binding_switch_resets_edge(tmp_path):
    definition = tmp_path / "binding.yml"
    database = tmp_path / "state.db"
    write_binding_definition(definition, "tests.integration.test_shared_materialization.first_binding_source")
    first = open_app(definition, database)
    collect_source(first, "feed", instant(0))  # no callback/reconcile follows
    first.conn.close()

    replay = open_app(definition, database)
    tick(replay, instant(1))
    assert replay.conn.execute("SELECT COUNT(*) FROM stage_results WHERE stage_name='documents'").fetchone()[0] == 1
    replay.conn.close()

    write_binding_definition(definition, "tests.integration.test_shared_materialization.second_binding_source")
    changed = open_app(definition, database)
    collect_source(changed, "feed", instant(2))  # again omit the completion callback
    reconcile(changed, "p", "documents", instant(2))
    active = changed.conn.execute(
        "SELECT i.logical_id FROM node_members nm JOIN items i ON i.id=nm.item_id "
        "WHERE nm.pipeline_name='p' AND nm.node_name='documents' AND nm.state!='stale' ORDER BY nm.ordinal"
    ).fetchall()
    assert [row[0] for row in active] == ["new"]

class BarrierHttp:
    def __init__(self, blocked_urls):
        self.started = {url: Event() for url in blocked_urls}
        self.release = {url: Event() for url in blocked_urls}
        self._first_calls = set(blocked_urls)
        self._lock = Lock()
    def get(self, url):
        with self._lock:
            blocks = url in self._first_calls
            self._first_calls.discard(url)
        if blocks:
            self.started[url].set()
            if not self.release[url].wait(timeout=5):
                raise RuntimeError("test barrier was not released")
        return f"<html><body><article>{url}</article></body></html>"

# append this boundary barrier and test to tests/integration/test_source_lifecycle.py
from datetime import datetime, timedelta, timezone
from threading import Event, Thread

from sluice.app import open_app
from sluice.contracts import SourceRecord, SourceResult
from sluice.reconcile import recover_startup
from sluice.source_runtime import collect_source

def source_recovery_instant(seconds: int) -> datetime:
    return datetime(2026, 8, 14, tzinfo=timezone.utc) + timedelta(seconds=seconds)

class BlockingSource:
    def __init__(self):
        self.started = Event()
        self.release = Event()
    def collect(self, config, checkpoint):
        self.started.set()
        if not self.release.wait(timeout=5):
            raise RuntimeError("test barrier was not released")
        return SourceResult((SourceRecord("late", {"title": "Late"}),), "done")

blocking_source = BlockingSource()

def test_startup_recovery_transfers_source_claim_and_rejects_late_finalize(tmp_path):
    definition = tmp_path / "source-recovery.yml"
    definition.write_text("""
version: 1
database: state.db
sources: {feed: {use: tests.integration.test_source_lifecycle.blocking_source, retries: 1, with: {}}}
""", encoding="utf-8")
    app = open_app(definition, tmp_path / "state.db")
    worker = Thread(target=collect_source, args=(app, "feed", source_recovery_instant(0)))
    worker.start()
    assert blocking_source.started.wait(timeout=5)
    old_id = app.conn.execute(
        "SELECT id FROM executions WHERE kind='source' AND state='running'"
    ).fetchone()[0]
    recover_startup(app, source_recovery_instant(1))
    blocking_source.release.set()
    worker.join(timeout=5)
    assert not worker.is_alive()
    assert app.conn.execute("SELECT state FROM executions WHERE id=?", (old_id,)).fetchone()[0] == "failed"
    assert app.conn.execute("SELECT COUNT(*) FROM publications").fetchone()[0] == 0
    assert app.conn.execute(
        "SELECT COUNT(*) FROM executions WHERE retry_parent_id=? AND state='queued'", (old_id,)
    ).fetchone()[0] == 1

def shared_app(tmp_path, http_client, *, count=1, concurrency=1):
    definition = tmp_path / "definition.yml"
    definition.write_text(f"""
version: 1
database: state.db
sources: {{feed: {{use: tests.integration.test_shared_materialization.source_collect, with: {{count: {count}}}}}}}
stages: {{prepare: {{input: each, tasks: [{{use: sluice.tasks.fetch_webpage, with: {{url_field: url}}}}]}}}}
pipelines:
  p1:
    nodes:
      prepare: {{stage: prepare, from: source.feed, timeout: 1s, retries: 1, concurrency: {concurrency}}}
      email: {{from: prepare, deliver: {{use: tests.integration.test_shared_materialization.sink_callable, with: {{target: one@example.test}}}}}}
  p2:
    nodes:
      prepare: {{stage: prepare, from: source.feed, timeout: 1s, retries: 1, concurrency: {concurrency}}}
      email: {{from: prepare, deliver: {{use: tests.integration.test_shared_materialization.sink_callable, with: {{target: two@example.test}}}}}}
""", encoding="utf-8")
    app = open_app(definition, tmp_path / "state.db", http_client=http_client)
    collect_source(app, "feed", instant(0))
    return app

def test_shared_owner_waiter_survives_restart_and_waiter_uses_cached_result(tmp_path):
    url = "https://example.test/0"
    http = BarrierHttp((url,))
    app = shared_app(tmp_path, http)
    worker = Thread(target=tick, args=(app, instant(0)))
    worker.start()
    assert http.started[url].wait(timeout=5)
    old_id = app.conn.execute("SELECT owner_execution_id FROM materialization_claims").fetchone()[0]
    assert app.conn.execute("SELECT COUNT(*) FROM materialization_waiters").fetchone()[0] == 1
    recover_startup(app, instant(1))
    http.release[url].set()
    worker.join(timeout=5)
    assert not worker.is_alive()
    tick(app, instant(2))
    assert app.conn.execute("SELECT state FROM executions WHERE id=?", (old_id,)).fetchone()[0] == "failed"
    assert app.conn.execute(
        "SELECT state FROM executions WHERE pipeline_name='p2' AND node_name='prepare' ORDER BY id DESC LIMIT 1"
    ).fetchone()[0] == "cached"
    assert app.conn.execute("SELECT COUNT(*) FROM stage_results WHERE stage_name='prepare'").fetchone()[0] == 1


def test_late_completion_after_timeout_cannot_publish(tmp_path):
    url = "https://example.test/0"
    http = BarrierHttp((url,))
    app = shared_app(tmp_path, http)
    worker = Thread(target=tick, args=(app, instant(0)))
    worker.start()
    assert http.started[url].wait(timeout=5)
    old_id = app.conn.execute("SELECT owner_execution_id FROM materialization_claims").fetchone()[0]
    tick(app, instant(1))
    http.release[url].set()
    worker.join(timeout=5)
    assert not worker.is_alive()
    assert app.conn.execute(
        "SELECT COUNT(*) FROM stage_results WHERE producer_execution_id=?", (old_id,)
    ).fetchone()[0] == 0
    assert app.conn.execute("SELECT COUNT(*) FROM executions WHERE retry_parent_id=?", (old_id,)).fetchone()[0] == 1


def test_each_completion_publishes_only_longest_resolved_prefix(tmp_path):
    first_url, second_url = "https://example.test/0", "https://example.test/1"
    http = BarrierHttp((first_url, second_url))
    app = shared_app(tmp_path, http, count=2, concurrency=2)
    reconcile_node(app, "p1", "prepare", instant(0))
    members = app.conn.execute(
        "SELECT item_id,generation FROM node_members WHERE pipeline_name='p1' AND node_name='prepare' ORDER BY ordinal"
    ).fetchall()
    workers = [Thread(
        target=execute_stage,
        args=(app, "prepare", StageEnvelope((item_id,))),
        kwargs={"pipeline_name": "p1", "node_name": "prepare", "generation": generation,
                "batch_id": None, "at": instant(0)},
    ) for item_id, generation in members]
    for worker in workers:
        worker.start()
    assert http.started[first_url].wait(timeout=5)
    assert http.started[second_url].wait(timeout=5)
    http.release[second_url].set()
    workers[1].join(timeout=5)
    assert not workers[1].is_alive()
    current = app.conn.execute(
        "SELECT ns.current_publication_id FROM node_current_bindings nb JOIN node_state ns "
        "ON ns.pipeline_name=nb.pipeline_name AND ns.node_name=nb.node_name AND ns.binding_digest=nb.binding_digest "
        "WHERE nb.pipeline_name='p1' AND nb.node_name='prepare'"
    ).fetchone()[0]
    assert app.conn.execute("SELECT COUNT(*) FROM publication_members WHERE publication_id=?", (current,)).fetchone()[0] == 0
    http.release[first_url].set()
    workers[0].join(timeout=5)
    assert not workers[0].is_alive()
    logical_ids = [row[0] for row in app.conn.execute(
        "SELECT i.logical_id FROM node_current_bindings nb JOIN node_state ns "
        "ON ns.pipeline_name=nb.pipeline_name AND ns.node_name=nb.node_name AND ns.binding_digest=nb.binding_digest "
        "JOIN publication_members pm ON pm.publication_id=ns.current_publication_id "
        "JOIN items i ON i.id=pm.item_id WHERE nb.pipeline_name='p1' AND nb.node_name='prepare' ORDER BY pm.ordinal"
    )]
    assert len(logical_ids) == 2


def ordered_source(config, checkpoint):
    return SourceResult(
        (SourceRecord("a", {"title": "A"}), SourceRecord("b", {"title": "B"})), "done"
    )

def write_union_definition(path, left_stage):
    path.write_text(f"""
version: 1
database: state.db
sources: {{feed: {{use: tests.integration.test_shared_materialization.ordered_source, with: {{}}}}}}
stages:
  asc: {{input: batch, tasks: [{{use: sluice.tasks.sort, with: {{by: [{{field: title, order: asc}}]}}}}]}}
  desc: {{input: batch, tasks: [{{use: sluice.tasks.sort, with: {{by: [{{field: title, order: desc}}]}}}}]}}
  empty: {{input: batch, tasks: [{{use: sluice.tasks.filter, with: {{all: [{{field: absent, op: exists}}]}}}}]}}
  documents: {{input: each, tasks: [{{use: sluice.tasks.to_markdown}}]}}
pipelines:
  p:
    nodes:
      left: {{stage: {left_stage}, from: source.feed, batch: {{count: 2}}}}
      right: {{stage: desc, from: source.feed, batch: {{count: 2}}}}
      documents: {{stage: documents, from: [left, right]}}
      email: {{from: documents, deliver: {{use: tests.integration.test_shared_materialization.sink_callable, with: {{}}}}}}
""", encoding="utf-8")

def test_reorder_duplicate_fallback_and_snapshot_omission_preserve_current_union(tmp_path):
    definition = tmp_path / "union.yml"
    write_union_definition(definition, "asc")
    first = open_app(definition, tmp_path / "state.db")
    tick(first, instant(0))
    assert [row[0] for row in first.conn.execute(
        "SELECT i.logical_id FROM node_members nm JOIN items i ON i.id=nm.item_id "
        "WHERE nm.pipeline_name='p' AND nm.node_name='documents' AND nm.state!='stale' ORDER BY nm.ordinal"
    )] == ["a", "b"]
    right_results = first.conn.execute("SELECT COUNT(*) FROM stage_results WHERE stage_name='desc'").fetchone()[0]
    first.conn.close()

    write_union_definition(definition, "empty")
    second = open_app(definition, tmp_path / "state.db")
    tick(second, instant(1))
    assert [row[0] for row in second.conn.execute(
        "SELECT i.logical_id FROM node_members nm JOIN items i ON i.id=nm.item_id "
        "WHERE nm.pipeline_name='p' AND nm.node_name='documents' AND nm.state!='stale' ORDER BY nm.ordinal"
    )] == ["b", "a"]
    empty_publication = second.conn.execute(
        "SELECT ns.current_publication_id FROM node_current_bindings nb JOIN node_state ns "
        "ON ns.pipeline_name=nb.pipeline_name AND ns.node_name=nb.node_name AND ns.binding_digest=nb.binding_digest "
        "WHERE nb.pipeline_name='p' AND nb.node_name='left'"
    ).fetchone()[0]
    assert second.conn.execute(
        "SELECT COUNT(*) FROM publication_members WHERE publication_id=?", (empty_publication,)
    ).fetchone()[0] == 0
    assert second.conn.execute("SELECT COUNT(*) FROM stage_results WHERE stage_name='desc'").fetchone()[0] == right_results

def sequential_source(config, checkpoint):
    ordinal = 1 if checkpoint is None else 2
    return SourceResult((SourceRecord(f"item-{ordinal}", {"title": f"Item {ordinal}"}),), str(ordinal))

def test_tick_resets_max_wait_from_first_post_freeze_arrival(tmp_path):
    definition = tmp_path / "two-waits.yml"
    definition.write_text("""
version: 1
database: state.db
sources: {feed: {use: tests.integration.test_shared_materialization.sequential_source, with: {}}}
stages: {summary: {input: batch, tasks: [{use: sluice.tasks.dedup, with: {keys: [title]}}]}}
pipelines:
  p:
    nodes:
      summary: {stage: summary, from: source.feed, batch: {max_wait: 1h}}
      email: {from: summary, deliver: {use: tests.integration.test_shared_materialization.sink_callable, with: {}}}
""", encoding="utf-8")
    app = open_app(definition, tmp_path / "state.db")
    collect_source(app, "feed", instant(0)); reconcile_node(app, "p", "summary", instant(0))
    tick(app, instant(3600))
    collect_source(app, "feed", instant(3700)); reconcile_node(app, "p", "summary", instant(3700))
    tick(app, instant(7299))
    assert app.conn.execute("SELECT COUNT(*) FROM batches").fetchone()[0] == 1
    tick(app, instant(7300))
    assert app.conn.execute("SELECT COUNT(*) FROM batches").fetchone()[0] == 2

def test_tick_advances_source_and_stage_intervals_independently(tmp_path):
    definition = tmp_path / "cadence.yml"
    definition.write_text("""
version: 1
database: state.db
sources:
  feed: {use: tests.integration.test_shared_materialization.one_item_source, schedule: 10m, with: {}}
stages: {documents: {input: each, tasks: [{use: sluice.tasks.to_markdown}]}}
pipelines:
  p:
    nodes:
      documents: {stage: documents, from: source.feed, schedule: 1h}
      email: {from: documents, deliver: {use: tests.integration.test_shared_materialization.sink_callable, with: {}}}
""", encoding="utf-8")
    app = open_app(definition, tmp_path / "state.db")
    tick(app, instant(0)); tick(app, instant(600))
    assert app.conn.execute("SELECT COUNT(*) FROM executions WHERE kind='source'").fetchone()[0] == 2
    assert app.conn.execute("SELECT COUNT(*) FROM executions WHERE kind='stage'").fetchone()[0] == 0
    tick(app, instant(3600))
    assert app.conn.execute("SELECT COUNT(*) FROM executions WHERE kind='stage'").fetchone()[0] >= 1
```

- [ ] **Step 2: Run the shared-materialization tests before implementing transfer/recovery.**

Run: `pytest -q tests/integration/test_source_lifecycle.py tests/integration/test_shared_materialization.py`

Expected: FAIL in the newly added owner/waiter restart, timeout guard, resolved-prefix, schedule-drain, and cache-repair cases because the final claim/tick transitions are not yet defined.

- [ ] **Step 3: Implement claim/waiter transactions and owner policy.**

Use a local `ThreadPoolExecutor` only to run already-claimed callable attempts up to each fully-qualified node's persisted concurrency; it is an implementation detail, not a RunnerBackend interface. `tick` first reconciles and claims all currently eligible nodes, then submits queued owners, so a blocked owner cannot prevent another Pipeline from durably becoming its waiter. Database claim/finalize work remains serialized by `write_transaction`, futures finalize independently, and output snapshots use current member ordinal rather than completion order.

In one SQLite transaction, cache lookup and claim creation must either attach a reusable result, create a stable `claim_id`/owner Execution, or insert a waiter with monotonic `waiting_sequence`, exact envelope, generation/frozen batch ID, original node binding digest, policy, canonical Stage configuration, and optional Rebuild target ID. `cache_version` is decoded from that canonical configuration and is not duplicated in a SQL column. Waiters are `waiting` for health and consume no attempt budget/concurrency. A forced rebuild row is never satisfied by a cache hit and is promoted before ordinary waiters after owner exhaustion; Task 8 gates the public forced path without adding a second claim implementation or concurrency matrix. Owner retry updates only `owner_execution_id`; existing waiters continue to identify the stable claim.

The transaction uses `stage_result_current` first, then `INSERT ... ON CONFLICT(stage_name,lookup_key) DO NOTHING` for the claim. The caller that inserted the claim creates the queued owner Execution and sets `owner_execution_id`; a caller that lost the insert appends one waiter using the claim row's `next_waiting_sequence` and increments that counter. Promotion orders `force DESC, waiting_sequence ASC`; before selecting a normal waiter it verifies that its recorded node binding/generation or batch ID is still current and deletes stale normal rows, while an explicit forced historical row remains eligible but is scope-excluded. It copies the selected waiter's `config_json`, binding digest, policy, envelope, and `rebuild_target_id` into the new queued Execution before deleting exactly that waiter; promoted work never reconstructs these fields from current YAML. A queued owner retains the claim while waiting for its fully qualified node's persisted concurrency slot.

For a fresh normal request whose first Task is `dedup`, `resolve_dedup_history_ids` selects history before computing the key and persists the resolved IDs in `execution_inputs`. `_request_materialization` then records one complete immutable request snapshot. `_execute_claimed_stage(execution_id, at)` decodes that snapshot, verifies retained files, loads ordered Item/history rows, calls Task 4's `run_stage_tasks`, and finalizes through the guarded transaction. Cache hits create a node-local `cached` Execution pointing to the current StageResult and invoke no Task. Retry and rebuild reuse recorded history rather than running the selector again.

- [ ] **Step 4: Implement guarded Stage/Source finalization and failure transitions.**

Stage success/dropped finalization must check `(stage_name, lookup key, execution_id)` and `state='running'`, insert/reuse Items and a StageResult, move the global current pointer, mark terminal state, transfer/release claim, and rebuild the owner node snapshot only if its original binding partition and generation are still active or its frozen batch ID is still current. The Stage branch of `_fail_execution` guards old `queued`/`running` state, preserves exact config/envelope/frozen batch, original binding digest, and `rebuild_target_id`, transfers claim without a release window, and leaves a failed batch frozen when retries exhaust. Its Source branch guards `source_state.active_execution_id` plus expected checkpoint and first compares the Execution binding with `source_current_bindings`: only the current binding creates a queued retry with identical config/policy/checkpoint and atomically replaces `active_execution_id`; an inactive historical binding is terminally failed, clears its old pointer, and never retries. Current-binding retry preserves the already-advanced `next_due_at` and never touches materialization claims. `_finalize_source` guards binding, Execution ID, and expected checkpoint while atomically inserting Items, delta publication, checkpoint, and success.

Every success/failure transaction begins with a conditional state update (`... WHERE id=? AND state='running'` for completion; `state IN ('queued','running')` for failure) and proceeds only when `rowcount == 1`. The same transaction inserts the immutable result, upserts `stage_result_current`, updates the Execution, and transfers the stable claim. A lost guard rolls back all proposed Items/results/publications and returns `False`; it never publishes a late callback.

- [ ] **Step 5: Implement startup recovery and callback-independent scans.**

Task 6 modifies `open_app` so that, after schema and binding initialization and before returning the App, it invokes `recover_startup` exactly once; recovery is never rerun on every tick. The barrier test's direct call simulates that production startup phase on an already-open connection. Interrupted Source/Stage attempts use the same `_fail_execution` transaction as timeout failure and pending/frozen inputs remain intact. Delivery recovery is deliberately absent until Task 9 creates the Delivery table/token contract. A private `_drain(app, at, targets)` implements the sole scan/claim/execute loop: `tick` passes all enabled nodes, while `reconcile` passes the selected target's transitive downstream closure. It scans running Source/Stage attempts whose persisted `started_at + timeout_seconds <= at`, queued owners with capacity, waiters whose stable claim has no live owner, queued retries, due Source/node schedules, count/max-wait batches, and members whose recorded result sequence trails `stage_result_current`. Stage finalization immediately makes changed snapshots eligible within that same closure, so one targeted reconcile materializes the target and advances its downstream nodes/Deliveries without repairing unrelated branches. It applies only durable transitions at or before `at`, executes outside SQLite transactions, and repeats until no transition remains; deliberately omitted callbacks are therefore repaired from durable state.

Claim, promotion, retry, guarded-finalization, and recovery paths call `log_event` with `pipeline`, `node`, `stage`, `execution_id`, `claim_id`, ordered `item_ids`, `result_id`, and `state`; Source recovery uses the Source field set from Task 3.

- [ ] **Step 6: Verify all four shared-stage recovery/order cases and commit.**

Run: `pytest -q tests/integration/test_source_lifecycle.py tests/integration/test_shared_materialization.py`

Expected: PASS for Source startup retry/stale finalization, Stage owner transfer through startup recovery, one shared owner with a cached waiter, timeout rejection of the old owner, concurrent `each` completion exposing only the longest resolved prefix, independent schedules, second max-wait reset, and the integrated binding-change/reorder/duplicate-fallback/snapshot-omission case without rerunning the unaffected Stage. Forced-before-normal promotion is gated once through public `rebuild_stage` in Task 8 rather than a private request helper here.

```bash
git add src/sluice/stage_runtime.py src/sluice/source_runtime.py src/sluice/reconcile.py src/sluice/app.py src/sluice/db.py tests/integration/test_source_lifecycle.py tests/integration/test_shared_materialization.py
git commit -m "feat: add shared materialization claims retries and recovery"
```

### Task 7: Add LLM extraction/summary validation, batch retry/drop semantics, and exact frozen-envelope retries

**Files:**
- Create: `src/sluice/tasks/llm.py`
- Create: `tests/integration/test_llm_batch_rebuild.py`
- Modify: `src/sluice/errors.py` (add the internal `InvalidStructuredOutput` retry classification)
- Modify: `src/sluice/resolution.py`
- Modify: `src/sluice/stage_runtime.py`

**Interfaces:**
- `llm_extract(config: Mapping[str, JSONValue], input: Item) -> NEW | DROP` and `llm_summary(config, input: BatchInput | Sequence[Item]) -> tuple[NEW, ...]` are `TaskContract.run` values after resolution; summary returns exactly one `NEW` marker in the batch sequence after successful validation.
- `validate_structured(value: JSONValue, schema: Mapping[str, JSONValue]) -> JSONValue` supports the initial `string`, `list[string]`, and `object` shape used by the YAML example and raises this slice's internal `InvalidStructuredOutput` on mismatch.
- The `LLMClient` protocol is the startup boundary already owned by Task 2. `bind_llm(llm_client) -> TaskContract` captures it in an immutable wrapper while preserving `run(config, input)`; no second protocol/class identity or module-global client is introduced.

Task 2 already reserves `invalid: drop` only for `sluice.tasks.llm_extract` in an `each` Stage and captures prompt/schema digests. This slice consumes that validated configuration; it does not add a second loader policy.

- [ ] **Step 1: Write red integration tests for per-item drop, invalid batch failure, and frozen exact retry.**

```python
from datetime import datetime, timedelta, timezone
import json
import pytest
from sluice.app import open_app
from sluice.contracts import SourceRecord, SourceResult, StageEnvelope
from sluice.errors import DefinitionError, ExecutionError
from sluice.reconcile import reconcile_node, tick
from sluice.source_runtime import collect_source
from sluice.stage_runtime import execute_stage

class InvalidLLM:
    def __init__(self, responses):
        self.responses = list(responses)
    def complete(self, model, prompt, input_payload):
        return self.responses.pop(0)

def source_collect(config, checkpoint):
    if checkpoint is None:
        return SourceResult((SourceRecord("item-a", {"content": "A"}), SourceRecord("item-b", {"content": "B"})), "done")
    return SourceResult((SourceRecord("item-c", {"content": "C"}),), "done-2")

def sink_callable(config, payload, delivery_key):
    return None

def instant(seconds: int) -> datetime:
    return datetime(2026, 8, 14, tzinfo=timezone.utc) + timedelta(seconds=seconds)

def llm_app(tmp_path, responses):
    definition = tmp_path / "definition.yml"
    definition.write_text("""
version: 1
database: state.db
sources: {feed: {use: tests.integration.test_llm_batch_rebuild.source_collect, with: {}}}
stages:
  extract:
    input: each
    tasks:
      - use: sluice.tasks.llm_extract
        with: {model: test, prompt: prompt.md, output: {title: string}, invalid: drop}
  summary:
    input: batch
    tasks: [{use: sluice.tasks.llm_summary, with: {model: test, prompt: prompt.md, output: {summary: string}}}]
pipelines:
  p:
    nodes:
      extract: {stage: extract, from: source.feed, retries: 1}
      summary: {stage: summary, from: source.feed, batch: {count: 2}}
      email: {from: summary, deliver: {use: tests.integration.test_llm_batch_rebuild.sink_callable, with: {target: test@example.test}}}
""", encoding="utf-8")
    (tmp_path / "prompt.md").write_text("Return JSON.", encoding="utf-8")
    return open_app(definition, tmp_path / "state.db", llm_client=InvalidLLM(responses))

def test_valid_structured_item_materializes_one_derived_output(tmp_path):
    app = llm_app(tmp_path, [{"title": "A"}])
    collect_source(app, "feed", instant(0))
    reconcile_node(app, "p", "extract", instant(0))
    item_id = app.conn.execute("SELECT id FROM items WHERE logical_id='item-a'").fetchone()[0]
    execution_id = execute_stage(
        app, "extract", StageEnvelope((item_id,)), pipeline_name="p", node_name="extract",
        generation=1, batch_id=None, at=instant(0),
    )
    row = app.conn.execute(
        "SELECT e.state, i.payload_json FROM executions e "
        "JOIN stage_results sr ON sr.id=e.result_id "
        "JOIN stage_result_items sri ON sri.result_id=sr.id "
        "JOIN items i ON i.id=sri.item_id WHERE e.id=?",
        (execution_id,),
    ).fetchone()
    assert row[0] == "succeeded"
    assert json.loads(row[1]) == {"title": "A"}

def test_invalid_item_llm_exhaustion_is_cached_empty_dropped_result(tmp_path):
    app = llm_app(tmp_path, [{"wrong": True}, {"wrong": True}])
    collect_source(app, "feed", instant(0))
    reconcile_node(app, "p", "extract", instant(0))
    item_id = app.conn.execute("SELECT id FROM items WHERE logical_id='item-a'").fetchone()[0]
    execution_id = execute_stage(
        app, "extract", StageEnvelope((item_id,)), pipeline_name="p", node_name="extract",
        generation=1, batch_id=None, at=instant(0),
    )
    assert app.conn.execute("SELECT state FROM executions WHERE id=?", (execution_id,)).fetchone()[0] == "dropped"
    assert app.conn.execute(
        "SELECT COUNT(*) FROM stage_result_items sri JOIN stage_results sr ON sr.id=sri.result_id WHERE sr.producer_execution_id=?",
        (execution_id,),
    ).fetchone()[0] == 0
    cached = execute_stage(
        app, "extract", StageEnvelope((item_id,)), pipeline_name="p", node_name="extract",
        generation=1, batch_id=None, at=instant(3),
    )
    assert app.conn.execute("SELECT state FROM executions WHERE id=?", (cached,)).fetchone()[0] == "cached"
    assert app.conn.execute("SELECT COUNT(*) FROM stage_results WHERE stage_name='extract'").fetchone()[0] == 1


def test_invalid_batch_llm_output_stays_failed_and_frozen(tmp_path):
    app = llm_app(tmp_path, [{"bad": True}])
    collect_source(app, "feed", instant(0))
    reconcile_node(app, "p", "summary", instant(0))
    batch_id = app.conn.execute(
        "SELECT current_frozen_batch_id FROM node_state WHERE pipeline_name='p' AND node_name='summary'"
    ).fetchone()[0]
    item_ids = [row[0] for row in app.conn.execute("SELECT item_id FROM batch_items WHERE batch_id=? ORDER BY ordinal", (batch_id,))]
    execution_id = execute_stage(
        app, "summary", StageEnvelope(tuple(item_ids)), pipeline_name="p", node_name="summary",
        generation=1, batch_id=batch_id, at=instant(0),
    )
    assert app.conn.execute("SELECT state FROM executions WHERE id=?", (execution_id,)).fetchone()[0] == "failed"
    assert app.conn.execute("SELECT state FROM batches WHERE id=?", (batch_id,)).fetchone()[0] == "failed"
    assert app.conn.execute("SELECT COUNT(*) FROM stage_results WHERE producer_execution_id=?", (execution_id,)).fetchone()[0] == 0
    assert [row[0] for row in app.conn.execute("SELECT item_id FROM batch_items WHERE batch_id=? ORDER BY ordinal", (batch_id,))] == item_ids
```

- [ ] **Step 2: Run the LLM/batch tests before adding the Task implementations.**

Run: `pytest -q tests/integration/test_llm_batch_rebuild.py`

Expected: FAIL with missing `sluice.tasks.llm` and no invalid-output transition.

- [ ] **Step 3: Implement explicit LLM adapter calls and schema validation.**

Read prompt file text only after `verify_stage_files` has matched its startup digest; include that digest and the inline output-schema structure in Stage cache configuration. This slice adds `InvalidStructuredOutput(ExecutionError)` as a private retry classification. `llm_extract` calls the bound startup adapter with model/prompt/item payload, validates the configured shape, returns `NEW(value, metadata)` on success, and raises that classification to the Stage retry transition. `llm_summary` calls the adapter with the ordered batch values, validates one aggregate result, returns the singleton batch tuple `(NEW(value),)`, and never returns `DROP`.

Add exactly `sluice.tasks.llm_extract -> sluice.tasks.llm.bind_extract` and `sluice.tasks.llm_summary -> sluice.tasks.llm.bind_summary` to `BUILTIN_TASK_BINDERS` in this slice.

- [ ] **Step 4: Implement the exact retry/drop rules in the Stage state machine.**

`_execute_claimed_stage` catches `InvalidStructuredOutput`. For an `each` extraction it invokes the same guarded `_fail_execution` transition while retry budget remains and continues with the returned retry ID. Only the exhausted final attempt with `invalid: drop` is atomically marked `dropped` and given a reusable empty StageResult; `execute_stage` returns that final ID. Normal reconciliation then creates a cached Execution without another LLM call. Any batch error, including invalid summary output, marks the attempt failed, publishes no result, and keeps `current_frozen_batch_id` and exact `batch_items` unchanged for manual retry/rebuild. A retry uses the same ordered Items and history references and never reselects upstream data.

- [ ] **Step 5: Verify the scenario family and commit.**

Run: `pytest -q tests/integration/test_llm_batch_rebuild.py`

Expected: PASS for valid structured extraction, invalid per-item drop caching after retries, invalid batch failure with the exact frozen Item order retained, and no Task-level persistence. `NEW([])` versus empty-batch short circuit is already gated once by Task 4's pure chain test; non-empty retry/rebuild history is gated once in Task 8.

```bash
git add src/sluice/tasks/llm.py src/sluice/errors.py src/sluice/resolution.py src/sluice/stage_runtime.py tests/integration/test_llm_batch_rebuild.py
git commit -m "feat: validate llm outputs and preserve batch retry semantics"
```

### Task 8: Implement exact-input Manual Stage Rebuild and historical-scope exclusion

**Files:**
- Modify: `src/sluice/stage_runtime.py` (add the one public `rebuild_stage` action beside the existing materialization state machine)
- Modify: `src/sluice/reconcile.py`
- Modify: `src/sluice/app.py` (resolve retained Stage snapshots at startup)
- Modify: `src/sluice/config.py` (decode and verify retained Stage snapshots)
- Modify: `src/sluice/resolution.py` (resolve retained Task paths through existing adapters)
- Modify: `tests/integration/test_llm_batch_rebuild.py`

**Interfaces:**
- `RebuildResult(execution_id: int | None, status: Literal["queued", "running", "succeeded", "failed"], forced: bool)` is a frozen result value; `rebuild_stage(app: App, execution_id: int, at: datetime) -> RebuildResult` always returns this one type. If it acquires the claim immediately it synchronously runs to a terminal result; `queued` is returned only while another owner holds the claim.
- `verify_execution_envelope(app: App, execution_id: int) -> StageEnvelope` raises `ExecutionError` on missing Items, inconsistent snapshot, or a lookup-key mismatch; it never substitutes current YAML.
- `decode_stage_snapshot(value: Mapping[str, JSONValue]) -> StageDef` reconstructs the complete retained Stage definition, and `verify_stage_files(stage: StageDef) -> None` verifies every normalized retained path/digest before execution.

```python
from sluice.errors import ExecutionError

def decode_stage_snapshot(value: Mapping[str, JSONValue]) -> StageDef:
    required = {"name", "input", "cache_version", "tasks", "output_schema"}
    if set(value) != required or value["input"] not in {"each", "batch"} or not isinstance(value["tasks"], list):
        raise ExecutionError("invalid retained Stage configuration")
    tasks = []
    for encoded in value["tasks"]:
        if not isinstance(encoded, dict) or set(encoded) != {"use", "with"} or not isinstance(encoded["with"], dict):
            raise ExecutionError("invalid retained Task configuration")
        runtime_config = dict(encoded["with"])
        for file_key in ("prompt", "schema", "output_schema"):
            if file_key in runtime_config:
                runtime_config.pop(f"{file_key}_digest", None)
        tasks.append(TaskDef(use=str(encoded["use"]), with_config=freeze_json(runtime_config)))
    canonical = canonical_json(value)
    output_schema = None if value["output_schema"] is None else freeze_json(value["output_schema"])
    stage = StageDef(str(value["name"]), value["input"], tuple(tasks), output_schema, int(value["cache_version"]), canonical)
    try:
        verify_stage_files(stage)
    except (DefinitionError, KeyError, TypeError, ValueError) as error:
        raise ExecutionError("retained Stage file metadata is invalid") from error
    return stage
```

- [ ] **Step 1: Write red tests for current failed scope, historical rebuild, unchanged output, and post-freeze pending preservation.**

```python
from datetime import datetime, timedelta, timezone
from sluice.stage_runtime import rebuild_stage
from sluice.reconcile import reconcile_node, tick
from sluice.source_runtime import collect_source

def instant(seconds: int) -> datetime:
    return datetime(2026, 8, 14, tzinfo=timezone.utc) + timedelta(seconds=seconds)

def failed_batch_app(tmp_path):
    app = llm_app(tmp_path, [{"bad": True}, {"summary": "rebuild-1"}, {"summary": "rebuild-2"}])
    collect_source(app, "feed", instant(0))
    reconcile_node(app, "p", "summary", instant(0))
    batch_id = app.conn.execute(
        "SELECT current_frozen_batch_id FROM node_state WHERE pipeline_name='p' AND node_name='summary'"
    ).fetchone()[0]
    item_ids = tuple(row[0] for row in app.conn.execute("SELECT item_id FROM batch_items WHERE batch_id=? ORDER BY ordinal", (batch_id,)))
    execution_id = execute_stage(
        app, "summary", StageEnvelope(item_ids), pipeline_name="p", node_name="summary",
        generation=1, batch_id=batch_id, at=instant(0),
    )
    collect_source(app, "feed", instant(1))
    reconcile_node(app, "p", "summary", instant(1))
    return app, execution_id, batch_id, item_ids

def test_rebuild_current_failed_batch_uses_exact_history_and_clears_only_current_freeze(tmp_path):
    app, target, batch_id, item_ids = failed_batch_app(tmp_path)
    result = rebuild_stage(app, target, instant(10))
    assert result.forced is True
    assert result.execution_id is not None
    rebuilt = result.execution_id
    assert [row[0] for row in app.conn.execute("SELECT item_id FROM execution_inputs WHERE execution_id=? AND slot='items' ORDER BY ordinal", (rebuilt,))] == list(item_ids)
    assert app.conn.execute("SELECT COUNT(*) FROM execution_inputs WHERE execution_id=? AND slot='history'", (rebuilt,)).fetchone()[0] == 0
    assert app.conn.execute("SELECT current_frozen_batch_id FROM node_state WHERE pipeline_name='p' AND node_name='summary'").fetchone()[0] is None
    assert [row[0] for row in app.conn.execute("SELECT i.logical_id FROM node_members nm JOIN items i ON i.id=nm.item_id WHERE nm.pipeline_name='p' AND nm.node_name='summary' AND nm.state='pending' ORDER BY nm.ordinal")] == ["item-c"]


def test_historical_rebuild_publishes_shared_history_but_does_not_touch_current_scope(tmp_path):
    app, target, original_batch_id, _ = failed_batch_app(tmp_path)
    rebuild_stage(app, target, instant(1))
    collect_source(app, "feed", instant(2))
    reconcile_node(app, "p", "summary", instant(2))
    current_before = app.conn.execute("SELECT current_frozen_batch_id FROM node_state WHERE pipeline_name='p' AND node_name='summary'").fetchone()[0]
    rebuilt = rebuild_stage(app, target, instant(10)).execution_id
    assert app.conn.execute("SELECT state FROM executions WHERE id=?", (rebuilt,)).fetchone()[0] in {"succeeded", "cached"}
    assert app.conn.execute("SELECT target_scope_excluded FROM executions WHERE id=?", (rebuilt,)).fetchone()[0] == 1
    assert app.conn.execute("SELECT current_frozen_batch_id FROM node_state WHERE pipeline_name='p' AND node_name='summary'").fetchone()[0] == current_before
    assert [row[0] for row in app.conn.execute("SELECT i.logical_id FROM node_members nm JOIN items i ON i.id=nm.item_id WHERE nm.pipeline_name='p' AND nm.node_name='summary' AND nm.state='pending' ORDER BY nm.ordinal")] == ["item-c"]
    tick(app, instant(11))
    assert app.conn.execute("SELECT current_frozen_batch_id FROM node_state WHERE pipeline_name='p' AND node_name='summary'").fetchone()[0] == current_before

def test_rebuild_rejects_changed_retained_prompt_file(tmp_path):
    app, target, _, _ = failed_batch_app(tmp_path)
    (tmp_path / "prompt.md").write_text("Changed after the retained snapshot.", encoding="utf-8")
    with pytest.raises(ExecutionError):
        rebuild_stage(app, target, instant(10))
```

The same scenario family must exercise a non-empty dedup history envelope, not only the empty first observation:

```python
def versioned_source(config, checkpoint):
    old = checkpoint is None
    score = 1 if old else "not-a-number"
    suffix = "old" if old else "new"
    records = tuple(
        SourceRecord(name, {"url": f"https://example.test/{name}", "content": suffix, "score": score})
        for name in ("a", "b")
    )
    return SourceResult(records, "v1" if old else "v2")

def test_retry_and_rebuild_preserve_non_empty_dedup_history_ids(tmp_path):
    definition = tmp_path / "history.yml"
    definition.write_text("""
version: 1
database: state.db
sources: {feed: {use: tests.integration.test_llm_batch_rebuild.versioned_source, with: {}}}
stages:
  select:
    input: batch
    tasks:
      - {use: sluice.tasks.dedup, with: {keys: [url]}}
      - use: sluice.tasks.filter
        with: {all: [{field: score, op: gte, value: 0}]}
pipelines:
  p:
    nodes:
      select: {stage: select, from: source.feed, retries: 1, batch: {count: 2, include_updates: true}}
      email: {from: select, deliver: {use: tests.integration.test_llm_batch_rebuild.sink_callable, with: {}}}
""", encoding="utf-8")
    app = open_app(definition, tmp_path / "state.db")
    collect_source(app, "feed", instant(0))
    reconcile_node(app, "p", "select", instant(0))
    first_batch = app.conn.execute("SELECT current_frozen_batch_id FROM node_state WHERE pipeline_name='p' AND node_name='select'").fetchone()[0]
    first_items = tuple(row[0] for row in app.conn.execute("SELECT item_id FROM batch_items WHERE batch_id=? ORDER BY ordinal", (first_batch,)))
    first = execute_stage(app, "select", StageEnvelope(first_items), pipeline_name="p", node_name="select", generation=1, batch_id=first_batch, at=instant(0))
    assert app.conn.execute("SELECT state FROM executions WHERE id=?", (first,)).fetchone()[0] == "succeeded"

    collect_source(app, "feed", instant(1))
    reconcile_node(app, "p", "select", instant(1))
    second_batch = app.conn.execute("SELECT current_frozen_batch_id FROM node_state WHERE pipeline_name='p' AND node_name='select'").fetchone()[0]
    second_items = tuple(row[0] for row in app.conn.execute("SELECT item_id FROM batch_items WHERE batch_id=? ORDER BY ordinal", (second_batch,)))
    failed = execute_stage(app, "select", StageEnvelope(second_items), pipeline_name="p", node_name="select", generation=2, batch_id=second_batch, at=instant(1))
    history_ids = tuple(row[0] for row in app.conn.execute(
        "SELECT item_id FROM execution_inputs WHERE execution_id=? AND slot='history' ORDER BY ordinal", (failed,)
    ))
    assert history_ids
    parent = app.conn.execute("SELECT retry_parent_id FROM executions WHERE id=?", (failed,)).fetchone()[0]
    assert parent is not None
    assert tuple(row[0] for row in app.conn.execute(
        "SELECT item_id FROM execution_inputs WHERE execution_id=? AND slot='history' ORDER BY ordinal", (parent,)
    )) == history_ids
    rebuilt = rebuild_stage(app, failed, instant(2)).execution_id
    assert tuple(row[0] for row in app.conn.execute(
        "SELECT item_id FROM execution_inputs WHERE execution_id=? AND slot='history' ORDER BY ordinal", (rebuilt,)
    )) == history_ids
```

- [ ] **Step 2: Run the rebuild tests before adding the action.**

Run: `pytest -q tests/integration/test_llm_batch_rebuild.py -k rebuild`

Expected: FAIL with `ImportError: cannot import name 'rebuild_stage' from 'sluice.stage_runtime'`.

- [ ] **Step 3: Implement snapshot verification and forced claim ordering.**

Read `executions.config_json` (which contains `cache_version`), original `binding_digest`, exact `execution_inputs` (`items` and `history`), and retained Items; decode with `decode_stage_snapshot(json.loads(config_json))`, reconstruct stored Task paths/configuration/file digests, and reject missing fields. Recompute the Stage lookup key, call the Task 2 file verifier against every stored digest, and reject a missing/mismatched file or key as corruption; never substitute current YAML or execute changed file contents. Task 8 also extends `open_app`: after opening SQLite, it scans distinct retained Stage snapshots, extracts their Task paths, and resolves them through the same adapters before returning the immutable App, so removed current-YAML stages are still rebuildable and no callable is loaded mid-run. If a retained path is unavailable, startup fails with `DefinitionError`.

`rebuild_stage` constructs the private `_StageRequest(force=True, retained_stage=decoded_stage, binding_digest=target.binding_digest, rebuild_target_id=target.id)` and calls the existing `_request_materialization`; it does not grow the public `execute_stage` signature or add a parallel rebuild state machine. If it queues behind an owner, the waiter and promoted Execution retain the target ID, original binding, canonical config, policy, and exact envelope, and `_execute_claimed_stage` always decodes `executions.config_json`. A forced waiter is promoted before normal waiters after the current owner terminalizes and is never satisfied from the current cache. On success, make the new StageResult current and reconcile all active consumers if output envelopes changed.

- [ ] **Step 4: Implement node-local scope guard and unchanged-output behavior.**

Every forced Execution stores `rebuild_target_id`. Satisfy a current failed `each` member only when its admitted generation remains active. Satisfy a failed batch only when its target `batch_id` still equals `node_state.current_frozen_batch_id`; clear that frozen ID while retaining post-freeze pending rows. If either check fails, the guarded finalize transaction sets `target_scope_excluded=1` on the forced Execution while still publishing the global StageResult. Later result-sequence healing for that exact target Pipeline/node/generation/batch advances its consumed sequence without applying the excluded result; other active consumers may use it normally. Thus a historical target or a scope replaced while rebuild waited cannot alter that node's current pending/frozen membership even on a later tick. If ordered output Item IDs and complete envelopes are unchanged, record the forced Execution and do not publish a downstream snapshot revision.

- [ ] **Step 5: Verify and commit the rebuild slice.**

Run: `pytest -q tests/integration/test_llm_batch_rebuild.py -k rebuild`

Expected: PASS for the public forced path, exact non-empty envelope/history reuse, current failed-batch satisfaction, persisted historical scope exclusion after a later tick, post-freeze pending preservation, and rejection of a changed retained prompt file. Forced-before-normal selection remains one branch of Task 6's stable-claim transaction rather than a second concurrency E2E matrix; lookup-key consistency is checked by `verify_execution_envelope` in these same rebuild paths.

```bash
git add src/sluice/stage_runtime.py src/sluice/reconcile.py src/sluice/app.py src/sluice/config.py src/sluice/resolution.py tests/integration/test_llm_batch_rebuild.py
git commit -m "feat: rebuild stages from exact retained executions"
```

### Task 9: Render channel outputs and deliver independently with token-guarded retries

**Files:**
- Modify: `src/sluice/tasks/content.py` (add `render_email`, `render_telegram` each Tasks)
- Create: `src/sluice/delivery.py`
- Create: `src/sluice/sinks/__init__.py`
- Create: `src/sluice/sinks/email.py`
- Create: `src/sluice/sinks/telegram.py`
- Create: `tests/integration/test_delivery.py`
- Modify: `src/sluice/canonical.py` (add `delivery_binding_digest` only when Delivery first needs it)
- Modify: `src/sluice/resolution.py`
- Modify: `src/sluice/db.py` (add the Delivery row/indexes)
- Modify: `src/sluice/reconcile.py` (connect generic node admission and startup/tick dispatch to Delivery rows)

**Interfaces:**
- `render_email(config, input: Item) -> NEW(str)` and `render_telegram(config, input: Item) -> NEW(str)` are plain cached each Task callables after resolution.
- `deliver(app: App, pipeline_name: str, node_name: str, rendered_item_id: str, at: datetime) -> str` claims/executes one Delivery row and returns its stable delivery key, not an incidental SQLite rowid.
- `retry_delivery(app: App, delivery_key: str, at: datetime) -> str` retries one failed Delivery without recomputing upstream Stages and returns the same key.
- `_DeliveryAttempt(delivery_key, attempt_token)`, `_claim_delivery`, `_complete_delivery`, `_fail_delivery_attempt`, and `_ensure_delivery_rows` are private transitions used by `deliver`, `retry_delivery`, the existing `tick`, and `recover_startup`. They do not form a second public state-machine API.

- [ ] **Step 1: Write the Delivery scenario-family red tests with inline Sink boundary callables.**

```python
from datetime import datetime, timedelta, timezone
from threading import Event, Thread
from sluice.app import open_app
from sluice.contracts import SourceRecord, SourceResult
from sluice.delivery import deliver, retry_delivery
from sluice.reconcile import recover_startup, tick
from sluice.source_runtime import collect_source

def instant(seconds: int) -> datetime:
    return datetime(2026, 8, 14, tzinfo=timezone.utc) + timedelta(seconds=seconds)

def source_collect(config, checkpoint):
    return SourceResult((SourceRecord("rendered", {"content": "message"}),), "done")

def delivery_app(tmp_path, sink_clients, email_target="a@example.test"):
    definition = tmp_path / "definition.yml"
    definition.write_text(f"""
version: 1
database: state.db
sources: {{feed: {{use: tests.integration.test_delivery.source_collect, with: {{}}}}}}
pipelines:
  p:
    nodes:
      email: {{from: source.feed, retries: 1, concurrency: 1, deliver: {{use: sluice.sinks.email, with: {{target: {email_target}}}}}}}
      telegram: {{from: source.feed, retries: 1, concurrency: 1, deliver: {{use: sluice.sinks.telegram, with: {{target: primary}}}}}}
""", encoding="utf-8")
    app = open_app(definition, tmp_path / "state.db", sink_clients=sink_clients)
    collect_source(app, "feed", instant(0))
    item_id = app.conn.execute("SELECT id FROM items").fetchone()[0]
    return app, item_id

def test_sibling_deliveries_retry_independently_and_null_is_success(tmp_path):
    sent = []
    def email(config, payload, key):
        sent.append(("email", key)); return None
    def telegram(config, payload, key):
        raise RuntimeError("down")
    app, _ = delivery_app(tmp_path, {"sluice.sinks.email": email, "sluice.sinks.telegram": telegram})
    tick(app, instant(0))  # generic node admission creates and drains both Delivery rows
    email_key = app.conn.execute("SELECT delivery_key FROM deliveries WHERE node_name='email'").fetchone()[0]
    telegram_key = app.conn.execute("SELECT delivery_key FROM deliveries WHERE node_name='telegram'").fetchone()[0]
    assert app.conn.execute("SELECT state FROM deliveries WHERE delivery_key=?", (email_key,)).fetchone()[0] == "succeeded"
    assert app.conn.execute("SELECT external_id FROM deliveries WHERE delivery_key=?", (email_key,)).fetchone()[0] is None
    assert app.conn.execute("SELECT state FROM deliveries WHERE delivery_key=?", (telegram_key,)).fetchone()[0] == "failed"
    upstream_before = app.conn.execute("SELECT COUNT(*) FROM executions").fetchone()[0]
    retry_delivery(app, telegram_key, instant(1))
    assert app.conn.execute("SELECT COUNT(*) FROM executions").fetchone()[0] == upstream_before
    assert app.conn.execute("SELECT state FROM deliveries WHERE delivery_key=?", (telegram_key,)).fetchone()[0] == "failed"
    assert app.conn.execute("SELECT state FROM deliveries WHERE delivery_key=?", (email_key,)).fetchone()[0] == "succeeded"


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
    app, item_id = delivery_app(tmp_path, {
        "sluice.sinks.email": email,
        "sluice.sinks.telegram": lambda config, payload, key: "ok",
    })
    worker = Thread(target=deliver, args=(app, "p", "email", item_id, instant(0)))
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
    assert second_app.conn.execute("SELECT COUNT(*) FROM deliveries WHERE state='succeeded'").fetchone()[0] == 2
```

- [ ] **Step 2: Run the Delivery tests before implementing rows and tokens.**

Run: `pytest -q tests/integration/test_delivery.py`

Expected: FAIL with missing `sluice.delivery` and sink registrations.

- [ ] **Step 3: Implement deterministic renderers and Sink resolution.**

Renderers must return JSON-compatible text derived only from the Item payload/config. Resolved Sinks expose `send(config, rendered_payload, delivery_key) -> str | None`; a string or null is success, any other return or raised exception fails, and a Sink cannot return `DROP`. Built-in email/telegram modules are wrapped around the startup sink adapter; tests use explicit inline sink boundary functions, never mocks of `delivery.py`.

Add `render_email`/`render_telegram` binders to `BUILTIN_TASK_BINDERS` and `sluice.sinks.email`/`sluice.sinks.telegram` binders to `BUILTIN_SINK_BINDERS` here; no earlier task predeclares these modules.

Extend `init_schema` here with the complete Delivery table and indexes:

```sql
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
```

- [ ] **Step 4: Implement Delivery key, claim, retry, and startup recovery.**

The existing generic `reconcile_node` remains the sole edge-cursor/ordered-union algorithm. For a terminal Delivery node it updates `node_members` exactly as for any other node and performs no Stage request. In the same `tick` loop, private `_ensure_delivery_rows` maps newly active rendered members to stable `queued` Delivery keys; once a durable row exists, that manifest member is no longer counted as Stage pending work. Delivery health comes only from the Delivery row: queued/retryable-failed is waiting, running is running, exhausted failed is failed, and succeeded is satisfied. `tick` calls public `deliver` in member order while the node has capacity. There is no `reconcile_delivery_node`, second edge cursor, or Delivery-specific snapshot algorithm. Snapshot omission deletes only queued rows that never acquired an attempt token; running, failed, and succeeded rows remain durable, and successful external sends are never withdrawn.

Compute binding digest from exact Sink `use` path and canonical `deliver.with`; the `config` passed to `SinkContract.send` is exactly that snapshotted `with` mapping, while retry/timeout/concurrency remain runner-owned policy. The delivery key hashes fully-qualified Pipeline/Delivery node, rendered Item ID, and complete binding digest. In one transaction insert or reuse the unique row, assign a monotonically increasing attempt token, set `updated_at=at` as that running attempt's start instant, and wait/reuse when already running. Completion succeeds only while the token owns the row; `tick` evaluates timeout as `updated_at + persisted policy timeout <= at`, and timeout/startup failure conditionally marks only that token failed. Retry decodes timeout/retry/concurrency from persisted `policy_json`, claims one failed row, increments retry count, and replaces `updated_at`; every later state transition updates it. A successful row is never resent, changing the declared Sink path/config creates a new key, and retries never run upstream Tasks or siblings.

Add a private Delivery branch to the existing `recover_startup` and `tick` dispatch after the Delivery schema exists: conditionally fail interrupted rows by token and scan queued/failed retries. The entrypoints and scan loop remain the ones introduced in Task 6. Bind `sink_clients` into immutable wrappers with the unchanged `send(config, rendered_payload, delivery_key)` signature; no global Sink client is retained on `App`.

Delivery claim/send/success/failure/retry/stale-completion paths call `log_event` with `pipeline`, `node`, `delivery_key`, `rendered_item_id`, `attempt_token`, `external_id`, and `state`. Do not log the rendered payload or add a Delivery-specific logging backend.

- [ ] **Step 5: Verify and commit independent Delivery semantics.**

Run: `pytest -q tests/integration/test_delivery.py`

Expected: PASS for null success, raised error/retry, independent sibling state, binding digest changes, concurrent claim suppression, stale completion rejection, interrupted-send recovery, and no upstream recomputation.

```bash
git add src/sluice/tasks/content.py src/sluice/delivery.py src/sluice/sinks src/sluice/canonical.py src/sluice/resolution.py src/sluice/db.py src/sluice/reconcile.py tests/integration/test_delivery.py
git commit -m "feat: deliver rendered items with independent retries"
```

### Task 10: Expose local CLI and Prefect parity over the same core functions

**Files:**
- Create: `src/sluice/cli.py`
- Create: `src/sluice/prefect_flow.py`
- Create: `tests/smoke/test_prefect_parity.py`

**Interfaces:**
- CLI commands in this slice are `sluice tick DEFINITION --at ISO8601`, `sluice reconcile DEFINITION PIPELINE NODE --at ISO8601`, `sluice rebuild DEFINITION EXECUTION_ID --at ISO8601`, and `sluice retry-delivery DEFINITION DELIVERY_KEY --at ISO8601`; each calls `open_app` then the corresponding production function. Task 11 adds the HTML `ui` command after `sluice.ui` exists.
- `prefect_tick(definition_path: str, db_path: str, at: datetime | None = None) -> None` is the only Prefect Flow body and calls `tick` directly; no outcome DTO, `RunnerBackend`, or Prefect-specific state machine is added.

- [ ] **Step 1: Write the parity smoke before adding entrypoints.**

```python
import sqlite3
from datetime import datetime, timedelta, timezone
from sluice.app import open_app
from sluice.contracts import SourceRecord, SourceResult
from sluice.prefect_flow import prefect_tick
from sluice.reconcile import tick

def instant(seconds: int) -> datetime:
    return datetime(2026, 8, 14, tzinfo=timezone.utc) + timedelta(seconds=seconds)

def acceptance_source(config, checkpoint):
    return SourceResult((SourceRecord("a", {"title": "A"}),), "done")

def acceptance_sink(config, payload, delivery_key):
    return "sent"

def write_acceptance_definition(tmp_path):
    definition = tmp_path / "definition.yml"
    definition.write_text("""
version: 1
database: state.db
sources: {feed: {use: tests.smoke.test_prefect_parity.acceptance_source, with: {}}}
stages: {documents: {input: each, tasks: [{use: sluice.tasks.to_markdown}]}}
pipelines:
  p:
    nodes:
      documents: {stage: documents, from: source.feed}
      email: {from: documents, deliver: {use: tests.smoke.test_prefect_parity.acceptance_sink, with: {target: test@example.test}}}
""", encoding="utf-8")
    return definition

def runtime_summary(connection):
    connection.row_factory = sqlite3.Row
    execution_states = dict(connection.execute(
        "SELECT state, COUNT(*) FROM executions GROUP BY state"
    ).fetchall())
    delivery_states = dict(connection.execute(
        "SELECT state, COUNT(*) FROM deliveries GROUP BY state"
    ).fetchall())
    item_logical_ids = tuple(row[0] for row in connection.execute(
        "SELECT logical_id FROM items ORDER BY logical_id"
    ))
    return execution_states, delivery_states, item_logical_ids

def test_prefect_flow_and_local_tick_produce_identical_sqlite_state(tmp_path):
    definition = write_acceptance_definition(tmp_path)
    local = open_app(definition, tmp_path / "local.db")
    tick(local, instant(0))
    prefect_tick(str(definition), str(tmp_path / "prefect.db"), instant(0))
    assert runtime_summary(local.conn) == runtime_summary(sqlite3.connect(tmp_path / "prefect.db"))
```

- [ ] **Step 2: Run the smoke to verify missing entrypoints.**

Run: `python -m pip install -e '.[test,prefect]' && pytest -q tests/smoke/test_prefect_parity.py`

Expected: FAIL with `ModuleNotFoundError: No module named 'sluice.prefect_flow'`.

- [ ] **Step 3: Implement thin CLI/Flow wrappers.**

Parse only the documented arguments, normalize ISO timestamps to UTC, and pass them unchanged to `tick`, `reconcile`, `rebuild_stage`, or `retry_delivery`. `prefect_tick` may use `@flow` but its body must construct `App` and invoke `tick`; it must not duplicate scheduling, retries, or database logic. Keep Prefect import in this module so local execution does not require the optional dependency unless installed.

- [ ] **Step 4: Verify entrypoint parity and commit.**

Run: `pytest -q tests/smoke/test_prefect_parity.py`

Expected: PASS; local and Prefect paths produce equivalent durable state for the same definition, database, adapters, and fixed `at`.

```bash
git add src/sluice/cli.py src/sluice/prefect_flow.py tests/smoke/test_prefect_parity.py
git commit -m "feat: expose local and prefect execution entrypoints"
```

### Task 11: Add the deterministic management read model, minimal UI/actions, and final acceptance smoke

**Files:**
- Create: `src/sluice/read_model.py`
- Create: `src/sluice/ui.py`
- Create: `tests/smoke/test_management.py`
- Create: `tests/smoke/test_acceptance.py`
- Modify: `src/sluice/cli.py` (add `sluice ui` and human-readable read-model output commands)

**Interfaces:**
- `PipelineSummary(name: str, state: Literal["idle","waiting","running","failed"], pending_count: int, failed_count: int, latest_delivery: str | None, latest_error: str | None)`, `NodeDetail(name: str, stage: str | None, executions: tuple[int, ...], frozen_items: tuple[str, ...], deliveries: tuple[str, ...])`, and `PipelineDetail(name: str, nodes: tuple[NodeDetail, ...], frozen_batches: Mapping[str, tuple[str, ...]], deliveries: Mapping[str, str])` are frozen read-model dataclasses.
- `pipeline_list(app: App) -> tuple[PipelineSummary, ...]` returns name, state, pending/failed counts, latest successful Delivery, and latest error.
- `pipeline_detail(app: App, pipeline_name: str) -> PipelineDetail` returns the Stage DAG, latest Executions per node, frozen batch membership, and independent Delivery states.
- `default_rebuild_target(app: App, pipeline_name: str, node_name: str) -> int | None` returns the latest current failed Stage Execution for that fully qualified node, or `None`; completed historical Executions remain selectable only through their explicit IDs.
- `rebuild_action(app: App, execution_id: int, at: datetime) -> RebuildResult` and `retry_delivery_action(app: App, delivery_key: str, at: datetime) -> str` call `rebuild_stage`/`retry_delivery` directly.
- `create_ui(app: App) -> flask.Flask` serves HTML list/detail pages and HTML form POST actions for rebuild and Delivery retry; it emits no JSON API and is not a management service.

- [ ] **Step 1: Write the management and acceptance smoke tests before implementing the read model/UI.**

```python
# tests/smoke/test_management.py
from datetime import datetime, timezone
from urllib.parse import quote
from sluice.app import open_app
from sluice.contracts import SourceRecord, SourceResult
from sluice.read_model import default_rebuild_target, pipeline_detail, pipeline_list
from sluice.reconcile import tick
from sluice.ui import create_ui

def management_source(config, checkpoint):
    return SourceResult((SourceRecord("logical-a", {"title": "A"}),), "done")

class ManagementLLM:
    def complete(self, model, prompt, input_payload):
        return {"invalid": True}

def management_sink(config, payload, delivery_key):
    raise RuntimeError("delivery unavailable")

def management_app(tmp_path):
    definition = tmp_path / "definition.yml"
    definition.write_text("""
version: 1
database: state.db
sources: {feed: {use: tests.smoke.test_management.management_source, with: {}}}
stages:
  pass: {input: each, tasks: [{use: sluice.tasks.to_markdown}]}
  drop:
    input: each
    tasks:
      - {use: sluice.tasks.llm_extract, with: {model: test, prompt: prompt.md, output: {title: string}, invalid: drop}}
  waiting: {input: each, tasks: [{use: sluice.tasks.to_markdown}]}
  failed_summary:
    input: batch
    tasks: [{use: sluice.tasks.llm_summary, with: {model: test, prompt: prompt.md, output: {summary: string}}}]
pipelines:
  daily_digest:
    nodes:
      pass: {stage: pass, from: source.feed}
      dropped: {stage: drop, from: source.feed}
      waiting: {stage: waiting, from: source.feed, schedule: 1h}
      failed_summary: {stage: failed_summary, from: source.feed, batch: {count: 1}}
      email: {from: pass, retries: 1, deliver: {use: tests.smoke.test_management.management_sink, with: {target: test@example.test}}}
""", encoding="utf-8")
    (tmp_path / "prompt.md").write_text("Return a title object.", encoding="utf-8")
    app = open_app(definition, tmp_path / "state.db", llm_client=ManagementLLM())
    tick(app, datetime(2026, 8, 14, tzinfo=timezone.utc))
    return app

def test_management_read_model_states_counts_and_actions(tmp_path):
    app = management_app(tmp_path)
    summaries = pipeline_list(app)
    summary = next(value for value in summaries if value.name == "daily_digest")
    assert summary.state == "failed"
    assert summary.failed_count == 2
    assert summary.pending_count == 1
    detail = pipeline_detail(app, "daily_digest")
    pass_node = next(node for node in detail.nodes if node.name == "pass")
    client = create_ui(app).test_client()
    listing = client.get("/pipelines")
    detail_page = client.get("/pipelines/daily_digest")
    assert listing.status_code == detail_page.status_code == 200
    assert listing.content_type.startswith("text/html")
    assert detail_page.content_type.startswith("text/html")
    failed_target = default_rebuild_target(app, "daily_digest", "failed_summary")
    assert failed_target is not None
    assert client.post("/pipelines/daily_digest/nodes/failed_summary/rebuild").status_code in {202, 303}
    execution_id = pass_node.executions[0]
    delivery_key = next(iter(detail.deliveries))
    assert client.post(f"/executions/{execution_id}/rebuild").status_code in {202, 303}
    assert client.post(f"/deliveries/{quote(delivery_key, safe='')}/retry").status_code in {202, 303}
```

```python
# tests/smoke/test_acceptance.py
from datetime import datetime, timedelta, timezone
from sluice.app import open_app
from sluice.contracts import SourceRecord, SourceResult
from sluice.reconcile import tick

def rss_source(config, checkpoint):
    return SourceResult((SourceRecord("rss-a", {"title": "RSS A"}),), "rss-done")
def site_source(config, checkpoint):
    return SourceResult((SourceRecord("site-b", {"title": "Site B"}),), "site-done")
class AcceptanceLLM:
    def complete(self, model, prompt, input_payload):
        return {"summary": "RSS A; Site B"}
def sink_ok(config, payload, delivery_key): return "sent"

def acceptance_app(tmp_path):
    definition = tmp_path / "definition.yml"
    definition.write_text("""
version: 1
database: state.db
sources:
  rss: {use: tests.smoke.test_acceptance.rss_source, with: {}}
  site: {use: tests.smoke.test_acceptance.site_source, with: {}}
stages:
  candidates: {input: batch, tasks: [{use: sluice.tasks.dedup, with: {keys: [title]}}]}
  summary:
    input: batch
    tasks: [{use: sluice.tasks.llm_summary, with: {model: test, prompt: prompt.md, output: {summary: string}}}]
  email_render: {input: each, tasks: [{use: sluice.tasks.render_email}]}
  telegram_render: {input: each, tasks: [{use: sluice.tasks.render_telegram}]}
pipelines:
  daily_digest:
    nodes:
      candidates: {stage: candidates, from: [source.rss, source.site], batch: {count: 2}}
      summary: {stage: summary, from: candidates, batch: {count: 2}}
      email_message: {stage: email_render, from: summary}
      telegram_message: {stage: telegram_render, from: summary}
      email: {from: email_message, deliver: {use: tests.smoke.test_acceptance.sink_ok, with: {target: email}}}
      telegram: {from: telegram_message, deliver: {use: tests.smoke.test_acceptance.sink_ok, with: {target: telegram}}}
""", encoding="utf-8")
    (tmp_path / "prompt.md").write_text("Summarize the ordered items.", encoding="utf-8")
    return open_app(definition, tmp_path / "state.db", llm_client=AcceptanceLLM())

def test_branching_merging_sources_shared_summary_and_two_deliveries(tmp_path):
    app = acceptance_app(tmp_path)
    tick(app, datetime(2026, 8, 14, tzinfo=timezone.utc))
    tick(app, datetime(2026, 8, 14, 1, tzinfo=timezone.utc))
    logical_ids = [row[0] for row in app.conn.execute(
        "SELECT i.logical_id FROM node_members nm JOIN items i ON i.id=nm.item_id WHERE nm.pipeline_name='daily_digest' AND nm.node_name='candidates' ORDER BY nm.ordinal"
    )]
    assert logical_ids == ["rss-a", "site-b"]
    assert app.conn.execute("SELECT COUNT(*) FROM stage_results WHERE stage_name='summary'").fetchone()[0] == 1
    assert {row[0]: row[1] for row in app.conn.execute("SELECT node_name,state FROM deliveries")} == {"email": "succeeded", "telegram": "succeeded"}
```

- [ ] **Step 2: Run the smoke tests to observe the missing read model/UI.**

Run: `pytest -q tests/smoke/test_management.py tests/smoke/test_acceptance.py`

Expected: FAIL with `ModuleNotFoundError: No module named 'sluice.read_model'`.

- [ ] **Step 3: Implement deterministic read-model projections directly from SQLite.**

Compute Pipeline state with exact precedence `failed > running > waiting > idle`; count only current unsatisfied referenced Source/Stage/Delivery failures, pending members excluding frozen, queued retries, shared waiters, and current frozen membership. Ignore superseded history and authorized dropped results. Detail output must preserve YAML DAG order, execution chronology by database sequence, batch Item order, and independent Delivery status. `default_rebuild_target` filters to the selected fully qualified Stage node's current scope and orders failed Executions by descending database sequence; it never selects a failed Delivery or a superseded/historical Stage attempt. Do not expose SQL schema as a public API.

- [ ] **Step 4: Implement minimal Flask pages/actions over the same functions.**

`GET /pipelines` renders names/state/counts/latest success/error as HTML; `GET /pipelines/<name>` renders DAG, latest executions, frozen membership, and Delivery rows as HTML. The node detail form posts to `POST /pipelines/<pipeline>/nodes/<node>/rebuild`, resolves `default_rebuild_target`, and returns `404` when no current failed Stage target exists; `POST /executions/<id>/rebuild` remains the explicit action for a user-selected failed or completed historical Execution. `POST /deliveries/<path:key>/retry` calls `retry_delivery`. Return `202` or a redirect with a visible HTML status for actions and `404` for unknown targets. Add `sluice ui DEFINITION [--host HOST --port PORT]` to `cli.py`, which calls `create_ui(open_app(definition_path)).run(host=host, port=port)`. Use Flask’s test client for one route/render smoke; do not add JSON endpoints, JavaScript/browser automation, or a separate API service.

Import `sluice.ui`/Flask only inside the `ui` subcommand so the core CLI remains usable without the optional `ui` extra.

- [ ] **Step 5: Run the complete focused suite and the final deterministic smoke.**

Run: `python -m pip install -e '.[test,prefect,ui]' && pytest -q tests/unit tests/integration tests/smoke`

Expected: PASS for all four integration scenario families, acceptance branching/merge/fan-out, Prefect/local parity, and management state/action projections. The suite must use fixed `datetime` values and boundary fakes only; it must not sleep, call live services, mock internals/SQLite, or compare incidental SQL/digest details.

- [ ] **Step 6: Commit the UI/read-model and final acceptance slice.**

```bash
git add src/sluice/read_model.py src/sluice/ui.py src/sluice/cli.py tests/smoke
git commit -m "feat: add management read model ui and acceptance smoke"
```

## Self-review checklist (run before implementation handoff)

- [ ] Spec §3 contracts are covered by Tasks 1–4; Source/Task/Sink startup resolution is in Task 2; Item immutability and typed identities are in Tasks 1 and 3.
- [ ] Spec §4 collection and batching are covered by Tasks 4–5; pending/frozen transactions, include-updates, immutable `batch_id`, and exact retry membership are covered by Tasks 5–7.
- [ ] Spec §§6–7 cache keys, Publications, node snapshots, shared ownership, waiter transfer, guarded finalization, and callback-independent reconciliation are covered by Tasks 4–6.
- [ ] Spec §8 retry/recovery/drop semantics are covered by Tasks 6–7 and Delivery recovery in Task 9.
- [ ] Spec §9 exact-input rebuild and historical scope exclusion are covered by Task 8, including no upstream repair.
- [ ] Spec §10 independent Delivery state, binding digest, token guard, and external duplicate window are covered by Task 9.
- [ ] Spec §§11–14 direct SQLite persistence, local/Prefect parity, UI read model, and structured runtime visibility are covered by Tasks 1, 6, 10, and 11.
- [ ] Spec §15’s four integration families plus one E2E, one Prefect smoke, and one UI smoke are explicit; no repository/test DSL/fake orchestrator/Clock/browser matrix appears in the file map or tasks.
- [ ] Spec §16 permanent non-goals are recorded in Global Constraints; no task introduces compatibility, distributed coordination, joins/windows, plugin discovery, or exactly-once delivery.
- [ ] Placeholder scan: every implementation step has concrete inputs, outputs, test command, expected result, and no unresolved planning marker or non-copyable control-flow sketch.
- [ ] Type/signature scan: public `App`, `Definition`, `StageEnvelope`, `BatchPolicy`, marker values, `tick`, targeted `reconcile`, admission-only `reconcile_node`, `execute_stage`, `recover_startup`, `rebuild_stage`, `deliver`, and read-model actions are introduced before later tasks consume them; claim/finalize/failure helpers remain private.

Plan complete and saved to `docs/superpowers/plans/2026-08-14-sluice-v4-cleanroom-implementation.md`. Two execution options:

1. **Subagent-Driven (recommended)** — dispatch a fresh subagent per task and review between tasks.
2. **Inline Execution** — execute tasks in this session with `superpowers:executing-plans` checkpoints.

Which approach?
