# Sluice v4 Clean-room Design

**Date:** 2026-08-14

**Status:** Approved for implementation planning

**Repository:** `sluice-pipeline-v4` orphan worktree

## 1. Purpose

Sluice collects information from multiple sources, processes each item through reusable steps, aggregates selected results, and publishes the final output to one or more sinks.

The basic flow is:

```text
Sources
  -> deterministic preprocessing
  -> per-item LLM extraction
  -> batch selection and summary
  -> channel-specific rendering
  -> Deliveries
```

The design must also support:

- different collection and processing schedules;
- reusable intermediate results shared by multiple pipelines;
- configurable filtering, sorting, limiting, and deduplication;
- cached Stage results and execution metadata;
- count- or time-triggered batch processing;
- independent retries for processing and delivery;
- Prefect-driven execution and direct local execution;
- a small management UI that lists pipelines, shows execution state, and rebuilds a Stage.

This is a clean-room implementation. The old repository may be read for algorithms and behavior, but v1, v2, and v3 configuration, storage, APIs, tests, and runtime structures are not compatibility requirements.

## 2. Design Principles

### 2.1 Preserve product capabilities, not old architecture

Multiple sources, multiple sinks, fetch processing, LLM processing, reusable stages, and configurable information flows are product capabilities and remain central.

The old runtime's storage, upgrade, compatibility, and deployment machinery is not inherited.

### 2.2 Stage is the materialization boundary

A Stage is reusable and has no hidden mutable state. Its successful output is persisted and may be reused by any Pipeline that explicitly references the same Stage.

A Task may call an external website or LLM, but result-affecting configuration and data must be explicit. It must not silently depend on wall-clock time, randomness, or mutable process globals. Once an external result is materialized, ordinary execution reuses it until explicit refresh or changed input.

Tasks inside a Stage are not individually cached. If an intermediate operation is expensive or independently reusable, it should become its own Stage. This keeps cache behavior visible and prevents Task-level bookkeeping from spreading through the system.

### 2.3 Pipeline is a persistent DAG, not a monolithic run

A Pipeline connects Source outputs, Stage uses, and one or more Delivery endpoints. Nodes may branch, merge, and run on different schedules.

There is no requirement to create one global Pipeline Run covering every upstream action. The management view derives Pipeline health from its node Executions, pending batches, and Deliveries.

### 2.4 Reconciliation is an algorithm, not a platform

Normal `reconcile(target)` derives eligible work from durable upstream publications, cache entries, and node state. It is an ordinary application function invoked by Prefect, the local CLI, Stage completion, or the management UI. Manual rebuild is the narrower exact-input operation defined in Section 9 and does not perform upstream repair.

There is no separate Reconciler service, persistent reconciliation queue, event bus, or control-plane protocol.

### 2.5 Prefer explicit reuse

Two Pipelines share work by referencing the same named Stage definition. Stage identity is part of the cache key. Sluice does not attempt to prove that differently named Stages are semantically equivalent.

### 2.6 Keep the first runtime local

SQLite is the only state store. One process owns and writes a database at a time. Prefect and the local CLI are alternate entrypoints, not concurrent owners. v4 does not design multi-host coordination, distributed leases, or a distributed queue.

## 3. Public Model

The public model contains five concepts: `Source`, `Task`, `Stage`, `Pipeline`, and `Delivery`. `Item`, `StageResult`, and `Execution` are persisted values, not additional authoring layers.

### 3.1 Item

An Item is an immutable version of information.

```text
Item
  id                 hash of typed producer scope, logical_id, payload, and metadata
  producer_scope     (source, name) or (stage, name)
  logical_id         stable identity within that producer scope
  payload            JSON-compatible data
  metadata           immutable source- or Task-produced metadata
  content_hash       hash of canonical payload content
  observed_at        when Sluice first stored this Item version
  produced_by        first Source or Stage Execution ID
```

Standard text-oriented payloads conventionally use fields such as:

```text
url
title
content
published_at
updated_at
tags
extras
```

These conventions are not a global Schema Registry. In v4's initial scope, structured LLM Tasks may validate their output shape at runtime.

Items are append-only. Repeated production of the same producer scope, logical identity, payload, and immutable metadata is an idempotent insert. A changed payload or metadata produces a new Item version rather than updating the old one in place.

`produced_by` names the Execution that first inserted an Item. Later cache hits or refreshes that reuse the same Item ID remain visible through their Execution and StageResult records; they do not rewrite the Item.

IDs and hashes use canonical JSON: object keys are sorted, numbers retain JSON value semantics, strings remain unchanged, and timestamps are normalized to UTC. YAML formatting and JSON key order do not create new versions.

A Source Item keeps the time of its first stored observation. A newly derived Item starts with the latest `observed_at` among its inputs. If a later execution produces the same Item ID, it reuses the stored Item and its original `observed_at`; the later input times remain visible in that Execution's input envelope. Immutable Items are never updated merely to record a later execution.

Tasks inside a Stage pass transient values; intermediate Task output is not persisted. At the final Stage boundary, a Task may forward an existing Item reference unchanged. Filtering, sorting, limiting, and other collection-selection Tasks normally do this, so merely crossing a Stage boundary does not clone an Item.

Any new final value is materialized as a derived Item in the typed scope `(stage, Stage name)`. A one-to-one derived output gets a stable logical identity from the input's typed producer scope and logical identity; bare source-local keys from different Sources therefore cannot collide. A single aggregate output, such as a summary, derives its logical identity from the ordered input Item IDs. In the initial implementation, collection Stages may forward several existing Items, but a transforming `each` Stage emits at most one new Item per input and an aggregating `batch` Stage emits at most one new Item. General expansion into several new values is outside the initial Task contract.

### 3.2 Source

A Source implementation resolved at startup exposes:

```text
collect(config, checkpoint) -> SourceResult

SourceResult
  records          finite ordered sequence of SourceRecord
  next_checkpoint  JSON-compatible value or KEEP_CHECKPOINT

SourceRecord
  logical_id       non-empty source-local stable string
  payload          JSON-compatible value
  metadata         optional JSON object, default {}
```

`SourceResult`, `SourceRecord`, and the identity singleton `KEEP_CHECKPOINT` are production values exported by `sluice.contracts`. `KEEP_CHECKPOINT` is compared by identity and is never persisted; JSON null is therefore available as an explicit checkpoint value. `config` is exactly the Source's YAML `with` mapping, validated before the call and treated as immutable by the implementation.

RSS and webpage collectors are the initial Source implementations. A webpage Source may represent a single page or a site-specific list page that yields several records.

Records are processed in returned order. `KEEP_CHECKPOINT` leaves the previous checkpoint unchanged; an empty record sequence may still advance to an explicit next checkpoint. A raised exception, malformed record, or invalid checkpoint makes the whole Source Execution fail without publishing records or advancing the checkpoint.

The runtime turns a record into an Item using the Source name, `logical_id`, canonical payload, and metadata. A Source binding and checkpoint scope consists of Source name, declared `use` path, and canonical `with` configuration; schedule and execution policy are excluded. Before the external call, the runner claims that Source binding and expected checkpoint for one Execution. Atomic finalization is defined in Section 7.4; replaying unchanged records remains idempotent.

Collections are serialized per complete Source binding `(name, use path, canonical with configuration)`. A scheduled or manual trigger reuses the active Source Execution instead of starting a second collection from the same checkpoint.

Implementations use built-in names or local Python dotted paths resolved once at process startup. There is no discovery, package entry-point contract, registry, marketplace, or hot loading.

### 3.3 Task

A Task is one atomic Python operation. A Task implementation resolved at startup exposes its `input_mode` (`each` or `batch`) and a `run(config, input)` callable. `config` is exactly the Task's validated YAML `with` mapping and is treated as immutable. The Task returns only explicit production values exported by `sluice.contracts`:

```text
DROP                         singleton: no output; never Python/JSON null
FORWARD(item)                retain an existing input Item reference
NEW(value, metadata={})      create a transient new value; value may be Python/JSON null
```

A bare Python `None` or another undeclared return shape is an execution error. `FORWARD` may reference only the current `each` Item or an existing Item in the batch's `items` collection, and may forward it at most once; dedup `history` is read-only and cannot be forwarded. `FORWARD` cannot attach new provenance or disguise a new transient value. Intermediate `NEW` values may be ordinary in-process Python values, allowing fetch and parser Tasks to cooperate without persistence. At the final Stage boundary, every new payload must be JSON-compatible and metadata must be a JSON object; otherwise the attempt fails. A raised exception fails the Stage attempt and follows its retry policy. Tasks cannot assign or mutate Item IDs, `observed_at`, or `produced_by`.

An `each` Stage passes one current value through its Task chain. `DROP` stops that input; `FORWARD` or `NEW` supplies at most one transient value to the next Task. At `batch` Stage entry, the first Task receives a production `sluice.contracts.BatchInput` containing the ordered `items` list and, only for an initial `dedup`, its recorded `history` list. It returns an ordered sequence of `DROP`, `FORWARD`, or `NEW` entries. Dropped entries disappear; each later batch Task receives the ordered remaining values and returns the same explicit entry shapes. Only the final Task output is converted into the StageResult and persisted Items.

An empty batch output short-circuits the remaining Task chain and produces an empty StageResult. A Task that intentionally wants one JSON empty-list payload returns the singleton output `NEW([])` instead. An `each` chain may finish with at most one `NEW`; a batch chain may forward several existing Items but may finish with at most one aggregate `NEW`. Exceeding these initial cardinalities is a Stage error.

Initial built-in Tasks include:

- `dedup`
- `filter`
- `sort`
- `limit`
- `fetch_webpage`
- `extract_content`
- `to_markdown`
- `llm_extract`
- `llm_summary`
- channel renderers

`dedup`, `filter`, `sort`, `limit`, `llm_summary`, and collection-wide aggregators are `batch` Tasks. `fetch_webpage`, `extract_content`, `to_markdown`, `llm_extract`, and channel renderers are `each` Tasks. The loader rejects a built-in Task used in the wrong Stage mode. A local dotted-path Task declares the same `input_mode`; this startup contract does not add discovery or hot loading.

Tasks do not own persisted state, update checkpoints, schedule other work, or mutate Pipeline state. The reserved dedup `history` is recorded in the Stage input envelope rather than read through hidden Task state.

### 3.4 Stage

A Stage is an ordered list of Tasks and the only processing cache boundary.

Each Stage declares one input mode:

- `each`: execute independently for every input Item;
- `batch`: execute once for a frozen ordered list of Items.

A StageResult contains an ordered list of output Items. An `each` execution receives one Item and emits zero or one Item. A `batch` execution receives a frozen ordered input envelope and emits an ordered result list.

A Stage definition may include:

- Task import paths and Task configuration;
- optional structured LLM output validation;
- an explicit `cache_version` when implementation behavior changes without a YAML change.

Stage schedules, batch thresholds, retries, and concurrency belong to a Pipeline node using the Stage, not to the reusable Stage definition.

### 3.5 Pipeline

A Pipeline is a DAG of Source outputs, Stage uses, and Delivery endpoints.

- Multiple upstream references form an ordered union. Upstreams follow YAML declaration order; Items retain upstream result order; duplicate Item IDs keep their first position.
- Multiple downstream references naturally fan out.
- `from` accepts one upstream reference or an ordered list and supplies the node's ordered Item stream.
- Every Pipeline must contain at least one Delivery endpoint.

There are no public named inputs or implicit joins in the initial format. Correlation beyond ordered union belongs in an explicit Task after the inputs have been normalized into one finite Item stream.

The graph loader validates references, cycles, node names, input modes, and the existence of a Delivery. It does not perform static type inference or schema compatibility proofs.

### 3.6 Delivery

A Delivery is the side-effecting terminal boundary. It receives a rendered Item and calls a Sink implementation using the same startup-only built-in-name or local dotted-path resolution as Sources and Tasks.

Delivery is not a Stage and is never placed in the Stage cache. Each Sink endpoint has independent status and retry state, so one failed channel does not rerun summary generation or affect another channel.

## 4. Collection Operations

Collection behavior is expressed with ordinary Tasks rather than a general Operator algebra.

### 4.1 Deduplication and versions

`dedup` computes a Stage-scoped comparison identity from configured keys:

```yaml
with:
  keys: [url]
```

The comparison identity is scoped to the named Stage and computed from the exact configured values. It does not rewrite the accepted candidate's Item identity; accepted candidates are forwarded unchanged. URL normalization, redirect resolution, and tracking-parameter removal must be explicit preceding Tasks.

Within the current input collection, duplicate identities keep the candidate with the latest `observed_at`; equal timestamps use Item ID as a deterministic tie-breaker.

Before running a Stage whose first Task is `dedup`, the executor resolves at most one prior accepted Item for each current comparison identity. History is limited to Items published by the current reusable StageResults for that same named Stage; superseded refresh results remain available for audit but are never used to build dedup history. History is ordered by first appearance of the corresponding identity in the current input. Within one identity, “prior” is the latest Item ordered before the current candidate by `(observed_at, Item ID)`; StageResults whose input envelope contains the current candidate Item ID are excluded. Those immutable Items are recorded in the reserved `history` part of `BatchInput` before cache lookup and are visible only to the initial `dedup` Task.

`history` is an internal side input supported only when `dedup` is the first Task. It may be empty and never participates in readiness, `count`, pending membership, or frozen membership. It is nevertheless part of the immutable Execution input envelope and Stage cache key.

This makes history explicit, deterministic, and Stage-local. Once a candidate reaches a terminal Execution, normal reconciliation and rebuild reuse its recorded history snapshot and cache key. A later refresh of an older accepted Item affects future candidates but never retroactively reprocesses terminal dedup inputs in v4's initial scope. Items removed later by `filter` or `limit` are not accepted history, so a future candidate is compared with the latest version that the complete Stage actually published. There is no global hidden `seen` set.

For an existing comparison identity:

- identical payload and metadata emit no Item;
- changed metadata forwards the candidate even when its payload is unchanged;
- changed payload normally forwards the new candidate Item;
- an optional comparison rule may suppress insignificant changes by emitting no new Item.

The optional rule is:

```yaml
change:
  field: content
  min_rate: 0.05
```

`change_rate` applies only when payload changed and metadata did not. It is the Levenshtein edit distance divided by the larger Unicode-code-point length, with an empty/empty value defined as zero. A missing configured field is a Stage error. No previous accepted Item yields a rate of `1.0`. A rate below `min_rate` emits no Item; a rate at or above it forwards the candidate. The decision and rate may be recorded on the Execution outcome, but do not mutate the candidate Item or become an implicit downstream field.

### 4.2 Filtering

`filter` uses a small data-only rule structure. It supports nested `all`, `any`, and `not`, plus these initial operations:

```text
eq, ne, gt, gte, lt, lte, in, not_in, contains, exists
```

It does not execute Python, Jinja, or another expression language.

Rules may read payload fields such as `title`, `content`, and `published_at`. Stable Item fields use an explicit prefix, for example `$item.observed_at` and `$item.content_hash`, so a payload field cannot shadow them. Sorting uses the same field lookup rules.

`exists` is the only operation that treats a missing field specially. Other operations on a missing or null value evaluate false. Comparing incompatible non-null types is a Stage error; the runtime does not coerce strings into numbers or dates implicitly.

### 4.3 Sorting and limiting

`sort` accepts an ordered list of field, direction, and null-position rules. Nulls default to last. A stable Item ID is used as the final tie-breaker.

`limit` only truncates the already ordered collection. When no explicit sort precedes it, the runtime uses deterministic input order rather than inventing a ranking rule.

### 4.4 Batch accumulation

Batching is configuration on a Stage use:

```yaml
batch:
  count: 20
  max_wait: 6h
  include_updates: false
```

A node using an `each` Stage cannot declare `batch`. A node using a `batch` Stage must declare at least one of a positive integer `count`, a positive duration `max_wait`, or `on_schedule: true`. `on_schedule` requires a node schedule and freezes all currently pending input when that schedule becomes due. When several conditions are present, the first reached freezes the batch. There is no implicit “all available Items” policy. `include_updates` defaults to false and controls whether a later version of an identity that was already frozen or completed becomes a candidate for a later batch.

The runtime stores pending Item IDs, `first_pending_at`, and due times in `node_state`. It freezes a batch when a configured condition is reached. `max_wait` starts when the first Item enters an empty pending batch. The selected scheduler invokes due reconciliation even when no new upstream result arrives; polling cadence is an entrypoint setting, not Stage semantics. `count` and `max_wait` may freeze a batch before its scheduled time when configured; `on_schedule` freezes only when the node schedule is due.

- A frozen batch never changes.
- A retry uses the same ordered Item list.
- Before freezing, a newer version replaces an older pending version in place for the same `(producer scope, logical_id)` key, preserving deterministic position.
- New identities arriving after a freeze wait for the next batch. A later version of an identity already frozen or completed enters the next pending batch only when `include_updates` is true.
- Freezing and moving pending Items to the frozen batch occur in one SQLite transaction. That transaction allocates an immutable, monotonically increasing database `batch_id` and records the exact ordered envelope.
- A failed batch remains frozen; its retry does not consume newly pending Items.
- Every retry and rebuild of that batch retains the same `batch_id`; an Execution ID is attempt-specific and never substitutes for it.
- A node has at most one frozen batch. Newly pending Items do not freeze into another batch until the current frozen batch succeeds.
- A successful or cached batch commits its StageResult and clears `current_frozen_batch_id` in one transaction; the immutable batch row remains as history and the next freeze receives a new ID. Items that arrived after the freeze remain pending for the next batch.

There are no sliding windows, session windows, watermarks, or general stream-processing semantics.

## 5. YAML Definition

YAML is the only human-authored definition format. It is loaded with a safe YAML parser and converted directly into ordinary configuration objects. There is no public compiler IR or alternative Python DSL.

```yaml
version: 1
database: .sluice/sluice.db

sources:
  news_rss:
    use: sluice.sources.rss
    schedule: 1h
    with:
      url: https://example.com/feed.xml

  watched_site:
    use: my_sources.watched_site
    schedule: 10m
    with:
      url: https://example.cn/news

stages:
  select_articles:
    input: batch
    tasks:
      - use: sluice.tasks.dedup
        with:
          keys: [url]
      - use: sluice.tasks.filter
        with:
          all:
            - field: published_at
              op: gte
              value: "2026-08-01T00:00:00Z"
      - use: sluice.tasks.sort
        with:
          by:
            - field: published_at
              order: desc
            - field: title
              order: asc
      - use: sluice.tasks.limit
        with:
          count: 100

  prepare_article:
    input: each
    tasks:
      - use: sluice.tasks.fetch_webpage
      - use: sluice.tasks.extract_content
      - use: sluice.tasks.to_markdown

  extract_article:
    input: each
    tasks:
      - use: sluice.tasks.llm_extract
        with:
          model: example-small-model
          prompt: prompts/extract.md
          output:
            title: string
            summary: string
            tags: list[string]
            extras: object
          invalid: drop

  summarize:
    input: batch
    tasks:
      - use: sluice.tasks.llm_summary
        with:
          model: example-large-model
          prompt: prompts/summarize.md

  render_email:
    input: each
    tasks:
      - use: sluice.tasks.render_email

  render_telegram:
    input: each
    tasks:
      - use: sluice.tasks.render_telegram

pipelines:
  daily_digest:
    nodes:
      candidates:
        stage: select_articles
        from:
          - source.news_rss
          - source.watched_site
        schedule: 1h
        batch:
          on_schedule: true
          include_updates: true

      documents:
        stage: prepare_article
        from: candidates
        schedule: 3h

      records:
        stage: extract_article
        from: documents

      summary:
        stage: summarize
        from: records
        batch:
          count: 20
          max_wait: 6h

      email_message:
        stage: render_email
        from: summary

      telegram_message:
        stage: render_telegram
        from: summary

      email:
        from: email_message
        deliver:
          use: sluice.sinks.email
          with:
            target: digest@example.com

      telegram:
        from: telegram_message
        deliver:
          use: sluice.sinks.telegram
          with:
            target: primary
```

## 6. Cache and Materialization

The Stage lookup key is:

```text
hash(
  Stage name,
  cache_version,
  canonical result-affecting Stage configuration,
  input envelope
)
```

The persisted input envelope is a deterministic ordered list of immutable Item IDs, separated into `items` and reserved `history` when deduplication uses one. Cache-key construction resolves those references and hashes each complete immutable Item value, so Item identity, payload, metadata, stable Item fields, and order affect the key without duplicating payloads in every Execution or StageResult. For `each`, `items` contains one reference; for `batch`, it contains the frozen references.

The canonical Stage configuration includes Task paths and result-affecting Task configuration. When a Task references a prompt or output-schema file, the loader includes that file's content digest. `cache_version` defaults to `1`.

Pipeline name, schedule, batch trigger, retry count, concurrency, and executor are excluded.

Imported Python source is not inspected or automatically hashed. A behavior-changing code edit requires an explicit `cache_version` change or a manual refresh. This avoids brittle source inspection and package-version machinery.

Each StageResult has an immutable result ID and a database-assigned monotonic sequence. “Latest” means the greatest sequence for one lookup key; wall-clock timestamp ties do not select a winner.

Cache behavior:

- A StageResult is reusable when its producer Execution is `succeeded` with a normal ordered output or `dropped` with a policy-authorized empty output. Failed Executions publish no StageResult.
- Normal execution uses the latest reusable StageResult for the lookup key. Several immutable results may share one lookup key after explicit refreshes.
- A refresh bypasses the current StageResult and appends a new Execution and StageResult. The new result becomes the current publication for that lookup key and supersedes its predecessor without deleting it.
- Old results remain immutable.
- If the refreshed ordered output Item ID list and complete envelopes are unchanged, downstream cache keys remain unchanged and downstream Stages do not rerun.
- If membership, order, or an output Item envelope changes, all Pipeline nodes explicitly referencing that Stage and input become eligible for reconciliation.

The initial implementation retains all Items and StageResults. It does not implement retention tiers, TTL cleanup, or automatic eviction.

## 7. Trigger, Publication, and Execution Flow

Every Pipeline node is identified by the fully qualified pair `(Pipeline name, node name)`. Bare node names are never used as persistence, scheduling, admission, or health keys. Global Stage materialization remains separately identified by `(Stage name, lookup key)` so different Pipelines may share it.

### 7.1 Production tick and triggers

`tick(at: UTC instant)` is an ordinary production application function. The local foreground loop passes the current UTC instant; Prefect or the CLI calls the same function; tests may pass a fixed instant. `at` is used only to evaluate persisted schedules, `max_wait`, retry eligibility, and trigger timestamps. It never enters an Item, Stage cache key, or business payload.

Each tick scans durable state for all enabled Pipelines. Its selection set is the union of:

- unseen upstream Publication revisions;
- Stage materialization sequences newer than the result sequence recorded by an active node member;
- waiting rows whose materialization claim has no active owner or whose current result sequence advanced;
- queued Stage materialization owners and Delivery retries whose fully qualified node has capacity;
- queued Source retries for their serialized binding;
- Source or node schedules whose `next_due_at <= at`;
- batches whose `max_wait` has elapsed; and
- pending batches whose persisted count meets `count`.

This scan is the recovery path for a lost completion callback, an unclean restart, and a newly added node. A completion callback may request the same reconciliation sooner, but correctness never depends on receiving it.

Reconciliation may therefore begin from:

- `tick(at)` discovering persisted work;
- a Source or Stage completion callback;
- a batch reaching `count`;
- a manual management action.

v4 initially supports UTC interval durations such as `10m` or `3h`. A Source is due immediately when first enabled. Its claim transaction records the trigger and advances `next_due_at` to `at + interval` before the external call; rollback leaves it due, while a later collection failure does not rewind the schedule. A scheduled Stage node starts its interval when its first pending input arrives. Claiming scheduled `each` work or freezing an `on_schedule` batch consumes that due time in the same transaction; a later pending arrival starts the next interval. Due times persist in `node_state`, and missed intervals coalesce into one reconciliation after restart. Cron, time zones, and configurable misfire policies are outside the initial scope.

No-trigger Stage nodes become eligible as soon as new input is admitted. A scheduled `each` node only claims pending input when due. A scheduled `batch` node may freeze earlier when an explicitly configured `count` or `max_wait` condition is reached; otherwise `on_schedule` waits for its due time.

### 7.2 Durable publication and admission

A publication is an internal persisted manifest, not a new YAML concept:

```text
Publication
  kind          delta | snapshot
  producer      (Source name, binding digest) or (fully qualified node, binding digest)
  revision      monotonic for that producer binding
  digest        hash of kind, producer, and revision-independent ordered membership
  members       ordered (Item ID, ordinal) rows
```

A node binding digest hashes its Stage identity and canonical Stage definition, ordered `from` edges, schedule, and batch policy. Timeout, retry, and concurrency controls are excluded. During process-start configuration loading, activating a changed digest atomically updates the fully qualified node's current-binding pointer and creates an empty initial snapshot for the new producer binding before bootstrap; the old binding remains historical. There is no live reload. Downstream edges always follow the producer node's current-binding pointer and never merge old and new bindings.

A successful Source collection appends one `delta` revision containing only newly inserted or changed Item versions. Delta revisions never retract an Item merely because a later collection omits it. A Source's durable current view is the concatenation of its revisions in Source sequence and record order. Each Source name has a persisted current-binding pointer; activating a changed `use`/`with` binding atomically points to a new producer binding with an empty initial delta, makes the prior binding's deltas historical, and causes downstream `source.<name>` edges to follow only the new binding.

A StageResult is the global cached value for one Stage lookup key; it is not itself a Pipeline stream. Each fully qualified Stage node instead maintains one cumulative current `snapshot` publication for its current binding digest. The snapshot contains the node's active visible outputs and points to both their StageResults and the materialization sequence used. Node membership keeps admission/attempt state separate from an optional `visible_result`. A changed StageResult, an authorized empty result, or removed current input rebuilds that snapshot and publishes a new revision, including an empty revision when necessary. Starting a retry or forced refresh does not clear `visible_result`; the old result remains visible until a guarded `succeeded` or `dropped` replacement atomically replaces or clears it. A failed replacement publishes nothing and leaves the old visible result unchanged.

Order is persisted rather than inferred from callback or completion timing:

- a Source member ordinal is `(Source revision, record index)`;
- a node merges upstreams by YAML edge declaration index and then upstream member ordinal;
- duplicate Item IDs keep the first merged position;
- an `each` output uses its member's current admission ordinal;
- a `batch` output uses `(batch_id, output index)`.

Each Execution records an immutable claim ordinal for audit, while active admission keeps a separate current ordinal. Upstream reordering updates current ordinals for pending, claimed, and terminal `each` members without changing an Execution's input; a terminal output is reordered in the next node snapshot without rerunning its Task. A frozen batch retains its frozen order.

To keep completion timing from becoming observable ordering, an `each` node publishes not-yet-visible members from one admission generation only through the longest ordered resolved prefix: a member is resolved when it has a `visible_result`, is terminal without output (`dropped` or exhausted `failed`), or is stale. A replacement attempt with an older visible result remains resolved; only an initial pending/running member with no result blocks later unpublished output. Outputs already published by an earlier generation are never removed merely because a later upstream revision inserts an earlier pending member; that member is inserted at its current ordinal when it becomes resolved. Consequently concurrent completion of one admitted set cannot allow its later Item to flow first, while later upstream revisions do not retract valid output just to wait. If an earlier edge stops exposing a duplicate Item, a later edge's copy becomes visible at its deterministic position.

For each fully qualified node, binding digest, and upstream edge, `node_state` stores the applied delta cursor or current snapshot revision and digest. Each active union member stores Item/logical identity, membership generation, current ordinal, `pending | claimed | frozen | terminal | stale` admission state, and—when materialized—its Stage lookup key, consumed materialization sequence, and optional visible result. `reconcile_node(node, at)` performs one idempotent algorithm:

1. scan durable Source deltas after each cursor and the latest Stage snapshot for each upstream producer binding; a new node starts with empty cursors and therefore bootstraps from retained publications;
2. rebuild the desired ordered union and diff it against active admission;
3. append new members, replace a newer unclaimed version in place for the same `(producer scope, logical_id)`, apply the batch node's `include_updates` rule after freeze/terminal consumption, and mark omitted snapshot members stale;
4. persist cursor, membership, ordering, and schedule changes together;
5. claim eligible `each` members or atomically freeze an eligible batch.

Source delta omission is never a tombstone. A new Item ID for a previously terminal identity is a distinct candidate for an `each` node; a batch node admits it only under `include_updates`. Stage snapshot omission removes pending unclaimed input. An already claimed `each` attempt may finish and populate the global cache, but if its admitted generation is stale at finalization its output is excluded from the node's current snapshot and cannot flow downstream. A terminal `each` member that later becomes stale is likewise removed from the current snapshot, causing a superseding snapshot to propagate; its Execution and StageResult remain immutable cache history. A frozen batch is an irreversible consumption boundary: upstream supersession cannot change or cancel its members, and its eventual output is admitted normally. An already sent Delivery is never withdrawn.

When a scheduled `each` node becomes due, one transaction claims current pending members in ordinal order and creates one independent Execution per Item. Later arrivals remain pending. Each failed Item follows its own retry history and does not roll back successful siblings.

### 7.3 Shared materialization ownership

Cache lookup and the global materialization claim occur in one SQLite transaction. At most one node owns active computation for a `(Stage name, lookup key)`. The claim has a stable database-assigned `claim_id` and a replaceable `owner_execution_id`; retry and waiter handoff update the owner field without making existing waiters follow an obsolete attempt ID.

The first claimant creates an owner Execution containing the canonical Stage configuration snapshot, exact input envelope, and effective timeout, retry budget, and concurrency policy. Other nodes persist a waiting row containing `(Stage, lookup key, claim_id)`, exact admission generation or frozen batch ID, input envelope, their effective policy, and a database-assigned monotonic `waiting_sequence`. Waiting consumes none of that node's attempt budget or concurrency and never cancels the owner. Its Pipeline health is `waiting`, not `running` or `failed`.

The owner retains the global claim through its retry chain. On success, the new materialization sequence lets every still-active waiter create its own guarded `cached` Execution and update its node snapshot; a stale waiter may record history but cannot republish removed input. The same sequence cursor lets a previously failed owner heal from a result later produced by another node, subject to the historical-Rebuild scope exclusion in Section 9. A callback is only an optimization because `tick` scans both waiting rows and active members whose recorded result sequence is behind the current StageResult.

If the owner exhausts its retry budget, the same transaction that terminalizes it selects the next active request: the forced refresh with the smallest `waiting_sequence` first, otherwise the normal waiter with the smallest `(waiting_sequence, fully qualified node)`, otherwise no owner. A normal waiter whose admission became stale is skipped; an explicit historical Rebuild remains eligible but cannot update stale node scope under Section 9. Promotion creates a queued owner Execution and transfers the claim under its persisted policy. The claim remains owned while queued, and the runner changes it to `running` only when that node has a free concurrency slot. This defines ownership without oversubscription and without a separate Invocation or computation domain object.

A Rebuild requested while the key is owned persists a waiting row marked `force` with its own `waiting_sequence` before waiting; the forced Execution is created only when that row receives the claim. It is never satisfied by a cache hit. After the current owner becomes terminal, forced rows receive the next compute claim before ordinary cache-miss waiters and run serially; normal waiters may still consume the last successful result while the refresh waits. This removes a post-wait claim race without adding a separate request model.

`reconcile(target)` resolves the target's exact input envelope, uses a current reusable StageResult when present, joins the ownership rule on a cache miss, and reconciles downstream node snapshots after publication. Prefect and local execution call the same functions and do not change these semantics.

### 7.4 Execution states and guarded finalization

Execution states are limited to:

```text
queued, running, succeeded, failed, cached, dropped
```

For `each`, one Execution and StageResult correspond to one input Item. For `batch`, one corresponds to one frozen input envelope. A cache hit records a node-local `cached` Execution pointing to the reused StageResult; no Task runs. A policy-authorized drop records a terminal `dropped` Execution and reusable empty StageResult. A failed Execution publishes no result.

Source collection is also recorded as an Execution with kind `source`, so Source timing and errors use the same history. A Source Execution stores its canonical binding/configuration snapshot, expected checkpoint, and effective policy; Stage and Pipeline-node fields are null. Every owner, retry, and cached Stage Execution stores its fully qualified node, canonical Stage configuration and `cache_version` snapshot, exact input envelope, admitted generation and claim ordinal or frozen batch ID, effective policy/retry budget, lookup key, retry parent, optional result, timing, and error.

An Execution ID is the local ownership token for one attempt. Source and Stage finalization are conditional on that Execution still being `running` and still owning its claim. If a late completion loses this compare-and-swap, all of its proposed output is discarded. If finalization wins first, timeout observes the terminal state and creates no retry. This is a single-process stale-write guard, not distributed fencing.

Raised errors, timeouts, and startup interruption all use one `fail_and_retry` transaction. Guarded by the old Execution ID, it marks the old `queued` or `running` attempt failed. When its persisted retry budget remains, the same transaction creates a queued retry with the identical Stage/Source configuration snapshot, input, policy, and frozen batch ID, then transfers the existing claim directly to the new Execution ID. There is no release window. When the budget is exhausted, an `each` member with no prior visible result becomes terminal failed, while a failed replacement leaves its prior visible result current. A batch instead remains the node's failed `current_frozen_batch_id` with its exact envelope intact until a manual retry/rebuild or shared result succeeds. The transaction then selects the next global Stage request by Section 7.3's forced-then-normal order or releases the Stage/Source claim; promoting another node never clears the failed batch. Exhausted work is not automatically attempted again by later ticks.

A Source finalization transaction, guarded by `(Source binding, Execution ID, expected checkpoint)`, atomically:

1. inserts or reuses Items;
2. writes the ordered delta Publication and digest;
3. advances the checkpoint by compare-and-swap;
4. marks the Source Execution succeeded and releases its claim.

A Stage finalization transaction, guarded by `(Stage name, lookup key, Execution ID)`, atomically:

1. inserts or reuses output Items;
2. writes the StageResult and global current-result pointer;
3. marks the Execution `succeeded` or `dropped`, transfers the claim to the oldest active forced row when one exists, otherwise releases it;
4. rebuilds the owner node's current snapshot only when the Execution's admitted generation remains active or its stable frozen batch ID is still the node's current frozen batch.

Any failure rolls back the entire corresponding finalization. Downstream admission is deliberately not part of either producer transaction: the durable Publication is the source of truth, and `reconcile_node` or the next `tick` applies it idempotently. A crash can delay admission but cannot advance a checkpoint or expose a partial result without its recoverable manifest.

There is no separate Invocation and Attempt domain model. Every actual attempt is an Execution row; a retry points to the previous Execution.

## 8. Failure and Retry Semantics

Stage and Delivery node configuration may specify only:

```yaml
timeout: 120s
retries: 3
concurrency: 4
```

`timeout` limits how long the runner awaits one attempt. `concurrency` limits simultaneously `running` Stage Executions or Delivery rows owned by that fully qualified Pipeline node inside the single writer process; queued owners and failed Delivery rows do not consume a slot. These are execution controls and do not affect Stage cache identity. For shared materialization, they apply only while the node is the owner defined in Section 7.3; a waiting node spends no attempt budget. A timed-out Delivery has the same possible external-send ambiguity described in Section 10.

Sources may use `timeout` and `retries`; Source collection remains serialized, so it has no concurrency setting. `retries` counts additional attempts after the first. Defaults are no timeout, zero retries, and node concurrency one.

The runtime retries any raised execution error up to the configured count. Configuration and graph-loading errors fail before execution. There is no public error taxonomy, circuit breaker, rate-limit DSL, or resource scheduler.

- `each` Stage failures affect only the corresponding Item.
- A batch Stage failure keeps the frozen input list unchanged.
- Retrying creates a new Execution with the same inputs.
- An invalid LLM output may be retried according to node configuration.
- `invalid: drop` applies only to per-item `llm_extract`. When its validation retries are exhausted, the Execution is recorded as dropped and publishes an empty StageResult. Later normal reconciliation reuses that empty result instead of repeatedly calling the LLM.
- A failed batch, including invalid batch LLM output, publishes no StageResult and remains frozen for retry; it cannot be silently dropped.

The process holds one local writer lock for the database. On startup after an unclean exit, every pre-existing `queued` or `running` Source or Stage Execution is passed through the same guarded `fail_and_retry` transition, so its claim is atomically transferred, promoted, or released rather than left orphaned. In-flight Delivery rows are conditionally marked failed with an interrupted-send error under their current attempt token. Pending and frozen inputs remain intact. Retrying an interrupted Delivery has the external duplicate window described in Section 10. This provides crash recovery without leases, heartbeats, or distributed fencing.

## 9. Manual Rebuild

The management action “Rebuild Stage” targets an existing Execution. Every owner, retry, and cached Stage Execution retains the canonical Stage configuration and `cache_version` snapshot used for its lookup key. Rebuild verifies that this snapshot plus the recorded input recomputes the target lookup key, then reruns it against the exact persisted envelope, including Item versions, order, and dedup history. A missing or inconsistent snapshot is data corruption, not permission to substitute current YAML. The UI defaults to the latest failed Execution for the selected node. Rebuild is a forced refresh of the selected target only; it is not an upstream repair operation.

1. The exact target envelope is read from the existing Execution; retained Items are not reselected or regenerated from upstream. A missing referenced Item is data corruption and fails the action.
2. If the target lookup key is already running, rebuild persists the forced waiting row described in Section 7.3; its target Execution is created only after it receives the claim, so a second active computation is never created for the key.
3. The target bypasses its current StageResult. No upstream Stage is forced or rerun.
4. The new StageResult becomes the shared current result for that Stage and lookup key. If its ordered output envelopes changed, Pipeline nodes currently consuming that shared materialization are reconciled; “target only” limits what is force-executed, not where a changed shared result may propagate.
5. If the target belongs to the fully qualified node's current failed admission, success satisfies that admission. For a batch, this is allowed only when the target's stable frozen batch ID still equals `node_state.current_frozen_batch_id`; the transaction clears that frozen batch and leaves all post-freeze pending arrivals untouched.
6. If the target is historical, or a normal retry already replaced or cleared its current scope while rebuild waited, the result remains valid shared cache history but cannot change that fully qualified target node's different current pending or frozen scope—even when it happens to have the same Stage lookup key. The producing Execution identifies this node-local scope exclusion; other Pipeline nodes may reuse the result normally.
7. If the ordered output Item IDs and envelopes are unchanged, the forced Execution is recorded but no downstream snapshot revision is published.

Rebuild never overwrites old Items, StageResults, or Execution records. Apart from satisfying the exact current failed scope described above, it never consumes or changes normal pending or frozen work.

## 10. Delivery Semantics

Delivery formatting remains an ordinary cached Stage. The final external send is a Delivery node.

The configured Sink resolves to a synchronous callable with the minimal shape:

```text
send(config, rendered_payload, delivery_key) -> external_id | null
```

`config` is exactly the validated `deliver.with` mapping and is treated as immutable; `use`, timeout, retry, and concurrency remain runner-owned. `rendered_payload` is JSON-compatible, normally text. A string or null return is success; null means that the Sink has no external ID. A raised exception or any other return type is a failed attempt and follows Delivery retries. A Sink cannot return `DROP`.

One Delivery record is created per rendered output Item. Each record contains:

- Pipeline and Delivery node;
- input StageResult or Item;
- Delivery binding digest;
- status and retry count;
- last error;
- optional external message ID.

The Delivery binding digest hashes the Sink `use` path and the same canonical `with` mapping passed as `config`. A stable key based on the fully qualified Pipeline and Delivery node, rendered Item ID, and that complete binding digest skips a retry only when a local successful row already exists. Changing the declared `use` path or `with` configuration therefore cannot be suppressed by an older success row; changing code behind the same path is not detected automatically. Because the rendered Item identity derives from its upstream inputs, different batches may deliver identical text while an unchanged rebuild of the same inputs does not resend it. A crash after an external send but before the SQLite success commit may still duplicate the message. This is best-effort local suppression, not an exactly-once protocol.

Before calling the Sink, one SQLite transaction inserts or claims the Delivery row as `running` and assigns a monotonically increasing local attempt token. A concurrent caller that finds the same key already running waits for or reuses that attempt rather than sending again. A failed row may be claimed by one retry; a successful row is never claimed again. Timeout and startup recovery conditionally mark only the current token failed, and completion may commit only while its token still owns the running row. A late return from an older attempt is ignored. This prevents local concurrent duplicates and stale local writes but cannot close the external-send crash window.

Delivery nodes use the same `timeout`, `retries`, and `concurrency` fields as Stage nodes. Retrying one Delivery never recomputes upstream Stages, never resends already successful Items, and never changes sibling Delivery state.

## 11. Persistence

The initial implementation uses one SQLite database containing JSON and Markdown payloads directly.

It uses six logical table groups:

| Table | Purpose |
|---|---|
| `items` | Immutable producer-scoped identities, versions, metadata, and payloads |
| `stage_results` | Result ID, monotonic sequence, Stage name, lookup key, immutable ordered input/output Item references, producer Execution/state (including any node-local historical-Rebuild exclusion), creation time, the current-result pointer, and the persisted materialization claim (`claim_id`, owner Execution) per lookup key |
| `executions` | Source/Stage kind, Source binding or fully qualified owner node, canonical Source/Stage configuration snapshot and digest, exact ordered input Item/history references, admitted generation/claim ordinal or frozen batch ID, effective policy and retry budget, lookup key, state, result, timing, retry parent, and error |
| `publications` | Delta/snapshot kind, producer binding, monotonic revision, digest, current pointer, and ordered Item/member references |
| `node_state` | Source current bindings/claims/checkpoints/schedules; fully qualified node binding state; per-edge publication cursors; ordered membership, generation, lookup/result sequence and visible result; due time; current frozen batch; persisted waiter claim/sequence/policy; and current output publication |
| `deliveries` | Unique delivery key, rendered Item ID, Delivery binding digest, state, retry/attempt token, error, and external ID |

Definitions remain in YAML; the database contains runtime values only.

These are logical groups, not a requirement that every manifest or membership be stored in one JSON row. Publication members, applied edge revisions, Execution inputs, and node membership use child rows where ordering and uniqueness matter.

SQLite constraints and guarded transactions enforce:

- one active Source Execution per Source binding and expected checkpoint;
- one active materialization owner per `(Stage name, lookup key)`;
- one current StageResult per lookup key and one Publication revision per producer binding;
- one application of an upstream `(edge, producer binding, revision)` to a fully qualified node and binding digest;
- one active union membership per fully qualified node, binding digest, and Item ID, while allowing only one unclaimed pending version per logical identity and retaining per-edge contributions so deterministic duplicate fallback works;
- one current frozen batch ID per node binding; and
- one Delivery row per delivery key with one current attempt token.

These are local state-machine constraints inside the single writer process, not a task queue, lease, or distributed protocol.

## 12. Prefect and Local Execution

Core execution is expressed as ordinary functions such as:

```text
tick
collect_source
reconcile
reconcile_node
execute_stage
rebuild_stage
deliver
```

Prefect Flows call these functions directly and provide schedules and execution visibility. The local CLI may run one reconciliation or stay in a foreground loop to provide Source schedules and due-time checks.

There is no `RunnerBackend` framework. A deployment chooses Prefect or the local process as the single writer for one SQLite database; running both simultaneously or sharing that database across hosts is unsupported.

## 13. Management UI

The initial UI is intentionally small.

### Pipeline list

- Pipeline name;
- current idle, running, waiting, or failed state;
- pending and failed counts;
- latest successful Delivery;
- latest error.

### Pipeline detail

- Stage DAG;
- latest Executions for each node;
- frozen batch membership;
- independent Delivery states.

### Actions

- rebuild a selected failed or completed Stage Execution from its exact retained input;
- retry a failed Delivery.

The UI and CLI call the same application functions. There is no separate management service or public management API in the initial scope.

Pipeline state is a deterministic read model with precedence `failed > running > waiting > idle`. Only a current unsatisfied referenced Source, Stage, or Delivery failure counts as failed; superseded history and authorized `dropped` results do not. Running means a referenced Source or Pipeline node currently owns an attempt. Waiting includes admitted pending input, a frozen batch, a queued retry, or a shared materialization owned elsewhere. Pending count excludes frozen members, which the detail view reports separately. The initial UI is required scope and is verified through this SQLite-backed read model, not a browser test matrix.

## 14. Logs and Runtime Visibility

Runtime visibility comes from the SQLite execution records and ordinary structured logs containing Pipeline, node, Stage, Execution, Item, and Delivery IDs.

The initial design does not include OpenTelemetry, a metrics server, tracing infrastructure, or a custom monitoring platform.

## 15. Testing Strategy

Development uses behavior-level TDD. Add one red test immediately before implementing one externally observable behavior or persistence invariant. Prefer the boundary `YAML -> production functions/CLI from Section 12 -> SQLite/structured outcome`; there is no separate test runner API, and tests must not define production interfaces. The test set grows with implemented vertical slices instead of being written upfront as a speculative matrix.

Keep unit tests to small table-driven cases for branch-heavy deterministic decisions: callable result markers (`DROP`, existing-Item `FORWARD`, `NEW(None)`, invalid foreign/multiple outputs, and empty-batch short circuit), cache-key semantic equality, collection Tasks including empty dedup history and change-threshold boundaries, DAG validation, and pure batch freeze-eligibility/order decisions. Actual pending-to-frozen transactions use integration tests. Those tests load real YAML, use a temporary SQLite database, and call the public local runner. Small handwritten fixtures or fakes are allowed only at Source/HTTP, LLM, and Sink boundaries, using the production contracts rather than new test abstractions. Such a boundary fake may expose an explicit barrier so the test can drive a timeout or completion order without sleeping. The minimum scenario families verify:

1. Source Items, delta Publication, Execution success, and checkpoint commit all-or-none, including `KEEP_CHECKPOINT`, explicit null, malformed results, startup recovery of an interrupted retry, stale-finalization rejection, and recovery of a missed callback;
2. one Stage materialization is shared by two Pipelines under owner/waiter claim transfer across restart; late or stale completion cannot publish, and concurrent `each` completion, upstream reorder/duplicate fallback, snapshot omission, and binding replacement preserve the defined current snapshot;
3. per-item invalid LLM output exhausts into a cached empty `dropped` result, while invalid batch output remains failed/frozen; the frozen batch retries its exact input/history, and current versus historical rebuild preserves post-freeze pending state;
4. sibling Deliveries retry independently, null Sink success differs from a raised error, the binding digest changes with Sink configuration, and startup recovery of an interrupted or stale attempt cannot overwrite the current row.

Schedule and `max_wait` tests exercise persisted due-time state through production `tick(at)`; they neither sleep nor introduce a test-only clock. After the required slices exist, one short deterministic acceptance smoke covers the principal end-to-end wiring, one Prefect smoke checks entrypoint parity, and one SQLite-backed management read-model smoke checks the required states and actions. These focused scenarios are not duplicated as a second full end-to-end matrix.

Do not mock internal application functions or SQLite, build repository/test-DSL/fake-orchestrator layers, persist Task internals for test seams, or write extension contracts before the extension exists. Assert semantic invariants rather than exact digests, SQL layouts, internal call counts, coverage targets, or test counts. Do not use live external services, wall-clock sleeps, or a browser matrix. A regression belongs at the smallest layer that exposed the bug.

## 16. Permanent Non-goals

The following are intentionally excluded, not deferred extension points:

- v1, v2, or v3 compatibility, migration, or adapters;
- a sandbox, trust/permission model, or hot loading for custom code;
- discovery, package entry points, a plugin registry, or a marketplace;
- schema adapters, a Schema Registry, or static graph type inference;
- a public Operator algebra, GraphSpec compiler, or alternate authoring DSL;
- automatic equivalence detection between different named Stages;
- distributed coordination, leases, fencing, or a distributed task queue;
- general joins, stream windows, or watermarks;
- a general exactly-once delivery protocol.

The initial implementation adds no binary-media or object-storage abstraction; text and JSON remain in SQLite.

## 17. Acceptance Criteria

The design is successfully implemented when:

1. YAML can define a branching and merging Pipeline with multiple Sources and Deliveries.
2. RSS and webpage Source finalization atomically records immutable Items, an ordered delta Publication, Execution success, and checkpoint advancement.
3. Two Pipelines referencing the same Stage and inputs reuse one StageResult through the defined owner/waiter rule, and a timed-out older Execution cannot publish late.
4. Different Source and Stage schedules operate independently through `tick(at)`; retained Publications repair a missed callback and preserve deterministic order.
5. Deduplication keys, change thresholds, filtering, sorting, and limiting work deterministically.
6. Per-item LLM extraction validates structured output and drops only invalid exhausted items.
7. Count/max-wait batching freezes input membership, retries it unchanged, and preserves arrivals after the freeze.
8. Summary output fans out to independently rendered and delivered channels.
9. Manual Stage rebuild uses the selected Execution's exact retained input without upstream regeneration; only a matching current failed scope is satisfied, while a historical rebuild leaves current pending/frozen state untouched.
10. SQLite exposes the deterministic pipeline, node, batch, and Delivery read model required by the initial management UI.
11. Local execution and Prefect execution call the same core behavior.
