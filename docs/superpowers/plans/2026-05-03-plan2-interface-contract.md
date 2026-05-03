# Plan 2 Interface Contract

> Locks the writer/phase interface decisions before Plan 1 is committed.
> Plan 2 MUST honour these contracts; deviating requires updating this doc first.

---

## D1 – Writer injection (resolves H4)

Plan 2 `run_pipeline` receives an already-started `WriterThread` from the caller:

```python
async def run_pipeline(
    task: TaskConfig,
    pipeline_name: str,
    *,
    writer: WriterThread,
    now: datetime | None = None,
    trigger: str = "manual",
) -> RunResult: ...
```

The **caller** (Prefect flow / CLI) owns the writer lifecycle:

```python
writer = WriterThread(active_path=task.db_path, seen_path=task.seen_db)
await writer.start()
try:
    result = await run_pipeline(task, "my_pipeline", writer=writer, ...)
finally:
    await writer.stop()
```

Plan 1 stub keeps its internal writer for now; Plan 2 changes the signature and deletes
the internal `writer = WriterThread(...)` block in `run_pipeline_stub`.

**Why not internal?** In Plan 2 the Prefect task wraps the full pipeline run; it needs to
emit sink data between tx1 and tx2 while the writer is still live. An internally-created
writer that is started and stopped per call cannot span those two phases.

---

## D2 – Migration pre-flight stays outside the writer (resolves H4)

`migrate_task_dynamic_tables(conn, task)` continues to run **before** `writer.start()`,
using its own short-lived connection:

```python
await _ensure_dynamic_tables(task)   # own connection, DDL, commits, closes
writer = WriterThread(...)
await writer.start()
```

DDL (`CREATE TABLE IF NOT EXISTS`) is idempotent. No writer invariant is required.

**Why not inside the writer?** DDL inside `BEGIN IMMEDIATE` is technically valid in
SQLite but unusual. Keeping it as a pre-flight preserves a clean invariant: the writer
only ever runs DML inside its `BEGIN IMMEDIATE / COMMIT` loop.

---

## D3 – WriteJob phases = separate submits, no phase enum

`WriteJob.run` stays `(active, seen) -> None`. Phase separation is achieved by submitting
two separate `WriteJob` instances:

```
submit(job_tx1)  →  await future           # items + stage-start + pipeline upsert-running
<run actual stage computation>
submit(job_tx2)  →  await future           # pipeline_<P> terminal + pipeline finalize
```

No `phase` tag, no phase-aware dispatch inside `WriterThread`. The writer is a dumb
ordered queue; phase ordering is the runner's responsibility.

---

## D4 – Seen writes only in tx1-type jobs

Only jobs that insert new items into `items` also write to the seen DB (via
`ItemSeenStore.insert_many`). tx2 and sink-emit jobs leave seen untouched.

The writer always calls `seen.commit()` after `active.commit()`. If the job made no seen
writes, `seen.commit()` is a no-op in aiosqlite (no open transaction → silently succeeds).
No special casing needed.

---

## Summary table

| Contract | Affects | Status after Plan 1 |
|----------|---------|---------------------|
| D1 – injected writer | `runner.py` signature | Deferred to Plan 2 |
| D2 – migration pre-flight | `_ensure_dynamic_tables` pattern | Already correct |
| D3 – separate-submit phases | `WriteJob` type | Already correct |
| D4 – seen writes in tx1 only | job implementations | Documented; enforced in Plan 2 |
