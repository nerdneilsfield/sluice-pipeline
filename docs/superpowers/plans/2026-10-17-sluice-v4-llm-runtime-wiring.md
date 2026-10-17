# v4 LLM runtime wiring implementation plan

Plan readiness: ready. D1 resolved 2026-10-17.

## Outcome and scope

A YAML definition using `sluice.tasks.llm_extract` / `sluice.tasks.llm_summary`
runs through `sluice tick` and `prefect_tick` without Python-side injection.
Today both entry points call `open_app(...)` with no `llm_client`
(`src/sluice/cli.py:main`, `src/sluice/prefect_flow.py:prefect_tick`), so every
LLM Task fails at execution with `LLM Task has no startup LLM adapter`
(`src/sluice/tasks/llm.py:_complete`). Only tests that pass
`open_app(..., llm_client=...)` exercise LLM Tasks.

In scope:

- A top-level `llm` block configuring one OpenAI-compatible endpoint, built
  into the existing `OpenAICompatibleClient` at startup.
- Startup and rebuild checks so a missing client fails before work is created.
- Stop persisting vendor response bodies in LLM errors (reachable leak into
  `executions.error`).
- A runnable RSS → fetch → LLM → console example plus README.

Out of scope (separate request each): multiple providers or fallback chains,
native Anthropic API, Notion Sink, token budgets, concurrent per-item LLM
calls, JSON `response_format`, passing Item metadata (title/link) to the model,
the intermittent
`test_stale_waiter_after_failed_owner_terminates_deterministically` failure in
`docs/v4-verification.md`.

Existing changes: none in this worktree (clean at `51434ea`, apart from this
plan file).
Authorization: plan-only. Implementation needs explicit approval.

## Design and constraints

### Current behavior

- `open_app` (`src/sluice/app.py`) freezes `llm_client` into
  `BoundaryAdapters` (`src/sluice/resolution.py:freeze_adapters`); built-in
  binders `bind_extract` / `bind_summary` capture it, and accept `None`.
- `OpenAICompatibleClient` (`src/sluice/llm/openai_compatible.py`) implements
  `LLMClient.complete(model, prompt, input_payload)` with retries via `urllib`.
  It does not go through the web SSRF guards. It holds no pooled connections,
  so it needs no close hook.
- `_TOP_LEVEL_KEYS` in `src/sluice/config.py` rejects unknown top-level keys.
  `Definition` (`src/sluice/contracts.py`) already carries an optional `web`
  field built the same way.
- Stage identity, node binding digest and lookup key are computed from the
  canonical Stage snapshot (`use`/`with`/`web_binding` per Task), never from the
  whole `Definition` (`src/sluice/canonical.py`, `config.py` snapshot encoder).
- `open_app` classifies retained snapshots: terminal
  (`succeeded/failed/dropped/cached`) may defer missing web secrets; queued,
  running and `materialization_waiters` snapshots must resolve at startup. This
  currently applies only to web providers; retained LLM Tasks get bound to
  `None` silently.
- `rebuild_stage` (`src/sluice/stage_runtime.py`) already re-checks retained
  web secrets with `register_provider(..., allow_missing_secrets=False)` before
  building the forced request. That loop is where the LLM check goes.
- `_read_error_body` appends up to 1000 chars of the vendor response to the
  `ExecutionError` raised for non-retryable HTTP errors, and the `status >= 400`
  branch does the same. `_fail_execution` stores `str(error)` in
  `executions.error`. A vendor that echoes the request or key leaks it into
  SQLite.

### Change

1. **Definition contract.** Optional top-level `llm`, decoded into a frozen
   `LLMConfig` on `Definition.llm` (default `None`):

   ```yaml
   llm:
     base_url: https://api.example.com/v1   # required
     api_key: env:LLM_API_KEY               # optional, exact env:NAME only
     timeout_seconds: 60                     # optional, finite > 0, not bool
     max_attempts: 3                         # optional, int >= 1, not bool
   ```

   - `base_url`: absolute `http`/`https` URL with a host; no userinfo, query or
     fragment, so credentials cannot hide in the URL. Plain `http` and loopback
     hosts are allowed: the endpoint is trusted startup configuration, not a
     web target, and the integration test needs `http://[IP]:PORT`.
   - `api_key`: same `env:NAME` regex as `web/config.py`. The loader does not
     read the environment; resolution happens in `open_app`, matching
     `web/registry.py`. Literal keys are rejected at load time. An unset
     variable is an error; an empty value means no `Authorization` header
     (same as web: only `None` counts as missing). Document this.
   - Unknown keys are rejected.

2. **Construction in `open_app`.** Before `freeze_adapters`:
   - `llm_client is not None` (identity check, not truthiness) → use it and do
     not read `definition.llm` env references at all.
   - otherwise, if `definition.llm` is set → resolve `api_key` and build
     `OpenAICompatibleClient`. An unset env var raises `DefinitionError`, even
     when no current Stage uses LLM.
   - otherwise → no client.

   Failures go through the existing catalog-close path. `cli.main` and
   `prefect_tick` need no change.

3. **Client availability rules.** "LLM Task" means built-in
   `sluice.tasks.llm_extract` or `sluice.tasks.llm_summary`.

   | Where the LLM Task is | No client | When checked |
   | --- | --- | --- |
   | Current definition Stage | `DefinitionError` | Before the DB is opened; no DB file created |
   | Retained snapshot that is queued/running or a waiter (`allow_missing=False`) | `DefinitionError` | In the retained-snapshot loop, before `recover_startup`; the DB may already exist |
   | Retained terminal snapshot only | App opens normally; history visible; unrelated work runs | — |
   | Explicit `rebuild_stage` of such a snapshot | `DefinitionError` | In `rebuild_stage`'s retained-Task loop, before any owner/waiter/Execution is written |

   Why both retained arms matter: `recover_startup` (`src/sluice/reconcile.py`)
   runs no Task itself, but it calls `_fail_execution` without `supersede`.
   When retries remain, that creates a new queued Execution and transfers the
   claim; releasing a claim can also promote a waiter to queued
   (`src/sluice/stage_runtime.py:_fail_execution`). Either path leaves LLM work
   that a later tick would run with a `None` client. So queued/running and
   waiter snapshots must both be checked before recovery; only terminal-only
   snapshots may defer to the rebuild check.

   Anchor the current-Stage check before `open_database` in `open_app` (the
   current-Stage resolution loop already precedes it); otherwise A3's "no DB
   file" does not hold.

   The retained check must not be skipped when the same `use` is already in
   `task_cache`. `App` exposes whether an LLM client is available (for example
   an `llm_available` flag set from the frozen adapters); rebuild never creates
   or rebinds a client. Rebuild uses this process's endpoint, not a historical
   one, which follows from D1.

4. **Error hygiene in `OpenAICompatibleClient`.** Persisted `ExecutionError`
   messages carry only the HTTP status and attempt count, never the vendor
   body. The leak is the non-retryable `HTTPError` branch; retryable
   exhaustion already raises `failed after N attempts` with the body only in
   `__cause__`, and `_fail_execution` stores just `str(error)`. The
   `status >= 400` return-value branch is unreachable through `urlopen`
   (it raises `HTTPError` first); redact it too, but do not write a test for
   it. Keep the numeric status in the message: the existing unit test asserts
   `match="401"`. Mark `api_key` `repr=False`. The exception chain still holds the
   original `HTTPError`; the plan claims only that `executions.error` and the
   raised message do not contain the body or key, not that tracebacks are
   clean.

### D1: endpoint identity (resolved: excluded)

`llm.*` (URL, credentials, timeout, attempts) is not part of Stage identity,
node binding digest or lookup key. Model, prompt digest and output shape
already are, through Task `with`. Pointing the same model name at a new
endpoint reuses existing StageResults. To force recomputation, use `rebuild`
or bump the Stage `cache_version`. Nothing is added to `TaskDef`,
`web_binding` or the snapshot format, so no migration is needed.

### Data-flow constraints the example must respect

- Prompt and schema paths are already resolved relative to the YAML and
  digested into the snapshot. Missing prompt files block startup, including
  for retained snapshots; the terminal exception above does not relax that.
- Web fetch/extract chains must end in a projector inside their own `each`
  Stage, so the LLM goes in a separate Stage.
- The model receives only the Item payload, never `metadata.web`. For
  `llm_extract` after `to_markdown`, the payload is a Markdown **string**, so
  the user message is a JSON string. For `llm_summary`, the input is a JSON
  **array of upstream payloads**, i.e. the extract results such as
  `[{"summary": "..."}]`, not Markdown. Write the summary prompt against that
  array; its output `{summary: string}` is what `{summary}` renders. `llm_extract` returns `NEW(value)` without merging RSS
  fields, so downstream templates cannot assume `{title}` or `{link}`. Missing
  template keys render as empty strings (`_Values.__missing__`).
- `with.output` is the small shape language (`string`, `list[string]`,
  `object`, nested maps), not JSON Schema. The client sends no
  `response_format`, so the prompt must ask for bare JSON.

## Acceptance map

| ID | Behavior or invariant | Task | Check and expected result |
| --- | --- | --- | --- |
| A1 | `tick` runs LLM Stages from YAML with no injection | T1 | Integration test: localhost `ThreadingHTTPServer` fake; `cli.main(["tick", def, "--at", ...])`; LLM Stage Executions `succeeded`; final Delivery payload equals the expected rendered text |
| A2 | Request uses configured endpoint, Task model and env key | T1 | Fake records path `/v1/chat/completions` (base_url `http://[IP]:PORT/v1`), body `model` from Task, `Authorization: Bearer <env value>` |
| A3 | Current LLM Task without client fails at startup | T1 | `open_app` raises `DefinitionError`; DB file not created |
| A4 | Config validation | T1 | `load_definition` rejects literal `api_key`, unknown keys, URL with userinfo/query, bool/zero `timeout_seconds`, `max_attempts: 0`; unset env var → `open_app` `DefinitionError` |
| A5 | Injected client wins | T1 | `open_app(..., llm_client=fake)` with an `llm` block whose env var is unset opens and uses the fake |
| A6 | Retained rules | T1 | Terminal-only LLM history + no `llm` block → App opens; rebuild of it → `DefinitionError`, `executions` row count unchanged. Queued LLM snapshot with retries remaining + no client → `DefinitionError` at open. Waiter-only LLM snapshot + no client → `DefinitionError` at open |
| A7 | No vendor body or key in persisted errors | T1 | CLI integration: fake returns HTTP 400 with a sentinel in the body; Execution `failed`, `executions.error` lacks the sentinel (the CLI returns 0; Stage errors are persisted, not raised). Unit (`tests/unit/test_openai_client.py`): calling the client directly raises `ExecutionError` containing `400` but not the sentinel |
| A8 | D1 identity stability | T1 | `load_definition` twice with different `llm.base_url`/env value → identical canonical config JSON and `node_binding_digest` per Stage; `stage_lookup_key(stage, [fixed synthetic Item], [])` identical. No tick (an unchanged Source produces no new work, so a tick-based check is vacuous). |
| A9 | Existing behavior unchanged | T1 | Full suite passes; no count hardcoded |
| A10 | Documented example runs | T2 | `load_definition("examples/rss-llm-digest.yml")` succeeds offline; with a real endpoint, `tick` records Stage and Delivery `succeeded` in SQLite, or A10 is recorded as not run |

## Execution

Run tasks in order; verify, inspect the diff, commit, then update this plan.

### T1: Build and guard the LLM client from the definition

Status: done
Depends on: none
Acceptance: A1–A9
Targets: `src/sluice/config.py` (`_TOP_LEVEL_KEYS`, `_decode_definition`),
`src/sluice/contracts.py` (`Definition`, new `LLMConfig`), `src/sluice/app.py`
(`open_app`, `App` availability flag), `src/sluice/stage_runtime.py`
(`rebuild_stage` retained-Task loop), `src/sluice/llm/openai_compatible.py`
(error messages, `api_key` repr), tests in `tests/integration/` and
`tests/unit/test_openai_client.py`
Contracts: `llm` block and availability table above; `open_app` signature
unchanged

- [ ] Decode and validate `llm` into `Definition.llm`.
- [ ] Construct or select the client in `open_app` before freezing adapters;
      apply the current/retained availability rules.
- [ ] Add the rebuild check before any persisted write.
- [ ] Strip vendor bodies from client errors; `api_key` `repr=False`.
- [ ] Tests: one new integration module driving `cli.main` against a
      localhost fake (A1, A2, A7, A8), parameterized rejection cases (A3–A6).
      Reuse `source_collect` / `sink_callable` / YAML patterns from
      `tests/integration/test_llm_batch_rebuild.py`; no HTTP fake helper
      exists in `tests/` yet, so the fake lives in the new module. Clear proxy
      env (`NO_PROXY=*`) inside the test so urllib reaches localhost. Use
      `batch: {count: 1}` for the summary node so one tick yields a Delivery.
      These tests earn their keep: they are the only coverage of the
      production construction path and the secret boundary.
- [ ] Verify in `../sluice-pipeline-v4`: `.venv/bin/ruff check .` → all checks
      passed; `.venv/bin/pytest -p no:cacheprovider -q tests` → 0 failures.
- [ ] Inspect the diff: no provider abstraction, no fallback chain, no new
      dependency, no snapshot format change.
- [ ] Commit, ideally split as `fix(llm): keep vendor bodies out of execution
      errors` and `feat(llm): build OpenAI-compatible client from definition`.

Evidence:

### T2: Runnable LLM digest example and docs

Status: done
Depends on: T1
Acceptance: A10
Targets: new `examples/rss-llm-digest.yml`, new `examples/prompts/extract.md`
and `examples/prompts/summary.md`, `README.md` (new "Summarize with an LLM"
section), link from `docs/notifications.md`

- [ ] Example stages: `article` (`each`: `fetch_webpage` → `extract_content`
      → `to_markdown`) → `extract` (`each`: `llm_extract`,
      `output: {summary: string}`, `invalid: drop`) → `digest` (`batch`:
      `llm_summary`, `output: {summary: string}`, node `batch: {count: 1}` or
      a documented schedule) → `message` (`each`: `render_telegram`,
      `template: "{summary}"`) → console Sink `examples.console.send`. Prompts
      ask for bare JSON with exactly the declared fields. Explain how to swap
      in Telegram/email/Feishu nodes.
- [ ] README: the `llm` block, `LLM_API_KEY`, that the endpoint is not part of
      cache identity (use `rebuild` or `cache_version` to recompute), that the
      model sees only the projected text, and the `PYTHONPATH`/`--at`/SQLite
      inspection pattern already used for the webpage example.
- [ ] Verify offline: `.venv/bin/python -c "from sluice.config import
      load_definition; load_definition('examples/rss-llm-digest.yml')"` → no
      error. Online with credentials: `tick` and query `executions` /
      `deliveries` → `succeeded`. Without credentials, record A10 online as
      not run.
- [ ] Commit: `docs: add RSS LLM digest example`.

Evidence:

## Final acceptance

A1–A9 from the T1 suite run. A10 from the offline load plus, when credentials
are available, one `tick` of the example verified through SQLite state rather
than the exit code.

## Progress and handoff

T1 done (commits 342727d, 242d7c2); T2 done (docs commit).

- T1 tests: `tests/integration/test_llm_runtime_wiring.py` (A1-A8) and
  `test_openai_client_error_message_omits_vendor_response_body` (A7 unit).
  Without the src changes, 10 of the 22 new/touched tests fail.
- A9: full suite 435 passed, 1 failed:
  `tests/smoke/test_prefect_parity.py` needs the optional `prefect` extra,
  which is not installed in this venv. It fails identically on the base
  commit.
- A10 offline: `load_definition("examples/rss-llm-digest.yml")` succeeds.
  A10 online (real endpoint, `tick`, SQLite states): not run, no credentials.
- Environment: `.venv/bin/python` is a dangling symlink (Homebrew Python 3.14
  removed), so tests ran as `PYTHONPATH=.venv/lib/python3.14/site-packages
  /usr/bin/python3 -m pytest ...` (Python 3.12). `ruff format --check .`
  already reports `src/sluice/web/projectors.py` and 4 other files on the
  base commit; it is not a gate here.

Verified facts the plan relies on:
- Structured-output validation and `_decode_completion` errors carry no model
  text; HTTP body redaction is the only persistence leak in scope.
- `verify_execution_envelope` only reads, so the rebuild check placement holds.
- `tick` runs `_drain`, a fixed-point loop of up to 32 passes over all nodes,
  so Source → each → batch(count:1) → render → Delivery completes in one tick.
- `fetch_webpage` falls back from `url` to `link` (`tasks/content.py:_url`),
  so an RSS Item feeds T2's `article` Stage directly.
- `recover_startup` calls `_fail_execution` without `supersede`; retries
  requeue when `retry_count < policy.retries`.

Next: T1.
