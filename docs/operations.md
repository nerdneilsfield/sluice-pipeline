# Operate Sluice

`sluice` is a one-shot runner, not a background scheduler. Each command opens the definition, does its work, and exits. This page covers scheduling, the execution commands, the management UI, Prefect, the database lock, and inspecting state.

## Run tick periodically

Invoke `tick` periodically through cron or an external scheduler. Supply an ISO-8601 timestamp with a timezone for every execution command. The `--at` value is required.

```sh
sluice tick definition.yml --at 2026-09-22T00:00:00Z
```

For a cron job, pass the current time instead of a fixed one, as in the README examples:

```sh
sluice tick definition.yml --at "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
```

Environment variables that a definition references with `env:NAME` must be available to the process that runs `tick`. Replace `definition.yml` with your definition path.

## Reconcile, rebuild, and retry a Delivery

```sh
sluice reconcile definition.yml pipeline_name node_name --at 2026-09-22T00:00:00Z
sluice rebuild definition.yml 42 --at 2026-09-22T00:00:00Z
sluice retry-delivery definition.yml DELIVERY_KEY --at 2026-09-22T00:00:00Z
```

Replace `definition.yml`, Pipeline/Node names, execution ID `42`, and `DELIVERY_KEY` with values from your deployment.

### Reconcile a node

`reconcile` limits the run to one Pipeline and a set of Nodes: the named Node, its ancestors, and every downstream Node that depends on any Node in that set. It requests Stage work and dispatches Deliveries for those Nodes, so reconciling an upstream Node can run downstream Stages and send real messages through their Sinks.

`reconcile` does not collect due Sources. It consumes Source publications that already exist, so run `tick` first, or let a scheduled `tick` collect, when you need fresh input. `tick` scans the whole definition and also collects due Sources.

### Rebuild a Stage execution

`rebuild` forces a Stage materialization from one retained Stage execution, identified by its execution ID. A rebuild uses the retained execution input and snapshot. If the Stage's LLM Tasks need a client, the definition must still provide one; see [LLM startup rules](llm.md#startup-rules).

### Retry a Delivery

`retry-delivery` retries one failed Delivery without requesting any upstream work. Retrying a Delivery can repeat an external side effect if the Sink did not honor the stable Delivery key. See [Limits and retry behavior](notifications.md#limits-and-retry-behavior) for the built-in senders.

## Management UI

```sh
sluice ui definition.yml --host 127.0.0.1 --port 8000
```

The `ui` extra provides the management interface at `http://127.0.0.1:8000/pipelines`.

> **Warning:** The UI has mutation actions and no authentication. Keep it on a trusted local interface.

The UI refuses requests whose `Host` header is not the bound address with the bound port (for a loopback bind: `localhost`, `127.0.0.1` or `[::1]`), which blocks DNS rebinding. It also refuses cross-site `POST` requests: `Sec-Fetch-Site` must be `same-origin` or `none` and `Origin`, when present, must match the request's own origin. Rejected requests get HTTP 403. This is not authentication; binding a non-loopback `--host` still exposes the actions to anyone who can reach it.

A wildcard bind (`--host 0.0.0.0` or `--host ::`, as used when the UI runs in a Docker container) has no single name to match, so the `Host` check is skipped and DNS-rebinding protection is given up; the `POST` checks still apply. A wildcard bind is not authentication either: keep it on trusted networks.

Stop the UI before running another process against the same database; see [Database lock](#database-lock).

## Prefect

With the `prefect` extra installed, call the same runtime through:

```python
from sluice.prefect_flow import prefect_tick

prefect_tick('/absolute/path/definition.yml', '/absolute/path/state.db')
```

To install the extras from PyPI, run `pip install 'sluice-pipeline[web,ui,prefect]'`. From a checkout, run `pip install '.[web,ui,prefect]'` from the repository root, or pass the checkout path instead of `.`. Omit extras you do not use.

## Database lock

The runtime uses `fcntl` file locking and targets Unix-like systems. Only one App can own a database at a time, so a second process opened against the same database cannot run while the first holds it. This applies to `tick`, the other execution commands, the UI, and `prefect_tick` alike.

Use a separate database for v4 rather than pointing it at an older Sluice deployment.

## Inspect state in SQLite

Check recorded states instead of relying on the CLI exit status. The CLI can return normally after recording an execution failure. The examples use the webpage example's database; replace the path with your `database` value, which is relative to the YAML file.

```sh
.venv/bin/python - <<'PY'
import sqlite3
with sqlite3.connect('examples/webpage.db') as db:
    print(db.execute('SELECT kind, state, error FROM executions').fetchall())
    print(db.execute('SELECT state FROM deliveries').fetchall())
PY
```

On a successful first run, Source, Stage, and Delivery states are `succeeded`. The Source has a one-hour schedule in the examples; an immediate second tick need not fetch again. Unchanged content does not create another delivery.

If recording a final state fails (for example a SQLite I/O error), `tick` fails with that error. Sluice first tries to mark a Delivery as `failed` with the error `delivery completion failed`, and a Source attempt as `failed` with `Source finalization failed`. If that write fails too, the attempt stays `running`, the error is raised, and a Delivery also logs a `delivery_finalize_failed` event. A `running` row left this way is recovered the next time the database is opened. A Delivery marked `failed` after the Sink already accepted the message can be sent again on retry.

For per-destination Delivery state in the RSS notification example, see [Run and inspect](notifications.md#run-and-inspect).
