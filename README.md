# Sluice v4

Sluice collects RSS and webpage content, runs YAML-defined task graphs, and delivers results through independently retried Sinks. SQLite stores publications, execution state, shared Stage results, and Delivery attempts. Local commands and the optional Prefect flow use the same runtime.

This branch is the v4 clean-room implementation. Use a separate database rather than pointing it at an older Sluice deployment. The runtime uses `fcntl` file locking and targets Unix-like systems; only one App can own a database at a time.

## Run the webpage example

Use Python 3.12 or later and [uv](https://docs.astral.sh/uv/). Run these commands from the repository root:

```sh
uv sync --locked --extra dev --extra web --extra ui --extra prefect
PYTHONPATH="$PWD" .venv/bin/sluice tick examples/webpage.yml \
  --at "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
```

The example fetches `https://example.com/`, extracts visible content, converts it to text, and prints a JSON object containing `delivery_key` and `payload`. It does not send email or Telegram messages. Internet access is required. `PYTHONPATH` makes the example's custom Sink, `examples.console.send`, importable; it is not needed for definitions using only installed built-ins.

State is stored in `examples/webpage.db`, relative to the YAML file. To inspect the result without relying on the CLI exit status:

```sh
.venv/bin/python - <<'PY'
import sqlite3
with sqlite3.connect('examples/webpage.db') as db:
    print(db.execute('SELECT kind, state, error FROM executions').fetchall())
    print(db.execute('SELECT state FROM deliveries').fetchall())
PY
```

On a successful first run, Source, Stage, and Delivery states are `succeeded`. The CLI can return normally after recording an execution failure, so check these states when diagnosing a run. The Source has a one-hour schedule; an immediate second tick need not fetch again. Unchanged content does not create another delivery.

## Configure a pipeline

[examples/webpage.yml](examples/webpage.yml) contains an explicit HTTPX provider, fetch chain, extractor chain, Source, Stage, and console Delivery. YAML `version: 1` is the definition format, not the package major version.

- `sources` collect immutable Items and publish changes. Built-ins include `sluice.sources.rss` and `sluice.sources.webpage`.
- `stages` run `each` or `batch` task chains. Web fetch Tasks produce transient artifacts; end their chain with `sluice.tasks.to_markdown` or `sluice.tasks.to_text` before materialization.
- `pipelines` connect Sources and Stages through `from` edges. Every Pipeline needs a Delivery endpoint.
- `web` declares named providers and ordered fallback chains. HTTPX and BeautifulSoup work with core dependencies; the `web` extra adds curl_cffi, trafilatura, newspaper4k, and resiliparse.

Direct web providers reject private and loopback targets to prevent SSRF. Hosted service endpoints have a separate policy. Keep credentials out of YAML values; provider credential fields accept environment references such as `api_key: env:FIRECRAWL_API_KEY`. Changing a secret's value does not change the authored binding identity.

For Crawl4AI, use the [0.9.x configuration guide](docs/crawl4ai.md); older API formats are not supported.

Configuration and extension contracts are documented in the [core design](docs/superpowers/specs/2026-08-14-sluice-v4-cleanroom-design.md) and [web design](docs/superpowers/specs/2026-08-18-sluice-v4-web-content-pipeline-design.md). Custom dotted-path callables must be importable when the App starts.

## Send RSS notifications

RSS 2.0, Atom, and RSS 1.0 feeds can deliver to Telegram, SMTP email, and Feishu custom bots using built-in senders. See [notification setup](docs/notifications.md) and [the three-channel RSS example](examples/rss-notifications.yml). This example sends real messages and needs your credentials; the webpage example above only prints to the console.

## Summarize with an LLM

[examples/rss-llm-digest.yml](examples/rss-llm-digest.yml) runs RSS → fetch → `llm_extract` → `llm_summary` → console. The top-level `llm` block configures one OpenAI-compatible `/chat/completions` endpoint; `tick` and `prefect_tick` build the client from it at startup.

```yaml
llm:
  base_url: https://api.example.com/v1   # http(s) with a host; no userinfo, query or fragment
  api_key: env:LLM_API_KEY               # optional; exact env:NAME only, literal keys are rejected
  timeout_seconds: 60                    # optional, finite and > 0
  max_attempts: 3                        # optional, integer >= 1
```

```sh
export LLM_API_KEY=...   # edit base_url and model in the YAML for your provider
PYTHONPATH="$PWD" .venv/bin/sluice tick examples/rss-llm-digest.yml \
  --at "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
```

Inspect `executions` and `deliveries` in `examples/rss-llm-digest.db` as in the webpage example; Stage and Delivery states should be `succeeded`.

- An unset `LLM_API_KEY` fails startup, even if no Stage uses an LLM Task. An empty value is accepted and sends no `Authorization` header.
- A definition that uses `llm_extract` or `llm_summary` without an `llm` block fails at startup. Retained queued, running or waiting LLM work also needs a client; terminal history opens without one, but `rebuild` of it fails.
- The endpoint is not part of cache identity. Changing `base_url` or the key reuses existing results for the same model, prompt and output shape. Use `rebuild` or bump the Stage `cache_version` to recompute.
- The model sees only the projected Item payload: Markdown text for `llm_extract`, and a JSON array of the extract results for `llm_summary`. Title, link and `metadata.web` are not sent, so templates after `llm_extract` cannot use `{title}` or `{link}`.
- The client sends no `response_format`; the prompts ask for bare JSON.
- Failed LLM calls store only the HTTP status in `executions.error`, never the vendor response body.
- To send the digest to Telegram, email or Feishu instead of the console, replace the `console` node with the Sink from [notification setup](docs/notifications.md).

## Run and manage executions

`sluice` is a one-shot runner, not a background scheduler. Invoke `tick` periodically through cron or an external scheduler. Supply an ISO-8601 timestamp with a timezone for execution commands:

```sh
sluice tick definition.yml --at 2026-09-22T00:00:00Z
sluice reconcile definition.yml pipeline_name node_name --at 2026-09-22T00:00:00Z
sluice rebuild definition.yml 42 --at 2026-09-22T00:00:00Z
sluice retry-delivery definition.yml DELIVERY_KEY --at 2026-09-22T00:00:00Z
sluice ui definition.yml --host 127.0.0.1 --port 8000
```

Replace `definition.yml`, Pipeline/Node names, execution ID `42`, and `DELIVERY_KEY` with values from your deployment. A rebuild uses the retained execution input and snapshot. Retrying a Delivery can repeat an external side effect if the Sink did not honor the stable Delivery key.

The `ui` extra provides the management interface at `http://127.0.0.1:8000/pipelines`. Keep it on a trusted local interface: it has mutation actions and no authentication. Stop the UI before running another process against the same database.

With the `prefect` extra installed, call the same runtime through:

```python
from sluice.prefect_flow import prefect_tick

prefect_tick('/absolute/path/definition.yml', '/absolute/path/state.db')
```

For an installation outside the checkout, use `pip install '.[web,ui,prefect]'`. Omit extras you do not use. Email, Telegram, and LLM integrations need their own configuration and credentials; the console example does not exercise them.

## Verify changes

After syncing the extras above, run:

```sh
.venv/bin/ruff check .
.venv/bin/pytest -p no:cacheprovider -q tests
.venv/bin/python -m compileall -q src tests examples
uv pip check
git diff --check
```

The suite uses fake provider transports; the Prefect parity test starts a temporary local server. It does not validate paid providers against live vendor accounts. The GitHub Actions workflow runs Ruff and the suite with the same locked extras on Python 3.12, 3.13, and 3.14. `ty` cleanup is deferred and is not a CI gate.

A newspaper4k warning about missing `nltk` is expected: its optional NLP features are not required for content extraction. Prefect may log `ValueError: I/O operation on closed file` while stopping its temporary server after pytest exits. Treat that specific teardown message as non-test output only if pytest reports all tests passing and exits zero.

See [v4 verification](docs/v4-verification.md) for the checked scope and remaining limits.

## Build and publish

The distribution is named `sluice-pipeline`; the Python import and command are
`sluice`. Build locally with `uv build` or build the container with:

```sh
docker build -f scripts/docker/Dockerfile -t sluice-pipeline:local .
docker run --rm sluice-pipeline:local --help
```

The container includes the `web`, `ui`, and `prefect` runtime extras, not dev/test
tools. Mount definitions and writable state directories when running a pipeline.

The migrated Docker and PyPI workflows run when a `v*.*.*` tag is pushed and
require the tag version to match `pyproject.toml`. They retain the original
release destinations:

- PyPI: `sluice-pipeline`, using trusted publishing in the `pypi` GitHub environment.
- GHCR: `ghcr.io/nerdneilsfield/sluice-pipeline`, using `GITHUB_TOKEN`.
- Docker Hub: `nerdneils/sluice-pipeline`, using the `DOCKERHUB_TOKEN` secret.

Docker publishes Linux AMD64 and ARM64 images. These workflows retain the
repository's existing external credentials/environment requirements; local builds
do not verify PyPI trust configuration or registry permissions. Pushing a release
tag triggers real uploads. No release tag was pushed during migration.
