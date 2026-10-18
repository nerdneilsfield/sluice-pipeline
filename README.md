English | [简体中文](README.zh_CN.md)

# Sluice v4

[![CI](https://github.com/nerdneilsfield/sluice-pipeline/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/nerdneilsfield/sluice-pipeline/actions/workflows/ci.yml)
[![Docker](https://github.com/nerdneilsfield/sluice-pipeline/actions/workflows/docker.yml/badge.svg)](https://github.com/nerdneilsfield/sluice-pipeline/actions/workflows/docker.yml)
[![Publish to PyPI](https://github.com/nerdneilsfield/sluice-pipeline/actions/workflows/publish.yml/badge.svg)](https://github.com/nerdneilsfield/sluice-pipeline/actions/workflows/publish.yml)
[![PyPI](https://img.shields.io/pypi/v/sluice-pipeline)](https://pypi.org/project/sluice-pipeline/)
[![Python 3.12+](https://img.shields.io/badge/python-%3E%3D3.12-blue)](pyproject.toml)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

Sluice collects RSS and webpage content, runs YAML-defined task graphs, and delivers results through independently retried Sinks. SQLite stores publications, execution state, shared Stage results, and Delivery attempts. Local commands and the optional Prefect flow use the same runtime.

Typical uses are watching a webpage for changes, forwarding RSS entries to Telegram, email, or Feishu, and sending an LLM-written digest of a feed. Each use is a YAML definition that you run with the `sluice` command.

This branch is the v4 clean-room implementation. Use a separate database rather than pointing it at an older Sluice deployment. The runtime uses `fcntl` file locking and targets Unix-like systems; only one App can own a database at a time. See [Operate Sluice](docs/operations.md).

## Install

Sluice needs Python 3.12 or later. Choose one of the three routes below: pip for using Sluice, Docker for running it in a container, or source for development.

### With pip

This is the recommended route for users. The distribution `sluice-pipeline` is published on [PyPI](https://pypi.org/project/sluice-pipeline/); the Python import and command are `sluice`. Install it with the extras you need:

```sh
pip install 'sluice-pipeline[web,ui,prefect]'
sluice --help
```

Omit extras you do not use. Email, Telegram, and LLM integrations need their own configuration and credentials; the console example does not exercise them.

### With Docker

Published images are available for `linux/amd64` and `linux/arm64` from GitHub Container Registry (`ghcr.io/nerdneilsfield/sluice-pipeline`) and Docker Hub (`nerdneils/sluice-pipeline`). Both carry the tags `0.4.0`, `0.4`, `0`, and `latest`. Pull one and check that it starts:

```sh
docker pull ghcr.io/nerdneilsfield/sluice-pipeline:latest
docker run --rm ghcr.io/nerdneilsfield/sluice-pipeline:latest --help
```

To build the image from a checkout instead, run this from the repository root and use `sluice-pipeline:local` in place of the published image name below:

```sh
docker build -f scripts/docker/Dockerfile -t sluice-pipeline:local .
docker run --rm sluice-pipeline:local --help
```

The image runs `sluice` as its entrypoint from `/app` and includes the `web`, `ui`, and `prefect` runtime extras. The directories `/app/configs` and `/app/data` exist for definitions and state. The default `database` path is relative to the YAML file, so for a read-only definition mount, set an absolute `database` such as `/app/data/state.db` in the YAML:

```sh
docker run --rm \
  -v "$PWD/configs:/app/configs:ro" \
  -v "$PWD/data:/app/data" \
  ghcr.io/nerdneilsfield/sluice-pipeline:latest tick /app/configs/definition.yml \
  --at "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
```

Replace `configs/definition.yml` with your definition. The container runs as the unprivileged user `sluice` (uid/gid 10001), so a host directory mounted at `/app/data` must be writable by it: run `sudo chown -R 10001:10001 ./data` once, or pass `--user "$(id -u):$(id -g)"` to `docker run` to write as your own user. A definition that uses a custom Sink or other dotted-path callable needs that module importable inside the container, for example by mounting it under `/app/configs` and adding `-e PYTHONPATH=/app/configs`.

### From source

Use this route for development, or to run the bundled examples. Install [uv](https://docs.astral.sh/uv/), then run this command from the repository root:

```sh
uv sync --locked --extra dev --extra web --extra ui --extra prefect
```

The command installs `sluice` as `.venv/bin/sluice`. The examples below use that path.

## Run the webpage example

From the repository root, after installing from source:

```sh
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

## Pipeline concepts

[examples/webpage.yml](examples/webpage.yml) contains an explicit HTTPX provider, fetch chain, extractor chain, Source, Stage, and console Delivery. YAML `version: 1` is the definition format, not the package major version.

- `sources` collect immutable Items and publish changes. Built-ins are `sluice.sources.rss`, `webpage`, `json_api`, `hacker_news`, `github_trending`, and `github_activity`. Built-in Sinks include Telegram, email, Feishu, `sluice.sinks.webhook`, and `sluice.sinks.file`. Keys and rules: [Sources and Sinks reference](docs/sources-and-sinks.md).
- `stages` run `each` or `batch` task chains.
- `pipelines` connect Sources and Stages through `from` edges. Every Pipeline needs a Delivery endpoint.
- `web` declares named providers and ordered fallback chains. HTTPX and BeautifulSoup work with core dependencies; the `web` extra adds curl_cffi, trafilatura, newspaper4k, and resiliparse.

Direct web providers reject private and loopback targets to prevent SSRF. Hosted service endpoints have a separate policy. Keep credentials out of YAML values; provider credential fields accept environment references such as `api_key: env:FIRECRAWL_API_KEY`.

For Crawl4AI, use the [0.9.x configuration guide](docs/crawl4ai.md); older API formats are not supported.

Rules for task chains and transient artifacts, the authored binding identity, and the extension contracts, including dotted-path callable requirements, are in [Write a definition](docs/configuration.md).

## Scenario examples

**Webpage.** [examples/webpage.yml](examples/webpage.yml) fetches a page, extracts its text, and prints it through a console Sink. It is the example run above.

**RSS notifications.** RSS 2.0, Atom, and RSS 1.0 feeds can deliver to Telegram, SMTP email, and Feishu custom bots using built-in senders. See [notification setup](docs/notifications.md) and [the three-channel RSS example](examples/rss-notifications.yml). This example sends real messages and needs your credentials; the webpage example above only prints to the console.

**Hacker News to files.** [examples/hn-to-files.yml](examples/hn-to-files.yml) writes one text file per top story with the file Sink and needs no credentials. The first run emits up to `limit` stories. See the [Sources and Sinks reference](docs/sources-and-sinks.md).

**LLM digest.** [examples/rss-llm-digest.yml](examples/rss-llm-digest.yml) runs RSS → fetch → `llm_extract` → `llm_summary` → console, using one OpenAI-compatible `/chat/completions` endpoint configured in a top-level `llm` block. See [LLM digest setup](docs/llm.md).

## Common commands

`sluice` is a one-shot runner, not a background scheduler. Supply an ISO-8601 timestamp with a timezone for execution commands:

```sh
sluice tick definition.yml --at 2026-09-22T00:00:00Z
sluice reconcile definition.yml pipeline_name node_name --at 2026-09-22T00:00:00Z
sluice rebuild definition.yml 42 --at 2026-09-22T00:00:00Z
sluice retry-delivery definition.yml DELIVERY_KEY --at 2026-09-22T00:00:00Z
sluice ui definition.yml --host 127.0.0.1 --port 8000
```

Replace `definition.yml`, Pipeline/Node names, execution ID `42`, and `DELIVERY_KEY` with values from your deployment. Retrying a Delivery can repeat an external side effect if the Sink did not honor the stable Delivery key. The management UI has mutation actions and no authentication; keep it on a trusted local interface. It refuses cross-site requests and requests with a foreign `Host` header. A wildcard bind (`--host 0.0.0.0` or `::`, as in Docker) skips the `Host` check and so gives up DNS-rebinding protection; it is still not authentication, so keep it on trusted networks.

[Operate Sluice](docs/operations.md) covers scheduling `tick` with cron, rebuild and retry semantics, the UI, Prefect, the database lock, and inspecting state.

## Contributing

Contributions are welcome. [CONTRIBUTING.md](CONTRIBUTING.md) describes the development setup, the verification commands, the CI matrix, and the release process. [v4 verification](docs/v4-verification.md) records the checked scope and remaining limits.
