# Contributing to Sluice

## Set up

Use Python 3.12 or later and [uv](https://docs.astral.sh/uv/). Run this command from the repository root:

```sh
uv sync --locked --extra dev --extra web --extra ui --extra prefect
```

## Verify changes

After syncing the extras above, run:

```sh
.venv/bin/ruff check .
.venv/bin/pytest -p no:cacheprovider -q tests
.venv/bin/python -m compileall -q src tests examples
uv pip check
git diff --check
```

The suite uses fake provider transports; the Prefect parity test starts a temporary local server. It does not validate paid providers against live vendor accounts.

See [v4 verification](docs/v4-verification.md) for the checked scope and remaining limits.

### Expected warnings

A newspaper4k warning about missing `nltk` is expected: its optional NLP features are not required for content extraction. Prefect may log `ValueError: I/O operation on closed file` while stopping its temporary server after pytest exits. Treat that specific teardown message as non-test output only if pytest reports all tests passing and exits zero.

## Continuous integration

The GitHub Actions workflow runs Ruff and the suite with the same locked extras on Python 3.12, 3.13, and 3.14. `ty` cleanup is deferred and is not a CI gate.

## Design specs

Configuration and extension contracts are documented in the [core design](docs/superpowers/specs/2026-08-14-sluice-v4-cleanroom-design.md) and [web design](docs/superpowers/specs/2026-08-18-sluice-v4-web-content-pipeline-design.md). Custom dotted-path callables must be importable when the App starts.

## Build and publish

The distribution is named `sluice-pipeline`; the Python import and command are
`sluice`. Build locally with `uv build` or build the container with:

```sh
docker build -f scripts/docker/Dockerfile -t sluice-pipeline:local .
docker run --rm sluice-pipeline:local --help
```

The container includes the `web`, `ui`, and `prefect` runtime extras, not dev/test
tools. Mount definitions and writable state directories when running a pipeline.

The Docker and PyPI workflows run when a `v*.*.*` tag is pushed and require the
tag version to match `pyproject.toml`. Pushing a release tag triggers real
uploads to these destinations:

- PyPI: `sluice-pipeline`, using trusted publishing in the `pypi` GitHub environment.
- GHCR: `ghcr.io/nerdneilsfield/sluice-pipeline`, using `GITHUB_TOKEN`.
- Docker Hub: `nerdneils/sluice-pipeline`, using the `DOCKERHUB_TOKEN` secret.

Docker publishes Linux AMD64 and ARM64 images tagged with the full version, the
minor version, the major version, and `latest`. Local builds do not verify PyPI
trust configuration or registry permissions.

v0.4.0 was published through these workflows: the wheel and sdist are on PyPI,
and both image registries carry `0.4.0`, `0.4`, `0`, and `latest` for both
architectures.
