# v4 verification

## Checked scope

The `v4-cleanroom` implementation includes the core runtime and all twelve Web implementation tasks. The historical plans contain development-time red/green and review checklists; their unchecked boxes are not an inventory of missing code. This record describes the final checks actually rerun, rather than asserting that every historical process step took place.

The closing changes fix `html-to-markdown` result handling and literal-text escaping, finish Ruff cleanup while retaining durable failure boundaries, and add a runnable example, README, and CI workflow. A subsequent notification pass adds default Telegram, SMTP email, and signed Feishu webhook senders plus the RSS notification example. The full-suite results below include those sender tests. Type-checker (`ty`) cleanup is deferred by request.

## Results

Checks used the committed `uv.lock` with `dev`, `web`, `ui`, and `prefect` extras on Linux.

| Check | Observed result |
| --- | --- |
| Full pytest suite, Python 3.14 | 402 passed, 1 warning; exit 0 |
| Full pytest suite, Python 3.12.3 | 402 passed, 1 warning; exit 0 |
| `ruff check .` | All checks passed |
| `python -m compileall -q src tests examples` | Exit 0 |
| `uv pip check` | Installed packages compatible |
| `git diff --check` | Exit 0 |
| Fresh SQLite schema initialized twice | Both calls succeeded |
| Fresh schema `PRAGMA foreign_key_check` | No rows |
| Fresh schema `PRAGMA integrity_check` | `ok` |
| CLI `sluice --help` | Command parser available |
| CLI tick with the explicit Web example | Source and Stage succeeded; console Delivery succeeded |

The CLI check used a temporary copy of `examples/webpage.yml`, a fresh SQLite database, and a real HTTPX request to `https://example.com/`. The console Sink printed the extracted text and Delivery key. Execution and Delivery states were queried from SQLite; success was not inferred solely from process exit status.

The full suite includes management actions, Prefect/local parity, retained Web bindings, manual rebuild, shared materialization, cancellation and deadline handling, and provider safety regressions. Provider tests use fake transports; they do not demonstrate live paid-vendor interoperability.

## Reproduce the schema check

After installing the extras listed in the README:

```sh
.venv/bin/python - <<'PY'
from pathlib import Path
from tempfile import TemporaryDirectory
from sluice.db import init_schema, open_database

with TemporaryDirectory() as directory:
    db = open_database(Path(directory) / 'state.db')
    try:
        init_schema(db)
        init_schema(db)
        assert db.execute('PRAGMA foreign_key_check').fetchall() == []
        assert db.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
    finally:
        db.close()
print('schema checks passed')
PY
```

The README contains the full-suite and CLI commands. For a fresh CLI check, copy the example YAML to a temporary directory, keep `PYTHONPATH` set to the repository root, and run `tick` against that copy. The database is created beside the copied definition.

## Limits and integration status

- newspaper4k warns that optional `nltk` support is absent; extraction tests pass without it.
- Prefect can emit a closed-console logging traceback during temporary-server shutdown after pytest completes. Both full-suite runs exited zero.
- GitHub Actions is configured for Python 3.12 and 3.14, but has not been run remotely during this local work.
- No live email, Telegram, LLM, or paid Web provider credentials were used.
- No independent review round was launched during this closing pass.
- `.superpowers` is ignored and absent from the current tracked tree, but older reachable commits still contain it. The historical plan's history-cleanliness condition is not met; removing those records would require a separately authorized history rewrite.
- The work remains on `v4-cleanroom`. Merge, push, and release are separate actions and have not been performed.
