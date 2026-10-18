"""One-shot local execution entrypoints for the Sluice runtime.

The CLI is deliberately a small argument parser around the ordinary runtime
functions.  It does not own scheduling, retries, or any other durable state
machine; those semantics remain in :mod:`sluice.reconcile`,
:mod:`sluice.stage_runtime`, and :mod:`sluice.delivery`.
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from datetime import datetime

from .app import open_app
from .canonical import utc_datetime
from .delivery import retry_delivery
from .reconcile import reconcile, tick
from .stage_runtime import rebuild_stage


def _timestamp(value: str) -> datetime:
    """Parse one documented ISO-8601 timestamp and normalize it to UTC."""

    try:
        return utc_datetime(value)
    except Exception as error:
        raise argparse.ArgumentTypeError(str(error)) from error


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="sluice")
    commands = parser.add_subparsers(dest="command", required=True)

    tick_parser = commands.add_parser("tick")
    tick_parser.add_argument("definition")
    tick_parser.add_argument("--at", required=True, type=_timestamp)

    reconcile_parser = commands.add_parser("reconcile")
    reconcile_parser.add_argument("definition")
    reconcile_parser.add_argument("pipeline")
    reconcile_parser.add_argument("node")
    reconcile_parser.add_argument("--at", required=True, type=_timestamp)

    rebuild_parser = commands.add_parser("rebuild")
    rebuild_parser.add_argument("definition")
    rebuild_parser.add_argument("execution_id", type=int)
    rebuild_parser.add_argument("--at", required=True, type=_timestamp)

    retry_parser = commands.add_parser("retry-delivery")
    retry_parser.add_argument("definition")
    retry_parser.add_argument("delivery_key")
    retry_parser.add_argument("--at", required=True, type=_timestamp)

    ui_parser = commands.add_parser("ui")
    ui_parser.add_argument("definition")
    ui_parser.add_argument("--host", default="127.0.0.1")
    ui_parser.add_argument("--port", default=8000, type=int)

    return parser


def _close_app(app: object) -> None:
    """Close one App through its admission-aware lifecycle."""

    close = getattr(app, "close", None)
    if callable(close):
        close()


def main(argv: Sequence[str] | None = None) -> int:
    """Run one documented local command and return its process status."""

    args = _parser().parse_args(argv)
    app = open_app(args.definition)
    try:
        if args.command == "tick":
            tick(app, args.at)
        elif args.command == "reconcile":
            reconcile(app, args.pipeline, args.node, args.at)
        elif args.command == "rebuild":
            rebuild_stage(app, args.execution_id, args.at)
        elif args.command == "retry-delivery":
            retry_delivery(app, args.delivery_key, args.at)
        elif args.command == "ui":
            # Keep Flask optional for every non-UI command, including parser
            # construction and ``--help``.
            from .ui import create_ui

            create_ui(app, host=args.host, port=args.port).run(
                host=args.host, port=args.port
            )
        else:  # pragma: no cover - argparse constrains command values.
            raise AssertionError(f"unknown command: {args.command!r}")
    finally:
        _close_app(app)
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised by the script entrypoint.
    raise SystemExit(main())


__all__ = ["main"]
