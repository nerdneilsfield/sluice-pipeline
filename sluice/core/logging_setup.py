"""Loguru-backed logging setup for CLI and pipeline diagnostics."""

from __future__ import annotations

import logging
import os
import sys
from collections.abc import Mapping
from datetime import datetime
from json import dumps
from pathlib import Path
from typing import Any, cast

from loguru import logger

_THIRD_PARTY_LOGGERS = [
    "aiosqlite",
    "httpx",
    "httpcore",
    "feedparser",
    "trafilatura",
    "prefect",
    "markdown_it",
]

logger.disable("sluice")


class _TqdmStderrSink:
    """Write through tqdm when a progress bar is active."""

    @staticmethod
    def _target():
        stream = sys.stderr
        if stream is None or getattr(stream, "closed", False):
            return sys.__stderr__
        return stream

    @staticmethod
    def _tqdm_active() -> bool:
        try:
            from tqdm import tqdm

            return len(getattr(tqdm, "_instances", set())) > 0
        except ImportError:
            return False

    def write(self, message: str) -> int:
        if self._tqdm_active():
            from tqdm import tqdm

            tqdm.write(message, file=self._target(), end="")
            return len(message)
        return self._target().write(message)

    def flush(self) -> None:
        self._target().flush()


_CONSOLE_SINK = _TqdmStderrSink()
_CONTEXT_SKIP_KEYS = {"component", "_console_context"}
_CONTEXT_KEY_ORDER = [
    "pipeline_id",
    "run_key",
    "run_date",
    "source",
    "source_id",
    "stage",
    "sink_id",
    "sink_type",
    "model",
    "fetcher",
    "url",
    "items_in",
    "items_out",
    "total_items",
    "entries",
    "emitted",
    "skipped_future",
    "skipped_window",
    "fetched",
    "used_existing",
    "empty",
    "failed",
    "chars",
    "min_chars",
    "prompt_chars",
    "projected_calls",
    "projected_usd",
    "attempts",
    "details",
    "error_class",
    "error",
]


def _write_console(message: Any) -> None:
    _CONSOLE_SINK.write(str(message))
    _CONSOLE_SINK.flush()


def _stringify_context_value(value: Any) -> str:
    if isinstance(value, str):
        rendered = value
    elif isinstance(value, Mapping | list | tuple):
        rendered = dumps(value, ensure_ascii=False, default=str)
    else:
        rendered = str(value)
    rendered = rendered.replace("\n", "\\n")
    if len(rendered) > 180:
        return rendered[:177] + "..."
    return rendered


def _format_context(extra: dict[str, Any]) -> str:
    keys = [key for key in _CONTEXT_KEY_ORDER if key in extra]
    keys.extend(sorted(k for k in extra if k not in _CONTEXT_SKIP_KEYS and k not in keys))
    parts = []
    for key in keys:
        if key in _CONTEXT_SKIP_KEYS:
            continue
        value = extra.get(key)
        if value is None:
            continue
        parts.append(f"{key}={_stringify_context_value(value)}")
    if not parts:
        return ""
    return " | " + " ".join(parts)


def _console_format(record: dict[str, Any]) -> str:
    extra = record["extra"]
    extra.setdefault("component", "sluice")
    extra["_console_context"] = _format_context(extra)
    return (
        "<green>{time:HH:mm:ss.SSS}</green> "
        "<level>{level:<8}</level> "
        "<cyan>{extra[component]}</cyan> "
        "<level>{message}</level>"
        "<level>{extra[_console_context]}</level>\n"
        "{exception}"
    )


class _InterceptHandler(logging.Handler):
    """Route stdlib logging records into loguru."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            level: str | int = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno
        logger.bind(component=record.name).opt(depth=6, exception=record.exc_info).log(
            level, record.getMessage()
        )


class SluiceLogger:
    """Small CLI facade matching the CodeWiki-style logger shape."""

    def __init__(self, verbose: bool = False, name: str = "sluice.cli"):
        self.verbose = verbose
        self.start_time = datetime.now()
        self._logger = get_logger(name)

    def bind(self, **extra: Any):
        return self._logger.bind(**extra)

    def debug(self, message: str) -> None:
        if self.verbose:
            self._logger.debug(message)

    def info(self, message: str) -> None:
        self._logger.info(message)

    def success(self, message: str) -> None:
        self._logger.info(f"SUCCESS {message}")

    def warning(self, message: str) -> None:
        self._logger.warning(message)

    def error(self, message: str) -> None:
        self._logger.error(message)

    def step(self, message: str, step: int | None = None, total: int | None = None) -> None:
        prefix = f"[{step}/{total}] " if step is not None and total is not None else ""
        self._logger.info(f"{prefix}{message}")

    def elapsed_time(self) -> str:
        elapsed = datetime.now() - self.start_time
        seconds = int(elapsed.total_seconds())
        minutes, seconds = divmod(seconds, 60)
        if minutes:
            return f"{minutes}m {seconds}s"
        return f"{seconds}s"


def get_logger(name: str = "sluice"):
    """Return a loguru logger bound with a component name."""

    return logger.bind(component=name)


def create_logger(verbose: bool = False, name: str = "sluice.cli") -> SluiceLogger:
    return SluiceLogger(verbose=verbose, name=name)


def configure_cli_logging(*, verbose: bool = False, log_file: str | Path | None = None) -> None:
    """Configure console and optional JSONL file logging for CLI runs."""

    logger.remove()
    logger.enable("sluice")
    console_level = "DEBUG" if verbose else "INFO"
    logger.add(
        cast(Any, _write_console),
        level=console_level,
        colorize=True,
        backtrace=verbose,
        diagnose=verbose,
        format=cast(Any, _console_format),
    )

    target = str(log_file or os.environ.get("SLUICE_LOG_FILE", "")).strip()
    if target:
        path = Path(target).expanduser()
        path.parent.mkdir(parents=True, exist_ok=True)
        logger.add(
            path,
            level="DEBUG",
            serialize=True,
            enqueue=True,
            backtrace=True,
            diagnose=False,
            rotation="20 MB",
            retention=5,
        )

    logging.basicConfig(handlers=[_InterceptHandler()], level=0, force=True)
    logging.getLogger("sluice").setLevel(logging.DEBUG)
    for name in _THIRD_PARTY_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING)
