from __future__ import annotations

import logging

from sluice.canonical import canonical_json
from sluice.contracts import JSONValue


def log_event(logger: logging.Logger, event: str, **fields: JSONValue) -> None:
    """Write one deterministic structured event through standard logging."""

    logger.info(canonical_json({"event": event, **fields}))
