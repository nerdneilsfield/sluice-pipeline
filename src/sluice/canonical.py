from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from collections.abc import Mapping, Sequence
from typing import Any

from .contracts import Item
from .errors import ExecutionError


def utc_datetime(value: datetime | str) -> datetime:
    """Return *value* as an aware UTC datetime.

    Naive datetimes are interpreted as UTC. RFC3339 ``Z`` text is accepted in
    addition to the forms understood by :meth:`datetime.fromisoformat`.
    """

    if isinstance(value, str):
        text = value.strip()
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        try:
            value = datetime.fromisoformat(text)
        except (TypeError, ValueError) as exc:
            raise ExecutionError(f"invalid timestamp: {value!r}") from exc
    if not isinstance(value, datetime):
        raise ExecutionError(f"invalid timestamp: {value!r}")
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _normalize(value: Any) -> Any:
    """Convert immutable Item views and timestamps into JSON-compatible data."""

    if isinstance(value, datetime):
        normalized = utc_datetime(value)
        text = normalized.isoformat().replace("+00:00", "Z")
        return text
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise ExecutionError(f"JSON object keys must be strings: {key!r}")
            result[key] = _normalize(item)
        return result
    if isinstance(value, (list, tuple)):
        return [_normalize(item) for item in value]
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    raise ExecutionError(f"value is not JSON-compatible: {value!r}")


def canonical_json(value: Any) -> str:
    """Serialize a JSON-compatible value deterministically."""

    try:
        normalized = _normalize(value)
        return json.dumps(
            normalized,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
    except ExecutionError:
        raise
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise ExecutionError("value cannot be represented as canonical JSON") from exc


def digest(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def content_hash(payload: Any) -> str:
    return digest(payload)


def make_item_id(
    producer_scope: tuple[str, str],
    logical_id: str,
    payload: Any,
    metadata: Mapping[str, Any],
) -> str:
    if not isinstance(logical_id, str) or not logical_id:
        raise ExecutionError("logical_id must be a non-empty string")
    if not isinstance(metadata, Mapping):
        raise ExecutionError("metadata must be a JSON object")
    if (
        not isinstance(producer_scope, tuple)
        or len(producer_scope) != 2
        or producer_scope[0] not in ("source", "stage")
        or not isinstance(producer_scope[1], str)
        or not producer_scope[1]
    ):
        raise ExecutionError("producer_scope must be ('source'|'stage', non-empty name)")
    return digest(
        {
            "producer_scope": producer_scope,
            "logical_id": logical_id,
            "payload": payload,
            "metadata": metadata,
        }
    )


def load_item(row: Mapping[str, Any] | Item) -> Item:
    """Build a deeply immutable :class:`Item` from an item database row."""

    if isinstance(row, Item):
        return row
    try:
        producer_scope = (row["producer_kind"], row["producer_name"])
        payload = row["payload_json"]
        metadata = row["metadata_json"]
        if isinstance(payload, (str, bytes, bytearray)):
            payload = json.loads(payload)
        if isinstance(metadata, (str, bytes, bytearray)):
            metadata = json.loads(metadata)
        return Item(
            id=row["id"],
            producer_scope=producer_scope,
            logical_id=row["logical_id"],
            payload=payload,
            metadata=metadata,
            content_hash=row["content_hash"],
            observed_at=row["observed_at"],
            produced_by=row["produced_by"],
        )
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ExecutionError("invalid persisted Item row") from exc
