from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
from collections.abc import Mapping, Sequence
from typing import Any

from .contracts import BatchPolicy, DeliveryDef, Item, StageDef
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


def node_binding_digest(
    pipeline_name: str,
    node_name: str,
    stage: StageDef | None,
    from_refs: Sequence[str],
    schedule: timedelta | None,
    batch: BatchPolicy | None,
    delivery: DeliveryDef | None = None,
) -> str:
    """Return the immutable result-affecting identity of one Pipeline node.

    Execution policy (timeouts, retries, and concurrency) intentionally does
    not participate in this digest.  A Stage node snapshots its complete
    canonical Stage definition; a terminal Delivery node snapshots only its
    sink binding and uses ``stage=None`` rather than attempting a Stage lookup.
    """

    if not isinstance(pipeline_name, str) or not pipeline_name:
        raise ExecutionError("pipeline_name must be a non-empty string")
    if not isinstance(node_name, str) or not node_name:
        raise ExecutionError("node_name must be a non-empty string")
    if not isinstance(from_refs, Sequence) or isinstance(
        from_refs, (str, bytes, bytearray)
    ):
        raise ExecutionError("from_refs must be an ordered sequence")
    if any(not isinstance(ref, str) or not ref for ref in from_refs):
        raise ExecutionError("from_refs must contain non-empty strings")
    if stage is None and delivery is None:
        raise ExecutionError("node binding requires a Stage or Delivery")
    if stage is not None and not isinstance(stage, StageDef):
        raise ExecutionError("stage must be a StageDef or None")
    if delivery is not None and not isinstance(delivery, DeliveryDef):
        raise ExecutionError("delivery must be a DeliveryDef or None")
    if schedule is not None and (
        not isinstance(schedule, timedelta) or schedule <= timedelta(0)
    ):
        raise ExecutionError("schedule must be positive or None")
    if batch is not None and not isinstance(batch, BatchPolicy):
        raise ExecutionError("batch must be a BatchPolicy or None")

    stage_value: dict[str, Any] | None = None
    if stage is not None:
        stage_value = {
            "name": stage.name,
            "input_mode": stage.input_mode,
            "canonical_config_json": stage.canonical_config_json,
        }
    delivery_value: dict[str, Any] | None = None
    if delivery is not None:
        delivery_value = {
            "use": delivery.use,
            "with": delivery.with_config,
        }
    batch_value: dict[str, Any] | None = None
    if batch is not None:
        batch_value = {
            "count": batch.count,
            "max_wait_seconds": (
                None if batch.max_wait is None else batch.max_wait.total_seconds()
            ),
            "on_schedule": batch.on_schedule,
            "include_updates": batch.include_updates,
        }
    return digest(
        {
            "kind": "pipeline_node",
            "pipeline": pipeline_name,
            "node": node_name,
            "stage": stage_value,
            "from": list(from_refs),
            "schedule_seconds": (
                None if schedule is None else schedule.total_seconds()
            ),
            "batch": batch_value,
            "delivery": delivery_value,
        }
    )


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


def _item_snapshot(item: Item) -> dict[str, Any]:
    """Return every immutable value that can affect a Stage result."""

    if not isinstance(item, Item):
        raise ExecutionError("Stage inputs must contain Item values")
    return {
        "id": item.id,
        "producer_scope": item.producer_scope,
        "logical_id": item.logical_id,
        "payload": item.payload,
        "metadata": item.metadata,
        "content_hash": item.content_hash,
        "observed_at": item.observed_at,
        "produced_by": item.produced_by,
    }


def stage_lookup_key(
    stage: StageDef,
    items: Sequence[Item],
    history: Sequence[Item],
) -> str:
    """Hash the complete canonical Stage configuration and ordered inputs.

    The cache key intentionally includes immutable Item values rather than
    only their IDs.  This detects corruption where an ID is reused with a
    different payload or provenance while preserving deterministic equality
    for semantically identical input sequences.
    """

    if not isinstance(stage, StageDef):
        raise ExecutionError("stage_lookup_key requires a StageDef")
    if not isinstance(items, Sequence) or isinstance(items, (str, bytes, bytearray)):
        raise ExecutionError("Stage items must be an ordered sequence")
    if not isinstance(history, Sequence) or isinstance(history, (str, bytes, bytearray)):
        raise ExecutionError("Stage history must be an ordered sequence")
    return digest(
        {
            "stage": {
                "name": stage.name,
                "input_mode": stage.input_mode,
                "cache_version": stage.cache_version,
                "canonical_config_json": stage.canonical_config_json,
            },
            "items": [_item_snapshot(item) for item in items],
            "history": [_item_snapshot(item) for item in history],
        }
    )


def derived_each_logical_id(input_item: Item) -> str:
    """Derive an ``each`` output identity from typed producer scope and ID."""

    if not isinstance(input_item, Item):
        raise ExecutionError("derived_each_logical_id requires an Item")
    return digest(
        {
            "kind": "each",
            "producer_scope": input_item.producer_scope,
            "logical_id": input_item.logical_id,
        }
    )


def derived_batch_logical_id(ordered_items: Sequence[Item]) -> str:
    """Derive an ordered batch identity from the exact input Item IDs."""

    if not isinstance(ordered_items, Sequence) or isinstance(
        ordered_items, (str, bytes, bytearray)
    ):
        raise ExecutionError("derived_batch_logical_id requires an ordered sequence")
    item_ids: list[str] = []
    for item in ordered_items:
        if not isinstance(item, Item):
            raise ExecutionError("derived_batch_logical_id requires Item values")
        item_ids.append(item.id)
    return digest({"kind": "batch", "item_ids": item_ids})


def derived_observed_at(ordered_items: Sequence[Item]) -> str:
    """Return the greatest original input timestamp in canonical UTC form."""

    if not isinstance(ordered_items, Sequence) or isinstance(
        ordered_items, (str, bytes, bytearray)
    ):
        raise ExecutionError("derived_observed_at requires an ordered sequence")
    if not ordered_items:
        raise ExecutionError("derived_observed_at requires at least one Item")
    values: list[tuple[datetime, str]] = []
    for item in ordered_items:
        if not isinstance(item, Item):
            raise ExecutionError("derived_observed_at requires Item values")
        normalized = utc_datetime(item.observed_at)
        values.append((normalized, normalized.isoformat().replace("+00:00", "Z")))
    return max(values, key=lambda value: value[0])[1]


def materialized_item_id(
    stage_name: str,
    logical_id: str,
    payload: Any,
    metadata: Mapping[str, Any],
) -> str:
    """Build the typed Stage Item identity from final payload and metadata."""

    if not isinstance(stage_name, str) or not stage_name:
        raise ExecutionError("stage_name must be a non-empty string")
    return make_item_id(("stage", stage_name), logical_id, payload, metadata)


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
