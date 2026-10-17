"""Pure, in-process Stage Task-chain execution primitives.

This module deliberately has no database or scheduler dependency.  Durable
claims, cache pointers, retries, and materialization are layered on top of
the values returned here by later runtime slices.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Literal

from .canonical import (
    canonical_json,
    content_hash,
    derived_batch_logical_id,
    derived_each_logical_id,
    derived_observed_at,
    digest,
    load_item,
    materialized_item_id,
    stage_lookup_key,
    utc_datetime,
)
from .config import decode_stage_snapshot
from .contracts import (
    FORWARD,
    NEW,
    BatchInput,
    Drop,
    ExecutionPolicy,
    Item,
    StageDef,
    StageEnvelope,
    StageOutcome,
    TaskDef,
)
from .db import write_transaction
from .errors import DefinitionError, ExecutionError, InvalidStructuredOutput
from .logging import log_event
from .resolution import BUILTIN_LLM_TASKS
from .web.contracts import (
    FetchChainExhausted,
    WebAttemptCancelled,
    WebExecutionProbe,
    WebStageDeadlineExpired,
    reset_web_execution_probe,
    set_web_execution_probe,
)
from .web.sanitize import sanitize_web_error

if TYPE_CHECKING:
    from .app import App


def _items_by_id(items: Sequence[Item], label: str) -> dict[str, Item]:
    result: dict[str, Item] = {}
    for item in items:
        if not isinstance(item, Item):
            raise ExecutionError(f"{label} must contain Item values")
        previous = result.get(item.id)
        if previous is not None and previous != item:
            raise ExecutionError(f"{label} contains conflicting Item IDs")
        result[item.id] = item
    return result


def _validate_forward(
    marker: FORWARD,
    current: Mapping[str, Item],
    history: Mapping[str, Item],
    forwarded: set[str],
) -> FORWARD:
    item = marker.item
    if not isinstance(item, Item):
        raise ExecutionError("FORWARD must contain an Item")
    if item.id in history:
        raise ExecutionError("FORWARD cannot reference dedup history")
    expected = current.get(item.id)
    if expected is None or expected != item:
        raise ExecutionError("FORWARD must reference a current Stage Item")
    if item.id in forwarded:
        raise ExecutionError("a current Item may be forwarded at most once")
    forwarded.add(item.id)
    return marker


def _validate_marker(
    marker: object,
    current: Mapping[str, Item],
    history: Mapping[str, Item],
    forwarded: set[str],
) -> FORWARD | NEW | None:
    if isinstance(marker, Drop):
        return None
    if isinstance(marker, FORWARD):
        return _validate_forward(marker, current, history, forwarded)
    if isinstance(marker, NEW):
        # Intermediate NEW values intentionally remain ordinary in-process
        # values.  JSON compatibility is enforced by final materialization,
        # not at every Task boundary.
        if not isinstance(marker.metadata, Mapping):
            raise ExecutionError("NEW metadata must be a JSON object")
        return marker
    raise ExecutionError("Task must return DROP, FORWARD(Item), or NEW(value)")


def validate_task_outputs(
    input_mode: str,
    result: object,
    *,
    current_items: Sequence[Item],
    history_items: Sequence[Item],
    current_values: Sequence[object] | None = None,
) -> tuple[FORWARD | NEW, ...]:
    """Validate and normalize one Task result at a Stage boundary.

    ``DROP`` markers are decisions rather than values and therefore disappear
    from the normalized output.  An empty normalized tuple is the explicit
    short-circuit signal for a Stage chain; runtime policy decides whether an
    empty materialization is a normal success or an exhausted invalid drop.
    """

    if input_mode not in {"each", "batch"}:
        raise ExecutionError("Task input mode must be each or batch")
    if not isinstance(current_items, Sequence) or isinstance(
        current_items, (str, bytes, bytearray)
    ):
        raise ExecutionError("current_items must be an ordered sequence")
    if not isinstance(history_items, Sequence) or isinstance(
        history_items, (str, bytes, bytearray)
    ):
        raise ExecutionError("history_items must be an ordered sequence")
    if current_values is None:
        current = _items_by_id(current_items, "current_items")
    else:
        if not isinstance(current_values, Sequence) or isinstance(
            current_values, (str, bytes, bytearray)
        ):
            raise ExecutionError("current_values must be an ordered sequence")
        # Later Tasks see the transient values emitted by the previous Task.
        # NEW values are intentionally not Items, so only Item values can be
        # forwarded at this boundary.
        current = _items_by_id(
            tuple(value for value in current_values if isinstance(value, Item)),
            "current_values",
        )
    history = _items_by_id(history_items, "history_items")

    if input_mode == "each":
        if isinstance(result, (tuple, list)):
            raise ExecutionError("each Task must return one marker")
        marker = _validate_marker(result, current, history, set())
        return () if marker is None else (marker,)

    if not isinstance(result, tuple):
        raise ExecutionError("batch Task must return an ordered marker tuple")
    forwarded: set[str] = set()
    normalized: list[FORWARD | NEW] = []
    aggregate_count = 0
    for marker in result:
        value = _validate_marker(marker, current, history, forwarded)
        if value is None:
            continue
        if isinstance(value, NEW):
            aggregate_count += 1
            if aggregate_count > 1:
                raise ExecutionError("batch Task may emit at most one NEW value")
        normalized.append(value)
    return tuple(normalized)


def _task_config(stage: StageDef, index: int) -> Mapping[str, object]:
    try:
        return stage.tasks[index].with_config
    except (IndexError, AttributeError) as error:
        raise ExecutionError(
            "resolved Task chain does not match Stage definition"
        ) from error


def _task_run(
    task: object,
    config: Mapping[str, object],
    value: object,
    *,
    task_def: TaskDef | None = None,
    web_content_client: object | None = None,
) -> object:
    if task_def is not None:
        from .resolution import BuiltinWebTask, private_web_invocation

        if isinstance(task, BuiltinWebTask):
            invocation = private_web_invocation(task_def)
            if invocation is None:
                raise DefinitionError("web Task has no private invocation")
            return task.run_web(invocation, value)
        if task_def.web_binding is not None:
            if web_content_client is None:
                raise ExecutionError("web content client is not configured")
            from .resolution import run_web

            invocation = private_web_invocation(task_def)
            if invocation is None:
                raise DefinitionError("web Task has no private invocation")
            return run_web(invocation, value, web_content_client)
    run = getattr(task, "run", None)
    if not callable(run):
        raise ExecutionError("resolved Task is not callable")
    try:
        return run(config, value)
    except ExecutionError:
        raise
    except BaseException as error:
        raise ExecutionError("Task execution failed") from error


@dataclass(frozen=True, slots=True)
class WebTaskFailure(ExecutionError):
    """Retain the built-in fetch Task index without serializing its cause."""

    task_index: int
    cause: FetchChainExhausted

    def __post_init__(self) -> None:
        if isinstance(self.task_index, bool) or not isinstance(self.task_index, int):
            raise TypeError("task_index must be an integer")
        if not isinstance(self.cause, FetchChainExhausted):
            raise TypeError("cause must be FetchChainExhausted")


def terminal_fetch_drop_eligible(stage: StageDef, failed_task_index: int) -> bool:
    """Return whether an exhausted fetch may publish its terminal empty result."""

    if stage.input_mode != "each" or failed_task_index != 0:
        return False
    tasks = stage.tasks
    if failed_task_index >= len(tasks):
        return False
    if tasks[failed_task_index].use != "sluice.tasks.fetch_webpage":
        return False
    prefix = tasks[:failed_task_index]
    if any(
        task.use in {"sluice.tasks.fetch_webpage", "sluice.tasks.extract_content"}
        for task in prefix
    ):
        return False
    suffix = tasks[failed_task_index + 1 :]
    if not suffix or suffix[-1].use not in {
        "sluice.tasks.to_markdown",
        "sluice.tasks.to_text",
    }:
        return False
    middle = suffix[:-1]
    return len(middle) <= 1 and all(
        task.use == "sluice.tasks.extract_content" for task in middle
    )


def retries_exhausted(execution: Mapping[str, object], policy: ExecutionPolicy) -> bool:
    """Evaluate the durable retry counter carried by the claimed execution."""

    retry_count = execution["retry_count"]
    return (
        isinstance(retry_count, int)
        and not isinstance(retry_count, bool)
        and retry_count >= policy.retries
    )


def _is_initial_dedup(stage: StageDef, task: object) -> bool:
    if not stage.tasks or not stage.tasks[0].use.endswith(".dedup"):
        return False
    # A resolved custom adapter may expose a name instead of the configured
    # dotted path; the Stage definition remains the authoritative declaration.
    return getattr(task, "input_mode", None) == "batch"


def run_stage_tasks(
    stage: StageDef,
    items: Sequence[Item],
    history: Sequence[Item],
    tasks: Sequence[object],
    *,
    web_content_client: object | None = None,
) -> StageOutcome:
    """Run a resolved Task chain without consulting durable runtime state."""

    if not isinstance(stage, StageDef):
        raise ExecutionError("run_stage_tasks requires a StageDef")
    if not isinstance(items, Sequence) or isinstance(items, (str, bytes, bytearray)):
        raise ExecutionError("Stage items must be an ordered sequence")
    if not isinstance(history, Sequence) or isinstance(
        history, (str, bytes, bytearray)
    ):
        raise ExecutionError("Stage history must be an ordered sequence")
    current_items = tuple(items)
    history_items = tuple(history)
    _items_by_id(current_items, "items")
    _items_by_id(history_items, "history")
    if not isinstance(tasks, Sequence) or isinstance(tasks, (str, bytes, bytearray)):
        raise ExecutionError("resolved Tasks must be an ordered sequence")
    if not tasks or len(tasks) != len(stage.tasks):
        raise ExecutionError("resolved Task chain does not match Stage definition")

    for task in tasks:
        mode = getattr(task, "input_mode", None)
        if mode != stage.input_mode:
            raise ExecutionError(
                f"resolved Task mode {mode!r} does not match Stage {stage.input_mode!r}"
            )

    if stage.input_mode == "each":
        if len(current_items) != 1:
            raise ExecutionError("each Stage requires exactly one current Item")
        value: object = current_items[0]
        for index, task in enumerate(tasks):
            try:
                result = _task_run(
                    task,
                    _task_config(stage, index),
                    value,
                    task_def=stage.tasks[index],
                    web_content_client=web_content_client,
                )
            except FetchChainExhausted as error:
                if stage.tasks[index].use == "sluice.tasks.fetch_webpage":
                    raise WebTaskFailure(index, error) from error
                raise
            markers = validate_task_outputs(
                "each",
                result,
                current_items=current_items,
                history_items=history_items,
                current_values=(value,),
            )
            if not markers:
                return StageOutcome(outputs=(), status="succeeded")
            marker = markers[0]
            value = marker.item if isinstance(marker, FORWARD) else marker.value
        # ``markers`` is non-empty here and only contains FORWARD/NEW.
        return StageOutcome(outputs=markers, status="succeeded")

    values: tuple[Any, ...] = tuple(current_items)
    markers: tuple[FORWARD | NEW, ...] = ()
    for index, task in enumerate(tasks):
        # Only an initial dedup may inspect the reserved history side input.
        task_input: object
        if index == 0:
            task_history = history_items if _is_initial_dedup(stage, task) else ()
            task_input = BatchInput(values, task_history)  # type: ignore[arg-type]
        else:
            task_input = values
        result = _task_run(
            task,
            _task_config(stage, index),
            task_input,
            task_def=stage.tasks[index],
            web_content_client=web_content_client,
        )
        markers = validate_task_outputs(
            "batch",
            result,
            current_items=current_items,
            history_items=history_items
            if index == 0 and _is_initial_dedup(stage, task)
            else (),
            current_values=values,
        )
        if not markers:
            return StageOutcome(outputs=(), status="succeeded")
        values = tuple(
            marker.item if isinstance(marker, FORWARD) else marker.value
            for marker in markers
        )
    return StageOutcome(outputs=markers, status="succeeded")


# ---------------------------------------------------------------------------
# Durable materialization state machine (Task 6)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _StageRequest:
    stage_name: str
    envelope: StageEnvelope
    owner: tuple[str, str] | None
    generation: int | None
    batch_id: int | None
    policy: ExecutionPolicy
    binding_digest: str
    force: bool = False
    retained_stage: StageDef | None = None
    rebuild_target_id: int | None = None
    history_persisted: bool = False


@dataclass(frozen=True, slots=True)
class _RequestOutcome:
    execution_id: int | None
    owner: bool = False
    cached: bool = False
    waiting: bool = False
    request: _StageRequest | None = None


@dataclass(frozen=True, slots=True)
class RebuildResult:
    execution_id: int | None
    status: Literal["queued", "running", "succeeded", "failed"]
    forced: bool


_STAGE_LOGGER = logging.getLogger("sluice.stage_runtime")


def _ts(value: datetime) -> str:
    from .canonical import utc_datetime

    return utc_datetime(value).isoformat().replace("+00:00", "Z")


def _policy_json(policy: ExecutionPolicy) -> str:
    return canonical_json(
        {
            "timeout_seconds": None
            if policy.timeout is None
            else policy.timeout.total_seconds(),
            "retries": policy.retries,
            "concurrency": policy.concurrency,
        }
    )


def _decode_policy(value: str) -> ExecutionPolicy:
    try:
        raw = json.loads(value)
        timeout = raw.get("timeout_seconds")
        retries = int(raw.get("retries", 0))
        concurrency = int(raw.get("concurrency", 1))
    except (TypeError, ValueError, json.JSONDecodeError, AttributeError) as error:
        raise ExecutionError("persisted Stage policy is invalid JSON") from error
    return ExecutionPolicy(
        timeout=None if timeout is None else timedelta(seconds=float(timeout)),
        retries=retries,
        concurrency=concurrency,
    )


def _decode_stage_snapshot(value: str | Mapping[str, object]) -> StageDef:
    try:
        raw = json.loads(value) if isinstance(value, str) else dict(value)
    except (TypeError, ValueError, json.JSONDecodeError) as error:
        raise ExecutionError("persisted Stage configuration is invalid JSON") from error
    if not isinstance(raw, Mapping):
        raise ExecutionError("persisted Stage configuration must be an object")
    return decode_stage_snapshot(raw)  # type: ignore[arg-type]


def _load_items(app: App, ids: Sequence[str]) -> tuple[Item, ...]:
    result: list[Item] = []
    with app.conn.transaction_lock:
        for item_id in ids:
            row = app.conn.execute(
                "SELECT * FROM items WHERE id=?", (item_id,)
            ).fetchone()
            if row is None:
                raise ExecutionError(f"Stage envelope Item is missing: {item_id}")
            result.append(load_item(row))
    return tuple(result)


def _execution_inputs_tx(app: App, execution_id: int, envelope: StageEnvelope) -> None:
    app.conn.executemany(
        "INSERT INTO execution_inputs(execution_id,slot,ordinal,item_id) VALUES (?,?,?,?)",
        [
            (execution_id, "items", ordinal, item_id)
            for ordinal, item_id in enumerate(envelope.items)
        ]
        + [
            (execution_id, "history", ordinal, item_id)
            for ordinal, item_id in enumerate(envelope.history)
        ],
    )


def _envelope_json(envelope: StageEnvelope) -> str:
    return canonical_json(
        {"items": list(envelope.items), "history": list(envelope.history)}
    )


def _items_for_execution(app: App, execution_id: int, slot: str) -> tuple[Item, ...]:
    with app.conn.transaction_lock:
        rows = app.conn.execute(
            "SELECT i.* FROM execution_inputs ei JOIN items i ON i.id=ei.item_id "
            "WHERE ei.execution_id=? AND ei.slot=? ORDER BY ei.ordinal",
            (execution_id, slot),
        ).fetchall()
    return tuple(load_item(row) for row in rows)


def verify_execution_envelope(app: App, execution_id: int) -> StageEnvelope:
    """Validate and return the immutable input envelope of a Stage Execution.

    Rebuilds are intentionally anchored to this row and its retained Stage
    snapshot.  No current YAML definition or upstream publication is
    consulted while validating the target.
    """

    if isinstance(execution_id, bool) or not isinstance(execution_id, int):
        raise ExecutionError("execution_id must be an integer")
    with app.conn.transaction_lock:
        target = app.conn.execute(
            "SELECT * FROM executions WHERE id=? AND kind='stage'", (execution_id,)
        ).fetchone()
        if target is None:
            raise ExecutionError(f"Stage Execution is missing: {execution_id}")
        try:
            raw_snapshot = json.loads(str(target["config_json"]))
        except (TypeError, ValueError, json.JSONDecodeError) as error:
            raise ExecutionError(
                "persisted Stage configuration is invalid JSON"
            ) from error
        if not isinstance(raw_snapshot, Mapping):
            raise ExecutionError("persisted Stage configuration must be an object")
        stage = decode_stage_snapshot(raw_snapshot)  # type: ignore[arg-type]
        if stage.name != str(target["stage_name"]):
            raise ExecutionError("Stage snapshot name does not match Execution")
        rows = app.conn.execute(
            "SELECT slot,ordinal,item_id FROM execution_inputs WHERE execution_id=? "
            "ORDER BY slot,ordinal",
            (execution_id,),
        ).fetchall()
        by_slot: dict[str, list[tuple[int, str]]] = {"items": [], "history": []}
        for row in rows:
            slot = str(row["slot"])
            if slot not in by_slot:
                raise ExecutionError("Stage Execution envelope has an invalid slot")
            ordinal = row["ordinal"]
            if isinstance(ordinal, bool) or not isinstance(ordinal, int) or ordinal < 0:
                raise ExecutionError("Stage Execution envelope has an invalid ordinal")
            by_slot[slot].append((ordinal, str(row["item_id"])))
        if not by_slot["items"]:
            raise ExecutionError("Stage Execution envelope is missing Items")
        for slot, values in by_slot.items():
            ordinals = [ordinal for ordinal, _item_id in values]
            if ordinals != list(range(len(ordinals))):
                raise ExecutionError(
                    "Stage Execution envelope ordinals are inconsistent"
                )
            item_ids = [item_id for _ordinal, item_id in values]
            if len(set(item_ids)) != len(item_ids):
                raise ExecutionError(
                    "Stage Execution envelope contains duplicate Items"
                )
            for item_id in item_ids:
                item = app.conn.execute(
                    "SELECT 1 FROM items WHERE id=?", (item_id,)
                ).fetchone()
                if item is None:
                    raise ExecutionError(f"Stage envelope Item is missing: {item_id}")
        item_ids = tuple(item_id for _ordinal, item_id in by_slot["items"])
        history_ids = tuple(item_id for _ordinal, item_id in by_slot["history"])
        items = tuple(
            load_item(
                app.conn.execute(
                    "SELECT * FROM items WHERE id=?", (item_id,)
                ).fetchone()
            )
            for item_id in item_ids
        )
        history = tuple(
            load_item(
                app.conn.execute(
                    "SELECT * FROM items WHERE id=?", (item_id,)
                ).fetchone()
            )
            for item_id in history_ids
        )
        try:
            expected_key = stage_lookup_key(stage, items, history)
        except Exception as error:
            raise ExecutionError("Stage Execution envelope is inconsistent") from error
        recorded_key = target["lookup_key"]
        if not isinstance(recorded_key, str) or recorded_key != expected_key:
            raise ExecutionError(
                "Stage Execution lookup key does not match retained envelope"
            )
    return StageEnvelope(item_ids, history_ids)


def resolve_dedup_history_ids(
    app: App, stage: StageDef, candidate_ids: Sequence[str]
) -> tuple[str, ...]:
    """Resolve eligible dedup history through current immutable result rows."""

    if not isinstance(stage, StageDef):
        raise ExecutionError("resolve_dedup_history_ids requires a StageDef")
    candidates = _load_items(app, candidate_ids)
    if not candidates:
        return ()
    rows = app.conn.execute(
        "SELECT sr.id AS result_id, sri.item_id AS output_id, ei.item_id AS input_id "
        "FROM stage_result_current src "
        "JOIN stage_results sr ON sr.id=src.result_id "
        "JOIN stage_result_items sri ON sri.result_id=sr.id "
        "JOIN execution_inputs ei ON ei.execution_id=sr.producer_execution_id AND ei.slot='items' "
        "WHERE src.stage_name=? AND sr.stage_name=? ORDER BY sr.sequence,sri.ordinal,ei.ordinal",
        (stage.name, stage.name),
    ).fetchall()
    grouped: dict[tuple[int, str], dict[str, Any]] = {}
    for row in rows:
        result_id = int(row["result_id"])
        output_id = str(row["output_id"])
        entry = grouped.setdefault(
            (result_id, output_id), {"output_id": output_id, "input_ids": []}
        )
        entry["input_ids"].append(str(row["input_id"]))
    accepted: list[tuple[Item, tuple[str, ...]]] = []
    for entry in grouped.values():
        output = app.conn.execute(
            "SELECT * FROM items WHERE id=?", (entry["output_id"],)
        ).fetchone()
        if output is None:
            continue
        accepted.append((load_item(output), tuple(dict.fromkeys(entry["input_ids"]))))
    from .tasks.collection import _validate_keys, select_dedup_history

    try:
        keys = _validate_keys(stage.tasks[0].with_config)
    except (IndexError, AttributeError, TypeError) as error:
        raise ExecutionError("dedup Stage is missing configured keys") from error

    return tuple(
        item.id for item in select_dedup_history(candidates, accepted, keys=keys)
    )


def _stage_for_request(app: App, request: _StageRequest) -> StageDef:
    if request.retained_stage is not None:
        return request.retained_stage
    try:
        return app.definition.stages[request.stage_name]
    except KeyError as error:
        raise ExecutionError(f"unknown Stage {request.stage_name!r}") from error


def _node_helpers():
    # reconcile imports stage_runtime lazily for its execution loop.
    from .reconcile import _binding, _node

    return _binding, _node


def _insert_stage_execution_tx(
    app: App,
    request: _StageRequest,
    stage: StageDef,
    lookup_key: str,
    envelope: StageEnvelope,
    state: str,
    at_text: str,
    *,
    result_id: int | None = None,
    claim_ordinal: int | None = None,
    retry_count: int = 0,
    retry_parent_id: int | None = None,
) -> int:
    pipeline_name = None if request.owner is None else request.owner[0]
    node_name = None if request.owner is None else request.owner[1]
    cursor = app.conn.execute(
        "INSERT INTO executions "
        "(kind,pipeline_name,node_name,stage_name,state,binding_digest,config_json,lookup_key,"
        "admitted_generation,claim_ordinal,batch_id,history_persisted,policy_json,retry_count,"
        "retry_parent_id,result_id,finished_at,rebuild_target_id) "
        "VALUES ('stage',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            pipeline_name,
            node_name,
            stage.name,
            state,
            request.binding_digest,
            stage.canonical_config_json,
            lookup_key,
            request.generation,
            claim_ordinal,
            request.batch_id,
            int(request.history_persisted),
            _policy_json(request.policy),
            retry_count,
            retry_parent_id,
            result_id,
            at_text if state == "cached" else None,
            request.rebuild_target_id,
        ),
    )
    execution_id = int(cursor.lastrowid)
    _execution_inputs_tx(app, execution_id, envelope)
    return execution_id


def _member_ids_for_request(request: _StageRequest) -> tuple[str, ...]:
    return tuple(request.envelope.items)


def _mark_members_claimed_tx(app: App, request: _StageRequest, lookup_key: str) -> None:
    if request.owner is None:
        return
    pipeline_name, node_name = request.owner
    binding = request.binding_digest
    for item_id in request.envelope.items:
        app.conn.execute(
            "UPDATE node_members SET state=CASE WHEN state='pending' THEN 'claimed' ELSE state END,lookup_key=? "
            "WHERE pipeline_name=? AND node_name=? AND binding_digest=? AND item_id=? "
            "AND state IN ('pending','claimed','frozen')",
            (lookup_key, pipeline_name, node_name, binding, item_id),
        )


def _member_scope_current_tx(app: App, request: _StageRequest) -> bool:
    if request.owner is None:
        return False
    pipeline_name, node_name = request.owner
    pointer = app.conn.execute(
        "SELECT binding_digest FROM node_current_bindings WHERE pipeline_name=? AND node_name=?",
        (pipeline_name, node_name),
    ).fetchone()
    if pointer is None or str(pointer[0]) != request.binding_digest:
        return False
    if request.batch_id is not None:
        state = app.conn.execute(
            "SELECT current_frozen_batch_id FROM node_state WHERE pipeline_name=? AND node_name=? "
            "AND binding_digest=?",
            (pipeline_name, node_name, request.binding_digest),
        ).fetchone()
        if state is None or state[0] != request.batch_id:
            return False
    for item_id in request.envelope.items:
        row = app.conn.execute(
            "SELECT generation,state,producer_kind,producer_name,logical_id FROM node_members WHERE pipeline_name=? AND node_name=? "
            "AND binding_digest=? AND item_id=?",
            (pipeline_name, node_name, request.binding_digest, item_id),
        ).fetchone()
        if row is None or row["state"] not in {
            "pending",
            "claimed",
            "frozen",
            "terminal",
        }:
            return False
        if (
            request.generation is not None
            and int(row["generation"]) != request.generation
        ):
            return False
        if request.generation is not None:
            newer = app.conn.execute(
                "SELECT 1 FROM node_members WHERE pipeline_name=? AND node_name=? AND binding_digest=? "
                "AND producer_kind=? AND producer_name=? AND logical_id=? AND generation>? AND state!='stale' LIMIT 1",
                (
                    pipeline_name,
                    node_name,
                    request.binding_digest,
                    row["producer_kind"],
                    row["producer_name"],
                    row["logical_id"],
                    request.generation,
                ),
            ).fetchone()
            if newer is not None:
                return False
    return True


def _current_result_items_tx(app: App, result_id: int) -> list[tuple[str, int]]:
    rows = app.conn.execute(
        "SELECT sri.item_id,sr.sequence FROM stage_result_items sri "
        "JOIN stage_results sr ON sr.id=sri.result_id WHERE sri.result_id=? ORDER BY sri.ordinal",
        (result_id,),
    ).fetchall()
    return [(str(row[0]), int(row[1])) for row in rows]


def _publish_node_snapshot_tx(
    app: App, pipeline_name: str, node_name: str, binding: str, at_text: str
) -> None:
    state = app.conn.execute(
        "SELECT current_publication_id FROM node_state WHERE pipeline_name=? AND node_name=? AND binding_digest=?",
        (pipeline_name, node_name, binding),
    ).fetchone()
    if state is None:
        return
    rows = app.conn.execute(
        "SELECT * FROM node_members WHERE pipeline_name=? AND node_name=? AND binding_digest=? "
        "AND state!='stale' ORDER BY ordinal,item_id",
        (pipeline_name, node_name, binding),
    ).fetchall()
    item_ids: list[str] = []
    provenance: list[tuple[int | None, int | None]] = []
    seen_results: set[int] = set()
    blocked = False
    for row in rows:
        result_id = row["visible_result_id"]
        if result_id is None:
            # A newly admitted each member blocks the visible prefix.  Frozen
            # batch members use one shared result and are likewise unresolved
            # until their owner completes.
            if row["state"] in {"pending", "claimed", "frozen"}:
                blocked = True
                break
            continue
        result_id = int(result_id)
        if result_id in seen_results:
            continue
        seen_results.add(result_id)
        item_rows = _current_result_items_tx(app, result_id)
        item_ids.extend(item_id for item_id, _sequence in item_rows)
        provenance.extend((result_id, sequence) for _item_id, sequence in item_rows)
    # A newly admitted earlier member must not retract an already visible
    # prefix.  Its completion will re-enter this transaction and extend the
    # snapshot once the prefix is resolved.
    if blocked:
        return
    current_members = app.conn.execute(
        "SELECT item_id FROM publication_members WHERE publication_id=? ORDER BY ordinal",
        (int(state[0]),),
    ).fetchall()
    current_ids = [str(row[0]) for row in current_members]
    if current_ids == item_ids:
        return
    producer_key = f"{pipeline_name}.{node_name}"
    revision = (
        int(
            app.conn.execute(
                "SELECT COALESCE(MAX(revision),0) FROM publications WHERE producer_key=? AND binding_digest=?",
                (producer_key, binding),
            ).fetchone()[0]
        )
        + 1
    )
    publication_digest = digest(
        {
            "kind": "snapshot",
            "producer_key": producer_key,
            "binding_digest": binding,
            "members": item_ids,
        }
    )
    publication = app.conn.execute(
        "INSERT INTO publications(kind,producer_key,binding_digest,revision,digest) VALUES ('snapshot',?,?,?,?)",
        (producer_key, binding, revision, publication_digest),
    )
    publication_id = int(publication.lastrowid)
    app.conn.executemany(
        "INSERT INTO publication_members(publication_id,ordinal,item_id,visible_result_id,materialization_sequence) VALUES (?,?,?,?,?)",
        [
            (
                publication_id,
                ordinal,
                item_id,
                provenance[ordinal][0],
                provenance[ordinal][1],
            )
            for ordinal, item_id in enumerate(item_ids)
        ],
    )
    app.conn.execute(
        "UPDATE node_state SET current_publication_id=? WHERE pipeline_name=? AND node_name=? AND binding_digest=?",
        (publication_id, pipeline_name, node_name, binding),
    )
    log_event(
        _STAGE_LOGGER,
        "snapshot_publication",
        pipeline=pipeline_name,
        node=node_name,
        binding_digest=binding,
        publication_id=publication_id,
        item_ids=item_ids,
        state="published",
    )


def _attach_result_tx(
    app: App,
    request: _StageRequest,
    result_id: int,
    at_text: str,
    *,
    execution_id: int | None = None,
) -> None:
    if request.owner is None:
        return
    pipeline_name, node_name = request.owner
    sequence = app.conn.execute(
        "SELECT sequence,lookup_key FROM stage_results WHERE id=?", (result_id,)
    ).fetchone()
    sequence_value = None if sequence is None else int(sequence["sequence"])
    lookup_value = None if sequence is None else str(sequence["lookup_key"])
    if not _member_scope_current_tx(app, request):
        if request.rebuild_target_id is not None:
            if execution_id is not None:
                app.conn.execute(
                    "UPDATE executions SET target_scope_excluded=1 WHERE id=? AND kind='stage'",
                    (execution_id,),
                )
            # The target's historical sequence has been observed by this
            # node, but its output must not replace the active local result.
            # Keeping visible_result_id untouched preserves the current
            # snapshot while preventing repeated healing of the same row.
            if sequence_value is not None:
                ids = tuple(request.envelope.items)
                if ids:
                    placeholders = ",".join("?" for _ in ids)
                    params: list[object] = [
                        sequence_value,
                        pipeline_name,
                        node_name,
                        request.binding_digest,
                    ]
                    params.extend(ids)
                    generation_clause = ""
                    if request.generation is not None:
                        generation_clause = " AND generation=?"
                        params.append(request.generation)
                    app.conn.execute(
                        "UPDATE node_members SET consumed_sequence=? WHERE pipeline_name=? "
                        "AND node_name=? AND binding_digest=? AND item_id IN ("
                        + placeholders
                        + ")"
                        + generation_clause,
                        tuple(params),
                    )
        return
    for item_id in request.envelope.items:
        app.conn.execute(
            "UPDATE node_members SET state='terminal',consumed_sequence=?,visible_result_id=?,lookup_key=? "
            "WHERE pipeline_name=? AND node_name=? AND binding_digest=? AND item_id=? "
            "AND state IN ('pending','claimed','frozen','terminal')",
            (
                sequence_value,
                result_id,
                lookup_value,
                pipeline_name,
                node_name,
                request.binding_digest,
                item_id,
            ),
        )
    if request.batch_id is not None:
        app.conn.execute(
            "UPDATE batches SET state='succeeded' WHERE id=? AND state IN ('frozen','failed')",
            (request.batch_id,),
        )
        app.conn.execute(
            "UPDATE node_state SET current_frozen_batch_id=NULL WHERE pipeline_name=? AND node_name=? "
            "AND binding_digest=? AND current_frozen_batch_id=?",
            (pipeline_name, node_name, request.binding_digest, request.batch_id),
        )
    _publish_node_snapshot_tx(
        app, pipeline_name, node_name, request.binding_digest, at_text
    )


def _promote_waiters_tx(
    app: App, stage_name: str, lookup_key: str, result_id: int | None, at_text: str
) -> None:
    claim = app.conn.execute(
        "SELECT * FROM materialization_claims WHERE stage_name=? AND lookup_key=?",
        (stage_name, lookup_key),
    ).fetchone()
    if claim is None or claim["owner_execution_id"] is not None:
        return
    waiters = app.conn.execute(
        "SELECT * FROM materialization_waiters WHERE stage_name=? AND lookup_key=? "
        "ORDER BY force DESC,waiting_sequence",
        (stage_name, lookup_key),
    ).fetchall()
    for waiter in waiters:
        pipeline_name = waiter["pipeline_name"]
        node_name = waiter["node_name"]
        envelope_raw = json.loads(str(waiter["envelope_json"]))
        envelope = StageEnvelope(
            tuple(envelope_raw.get("items", ())), tuple(envelope_raw.get("history", ()))
        )
        binding = waiter["binding_digest"]
        retained_stage = _decode_stage_snapshot(str(waiter["config_json"]))
        request = _StageRequest(
            stage_name=stage_name,
            envelope=envelope,
            owner=None
            if pipeline_name is None or node_name is None
            else (str(pipeline_name), str(node_name)),
            generation=None
            if waiter["generation"] is None
            else int(waiter["generation"]),
            batch_id=None if waiter["batch_id"] is None else int(waiter["batch_id"]),
            policy=_decode_policy(str(waiter["policy_json"])),
            binding_digest=str(binding or ""),
            force=bool(waiter["force"]),
            retained_stage=retained_stage,
            rebuild_target_id=waiter["rebuild_target_id"],
            history_persisted=bool(waiter["history_persisted"]),
        )
        if not request.force and result_id is not None:
            execution_id = _insert_stage_execution_tx(
                app,
                request,
                _decode_stage_snapshot(str(waiter["config_json"])),
                lookup_key,
                envelope,
                "cached",
                at_text,
                result_id=int(result_id),
            )
            app.conn.execute(
                "DELETE FROM materialization_waiters WHERE stage_name=? AND lookup_key=? AND waiting_sequence=?",
                (stage_name, lookup_key, waiter["waiting_sequence"]),
            )
            _attach_result_tx(app, request, int(result_id), at_text)
            continue
        # Verify a normal waiter still points at a live binding/generation;
        # stale historical rows are discarded without consuming a claim.
        if not request.force and not _member_scope_current_tx(app, request):
            app.conn.execute(
                "DELETE FROM materialization_waiters WHERE stage_name=? AND lookup_key=? AND waiting_sequence=?",
                (stage_name, lookup_key, waiter["waiting_sequence"]),
            )
            continue
        execution_id = _insert_stage_execution_tx(
            app,
            request,
            retained_stage,
            lookup_key,
            envelope,
            "queued",
            at_text,
        )
        app.conn.execute(
            "DELETE FROM materialization_waiters WHERE stage_name=? AND lookup_key=? AND waiting_sequence=?",
            (stage_name, lookup_key, waiter["waiting_sequence"]),
        )
        app.conn.execute(
            "UPDATE materialization_claims SET owner_execution_id=? WHERE stage_name=? AND lookup_key=?",
            (execution_id, stage_name, lookup_key),
        )
        _mark_members_claimed_tx(app, request, lookup_key)
        break


def _request_materialization(
    app: App, request: _StageRequest, at: datetime
) -> _RequestOutcome:
    if not isinstance(request, _StageRequest):
        raise ExecutionError("invalid Stage request")
    if not request.envelope.items:
        raise ExecutionError("Stage envelope must contain at least one Item")
    stage = _stage_for_request(app, request)
    if stage.name != request.stage_name:
        raise ExecutionError("Stage request name does not match Stage definition")
    # A batch ID is an immutable admission envelope, not merely a hint for
    # lookup.  Manual retries may provide only the frozen Item IDs; recover
    # the original dedup history from the first attempt rather than selecting
    # a new upstream history at retry time.
    batch_history_persisted = request.history_persisted
    if request.batch_id is not None:
        batch_rows = app.conn.execute(
            "SELECT item_id FROM batch_items WHERE batch_id=? ORDER BY ordinal",
            (request.batch_id,),
        ).fetchall()
        frozen_items = tuple(str(row[0]) for row in batch_rows)
        if not frozen_items:
            raise ExecutionError("Stage batch envelope is missing")
        if tuple(request.envelope.items) != frozen_items:
            raise ExecutionError("Stage retry must use the exact frozen batch Items")
        first_attempt = app.conn.execute(
            "SELECT id FROM executions WHERE kind='stage' AND batch_id=? ORDER BY id LIMIT 1",
            (request.batch_id,),
        ).fetchone()
        if first_attempt is not None:
            batch_history_persisted = True
            history_rows = app.conn.execute(
                "SELECT item_id FROM execution_inputs WHERE execution_id=? AND slot='history' ORDER BY ordinal",
                (first_attempt[0],),
            ).fetchall()
            frozen_history = tuple(str(row[0]) for row in history_rows)
            if (
                request.envelope.history
                and tuple(request.envelope.history) != frozen_history
            ):
                raise ExecutionError("Stage retry must use the original frozen history")
            if not request.envelope.history and frozen_history:
                request = _StageRequest(
                    stage_name=request.stage_name,
                    envelope=StageEnvelope(request.envelope.items, frozen_history),
                    owner=request.owner,
                    generation=request.generation,
                    batch_id=request.batch_id,
                    policy=request.policy,
                    binding_digest=request.binding_digest,
                    force=request.force,
                    retained_stage=request.retained_stage,
                    rebuild_target_id=request.rebuild_target_id,
                    history_persisted=request.history_persisted,
                )
    if batch_history_persisted and not request.history_persisted:
        request = _StageRequest(
            stage_name=request.stage_name,
            envelope=request.envelope,
            owner=request.owner,
            generation=request.generation,
            batch_id=request.batch_id,
            policy=request.policy,
            binding_digest=request.binding_digest,
            force=request.force,
            retained_stage=request.retained_stage,
            rebuild_target_id=request.rebuild_target_id,
            history_persisted=True,
        )
    # Fresh normal requests resolve dedup history exactly once before the key
    # is computed.  Retries/cache/rebuild carry the persisted envelope.
    history = request.envelope.history
    if (
        not request.force
        and not history
        and not batch_history_persisted
        and stage.input_mode == "batch"
        and stage.tasks
        and stage.tasks[0].use.endswith(".dedup")
    ):
        history = resolve_dedup_history_ids(app, stage, request.envelope.items)
        request = _StageRequest(
            stage_name=request.stage_name,
            envelope=StageEnvelope(request.envelope.items, history),
            owner=request.owner,
            generation=request.generation,
            batch_id=request.batch_id,
            policy=request.policy,
            binding_digest=request.binding_digest,
            force=request.force,
            retained_stage=request.retained_stage,
            rebuild_target_id=request.rebuild_target_id,
            history_persisted=True,
        )
    items = _load_items(app, request.envelope.items)
    history_items = _load_items(app, history)
    lookup_key = stage_lookup_key(stage, items, history_items)
    at_text = _ts(at)
    with write_transaction(app.conn):
        current = app.conn.execute(
            "SELECT result_id FROM stage_result_current WHERE stage_name=? AND lookup_key=?",
            (stage.name, lookup_key),
        ).fetchone()
        if current is not None and not request.force:
            execution_id = _insert_stage_execution_tx(
                app,
                request,
                stage,
                lookup_key,
                request.envelope,
                "cached",
                at_text,
                result_id=int(current[0]),
            )
            _attach_result_tx(app, request, int(current[0]), at_text)
            return _RequestOutcome(execution_id, cached=True, request=request)

        app.conn.execute(
            "INSERT OR IGNORE INTO materialization_claims(stage_name,lookup_key) VALUES (?,?)",
            (stage.name, lookup_key),
        )
        claim = app.conn.execute(
            "SELECT * FROM materialization_claims WHERE stage_name=? AND lookup_key=?",
            (stage.name, lookup_key),
        ).fetchone()
        if claim is None:
            raise ExecutionError("materialization claim unavailable")
        owner_id = claim["owner_execution_id"]
        if owner_id is not None:
            owner_state = app.conn.execute(
                "SELECT state FROM executions WHERE id=?", (owner_id,)
            ).fetchone()
            if owner_state is not None and owner_state[0] in {"queued", "running"}:
                existing = app.conn.execute(
                    "SELECT 1 FROM materialization_waiters WHERE stage_name=? AND lookup_key=? "
                    "AND pipeline_name IS ? AND node_name IS ? AND envelope_json=? "
                    "AND force=? AND rebuild_target_id IS ? AND history_persisted=?",
                    (
                        stage.name,
                        lookup_key,
                        request.owner[0] if request.owner else None,
                        request.owner[1] if request.owner else None,
                        _envelope_json(request.envelope),
                        int(request.force),
                        request.rebuild_target_id,
                        int(request.history_persisted),
                    ),
                ).fetchone()
                if existing is None:
                    sequence = int(claim["next_waiting_sequence"])
                    app.conn.execute(
                        "INSERT INTO materialization_waiters(stage_name,lookup_key,claim_id,waiting_sequence,pipeline_name,node_name,force,history_persisted,generation,batch_id,envelope_json,policy_json,config_json,binding_digest,rebuild_target_id) "
                        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            stage.name,
                            lookup_key,
                            claim["claim_id"],
                            sequence,
                            request.owner[0] if request.owner else None,
                            request.owner[1] if request.owner else None,
                            int(request.force),
                            int(request.history_persisted),
                            request.generation,
                            request.batch_id,
                            _envelope_json(request.envelope),
                            _policy_json(request.policy),
                            stage.canonical_config_json,
                            request.binding_digest,
                            request.rebuild_target_id,
                        ),
                    )
                    app.conn.execute(
                        "UPDATE materialization_claims SET next_waiting_sequence=next_waiting_sequence+1 WHERE claim_id=?",
                        (claim["claim_id"],),
                    )
                _mark_members_claimed_tx(app, request, lookup_key)
                return _RequestOutcome(int(owner_id), waiting=True, request=request)
            app.conn.execute(
                "UPDATE materialization_claims SET owner_execution_id=NULL WHERE claim_id=?",
                (claim["claim_id"],),
            )

        execution_id = _insert_stage_execution_tx(
            app,
            request,
            stage,
            lookup_key,
            request.envelope,
            "queued",
            at_text,
        )
        app.conn.execute(
            "UPDATE materialization_claims SET owner_execution_id=? WHERE claim_id=?",
            (execution_id, claim["claim_id"]),
        )
        _mark_members_claimed_tx(app, request, lookup_key)
        return _RequestOutcome(execution_id, owner=True, request=request)


def _start_claimed_execution(app: App, execution_id: int, at: datetime) -> bool:
    with write_transaction(app.conn):
        changed = app.conn.execute(
            "UPDATE executions SET state='running',started_at=? WHERE id=? AND kind='stage' AND state='queued' "
            "AND EXISTS (SELECT 1 FROM materialization_claims mc WHERE mc.stage_name=executions.stage_name "
            "AND mc.lookup_key=executions.lookup_key AND mc.owner_execution_id=executions.id)",
            (_ts(at), execution_id),
        )
        return changed.rowcount == 1


def _finalize_stage(
    app: App,
    execution_id: int,
    outcome: StageOutcome,
    at: datetime,
    *,
    terminal_fetch_drop: bool = False,
    expected_retry_count: int | None = None,
) -> bool:
    at_text = _ts(at)
    reconcile_consumers: list[tuple[str, str]] = []
    with write_transaction(app.conn):
        row = app.conn.execute(
            "SELECT * FROM executions WHERE id=? AND kind='stage' AND state='running'",
            (execution_id,),
        ).fetchone()
        if row is None:
            return False
        guard = app.conn.execute(
            "UPDATE executions SET state=?,error=CASE WHEN ? THEN NULL ELSE error END,finished_at=? WHERE id=? AND kind='stage' AND state='running' "
            "AND stage_name=? AND lookup_key=? AND (? IS NULL OR retry_count=?) "
            "AND EXISTS (SELECT 1 FROM materialization_claims mc WHERE mc.stage_name=? "
            "AND mc.lookup_key=? AND mc.owner_execution_id=?)",
            (
                outcome.status,
                int(terminal_fetch_drop),
                at_text,
                execution_id,
                row["stage_name"],
                row["lookup_key"],
                expected_retry_count,
                expected_retry_count,
                row["stage_name"],
                row["lookup_key"],
                execution_id,
            ),
        )
        if guard.rowcount != 1:
            return False
        stage = _decode_stage_snapshot(str(row["config_json"]))
        items = _items_for_execution(app, execution_id, "items")
        outputs: list[Item] = []
        for marker in outcome.outputs:
            if isinstance(marker, FORWARD):
                outputs.append(marker.item)
                continue
            if not isinstance(marker, NEW):
                raise ExecutionError("invalid Stage output marker")
            if stage.input_mode == "each":
                logical_id = derived_each_logical_id(items[0])
            else:
                logical_id = derived_batch_logical_id(items)
            payload = marker.value
            metadata = marker.metadata
            # canonical_json enforces strict JSON-compatible final values.
            canonical_json(payload)
            canonical_json(metadata)
            item_id = materialized_item_id(stage.name, logical_id, payload, metadata)
            output = Item(
                id=item_id,
                producer_scope=("stage", stage.name),
                logical_id=logical_id,
                payload=payload,
                metadata=metadata,
                content_hash=content_hash(payload),
                observed_at=derived_observed_at(items),
                produced_by=execution_id,
            )
            app.conn.execute(
                "INSERT OR IGNORE INTO items(id,producer_kind,producer_name,logical_id,payload_json,metadata_json,content_hash,observed_at,produced_by) VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    output.id,
                    "stage",
                    stage.name,
                    output.logical_id,
                    canonical_json(output.payload),
                    canonical_json(output.metadata),
                    output.content_hash,
                    output.observed_at,
                    output.produced_by,
                ),
            )
            outputs.append(output)
        previous = app.conn.execute(
            "SELECT result_id FROM stage_result_current WHERE stage_name=? AND lookup_key=?",
            (stage.name, row["lookup_key"]),
        ).fetchone()
        previous_ids = (
            []
            if previous is None
            else [
                item_id
                for item_id, _sequence in _current_result_items_tx(
                    app, int(previous[0])
                )
            ]
        )
        output_ids = [output.id for output in outputs]
        output_changed = previous_ids != output_ids
        sequence = (
            int(
                app.conn.execute(
                    "SELECT COALESCE(MAX(sequence),0) FROM stage_results WHERE stage_name=?",
                    (stage.name,),
                ).fetchone()[0]
            )
            + 1
        )
        result = app.conn.execute(
            "INSERT INTO stage_results(stage_name,lookup_key,sequence,created_at,producer_execution_id) VALUES (?,?,?,?,?)",
            (stage.name, row["lookup_key"], sequence, at_text, execution_id),
        )
        result_id = int(result.lastrowid)
        app.conn.executemany(
            "INSERT INTO stage_result_items(result_id,ordinal,item_id) VALUES (?,?,?)",
            ((result_id, ordinal, output.id) for ordinal, output in enumerate(outputs)),
        )
        app.conn.execute(
            "INSERT INTO stage_result_current(stage_name,lookup_key,result_id) VALUES (?,?,?) "
            "ON CONFLICT(stage_name,lookup_key) DO UPDATE SET result_id=excluded.result_id",
            (stage.name, row["lookup_key"], result_id),
        )
        app.conn.execute(
            "UPDATE executions SET result_id=? WHERE id=?", (result_id, execution_id)
        )
        request = _StageRequest(
            stage_name=stage.name,
            envelope=StageEnvelope(
                tuple(
                    str(r[0])
                    for r in app.conn.execute(
                        "SELECT item_id FROM execution_inputs WHERE execution_id=? AND slot='items' ORDER BY ordinal",
                        (execution_id,),
                    )
                ),
                tuple(
                    str(r[0])
                    for r in app.conn.execute(
                        "SELECT item_id FROM execution_inputs WHERE execution_id=? AND slot='history' ORDER BY ordinal",
                        (execution_id,),
                    )
                ),
            ),
            owner=None
            if row["pipeline_name"] is None or row["node_name"] is None
            else (str(row["pipeline_name"]), str(row["node_name"])),
            generation=row["admitted_generation"],
            batch_id=row["batch_id"],
            policy=_decode_policy(str(row["policy_json"])),
            binding_digest=str(row["binding_digest"]),
            history_persisted=bool(row["history_persisted"]),
            rebuild_target_id=row["rebuild_target_id"],
        )
        _attach_result_tx(app, request, result_id, at_text, execution_id=execution_id)
        app.conn.execute(
            "UPDATE materialization_claims SET owner_execution_id=NULL WHERE stage_name=? AND lookup_key=? AND owner_execution_id=?",
            (stage.name, row["lookup_key"], execution_id),
        )
        _promote_waiters_tx(app, stage.name, str(row["lookup_key"]), result_id, at_text)
        log_event(
            _STAGE_LOGGER,
            "stage_finalization",
            pipeline=row["pipeline_name"],
            node=row["node_name"],
            stage=stage.name,
            execution_id=execution_id,
            claim_id=None,
            item_ids=list(request.envelope.items),
            result_id=result_id,
            state=outcome.status,
        )
        if row["rebuild_target_id"] is not None and output_changed:
            for pipeline_name, pipeline in app.definition.pipelines.items():
                for node_name, node in pipeline.nodes.items():
                    if node.stage == stage.name:
                        reconcile_consumers.append((pipeline_name, node_name))
    if reconcile_consumers:
        # A forced result is shared cache state.  Let each active Stage node
        # re-admit it through the normal targeted drain, while preserving the
        # guarded target scope handled by _attach_result_tx above.
        from .reconcile import reconcile

        for pipeline_name, node_name in reconcile_consumers:
            try:
                reconcile(app, pipeline_name, node_name, at)
            except ExecutionError:
                _STAGE_LOGGER.exception(
                    "forced_result_reconcile_failed",
                    extra={"pipeline": pipeline_name, "node": node_name},
                )
    return True


def _fail_execution(
    app: App,
    execution_id: int,
    error: BaseException,
    at: datetime,
    *,
    drop_invalid: bool = False,
    supersede: bool = False,
) -> int | None:
    """Guard a queued/running attempt and transfer its stable claim.

    ``supersede`` is reserved for startup invalidation of retained web
    snapshots.  It finalizes the current attempt without creating a retry,
    while reusing the normal claim/member/waiter cleanup below.
    """

    message = str(error) or error.__class__.__name__
    at_text = _ts(at)
    retry_id: int | None = None
    result_id: int | None = None
    with write_transaction(app.conn):
        row = app.conn.execute(
            "SELECT * FROM executions WHERE id=? AND kind='stage' AND state IN ('queued','running')",
            (execution_id,),
        ).fetchone()
        if row is None:
            return None
        policy = _decode_policy(str(row["policy_json"]))
        exhausted_invalid = bool(
            drop_invalid
            and isinstance(error, InvalidStructuredOutput)
            and int(row["retry_count"]) >= policy.retries
        )
        terminal_state = "dropped" if exhausted_invalid else "failed"
        guard = app.conn.execute(
            "UPDATE executions SET state=?,error=?,finished_at=? WHERE id=? AND kind='stage' "
            "AND state IN ('queued','running')",
            (terminal_state, message, at_text, execution_id),
        )
        if guard.rowcount != 1:
            return None
        if not supersede and int(row["retry_count"]) < policy.retries:
            request = _StageRequest(
                stage_name=str(row["stage_name"]),
                envelope=StageEnvelope(
                    tuple(
                        str(r[0])
                        for r in app.conn.execute(
                            "SELECT item_id FROM execution_inputs WHERE execution_id=? AND slot='items' ORDER BY ordinal",
                            (execution_id,),
                        )
                    ),
                    tuple(
                        str(r[0])
                        for r in app.conn.execute(
                            "SELECT item_id FROM execution_inputs WHERE execution_id=? AND slot='history' ORDER BY ordinal",
                            (execution_id,),
                        )
                    ),
                ),
                owner=None
                if row["pipeline_name"] is None or row["node_name"] is None
                else (str(row["pipeline_name"]), str(row["node_name"])),
                generation=row["admitted_generation"],
                batch_id=row["batch_id"],
                policy=policy,
                binding_digest=str(row["binding_digest"]),
                history_persisted=bool(row["history_persisted"]),
                rebuild_target_id=row["rebuild_target_id"],
            )
            stage = _decode_stage_snapshot(str(row["config_json"]))
            retry_id = _insert_stage_execution_tx(
                app,
                request,
                stage,
                str(row["lookup_key"]),
                request.envelope,
                "queued",
                at_text,
                retry_count=int(row["retry_count"]) + 1,
                retry_parent_id=execution_id,
            )
            # Keep the claim occupied continuously; no release window allows a
            # duplicate normal owner to be created between attempts.
            app.conn.execute(
                "UPDATE materialization_claims SET owner_execution_id=? WHERE stage_name=? AND lookup_key=?",
                (retry_id, row["stage_name"], row["lookup_key"]),
            )
        elif exhausted_invalid:
            # An exhausted per-item invalid:drop attempt is a terminal empty
            # materialization.  It must occupy the same reusable cache slot as
            # a successful result so later reconciliation cannot call the LLM
            # again for this exact input.
            stage = _decode_stage_snapshot(str(row["config_json"]))
            sequence = (
                int(
                    app.conn.execute(
                        "SELECT COALESCE(MAX(sequence),0) FROM stage_results WHERE stage_name=?",
                        (row["stage_name"],),
                    ).fetchone()[0]
                )
                + 1
            )
            result = app.conn.execute(
                "INSERT INTO stage_results(stage_name,lookup_key,sequence,created_at,producer_execution_id) "
                "VALUES (?,?,?,?,?)",
                (row["stage_name"], row["lookup_key"], sequence, at_text, execution_id),
            )
            result_id = int(result.lastrowid)
            app.conn.execute(
                "INSERT INTO stage_result_current(stage_name,lookup_key,result_id) VALUES (?,?,?) "
                "ON CONFLICT(stage_name,lookup_key) DO UPDATE SET result_id=excluded.result_id",
                (row["stage_name"], row["lookup_key"], result_id),
            )
            app.conn.execute(
                "UPDATE executions SET result_id=? WHERE id=?",
                (result_id, execution_id),
            )
            request = _StageRequest(
                stage_name=stage.name,
                envelope=StageEnvelope(
                    tuple(
                        str(r[0])
                        for r in app.conn.execute(
                            "SELECT item_id FROM execution_inputs WHERE execution_id=? AND slot='items' ORDER BY ordinal",
                            (execution_id,),
                        )
                    ),
                    tuple(
                        str(r[0])
                        for r in app.conn.execute(
                            "SELECT item_id FROM execution_inputs WHERE execution_id=? AND slot='history' ORDER BY ordinal",
                            (execution_id,),
                        )
                    ),
                ),
                owner=None
                if row["pipeline_name"] is None or row["node_name"] is None
                else (str(row["pipeline_name"]), str(row["node_name"])),
                generation=row["admitted_generation"],
                batch_id=row["batch_id"],
                policy=policy,
                binding_digest=str(row["binding_digest"]),
                history_persisted=bool(row["history_persisted"]),
                rebuild_target_id=row["rebuild_target_id"],
            )
            _attach_result_tx(
                app, request, result_id, at_text, execution_id=execution_id
            )
            app.conn.execute(
                "UPDATE materialization_claims SET owner_execution_id=NULL WHERE stage_name=? "
                "AND lookup_key=? AND owner_execution_id=?",
                (row["stage_name"], row["lookup_key"], execution_id),
            )
            _promote_waiters_tx(
                app, str(row["stage_name"]), str(row["lookup_key"]), result_id, at_text
            )
        else:
            app.conn.execute(
                "UPDATE materialization_claims SET owner_execution_id=NULL WHERE stage_name=? AND lookup_key=? AND owner_execution_id=?",
                (row["stage_name"], row["lookup_key"], execution_id),
            )
            if row["batch_id"] is not None:
                app.conn.execute(
                    "UPDATE batches SET state='failed' WHERE id=? AND state='frozen'",
                    (row["batch_id"],),
                )
                if supersede:
                    app.conn.execute(
                        "UPDATE node_state SET current_frozen_batch_id=NULL WHERE "
                        "pipeline_name=? AND node_name=? AND current_frozen_batch_id=?",
                        (row["pipeline_name"], row["node_name"], row["batch_id"]),
                    )
                # Keep the member rows frozen.  The durable failed batch is
                # intentionally still the node's current envelope so a later
                # explicit retry/rebuild can attach its result and clear it.
            else:
                app.conn.execute(
                    "UPDATE node_members SET state='terminal' WHERE pipeline_name=? AND node_name=? "
                    "AND binding_digest=? AND item_id IN (SELECT item_id FROM execution_inputs WHERE execution_id=? AND slot='items') "
                    "AND state IN ('claimed','pending')",
                    (
                        row["pipeline_name"],
                        row["node_name"],
                        row["binding_digest"],
                        execution_id,
                    ),
                )
            _promote_waiters_tx(
                app, str(row["stage_name"]), str(row["lookup_key"]), None, at_text
            )
        log_event(
            _STAGE_LOGGER,
            "stage_failure",
            pipeline=row["pipeline_name"],
            node=row["node_name"],
            stage=row["stage_name"],
            execution_id=execution_id,
            claim_id=None,
            item_ids=[
                str(r[0])
                for r in app.conn.execute(
                    "SELECT item_id FROM execution_inputs WHERE execution_id=? AND slot='items' ORDER BY ordinal",
                    (execution_id,),
                )
            ],
            result_id=result_id,
            state=terminal_state,
            retry_execution_id=retry_id,
        )
    return retry_id


def _stage_deadline(
    started_at: object, policy: ExecutionPolicy, at: datetime
) -> float | None:
    if policy.timeout is None:
        return None
    started = utc_datetime(str(started_at))
    remaining = (started + policy.timeout - utc_datetime(at)).total_seconds()
    return time.monotonic() + remaining


def _execution_probe(
    app: App, execution_id: int, stage_deadline: float | None
) -> WebExecutionProbe:
    def check() -> None:
        with app.conn.transaction_lock:
            row = app.conn.execute(
                "SELECT 1 FROM executions e JOIN materialization_claims mc "
                "ON mc.owner_execution_id=e.id WHERE e.id=? AND e.kind='stage' "
                "AND e.state='running'",
                (execution_id,),
            ).fetchone()
        if row is None:
            raise WebAttemptCancelled()
        if stage_deadline is not None and time.monotonic() >= stage_deadline:
            raise WebStageDeadlineExpired()

    return WebExecutionProbe(checker=check, stage_deadline=stage_deadline)


def _execution_still_owned(app: App, execution_id: int) -> bool:
    with app.conn.transaction_lock:
        row = app.conn.execute(
            "SELECT 1 FROM executions e JOIN materialization_claims mc "
            "ON mc.owner_execution_id=e.id WHERE e.id=? AND e.kind='stage' AND e.state='running'",
            (execution_id,),
        ).fetchone()
    return row is not None


def _execute_claimed_stage(app: App, execution_id: int, at: datetime) -> int:
    if not _start_claimed_execution(app, execution_id, at):
        return execution_id
    with app.conn.transaction_lock:
        row = app.conn.execute(
            "SELECT * FROM executions WHERE id=?", (execution_id,)
        ).fetchone()
    if row is None:
        return execution_id
    stage = _decode_stage_snapshot(str(row["config_json"]))
    policy = _decode_policy(str(row["policy_json"]))
    deadline = _stage_deadline(row["started_at"], policy, at)
    if deadline is not None and time.monotonic() >= deadline:
        _fail_execution(app, execution_id, WebStageDeadlineExpired(), at)
        return execution_id
    token = set_web_execution_probe(_execution_probe(app, execution_id, deadline))
    try:
        items = _items_for_execution(app, execution_id, "items")
        history = _items_for_execution(app, execution_id, "history")
        tasks = [app.tasks[task.use] for task in stage.tasks]
        outcome = run_stage_tasks(
            stage,
            items,
            history,
            tasks,
            web_content_client=getattr(app, "web_content_client", None),
        )
        _finalize_stage(app, execution_id, outcome, at)
        return execution_id
    except WebTaskFailure as failure:
        if terminal_fetch_drop_eligible(
            stage, failure.task_index
        ) and retries_exhausted(row, policy):
            _finalize_stage(
                app,
                execution_id,
                StageOutcome(outputs=(), status="succeeded"),
                at,
                terminal_fetch_drop=True,
                expected_retry_count=int(row["retry_count"]),
            )
            return execution_id
        retry_id = _fail_execution(
            app, execution_id, sanitize_web_error(failure.cause), at
        )
    except WebAttemptCancelled as error:
        if not _execution_still_owned(app, execution_id):
            return execution_id
        retry_id = _fail_execution(app, execution_id, sanitize_web_error(error), at)
    except WebStageDeadlineExpired as error:
        retry_id = _fail_execution(app, execution_id, sanitize_web_error(error), at)
    except InvalidStructuredOutput as error:
        retry_id = _fail_execution(
            app,
            execution_id,
            error,
            at,
            drop_invalid=(
                stage.input_mode == "each"
                and any(
                    task.use == "sluice.tasks.llm_extract"
                    and task.with_config.get("invalid") == "drop"
                    for task in stage.tasks
                )
            ),
        )
    except BaseException as error:  # noqa: BLE001 - finalize the durable claim on any exit
        has_web_task = any(task.web_binding is not None for task in stage.tasks)
        retry_id = _fail_execution(
            app,
            execution_id,
            sanitize_web_error(error) if has_web_task else error,
            at,
        )
    finally:
        reset_web_execution_probe(token)
    if retry_id is not None:
        return _execute_claimed_stage(app, retry_id, at)
    return execution_id


def _await_waiter_execution(
    app: App, request: _StageRequest, owner_execution_id: int, at: datetime
) -> int:
    """Wait for a shared owner and return this caller's terminal attempt.

    Waiters do not consume an execution attempt while an owner is active.  A
    successful owner promotes them to a cached terminal Execution; if the
    owner exhausts and promotes this waiter as the next owner, this caller
    executes that queued attempt and then observes its terminal result.
    """

    stage = _stage_for_request(app, request)
    items = _load_items(app, request.envelope.items)
    history = _load_items(app, request.envelope.history)
    lookup_key = stage_lookup_key(stage, items, history)
    owner_id = int(owner_execution_id)
    while True:
        current = app.conn.execute(
            "SELECT result_id FROM stage_result_current WHERE stage_name=? AND lookup_key=?",
            (stage.name, lookup_key),
        ).fetchone()
        if current is not None:
            cached = app.conn.execute(
                "SELECT id FROM executions WHERE kind='stage' AND state='cached' "
                "AND stage_name=? AND lookup_key=? AND pipeline_name IS ? AND node_name IS ? "
                "AND result_id=? ORDER BY id DESC LIMIT 1",
                (
                    stage.name,
                    lookup_key,
                    request.owner[0] if request.owner else None,
                    request.owner[1] if request.owner else None,
                    int(current[0]),
                ),
            ).fetchone()
            if cached is not None:
                return int(cached[0])
            outcome = _request_materialization(app, request, at)
            if outcome.execution_id is not None:
                return int(outcome.execution_id)

        claim = app.conn.execute(
            "SELECT owner_execution_id FROM materialization_claims WHERE stage_name=? AND lookup_key=?",
            (stage.name, lookup_key),
        ).fetchone()
        if claim is not None and claim[0] is not None:
            claim_owner = int(claim[0])
            execution = app.conn.execute(
                "SELECT state,pipeline_name,node_name FROM executions WHERE id=?",
                (claim_owner,),
            ).fetchone()
            if (
                execution is not None
                and execution[0] == "queued"
                and request.owner is not None
                and execution[1] == request.owner[0]
                and execution[2] == request.owner[1]
            ):
                owner_id = claim_owner
                _execute_claimed_stage(app, owner_id, at)
                continue
            owner_id = claim_owner
        else:
            # A normal waiter can be discarded when reconciliation makes its
            # node member stale while the shared owner is failing.  In that
            # case promotion intentionally consumes the waiter row without
            # creating an Execution; stop polling instead of spinning forever.
            waiter = app.conn.execute(
                "SELECT 1 FROM materialization_waiters WHERE stage_name=? AND lookup_key=? "
                "AND pipeline_name IS ? AND node_name IS ? AND envelope_json=?",
                (
                    stage.name,
                    lookup_key,
                    request.owner[0] if request.owner else None,
                    request.owner[1] if request.owner else None,
                    _envelope_json(request.envelope),
                ),
            ).fetchone()
            if waiter is None:
                raise ExecutionError(
                    "Stage waiter was discarded after the shared owner failed"
                )
            if owner_id:
                execution = app.conn.execute(
                    "SELECT state FROM executions WHERE id=?", (owner_id,)
                ).fetchone()
                if execution is not None and execution[0] in {
                    "failed",
                    "dropped",
                    "succeeded",
                    "cached",
                }:
                    # The owner has reached a terminal state; continue
                    # polling the durable waiter promotion.
                    pass
        time.sleep(0.001)


def execute_stage(
    app: App,
    stage_name: str,
    envelope: StageEnvelope,
    *,
    pipeline_name: str,
    node_name: str,
    generation: int | None,
    batch_id: int | None,
    at: datetime,
) -> int:
    """Synchronously request and execute one persisted Stage envelope."""

    from .app import App as AppType

    if not isinstance(app, AppType):
        raise ExecutionError("execute_stage requires an App")
    if not isinstance(envelope, StageEnvelope):
        raise ExecutionError("execute_stage requires a StageEnvelope")
    _binding, _node = _node_helpers()
    node = _node(app, pipeline_name, node_name)
    if node.stage != stage_name:
        raise ExecutionError(
            f"node {pipeline_name}.{node_name} does not use Stage {stage_name!r}"
        )
    binding = _binding(app, pipeline_name, node_name)
    request = _StageRequest(
        stage_name=stage_name,
        envelope=envelope,
        owner=(pipeline_name, node_name),
        generation=generation,
        batch_id=batch_id,
        policy=node.policy,
        binding_digest=binding,
    )
    outcome = _request_materialization(app, request, at)
    if outcome.owner and outcome.execution_id is not None:
        return _execute_claimed_stage(app, outcome.execution_id, at)
    if outcome.execution_id is None:
        raise ExecutionError("Stage request did not produce an Execution")
    if outcome.waiting:
        return _await_waiter_execution(
            app,
            outcome.request if outcome.request is not None else request,
            int(outcome.execution_id),
            at,
        )
    return int(outcome.execution_id)


def rebuild_stage(app: App, execution_id: int, at: datetime) -> RebuildResult:
    """Force a Stage materialization from one exact retained Execution.

    The target's canonical Stage snapshot, binding, policy, and input
    envelope are copied into a private request.  A currently running owner
    receives a durable forced waiter; otherwise the forced owner executes
    synchronously and this action reports its terminal state.
    """

    from .app import App as AppType

    if not isinstance(app, AppType):
        raise ExecutionError("rebuild_stage requires an App")
    envelope = verify_execution_envelope(app, execution_id)
    with app.conn.transaction_lock:
        target = app.conn.execute(
            "SELECT * FROM executions WHERE id=? AND kind='stage'", (execution_id,)
        ).fetchone()
    if target is None:
        # The verifier already guards this branch; keep a stable public error
        # if a connection implementation changes between the reads.
        raise ExecutionError(f"Stage Execution is missing: {execution_id}")
    try:
        raw_snapshot = json.loads(str(target["config_json"]))
    except (TypeError, ValueError, json.JSONDecodeError) as error:
        raise ExecutionError("persisted Stage configuration is invalid JSON") from error
    if not isinstance(raw_snapshot, Mapping):
        raise ExecutionError("persisted Stage configuration must be an object")
    retained_stage = decode_stage_snapshot(raw_snapshot)  # type: ignore[arg-type]
    if retained_stage.name != str(target["stage_name"]):
        raise ExecutionError("Stage snapshot name does not match Execution")
    legacy_web_uses = {
        "sluice.tasks.fetch_webpage",
        "sluice.tasks.extract_content",
        "sluice.tasks.to_markdown",
        "sluice.tasks.to_text",
    }
    for task in retained_stage.tasks:
        if task.use in legacy_web_uses and task.web_binding is None:
            raise DefinitionError("legacy web snapshot cannot rebuild")
        if task.use in BUILTIN_LLM_TASKS and not app.llm_available:
            raise DefinitionError(f"{task.use} cannot rebuild without an LLM client")
        if task.web_binding is not None:
            catalog = getattr(app, "web_runtime_catalog", None)
            if catalog is not None:
                for provider in task.web_binding.providers:
                    # Explicit rebuild is the one point where a retained
                    # terminal snapshot must resolve its old secret refs.
                    catalog.register_provider(provider, allow_missing_secrets=False)
    pipeline_name = target["pipeline_name"]
    node_name = target["node_name"]
    owner = (
        None
        if pipeline_name is None or node_name is None
        else (str(pipeline_name), str(node_name))
    )
    binding_digest = target["binding_digest"]
    if not isinstance(binding_digest, str) or not binding_digest:
        raise ExecutionError("Stage Execution binding is invalid")
    generation = target["admitted_generation"]
    batch_id = target["batch_id"]
    request = _StageRequest(
        stage_name=retained_stage.name,
        envelope=envelope,
        owner=owner,
        generation=None if generation is None else int(generation),
        batch_id=None if batch_id is None else int(batch_id),
        policy=_decode_policy(str(target["policy_json"])),
        binding_digest=binding_digest,
        force=True,
        retained_stage=retained_stage,
        rebuild_target_id=int(execution_id),
        history_persisted=bool(target["history_persisted"]),
    )
    outcome = _request_materialization(app, request, at)
    if outcome.waiting:
        return RebuildResult(execution_id=None, status="queued", forced=True)
    if outcome.execution_id is None:
        raise ExecutionError("Stage rebuild did not produce an Execution")
    result_id = int(outcome.execution_id)
    if outcome.owner:
        result_id = _execute_claimed_stage(app, result_id, at)
    row = app.conn.execute(
        "SELECT state FROM executions WHERE id=? AND kind='stage'", (result_id,)
    ).fetchone()
    if row is None:
        raise ExecutionError("Stage rebuild Execution disappeared")
    state = str(row[0])
    if state in {"succeeded", "cached"}:
        status: Literal["queued", "running", "succeeded", "failed"] = "succeeded"
    elif state == "running":
        status = "running"
    elif state in {"queued"}:
        status = "queued"
    else:
        status = "failed"
    return RebuildResult(execution_id=result_id, status=status, forced=True)


__all__ = [
    "RebuildResult",
    "_RequestOutcome",
    "_StageRequest",
    "_fail_execution",
    "_finalize_stage",
    "_request_materialization",
    "_start_claimed_execution",
    "execute_stage",
    "rebuild_stage",
    "resolve_dedup_history_ids",
    "run_stage_tasks",
    "validate_task_outputs",
    "verify_execution_envelope",
]
