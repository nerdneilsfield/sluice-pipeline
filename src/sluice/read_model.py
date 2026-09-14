"""Deterministic, SQLite-backed management projections.

The runtime tables are an implementation detail of the reconciler.  This
module is the small read-only boundary used by the management UI and CLI
actions: it projects the current definition and durable rows into immutable
dataclasses without introducing another state machine or a JSON API.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Literal

from .app import App
from .canonical import utc_datetime
from .delivery import retry_delivery
from .errors import ExecutionError
from .stage_runtime import RebuildResult, rebuild_stage

PipelineState = Literal["idle", "waiting", "running", "failed"]


@dataclass(frozen=True, slots=True)
class PipelineSummary:
    name: str
    state: PipelineState
    pending_count: int
    failed_count: int
    latest_delivery: str | None
    latest_error: str | None


@dataclass(frozen=True, slots=True)
class NodeDetail:
    name: str
    stage: str | None
    executions: tuple[int, ...]
    frozen_items: tuple[str, ...]
    deliveries: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PipelineDetail:
    name: str
    nodes: tuple[NodeDetail, ...]
    frozen_batches: Mapping[str, tuple[str, ...]]
    deliveries: Mapping[str, str]

    def __post_init__(self) -> None:
        # Accept ordinary mappings at the constructor boundary while keeping
        # the published projection immutable to callers.
        object.__setattr__(
            self, "frozen_batches", MappingProxyType(dict(self.frozen_batches))
        )
        object.__setattr__(self, "deliveries", MappingProxyType(dict(self.deliveries)))


def _require_app(app: App) -> None:
    if not isinstance(app, App):
        raise ExecutionError("management read model requires an App")


def _pipeline(app: App, name: str):
    _require_app(app)
    try:
        return app.definition.pipelines[name]
    except KeyError as error:
        raise ExecutionError(f"unknown Pipeline: {name}") from error


def _current_node_binding(app: App, pipeline_name: str, node_name: str) -> str | None:
    row = app.conn.execute(
        "SELECT binding_digest FROM node_current_bindings "
        "WHERE pipeline_name=? AND node_name=?",
        (pipeline_name, node_name),
    ).fetchone()
    return None if row is None else str(row[0])


def _current_source_binding(app: App, source_name: str) -> str | None:
    row = app.conn.execute(
        "SELECT binding_digest FROM source_current_bindings WHERE source_name=?",
        (source_name,),
    ).fetchone()
    return None if row is None else str(row[0])


def _current_batch_id(
    app: App, pipeline_name: str, node_name: str, binding: str
) -> int | None:
    row = app.conn.execute(
        "SELECT current_frozen_batch_id FROM node_state WHERE pipeline_name=? "
        "AND node_name=? AND binding_digest=?",
        (pipeline_name, node_name, binding),
    ).fetchone()
    return None if row is None or row[0] is None else int(row[0])


def _batch_state(app: App, batch_id: int | None) -> str | None:
    if batch_id is None:
        return None
    row = app.conn.execute(
        "SELECT state FROM batches WHERE id=?", (batch_id,)
    ).fetchone()
    return None if row is None else str(row[0])


def _execution_children(app: App, execution_id: int) -> bool:
    return (
        app.conn.execute(
            "SELECT 1 FROM executions WHERE retry_parent_id=? LIMIT 1", (execution_id,)
        ).fetchone()
        is not None
    )


def _execution_scope_current(
    app: App, row, *, require_unsatisfied: bool = False
) -> bool:
    """Return whether one Stage row still belongs to active node work.

    A binding digest is the durable scope boundary.  The member checks below
    additionally exclude an old generation after a source/node publication
    replacement, which prevents historical failures from poisoning health or
    becoming the default rebuild target.
    """

    pipeline_name = row["pipeline_name"]
    node_name = row["node_name"]
    if pipeline_name is None or node_name is None or row["kind"] != "stage":
        return False
    binding = _current_node_binding(app, str(pipeline_name), str(node_name))
    if binding is None or binding != str(row["binding_digest"]):
        return False
    if int(row["target_scope_excluded"] or 0):
        return False
    item_rows = app.conn.execute(
        "SELECT item_id FROM execution_inputs WHERE execution_id=? AND slot='items' "
        "ORDER BY ordinal",
        (int(row["id"]),),
    ).fetchall()
    if not item_rows:
        return False
    item_ids = [str(item_row[0]) for item_row in item_rows]
    placeholders = ",".join("?" for _ in item_ids)
    members = app.conn.execute(
        "SELECT item_id,generation,state,visible_result_id,lookup_key FROM node_members "
        "WHERE pipeline_name=? AND node_name=? AND binding_digest=? "
        f"AND item_id IN ({placeholders})",
        (str(pipeline_name), str(node_name), binding, *item_ids),
    ).fetchall()
    if len(members) != len(item_ids):
        return False
    by_id = {str(member["item_id"]): member for member in members}
    for item_id in item_ids:
        member = by_id.get(item_id)
        if member is None or str(member["state"]) == "stale":
            return False
        generation = row["admitted_generation"]
        if generation is not None and int(member["generation"]) != int(generation):
            return False
        if require_unsatisfied and member["visible_result_id"] is not None:
            return False
    batch_id = row["batch_id"]
    if batch_id is not None:
        current_batch = _current_batch_id(
            app, str(pipeline_name), str(node_name), binding
        )
        if current_batch != int(batch_id):
            return False
        if require_unsatisfied and _batch_state(app, current_batch) != "failed":
            return False
    return True


def _leaf_stage_rows(
    app: App, pipeline_name: str, node_name: str, binding: str
) -> list:
    rows = app.conn.execute(
        "SELECT * FROM executions WHERE kind='stage' AND pipeline_name=? "
        "AND node_name=? AND binding_digest=? ORDER BY id",
        (pipeline_name, node_name, binding),
    ).fetchall()
    # Retry chains are represented by parent/child rows.  The child is the
    # latest attempt; exposing only leaves keeps history deterministic while
    # retaining every independent lookup/batch execution.
    return [
        row
        for row in rows
        if not _execution_children(app, int(row["id"]))
        and _execution_scope_current(app, row)
    ]


def _failed_stage_rows(
    app: App, pipeline_name: str, node_name: str, binding: str
) -> list:
    return [
        row
        for row in _leaf_stage_rows(app, pipeline_name, node_name, binding)
        if str(row["state"]) == "failed"
        and _execution_scope_current(app, row, require_unsatisfied=True)
    ]


def _source_failure_rows(app: App, source_name: str) -> list:
    binding = _current_source_binding(app, source_name)
    if binding is None:
        return []
    rows = app.conn.execute(
        "SELECT * FROM executions WHERE kind='source' AND source_name=? "
        "AND binding_digest=? ORDER BY id",
        (source_name, binding),
    ).fetchall()
    # Source collections are serialized by the active binding.  A later
    # successful collection satisfies an earlier failed attempt even when it
    # was not created as a retry child (for example, a later scheduled tick).
    if not rows:
        return []
    row = rows[-1]
    if str(row["state"]) != "failed" or _execution_children(app, int(row["id"])):
        return []
    return [row]


def _delivery_rows(app: App, pipeline_name: str):
    return app.conn.execute(
        "SELECT rowid AS _rm_rowid,* FROM deliveries WHERE pipeline_name=? "
        "ORDER BY rowid,delivery_key",
        (pipeline_name,),
    ).fetchall()


def _current_delivery_rows(app: App, pipeline_name: str):
    pipeline = _pipeline(app, pipeline_name)
    rows = []
    for row in _delivery_rows(app, pipeline_name):
        node_name = str(row["node_name"])
        node = pipeline.nodes.get(node_name)
        if node is None or node.deliver is None:
            continue
        # Delivery binding is calculated from immutable YAML and compared to
        # the row snapshot so a changed endpoint remains historical only.  A
        # durable failed/succeeded row whose rendered member was later omitted
        # remains auditable but is no longer current health.
        from .delivery import _binding, _node_binding

        try:
            current_binding = _binding(app, pipeline_name, node_name)
        except ExecutionError:
            continue
        if str(row["binding_digest"]) != current_binding:
            continue
        member = app.conn.execute(
            "SELECT state FROM node_members WHERE pipeline_name=? AND node_name=? "
            "AND binding_digest=? AND item_id=?",
            (
                pipeline_name,
                node_name,
                _node_binding(app, pipeline_name, node_name),
                str(row["rendered_item_id"]),
            ),
        ).fetchone()
        if member is None or str(member[0]) == "stale":
            continue
        rows.append(row)
    return rows


def _pending_for_node(
    app: App, pipeline_name: str, node_name: str, binding: str
) -> int:
    pending = int(
        app.conn.execute(
            "SELECT COUNT(*) FROM node_members WHERE pipeline_name=? AND node_name=? "
            "AND binding_digest=? AND state='pending'",
            (pipeline_name, node_name, binding),
        ).fetchone()[0]
    )
    # Queued owners/retries and waiters are work even when their node member
    # has moved from pending to claimed/frozen.
    queued = int(
        app.conn.execute(
            "SELECT COUNT(*) FROM executions WHERE kind='stage' AND pipeline_name=? "
            "AND node_name=? AND binding_digest=? AND state='queued'",
            (pipeline_name, node_name, binding),
        ).fetchone()[0]
    )
    waiters = int(
        app.conn.execute(
            "SELECT COUNT(*) FROM materialization_waiters WHERE pipeline_name=? "
            "AND node_name=? AND (binding_digest IS NULL OR binding_digest=?)",
            (pipeline_name, node_name, binding),
        ).fetchone()[0]
    )
    return pending + queued + waiters


def _delivery_retry_limit(row) -> int:
    try:
        raw = json.loads(str(row["policy_json"]))
        value = raw.get("retries", 0) if isinstance(raw, Mapping) else 0
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            return 0
        return value
    except (TypeError, ValueError, json.JSONDecodeError):
        return 0


def _delivery_is_retryable(row) -> bool:
    return str(row["state"]) == "failed" and int(
        row["retry_count"]
    ) < _delivery_retry_limit(row)


def _delivery_pending_count(rows: list) -> int:
    return sum(
        1
        for row in rows
        if str(row["state"]) == "queued" or _delivery_is_retryable(row)
    )


def _delivery_failed_rows(rows: list) -> list:
    return [row for row in rows if str(row["state"]) == "failed"]


def _pipeline_health(
    app: App, pipeline_name: str
) -> tuple[PipelineState, int, int, str | None, str | None]:
    pipeline = _pipeline(app, pipeline_name)
    pending_count = 0
    failed_rows: list[tuple[str, int, str]] = []
    running = False
    waiting = False

    # Source failures/runs affect every Pipeline that references the source.
    referenced_sources = {
        ref[len("source.") :]
        for node in pipeline.nodes.values()
        for ref in node.from_refs
        if ref.startswith("source.")
    }
    for source_name in referenced_sources:
        binding = _current_source_binding(app, source_name)
        if binding is None:
            continue
        active = app.conn.execute(
            "SELECT e.* FROM source_state ss JOIN executions e ON e.id=ss.active_execution_id "
            "WHERE ss.source_name=? AND ss.binding_digest=?",
            (source_name, binding),
        ).fetchone()
        if active is not None:
            state = str(active["state"])
            if state == "running":
                running = True
            elif state == "queued":
                waiting = True
                pending_count += 1
        failures = _source_failure_rows(app, source_name)
        for row in failures:
            failed_rows.append(
                (
                    str(row["finished_at"] or ""),
                    int(row["id"]),
                    str(row["error"] or "source failed"),
                )
            )

    for node_name, node in pipeline.nodes.items():
        binding = _current_node_binding(app, pipeline_name, node_name)
        if binding is None:
            continue
        if node.deliver is not None:
            rows = _current_delivery_rows(app, pipeline_name)
            rows = [row for row in rows if str(row["node_name"]) == node_name]
            pending_count += _delivery_pending_count(rows)
            failed = _delivery_failed_rows(rows)
            for row in failed:
                if _delivery_is_retryable(row):
                    waiting = True
                    continue
                failed_rows.append(
                    (
                        str(row["updated_at"] or ""),
                        int(row["_rm_rowid"]),
                        str(row["error"] or "delivery failed"),
                    )
                )
            if any(str(row["state"]) == "running" for row in rows):
                running = True
            if any(str(row["state"]) == "queued" for row in rows):
                waiting = True
            continue

        pending = _pending_for_node(app, pipeline_name, node_name, binding)
        pending_count += pending
        if pending:
            waiting = True
        stage_rows = _leaf_stage_rows(app, pipeline_name, node_name, binding)
        if any(str(row["state"]) == "running" for row in stage_rows):
            running = True
        if any(str(row["state"]) == "queued" for row in stage_rows):
            waiting = True
        failed = _failed_stage_rows(app, pipeline_name, node_name, binding)
        for row in failed:
            failed_rows.append(
                (
                    str(row["finished_at"] or ""),
                    int(row["id"]),
                    str(row["error"] or "Stage failed"),
                )
            )
        current_batch = _current_batch_id(app, pipeline_name, node_name, binding)
        if current_batch is not None and _batch_state(app, current_batch) in {
            "frozen",
            "failed",
        }:
            waiting = True

    deliveries = _current_delivery_rows(app, pipeline_name)
    latest_success = None
    successful = [row for row in deliveries if str(row["state"]) == "succeeded"]
    if successful:
        latest = max(
            successful,
            key=lambda row: (str(row["updated_at"] or ""), int(row["_rm_rowid"])),
        )
        latest_success = str(latest["delivery_key"])
    failed_rows.sort(key=lambda value: (value[0], value[1]))
    failed_count = len(failed_rows)
    latest_error = failed_rows[-1][2] if failed_rows else None
    state: PipelineState
    if failed_count:
        state = "failed"
    elif running:
        state = "running"
    elif waiting:
        state = "waiting"
    else:
        state = "idle"
    return state, pending_count, failed_count, latest_success, latest_error


def pipeline_list(app: App) -> tuple[PipelineSummary, ...]:
    """Project every configured Pipeline in YAML order."""

    _require_app(app)
    result: list[PipelineSummary] = []
    for name in app.definition.pipelines:
        state, pending, failed, latest_delivery, latest_error = _pipeline_health(
            app, name
        )
        result.append(
            PipelineSummary(
                name=name,
                state=state,
                pending_count=pending,
                failed_count=failed,
                latest_delivery=latest_delivery,
                latest_error=latest_error,
            )
        )
    return tuple(result)


def pipeline_detail(app: App, pipeline_name: str) -> PipelineDetail:
    """Return a deterministic DAG/execution/batch/Delivery projection."""

    pipeline = _pipeline(app, pipeline_name)
    node_details: list[NodeDetail] = []
    frozen_batches: dict[str, tuple[str, ...]] = {}
    delivery_states: dict[str, str] = {}
    all_deliveries = _current_delivery_rows(app, pipeline_name)
    for row in all_deliveries:
        delivery_states[str(row["delivery_key"])] = str(row["state"])

    for node_name, node in pipeline.nodes.items():
        binding = _current_node_binding(app, pipeline_name, node_name)
        executions: tuple[int, ...] = ()
        frozen_items: tuple[str, ...] = ()
        delivery_keys: tuple[str, ...] = ()
        if binding is not None:
            if node.stage is not None:
                executions = tuple(
                    int(row["id"])
                    for row in _leaf_stage_rows(app, pipeline_name, node_name, binding)
                )
                batch_id = _current_batch_id(app, pipeline_name, node_name, binding)
                state = _batch_state(app, batch_id)
                if batch_id is not None and state in {"frozen", "failed"}:
                    rows = app.conn.execute(
                        "SELECT item_id FROM batch_items WHERE batch_id=? ORDER BY ordinal",
                        (batch_id,),
                    ).fetchall()
                    frozen_items = tuple(str(row[0]) for row in rows)
                    frozen_batches[node_name] = frozen_items
            else:
                delivery_keys = tuple(
                    str(row["delivery_key"])
                    for row in all_deliveries
                    if str(row["node_name"]) == node_name
                )
        node_details.append(
            NodeDetail(
                name=node_name,
                stage=node.stage,
                executions=executions,
                frozen_items=frozen_items,
                deliveries=delivery_keys,
            )
        )
    return PipelineDetail(
        name=pipeline_name,
        nodes=tuple(node_details),
        frozen_batches=MappingProxyType(frozen_batches),
        deliveries=MappingProxyType(delivery_states),
    )


def default_rebuild_target(app: App, pipeline_name: str, node_name: str) -> int | None:
    """Return the newest failed Stage attempt still unsatisfied in this node."""

    pipeline = _pipeline(app, pipeline_name)
    node = pipeline.nodes.get(node_name)
    if node is None:
        raise ExecutionError(f"unknown Pipeline node: {pipeline_name}.{node_name}")
    if node.stage is None:
        return None
    binding = _current_node_binding(app, pipeline_name, node_name)
    if binding is None:
        return None
    candidates = _failed_stage_rows(app, pipeline_name, node_name, binding)
    if not candidates:
        return None
    return int(max(candidates, key=lambda row: int(row["id"]))["id"])


def rebuild_action(app: App, execution_id: int, at) -> RebuildResult:
    """Run the production exact-input Stage rebuild action."""

    return rebuild_stage(app, execution_id, utc_datetime(at))


def retry_delivery_action(app: App, delivery_key: str, at) -> str:
    """Run the production independent Delivery retry action."""

    return retry_delivery(app, delivery_key, utc_datetime(at))


__all__ = [
    "NodeDetail",
    "PipelineDetail",
    "PipelineSummary",
    "default_rebuild_target",
    "pipeline_detail",
    "pipeline_list",
    "rebuild_action",
    "retry_delivery_action",
]
