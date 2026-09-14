"""Durable Pipeline publication admission and deterministic batch freezing.

Task 5 deliberately stops at admission.  This module never invokes a Source,
Task, Stage, or Delivery callable; it only reconciles retained Publications
into SQLite membership and freezes immutable envelopes for the later runner.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Any

from .app import App
from .canonical import canonical_json, digest, node_binding_digest, utc_datetime
from .contracts import BatchPolicy, NodeDef, StageEnvelope
from .db import write_transaction
from .errors import ExecutionError
from .logging import log_event
from .source_runtime import (
    _binding_digest as source_binding_digest,
)
from .source_runtime import (
    _execute_queued_source,
    _recover_source_execution,
    collect_source,
)

_LOGGER = logging.getLogger("sluice.reconcile")


def _timestamp(value: datetime) -> str:
    return utc_datetime(value).isoformat().replace("+00:00", "Z")


def _decode_timestamp(value: str | None) -> datetime | None:
    if value is None:
        return None
    return utc_datetime(value)


def _node(app: App, pipeline_name: str, node_name: str) -> NodeDef:
    try:
        pipeline = app.definition.pipelines[pipeline_name]
        return pipeline.nodes[node_name]
    except KeyError as error:
        raise ExecutionError(
            f"unknown Pipeline node: {pipeline_name}.{node_name}"
        ) from error


def _stage_for_node(app: App, node: NodeDef):
    if node.stage is None:
        return None
    try:
        return app.definition.stages[node.stage]
    except KeyError as error:
        # Definitions are validated at load time, but retaining this guard
        # makes a live-definition replacement fail before touching SQLite.
        raise ExecutionError(f"unknown Stage {node.stage!r}") from error


def _batch_json(policy: BatchPolicy | None) -> str | None:
    if policy is None:
        return None
    return canonical_json(
        {
            "count": policy.count,
            "max_wait_seconds": (
                None if policy.max_wait is None else policy.max_wait.total_seconds()
            ),
            "on_schedule": policy.on_schedule,
            "include_updates": policy.include_updates,
        }
    )


def _schedule_json(schedule: timedelta | None) -> str:
    return canonical_json(
        {"seconds": None if schedule is None else schedule.total_seconds()}
    )


def _binding(app: App, pipeline_name: str, node_name: str) -> str:
    node = _node(app, pipeline_name, node_name)
    return node_binding_digest(
        pipeline_name,
        node_name,
        _stage_for_node(app, node),
        node.from_refs,
        node.schedule,
        node.batch,
        node.deliver,
    )


def freeze_eligible(
    policy: BatchPolicy,
    pending_count: int,
    first_pending_at: datetime | None,
    at: datetime,
    schedule_due: bool,
) -> bool:
    """Return whether a pending batch satisfies one durable freeze trigger."""

    if not isinstance(policy, BatchPolicy):
        raise ExecutionError("freeze_eligible requires a BatchPolicy")
    if isinstance(pending_count, bool) or not isinstance(pending_count, int):
        raise ExecutionError("pending_count must be an integer")
    if pending_count < 0:
        raise ExecutionError("pending_count must be non-negative")
    if first_pending_at is not None and not isinstance(first_pending_at, datetime):
        raise ExecutionError("first_pending_at must be a datetime or None")
    if not isinstance(at, datetime):
        raise ExecutionError("at must be a datetime")
    if not isinstance(schedule_due, bool):
        raise ExecutionError("schedule_due must be a boolean")
    now = utc_datetime(at)
    first = None if first_pending_at is None else utc_datetime(first_pending_at)
    if pending_count == 0:
        return False
    if policy.count is not None and pending_count >= policy.count:
        return True
    if (
        policy.max_wait is not None
        and first is not None
        and now >= first + policy.max_wait
    ):
        return True
    return bool(policy.on_schedule and schedule_due)


def _emit(
    event: str,
    *,
    pipeline_name: str,
    node_name: str,
    binding: str,
    publication_id: int | None,
    item_ids: Sequence[str],
    batch_id: int | None,
    state: str,
) -> None:
    log_event(
        _LOGGER,
        event,
        pipeline=pipeline_name,
        node=node_name,
        binding_digest=binding,
        publication_id=publication_id,
        item_ids=list(item_ids),
        batch_id=batch_id,
        state=state,
    )


def _producer_for_ref_tx(
    app: App,
    pipeline_name: str,
    ref: str,
    at_text: str,
) -> tuple[str, str, str]:
    """Return ``(producer_key, binding_digest, kind)`` for one edge."""

    if ref.startswith("source."):
        source_name = ref[len("source.") :]
        try:
            source = app.definition.sources[source_name]
        except KeyError as error:
            raise ExecutionError(f"unknown Source reference {ref!r}") from error
        binding = _ensure_source_binding_tx(app, source_name, source, at_text)
        return source_name, binding, "source"

    # An upstream node's current pointer is authoritative.  Ensure that an
    # un-reconciled upstream node has its immutable empty snapshot first.
    upstream_binding = _ensure_node_binding_tx(app, pipeline_name, ref, at_text, set())
    current = app.conn.execute(
        "SELECT binding_digest FROM node_current_bindings "
        "WHERE pipeline_name=? AND node_name=?",
        (pipeline_name, ref),
    ).fetchone()
    if current is None:
        # The initializer above always inserts this pointer; retaining a
        # defensive failure avoids silently admitting an edge to no producer.
        raise ExecutionError(
            f"upstream node binding is unavailable: {pipeline_name}.{ref}"
        )
    return f"{pipeline_name}.{ref}", str(current[0]), "node"


def _ensure_source_binding_tx(
    app: App, source_name: str, source: Any, at_text: str
) -> str:
    """Activate the configured Source binding before reading its edges."""

    binding = source_binding_digest(source_name, source)
    current = app.conn.execute(
        "SELECT binding_digest FROM source_current_bindings WHERE source_name=?",
        (source_name,),
    ).fetchone()
    current_binding = None if current is None else str(current[0])
    if current_binding != binding:
        if current_binding is not None:
            old_state = app.conn.execute(
                "SELECT active_execution_id FROM source_state WHERE source_name=? AND binding_digest=?",
                (source_name, current_binding),
            ).fetchone()
            old_execution_id = None if old_state is None else old_state[0]
            if old_execution_id is not None:
                app.conn.execute(
                    "UPDATE executions SET state='failed',error=?,finished_at=? "
                    "WHERE id=? AND kind='source' AND state IN ('queued','running')",
                    ("superseded binding", at_text, old_execution_id),
                )
                app.conn.execute(
                    "UPDATE source_state SET active_execution_id=NULL "
                    "WHERE source_name=? AND binding_digest=? AND active_execution_id=?",
                    (source_name, current_binding, old_execution_id),
                )
        app.conn.execute(
            "INSERT OR IGNORE INTO source_state "
            "(source_name,binding_digest,checkpoint_json,next_due_at,active_execution_id) "
            "VALUES (?,?,NULL,?,NULL)",
            (
                source_name,
                binding,
                at_text if source.schedule is not None else None,
            ),
        )
        # A binding activated by targeted reconciliation represents an empty
        # source until its first scheduled/manual collection.  Keep that
        # durable cursor separate from historical binding publications.
        revision_zero = app.conn.execute(
            "SELECT 1 FROM publications WHERE producer_key=? AND binding_digest=? AND revision=0",
            (source_name, binding),
        ).fetchone()
        if revision_zero is None:
            empty_digest = digest(
                {
                    "kind": "delta",
                    "producer_key": source_name,
                    "binding_digest": binding,
                    "members": [],
                }
            )
            app.conn.execute(
                "INSERT INTO publications(kind,producer_key,binding_digest,revision,digest) "
                "VALUES ('delta',?,?,0,?)",
                (source_name, binding, empty_digest),
            )
        app.conn.execute(
            "INSERT INTO source_current_bindings(source_name,binding_digest) VALUES (?,?) "
            "ON CONFLICT(source_name) DO UPDATE SET binding_digest=excluded.binding_digest",
            (source_name, binding),
        )
    return binding


def _ensure_node_binding_tx(
    app: App,
    pipeline_name: str,
    node_name: str,
    at_text: str,
    active: set[tuple[str, str]],
) -> str:
    marker = (pipeline_name, node_name)
    if marker in active:
        raise ExecutionError(
            f"cycle detected while initializing {pipeline_name}.{node_name}"
        )
    active.add(marker)
    try:
        node = _node(app, pipeline_name, node_name)
        binding = _binding(app, pipeline_name, node_name)
        producer_key = f"{pipeline_name}.{node_name}"
        empty_digest = digest(
            {
                "kind": "snapshot",
                "producer_key": producer_key,
                "binding_digest": binding,
                "members": [],
            }
        )
        publication = app.conn.execute(
            "SELECT id FROM publications WHERE producer_key=? AND binding_digest=? AND revision=0",
            (producer_key, binding),
        ).fetchone()
        if publication is None:
            publication = app.conn.execute(
                "INSERT INTO publications(kind,producer_key,binding_digest,revision,digest) "
                "VALUES ('snapshot',?,?,0,?)",
                (producer_key, binding, empty_digest),
            )
            publication_id = int(publication.lastrowid)
        else:
            publication_id = int(publication[0])

        schedule_json = _schedule_json(node.schedule)
        batch_json = _batch_json(node.batch)
        app.conn.execute(
            "INSERT OR IGNORE INTO node_state "
            "(pipeline_name,node_name,binding_digest,next_due_at,first_pending_at,"
            "current_frozen_batch_id,current_publication_id,schedule_json,batch_policy_json) "
            "VALUES (?,?,?,NULL,NULL,NULL,?,?,?)",
            (
                pipeline_name,
                node_name,
                binding,
                publication_id,
                schedule_json,
                batch_json,
            ),
        )
        old_pointer = app.conn.execute(
            "SELECT binding_digest FROM node_current_bindings "
            "WHERE pipeline_name=? AND node_name=?",
            (pipeline_name, node_name),
        ).fetchone()
        previous = None if old_pointer is None else str(old_pointer[0])
        app.conn.execute(
            "INSERT INTO node_current_bindings(pipeline_name,node_name,binding_digest) "
            "VALUES (?,?,?) ON CONFLICT(pipeline_name,node_name) DO UPDATE "
            "SET binding_digest=excluded.binding_digest",
            (pipeline_name, node_name, binding),
        )

        for edge_index, ref in enumerate(node.from_refs):
            producer_key_edge, producer_binding, _kind = _producer_for_ref_tx(
                app, pipeline_name, ref, at_text
            )
            # A newly initialized upstream node may itself recurse into this
            # function; its current pointer is committed by this same txn.
            app.conn.execute(
                "INSERT OR IGNORE INTO node_edge_state "
                "(pipeline_name,node_name,binding_digest,edge_index,producer_key,"
                "producer_binding_digest,last_revision,applied_digest) VALUES (?,?,?,?,?,?,0,NULL)",
                (
                    pipeline_name,
                    node_name,
                    binding,
                    edge_index,
                    producer_key_edge,
                    producer_binding,
                ),
            )

        if previous != binding:
            _emit(
                "node_binding_activation",
                pipeline_name=pipeline_name,
                node_name=node_name,
                binding=binding,
                publication_id=publication_id,
                item_ids=(),
                batch_id=None,
                state="activated",
            )
        return binding
    finally:
        active.remove(marker)


def _ensure_node_binding(
    app: App, pipeline_name: str, node_name: str, at: datetime
) -> str:
    """Initialize one node binding, its empty snapshot, and edge cursors."""

    at_text = _timestamp(at)
    with write_transaction(app.conn):
        return _ensure_node_binding_tx(app, pipeline_name, node_name, at_text, set())


def _sync_source_edge_tx(
    app: App,
    pipeline_name: str,
    node_name: str,
    binding: str,
    edge: sqlite3.Row,
) -> None:
    edge_index = int(edge["edge_index"])
    producer_key = str(edge["producer_key"])
    producer_binding = str(edge["producer_binding_digest"])
    last_revision = int(edge["last_revision"])
    publications = app.conn.execute(
        "SELECT id,revision,digest FROM publications "
        "WHERE kind='delta' AND producer_key=? AND binding_digest=? AND revision>? "
        "ORDER BY revision",
        (producer_key, producer_binding, last_revision),
    ).fetchall()
    next_ordinal_row = app.conn.execute(
        "SELECT COALESCE(MAX(upstream_ordinal),-1) FROM node_edge_members "
        "WHERE pipeline_name=? AND node_name=? AND binding_digest=? AND edge_index=?",
        (pipeline_name, node_name, binding, edge_index),
    ).fetchone()
    next_ordinal = int(next_ordinal_row[0]) + 1
    applied_digest = edge["applied_digest"]
    for publication in publications:
        members = app.conn.execute(
            "SELECT ordinal,item_id FROM publication_members WHERE publication_id=? ORDER BY ordinal",
            (publication["id"],),
        ).fetchall()
        for member in members:
            item_id = str(member["item_id"])
            existing = app.conn.execute(
                "SELECT upstream_ordinal FROM node_edge_members WHERE "
                "pipeline_name=? AND node_name=? AND binding_digest=? AND edge_index=? AND item_id=?",
                (pipeline_name, node_name, binding, edge_index, item_id),
            ).fetchone()
            if existing is None:
                app.conn.execute(
                    "INSERT INTO node_edge_members "
                    "(pipeline_name,node_name,binding_digest,edge_index,item_id,upstream_ordinal,active) "
                    "VALUES (?,?,?,?,?,?,1)",
                    (
                        pipeline_name,
                        node_name,
                        binding,
                        edge_index,
                        item_id,
                        next_ordinal,
                    ),
                )
                next_ordinal += 1
            else:
                app.conn.execute(
                    "UPDATE node_edge_members SET active=1 WHERE "
                    "pipeline_name=? AND node_name=? AND binding_digest=? AND edge_index=? AND item_id=?",
                    (pipeline_name, node_name, binding, edge_index, item_id),
                )
        last_revision = int(publication["revision"])
        applied_digest = str(publication["digest"])

    if not publications and applied_digest is None:
        latest = app.conn.execute(
            "SELECT digest FROM publications WHERE kind='delta' AND producer_key=? "
            "AND binding_digest=? ORDER BY revision DESC LIMIT 1",
            (producer_key, producer_binding),
        ).fetchone()
        if latest is not None:
            applied_digest = str(latest[0])
    app.conn.execute(
        "UPDATE node_edge_state SET last_revision=?,applied_digest=? WHERE "
        "pipeline_name=? AND node_name=? AND binding_digest=? AND edge_index=?",
        (
            last_revision,
            applied_digest,
            pipeline_name,
            node_name,
            binding,
            edge_index,
        ),
    )


def _sync_snapshot_edge_tx(
    app: App,
    pipeline_name: str,
    node_name: str,
    binding: str,
    edge: sqlite3.Row,
) -> None:
    edge_index = int(edge["edge_index"])
    producer_key = str(edge["producer_key"])
    producer_binding = str(edge["producer_binding_digest"])
    state = app.conn.execute(
        "SELECT current_publication_id FROM node_state WHERE pipeline_name=? AND node_name=? "
        "AND binding_digest=?",
        (pipeline_name, producer_key.rsplit(".", 1)[-1], producer_binding),
    ).fetchone()
    # producer_key is fully-qualified (Pipeline.node); split only after the
    # existence check above to avoid joining against arbitrary source keys.
    if state is None:
        parts = producer_key.split(".", 1)
        if len(parts) != 2:
            return
        state = app.conn.execute(
            "SELECT current_publication_id FROM node_state WHERE pipeline_name=? AND node_name=? "
            "AND binding_digest=?",
            (parts[0], parts[1], producer_binding),
        ).fetchone()
    if state is None:
        raise ExecutionError(f"upstream node state is unavailable: {producer_key}")
    publication = app.conn.execute(
        "SELECT id,revision,digest FROM publications WHERE id=? AND kind='snapshot'",
        (state[0],),
    ).fetchone()
    if publication is None:
        raise ExecutionError(f"upstream snapshot is unavailable: {producer_key}")
    if edge["applied_digest"] == publication["digest"]:
        return

    app.conn.execute(
        "UPDATE node_edge_members SET active=0 WHERE pipeline_name=? AND node_name=? "
        "AND binding_digest=? AND edge_index=?",
        (pipeline_name, node_name, binding, edge_index),
    )
    members = app.conn.execute(
        "SELECT ordinal,item_id FROM publication_members WHERE publication_id=? ORDER BY ordinal",
        (publication["id"],),
    ).fetchall()
    for member in members:
        item_id = str(member["item_id"])
        existing = app.conn.execute(
            "SELECT 1 FROM node_edge_members WHERE pipeline_name=? AND node_name=? "
            "AND binding_digest=? AND edge_index=? AND item_id=?",
            (pipeline_name, node_name, binding, edge_index, item_id),
        ).fetchone()
        if existing is None:
            app.conn.execute(
                "INSERT INTO node_edge_members "
                "(pipeline_name,node_name,binding_digest,edge_index,item_id,upstream_ordinal,active) "
                "VALUES (?,?,?,?,?,?,1)",
                (
                    pipeline_name,
                    node_name,
                    binding,
                    edge_index,
                    item_id,
                    int(member["ordinal"]),
                ),
            )
        else:
            app.conn.execute(
                "UPDATE node_edge_members SET upstream_ordinal=?,active=1 WHERE "
                "pipeline_name=? AND node_name=? AND binding_digest=? AND edge_index=? AND item_id=?",
                (
                    int(member["ordinal"]),
                    pipeline_name,
                    node_name,
                    binding,
                    edge_index,
                    item_id,
                ),
            )
    app.conn.execute(
        "UPDATE node_edge_state SET last_revision=?,applied_digest=? WHERE "
        "pipeline_name=? AND node_name=? AND binding_digest=? AND edge_index=?",
        (
            int(publication["revision"]),
            str(publication["digest"]),
            pipeline_name,
            node_name,
            binding,
            edge_index,
        ),
    )


def _sync_edges_tx(
    app: App,
    pipeline_name: str,
    node_name: str,
    binding: str,
    at_text: str,
) -> None:
    node = _node(app, pipeline_name, node_name)
    for edge_index, ref in enumerate(node.from_refs):
        producer_key, producer_binding, kind = _producer_for_ref_tx(
            app, pipeline_name, ref, at_text
        )
        edge = app.conn.execute(
            "SELECT * FROM node_edge_state WHERE pipeline_name=? AND node_name=? "
            "AND binding_digest=? AND edge_index=?",
            (pipeline_name, node_name, binding, edge_index),
        ).fetchone()
        if edge is None:
            raise ExecutionError("node edge cursor is unavailable")
        if (
            edge["producer_key"] != producer_key
            or edge["producer_binding_digest"] != producer_binding
        ):
            # The producer pointer switch and its old contribution removal are
            # intentionally one transaction with the new cursor application.
            app.conn.execute(
                "DELETE FROM node_edge_members WHERE pipeline_name=? AND node_name=? "
                "AND binding_digest=? AND edge_index=?",
                (pipeline_name, node_name, binding, edge_index),
            )
            app.conn.execute(
                "UPDATE node_edge_state SET producer_key=?,producer_binding_digest=?,"
                "last_revision=0,applied_digest=NULL WHERE pipeline_name=? AND node_name=? "
                "AND binding_digest=? AND edge_index=?",
                (
                    producer_key,
                    producer_binding,
                    pipeline_name,
                    node_name,
                    binding,
                    edge_index,
                ),
            )
            edge = app.conn.execute(
                "SELECT * FROM node_edge_state WHERE pipeline_name=? AND node_name=? "
                "AND binding_digest=? AND edge_index=?",
                (pipeline_name, node_name, binding, edge_index),
            ).fetchone()
        if kind == "source":
            _sync_source_edge_tx(app, pipeline_name, node_name, binding, edge)
        else:
            _sync_snapshot_edge_tx(app, pipeline_name, node_name, binding, edge)


def _desired_union_tx(
    app: App, pipeline_name: str, node_name: str, binding: str
) -> list[str]:
    rows = app.conn.execute(
        "SELECT edge_index,item_id,upstream_ordinal FROM node_edge_members "
        "WHERE pipeline_name=? AND node_name=? AND binding_digest=? AND active=1 "
        "ORDER BY edge_index,upstream_ordinal,item_id",
        (pipeline_name, node_name, binding),
    ).fetchall()
    seen: set[str] = set()
    result: list[str] = []
    for row in rows:
        item_id = str(row["item_id"])
        if item_id in seen:
            continue
        seen.add(item_id)
        result.append(item_id)
    return result


def _identity(row: Mapping[str, Any]) -> tuple[str, str, str]:
    return (
        str(row["producer_kind"]),
        str(row["producer_name"]),
        str(row["logical_id"]),
    )


def _rebuild_members_tx(
    app: App,
    pipeline_name: str,
    node_name: str,
    binding: str,
    desired: Sequence[str],
) -> None:
    node = _node(app, pipeline_name, node_name)
    # Terminal Delivery nodes and each Stages accept distinct Item generations.
    # Only batch Stages gate newer generations behind include_updates.
    include_updates = bool(
        node.stage is None or node.batch is None or node.batch.include_updates
    )
    rows = app.conn.execute(
        "SELECT * FROM node_members WHERE pipeline_name=? AND node_name=? AND binding_digest=?",
        (pipeline_name, node_name, binding),
    ).fetchall()
    by_id: dict[str, sqlite3.Row] = {str(row["item_id"]): row for row in rows}
    desired_set = set(desired)
    desired_positions = {item_id: ordinal for ordinal, item_id in enumerate(desired)}
    for ordinal, item_id in enumerate(desired):
        item = app.conn.execute(
            "SELECT producer_kind,producer_name,logical_id FROM items WHERE id=?",
            (item_id,),
        ).fetchone()
        if item is None:
            raise ExecutionError(f"admission Item is missing: {item_id}")
        ident = (
            str(item["producer_kind"]),
            str(item["producer_name"]),
            str(item["logical_id"]),
        )
        current = by_id.get(item_id)
        if current is not None:
            if current["state"] == "stale":
                app.conn.execute(
                    "UPDATE node_members SET state='pending',ordinal=? WHERE pipeline_name=? "
                    "AND node_name=? AND binding_digest=? AND item_id=?",
                    (ordinal, pipeline_name, node_name, binding, item_id),
                )
            elif current["state"] in {"pending", "claimed", "terminal"}:
                app.conn.execute(
                    "UPDATE node_members SET ordinal=? WHERE pipeline_name=? AND node_name=? "
                    "AND binding_digest=? AND item_id=?",
                    (ordinal, pipeline_name, node_name, binding, item_id),
                )
            continue

        # Re-read the identity after every replacement.  A single source
        # delta may contain several generations of the same logical Item;
        # stale sqlite3.Row snapshots would otherwise keep targeting the first
        # (already replaced) item ID.
        candidates = app.conn.execute(
            "SELECT * FROM node_members WHERE pipeline_name=? AND node_name=? "
            "AND binding_digest=? AND producer_kind=? AND producer_name=? AND logical_id=? "
            "ORDER BY generation,ordinal",
            (pipeline_name, node_name, binding, ident[0], ident[1], ident[2]),
        ).fetchall()
        pending = next((row for row in candidates if row["state"] == "pending"), None)
        if pending is not None:
            old_id = str(pending["item_id"])
            # For a batch without include_updates, retain a completed old
            # member but replace a pending old member with the later desired
            # generation.  On the next reconcile the pending row is the later
            # item, so earlier desired generations must be skipped rather than
            # replacing it back and toggling forever.
            pending_position = desired_positions.get(old_id)
            if (
                not include_updates
                and pending_position is not None
                and pending_position > ordinal
            ):
                continue
            generation = int(pending["generation"]) + 1
            app.conn.execute(
                "UPDATE node_members SET item_id=?,generation=?,ordinal=?,producer_kind=?,"
                "producer_name=?,logical_id=?,state='pending' WHERE pipeline_name=? AND node_name=? "
                "AND binding_digest=? AND item_id=?",
                (
                    item_id,
                    generation,
                    ordinal,
                    ident[0],
                    ident[1],
                    ident[2],
                    pipeline_name,
                    node_name,
                    binding,
                    old_id,
                ),
            )
            by_id.pop(old_id, None)
            by_id[item_id] = app.conn.execute(
                "SELECT * FROM node_members WHERE pipeline_name=? AND node_name=? "
                "AND binding_digest=? AND item_id=?",
                (pipeline_name, node_name, binding, item_id),
            ).fetchone()
            continue

        latest = candidates[-1] if candidates else None
        consumed = bool(
            latest is not None
            and (
                latest["state"] in {"claimed", "frozen", "terminal"}
                or latest["visible_result_id"] is not None
                or latest["consumed_sequence"] is not None
            )
        )
        if consumed and not include_updates:
            continue
        generation = (
            1
            if latest is None
            else max(int(row["generation"]) for row in candidates) + 1
        )
        app.conn.execute(
            "INSERT INTO node_members "
            "(pipeline_name,node_name,binding_digest,item_id,producer_kind,producer_name,logical_id,"
            "generation,ordinal,state,lookup_key,consumed_sequence,visible_result_id) "
            "VALUES (?,?,?,?,?,?,?,?,?,'pending',NULL,NULL,NULL)",
            (
                pipeline_name,
                node_name,
                binding,
                item_id,
                ident[0],
                ident[1],
                ident[2],
                generation,
                ordinal,
            ),
        )

    # Omitted snapshot members become stale.  Frozen members stay frozen so
    # their immutable batch envelope remains auditable; terminal members are
    # retained as stale history so a source/node binding switch cannot expose
    # an obsolete generation as active work.
    for row in rows:
        item_id = str(row["item_id"])
        if item_id in desired_set:
            continue
        if row["state"] in {"pending", "claimed", "terminal"}:
            app.conn.execute(
                "UPDATE node_members SET state='stale' WHERE pipeline_name=? AND node_name=? "
                "AND binding_digest=? AND item_id=? AND state IN ('pending','claimed','terminal')",
                (pipeline_name, node_name, binding, item_id),
            )


def _freeze_tx(
    app: App,
    pipeline_name: str,
    node_name: str,
    binding: str,
    at: datetime,
    at_text: str,
    state: sqlite3.Row,
) -> int | None:
    node = _node(app, pipeline_name, node_name)
    policy = node.batch
    if policy is None:
        return None
    current_batch = state["current_frozen_batch_id"]
    if current_batch is not None:
        current = app.conn.execute(
            "SELECT state FROM batches WHERE id=?",
            (current_batch,),
        ).fetchone()
        if current is not None and current[0] in {"frozen", "failed"}:
            return None
        app.conn.execute(
            "UPDATE node_state SET current_frozen_batch_id=NULL WHERE pipeline_name=? "
            "AND node_name=? AND binding_digest=? AND current_frozen_batch_id=?",
            (pipeline_name, node_name, binding, current_batch),
        )

    first = _decode_timestamp(state["first_pending_at"])
    next_due = _decode_timestamp(state["next_due_at"])
    pending_count = int(
        app.conn.execute(
            "SELECT COUNT(*) FROM node_members WHERE pipeline_name=? AND node_name=? "
            "AND binding_digest=? AND state='pending'",
            (pipeline_name, node_name, binding),
        ).fetchone()[0]
    )
    schedule_due = next_due is not None and at >= next_due
    if not freeze_eligible(policy, pending_count, first, at, schedule_due):
        return None
    limit = policy.count if policy.count is not None else pending_count
    pending = app.conn.execute(
        "SELECT item_id FROM node_members WHERE pipeline_name=? AND node_name=? "
        "AND binding_digest=? AND state='pending' ORDER BY ordinal,item_id LIMIT ?",
        (pipeline_name, node_name, binding, int(limit)),
    ).fetchall()
    item_ids = [str(row[0]) for row in pending]
    if not item_ids:
        return None
    batch = app.conn.execute(
        "INSERT INTO batches(pipeline_name,node_name,binding_digest,state,frozen_at) "
        "VALUES (?,?,?,'frozen',?)",
        (pipeline_name, node_name, binding, at_text),
    )
    batch_id = int(batch.lastrowid)
    app.conn.executemany(
        "INSERT INTO batch_items(batch_id,ordinal,item_id) VALUES (?,?,?)",
        ((batch_id, ordinal, item_id) for ordinal, item_id in enumerate(item_ids)),
    )
    placeholders = ",".join("?" for _ in item_ids)
    app.conn.execute(
        f"UPDATE node_members SET state='frozen' WHERE pipeline_name=? AND node_name=? "
        f"AND binding_digest=? AND state='pending' AND item_id IN ({placeholders})",
        (pipeline_name, node_name, binding, *item_ids),
    )
    app.conn.execute(
        "UPDATE node_state SET current_frozen_batch_id=?,first_pending_at=NULL,next_due_at=NULL "
        "WHERE pipeline_name=? AND node_name=? AND binding_digest=?",
        (batch_id, pipeline_name, node_name, binding),
    )
    publication_id = int(state["current_publication_id"])
    _emit(
        "batch_freeze",
        pipeline_name=pipeline_name,
        node_name=node_name,
        binding=binding,
        publication_id=publication_id,
        item_ids=item_ids,
        batch_id=batch_id,
        state="frozen",
    )
    return batch_id


def _advance_schedule_and_freeze_tx(
    app: App,
    pipeline_name: str,
    node_name: str,
    binding: str,
    at: datetime,
    at_text: str,
) -> None:
    node = _node(app, pipeline_name, node_name)
    state = app.conn.execute(
        "SELECT * FROM node_state WHERE pipeline_name=? AND node_name=? AND binding_digest=?",
        (pipeline_name, node_name, binding),
    ).fetchone()
    if state is None:
        raise ExecutionError("node state is unavailable")
    pending_count = int(
        app.conn.execute(
            "SELECT COUNT(*) FROM node_members WHERE pipeline_name=? AND node_name=? "
            "AND binding_digest=? AND state='pending'",
            (pipeline_name, node_name, binding),
        ).fetchone()[0]
    )
    first = _decode_timestamp(state["first_pending_at"])
    next_due = _decode_timestamp(state["next_due_at"])
    if pending_count and first is None:
        first = at
    if not pending_count:
        first = None
        if state["current_frozen_batch_id"] is None:
            next_due = None
    if (
        pending_count
        and node.schedule is not None
        and next_due is None
        and first is not None
    ):
        next_due = first + node.schedule

    updated_state = (
        app.conn.execute(
            "SELECT state FROM batches WHERE id=?",
            (state["current_frozen_batch_id"],),
        ).fetchone()
        if state["current_frozen_batch_id"] is not None
        else None
    )
    # A failed batch remains the durable current envelope until a later
    # recovery/rebuild explicitly resolves it; admitting a second batch here
    # would violate immutable batch ownership just as surely as replacing a
    # still-frozen one.
    frozen_active = updated_state is not None and updated_state[0] in {
        "frozen",
        "failed",
    }
    if state["current_frozen_batch_id"] is not None and not frozen_active:
        app.conn.execute(
            "UPDATE node_state SET current_frozen_batch_id=NULL WHERE pipeline_name=? "
            "AND node_name=? AND binding_digest=?",
            (pipeline_name, node_name, binding),
        )
        state = app.conn.execute(
            "SELECT * FROM node_state WHERE pipeline_name=? AND node_name=? AND binding_digest=?",
            (pipeline_name, node_name, binding),
        ).fetchone()

    app.conn.execute(
        "UPDATE node_state SET first_pending_at=?,next_due_at=? WHERE pipeline_name=? "
        "AND node_name=? AND binding_digest=?",
        (
            None if first is None else _timestamp(first),
            None if next_due is None else _timestamp(next_due),
            pipeline_name,
            node_name,
            binding,
        ),
    )
    # Re-read so a completed batch pointer clear and this timestamp update are
    # evaluated against one authoritative row.
    state = app.conn.execute(
        "SELECT * FROM node_state WHERE pipeline_name=? AND node_name=? AND binding_digest=?",
        (pipeline_name, node_name, binding),
    ).fetchone()
    _freeze_tx(app, pipeline_name, node_name, binding, at, at_text, state)


def _stage_requests_for_node(
    app: App, pipeline_name: str, node_name: str, at: datetime
):
    """Yield immutable Stage requests for currently admitted node work."""

    node = _node(app, pipeline_name, node_name)
    if node.stage is None:
        return []
    binding = _binding(app, pipeline_name, node_name)
    state = app.conn.execute(
        "SELECT * FROM node_state WHERE pipeline_name=? AND node_name=? AND binding_digest=?",
        (pipeline_name, node_name, binding),
    ).fetchone()
    if state is None:
        return []
    # Scheduled each nodes wait until their persisted due timestamp.  Batch
    # nodes are already gated by the immutable frozen batch row.
    if node.batch is None and node.schedule is not None:
        due = _decode_timestamp(state["next_due_at"])
        if due is not None and utc_datetime(at) < due:
            return []
    from .stage_runtime import _StageRequest

    requests: list[_StageRequest] = []
    if node.batch is not None:
        batch_id = state["current_frozen_batch_id"]
        if batch_id is not None:
            batch_state = app.conn.execute(
                "SELECT state FROM batches WHERE id=?", (batch_id,)
            ).fetchone()
            if batch_state is not None and batch_state[0] == "frozen":
                rows = app.conn.execute(
                    "SELECT item_id FROM batch_items WHERE batch_id=? ORDER BY ordinal",
                    (batch_id,),
                ).fetchall()
                item_ids = tuple(str(row[0]) for row in rows)
                if item_ids:
                    requests.append(
                        _StageRequest(
                            stage_name=str(node.stage),
                            envelope=StageEnvelope(item_ids),
                            owner=(pipeline_name, node_name),
                            generation=None,
                            batch_id=int(batch_id),
                            policy=node.policy,
                            binding_digest=binding,
                        )
                    )

    pending_rows = app.conn.execute(
        "SELECT item_id,generation,lookup_key,state FROM node_members WHERE pipeline_name=? "
        "AND node_name=? AND binding_digest=? AND state='pending' ORDER BY ordinal,item_id",
        (pipeline_name, node_name, binding),
    ).fetchall()
    if node.batch is None:
        rows = pending_rows
    else:
        rows = ()
    for row in rows:
        requests.append(
            _StageRequest(
                stage_name=str(node.stage),
                envelope=StageEnvelope((str(row["item_id"]),)),
                owner=(pipeline_name, node_name),
                generation=int(row["generation"]),
                batch_id=None,
                policy=node.policy,
                binding_digest=binding,
            )
        )

    # A successful shared Stage refresh advances the global result sequence,
    # but active consumers may already be terminal against the prior result.
    # Re-admit only those lagging members through the ordinary cache path.  A
    # current-result join and sequence comparison make historical rebuild
    # scope exclusions (which record the observed sequence) idempotent.
    lagging = app.conn.execute(
        "SELECT nm.item_id,nm.generation,nm.lookup_key,nm.ordinal,sr.sequence "
        "FROM node_members nm "
        "JOIN stage_result_current src ON src.stage_name=? AND src.lookup_key=nm.lookup_key "
        "JOIN stage_results sr ON sr.id=src.result_id "
        "WHERE nm.pipeline_name=? AND nm.node_name=? AND nm.binding_digest=? "
        "AND nm.state='terminal' "
        "AND (nm.consumed_sequence IS NULL OR nm.consumed_sequence < sr.sequence) "
        "ORDER BY nm.ordinal,nm.item_id",
        (str(node.stage), pipeline_name, node_name, binding),
    ).fetchall()
    if node.batch is None:
        for row in lagging:
            requests.append(
                _StageRequest(
                    stage_name=str(node.stage),
                    envelope=StageEnvelope((str(row["item_id"]),)),
                    owner=(pipeline_name, node_name),
                    generation=int(row["generation"]),
                    batch_id=None,
                    policy=node.policy,
                    binding_digest=binding,
                )
            )
        return requests

    # Batch members have one shared immutable envelope.  Recover that exact
    # envelope from this node's last terminal Execution instead of issuing a
    # per-item request (which would change a batch cache key).  The old batch
    # ID is intentionally not used as a current-freeze guard: a successful
    # batch clears that pointer, while its terminal members remain active and
    # eligible for shared-result healing.
    seen_lookup_keys: set[str] = set()
    for row in lagging:
        lookup_key = row["lookup_key"]
        if not isinstance(lookup_key, str) or lookup_key in seen_lookup_keys:
            continue
        seen_lookup_keys.add(lookup_key)
        execution = app.conn.execute(
            "SELECT id FROM executions WHERE kind='stage' AND pipeline_name=? "
            "AND node_name=? AND binding_digest=? AND stage_name=? AND lookup_key=? "
            "AND result_id IS NOT NULL AND state IN ('succeeded','cached','dropped') "
            "ORDER BY id DESC LIMIT 1",
            (pipeline_name, node_name, binding, str(node.stage), lookup_key),
        ).fetchone()
        if execution is None:
            continue
        item_rows = app.conn.execute(
            "SELECT item_id FROM execution_inputs WHERE execution_id=? AND slot='items' ORDER BY ordinal",
            (execution[0],),
        ).fetchall()
        item_ids = tuple(str(item[0]) for item in item_rows)
        if not item_ids:
            continue
        history_rows = app.conn.execute(
            "SELECT item_id FROM execution_inputs WHERE execution_id=? AND slot='history' ORDER BY ordinal",
            (execution[0],),
        ).fetchall()
        history_ids = tuple(str(item[0]) for item in history_rows)
        requests.append(
            _StageRequest(
                stage_name=str(node.stage),
                envelope=StageEnvelope(item_ids, history_ids),
                owner=(pipeline_name, node_name),
                generation=None,
                batch_id=None,
                policy=node.policy,
                binding_digest=binding,
                history_persisted=True,
            )
        )
    return requests


def _request_node_work(
    app: App, pipeline_name: str, node_name: str, at: datetime
) -> list[int]:
    from .stage_runtime import _request_materialization

    execution_ids: list[int] = []
    for request in _stage_requests_for_node(app, pipeline_name, node_name, at):
        outcome = _request_materialization(app, request, at)
        if outcome.execution_id is not None:
            execution_ids.append(int(outcome.execution_id))
    return execution_ids


def _source_due_names(app: App, at: datetime) -> list[str]:
    due: list[str] = []
    for source_name, source in app.definition.sources.items():
        binding = source_binding_digest(source_name, source)
        state = app.conn.execute(
            "SELECT * FROM source_state WHERE source_name=? AND binding_digest=?",
            (source_name, binding),
        ).fetchone()
        if state is None:
            due.append(source_name)
            continue
        active = state["active_execution_id"]
        if active is not None:
            row = app.conn.execute(
                "SELECT state FROM executions WHERE id=?", (active,)
            ).fetchone()
            if row is not None and row[0] in {"queued", "running"}:
                continue
        next_due = _decode_timestamp(state["next_due_at"])
        if next_due is not None and utc_datetime(at) >= next_due:
            due.append(source_name)
    return due


def _run_due_sources(app: App, at: datetime) -> bool:
    changed = False
    # Queued retries are durable attempts and run before a new schedule claim.
    queued = app.conn.execute(
        "SELECT id FROM executions WHERE kind='source' AND state='queued' ORDER BY id"
    ).fetchall()
    for row in queued:
        _execute_queued_source(app, int(row[0]), at)
        changed = True
    for source_name in _source_due_names(app, at):
        try:
            collect_source(app, source_name, at, trigger="schedule")
        except ExecutionError:
            # collect_source has durably recorded the failure.  Keep draining
            # independent sources/nodes in this callback-independent pass.
            pass
        changed = True
    return changed


def _timeout_executions(
    app: App, at: datetime, allowed_nodes: set[tuple[str, str]] | None = None
) -> bool:
    from .stage_runtime import _decode_policy, _fail_execution

    changed = False
    rows = app.conn.execute(
        "SELECT * FROM executions WHERE state='running' ORDER BY id"
    ).fetchall()
    for row in rows:
        if (
            row["kind"] == "stage"
            and allowed_nodes is not None
            and (str(row["pipeline_name"]), str(row["node_name"])) not in allowed_nodes
        ):
            continue
        if row["kind"] == "source" and allowed_nodes is not None:
            continue
        started = _decode_timestamp(row["started_at"])
        if started is None:
            continue
        try:
            policy = _decode_policy(str(row["policy_json"]))
        except ExecutionError:
            continue
        if policy.timeout is None or utc_datetime(at) < started + policy.timeout:
            continue
        if row["kind"] == "stage":
            _fail_execution(
                app, int(row["id"]), ExecutionError("execution timeout"), at
            )
        else:
            _recover_source_execution(app, int(row["id"]), at)
        changed = True
    return changed


def _start_queued_stage_owners(
    app: App,
    at: datetime,
    allowed_nodes: set[tuple[str, str]] | None = None,
) -> bool:
    from .stage_runtime import _decode_policy, _execute_claimed_stage

    rows = app.conn.execute(
        "SELECT e.* FROM executions e JOIN materialization_claims mc ON mc.owner_execution_id=e.id "
        "WHERE e.kind='stage' AND e.state='queued' ORDER BY e.id"
    ).fetchall()
    if not rows:
        return False
    grouped: dict[tuple[str, str], list[sqlite3.Row]] = {}
    for row in rows:
        key = (str(row["pipeline_name"] or ""), str(row["node_name"] or ""))
        if allowed_nodes is not None and key not in allowed_nodes:
            continue
        grouped.setdefault(key, []).append(row)
    futures = []
    with ThreadPoolExecutor(max_workers=max(1, len(rows))) as pool:
        for (pipeline_name, node_name), candidates in grouped.items():
            if not pipeline_name or not node_name:
                continue
            running = int(
                app.conn.execute(
                    "SELECT COUNT(*) FROM executions WHERE kind='stage' AND pipeline_name=? AND node_name=? AND state='running'",
                    (pipeline_name, node_name),
                ).fetchone()[0]
            )
            try:
                limit = _decode_policy(str(candidates[0]["policy_json"])).concurrency
            except ExecutionError:
                limit = 1
            for row in candidates[: max(0, limit - running)]:
                futures.append(
                    pool.submit(_execute_claimed_stage, app, int(row["id"]), at)
                )
        for future in futures:
            future.result()
    return bool(futures)


def _dependency_closure(app: App, pipeline_name: str, node_name: str) -> list[str]:
    pipeline = app.definition.pipelines.get(pipeline_name)
    if pipeline is None or node_name not in pipeline.nodes:
        raise ExecutionError(f"unknown Pipeline node: {pipeline_name}.{node_name}")
    needed: set[str] = set()

    def add_ancestors(name: str) -> None:
        if name in needed:
            return
        needed.add(name)
        for ref in pipeline.nodes[name].from_refs:
            if ref in pipeline.nodes:
                add_ancestors(ref)

    add_ancestors(node_name)
    changed = True
    while changed:
        changed = False
        for name, node in pipeline.nodes.items():
            if name in needed:
                continue
            if any(ref in needed for ref in node.from_refs):
                needed.add(name)
                changed = True
    return [name for name in pipeline.nodes if name in needed]


def _downstream_closure(app: App, pipeline_name: str, node_name: str) -> set[str]:
    pipeline = app.definition.pipelines.get(pipeline_name)
    if pipeline is None or node_name not in pipeline.nodes:
        raise ExecutionError(f"unknown Pipeline node: {pipeline_name}.{node_name}")
    needed = {node_name}
    changed = True
    while changed:
        changed = False
        for name, node in pipeline.nodes.items():
            if name in needed:
                continue
            if any(ref in needed for ref in node.from_refs):
                needed.add(name)
                changed = True
    return needed


def _drain(
    app: App,
    at: datetime,
    targets: Sequence[tuple[str, str]] | None,
    *,
    scan_roots: Sequence[tuple[str, str]] | None = None,
) -> None:
    """Single durable reconcile/claim/execute loop used by tick and reconcile."""

    # A bounded fixed point protects a malformed external callback from
    # spinning forever while still allowing completion snapshots to flow to
    # all immediately eligible downstream nodes in the same call.
    all_nodes = tuple(
        (pipeline_name, node_name)
        for pipeline_name, pipeline in app.definition.pipelines.items()
        for node_name in pipeline.nodes
    )
    global_scan = targets is None
    admission_targets = all_nodes if targets is None else tuple(targets)
    scan_targets = admission_targets if scan_roots is None else tuple(scan_roots)
    allowed_nodes = (
        None
        if global_scan
        else {
            (pipeline_name, candidate)
            for pipeline_name, node_name in scan_targets
            for candidate in _downstream_closure(app, pipeline_name, node_name)
        }
    )
    for _iteration in range(32):
        changed = False
        changed = _timeout_executions(app, at, allowed_nodes) or changed
        from .delivery import _timeout_deliveries

        changed = _timeout_deliveries(app, at, allowed_nodes) or changed
        if global_scan:
            changed = _run_due_sources(app, at) or changed
        ordered: list[tuple[str, str]] = []
        for pipeline_name, node_name in admission_targets:
            for candidate in _dependency_closure(app, pipeline_name, node_name):
                marker = (pipeline_name, candidate)
                if marker not in ordered:
                    ordered.append(marker)
        for pipeline_name, node_name in ordered:
            reconcile_node(app, pipeline_name, node_name, at)
        for pipeline_name, node_name in ordered:
            if _request_node_work(app, pipeline_name, node_name, at):
                changed = True
        changed = _start_queued_stage_owners(app, at, allowed_nodes) or changed
        from .delivery import _dispatch_deliveries

        changed = _dispatch_deliveries(app, at, allowed_nodes) or changed
        if not changed:
            break


def tick(app: App, at: datetime) -> None:
    with app.admit():
        _drain(app, at, None)


def reconcile(app: App, pipeline_name: str, node_name: str, at: datetime) -> None:
    with app.admit():
        targets = tuple(
            (pipeline_name, name)
            for name in _dependency_closure(app, pipeline_name, node_name)
        )
        _drain(app, at, targets, scan_roots=((pipeline_name, node_name),))


def recover_startup(app: App, at: datetime) -> None:
    """Recover interrupted Source/Stage attempts exactly once per startup."""

    from .config import decode_stage_snapshot
    from .delivery import _recover_deliveries
    from .stage_runtime import _fail_execution

    # Remove legacy waiters before releasing any superseded owner claim.  This
    # prevents normal claim promotion from turning a retained legacy waiter
    # into a newly queued execution during the same startup transition.
    waiter_rows = app.conn.execute(
        "SELECT stage_name,lookup_key,waiting_sequence,config_json "
        "FROM materialization_waiters"
    ).fetchall()
    with write_transaction(app.conn):
        for waiter in waiter_rows:
            try:
                snapshot = decode_stage_snapshot(json.loads(str(waiter["config_json"])))
                legacy = any(
                    task.use
                    in {
                        "sluice.tasks.fetch_webpage",
                        "sluice.tasks.extract_content",
                        "sluice.tasks.to_markdown",
                        "sluice.tasks.to_text",
                    }
                    and task.web_binding is None
                    for task in snapshot.tasks
                )
            except Exception:
                legacy = False
            if legacy:
                app.conn.execute(
                    "DELETE FROM materialization_waiters WHERE stage_name=? AND lookup_key=? "
                    "AND waiting_sequence=?",
                    (
                        waiter["stage_name"],
                        waiter["lookup_key"],
                        waiter["waiting_sequence"],
                    ),
                )

    rows = app.conn.execute(
        "SELECT id,kind,state,source_name,config_json,binding_digest FROM executions "
        "WHERE state IN ('queued','running') ORDER BY id"
    ).fetchall()

    for row in rows:
        execution_id = int(row["id"])
        kind = str(row["kind"])
        legacy = False
        binding_changed = False
        if kind == "stage":
            try:
                snapshot = decode_stage_snapshot(json.loads(str(row["config_json"])))
                legacy = any(
                    task.use
                    in {
                        "sluice.tasks.fetch_webpage",
                        "sluice.tasks.extract_content",
                        "sluice.tasks.to_markdown",
                        "sluice.tasks.to_text",
                    }
                    and task.web_binding is None
                    for task in snapshot.tasks
                )
            except Exception:
                legacy = False
        elif kind == "source":
            source_name = row["source_name"]
            configured = (
                app.definition.sources.get(str(source_name))
                if source_name is not None
                else None
            )
            # A retained Source is executable only when the same named
            # definition still exists and its expanded binding digest is
            # unchanged.  Removal is therefore a supersession as well as an
            # edited chain, and must happen before any provider call.
            if configured is None:
                binding_changed = True
            else:
                binding_changed = str(
                    row["binding_digest"] or ""
                ) != source_binding_digest(str(source_name), configured)
                legacy = (
                    configured.use in {"sluice.sources.rss", "sluice.sources.webpage"}
                    and configured.web_binding is None
                )
        if kind == "stage" and legacy:
            _fail_execution(
                app,
                execution_id,
                ExecutionError("superseded legacy web snapshot"),
                at,
                supersede=True,
            )
            continue
        if legacy or binding_changed:
            with write_transaction(app.conn):
                app.conn.execute(
                    "UPDATE executions SET state='failed',error=?,finished_at=? "
                    "WHERE id=? AND state IN ('queued','running')",
                    (
                        "superseded legacy web snapshot"
                        if legacy
                        else "superseded binding",
                        _timestamp(at),
                        execution_id,
                    ),
                )
                app.conn.execute(
                    "UPDATE materialization_claims SET owner_execution_id=NULL WHERE owner_execution_id=?",
                    (execution_id,),
                )
                if kind == "source":
                    app.conn.execute(
                        "UPDATE source_state SET active_execution_id=NULL WHERE active_execution_id=?",
                        (execution_id,),
                    )
            continue
        if kind == "source":
            _recover_source_execution(app, execution_id, at)
        else:
            _fail_execution(app, execution_id, ExecutionError("startup recovery"), at)

    _recover_deliveries(app, at)


def reconcile_node(app: App, pipeline_name: str, node_name: str, at: datetime) -> None:
    """Apply one idempotent publication-cursor and admission transaction."""

    if not isinstance(app, App):
        raise ExecutionError("reconcile_node requires an App")
    at_text = _timestamp(at)
    at_dt = utc_datetime(at)
    from .stage_runtime import _publish_node_snapshot_tx

    with write_transaction(app.conn):
        binding = _ensure_node_binding_tx(app, pipeline_name, node_name, at_text, set())
        _sync_edges_tx(app, pipeline_name, node_name, binding, at_text)
        desired = _desired_union_tx(app, pipeline_name, node_name, binding)
        _rebuild_members_tx(app, pipeline_name, node_name, binding, desired)
        _publish_node_snapshot_tx(app, pipeline_name, node_name, binding, at_text)
        _advance_schedule_and_freeze_tx(
            app, pipeline_name, node_name, binding, at_dt, at_text
        )
        state = app.conn.execute(
            "SELECT current_publication_id FROM node_state WHERE pipeline_name=? AND node_name=? "
            "AND binding_digest=?",
            (pipeline_name, node_name, binding),
        ).fetchone()
        _emit(
            "admission_reconcile",
            pipeline_name=pipeline_name,
            node_name=node_name,
            binding=binding,
            publication_id=None if state is None else int(state[0]),
            item_ids=desired,
            batch_id=None,
            state="admitted",
        )


__all__ = [
    "freeze_eligible",
    "reconcile",
    "reconcile_node",
    "recover_startup",
    "tick",
]
