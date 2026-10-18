"""Durable, independently retryable terminal Delivery attempts."""

from __future__ import annotations

import json
import logging
import sqlite3
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .canonical import (
    canonical_json,
    delivery_binding_digest,
    digest,
    load_item,
    utc_datetime,
)
from .db import write_transaction
from .errors import ExecutionError
from .logging import log_event

_LOGGER = logging.getLogger("sluice.delivery")


@dataclass(frozen=True, slots=True)
class _DeliveryAttempt:
    delivery_key: str
    attempt_token: int


def _timestamp(value: datetime) -> str:
    return utc_datetime(value).isoformat().replace("+00:00", "Z")


def _node(app: Any, pipeline_name: str, node_name: str):
    try:
        return app.definition.pipelines[pipeline_name].nodes[node_name]
    except (AttributeError, KeyError) as error:
        raise ExecutionError(
            f"unknown Pipeline node: {pipeline_name}.{node_name}"
        ) from error


def _delivery(app: Any, pipeline_name: str, node_name: str):
    node = _node(app, pipeline_name, node_name)
    if node.deliver is None:
        raise ExecutionError(
            f"Pipeline node {pipeline_name}.{node_name} is not a Delivery"
        )
    return node


def _node_binding(app: Any, pipeline_name: str, node_name: str) -> str:
    """Read the generic node binding used by ``node_members``."""

    from .canonical import node_binding_digest

    node = _delivery(app, pipeline_name, node_name)
    return node_binding_digest(
        pipeline_name,
        node_name,
        None,
        node.from_refs,
        node.schedule,
        node.batch,
        node.deliver,
    )


def _binding(app: Any, pipeline_name: str, node_name: str) -> str:
    node = _delivery(app, pipeline_name, node_name)
    return delivery_binding_digest(node.deliver)


def _delivery_key(
    pipeline_name: str,
    node_name: str,
    rendered_item_id: str,
    binding_digest: str,
) -> str:
    if not isinstance(rendered_item_id, str) or not rendered_item_id:
        raise ExecutionError("rendered_item_id must be a non-empty string")
    if not isinstance(binding_digest, str) or not binding_digest:
        raise ExecutionError("binding_digest must be a non-empty string")
    return digest(
        {
            "kind": "delivery",
            "pipeline": pipeline_name,
            "node": node_name,
            "rendered_item_id": rendered_item_id,
            "binding_digest": binding_digest,
        }
    )


def _policy_json(policy: Any) -> str:
    from .stage_runtime import _policy_json as stage_policy_json

    return stage_policy_json(policy)


def _decode_policy(value: str):
    from .stage_runtime import _decode_policy as stage_decode_policy

    return stage_decode_policy(value)


def _item_payload(app: Any, rendered_item_id: str) -> Any:
    with app.conn.transaction_lock:
        row = app.conn.execute(
            "SELECT * FROM items WHERE id=?", (rendered_item_id,)
        ).fetchone()
    if row is None:
        raise ExecutionError(f"rendered Item is missing: {rendered_item_id}")
    return load_item(row).payload


def _log_attempt(
    event: str,
    row: sqlite3.Row | Mapping[str, Any],
    *,
    attempt_token: int | None,
    external_id: str | None,
    state: str,
) -> None:
    log_event(
        _LOGGER,
        event,
        pipeline=str(row["pipeline_name"]),
        node=str(row["node_name"]),
        delivery_key=str(row["delivery_key"]),
        rendered_item_id=str(row["rendered_item_id"]),
        attempt_token=attempt_token,
        external_id=external_id,
        state=state,
    )


def _ensure_delivery_rows(
    app: Any, pipeline_name: str, node_name: str, at: datetime
) -> tuple[str, ...]:
    """Materialize one queued row for each currently active terminal member."""

    node = _delivery(app, pipeline_name, node_name)
    binding = _binding(app, pipeline_name, node_name)
    node_binding = _node_binding(app, pipeline_name, node_name)
    config_json = canonical_json(node.deliver.with_config)
    policy_json = _policy_json(node.policy)
    at_text = _timestamp(at)
    keys: list[str] = []
    with write_transaction(app.conn):
        members = app.conn.execute(
            "SELECT item_id,state FROM node_members WHERE pipeline_name=? AND node_name=? "
            "AND binding_digest=? AND state!='stale' ORDER BY ordinal,item_id",
            (pipeline_name, node_name, node_binding),
        ).fetchall()
        active_ids: list[str] = []
        for member in members:
            rendered_item_id = str(member["item_id"])
            # Foreign-key validation is intentionally explicit: a corrupted
            # manifest must not create an un-sendable durable row.
            if (
                app.conn.execute(
                    "SELECT 1 FROM items WHERE id=?", (rendered_item_id,)
                ).fetchone()
                is None
            ):
                raise ExecutionError(f"rendered Item is missing: {rendered_item_id}")
            key = _delivery_key(pipeline_name, node_name, rendered_item_id, binding)
            keys.append(key)
            active_ids.append(rendered_item_id)
            app.conn.execute(
                "INSERT OR IGNORE INTO deliveries "
                "(delivery_key,pipeline_name,node_name,rendered_item_id,binding_digest,"
                "config_json,policy_json,state,retry_count,attempt_token,error,external_id,updated_at) "
                "VALUES (?,?,?,?,?,?,?,'queued',0,NULL,NULL,NULL,?)",
                (
                    key,
                    pipeline_name,
                    node_name,
                    rendered_item_id,
                    binding,
                    config_json,
                    policy_json,
                    at_text,
                ),
            )
            # Delivery membership has no Stage result to claim.  Once its
            # durable row exists it is terminal for generic admission; health
            # is read exclusively from the Delivery row below.
            app.conn.execute(
                "UPDATE node_members SET state='terminal' WHERE pipeline_name=? AND node_name=? "
                "AND binding_digest=? AND item_id=? AND state IN ('pending','claimed','frozen')",
                (pipeline_name, node_name, node_binding, rendered_item_id),
            )

        # Policy is not part of the binding digest, so rows keep the policy
        # they were created with unless refreshed.  Running rows keep theirs.
        app.conn.execute(
            "UPDATE deliveries SET policy_json=? WHERE pipeline_name=? AND node_name=? "
            "AND binding_digest=? AND state IN ('queued','failed') AND policy_json!=?",
            (policy_json, pipeline_name, node_name, binding, policy_json),
        )

        # Never-attempted rows of a replaced Sink binding can no longer be
        # sent (claims reject a changed binding); attempted rows stay as history.
        app.conn.execute(
            "DELETE FROM deliveries WHERE pipeline_name=? AND node_name=? AND binding_digest!=? "
            "AND state='queued' AND attempt_token IS NULL AND retry_count=0",
            (pipeline_name, node_name, binding),
        )

        # A snapshot omission withdraws only never-started work.  Any attempt
        # token (including a failed/succeeded row) makes the send auditable and
        # therefore durable even after membership changes.
        if active_ids:
            placeholders = ",".join("?" for _ in active_ids)
            app.conn.execute(
                "DELETE FROM deliveries WHERE pipeline_name=? AND node_name=? AND binding_digest=? "
                "AND state='queued' AND attempt_token IS NULL AND rendered_item_id NOT IN ("
                + placeholders
                + ")",
                (pipeline_name, node_name, binding, *active_ids),
            )
        else:
            app.conn.execute(
                "DELETE FROM deliveries WHERE pipeline_name=? AND node_name=? AND binding_digest=? "
                "AND state='queued' AND attempt_token IS NULL",
                (pipeline_name, node_name, binding),
            )
    return tuple(keys)


def _ensure_delivery_row_for_item(
    app: Any,
    pipeline_name: str,
    node_name: str,
    rendered_item_id: str,
    at: datetime,
) -> str:
    _delivery(app, pipeline_name, node_name)
    binding = _binding(app, pipeline_name, node_name)
    key = _delivery_key(pipeline_name, node_name, rendered_item_id, binding)
    _ensure_delivery_rows(app, pipeline_name, node_name, at)
    node = _delivery(app, pipeline_name, node_name)
    with write_transaction(app.conn):
        node_binding = _node_binding(app, pipeline_name, node_name)
        member = app.conn.execute(
            "SELECT 1 FROM node_members WHERE pipeline_name=? AND node_name=? "
            "AND binding_digest=? AND item_id=? AND state!='stale'",
            (pipeline_name, node_name, node_binding, rendered_item_id),
        ).fetchone()
        if member is None:
            raise ExecutionError(
                f"rendered Item is not admitted to {pipeline_name}.{node_name}: "
                f"{rendered_item_id}"
            )
        app.conn.execute(
            "INSERT OR IGNORE INTO deliveries "
            "(delivery_key,pipeline_name,node_name,rendered_item_id,binding_digest,config_json,"
            "policy_json,state,retry_count,attempt_token,error,external_id,updated_at) "
            "VALUES (?,?,?,?,?,?,?,'queued',0,NULL,NULL,NULL,?)",
            (
                key,
                pipeline_name,
                node_name,
                rendered_item_id,
                binding,
                canonical_json(node.deliver.with_config),
                _policy_json(node.policy),
                _timestamp(at),
            ),
        )
    return key


def _claim_delivery(
    app: Any,
    delivery_key: str,
    at: datetime,
    *,
    manual_retry: bool = False,
) -> _DeliveryAttempt | None:
    at_text = _timestamp(at)
    with write_transaction(app.conn):
        row = app.conn.execute(
            "SELECT * FROM deliveries WHERE delivery_key=?", (delivery_key,)
        ).fetchone()
        if row is None:
            raise ExecutionError(f"Delivery is missing: {delivery_key}")
        state = str(row["state"])
        if state in {"succeeded", "running"}:
            return None
        if state not in {"queued", "failed"}:
            raise ExecutionError(f"invalid Delivery state: {state!r}")
        current_binding = _binding(
            app, str(row["pipeline_name"]), str(row["node_name"])
        )
        if current_binding != str(row["binding_digest"]):
            raise ExecutionError(
                "Delivery binding changed; the historical Delivery cannot be retried"
            )
        policy = _decode_policy(str(row["policy_json"]))
        if (
            state == "failed"
            and not manual_retry
            and int(row["retry_count"]) >= policy.retries
        ):
            return None
        running = int(
            app.conn.execute(
                "SELECT COUNT(*) FROM deliveries WHERE pipeline_name=? AND node_name=? "
                "AND state='running'",
                (row["pipeline_name"], row["node_name"]),
            ).fetchone()[0]
        )
        if running >= int(policy.concurrency):
            return None
        token = (0 if row["attempt_token"] is None else int(row["attempt_token"])) + 1
        retry_count = int(row["retry_count"]) + (1 if state == "failed" else 0)
        changed = app.conn.execute(
            "UPDATE deliveries SET state='running',retry_count=?,attempt_token=?,error=NULL,"
            "external_id=NULL,updated_at=? WHERE delivery_key=? AND state=?",
            (retry_count, token, at_text, delivery_key, state),
        )
        if changed.rowcount != 1:
            return None
        row = app.conn.execute(
            "SELECT * FROM deliveries WHERE delivery_key=?", (delivery_key,)
        ).fetchone()
        assert row is not None
        _log_attempt(
            "delivery_claim",
            row,
            attempt_token=token,
            external_id=None,
            state="running",
        )
        return _DeliveryAttempt(delivery_key, token)


def _complete_delivery(
    app: Any,
    attempt: _DeliveryAttempt,
    external_id: str | None,
    at: datetime,
) -> bool:
    at_text = _timestamp(at)
    with write_transaction(app.conn):
        row = app.conn.execute(
            "SELECT * FROM deliveries WHERE delivery_key=?", (attempt.delivery_key,)
        ).fetchone()
        if row is None:
            return False
        changed = app.conn.execute(
            "UPDATE deliveries SET state='succeeded',external_id=?,error=NULL,updated_at=? "
            "WHERE delivery_key=? AND state='running' AND attempt_token=?",
            (external_id, at_text, attempt.delivery_key, attempt.attempt_token),
        )
        if changed.rowcount != 1:
            _log_attempt(
                "delivery_stale_completion",
                row,
                attempt_token=attempt.attempt_token,
                external_id=external_id,
                state="stale",
            )
            return False
        row = app.conn.execute(
            "SELECT * FROM deliveries WHERE delivery_key=?", (attempt.delivery_key,)
        ).fetchone()
        assert row is not None
        _log_attempt(
            "delivery_complete",
            row,
            attempt_token=attempt.attempt_token,
            external_id=external_id,
            state="succeeded",
        )
        return True


def _fail_delivery_attempt(
    app: Any,
    attempt: _DeliveryAttempt,
    error: BaseException | str,
    at: datetime,
) -> bool:
    message = (
        str(error) or error.__class__.__name__
        if isinstance(error, BaseException)
        else str(error)
    )
    at_text = _timestamp(at)
    with write_transaction(app.conn):
        row = app.conn.execute(
            "SELECT * FROM deliveries WHERE delivery_key=?", (attempt.delivery_key,)
        ).fetchone()
        if row is None:
            return False
        changed = app.conn.execute(
            "UPDATE deliveries SET state='failed',error=?,external_id=NULL,updated_at=? "
            "WHERE delivery_key=? AND state='running' AND attempt_token=?",
            (message, at_text, attempt.delivery_key, attempt.attempt_token),
        )
        if changed.rowcount != 1:
            _log_attempt(
                "delivery_stale_failure",
                row,
                attempt_token=attempt.attempt_token,
                external_id=None,
                state="stale",
            )
            return False
        row = app.conn.execute(
            "SELECT * FROM deliveries WHERE delivery_key=?", (attempt.delivery_key,)
        ).fetchone()
        assert row is not None
        _log_attempt(
            "delivery_failure",
            row,
            attempt_token=attempt.attempt_token,
            external_id=None,
            state="failed",
        )
        return True


def _fail_delivery_attempt_or_log(
    app: Any,
    attempt: _DeliveryAttempt,
    error: BaseException | str,
    at: datetime,
) -> None:
    """Fail the attempt; if that write fails too, log the stuck row and re-raise."""

    try:
        _fail_delivery_attempt(app, attempt, error, at)
    except BaseException:
        log_event(
            _LOGGER,
            "delivery_finalize_failed",
            delivery_key=attempt.delivery_key,
            attempt_token=attempt.attempt_token,
            state="running",
        )
        raise


def _send_attempt(app: Any, attempt: _DeliveryAttempt, at: datetime) -> None:
    # Worker-thread reads must not observe another thread's open transaction.
    # The lock is released before the external Sink call.
    with app.conn.transaction_lock:
        row = app.conn.execute(
            "SELECT * FROM deliveries WHERE delivery_key=?", (attempt.delivery_key,)
        ).fetchone()
    if row is None:
        return
    try:
        raw_config = json.loads(str(row["config_json"]))
        if not isinstance(raw_config, Mapping):
            raise ExecutionError("persisted Delivery config must be an object")
        payload = _item_payload(app, str(row["rendered_item_id"]))
        pipeline_name = str(row["pipeline_name"])
        node_name = str(row["node_name"])
        node = _delivery(app, pipeline_name, node_name)
        if _binding(app, pipeline_name, node_name) != str(row["binding_digest"]):
            raise ExecutionError(
                "Delivery binding changed; the historical Delivery cannot be sent"
            )
        sink = app.sinks.get(node.deliver.use)
        if sink is None:
            raise ExecutionError(f"resolved Sink is unavailable: {node.deliver.use}")
        # Recovery/timeout may have replaced this attempt while the payload,
        # binding, and sink were being resolved.  Do not invoke an external
        # Sink unless this worker still owns the running row and token.  The
        # check is deliberately outside a long transaction: the external
        # call retains its documented duplicate window, while stale completion
        # and failure updates remain guarded by their own token predicates.
        # Re-read after all local preparation so recovery/timeout transitions
        # that happen in the meantime are observed by the ownership check.
        with app.conn.transaction_lock:
            current = app.conn.execute(
                "SELECT * FROM deliveries WHERE delivery_key=?",
                (attempt.delivery_key,),
            ).fetchone()
        if current is None:
            return
        current_token = current["attempt_token"]
        if (
            current["state"] != "running"
            or current_token is None
            or int(current_token) != attempt.attempt_token
        ):
            _log_attempt(
                "delivery_stale_send",
                current,
                attempt_token=attempt.attempt_token,
                external_id=None,
                state="stale",
            )
            return
        result = sink.send(raw_config, payload, attempt.delivery_key)
    except BaseException as error:  # noqa: BLE001 - finalize the durable claim on any exit
        _fail_delivery_attempt_or_log(app, attempt, error, at)
        return
    if result is not None and not isinstance(result, str):
        # The message was already sent: coerce instead of failing and retrying.
        log_event(
            _LOGGER,
            "delivery_non_string_result",
            delivery_key=attempt.delivery_key,
            result_type=type(result).__name__,
        )
        result = str(result)
    try:
        _complete_delivery(app, attempt, result, at)
    except BaseException:
        # A row left running would hold its concurrency slot indefinitely.
        # Fail it with a fixed reason and still surface the original error.
        _fail_delivery_attempt_or_log(app, attempt, "delivery completion failed", at)
        raise


def deliver(
    app: Any,
    pipeline_name: str,
    node_name: str,
    rendered_item_id: str,
    at: datetime,
) -> str:
    """Claim and execute one Delivery row, returning its stable key."""

    if not isinstance(rendered_item_id, str) or not rendered_item_id:
        raise ExecutionError("rendered_item_id must be a non-empty string")
    # Direct callers may intentionally bypass ``tick``.  Admission remains
    # generic; this call only makes sure the requested terminal member exists.
    from .reconcile import reconcile_node

    reconcile_node(app, pipeline_name, node_name, at)
    key = _ensure_delivery_row_for_item(
        app, pipeline_name, node_name, rendered_item_id, at
    )
    attempt = _claim_delivery(app, key, at)
    if attempt is not None:
        _send_attempt(app, attempt, at)
    return key


def retry_delivery(app: Any, delivery_key: str, at: datetime) -> str:
    """Retry one failed Delivery without requesting any upstream work."""

    if not isinstance(delivery_key, str) or not delivery_key:
        raise ExecutionError("delivery_key must be a non-empty string")
    attempt = _claim_delivery(app, delivery_key, at, manual_retry=True)
    if attempt is not None:
        _send_attempt(app, attempt, at)
    return delivery_key


def _timeout_deliveries(
    app: Any,
    at: datetime,
    allowed_nodes: set[tuple[str, str]] | None = None,
) -> bool:
    changed = False
    now = utc_datetime(at)
    with app.conn.transaction_lock:
        rows = app.conn.execute(
            "SELECT * FROM deliveries WHERE state='running' ORDER BY delivery_key"
        ).fetchall()
    for row in rows:
        if (
            allowed_nodes is not None
            and (
                str(row["pipeline_name"]),
                str(row["node_name"]),
            )
            not in allowed_nodes
        ):
            continue
        try:
            policy = _decode_policy(str(row["policy_json"]))
            updated = utc_datetime(str(row["updated_at"]))
        except ExecutionError:
            continue
        if policy.timeout is None or now < updated + policy.timeout:
            continue
        token = row["attempt_token"]
        if token is None:
            continue
        changed = (
            _fail_delivery_attempt(
                app,
                _DeliveryAttempt(str(row["delivery_key"]), int(token)),
                ExecutionError("delivery timeout"),
                at,
            )
            or changed
        )
    return changed


def _recover_deliveries(app: Any, at: datetime) -> bool:
    changed = False
    rows = app.conn.execute(
        "SELECT delivery_key,attempt_token FROM deliveries WHERE state='running' ORDER BY delivery_key"
    ).fetchall()
    for row in rows:
        if row["attempt_token"] is None:
            continue
        changed = (
            _fail_delivery_attempt(
                app,
                _DeliveryAttempt(str(row["delivery_key"]), int(row["attempt_token"])),
                ExecutionError("startup recovery"),
                at,
            )
            or changed
        )
    return changed


def _delivery_rows_for_dispatch(
    app: Any,
    pipeline_name: str,
    node_name: str,
    *,
    include_failed: bool,
) -> list[str]:
    _delivery(app, pipeline_name, node_name)
    binding = _binding(app, pipeline_name, node_name)
    node_binding = _node_binding(app, pipeline_name, node_name)
    state_filter = (
        "(d.state='queued' OR d.state='failed')"
        if include_failed
        else "d.state='queued'"
    )
    with app.conn.transaction_lock:
        rows = app.conn.execute(
            "SELECT d.delivery_key FROM deliveries d JOIN node_members nm ON "
            "nm.pipeline_name=d.pipeline_name AND nm.node_name=d.node_name "
            "AND nm.binding_digest=? AND nm.item_id=d.rendered_item_id "
            "WHERE nm.state!='stale' AND d.pipeline_name=? AND d.node_name=? AND d.binding_digest=? AND "
            + state_filter
            + " ORDER BY nm.ordinal,d.rendered_item_id",
            (node_binding, pipeline_name, node_name, binding),
        ).fetchall()
    return [str(row[0]) for row in rows]


def _dispatch_deliveries(
    app: Any,
    at: datetime,
    allowed_nodes: set[tuple[str, str]] | None = None,
) -> bool:
    """Ensure and drain terminal nodes inside the normal reconcile scan."""

    changed = False
    for pipeline_name, pipeline in app.definition.pipelines.items():
        for node_name, node in pipeline.nodes.items():
            if node.deliver is None:
                continue
            if (
                allowed_nodes is not None
                and (pipeline_name, node_name) not in allowed_nodes
            ):
                continue
            _ensure_delivery_rows(app, pipeline_name, node_name, at)
            with app.conn.transaction_lock:
                running = int(
                    app.conn.execute(
                        "SELECT COUNT(*) FROM deliveries WHERE pipeline_name=? AND node_name=? "
                        "AND state='running'",
                        (pipeline_name, node_name),
                    ).fetchone()[0]
                )
                policy_row = app.conn.execute(
                    "SELECT policy_json FROM deliveries WHERE pipeline_name=? AND node_name=? "
                    "AND binding_digest=? AND state IN ('queued','running','failed') "
                    "ORDER BY updated_at,delivery_key LIMIT 1",
                    (pipeline_name, node_name, _binding(app, pipeline_name, node_name)),
                ).fetchone()
            try:
                concurrency = (
                    _decode_policy(str(policy_row[0])).concurrency
                    if policy_row is not None
                    else int(node.policy.concurrency)
                )
            except (ExecutionError, TypeError, ValueError):
                concurrency = int(node.policy.concurrency)
            available = max(0, int(concurrency) - running)
            if available <= 0:
                continue
            dispatch_keys: list[str] = []
            for key in _delivery_rows_for_dispatch(
                app, pipeline_name, node_name, include_failed=True
            ):
                if available <= 0:
                    break
                with app.conn.transaction_lock:
                    before = app.conn.execute(
                        "SELECT state,retry_count FROM deliveries WHERE delivery_key=?",
                        (key,),
                    ).fetchone()
                if before is None:
                    continue
                # Automatic tick retries honor the persisted retry budget;
                # explicit retry_delivery is the manual escape hatch.
                if before[0] == "failed":
                    try:
                        with app.conn.transaction_lock:
                            policy_json = app.conn.execute(
                                "SELECT policy_json FROM deliveries WHERE delivery_key=?",
                                (key,),
                            ).fetchone()[0]
                        policy = _decode_policy(str(policy_json))
                    except (ExecutionError, TypeError, AttributeError):
                        continue
                    if int(before[1]) >= policy.retries:
                        continue
                dispatch_keys.append(key)
                available -= 1
            if dispatch_keys:
                # Claims are serialized by the shared SQLite writer lock, but
                # external Sink calls may overlap up to node concurrency.
                # Submission follows manifest order; each worker still sends
                # only after its own guarded row claim.
                def _run(
                    key: str,
                    pipeline_name: str = pipeline_name,
                    node_name: str = node_name,
                ) -> None:
                    with app.conn.transaction_lock:
                        row = app.conn.execute(
                            "SELECT rendered_item_id FROM deliveries WHERE delivery_key=?",
                            (key,),
                        ).fetchone()
                    if row is None:
                        return
                    deliver(
                        app,
                        pipeline_name,
                        node_name,
                        str(row[0]),
                        at,
                    )

                with ThreadPoolExecutor(max_workers=len(dispatch_keys)) as pool:
                    futures = [pool.submit(_run, key) for key in dispatch_keys]
                    for future in futures:
                        future.result()
                changed = True
    return changed


__all__ = [
    "_DeliveryAttempt",
    "_claim_delivery",
    "_complete_delivery",
    "_dispatch_deliveries",
    "_ensure_delivery_rows",
    "_fail_delivery_attempt",
    "_recover_deliveries",
    "_timeout_deliveries",
    "deliver",
    "retry_delivery",
]
