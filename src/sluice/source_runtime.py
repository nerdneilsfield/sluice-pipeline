"""Durable Source claim, collection, and publication runtime."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
import json
import logging
import sqlite3
from typing import Any

from sluice.app import App
from sluice.canonical import canonical_json, content_hash, digest, make_item_id, utc_datetime
from sluice.contracts import (
    KEEP_CHECKPOINT,
    Item,
    JSONValue,
    SourceRecord,
    SourceResult,
)
from sluice.db import write_transaction
from sluice.errors import ExecutionError
from sluice.logging import log_event


_LOGGER = logging.getLogger("sluice.source_runtime")


def _fresh_json(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: _fresh_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_fresh_json(item) for item in value]
    return value


def _source_config(name: str, source: Any) -> dict[str, object]:
    return {
        "source": name,
        "use": source.use,
        "with": _fresh_json(source.with_config),
    }


def _binding_digest(name: str, source: Any) -> str:
    return digest(_source_config(name, source))


def _timestamp(value: datetime) -> str:
    return utc_datetime(value).isoformat().replace("+00:00", "Z")


def _policy_json(source: Any) -> str:
    policy = source.policy
    timeout = None if policy.timeout is None else policy.timeout.total_seconds()
    return canonical_json(
        {"timeout_seconds": timeout, "retries": policy.retries, "concurrency": policy.concurrency}
    )


def _source_retry_count(policy_json: str) -> int:
    try:
        value = json.loads(policy_json)
    except (TypeError, ValueError, json.JSONDecodeError) as error:
        raise ExecutionError("persisted Source policy is invalid JSON") from error
    retries = value.get("retries", 0) if isinstance(value, Mapping) else 0
    if isinstance(retries, bool) or not isinstance(retries, int) or retries < 0:
        raise ExecutionError("persisted Source retry policy is invalid")
    return retries


def _strict_json(
    value: object,
    label: str,
    _active: set[int] | None = None,
) -> None:
    """Validate a Source boundary value before it enters durable state."""

    active = set() if _active is None else _active
    if value is None or isinstance(value, (bool, int, str)):
        return
    if isinstance(value, float):
        # canonical_json performs the finite-number check with a useful error.
        canonical_json(value)
        return
    if isinstance(value, Mapping):
        marker = id(value)
        if marker in active:
            raise ExecutionError(f"{label} contains a recursive value")
        active.add(marker)
        try:
            for key, item in value.items():
                if not isinstance(key, str):
                    raise ExecutionError(f"{label} object keys must be strings")
                _strict_json(item, label, active)
        finally:
            active.remove(marker)
        return
    if isinstance(value, (list, tuple)):
        marker = id(value)
        if marker in active:
            raise ExecutionError(f"{label} contains a recursive value")
        active.add(marker)
        try:
            for item in value:
                _strict_json(item, label, active)
        finally:
            active.remove(marker)
        return
    raise ExecutionError(f"{label} is not JSON-compatible")


def _validate_result(result: object) -> SourceResult:
    if not isinstance(result, SourceResult):
        raise ExecutionError("Source callable must return SourceResult")
    records = result.records
    if not isinstance(records, (tuple, list)):
        raise ExecutionError("SourceResult.records must be an ordered sequence")
    normalized: list[SourceRecord] = []
    for record in records:
        if not isinstance(record, SourceRecord):
            raise ExecutionError("SourceResult.records must contain SourceRecord values")
        if not isinstance(record.logical_id, str) or not record.logical_id.strip():
            raise ExecutionError("SourceRecord.logical_id must be a non-empty string")
        if not isinstance(record.metadata, Mapping):
            raise ExecutionError("SourceRecord.metadata must be a JSON object")
        _strict_json(record.payload, "SourceRecord.payload")
        _strict_json(record.metadata, "SourceRecord.metadata")
        # Force canonicalization now, before opening the finalization
        # transaction, so malformed records cannot partially publish.
        canonical_json(record.payload)
        canonical_json(record.metadata)
        normalized.append(record)
    if result.next_checkpoint is not KEEP_CHECKPOINT:
        _strict_json(result.next_checkpoint, "SourceResult.next_checkpoint")
        canonical_json(result.next_checkpoint)
    return SourceResult(tuple(normalized), result.next_checkpoint)


def _checkpoint_value(value: str | None) -> JSONValue:
    if value is None:
        return None
    try:
        decoded = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError) as error:
        raise ExecutionError("persisted Source checkpoint is invalid JSON") from error
    _strict_json(decoded, "Source checkpoint")
    return decoded


def _log_fields(
    source: str,
    binding: str,
    execution_id: int | None,
    checkpoint: object,
    revision: int | None,
    state: str,
) -> dict[str, JSONValue]:
    # Keep the structured contract JSON-compatible even when a diagnostic
    # caller passes the identity-only checkpoint sentinel.
    checkpoint_value: JSONValue = (
        {"marker": "KEEP_CHECKPOINT"}
        if checkpoint is KEEP_CHECKPOINT
        else checkpoint  # type: ignore[assignment]
    )
    return {
        "source": source,
        "binding_digest": binding,
        "execution_id": execution_id,
        "checkpoint": checkpoint_value,
        "revision": revision,
        "state": state,
    }


def _activate_binding(
    app: App,
    source_name: str,
    source: Any,
    binding: str,
    at_text: str,
    trigger: str,
) -> tuple[int | None, JSONValue, list[tuple[int, str]], bool]:
    """Create/select the current binding and claim one Source execution."""

    conn = app.conn
    source_config = {**_source_config(source_name, source), "trigger": trigger}
    config_json = canonical_json(source_config)
    policy_json = _policy_json(source)
    superseded: list[tuple[int, str]] = []
    activated = False

    with write_transaction(conn):
        current = conn.execute(
            "SELECT binding_digest FROM source_current_bindings WHERE source_name=?",
            (source_name,),
        ).fetchone()
        current_binding = None if current is None else current[0]
        if current_binding != binding:
            activated = current_binding is not None
            if current_binding is not None:
                old_state = conn.execute(
                    "SELECT active_execution_id FROM source_state WHERE source_name=? AND binding_digest=?",
                    (source_name, current_binding),
                ).fetchone()
                old_execution_id = None if old_state is None else old_state[0]
                if old_execution_id is not None:
                    old_update = conn.execute(
                        "UPDATE executions SET state='failed', error=?, finished_at=? "
                        "WHERE id=? AND kind='source' AND state IN ('queued','running')",
                        ("superseded binding", at_text, old_execution_id),
                    )
                    if old_update.rowcount:
                        superseded.append((old_execution_id, current_binding))
                    conn.execute(
                        "UPDATE source_state SET active_execution_id=NULL "
                        "WHERE source_name=? AND binding_digest=? AND active_execution_id=?",
                        (source_name, current_binding, old_execution_id),
                    )

            conn.execute(
                "INSERT OR IGNORE INTO source_state "
                "(source_name,binding_digest,checkpoint_json,next_due_at,active_execution_id) "
                "VALUES (?,?,NULL,NULL,NULL)",
                (source_name, binding),
            )
            # A changed binding starts a fresh durable publication partition.
            # The first ever binding has no row until a successful collection;
            # this keeps a failed initial attempt from publishing anything.
            if activated:
                revision_zero = conn.execute(
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
                    conn.execute(
                        "INSERT INTO publications(kind,producer_key,binding_digest,revision,digest) "
                        "VALUES ('delta',?,?,0,?)",
                        (source_name, binding, empty_digest),
                    )
            conn.execute(
                "INSERT INTO source_current_bindings(source_name,binding_digest) VALUES (?,?) "
                "ON CONFLICT(source_name) DO UPDATE SET binding_digest=excluded.binding_digest",
                (source_name, binding),
            )

        state = conn.execute(
            "SELECT checkpoint_json,active_execution_id FROM source_state "
            "WHERE source_name=? AND binding_digest=?",
            (source_name, binding),
        ).fetchone()
        if state is None:
            raise ExecutionError("Source binding activation failed")
        expected_json = state[0]
        active_execution_id = state[1]
        if active_execution_id is not None:
            active_state = conn.execute(
                "SELECT state FROM executions WHERE id=? AND kind='source'",
                (active_execution_id,),
            ).fetchone()
            if active_state is not None and active_state[0] in {"queued", "running"}:
                raise ExecutionError("Source binding already has an active execution")
            conn.execute(
                "UPDATE source_state SET active_execution_id=NULL "
                "WHERE source_name=? AND binding_digest=? AND active_execution_id=?",
                (source_name, binding, active_execution_id),
            )

        # Source schedules live on SourceDef rather than ExecutionPolicy.
        schedule = source.schedule
        next_due = (
            utc_datetime(datetime.fromisoformat(at_text.replace("Z", "+00:00"))) + schedule
            if schedule is not None
            else None
        )
        next_due_text = None if next_due is None else _timestamp(next_due)

        try:
            cursor = conn.execute(
                "INSERT INTO executions "
                "(kind,source_name,state,binding_digest,config_json,expected_checkpoint_json,policy_json,started_at) "
                "VALUES ('source',?,'running',?,?,?,?,?)",
                (
                    source_name,
                    binding,
                    config_json,
                    expected_json,
                    policy_json,
                    at_text,
                ),
            )
        except sqlite3.IntegrityError as error:
            raise ExecutionError("Source binding already has an active execution") from error
        execution_id = int(cursor.lastrowid)
        conn.execute(
            "UPDATE source_state SET next_due_at=?,active_execution_id=? "
            "WHERE source_name=? AND binding_digest=?",
            (next_due_text, execution_id, source_name, binding),
        )

    checkpoint = _checkpoint_value(expected_json)
    return execution_id, checkpoint, superseded, activated


def _record_failure(
    app: App,
    source_name: str,
    binding: str,
    execution_id: int,
    expected_json: str | None,
    error: BaseException,
    at_text: str,
) -> None:
    message = str(error) or error.__class__.__name__
    retry_id: int | None = None
    with write_transaction(app.conn):
        row = app.conn.execute(
            "SELECT config_json,policy_json,retry_count,expected_checkpoint_json,state "
            "FROM executions WHERE id=? AND kind='source' AND binding_digest=?",
            (execution_id, binding),
        ).fetchone()
        updated = app.conn.execute(
            "UPDATE executions SET state='failed',error=?,finished_at=? "
            "WHERE id=? AND kind='source' AND state IN ('queued','running') "
            "AND binding_digest=?",
            (message, at_text, execution_id, binding),
        )
        app.conn.execute(
            "UPDATE source_state SET active_execution_id=NULL "
            "WHERE source_name=? AND binding_digest=? AND active_execution_id=?",
            (source_name, binding, execution_id),
        )
        # A failed Source retries only while its binding is still current.  A
        # stale completion (for example one superseded by a definition change
        # or startup recovery) must never create a second retry chain.
        if updated.rowcount == 1 and row is not None:
            current = app.conn.execute(
                "SELECT binding_digest FROM source_current_bindings WHERE source_name=?",
                (source_name,),
            ).fetchone()
            retry_limit = _source_retry_count(str(row["policy_json"]))
            retry_count = int(row["retry_count"])
            if (
                current is not None
                and str(current[0]) == binding
                and retry_count < retry_limit
            ):
                cursor = app.conn.execute(
                    "INSERT INTO executions "
                    "(kind,source_name,state,binding_digest,config_json,expected_checkpoint_json,"
                    "policy_json,retry_count,retry_parent_id) VALUES ('source',?,'queued',?,?,?,?,?,?)",
                    (
                        source_name,
                        binding,
                        str(row["config_json"]),
                        row["expected_checkpoint_json"],
                        str(row["policy_json"]),
                        retry_count + 1,
                        execution_id,
                    ),
                )
                retry_id = int(cursor.lastrowid)
                app.conn.execute(
                    "UPDATE source_state SET active_execution_id=? WHERE source_name=? "
                    "AND binding_digest=? AND active_execution_id IS NULL",
                    (retry_id, source_name, binding),
                )
    log_event(
        _LOGGER,
        "source_rollback",
        **_log_fields(
            source_name,
            binding,
            execution_id,
            _checkpoint_value(expected_json),
            None,
            "failed" if updated.rowcount else "stale",
        ),
    )
    if retry_id is not None:
        log_event(
            _LOGGER,
            "source_retry_queued",
            **_log_fields(
                source_name,
                binding,
                retry_id,
                _checkpoint_value(expected_json),
                None,
                "queued",
            ),
            retry_parent_id=execution_id,
        )


def _recover_source_execution(app: App, execution_id: int, at: datetime) -> int | None:
    """Fail an interrupted Source and atomically hand its claim to a retry.

    Recovery deliberately uses the same immutable config/checkpoint snapshot as
    ordinary failure handling.  It returns the queued retry ID, when one was
    admitted, and is intentionally private to the reconciliation state machine.
    """

    at_text = _timestamp(at)
    retry_id: int | None = None
    source_name: str | None = None
    binding: str | None = None
    expected_json: str | None = None
    with write_transaction(app.conn):
        row = app.conn.execute(
            "SELECT * FROM executions WHERE id=? AND kind='source' AND state IN ('queued','running')",
            (execution_id,),
        ).fetchone()
        if row is None:
            return None
        source_name = str(row["source_name"])
        binding = str(row["binding_digest"])
        expected_json = row["expected_checkpoint_json"]
        guard = app.conn.execute(
            "UPDATE executions SET state='failed',error=?,finished_at=? WHERE id=? "
            "AND kind='source' AND state IN ('queued','running')",
            ("startup recovery", at_text, execution_id),
        )
        if guard.rowcount != 1:
            return None
        app.conn.execute(
            "UPDATE source_state SET active_execution_id=NULL WHERE source_name=? "
            "AND binding_digest=? AND active_execution_id=?",
            (source_name, binding, execution_id),
        )
        current = app.conn.execute(
            "SELECT binding_digest FROM source_current_bindings WHERE source_name=?",
            (source_name,),
        ).fetchone()
        retry_limit = _source_retry_count(str(row["policy_json"]))
        if (
            current is not None
            and str(current[0]) == binding
            and int(row["retry_count"]) < retry_limit
        ):
            cursor = app.conn.execute(
                "INSERT INTO executions "
                "(kind,source_name,state,binding_digest,config_json,expected_checkpoint_json,"
                "policy_json,retry_count,retry_parent_id) VALUES ('source',?,'queued',?,?,?,?,?,?)",
                (
                    source_name,
                    binding,
                    str(row["config_json"]),
                    expected_json,
                    str(row["policy_json"]),
                    int(row["retry_count"]) + 1,
                    execution_id,
                ),
            )
            retry_id = int(cursor.lastrowid)
            app.conn.execute(
                "UPDATE source_state SET active_execution_id=? WHERE source_name=? "
                "AND binding_digest=? AND active_execution_id IS NULL",
                (retry_id, source_name, binding),
            )
    log_event(
        _LOGGER,
        "source_recovery",
        **_log_fields(
            source_name or "",
            binding or "",
            execution_id,
            _checkpoint_value(expected_json),
            None,
            "failed",
        ),
        retry_execution_id=retry_id,
    )
    return retry_id


def _execute_queued_source(app: App, execution_id: int, at: datetime) -> int:
    """Run one persisted queued Source attempt (used by ``tick``)."""

    at_text = _timestamp(at)
    with write_transaction(app.conn):
        row = app.conn.execute(
            "SELECT * FROM executions WHERE id=? AND kind='source' AND state='queued'",
            (execution_id,),
        ).fetchone()
        if row is None:
            return execution_id
        source_name = str(row["source_name"])
        binding = str(row["binding_digest"])
        configured = app.definition.sources.get(source_name)
        configured_binding = (
            None if configured is None else _binding_digest(source_name, configured)
        )
        current = app.conn.execute(
            "SELECT binding_digest FROM source_current_bindings WHERE source_name=?",
            (source_name,),
        ).fetchone()
        current_binding = None if current is None else str(current[0])
        if configured_binding != binding or current_binding != binding:
            app.conn.execute(
                "UPDATE executions SET state='failed',error=?,finished_at=? "
                "WHERE id=? AND kind='source' AND state='queued'",
                ("superseded binding", at_text, execution_id),
            )
            app.conn.execute(
                "UPDATE source_state SET active_execution_id=NULL WHERE source_name=? "
                "AND binding_digest=? AND active_execution_id=?",
                (source_name, binding, execution_id),
            )
            return execution_id
        started = app.conn.execute(
            "UPDATE executions SET state='running',started_at=? WHERE id=? "
            "AND kind='source' AND state='queued'",
            (at_text, execution_id),
        )
        if started.rowcount != 1:
            return execution_id
        expected_json = row["expected_checkpoint_json"]
        config = json.loads(str(row["config_json"]))
        source_config = config.get("with", {}) if isinstance(config, Mapping) else {}
    try:
        source = app.definition.sources[source_name]
        contract = app.sources[source_name]
        checkpoint = _checkpoint_value(expected_json)
        result = _validate_result(contract.collect(_fresh_json(source_config), checkpoint))
    except BaseException as error:
        wrapped = error if isinstance(error, ExecutionError) else ExecutionError(str(error) or "Source collection failed")
        if wrapped is not error:
            wrapped.__cause__ = error
        _record_failure(app, source_name, binding, execution_id, expected_json, wrapped, at_text)
        return execution_id
    finalized, revision = _finalize_source(
        app, source_name, binding, execution_id, expected_json, at_text, result
    )
    if not finalized:
        _record_failure(
            app,
            source_name,
            binding,
            execution_id,
            expected_json,
            ExecutionError("stale Source finalization"),
            at_text,
        )
    else:
        log_event(
            _LOGGER,
            "source_finalization",
            **_log_fields(
                source_name,
                binding,
                execution_id,
                _checkpoint_value(expected_json),
                revision,
                "succeeded",
            ),
        )
    return execution_id


def _finalize_success(
    app: App,
    source_name: str,
    binding: str,
    execution_id: int,
    expected_json: str | None,
    at_text: str,
    result: SourceResult,
) -> tuple[bool, int | None]:
    conn = app.conn
    inserted_ids: list[str] = []
    revision: int | None = None
    with write_transaction(conn):
        # Guard before touching any output table.  A stale completion can
        # therefore never leave an Item or Publication behind.
        guard = conn.execute(
            "UPDATE executions SET state='succeeded',finished_at=? "
            "WHERE id=? AND kind='source' AND state='running' AND binding_digest=? "
            "AND expected_checkpoint_json IS ? "
            "AND EXISTS (SELECT 1 FROM source_current_bindings cb "
            "JOIN source_state ss ON ss.source_name=cb.source_name AND ss.binding_digest=cb.binding_digest "
            "WHERE cb.source_name=? AND cb.binding_digest=? AND ss.active_execution_id=? "
            "AND ss.checkpoint_json IS ?)",
            (
                at_text,
                execution_id,
                binding,
                expected_json,
                source_name,
                binding,
                execution_id,
                expected_json,
            ),
        )
        if guard.rowcount != 1:
            return False, None

        for record in result.records:
            item_id = make_item_id(
                ("source", source_name),
                record.logical_id,
                record.payload,
                record.metadata,
            )
            item = Item(
                id=item_id,
                producer_scope=("source", source_name),
                logical_id=record.logical_id,
                payload=record.payload,
                metadata=record.metadata,
                content_hash=content_hash(record.payload),
                observed_at=at_text,
                produced_by=execution_id,
            )
            payload_json = canonical_json(item.payload)
            metadata_json = canonical_json(item.metadata)
            inserted = conn.execute(
                "INSERT OR IGNORE INTO items "
                "(id,producer_kind,producer_name,logical_id,payload_json,metadata_json,content_hash,observed_at,produced_by) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    item_id,
                    "source",
                    source_name,
                    record.logical_id,
                    payload_json,
                    metadata_json,
                    item.content_hash,
                    item.observed_at,
                    item.produced_by,
                ),
            )
            if inserted.rowcount == 1:
                inserted_ids.append(item_id)

        max_revision = conn.execute(
            "SELECT COALESCE(MAX(revision),0) FROM publications "
            "WHERE producer_key=? AND binding_digest=?",
            (source_name, binding),
        ).fetchone()[0]
        revision = int(max_revision) + 1
        publication_digest = digest(
            {
                "kind": "delta",
                "producer_key": source_name,
                "binding_digest": binding,
                "members": inserted_ids,
            }
        )
        publication = conn.execute(
            "INSERT INTO publications(kind,producer_key,binding_digest,revision,digest) "
            "VALUES ('delta',?,?,?,?)",
            (source_name, binding, revision, publication_digest),
        )
        publication_id = int(publication.lastrowid)
        conn.executemany(
            "INSERT INTO publication_members(publication_id,ordinal,item_id) VALUES (?,?,?)",
            ((publication_id, ordinal, item_id) for ordinal, item_id in enumerate(inserted_ids)),
        )
        if result.next_checkpoint is not KEEP_CHECKPOINT:
            checkpoint_json = canonical_json(result.next_checkpoint)
            conn.execute(
                "UPDATE source_state SET checkpoint_json=?,active_execution_id=NULL "
                "WHERE source_name=? AND binding_digest=? AND active_execution_id=? "
                "AND checkpoint_json IS ?",
                (
                    checkpoint_json,
                    source_name,
                    binding,
                    execution_id,
                    expected_json,
                ),
            )
        else:
            conn.execute(
                "UPDATE source_state SET active_execution_id=NULL "
                "WHERE source_name=? AND binding_digest=? AND active_execution_id=? "
                "AND checkpoint_json IS ?",
                (source_name, binding, execution_id, expected_json),
            )
        conn.execute(
            "UPDATE executions SET result_id=? WHERE id=? AND state='succeeded'",
            (publication_id, execution_id),
        )
    return True, revision


def _finalize_source(
    app: App,
    source_name: str,
    binding: str,
    execution_id: int,
    expected_json: str | None,
    at_text: str,
    result: SourceResult,
) -> tuple[bool, int | None]:
    """Named Task 6 boundary for guarded Source finalization."""

    return _finalize_success(
        app, source_name, binding, execution_id, expected_json, at_text, result
    )


def collect_source(
    app: App,
    source_name: str,
    at: datetime,
    *,
    trigger: str = "manual",
) -> int:
    """Claim, collect, validate, and durably publish one configured Source."""

    try:
        source = app.definition.sources[source_name]
        contract = app.sources[source_name]
    except KeyError as error:
        raise ExecutionError(f"unknown Source: {source_name}") from error
    if not isinstance(trigger, str) or not trigger:
        raise ExecutionError("trigger must be a non-empty string")
    at_text = _timestamp(at)
    binding = _binding_digest(source_name, source)

    try:
        execution_id, checkpoint, superseded, _activated = _activate_binding(
            app, source_name, source, binding, at_text, trigger
        )
    except ExecutionError:
        raise
    for old_execution_id, old_binding in superseded:
        log_event(
            _LOGGER,
            "source_supersession",
            **_log_fields(
                source_name,
                old_binding,
                old_execution_id,
                None,
                None,
                "failed",
            ),
        )
    log_event(
        _LOGGER,
        "source_claim",
        **_log_fields(source_name, binding, execution_id, checkpoint, None, "running"),
    )

    try:
        result = contract.collect(_fresh_json(source.with_config), checkpoint)
        result = _validate_result(result)
        log_event(
            _LOGGER,
            "source_external_completion",
            **_log_fields(source_name, binding, execution_id, checkpoint, None, "returned"),
        )
    except BaseException as error:
        log_event(
            _LOGGER,
            "source_external_completion",
            **_log_fields(source_name, binding, execution_id, checkpoint, None, "failed"),
        )
        wrapped = (
            error
            if isinstance(error, ExecutionError)
            else ExecutionError(str(error) or "Source collection failed")
        )
        if wrapped is not error:
            wrapped.__cause__ = error
        _record_failure(app, source_name, binding, execution_id, _checkpoint_json(app, source_name, binding, execution_id), wrapped, at_text)
        raise wrapped

    expected_json = _checkpoint_json(app, source_name, binding, execution_id)
    try:
        finalized, revision = _finalize_source(
            app,
            source_name,
            binding,
            execution_id,
            expected_json,
            at_text,
            result,
        )
        if not finalized:
            stale = ExecutionError("stale Source finalization")
            log_event(
                _LOGGER,
                "source_stale_finalization",
                **_log_fields(source_name, binding, execution_id, checkpoint, None, "stale"),
            )
            _record_failure(app, source_name, binding, execution_id, expected_json, stale, at_text)
            # A late callback is an expected crash-recovery outcome.  Its
            # guarded finalization already rejected every output; return the
            # terminal attempt rather than surfacing an unhandled worker
            # exception to callback threads.
            return execution_id
    except ExecutionError:
        raise
    except BaseException as error:
        wrapped = ExecutionError("Source finalization failed")
        wrapped.__cause__ = error
        _record_failure(app, source_name, binding, execution_id, expected_json, wrapped, at_text)
        raise wrapped

    log_event(
        _LOGGER,
        "source_finalization",
        **_log_fields(source_name, binding, execution_id, checkpoint, revision, "succeeded"),
    )
    return execution_id


def _checkpoint_json(
    app: App, source_name: str, binding: str, execution_id: int
) -> str | None:
    row = app.conn.execute(
        "SELECT expected_checkpoint_json FROM executions WHERE id=? AND binding_digest=?",
        (execution_id, binding),
    ).fetchone()
    if row is None:
        return None
    return row[0]
