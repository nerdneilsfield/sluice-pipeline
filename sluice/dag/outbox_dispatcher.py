from __future__ import annotations

import json
import logging
import secrets
from datetime import datetime, timezone

import aiosqlite

from sluice.dag.stores.sink_outbox import SinkOutboxStore

logger = logging.getLogger(__name__)


class DeliveryError(Exception):
    """Raised when a sink delivery permanently fails."""


async def finalize_delivery(
    active: aiosqlite.Connection, run_id: int
) -> str | None:
    """Advance pipeline status from 'pending_delivery' to a terminal state
    based on outbox counts.

    Returns the new status if advanced, or None if the row was not in
    'pending_delivery' or outbox is not fully resolved yet.

    Monotonic: only updates rows WHERE status = 'pending_delivery'. Never
    moves success/partial/failed backward.

    Counts BOTH 'pending' and 'retryable' as "still in flight". The
    pipeline cannot advance while either is non-zero — otherwise
    unclaimed rows would silently leave messages undelivered.
    """
    cur = await active.execute(
        "SELECT status, COUNT(*) FROM sink_outbox WHERE run_id = ? "
        "GROUP BY status",
        (run_id,),
    )
    rows = await cur.fetchall()
    counts = dict(rows)
    sent = int(counts.get("sent", 0))
    retryable = int(counts.get("retryable", 0))
    failed = int(counts.get("failed", 0))
    pending = int(counts.get("pending", 0))
    total = sent + retryable + failed + pending
    if total == 0:
        return None
    if pending > 0 or retryable > 0:
        return None  # still in flight
    if failed > 0 and sent > 0:
        new_status = "partial"
    elif failed > 0:
        new_status = "failed"
    else:
        new_status = "success"

    now = datetime.now(timezone.utc).isoformat()
    cur = await active.execute(
        "UPDATE pipelines SET status = ?, finished_at = COALESCE(finished_at, ?) "
        "WHERE id = ? AND status = 'pending_delivery'",
        (new_status, now, run_id),
    )
    return new_status if cur.rowcount else None


async def dispatch_outbox(
    active_path: str,
    *,
    sink_factory,
    max_iterations: int = 1,
    batch_size: int = 50,
    claim_seconds: float = 60.0,
    retry_backoff_seconds: float = 30.0,
) -> dict[str, int]:
    """Process pending outbox rows for the given active DB.

    ``sink_factory(sink_type)`` must return an object with:
      - ``async emit(payload: dict, params: dict) -> str | None`` returning an
        external id (or None if the sink does not provide one).

    Each iteration:
      1. Claim a batch of pending/retryable rows.
      2. For each claimed row, invoke the sink. The payload_json is parsed
         and passed to sink.emit along with the row's metadata.
      3. Mark sent / retryable / failed based on outcome.
      4. After the batch, finalize_delivery for each affected run.

    Returns counts: {'sent': N, 'retryable': N, 'failed': N}.

    Sink lifecycle: the sink is constructed per batch via sink_factory and
    closed via ``await sink.aclose()`` if present (in finally). Sinks
    therefore must NOT be reused across batches by the caller.
    """
    from sluice.dag.db import open_active_db

    counts = {"sent": 0, "retryable": 0, "failed": 0}
    finalized_runs: set[int] = set()

    for _ in range(max_iterations):
        claim_owner = f"dispatcher-{secrets.token_hex(4)}"
        claim_token = secrets.token_hex(8)
        claim_until = (
            datetime.now(timezone.utc).timestamp() + claim_seconds
        )
        claim_until_iso = datetime.fromtimestamp(
            claim_until, tz=timezone.utc
        ).isoformat()

        async with open_active_db(active_path) as conn:
            store = SinkOutboxStore(conn)
            claimed = await store.claim_pending(
                claim_owner=claim_owner,
                claim_token=claim_token,
                claim_until=claim_until_iso,
                batch_size=batch_size,
            )
            if not claimed:
                break

            # Group by sink_type so we can reuse the sink instance within the batch.
            by_sink_type: dict[str, list[dict]] = {}
            for row in claimed:
                by_sink_type.setdefault(row["sink_type"], []).append(row)

            for sink_type, rows_for_sink in by_sink_type.items():
                sink = None
                try:
                    sink = sink_factory(sink_type)
                    for row in rows_for_sink:
                        await _dispatch_one(
                            store=store,
                            sink=sink,
                            row=row,
                            claim_owner=claim_owner,
                            claim_token=claim_token,
                            retry_backoff_seconds=retry_backoff_seconds,
                            counts=counts,
                        )
                finally:
                    if sink is not None and hasattr(sink, "aclose"):
                        try:
                            await sink.aclose()
                        except Exception as exc:  # noqa: BLE001
                            logger.warning("sink aclose failed: %s", exc)

            for row in claimed:
                finalized_runs.add(row["run_id"])

            # Try to advance pipeline status for each affected run.
            for run_id in finalized_runs:
                try:
                    await finalize_delivery(conn, run_id)
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "finalize_delivery failed for run_id=%s: %s",
                        run_id, exc,
                    )

            await conn.commit()

    return counts


async def _dispatch_one(
    *,
    store: SinkOutboxStore,
    sink,
    row: dict,
    claim_owner: str,
    claim_token: str,
    retry_backoff_seconds: float,
    counts: dict[str, int],
) -> None:
    """Send one outbox row and mark the outcome. Updates ``counts`` in place."""
    from sluice.dag.sinks.base import SinkChunk

    now = datetime.now(timezone.utc)
    try:
        payload = json.loads(row["payload_json"])
    except json.JSONDecodeError as exc:
        rc = await store.mark_failed(
            row_id=row["id"], claim_owner=claim_owner, claim_token=claim_token,
            error_class=type(exc).__name__, error_msg=f"payload_json invalid: {exc}",
        )
        if rc:
            counts["failed"] += 1
        return

    # Reconstruct a SinkChunk matching what sink.chunks() originally produced.
    chunk = SinkChunk(
        ordinal=row["ordinal"],
        payload=payload.get("payload"),
        message_kind=row["message_kind"],
        items_in_chunk=tuple(payload.get("items_in_chunk") or ()),
    )
    params = {
        "sink_id": row["sink_id"],
        "sink_type": row["sink_type"],
        "run_id": row["run_id"],
        "run_key": row["run_key"],
        "ordinal": row["ordinal"],
        "idempotency_key": row["idempotency_key"],
        "message_kind": row["message_kind"],
    }
    try:
        await sink.emit(chunk, params)
    except Exception as exc:  # noqa: BLE001
        msg = str(exc)
        cls = type(exc).__name__
        from datetime import timedelta

        next_at = (now + timedelta(seconds=retry_backoff_seconds)).isoformat()
        rc = await store.mark_retryable(
            row_id=row["id"], claim_owner=claim_owner, claim_token=claim_token,
            next_attempt_at=next_at, error_class=cls, error_msg=msg[:500],
        )
        if rc:
            counts["retryable"] += 1
        return

    sent_at = now.isoformat()
    rc = await store.mark_sent(
        row_id=row["id"], claim_owner=claim_owner, claim_token=claim_token,
        sent_at=sent_at,
    )
    if rc:
        counts["sent"] += 1
