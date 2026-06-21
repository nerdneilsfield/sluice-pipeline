from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone

import aiosqlite


class SinkOutboxStore:
    """Durable at-least-once sink outbox.

    Each row represents a sink delivery that must happen. The dispatcher
    (Task 7) claims rows, sends them, and marks them terminal. Pipeline
    state advances to 'success'/'partial'/'failed' only after all required
    rows reach a terminal status.
    """

    def __init__(self, conn: aiosqlite.Connection):
        self.conn = conn

    async def enqueue_many(
        self,
        *,
        run_id: int,
        run_key: str,
        sink_id: str,
        sink_type: str,
        payloads: list[tuple[int, str, str, str]],
    ) -> int:
        """Insert pending outbox rows.

        ``payloads`` is a list of ``(ordinal, idempotency_key, payload_json,
        message_kind)`` tuples. Idempotent: if a row with the same
        ``(run_id, sink_id, ordinal)`` already exists with a matching payload
        hash and idempotency key, it is left alone and not counted. A
        mismatch raises ``RuntimeError`` because that means the caller is
        trying to rewrite history.

        Returns the number of newly-inserted rows.

        Concurrency: uses ``INSERT OR IGNORE`` so concurrent enqueues for the
        same (run_id, sink_id, ordinal) do not raise IntegrityError. After
        each insert we re-SELECT to verify the row's payload hash matches
        and raise if not.
        """
        if not payloads:
            return 0
        now = datetime.now(timezone.utc).isoformat()
        inserted = 0
        for ordinal, idempotency_key, payload_json, message_kind in payloads:
            cur = await self.conn.execute(
                """
                INSERT OR IGNORE INTO sink_outbox (
                  run_id, run_key, sink_id, sink_type, ordinal,
                  idempotency_key, message_kind, payload_json, status,
                  attempts, next_attempt_at, claim_owner, claim_token,
                  claim_until, sent_at, last_error_class, last_error_msg,
                  created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, NULL, NULL,
                          NULL, NULL, NULL, NULL, NULL, ?, ?)
                """,
                (
                    run_id, run_key, sink_id, sink_type, ordinal,
                    idempotency_key, message_kind, payload_json,
                    now, now,
                ),
            )
            if cur.rowcount:
                inserted += 1
            # Verify existing-or-new row matches the caller's payload.
            existing = await (
                await self.conn.execute(
                    "SELECT idempotency_key, payload_json FROM sink_outbox "
                    "WHERE run_id = ? AND sink_id = ? AND ordinal = ?",
                    (run_id, sink_id, ordinal),
                )
            ).fetchone()
            if existing is None:
                continue  # INSERT OR IGNORE somehow left no row; defensive
            existing_idem, existing_payload = existing
            existing_hash = hashlib.sha256(
                (existing_payload or "").encode("utf-8")
            ).hexdigest()
            new_hash = hashlib.sha256(
                payload_json.encode("utf-8")
            ).hexdigest()
            if existing_idem != idempotency_key or existing_hash != new_hash:
                raise RuntimeError(
                    f"outbox row (run_id={run_id}, sink_id={sink_id!r}, "
                    f"ordinal={ordinal}) already exists with a different "
                    "payload or idempotency key; refusing to overwrite"
                )
        return inserted

    async def claim_pending(
        self,
        *,
        claim_owner: str,
        claim_token: str,
        claim_until: str,
        batch_size: int = 50,
        now: datetime | None = None,
    ) -> list[dict]:
        """Atomically claim a batch of pending/retryable rows.

        Updates the claim fields and returns the claimed rows. Claiming
        leaves ``status`` as 'pending' or 'retryable'; only the claim
        fields change. Subsequent calls with a different claim_token will
        not see these rows until the claim expires.
        """
        now = now or datetime.now(timezone.utc)
        now_iso = now.isoformat()
        cur = await self.conn.execute(
            """
            UPDATE sink_outbox
            SET claim_owner = ?, claim_token = ?, claim_until = ?,
                updated_at = ?
            WHERE id IN (
                SELECT id FROM sink_outbox
                WHERE status IN ('pending', 'retryable')
                  AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
                  AND (claim_until IS NULL OR claim_until < ?)
                ORDER BY id
                LIMIT ?
            )
            RETURNING id, run_id, run_key, sink_id, sink_type, ordinal,
                      idempotency_key, message_kind, payload_json, status,
                      attempts
            """,
            (claim_owner, claim_token, claim_until, now_iso,
             now_iso, now_iso, batch_size),
        )
        rows = await cur.fetchall()
        return [
            {
                "id": r[0], "run_id": r[1], "run_key": r[2], "sink_id": r[3],
                "sink_type": r[4], "ordinal": r[5], "idempotency_key": r[6],
                "message_kind": r[7], "payload_json": r[8], "status": r[9],
                "attempts": r[10],
            }
            for r in rows
        ]

    async def mark_sent(
        self,
        *,
        row_id: int,
        claim_owner: str,
        claim_token: str,
        sent_at: str,
        external_id: str | None = None,
    ) -> int:
        cur = await self.conn.execute(
            """
            UPDATE sink_outbox
            SET status = 'sent', sent_at = ?, attempts = attempts + 1,
                last_error_class = NULL, last_error_msg = NULL,
                claim_owner = NULL, claim_token = NULL, claim_until = NULL,
                updated_at = ?
            WHERE id = ? AND claim_owner = ? AND claim_token = ?
            """,
            (sent_at, sent_at, row_id, claim_owner, claim_token),
        )
        return cur.rowcount or 0

    async def mark_retryable(
        self,
        *,
        row_id: int,
        claim_owner: str,
        claim_token: str,
        next_attempt_at: str,
        error_class: str | None,
        error_msg: str | None,
    ) -> int:
        now = datetime.now(timezone.utc).isoformat()
        cur = await self.conn.execute(
            """
            UPDATE sink_outbox
            SET status = 'retryable', attempts = attempts + 1,
                next_attempt_at = ?,
                last_error_class = ?, last_error_msg = ?,
                claim_owner = NULL, claim_token = NULL, claim_until = NULL,
                updated_at = ?
            WHERE id = ? AND claim_owner = ? AND claim_token = ?
            """,
            (next_attempt_at, error_class, error_msg, now,
             row_id, claim_owner, claim_token),
        )
        return cur.rowcount or 0

    async def mark_failed(
        self,
        *,
        row_id: int,
        claim_owner: str,
        claim_token: str,
        error_class: str | None,
        error_msg: str | None,
    ) -> int:
        now = datetime.now(timezone.utc).isoformat()
        cur = await self.conn.execute(
            """
            UPDATE sink_outbox
            SET status = 'failed', attempts = attempts + 1,
                last_error_class = ?, last_error_msg = ?,
                claim_owner = NULL, claim_token = NULL, claim_until = NULL,
                updated_at = ?
            WHERE id = ? AND claim_owner = ? AND claim_token = ?
            """,
            (error_class, error_msg, now,
             row_id, claim_owner, claim_token),
        )
        return cur.rowcount or 0

    async def pending_for_run(self, run_id: int) -> list[dict]:
        cur = await self.conn.execute(
            "SELECT id, run_id, sink_id, ordinal, idempotency_key, status, "
            "payload_json FROM sink_outbox WHERE run_id = ? "
            "ORDER BY sink_id, ordinal",
            (run_id,),
        )
        return [
            {
                "id": r[0], "run_id": r[1], "sink_id": r[2], "ordinal": r[3],
                "idempotency_key": r[4], "status": r[5], "payload_json": r[6],
            }
            for r in await cur.fetchall()
        ]

    async def count_terminal_for_run(self, run_id: int) -> tuple[int, int, int]:
        """Return (sent, retryable, failed) counts for a run."""
        cur = await self.conn.execute(
            "SELECT status, COUNT(*) FROM sink_outbox WHERE run_id = ? "
            "GROUP BY status",
            (run_id,),
        )
        rows = await cur.fetchall()
        counts = dict(rows)
        return (
            int(counts.get("sent", 0)),
            int(counts.get("retryable", 0)),
            int(counts.get("failed", 0)),
        )
