import asyncio
from datetime import datetime, timezone

import aiosqlite
from loguru import logger

_BACKOFF_MS = (100, 200, 400, 800, 1600)


class DeliveryLog:
    def __init__(self, db_path: str):
        self._path = db_path

    async def record(
        self,
        *,
        pipeline_id,
        run_key,
        sink_id,
        sink_type,
        ordinal,
        message_kind,
        recipient,
        external_id,
        status,
        error_class,
        error_msg,
    ) -> None:
        sql = (
            "INSERT INTO sink_delivery_log "
            "(pipeline_id, run_key, sink_id, sink_type, attempt_at, "
            " ordinal, message_kind, recipient, external_id, "
            " status, error_class, error_msg) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        params = (
            pipeline_id,
            run_key,
            sink_id,
            sink_type,
            datetime.now(timezone.utc).isoformat(timespec="seconds"),
            ordinal,
            message_kind,
            recipient,
            external_id,
            status,
            error_class,
            error_msg,
        )
        last_exc: Exception | None = None
        for delay_ms in (0, *_BACKOFF_MS):
            if delay_ms:
                await asyncio.sleep(delay_ms / 1000)
            try:
                async with aiosqlite.connect(self._path) as db:
                    await db.execute("PRAGMA busy_timeout = 5000")
                    await db.execute(sql, params)
                    await db.commit()
                    return
            except aiosqlite.OperationalError as exc:
                is_locked = getattr(exc, "sqlite_errorcode", None) in (5, 6) or any(
                    kw in str(exc).lower() for kw in ("locked", "busy")
                )
                if not is_locked:
                    raise
                last_exc = exc
        logger.error(
            "delivery_log raise after backoff exhaustion: "
            f"sink={sink_id} run={run_key} status={status}: {last_exc}"
        )
        raise RuntimeError(
            f"delivery_log: failed to persist audit row after "
            f"{len(_BACKOFF_MS) + 1} attempts (~3.1s) — DB locked. "
            f"Last error: {last_exc}"
        ) from last_exc
