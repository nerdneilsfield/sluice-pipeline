from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

try:
    from croniter import croniter
except ModuleNotFoundError:  # pragma: no cover - exercised only in unsynced envs.
    croniter = None


def _parse_field(field: str, minimum: int, maximum: int) -> set[int]:
    values: set[int] = set()
    for part in field.split(","):
        if "/" in part:
            base, step_text = part.split("/", 1)
            step = int(step_text)
        else:
            base = part
            step = 1

        if base == "*":
            start = minimum
            end = maximum
        elif "-" in base:
            start_text, end_text = base.split("-", 1)
            start = int(start_text)
            end = int(end_text)
        else:
            start = end = int(base)

        values.update(range(start, end + 1, step))
    return {value for value in values if minimum <= value <= maximum}


def _fallback_match(cron: str, value: datetime) -> bool:
    minute, hour, day, month, weekday = cron.split()
    cron_weekday = (value.weekday() + 1) % 7
    weekdays = _parse_field(weekday, 0, 7)
    if 7 in weekdays:
        weekdays.add(0)
    return (
        value.minute in _parse_field(minute, 0, 59)
        and value.hour in _parse_field(hour, 0, 23)
        and value.day in _parse_field(day, 1, 31)
        and value.month in _parse_field(month, 1, 12)
        and cron_weekday in weekdays
    )


def _fallback_previous(cron: str, value: datetime) -> datetime:
    candidate = value.replace(second=0, microsecond=0)
    if candidate >= value:
        candidate -= timedelta(minutes=1)
    for _ in range(366 * 24 * 60):
        if _fallback_match(cron, candidate):
            return candidate
        candidate -= timedelta(minutes=1)
    raise ValueError(f"could not find previous cron occurrence for {cron!r}")


def compute_cycle_anchor(cron: str, tz: str, now: datetime) -> datetime:
    """Return the most recent cron occurrence at or before ``now`` in ``tz``."""
    zone = ZoneInfo(tz)
    local_now = now.astimezone(zone)
    if (
        local_now.second == 0
        and local_now.microsecond == 0
        and (
            croniter.match(cron, local_now)
            if croniter is not None
            else _fallback_match(cron, local_now)
        )
    ):
        return local_now

    if croniter is None:
        return _fallback_previous(cron, local_now)

    previous = croniter(cron, local_now).get_prev(datetime)
    if previous.tzinfo is None:
        return previous.replace(tzinfo=zone)
    return previous


def make_run_key(task: str, pipeline: str, cycle_anchor: datetime) -> str:
    return f"{task}__{pipeline}__{cycle_anchor.isoformat()}"
