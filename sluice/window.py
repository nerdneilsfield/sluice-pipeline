import re
from datetime import datetime, timedelta

_DUR_RE = re.compile(r"^(\d+)(ms|s|m|h|d)$")
_UNITS = {"ms": "milliseconds", "s": "seconds", "m": "minutes", "h": "hours", "d": "days"}


def parse_duration(s: str) -> timedelta:
    m = _DUR_RE.match(s.strip())
    if not m:
        raise ValueError(f"bad duration: {s}")
    return timedelta(**{_UNITS[m.group(2)]: int(m.group(1))})


def default_lookback_overlap(window_seconds: float) -> float:
    return max(min(3600.0, window_seconds * 0.5), window_seconds * 0.2)


def compute_window(
    *, now: datetime, window: str, lookback_overlap: str | None
) -> tuple[datetime, datetime]:
    w = parse_duration(window)
    if lookback_overlap is None:
        ws = w.total_seconds()
        overlap = timedelta(seconds=default_lookback_overlap(ws))
    else:
        overlap = parse_duration(lookback_overlap)
    return now - (w + overlap), now
