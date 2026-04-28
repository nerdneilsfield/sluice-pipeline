from datetime import datetime, timedelta
import re

_DUR_RE = re.compile(r"^(\d+)([smhd])$")
_UNITS = {"s": "seconds", "m": "minutes", "h": "hours", "d": "days"}


def parse_duration(s: str) -> timedelta:
    m = _DUR_RE.match(s.strip())
    if not m:
        raise ValueError(f"bad duration: {s}")
    return timedelta(**{_UNITS[m.group(2)]: int(m.group(1))})


def compute_window(
    *, now: datetime, window: str, lookback_overlap: str | None
) -> tuple[datetime, datetime]:
    w = parse_duration(window)
    if lookback_overlap is None:
        overlap = max(timedelta(hours=1), w * 0.2)
    else:
        overlap = parse_duration(lookback_overlap)
    return now - (w + overlap), now
