from datetime import datetime, timedelta, timezone

from sluice.window import compute_window, parse_duration


def test_parse_duration():
    assert parse_duration("24h") == timedelta(hours=24)
    assert parse_duration("3d") == timedelta(days=3)
    assert parse_duration("90m") == timedelta(minutes=90)
    assert parse_duration("7d") == timedelta(days=7)


def test_compute_window_default_overlap():
    now = datetime(2026, 4, 28, 8, 0, 0, tzinfo=timezone.utc)
    start, end = compute_window(now=now, window="24h", lookback_overlap=None)
    assert end == now
    assert end - start == timedelta(hours=24) + timedelta(hours=24 * 0.2)


def test_compute_window_explicit_overlap():
    now = datetime(2026, 4, 28, 8, 0, 0, tzinfo=timezone.utc)
    start, end = compute_window(now=now, window="24h", lookback_overlap="2h")
    assert end - start == timedelta(hours=26)


def test_min_overlap_one_hour():
    now = datetime(2026, 4, 28, tzinfo=timezone.utc)
    start, end = compute_window(now=now, window="1h", lookback_overlap=None)
    assert end - start == timedelta(hours=1.5)
