from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from sluice.dag.cycle import compute_cycle_anchor, make_run_key


def test_compute_anchor_daily_8am_utc():
    now = datetime(2026, 5, 3, 14, 0, tzinfo=timezone.utc)
    anchor = compute_cycle_anchor("0 8 * * *", "UTC", now)
    assert anchor == datetime(2026, 5, 3, 8, 0, tzinfo=timezone.utc)


def test_compute_anchor_before_today_anchor():
    now = datetime(2026, 5, 3, 7, 0, tzinfo=timezone.utc)
    anchor = compute_cycle_anchor("0 8 * * *", "UTC", now)
    assert anchor == datetime(2026, 5, 2, 8, 0, tzinfo=timezone.utc)


def test_compute_anchor_with_tz():
    now = datetime(2026, 5, 3, 0, 0, tzinfo=timezone.utc)
    anchor = compute_cycle_anchor("0 8 * * *", "Asia/Shanghai", now)
    assert anchor == datetime(2026, 5, 3, 8, 0, tzinfo=ZoneInfo("Asia/Shanghai"))


def test_compute_anchor_at_exact_anchor_returns_now():
    now = datetime(2026, 5, 3, 8, 0, tzinfo=timezone.utc)
    anchor = compute_cycle_anchor("0 8 * * *", "UTC", now)
    assert anchor == datetime(2026, 5, 3, 8, 0, tzinfo=timezone.utc)


def test_compute_anchor_one_second_after_anchor():
    now = datetime(2026, 5, 3, 8, 0, 1, tzinfo=timezone.utc)
    anchor = compute_cycle_anchor("0 8 * * *", "UTC", now)
    assert anchor == datetime(2026, 5, 3, 8, 0, tzinfo=timezone.utc)


def test_compute_anchor_exact_match_requires_zero_microsecond():
    now = datetime(2026, 5, 3, 8, 0, 0, 1, tzinfo=timezone.utc)
    anchor = compute_cycle_anchor("0 8 * * *", "UTC", now)
    assert anchor == datetime(2026, 5, 3, 8, 0, tzinfo=timezone.utc)


def test_make_run_key_format():
    anchor = datetime(2026, 5, 3, 8, 0, tzinfo=timezone.utc)
    rk = make_run_key("airflow", "report_telegram", anchor)
    assert rk == "airflow__report_telegram__2026-05-03T08:00:00+00:00"
