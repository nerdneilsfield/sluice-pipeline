from sluice.window import default_lookback_overlap


def test_hourly_lookback_is_30min():
    assert default_lookback_overlap(3600) == 1800.0


def test_daily_lookback_is_4_8h():
    assert default_lookback_overlap(86400) == 86400 * 0.2


def test_weekly_lookback_is_20pct():
    assert default_lookback_overlap(7 * 86400) == 7 * 86400 * 0.2
