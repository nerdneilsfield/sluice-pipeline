from datetime import datetime, timedelta, timezone

import pytest

from sluice.core.errors import ConfigError
from sluice.run_key import render_run_key, validate_template

TZ = timezone(timedelta(hours=8))
DT = datetime(2026, 4, 29, 14, 30, 0, tzinfo=TZ)


def test_render_default_template():
    assert render_run_key("{pipeline_id}/{run_date}", "p1", DT) == "p1/2026-04-29"


def test_render_with_hour():
    assert render_run_key("{pipeline_id}/{run_date}/{run_hour}", "p1", DT) == "p1/2026-04-29/14"


def test_render_with_iso():
    s = render_run_key("{pipeline_id}/{run_iso}", "p1", DT)
    assert s == "p1/2026-04-29T14:30:00+08:00"


def test_render_with_epoch():
    s = render_run_key("{pipeline_id}/{run_epoch}", "p1", DT)
    assert s == "p1/1777444200"


def test_validate_rejects_missing_pipeline_id():
    with pytest.raises(ConfigError):
        validate_template("{run_date}", cron="0 0 * * *", timezone_name="UTC")


def test_validate_hourly_without_hour_collides():
    with pytest.raises(ConfigError):
        validate_template(
            "{pipeline_id}/{run_date}",
            cron="0 * * * *",
            timezone_name="UTC",
            base=DT.astimezone(timezone.utc),
        )


def test_validate_hourly_with_hour_passes():
    validate_template(
        "{pipeline_id}/{run_date}/{run_hour}",
        cron="0 * * * *",
        timezone_name="UTC",
        base=DT.astimezone(timezone.utc),
    )


def test_validate_daily_with_run_date_passes():
    validate_template(
        "{pipeline_id}/{run_date}",
        cron="0 0 * * *",
        timezone_name="UTC",
        base=DT.astimezone(timezone.utc),
    )
