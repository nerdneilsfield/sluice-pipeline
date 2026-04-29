from datetime import timedelta

import pytest

from sluice.window import parse_duration


def test_parse_500ms():
    assert parse_duration("500ms") == timedelta(milliseconds=500)


def test_parse_1ms():
    assert parse_duration("1ms") == timedelta(milliseconds=1)


def test_existing_units_still_work():
    assert parse_duration("1s") == timedelta(seconds=1)
    assert parse_duration("2h") == timedelta(hours=2)


def test_bad_unit_still_rejected():
    with pytest.raises(ValueError):
        parse_duration("5xx")
