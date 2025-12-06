from datetime import datetime

import pytest
import pandas as pd

from cleaners.clean import (
    round_to_nearest_hour,
    clean_timestamp,
    safe_float,
    safe_int,
    safe_bool,
    clean_measurement_external,
    clean_measurement_user,
    prepare_for_insert,
)


class TestRoundToNearestHour:
    def test_rounds_correctly(self):
        assert round_to_nearest_hour(datetime(2025, 1, 15, 14, 20)).hour == 14
        assert round_to_nearest_hour(datetime(2025, 1, 15, 14, 45)).hour == 15


class TestCleanTimestamp:
    def test_handles_various_types(self):
        assert isinstance(clean_timestamp("2025-01-15 14:30:00"), datetime)
        assert isinstance(clean_timestamp(pd.Timestamp("2025-01-15 14:30:00")), datetime)
        assert isinstance(clean_timestamp(datetime(2025, 1, 15, 14, 30)), datetime)
        assert isinstance(clean_timestamp(None), datetime)


class TestSafeFloat:
    @pytest.mark.parametrize("value,expected", [
        ("3.14", 3.14),
        ("", None),
        (None, None),
        ("abc", None),
    ])
    def test_conversion(self, value, expected):
        result = safe_float(value, None)
        assert result == pytest.approx(expected) if expected else result is None


class TestSafeInt:
    @pytest.mark.parametrize("value,expected", [
        ("5", 5),
        ("4.8", 4),
        ("abc", 0),
    ])
    def test_conversion(self, value, expected):
        assert safe_int(value, 0) == expected


class TestSafeBool:
    @pytest.mark.parametrize("value,expected", [
        (True, True),
        ("YES", True),
        ("n", False),
        (None, False),
    ])
    def test_conversion(self, value, expected):
        assert safe_bool(value, False) is expected


class TestCleanMeasurementExternal:
    @pytest.fixture
    def raw_record(self):
        return {"timestamp": "2025-11-17 14:30:00", "pressure_hpa": "1013.5", "humidity": 65}

    def test_cleans_record(self, raw_record):
        cleaned = clean_measurement_external(raw_record)
        assert isinstance(cleaned["timestamp"], datetime)
        assert cleaned["pressure_hpa"] == pytest.approx(1013.5)
        assert "hour_of_day" in cleaned
        assert "weekend" in cleaned


class TestCleanMeasurementUser:
    @pytest.fixture
    def raw_record(self):
        return {"timestamp": datetime.now(), "sleep_hours": "7.5", "steps": "5000"}

    def test_cleans_record(self, raw_record):
        cleaned = clean_measurement_user(raw_record)
        assert isinstance(cleaned["timestamp"], datetime)
        assert cleaned["sleep_hours"] == pytest.approx(7.5)
        assert cleaned["steps"] == 5000


class TestPrepareForInsert:
    def test_handles_special_values(self):
        record = {"timestamp": pd.Timestamp("2025-11-17"), "value": float("nan"), "other": 42}
        prepared = prepare_for_insert(record)
        assert isinstance(prepared["timestamp"], datetime)
        assert prepared["value"] is None
        assert prepared["other"] == 42
