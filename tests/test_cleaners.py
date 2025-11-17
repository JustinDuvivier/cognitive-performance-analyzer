from datetime import datetime

import pytest
import pandas as pd

from cleaners.clean import (
    safe_float,
    safe_int,
    safe_bool,
    clean_external_factors,
    clean_user_tracking,
    merge_and_clean,
    prepare_for_insert,
)


@pytest.mark.parametrize(
    "value,default,expected",
    [
        ("3.14", None, 3.14),
        ("", None, None),
        (None, None, None),
        ("abc", 1.0, 1.0),
    ],
)
def test_safe_float_basic(value, default, expected):
    assert safe_float(value, default) == expected


@pytest.mark.parametrize(
    "value,default,expected",
    [
        ("5", 0, 5),
        ("4.8", 0, 4),  # via float
        ("", 7, 7),
        ("abc", 2, 2),
    ],
)
def test_safe_int_basic(value, default, expected):
    assert safe_int(value, default) == expected


@pytest.mark.parametrize(
    "value,default,expected",
    [
        (True, False, True),
        (False, True, False),
        ("Y", False, True),
        ("YES", False, True),
        ("n", True, False),
        (None, False, False),
    ],
)
def test_safe_bool_basic(value, default, expected):
    assert safe_bool(value, default) is expected


def test_clean_external_factors_types_and_defaults():
    raw = {
        "timestamp": "2025-11-17 14:30:00",
        "pressure_hpa": "1013.5",
        "pressure_change_24h": "",
        "temperature": "72.5",
        "humidity": 65,
        "pm25": "",
        "aqi": "52.7",
    }

    cleaned = clean_external_factors(raw)

    assert isinstance(cleaned["timestamp"], datetime)
    assert cleaned["pressure_hpa"] == 1013.5
    assert cleaned["pressure_change_24h"] == 0  # default when not provided
    assert cleaned["temperature"] == 72.5
    assert cleaned["humidity"] == 65.0
    assert 0 <= cleaned["hour_of_day"] <= 23
    assert 1 <= cleaned["day_of_week"] <= 7
    assert isinstance(cleaned["weekend"], bool)
    assert cleaned["pm25"] is None
    assert cleaned["aqi"] == 52  # cast to int


def test_clean_user_tracking_types_and_defaults():
    raw = {
        "timestamp": datetime.now(),
        "sleep_hours": "7.5",
        "breakfast_skipped": "Y",
        "lunch_skipped": "N",
        "phone_usage": "45",
        "caffeine_count": "2",
        "steps": "5000",
        "water_glasses": "6",
        "exercise": "n",
        "brain_fog_score": 4.8,
        "reaction_time_ms": "245",
        "verbal_memory_words": "12",
    }

    cleaned = clean_user_tracking(raw)

    assert isinstance(cleaned["timestamp"], datetime)
    assert cleaned["sleep_hours"] == 7.5
    assert cleaned["breakfast_skipped"] is True
    assert cleaned["lunch_skipped"] is False
    assert cleaned["phone_usage"] == 45
    assert cleaned["caffeine_count"] == 2
    assert cleaned["steps"] == 5000
    assert cleaned["water_glasses"] == 6
    assert cleaned["exercise"] is False
    assert cleaned["brain_fog_score"] == 4
    assert cleaned["reaction_time_ms"] == 245.0
    assert cleaned["verbal_memory_words"] == 12


def test_merge_and_clean_returns_lists():
    external_raw = [
        {"timestamp": datetime.now(), "pressure_hpa": 1013.5},
        {"timestamp": datetime.now(), "pressure_hpa": 1000.0},
    ]
    user_raw = [
        {"timestamp": datetime.now(), "sleep_hours": 7.0},
        {"timestamp": datetime.now(), "sleep_hours": 6.5},
    ]

    external_cleaned, user_cleaned = merge_and_clean(external_raw, user_raw)

    assert isinstance(external_cleaned, list)
    assert isinstance(user_cleaned, list)
    assert len(external_cleaned) == len(external_raw)
    assert len(user_cleaned) == len(user_raw)


def test_prepare_for_insert_handles_nan_and_timestamp():
    ts = pd.Timestamp("2025-11-17 14:30:00")
    record = {"timestamp": ts, "value": float("nan"), "other": 42}

    prepared = prepare_for_insert(record)

    assert isinstance(prepared["timestamp"], datetime)
    assert prepared["value"] is None
    assert prepared["other"] == 42


