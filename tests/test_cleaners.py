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
        "phone_usage": "45",
        "steps": "5000",
        "screen_time_minutes": "120",
        "active_energy_kcal": "320.5",
        "calories_intake": "900.2",
        "protein_g": "45.7",
        "carbs_g": "120.3",
        "fat_g": "30.9",
        "sequence_memory_score": 7.8,
        "reaction_time_ms": "245",
        "verbal_memory_words": "12",
    }

    cleaned = clean_user_tracking(raw)

    assert isinstance(cleaned["timestamp"], datetime)
    assert cleaned["sleep_hours"] == 7.5
    assert cleaned["phone_usage"] == 45
    assert cleaned["steps"] == 5000
    assert cleaned["screen_time_minutes"] == 120
    assert cleaned["active_energy_kcal"] == 320.5
    assert cleaned["calories_intake"] == 900.2
    assert cleaned["protein_g"] == 45.7
    assert cleaned["carbs_g"] == 120.3
    assert cleaned["fat_g"] == 30.9
    assert cleaned["sequence_memory_score"] == 7
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


