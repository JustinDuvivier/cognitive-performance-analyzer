import datetime

from validators.validate import validate_record, validate_batch


def test_validate_record_external_valid():
    record = {
        "timestamp": datetime.datetime.now(),
        "pressure_hpa": 1013.5,
        "pressure_change_24h": 5.2,
        "temperature": 72.0,
        "humidity": 55.0,
        "hour_of_day": 14,
        "day_of_week": 3,
        "weekend": False,
        "pm25": 12.0,
        "aqi": 50,
    }

    is_valid, errors = validate_record(record, "external_factors")

    assert is_valid is True
    assert errors == []


def test_validate_record_external_invalid():
    record = {
        "timestamp": datetime.datetime.now(),
        "pressure_hpa": 850,
        "temperature": 200,
        "humidity": 150,
        "hour_of_day": 25,
        "day_of_week": 8,
        "weekend": "yes",
        "pm25": 600,
        "aqi": 600,
    }

    is_valid, errors = validate_record(record, "external_factors")

    assert is_valid is False
    # At least pressure_hpa should be flagged
    assert any("pressure_hpa" in e for e in errors)
    assert any("temperature" in e for e in errors)
    assert any("humidity" in e for e in errors)


def test_validate_record_user_valid():
    record = {
        "timestamp": datetime.datetime.now(),
        "sleep_hours": 7.5,
        "phone_usage": 100,
        "steps": 8000,
        "screen_time_minutes": 120,
        "active_energy_kcal": 320.0,
        "calories_intake": 900.0,
        "protein_g": 45.0,
        "carbs_g": 120.0,
        "fat_g": 30.0,
        "sequence_memory_score": 20,
        "reaction_time_ms": 250,
        "verbal_memory_words": 20,
    }

    is_valid, errors = validate_record(record, "user_tracking")

    assert is_valid is True
    assert errors == []


def test_validate_record_user_invalid():
    record = {
        "timestamp": datetime.datetime.now(),
        "sleep_hours": 30,
        "screen_time_minutes": 2000,
        "sequence_memory_score": 150,
        "reaction_time_ms": 50,
    }

    is_valid, errors = validate_record(record, "user_tracking")

    assert is_valid is False
    assert any("sleep_hours" in e for e in errors)
    assert any("screen_time_minutes" in e for e in errors)
    assert any("sequence_memory_score" in e for e in errors)
    assert any("reaction_time_ms" in e for e in errors)


def test_validate_batch_mixed_records():
    good = {
        "timestamp": datetime.datetime.now(),
        "pressure_hpa": 1013,
        "temperature": 70,
        "humidity": 50,
        "hour_of_day": 10,
        "day_of_week": 2,
        "weekend": False,
    }
    bad = {
        "timestamp": datetime.datetime.now(),
        "pressure_hpa": 800,
        "temperature": 200,
        "humidity": 150,
        "hour_of_day": 24,
        "day_of_week": 9,
        "weekend": "no",
    }

    valid, invalid = validate_batch([good, bad, good], "external_factors")

    assert len(valid) == 2
    assert len(invalid) == 1
    assert invalid[0]["table"] == "external_factors"
    assert any("pressure_hpa" in e for e in invalid[0]["errors"])


