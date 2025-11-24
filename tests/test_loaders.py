from datetime import datetime
import loaders.load as load_module


def test_upsert_external_factors_no_connection(monkeypatch):
    monkeypatch.setattr(load_module, "get_db_connection", lambda: None)

    records = [
        {
            "timestamp": datetime.now(),
            "pressure_hpa": 1013.5,
            "pressure_change_24h": 0,
            "temperature": 72.0,
            "humidity": 60.0,
            "hour_of_day": 10,
            "day_of_week": 2,
            "weekend": False,
            "pm25": 12.0,
            "aqi": 50,
        }
    ]

    inserted, rejected = load_module.upsert_external_factors(records)
    assert inserted == 0
    assert rejected == records


def test_insert_user_tracking_no_connection(monkeypatch):
    monkeypatch.setattr(load_module, "get_db_connection", lambda: None)

    records = [
        {
            "timestamp": datetime.now(),
            "sleep_hours": 7.5,
            "phone_usage": 45,
            "steps": 5000,
            "screen_time_minutes": 120,
            "active_energy_kcal": 320.0,
            "calories_intake": 900.0,
            "protein_g": 45.0,
            "carbs_g": 120.0,
            "fat_g": 30.0,
            "sequence_memory_score": 20,
            "reaction_time_ms": 245.5,
            "verbal_memory_words": 12,
        }
    ]

    inserted, rejected = load_module.insert_user_tracking(records)
    assert inserted == 0
    assert rejected == records


def test_log_rejected_records_no_connection(monkeypatch):
    monkeypatch.setattr(load_module, "get_db_connection", lambda: None)

    rejected_records = [{"table": "external_factors", "record": {"foo": "bar"}, "error": "bad data"}]

    logged = load_module.log_rejected_records(rejected_records)
    assert logged == 0


def test_check_table_counts_no_connection(monkeypatch):
    monkeypatch.setattr(load_module, "get_db_connection", lambda: None)

    counts = load_module.check_table_counts()
    assert counts == {}

