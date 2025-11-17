from datetime import datetime

import pytest

import readers.api_reader as api_reader


def test_calculate_aqi_from_pm25_none():
    assert api_reader.calculate_aqi_from_pm25(None) is None


def test_calculate_aqi_from_pm25_ranges():
    # In the lowest range (0-12)
    value = 10.0
    expected = int((50 / 12.0) * value)
    assert api_reader.calculate_aqi_from_pm25(value) == expected

    # In a higher range (>150.4)
    assert api_reader.calculate_aqi_from_pm25(200.0) == 250


def test_calculate_pressure_change_no_history(monkeypatch):
    def fake_get_pressure_24h_ago(_timestamp):
        return None

    monkeypatch.setattr(api_reader, "get_pressure_24h_ago", fake_get_pressure_24h_ago)

    result = api_reader.calculate_pressure_change(1013.5, datetime.now())
    assert result == 0


def test_calculate_pressure_change_with_history(monkeypatch):
    def fake_get_pressure_24h_ago(_timestamp):
        return 1000.0

    monkeypatch.setattr(api_reader, "get_pressure_24h_ago", fake_get_pressure_24h_ago)

    result = api_reader.calculate_pressure_change(1013.5, datetime.now())
    assert pytest.approx(result, rel=1e-3) == 13.5



