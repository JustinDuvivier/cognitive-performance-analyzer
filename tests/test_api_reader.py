from datetime import datetime
from unittest.mock import MagicMock

import pytest

import readers.api_reader as api_reader


class TestGetLocationKey:
    def test_formats_coordinates(self):
        assert api_reader._get_location_key(40.7128, -74.0060) == "40.7128,-74.006"
        assert api_reader._get_location_key(40.71289999, -74.00601234) == "40.7129,-74.006"


class TestCalculateAqiFromPm25:
    def test_returns_none_for_none(self):
        assert api_reader.calculate_aqi_from_pm25(None) is None

    def test_calculates_aqi(self):
        assert api_reader.calculate_aqi_from_pm25(10.0) == pytest.approx(41, abs=1)
        assert api_reader.calculate_aqi_from_pm25(200.0) == 250


class TestCalculatePressureChange:
    def test_returns_zero_for_none_pressure(self):
        assert api_reader.calculate_pressure_change(None, datetime.now()) == 0

    def test_calculates_change(self, monkeypatch):
        monkeypatch.setattr(api_reader, "get_pressure_24h_ago", lambda t, p=None: 1000.0)
        result = api_reader.calculate_pressure_change(1013.5, datetime.now())
        assert result == pytest.approx(13.5)


class TestFetchWeatherDataForLocation:
    def test_no_api_key_returns_none(self, monkeypatch):
        monkeypatch.setattr(api_reader, "OPENWEATHER_API_KEY", None)
        assert api_reader.fetch_weather_data_for_location(40.7, -74.0) is None

    def test_successful_fetch(self, monkeypatch):
        mock_response = MagicMock()
        mock_response.json.return_value = {"main": {"pressure": 1013, "temp": 72.5, "humidity": 60}}
        monkeypatch.setattr(api_reader, "OPENWEATHER_API_KEY", "key")
        monkeypatch.setattr("requests.get", lambda *a, **k: mock_response)

        result = api_reader.fetch_weather_data_for_location(40.7, -74.0)
        assert result["pressure_hpa"] == 1013


class TestFetchAirQualityForLocation:
    def test_no_api_key_returns_defaults(self, monkeypatch):
        monkeypatch.setattr(api_reader, "OPENAQ_API_KEY", None)
        result = api_reader.fetch_air_quality_for_location(40.7, -74.0)
        assert result == {"pm25": None, "aqi": None}

    def test_successful_fetch(self, monkeypatch):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"results": [{"value": 25.5}]}
        monkeypatch.setattr(api_reader, "OPENAQ_API_KEY", "key")
        monkeypatch.setattr("requests.get", lambda *a, **k: mock_response)

        result = api_reader.fetch_air_quality_for_location(40.7, -74.0)
        assert result["pm25"] == pytest.approx(25.5)


class TestFetchAdditionalPollutantsForLocation:
    def test_no_api_key_returns_defaults(self, monkeypatch):
        monkeypatch.setattr(api_reader, "OPENWEATHER_API_KEY", None)
        result = api_reader.fetch_additional_pollutants_for_location(40.7, -74.0)
        assert result["co"] is None

    def test_successful_fetch(self, monkeypatch):
        mock_response = MagicMock()
        mock_response.json.return_value = {"list": [{"components": {"co": 100, "no2": 50}}]}
        monkeypatch.setattr(api_reader, "OPENWEATHER_API_KEY", "key")
        monkeypatch.setattr("requests.get", lambda *a, **k: mock_response)

        result = api_reader.fetch_additional_pollutants_for_location(40.7, -74.0)
        assert result["co"] == 100


class TestFetchLocationData:
    @pytest.fixture(autouse=True)
    def cleanup(self):
        yield
        api_reader._location_cache.clear()

    def test_uses_cache(self):
        api_reader._location_cache["40.7,-74.0"] = {"cached": True}
        result = api_reader._fetch_location_data(40.7, -74.0)
        assert result["cached"] is True

    def test_returns_none_if_weather_fails(self, monkeypatch):
        monkeypatch.setattr(api_reader, "fetch_weather_data_for_location", lambda a, b: None)
        assert api_reader._fetch_location_data(40.7, -74.0) is None


class TestFetchAllExternalData:
    @pytest.fixture(autouse=True)
    def cleanup(self):
        yield
        api_reader._location_cache.clear()

    def test_no_persons_returns_empty(self, monkeypatch):
        monkeypatch.setattr(api_reader, "get_all_persons", lambda: [])
        assert api_reader.fetch_all_external_data() == []

    def test_fetches_for_persons(self, monkeypatch):
        monkeypatch.setattr(api_reader, "get_all_persons", lambda: [
            {"person_id": 1, "name": "Alice", "latitude": 40.7, "longitude": -74.0, "location_name": "NYC"}
        ])
        monkeypatch.setattr(api_reader, "_fetch_location_data", lambda a, b: {"pressure_hpa": 1013})
        monkeypatch.setattr(api_reader, "calculate_pressure_change", lambda p, t, pid: 0)

        result = api_reader.fetch_all_external_data()
        assert len(result) == 1
        assert result[0]["person"] == "Alice"
