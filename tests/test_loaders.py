from datetime import datetime
from unittest.mock import MagicMock

import pytest

import loaders.load as load_module


class TestClearPersonCache:
    def test_clears_cache(self):
        load_module._person_cache["Test"] = 123
        load_module.clear_person_cache()
        assert load_module._person_cache == {}


class TestGetPersonId:
    @pytest.fixture(autouse=True)
    def cleanup(self):
        yield
        load_module.clear_person_cache()

    def test_returns_none_for_invalid_input(self):
        assert load_module.get_person_id("") is None
        assert load_module.get_person_id(None) is None

    def test_returns_cached_value(self):
        load_module._person_cache["Cached"] = 999
        assert load_module.get_person_id("Cached") == 999

    def test_db_error_returns_none(self, monkeypatch):
        mock_conn = MagicMock()
        mock_conn.cursor.side_effect = Exception("boom")
        monkeypatch.setattr(load_module, "get_db_connection", lambda: mock_conn)
        assert load_module.get_person_id("Alice") is None


class TestGetAllPersons:
    def test_no_connection_returns_empty(self, monkeypatch):
        monkeypatch.setattr(load_module, "get_db_connection", lambda: None)
        assert load_module.get_all_persons() == []

    def test_returns_persons(self, monkeypatch):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [(1, "Alice", "NYC", 40.7, -74.0)]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        monkeypatch.setattr(load_module, "get_db_connection", lambda: mock_conn)

        result = load_module.get_all_persons()
        assert len(result) == 1
        assert result[0]["name"] == "Alice"


class TestGetPressure24hAgo:
    def test_no_connection_returns_none(self, monkeypatch):
        monkeypatch.setattr(load_module, "get_db_connection", lambda: None)
        assert load_module.get_pressure_24h_ago(datetime.now()) is None

    def test_returns_pressure(self, monkeypatch):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1013.5,)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        monkeypatch.setattr(load_module, "get_db_connection", lambda: mock_conn)

        result = load_module.get_pressure_24h_ago(datetime.now())
        assert result == pytest.approx(1013.5)

    def test_returns_pressure_for_person(self, monkeypatch):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1000.0,)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        monkeypatch.setattr(load_module, "get_db_connection", lambda: mock_conn)

        result = load_module.get_pressure_24h_ago(datetime.now(), person_id=1)
        assert result == pytest.approx(1000.0)


class TestUpsertMeasurementExternal:
    def test_empty_records(self):
        inserted, rejected = load_module.upsert_measurement_external([])
        assert inserted == 0 and rejected == []

    def test_no_connection(self, monkeypatch):
        monkeypatch.setattr(load_module, "get_db_connection", lambda: None)
        inserted, rejected = load_module.upsert_measurement_external([{"data": "test"}])
        assert inserted == 0

    def test_inserts_and_resolves_person(self, monkeypatch):
        ts = datetime.now()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [(1, "Alice")]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        monkeypatch.setattr(load_module, "get_db_connection", lambda: mock_conn)
        monkeypatch.setattr(load_module, "execute_values", lambda *args, **kwargs: None)

        inserted, rejected = load_module.upsert_measurement_external([{"person": "Alice", "timestamp": ts}])

        assert inserted == 1
        assert rejected == []


class TestUpdateMeasurementUserData:
    def test_empty_records(self):
        updated, rejected = load_module.update_measurement_user_data([])
        assert updated == 0 and rejected == []

    def test_no_connection(self, monkeypatch):
        monkeypatch.setattr(load_module, "get_db_connection", lambda: None)
        updated, rejected = load_module.update_measurement_user_data([{"data": "test"}])
        assert updated == 0

    def test_rejects_when_missing_external_measurement(self, monkeypatch):
        ts = datetime.now()
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        monkeypatch.setattr(load_module, "get_db_connection", lambda: mock_conn)
        monkeypatch.setattr(load_module, "_get_existing_measurements", lambda cur, recs: set())

        updated, rejected = load_module.update_measurement_user_data([{"person_id": 1, "timestamp": ts, "person": "Alice"}])

        assert updated == 0
        assert rejected and "No measurement row exists" in rejected[0]["error"]

    def test_updates_when_existing_measurement(self, monkeypatch):
        ts = datetime.now()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 1
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        monkeypatch.setattr(load_module, "get_db_connection", lambda: mock_conn)
        monkeypatch.setattr(load_module, "_get_existing_measurements", lambda cur, recs: {(1, ts)})
        monkeypatch.setattr(load_module, "execute_values", lambda *args, **kwargs: None)

        updated, rejected = load_module.update_measurement_user_data([{"person_id": 1, "timestamp": ts, "person": "Alice"}])

        assert updated == 1
        assert rejected == []


class TestLogRejectedRecords:
    def test_empty_records(self):
        assert load_module.log_rejected_records([]) == 0

    def test_no_connection(self, monkeypatch):
        monkeypatch.setattr(load_module, "get_db_connection", lambda: None)
        assert load_module.log_rejected_records([{"error": "test"}]) == 0

    def test_logs_records(self, monkeypatch):
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        monkeypatch.setattr(load_module, "get_db_connection", lambda: mock_conn)
        monkeypatch.setattr(load_module, "execute_values", lambda *args, **kwargs: None)

        count = load_module.log_rejected_records([{"table": "t", "record": {"a": 1}, "error": "bad"}])

        assert count == 1


class TestCheckTableCounts:
    def test_no_connection_returns_empty(self, monkeypatch):
        monkeypatch.setattr(load_module, "get_db_connection", lambda: None)
        assert load_module.check_table_counts() == {}

    def test_returns_counts(self, monkeypatch):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (10, 100, 5, 50)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        monkeypatch.setattr(load_module, "get_db_connection", lambda: mock_conn)

        result = load_module.check_table_counts()
        assert result["persons"] == 10


class TestGetDbConnection:
    def test_success(self, monkeypatch):
        mock_conn = MagicMock()
        monkeypatch.setattr("psycopg2.connect", lambda **kwargs: mock_conn)
        assert load_module.get_db_connection() == mock_conn

    def test_failure_returns_none(self, monkeypatch):
        monkeypatch.setattr("psycopg2.connect", lambda **kwargs: (_ for _ in ()).throw(Exception()))
        assert load_module.get_db_connection() is None


class TestHelperFunctions:
    @pytest.fixture(autouse=True)
    def cleanup(self):
        yield
        load_module.clear_person_cache()

    def test_get_person_id_with_cursor(self):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (123,)
        result = load_module._get_person_id_with_cursor(mock_cursor, "User")
        assert result == 123

    def test_load_all_persons_to_cache(self):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [(1, "Alice"), (2, "Bob")]
        load_module._load_all_persons_to_cache(mock_cursor)
        assert load_module._person_cache["Alice"] == 1

    def test_get_existing_measurements(self):
        mock_cursor = MagicMock()
        ts = datetime.now()
        mock_cursor.fetchall.return_value = [(1, ts)]
        result = load_module._get_existing_measurements(mock_cursor, [{"person_id": 1, "timestamp": ts}])
        assert (1, ts) in result
