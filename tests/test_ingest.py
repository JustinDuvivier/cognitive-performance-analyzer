import pytest

import ingest
from ingest import (
    _format_invalid_for_rejects,
    _validate_clean_and_load,
    _run_measurement_external_flow,
    _run_measurement_user_flow,
)


class TestFormatInvalidForRejects:
    def test_formats_complete_invalid(self):
        invalid = {"table": "measurements", "record": {"foo": "bar"}, "errors": ["error1", "error2"]}
        result = _format_invalid_for_rejects(invalid)
        assert result["table"] == "measurements"
        assert result["error"] == "error1; error2"

    def test_handles_missing_fields(self):
        result = _format_invalid_for_rejects({})
        assert result["table"] == "unknown"
        assert result["error"] == "Unknown error"


class TestValidateCleanAndLoad:
    def test_tracks_validation_results(self, monkeypatch):
        monkeypatch.setattr(ingest, "validate_batch", lambda r, n: (r, []))
        stats = {"name": "test", "read": 2, "validated": 0, "rejected": 0, "loaded": 0, "db_rejected": 0}

        result_stats, _ = _validate_clean_and_load([{"a": 1}, {"b": 2}], "test", stats, [], lambda x: x, lambda x: (len(x), []))

        assert result_stats["validated"] == 2
        assert result_stats["loaded"] == 2

    def test_returns_when_all_invalid(self, monkeypatch):
        invalid = [{"table": "t", "record": {}, "errors": ["bad"]}]
        monkeypatch.setattr(ingest, "validate_batch", lambda r, n: ([], invalid))
        stats = {"name": "test", "read": 1, "validated": 0, "rejected": 0, "loaded": 0, "db_rejected": 0}

        result_stats, rejected = _validate_clean_and_load([], "test", stats, [], lambda x: x, lambda x: (0, []))

        assert result_stats["validated"] == 0
        assert result_stats["rejected"] == 1
        assert rejected[0]["error"] == "bad"

    def test_db_rejected_added(self, monkeypatch):
        monkeypatch.setattr(ingest, "validate_batch", lambda r, n: (r, []))
        stats = {"name": "test", "read": 1, "validated": 0, "rejected": 0, "loaded": 0, "db_rejected": 0}

        result_stats, rejected = _validate_clean_and_load([{"x": 1}], "test", stats, [], lambda x: x, lambda x: (0, [{"error": "db"}]))

        assert result_stats["db_rejected"] == 1
        assert rejected[0]["error"] == "db"


class TestRunMeasurementExternalFlow:
    def test_no_data_returns_empty(self, monkeypatch):
        monkeypatch.setattr(ingest, "fetch_all_external_data", lambda: [])
        stats, rejected = _run_measurement_external_flow()
        assert stats["read"] == 0
        assert rejected == []

    def test_with_data_runs_validation(self, monkeypatch):
        monkeypatch.setattr(ingest, "fetch_all_external_data", lambda: [{"a": 1}])
        called = {}

        def fake_validate(records, name, stats, rejected, cleaner, loader):
            called["ran"] = True
            stats["validated"] = len(records)
            stats["loaded"] = len(records)
            return stats, rejected

        monkeypatch.setattr(ingest, "_validate_clean_and_load", fake_validate)
        stats, rejected = _run_measurement_external_flow()

        assert called.get("ran") is True
        assert stats["validated"] == 1
        assert rejected == []


class TestRunMeasurementUserFlow:
    def test_no_data_returns_empty(self, monkeypatch):
        monkeypatch.setattr(ingest, "read_all_user_data", lambda: [])
        stats, rejected = _run_measurement_user_flow()
        assert stats["read"] == 0
        assert rejected == []

    def test_with_data_runs_validation(self, monkeypatch):
        monkeypatch.setattr(ingest, "read_all_user_data", lambda: [{"a": 1}, {"b": 2}])
        called = {}

        def fake_validate(records, name, stats, rejected, cleaner, loader):
            called["ran"] = True
            stats["validated"] = len(records)
            stats["loaded"] = len(records)
            return stats, rejected

        monkeypatch.setattr(ingest, "_validate_clean_and_load", fake_validate)
        stats, rejected = _run_measurement_user_flow()

        assert called.get("ran") is True
        assert stats["validated"] == 2
        assert rejected == []


class TestRunPipeline:
    @pytest.fixture
    def mock_pipeline(self, monkeypatch):
        monkeypatch.setattr(ingest, "_run_measurement_external_flow", lambda: (
            {"name": "measurements_external", "read": 1, "validated": 1, "rejected": 0, "loaded": 1, "db_rejected": 0}, []
        ))
        monkeypatch.setattr(ingest, "_run_measurement_user_flow", lambda: (
            {"name": "measurements_user", "read": 2, "validated": 2, "rejected": 0, "loaded": 2, "db_rejected": 0}, []
        ))
        monkeypatch.setattr(ingest, "log_rejected_records", lambda x: len(x))
        monkeypatch.setattr(ingest, "check_table_counts", lambda: {})

    def test_successful_run(self, mock_pipeline):
        result = ingest.run_pipeline()
        assert result["success"] is True
        assert result["total_read"] == 3
        assert result["total_loaded"] == 3

    def test_returns_source_stats(self, mock_pipeline):
        result = ingest.run_pipeline()
        names = {s["name"] for s in result["source_stats"]}
        assert names == {"measurements_external", "measurements_user"}

    def test_logs_rejections_and_counts(self, monkeypatch):
        monkeypatch.setattr(ingest, "_run_measurement_external_flow", lambda: (
            {"name": "measurements_external", "read": 1, "validated": 1, "rejected": 1, "loaded": 0, "db_rejected": 0},
            [{"error": "bad"}],
        ))
        monkeypatch.setattr(ingest, "_run_measurement_user_flow", lambda: (
            {"name": "measurements_user", "read": 0, "validated": 0, "rejected": 0, "loaded": 0, "db_rejected": 1},
            [{"error": "db"}],
        ))
        logged = {}
        monkeypatch.setattr(ingest, "log_rejected_records", lambda records: logged.setdefault("count", len(records)) or len(records))
        monkeypatch.setattr(ingest, "check_table_counts", lambda: {"persons": 1})

        result = ingest.run_pipeline()

        assert logged["count"] == 2
        assert result["total_rejected"] == 2
