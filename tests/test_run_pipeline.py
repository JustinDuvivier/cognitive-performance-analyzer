import pytest

import run_pipeline
from run_pipeline import (
    format_invalid_for_rejects,
    validate_clean_and_load,
    run_measurement_external_flow,
    run_measurement_user_flow,
)


class TestFormatInvalidForRejects:
    def test_formats_complete_invalid(self):
        invalid_record = {"table": "fact_cognitive_performance", "record": {"foo": "bar"}, "errors": ["error1", "error2"]}
        formatted = format_invalid_for_rejects(invalid_record)
        assert formatted["table"] == "fact_cognitive_performance"
        assert formatted["error"] == "error1; error2"

    def test_handles_missing_fields(self):
        formatted = format_invalid_for_rejects({})
        assert formatted["table"] == "unknown"
        assert formatted["error"] == "Unknown error"


class TestValidateCleanAndLoad:
    def test_tracks_validation_results(self, monkeypatch):
        monkeypatch.setattr(run_pipeline, "validate_batch", lambda r, n: (r, []))
        input_stats = {"name": "test", "read": 2, "validated": 0, "rejected": 0, "loaded": 0, "db_rejected": 0}

        result_stats, _ = validate_clean_and_load([{"a": 1}, {"b": 2}], "test", input_stats, [], lambda x: x, lambda x: (len(x), []))

        assert result_stats["validated"] == 2
        assert result_stats["loaded"] == 2

    def test_returns_when_all_invalid(self, monkeypatch):
        invalid_records = [{"table": "t", "record": {}, "errors": ["bad"]}]
        monkeypatch.setattr(run_pipeline, "validate_batch", lambda r, n: ([], invalid_records))
        input_stats = {"name": "test", "read": 1, "validated": 0, "rejected": 0, "loaded": 0, "db_rejected": 0}

        result_stats, result_rejected = validate_clean_and_load([], "test", input_stats, [], lambda x: x, lambda x: (0, []))

        assert result_stats["validated"] == 0
        assert result_stats["rejected"] == 1
        assert result_rejected[0]["error"] == "bad"

    def test_db_rejected_added(self, monkeypatch):
        monkeypatch.setattr(run_pipeline, "validate_batch", lambda r, n: (r, []))
        input_stats = {"name": "test", "read": 1, "validated": 0, "rejected": 0, "loaded": 0, "db_rejected": 0}

        result_stats, result_rejected = validate_clean_and_load([{"x": 1}], "test", input_stats, [], lambda x: x, lambda x: (0, [{"error": "db"}]))

        assert result_stats["db_rejected"] == 1
        assert result_rejected[0]["error"] == "db"


class TestRunMeasurementExternalFlow:
    def test_no_data_returns_empty(self, monkeypatch):
        monkeypatch.setattr(run_pipeline, "read_all_external_data", lambda: [])
        result_stats, result_rejected = run_measurement_external_flow()
        assert result_stats["read"] == 0
        assert result_rejected == []

    def test_with_data_runs_validation(self, monkeypatch):
        monkeypatch.setattr(run_pipeline, "read_all_external_data", lambda: [{"a": 1}])
        called = {}

        def fake_validate(records, _name, input_stats, input_rejected, _cleaner, _loader):
            called["ran"] = True
            input_stats["validated"] = len(records)
            input_stats["loaded"] = len(records)
            return input_stats, input_rejected

        monkeypatch.setattr(run_pipeline, "validate_clean_and_load", fake_validate)
        result_stats, result_rejected = run_measurement_external_flow()

        assert called.get("ran") is True
        assert result_stats["validated"] == 1
        assert result_rejected == []


class TestRunMeasurementUserFlow:
    def test_no_data_returns_empty(self, monkeypatch):
        monkeypatch.setattr(run_pipeline, "read_all_user_data", lambda: [])
        result_stats, result_rejected = run_measurement_user_flow()
        assert result_stats["read"] == 0
        assert result_rejected == []

    def test_with_data_runs_validation(self, monkeypatch):
        monkeypatch.setattr(run_pipeline, "read_all_user_data", lambda: [{"a": 1}, {"b": 2}])
        called = {}

        def fake_validate(records, _name, input_stats, input_rejected, _cleaner, _loader):
            called["ran"] = True
            input_stats["validated"] = len(records)
            input_stats["loaded"] = len(records)
            return input_stats, input_rejected

        monkeypatch.setattr(run_pipeline, "validate_clean_and_load", fake_validate)
        result_stats, result_rejected = run_measurement_user_flow()

        assert called.get("ran") is True
        assert result_stats["validated"] == 2
        assert result_rejected == []


class TestRunPipeline:
    @pytest.fixture
    def mock_pipeline(self, monkeypatch):
        monkeypatch.setattr(run_pipeline, "run_measurement_external_flow", lambda: (
            {"name": "measurements_external", "read": 1, "validated": 1, "rejected": 0, "loaded": 1, "db_rejected": 0}, []
        ))
        monkeypatch.setattr(run_pipeline, "run_measurement_user_flow", lambda: (
            {"name": "measurements_user", "read": 2, "validated": 2, "rejected": 0, "loaded": 2, "db_rejected": 0}, []
        ))
        monkeypatch.setattr(run_pipeline, "log_rejected_records", lambda x: len(x))
        monkeypatch.setattr(run_pipeline, "check_table_counts", lambda: {})
        monkeypatch.setattr(run_pipeline, "log_pipeline_start", lambda x: __import__('datetime').datetime.now())
        monkeypatch.setattr(run_pipeline, "log_pipeline_end", lambda *args: None)

    def test_successful_run(self, mock_pipeline):
        pipeline_result = run_pipeline.run_pipeline()
        assert pipeline_result["success"] is True
        assert pipeline_result["total_read"] == 3
        assert pipeline_result["total_loaded"] == 3

    def test_returns_source_stats(self, mock_pipeline):
        pipeline_result = run_pipeline.run_pipeline()
        names = {s["name"] for s in pipeline_result["source_stats"]}
        assert names == {"measurements_external", "measurements_user"}

    def test_logs_rejections_and_counts(self, monkeypatch):
        monkeypatch.setattr(run_pipeline, "run_measurement_external_flow", lambda: (
            {"name": "measurements_external", "read": 1, "validated": 1, "rejected": 1, "loaded": 0, "db_rejected": 0},
            [{"error": "bad"}],
        ))
        monkeypatch.setattr(run_pipeline, "run_measurement_user_flow", lambda: (
            {"name": "measurements_user", "read": 0, "validated": 0, "rejected": 0, "loaded": 0, "db_rejected": 1},
            [{"error": "db"}],
        ))
        logged = {}
        monkeypatch.setattr(run_pipeline, "log_rejected_records", lambda records: logged.setdefault("count", len(records)) or len(records))
        monkeypatch.setattr(run_pipeline, "check_table_counts", lambda: {"dim_persons": 1})
        monkeypatch.setattr(run_pipeline, "log_pipeline_start", lambda x: __import__('datetime').datetime.now())
        monkeypatch.setattr(run_pipeline, "log_pipeline_end", lambda *args: None)

        pipeline_result = run_pipeline.run_pipeline()

        assert logged["count"] == 2
        assert pipeline_result["total_rejected"] == 2
