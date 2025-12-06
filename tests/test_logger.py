from datetime import datetime

import pytest

import loggers.logger as logger_module


class TestSetupLogging:
    def test_configures_logging(self):
        logger_module.setup_logging()
        logger = logger_module.get_logger("test")
        assert logger is not None


class TestGetLogger:
    def test_returns_logger(self):
        logger = logger_module.get_logger("test_module")
        assert logger.name == "test_module"


class TestLogRejectedRecords:
    def test_empty_records(self):
        assert logger_module.log_rejected_records([]) == 0

    def test_no_connection(self, monkeypatch):
        monkeypatch.setattr(logger_module, "_get_db_connection", lambda: None)
        assert logger_module.log_rejected_records([{"error": "test"}]) == 0

    def test_logs_records(self, monkeypatch):
        class MockCursor:
            def close(self):
                pass

        class MockConn:
            def cursor(self):
                return MockCursor()
            def commit(self):
                pass
            def close(self):
                pass

        monkeypatch.setattr(logger_module, "_get_db_connection", lambda: MockConn())
        monkeypatch.setattr(logger_module, "execute_values", lambda *args, **kwargs: None)

        count = logger_module.log_rejected_records([{"table": "t", "record": {"a": 1}, "error": "bad"}])

        assert count == 1


class TestLogPipelineStart:
    def test_returns_datetime(self):
        start = logger_module.log_pipeline_start("Test Pipeline")
        assert isinstance(start, datetime)


class TestLogPipelineEnd:
    def test_logs_stats(self):
        start = datetime.now()
        stats = {
            "total_read": 100,
            "total_validated": 95,
            "total_loaded": 90,
            "total_rejected": 10,
            "source_stats": [{"name": "test", "read": 100, "validated": 95, "loaded": 90, "rejected": 5, "db_rejected": 5}],
            "db_counts": {"persons": 4, "measurements": 90},
        }
        logger_module.log_pipeline_end("Test Pipeline", start, stats)


class TestFormatRejection:
    @pytest.mark.parametrize("errors,expected", [
        (["error1", "error2"], "error1; error2"),
        ([], "Unknown error"),
    ])
    def test_formats_errors(self, errors, expected):
        result = logger_module.format_rejection("test", {}, errors)
        assert result["error"] == expected

    def test_includes_table_and_record(self):
        result = logger_module.format_rejection("measurements", {"id": 1}, ["err"])
        assert result["table"] == "measurements"
        assert result["record"] == {"id": 1}


class TestLogHelpers:
    def test_log_validation_warning(self):
        logger_module.log_validation_warning("test_source", 5)

    def test_log_db_warning(self):
        logger_module.log_db_warning("test_source", 3)

    def test_log_source_start(self):
        logger_module.log_source_start("test", "Reading data")

    def test_log_source_result(self):
        logger_module.log_source_result("test", 100, "Read")
