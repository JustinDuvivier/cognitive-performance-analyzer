import datetime

import pytest

from validators.validate import (
    validate_record,
    validate_batch,
    _build_field_validator,
    _get_table_rules,
    VALIDATION_RULES,
)


class TestValidateRecord:
    @pytest.fixture
    def valid_external_record(self):
        return {"timestamp": datetime.datetime.now(), "pressure_hpa": 1013.5, "temperature": 72.0, "humidity": 55.0, "hour_of_day": 14, "day_of_week": 3, "weekend": False}

    def test_valid_record_passes(self, valid_external_record):
        is_valid, errors = validate_record(valid_external_record, "measurements_external")
        assert is_valid is True
        assert errors == []

    def test_invalid_values_fail(self):
        record = {"pressure_hpa": 800, "temperature": 200}
        is_valid, errors = validate_record(record, "measurements_external")
        assert is_valid is False
        assert len(errors) > 0

    @pytest.mark.parametrize("table", ["unknown_table", "other_unknown"])
    def test_unknown_table_passes(self, table):
        is_valid, _ = validate_record({"foo": "bar"}, table)
        assert is_valid is True

    def test_unknown_table_passes(self):
        is_valid, _ = validate_record({"foo": "bar"}, "unknown_table")
        assert is_valid is True


class TestValidateBatch:
    def test_separates_valid_and_invalid(self):
        good = {"pressure_hpa": 1013, "temperature": 70, "humidity": 50, "hour_of_day": 10, "day_of_week": 2, "weekend": False}
        bad = {"pressure_hpa": 800, "temperature": 200}
        valid, invalid = validate_batch([good, bad], "measurements_external")
        assert len(valid) == 1
        assert len(invalid) == 1


class TestBuildFieldValidator:
    @pytest.mark.parametrize(
        "value,expected",
        [(True, True), ("yes", False), (False, True)],
    )
    def test_bool_validator(self, value, expected):
        validator = _build_field_validator("field", {"type": "bool"})
        assert validator(value) is expected

    @pytest.mark.parametrize(
        "value,expected",
        [(50, True), (150, False), (-1, False)],
    )
    def test_range_validator(self, value, expected):
        validator = _build_field_validator("field", {"min": 0, "max": 100})
        assert validator(value) is expected

    def test_null_handling(self):
        validator = _build_field_validator("field", {"min": 0, "max": 100, "allow_null": True})
        assert validator(None) is True


class TestGetTableRules:
    def test_known_table_returns_rules(self):
        rules = _get_table_rules("measurements_external")
        assert "pressure_hpa" in rules

    def test_unknown_table_returns_empty(self):
        assert _get_table_rules("unknown") == {}


class TestValidationRules:
    def test_rules_loaded(self):
        assert "measurements_external" in VALIDATION_RULES
        assert "measurements_user" in VALIDATION_RULES
