import datetime

import pytest

import validators.validate


class TestValidateRecord:
    @pytest.fixture
    def valid_external_record(self):
        return {"timestamp": datetime.datetime.now(), "pressure_hpa": 1013.5, "temperature": 72.0, "humidity": 55.0, "hour_of_day": 14, "day_of_week": 3, "weekend": False}

    def test_valid_record_passes(self, valid_external_record):
        is_valid, errors = validators.validate.validate_record(valid_external_record, "measurements_external")
        assert is_valid is True
        assert errors == []

    def test_invalid_values_fail(self):
        record = {"pressure_hpa": 800, "temperature": 200}
        is_valid, errors = validators.validate.validate_record(record, "measurements_external")
        assert is_valid is False
        assert len(errors) > 0

    @pytest.mark.parametrize("table", ["unknown_table", "other_unknown"])
    def test_unknown_table_passes(self, table):
        is_valid, _ = validators.validate.validate_record({"foo": "bar"}, table)
        assert is_valid is True


class TestValidateBatch:
    def test_separates_valid_and_invalid(self):
        good = {"pressure_hpa": 1013, "temperature": 70, "humidity": 50, "hour_of_day": 10, "day_of_week": 2, "weekend": False}
        bad = {"pressure_hpa": 800, "temperature": 200}
        valid, invalid = validators.validate.validate_batch([good, bad], "measurements_external")
        assert len(valid) == 1
        assert len(invalid) == 1


class TestValidateField:
    @pytest.mark.parametrize("value,expected", [
        (True, True),
        ("yes", False),
        (False, True),
    ])
    def test_bool_validation(self, value, expected):
        result = validators.validate._validate_field(value, {"type": "bool"})
        assert result is expected

    @pytest.mark.parametrize("value,expected", [
        (50, True),
        (150, False),
        (-1, False),
    ])
    def test_range_validation(self, value, expected):
        result = validators.validate._validate_field(value, {"min": 0, "max": 100})
        assert result is expected

    def test_null_allowed(self):
        result = validators.validate._validate_field(None, {"min": 0, "max": 100, "allow_null": True})
        assert result is True

    def test_null_not_allowed(self):
        result = validators.validate._validate_field(None, {"min": 0, "max": 100, "allow_null": False})
        assert result is False


class TestValidationRules:
    def test_rules_loaded(self):
        assert "measurements_external" in validators.validate.VALIDATION_RULES
        assert "measurements_user" in validators.validate.VALIDATION_RULES
