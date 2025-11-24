import logging
import os
from typing import Any, Dict

import yaml

logger = logging.getLogger(__name__)


def _load_validation_rules() -> Dict[str, Dict[str, Any]]:
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    config_path = os.path.join(project_root, "config", "validation_rules.yaml")

    if not os.path.exists(config_path):
        logger.warning("Validation rules config not found at %s", config_path)
        return {}

    with open(config_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    return data


VALIDATION_RULES = _load_validation_rules()


def _build_field_validator(field_name: str, spec: Dict[str, Any]):
    field_type = spec.get("type")
    min_val = spec.get("min")
    max_val = spec.get("max")
    allow_null = spec.get("allow_null", False)

    if field_type == "bool":
        def validator(x):
            if x is None and allow_null:
                return True
            return isinstance(x, bool)
    else:
        def validator(x):
            if x is None:
                return allow_null
            try:
                if min_val is not None and x < min_val:
                    return False
                if max_val is not None and x > max_val:
                    return False
                return True
            except TypeError:
                return False

    return validator


def _get_table_rules(table_name: str) -> Dict[str, Any]:
    table_spec = VALIDATION_RULES.get(table_name)
    if not table_spec:
        return {}

    rules = {}
    for field, spec in table_spec.items():
        rules[field] = _build_field_validator(field, spec or {})
    return rules


def validate_record(record, table_name):
    if table_name not in VALIDATION_RULES:
        logger.warning(f"No validation rules for table {table_name}")
        return True, []

    rules = _get_table_rules(table_name)
    errors = []

    for field, rule in rules.items():
        if field in record:
            value = record[field]
            try:
                if not rule(value):
                    errors.append(f"{field}={value} failed validation")
            except Exception as e:
                errors.append(f"{field}={value} caused error: {e}")

    is_valid = len(errors) == 0
    return is_valid, errors


def validate_batch(records, table_name):
    valid_records = []
    invalid_records = []

    for record in records:
        is_valid, errors = validate_record(record, table_name)
        if is_valid:
            valid_records.append(record)
        else:
            invalid_records.append({
                'record': record,
                'errors': errors,
                'table': table_name
            })
            logger.warning(f"Invalid record for {table_name}: {errors}")

    logger.debug(f"Validated {len(records)} records: {len(valid_records)} valid, {len(invalid_records)} invalid")
    return valid_records, invalid_records