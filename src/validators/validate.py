import logging
from typing import Any

import yaml

from config.config import PROJECT_ROOT

logger = logging.getLogger(__name__)


def _load_validation_rules() -> dict[str, dict[str, Any]]:
    config_path = PROJECT_ROOT / "src" / "config" / "validation_rules.yaml"

    if not config_path.exists():
        logger.warning("Validation rules config not found at %s", config_path)
        return {}

    with open(config_path, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    return data


VALIDATION_RULES = _load_validation_rules()


def _validate_field(value: Any, spec: dict[str, Any]) -> bool:
    """Check if a single field value passes its validation rules."""
    field_type = spec.get("type")
    min_val = spec.get("min")
    max_val = spec.get("max")
    allow_null = spec.get("allow_null", False)

    if value is None:
        return allow_null

    if field_type == "bool":
        return isinstance(value, bool)

    try:
        if min_val is not None and value < min_val:
            return False
        if max_val is not None and value > max_val:
            return False
        return True
    except TypeError:
        return False


def validate_record(record: dict, table_name: str) -> tuple[bool, list[str]]:
    """Validate a single record against the rules for its table."""
    if table_name not in VALIDATION_RULES:
        logger.warning(f"No validation rules for table {table_name}")
        return True, []

    table_rules = VALIDATION_RULES[table_name]
    errors = []

    for field, spec in table_rules.items():
        if field in record:
            value = record[field]
            if not _validate_field(value, spec or {}):
                errors.append(f"{field}={value} failed validation")

    return len(errors) == 0, errors


def validate_batch(records: list[dict], table_name: str) -> tuple[list[dict], list[dict]]:
    """Validate a list of records, separating valid from invalid."""
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
