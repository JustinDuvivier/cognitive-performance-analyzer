import logging

logger = logging.getLogger(__name__)

# Validation rules
VALIDATION_RULES = {
    'external_factors': {
        'pressure_hpa': lambda x: 900 <= x <= 1100 if x is not None else True,
        'pressure_change_24h': lambda x: -200 <= x <= 200 if x is not None else True,  # Reasonable range for 24h change
        'temperature': lambda x: -50 <= x <= 150 if x is not None else True,
        'humidity': lambda x: 0 <= x <= 100 if x is not None else True,
        'hour_of_day': lambda x: 0 <= x <= 23,
        'day_of_week': lambda x: 1 <= x <= 7,  # ISO: Monday=1, Sunday=7
        'weekend': lambda x: isinstance(x, bool),
        'pm25': lambda x: 0 <= x <= 500 if x is not None else True,
        'aqi': lambda x: 0 <= x <= 500 if x is not None else True,
    },
    'user_tracking': {
        'sleep_hours': lambda x: 0 <= x <= 24,
        'breakfast_skipped': lambda x: isinstance(x, bool),
        'lunch_skipped': lambda x: isinstance(x, bool),
        'phone_usage': lambda x: 0 <= x <= 500,
        'caffeine_count': lambda x: 0 <= x <= 20,
        'steps': lambda x: 0 <= x <= 100000,
        'water_glasses': lambda x: 0 <= x <= 30,
        'exercise': lambda x: isinstance(x, bool),
        'brain_fog_score': lambda x: 1 <= x <= 10,
        'reaction_time_ms': lambda x: 100 <= x <= 2000,
        'verbal_memory_words': lambda x: 0 <= x <= 100,
    }
}


def validate_record(record, table_name):
    """Validate a single record against rules for the specified table"""
    if table_name not in VALIDATION_RULES:
        logger.warning(f"No validation rules for table {table_name}")
        return True, []

    rules = VALIDATION_RULES[table_name]
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
    """Validate a batch of records"""
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