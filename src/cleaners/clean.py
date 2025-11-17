import logging
from datetime import datetime
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


def clean_timestamp(timestamp):
    """Ensure timestamp is a datetime object"""
    if isinstance(timestamp, str):
        return pd.to_datetime(timestamp)
    elif isinstance(timestamp, pd.Timestamp):
        return timestamp.to_pydatetime()
    elif isinstance(timestamp, datetime):
        return timestamp
    else:
        return datetime.now()


def safe_float(value, default=None):
    """Safely convert value to float, handling empty strings and None"""
    if value is None or value == '':
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def safe_int(value, default=0):
    """Safely convert value to int, handling empty strings and None"""
    if value is None or value == '':
        return default
    try:
        return int(float(value))  # Convert through float to handle '4.8' -> 4
    except (ValueError, TypeError):
        return default


def safe_bool(value, default=False):
    """Safely convert value to bool"""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.upper() in ['Y', 'YES', 'TRUE', '1']
    return bool(value) if value is not None else default


def clean_external_factors(record):
    """Clean and normalize external factors data"""
    cleaned = {}

    # Required fields
    cleaned['timestamp'] = clean_timestamp(record.get('timestamp', datetime.now()))

    # Numeric fields with defaults
    cleaned['pressure_hpa'] = safe_float(record.get('pressure_hpa'), None)
    # pressure_change_24h is calculated from historical data, default to 0 if not calculated
    cleaned['pressure_change_24h'] = safe_float(record.get('pressure_change_24h'), 0)
    cleaned['temperature'] = safe_float(record.get('temperature'), None)
    cleaned['humidity'] = safe_float(record.get('humidity'), None)
    cleaned['hour_of_day'] = safe_int(record.get('hour_of_day', datetime.now().hour))
    cleaned['day_of_week'] = safe_int(record.get('day_of_week', datetime.now().isoweekday()))  # ISO: Monday=1, Sunday=7
    cleaned['weekend'] = safe_bool(record.get('weekend', datetime.now().isoweekday() >= 6))  # Saturday=6, Sunday=7

    # Optional fields (can be None)
    cleaned['pm25'] = safe_float(record.get('pm25'), None)
    cleaned['aqi'] = safe_int(record.get('aqi'), None)

    return cleaned


def clean_user_tracking(record):
    """Clean and normalize user tracking data"""
    cleaned = {}

    # Required timestamp
    cleaned['timestamp'] = clean_timestamp(record.get('timestamp', datetime.now()))

    # Behavioral fields
    cleaned['sleep_hours'] = safe_float(record.get('sleep_hours'), 8)
    cleaned['breakfast_skipped'] = safe_bool(record.get('breakfast_skipped'), False)
    cleaned['lunch_skipped'] = safe_bool(record.get('lunch_skipped'), False)
    cleaned['phone_usage'] = safe_int(record.get('phone_usage'), 0)
    cleaned['caffeine_count'] = safe_int(record.get('caffeine_count'), 0)
    cleaned['steps'] = safe_int(record.get('steps'), 0)
    cleaned['water_glasses'] = safe_int(record.get('water_glasses'), 0)
    cleaned['exercise'] = safe_bool(record.get('exercise'), False)

    # Cognitive fields
    cleaned['brain_fog_score'] = safe_int(record.get('brain_fog_score'), 5)
    cleaned['reaction_time_ms'] = safe_float(record.get('reaction_time_ms'), 300)
    cleaned['verbal_memory_words'] = safe_int(record.get('verbal_memory_words'), 10)

    return cleaned


def merge_and_clean(external_data, user_data):
    """Merge external and user data for the same timestamp"""
    merged_records = []

    # Convert to DataFrames for easier merging
    if external_data:
        external_df = pd.DataFrame([clean_external_factors(r) for r in external_data])
    else:
        external_df = pd.DataFrame()

    if user_data:
        user_df = pd.DataFrame([clean_user_tracking(r) for r in user_data])
    else:
        user_df = pd.DataFrame()

    logger.debug(f"Cleaning {len(external_df)} external records and {len(user_df)} user records")

    return external_df.to_dict('records') if not external_df.empty else [], \
        user_df.to_dict('records') if not user_df.empty else []


def prepare_for_insert(record):
    """Final preparation before database insertion"""
    # Ensure all None values are properly handled for PostgreSQL
    prepared = {}

    for key, value in record.items():
        # Convert NaN to None for database
        if isinstance(value, float) and np.isnan(value):
            prepared[key] = None
        # Convert pandas Timestamp to datetime
        elif isinstance(value, pd.Timestamp):
            prepared[key] = value.to_pydatetime()
        else:
            prepared[key] = value

    return prepared