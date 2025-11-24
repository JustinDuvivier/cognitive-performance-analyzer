import logging
from datetime import datetime
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


def clean_timestamp(timestamp):
    if isinstance(timestamp, str):
        return pd.to_datetime(timestamp)
    elif isinstance(timestamp, pd.Timestamp):
        return timestamp.to_pydatetime()
    elif isinstance(timestamp, datetime):
        return timestamp
    else:
        return datetime.now()


def safe_float(value, default=None):
    if value is None or value == '':
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def safe_int(value, default=0):
    if value is None or value == '':
        return default
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return default


def safe_bool(value, default=False):
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.upper() in ['Y', 'YES', 'TRUE', '1']
    return bool(value) if value is not None else default


def clean_external_factors(record):
    cleaned = {}

    cleaned['timestamp'] = clean_timestamp(record.get('timestamp', datetime.now()))

    cleaned['pressure_hpa'] = safe_float(record.get('pressure_hpa'), None)
    cleaned['pressure_change_24h'] = safe_float(record.get('pressure_change_24h'), 0)
    cleaned['temperature'] = safe_float(record.get('temperature'), None)
    cleaned['humidity'] = safe_float(record.get('humidity'), None)
    cleaned['hour_of_day'] = safe_int(record.get('hour_of_day', datetime.now().hour))
    cleaned['day_of_week'] = safe_int(record.get('day_of_week', datetime.now().isoweekday()))  # ISO: Monday=1, Sunday=7
    cleaned['weekend'] = safe_bool(record.get('weekend', datetime.now().isoweekday() >= 6))  # Saturday=6, Sunday=7

    cleaned['pm25'] = safe_float(record.get('pm25'), None)
    cleaned['aqi'] = safe_int(record.get('aqi'), None)

    cleaned['co'] = safe_float(record.get('co'), None)
    cleaned['no'] = safe_float(record.get('no'), None)
    cleaned['no2'] = safe_float(record.get('no2'), None)
    cleaned['o3'] = safe_float(record.get('o3'), None)
    cleaned['so2'] = safe_float(record.get('so2'), None)
    cleaned['pm10'] = safe_float(record.get('pm10'), None)
    cleaned['nh3'] = safe_float(record.get('nh3'), None)

    return cleaned


def clean_user_tracking(record):
    cleaned = {}
    cleaned['timestamp'] = clean_timestamp(record.get('timestamp', datetime.now()))
    cleaned['sleep_hours'] = safe_float(record.get('sleep_hours'), 8)
    cleaned['phone_usage'] = safe_int(record.get('phone_usage'), 0)
    cleaned['steps'] = safe_int(record.get('steps'), 0)
    cleaned['screen_time_minutes'] = safe_int(record.get('screen_time_minutes'), 0)
    cleaned['active_energy_kcal'] = safe_float(record.get('active_energy_kcal'), 0)
    cleaned['calories_intake'] = safe_float(record.get('calories_intake'), 0)
    cleaned['protein_g'] = safe_float(record.get('protein_g'), 0)
    cleaned['carbs_g'] = safe_float(record.get('carbs_g'), 0)
    cleaned['fat_g'] = safe_float(record.get('fat_g'), 0)

    cleaned['sequence_memory_score'] = safe_int(record.get('sequence_memory_score'), 0)
    cleaned['reaction_time_ms'] = safe_float(record.get('reaction_time_ms'), 300)
    cleaned['verbal_memory_words'] = safe_int(record.get('verbal_memory_words'), 10)

    return cleaned


def merge_and_clean(external_data, user_data):
    merged_records = []

    if external_data:
        external_df = pd.DataFrame([clean_external_factors(r) for r in external_data])
    else:
        external_df = pd.DataFrame()

    if user_data:
        user_df = pd.DataFrame([clean_user_tracking(r) for r in user_data])
    else:
        user_df = pd.DataFrame()

    logger.debug(f"Cleaning {len(external_df)} external records and {len(user_df)} user records")

    return (
        external_df.to_dict('records') if not external_df.empty else [],
        user_df.to_dict('records') if not user_df.empty else [],
    )


def prepare_for_insert(record):
    prepared = {}

    for key, value in record.items():
        if isinstance(value, float) and np.isnan(value):
            prepared[key] = None
        elif isinstance(value, pd.Timestamp):
            prepared[key] = value.to_pydatetime()
        else:
            prepared[key] = value

    return prepared