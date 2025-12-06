import logging
from datetime import datetime, timedelta
from typing import Any

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


def round_to_nearest_hour(dt: datetime) -> datetime:
    rounded = dt + timedelta(minutes=30)
    return rounded.replace(minute=0, second=0, microsecond=0)


def clean_timestamp(timestamp: str | pd.Timestamp | datetime | None, round_to_hour: bool = True) -> datetime:
    if isinstance(timestamp, str):
        dt = pd.to_datetime(timestamp).to_pydatetime()
    elif isinstance(timestamp, pd.Timestamp):
        dt = timestamp.to_pydatetime()
    elif isinstance(timestamp, datetime):
        dt = timestamp
    else:
        dt = datetime.now()
    
    if round_to_hour:
        dt = round_to_nearest_hour(dt)
    
    return dt


def safe_float(value: Any, default: float | None = None) -> float | None:
    if value is None or value == '':
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def safe_int(value: Any, default: int | None = 0) -> int | None:
    if value is None or value == '':
        return default
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return default


def safe_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.upper() in ['Y', 'YES', 'TRUE', '1']
    return bool(value) if value is not None else default


def clean_measurement_external(record: dict) -> dict:
    cleaned = {}

    if 'person_id' in record:
        cleaned['person_id'] = safe_int(record.get('person_id'))
    if 'person' in record:
        cleaned['person'] = str(record.get('person', '')).strip()

    cleaned['timestamp'] = clean_timestamp(record.get('timestamp', datetime.now()), round_to_hour=True)

    cleaned['pressure_hpa'] = safe_float(record.get('pressure_hpa'), None)
    cleaned['pressure_change_24h'] = safe_float(record.get('pressure_change_24h'), 0)
    cleaned['temperature'] = safe_float(record.get('temperature'), None)
    cleaned['humidity'] = safe_float(record.get('humidity'), None)

    cleaned['hour_of_day'] = cleaned['timestamp'].hour
    cleaned['day_of_week'] = cleaned['timestamp'].isoweekday()
    cleaned['weekend'] = cleaned['timestamp'].isoweekday() >= 6  

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


def clean_measurement_user(record: dict) -> dict:
    cleaned = {}

    if 'person_id' in record:
        cleaned['person_id'] = safe_int(record.get('person_id'))
    if 'person' in record:
        cleaned['person'] = str(record.get('person', '')).strip()

    cleaned['timestamp'] = clean_timestamp(record.get('timestamp', datetime.now()), round_to_hour=True)
    
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


def prepare_for_insert(record: dict) -> dict:
    prepared = {}

    for key, value in record.items():
        if isinstance(value, float) and np.isnan(value):
            prepared[key] = None
        elif isinstance(value, pd.Timestamp):
            prepared[key] = value.to_pydatetime()
        else:
            prepared[key] = value

    return prepared
