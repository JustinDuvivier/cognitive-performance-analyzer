import logging
from datetime import datetime, timedelta

import psycopg2
from psycopg2.extras import execute_values

from config.config import DB_CONFIG

logger = logging.getLogger(__name__)

_person_cache = {}


def get_db_connection() -> psycopg2.extensions.connection | None:
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        return None


def _get_person_id_with_cursor(cur, person_name: str | None) -> int | None:
    if not person_name:
        return None

    if person_name in _person_cache:
        return _person_cache[person_name]

    cur.execute("SELECT person_id FROM persons WHERE name = %s", (person_name,))
    result = cur.fetchone()

    if result:
        _person_cache[person_name] = result[0]
        return result[0]

    logger.warning(f"Person not found: {person_name}")
    return None


def _load_all_persons_to_cache(cur: psycopg2.extensions.cursor) -> None:
    cur.execute("SELECT person_id, name FROM persons")
    for row in cur.fetchall():
        _person_cache[row[1]] = row[0]


def get_person_id(person_name: str | None) -> int | None:
    if not person_name:
        return None

    if person_name in _person_cache:
        return _person_cache[person_name]

    conn = get_db_connection()
    if not conn:
        return None

    try:
        cur = conn.cursor()
        return _get_person_id_with_cursor(cur, person_name)
    except Exception as e:
        logger.error(f"Error looking up person: {e}")
        return None
    finally:
        conn.close()


def get_all_persons() -> list[dict]:
    conn = get_db_connection()
    if not conn:
        return []

    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT person_id, name, location_name, latitude, longitude 
            FROM persons 
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        """)
        results = cur.fetchall()

        persons = []
        for row in results:
            _person_cache[row[1]] = row[0]
            persons.append({
                'person_id': row[0],
                'name': row[1],
                'location_name': row[2],
                'latitude': row[3],
                'longitude': row[4]
            })

        return persons

    except Exception as e:
        logger.error(f"Error fetching persons: {e}")
        return []
    finally:
        conn.close()


def get_pressure_24h_ago(current_timestamp: datetime, person_id: int | None = None) -> float | None:
    conn = get_db_connection()
    if not conn:
        return None

    try:
        cur = conn.cursor()
        target_time = current_timestamp - timedelta(hours=24)
        time_window_start = target_time - timedelta(minutes=30)
        time_window_end = target_time + timedelta(minutes=30)

        if person_id:
            query = """
                SELECT pressure_hpa 
                FROM measurements 
                WHERE timestamp BETWEEN %s AND %s 
                AND pressure_hpa IS NOT NULL
                AND person_id = %s
                ORDER BY ABS(EXTRACT(EPOCH FROM (timestamp - %s)))
                LIMIT 1
            """
            cur.execute(query, (time_window_start, time_window_end, person_id, target_time))
        else:
            query = """
                SELECT pressure_hpa 
                FROM measurements 
                WHERE timestamp BETWEEN %s AND %s 
                AND pressure_hpa IS NOT NULL
                ORDER BY ABS(EXTRACT(EPOCH FROM (timestamp - %s)))
                LIMIT 1
            """
            cur.execute(query, (time_window_start, time_window_end, target_time))

        result = cur.fetchone()
        return result[0] if result else None

    except Exception as e:
        logger.debug(f"Could not fetch historical pressure: {e}")
        return None
    finally:
        conn.close()


def _get_existing_measurements(cur: psycopg2.extensions.cursor, records: list[dict]) -> set:
    if not records:
        return set()

    pairs = []
    for r in records:
        person_id = r.get('person_id')
        timestamp = r.get('timestamp')
        if person_id and timestamp:
            pairs.append((person_id, timestamp))

    if not pairs:
        return set()

    placeholders = ','.join(['(%s, %s)'] * len(pairs))
    flat_values = [val for pair in pairs for val in pair]

    query = f"""
        SELECT person_id, timestamp 
        FROM measurements 
        WHERE (person_id, timestamp) IN ({placeholders})
    """
    cur.execute(query, flat_values)

    return {(row[0], row[1]) for row in cur.fetchall()}


def _resolve_person_ids(
    cur: psycopg2.extensions.cursor,
    records: list[dict]
) -> tuple[list[dict], list[dict]]:
    """Resolve person_id for records and separate valid from rejected.
    
    Returns:
        A tuple of (valid_records, rejected_records).
    """
    _load_all_persons_to_cache(cur)

    valid_records = []
    rejected = []

    for record in records:
        person_id = record.get('person_id')
        if not person_id and 'person' in record:
            person_id = _person_cache.get(record['person'])

        if not person_id:
            rejected.append({
                'record': record,
                'error': f"Could not resolve person_id for record: {record.get('person', 'unknown')}",
                'table': 'measurements'
            })
            continue

        record['person_id'] = person_id
        valid_records.append(record)

    return valid_records, rejected


def upsert_measurement_external(records: list[dict]) -> tuple[int, list[dict]]:
    if not records:
        return 0, []

    conn = get_db_connection()
    if not conn:
        return 0, records

    inserted = 0
    rejected: list[dict] = []

    try:
        cur = conn.cursor()
        valid_records, rejected = _resolve_person_ids(cur, records)

        if valid_records:
            values = [
                (
                    r['person_id'],
                    r['timestamp'],
                    r.get('pressure_hpa'),
                    r.get('pressure_change_24h'),
                    r.get('temperature'),
                    r.get('humidity'),
                    r.get('hour_of_day'),
                    r.get('day_of_week'),
                    r.get('weekend'),
                    r.get('pm25'),
                    r.get('aqi'),
                    r.get('co'),
                    r.get('no'),
                    r.get('no2'),
                    r.get('o3'),
                    r.get('so2'),
                    r.get('pm10'),
                    r.get('nh3'),
                )
                for r in valid_records
            ]

            query = """
                INSERT INTO measurements (
                    person_id, timestamp, pressure_hpa, pressure_change_24h,
                    temperature, humidity, hour_of_day, day_of_week, weekend,
                    pm25, aqi, co, no, no2, o3, so2, pm10, nh3
                ) VALUES %s
                ON CONFLICT (person_id, timestamp)
                DO UPDATE SET
                    pressure_hpa = EXCLUDED.pressure_hpa,
                    pressure_change_24h = EXCLUDED.pressure_change_24h,
                    temperature = EXCLUDED.temperature,
                    humidity = EXCLUDED.humidity,
                    hour_of_day = EXCLUDED.hour_of_day,
                    day_of_week = EXCLUDED.day_of_week,
                    weekend = EXCLUDED.weekend,
                    pm25 = EXCLUDED.pm25,
                    aqi = EXCLUDED.aqi,
                    co = EXCLUDED.co,
                    no = EXCLUDED.no,
                    no2 = EXCLUDED.no2,
                    o3 = EXCLUDED.o3,
                    so2 = EXCLUDED.so2,
                    pm10 = EXCLUDED.pm10,
                    nh3 = EXCLUDED.nh3
            """

            execute_values(cur, query, values)
            inserted = len(valid_records)

        conn.commit()
        logger.debug(f"Batch inserted/updated {inserted} measurement external records")

    except Exception as e:
        logger.error(f"Database error: {e}")
        conn.rollback()
        rejected.extend([{'record': r, 'error': str(e), 'table': 'measurements'} for r in records])
    finally:
        conn.close()

    return inserted, rejected


def upsert_measurement_user(records: list[dict]) -> tuple[int, list[dict]]:
    if not records:
        return 0, []

    conn = get_db_connection()
    if not conn:
        return 0, records

    inserted = 0
    rejected: list[dict] = []

    try:
        cur = conn.cursor()
        valid_records, rejected = _resolve_person_ids(cur, records)

        if valid_records:
            values = [
                (
                    r['person_id'],
                    r['timestamp'],
                    r.get('sleep_hours'),
                    r.get('phone_usage'),
                    r.get('steps'),
                    r.get('screen_time_minutes'),
                    r.get('active_energy_kcal'),
                    r.get('calories_intake'),
                    r.get('protein_g'),
                    r.get('carbs_g'),
                    r.get('fat_g'),
                    r.get('sequence_memory_score'),
                    r.get('reaction_time_ms'),
                    r.get('verbal_memory_words'),
                )
                for r in valid_records
            ]

            query = """
                INSERT INTO measurements (
                    person_id, timestamp,
                    sleep_hours, phone_usage, steps, screen_time_minutes,
                    active_energy_kcal, calories_intake, protein_g, carbs_g, fat_g,
                    sequence_memory_score, reaction_time_ms, verbal_memory_words
                ) VALUES %s
                ON CONFLICT (person_id, timestamp)
                DO UPDATE SET
                    sleep_hours = EXCLUDED.sleep_hours,
                    phone_usage = EXCLUDED.phone_usage,
                    steps = EXCLUDED.steps,
                    screen_time_minutes = EXCLUDED.screen_time_minutes,
                    active_energy_kcal = EXCLUDED.active_energy_kcal,
                    calories_intake = EXCLUDED.calories_intake,
                    protein_g = EXCLUDED.protein_g,
                    carbs_g = EXCLUDED.carbs_g,
                    fat_g = EXCLUDED.fat_g,
                    sequence_memory_score = EXCLUDED.sequence_memory_score,
                    reaction_time_ms = EXCLUDED.reaction_time_ms,
                    verbal_memory_words = EXCLUDED.verbal_memory_words
            """

            execute_values(cur, query, values)
            inserted = len(valid_records)

        conn.commit()
        logger.debug(f"Batch inserted/updated {inserted} measurement user records")

    except Exception as e:
        logger.error(f"Database error: {e}")
        conn.rollback()
        rejected.extend([{'record': r, 'error': str(e), 'table': 'measurements'} for r in records])
    finally:
        conn.close()

    return inserted, rejected


def check_table_counts() -> dict[str, int]:
    conn = get_db_connection()
    if not conn:
        return {}

    counts = {}

    try:
        cur = conn.cursor()

        cur.execute("""
            SELECT 
                (SELECT COUNT(*) FROM persons) as persons,
                (SELECT COUNT(*) FROM measurements) as measurements,
                (SELECT COUNT(*) FROM rejected_records) as rejected_records,
                (SELECT COUNT(*) FROM measurements WHERE sleep_hours IS NOT NULL) as with_user_data
        """)

        row = cur.fetchone()
        counts['persons'] = row[0]
        counts['measurements'] = row[1]
        counts['rejected_records'] = row[2]
        counts['measurements_with_user_data'] = row[3]

    except Exception as e:
        logger.error(f"Failed to get counts: {e}")
    finally:
        conn.close()

    return counts


def clear_person_cache() -> None:
    global _person_cache
    _person_cache = {}
