import psycopg2
from psycopg2.extras import execute_values
import logging
import json
import sys
import os
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import DB_CONFIG

logger = logging.getLogger(__name__)

_person_cache = {}


def get_db_connection() -> psycopg2.extensions.connection | None:
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        return None


def _get_person_id_with_cursor(cur, person_name: str) -> int | None:
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


def get_person_id(person_name: str) -> int | None:
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


def upsert_measurement_external(records: list[dict]) -> tuple[int, list[dict]]:
    if not records:
        return 0, []

    conn = get_db_connection()
    if not conn:
        return 0, records

    inserted = 0
    rejected = []

    try:
        cur = conn.cursor()
        _load_all_persons_to_cache(cur)

        valid_records = []
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


def update_measurement_user_data(records: list[dict]) -> tuple[int, list[dict]]:
    if not records:
        return 0, []

    conn = get_db_connection()
    if not conn:
        return 0, records

    updated = 0
    rejected = []

    try:
        cur = conn.cursor()
        _load_all_persons_to_cache(cur)

        resolved_records = []
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
            resolved_records.append(record)

        existing = _get_existing_measurements(cur, resolved_records)

        valid_records = []
        for record in resolved_records:
            person_id = record['person_id']
            timestamp = record.get('timestamp')
            person_name = record.get('person', f'ID:{person_id}')

            if (person_id, timestamp) not in existing:
                rejected.append({
                    'record': record,
                    'error': f"No measurement row exists for {person_name} at {timestamp}. External factors must be loaded first.",
                    'table': 'measurements'
                })
                continue

            valid_records.append(record)

        if valid_records:
            cur.execute("""
                CREATE TEMP TABLE temp_user_updates (
                    person_id INT,
                    timestamp TIMESTAMP,
                    sleep_hours FLOAT,
                    phone_usage INT,
                    steps INT,
                    screen_time_minutes INT,
                    active_energy_kcal FLOAT,
                    calories_intake FLOAT,
                    protein_g FLOAT,
                    carbs_g FLOAT,
                    fat_g FLOAT,
                    sequence_memory_score INT,
                    reaction_time_ms FLOAT,
                    verbal_memory_words INT
                ) ON COMMIT DROP
            """)

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

            execute_values(
                cur,
                """INSERT INTO temp_user_updates VALUES %s""",
                values
            )

            cur.execute("""
                UPDATE measurements m SET
                    sleep_hours = t.sleep_hours,
                    phone_usage = t.phone_usage,
                    steps = t.steps,
                    screen_time_minutes = t.screen_time_minutes,
                    active_energy_kcal = t.active_energy_kcal,
                    calories_intake = t.calories_intake,
                    protein_g = t.protein_g,
                    carbs_g = t.carbs_g,
                    fat_g = t.fat_g,
                    sequence_memory_score = t.sequence_memory_score,
                    reaction_time_ms = t.reaction_time_ms,
                    verbal_memory_words = t.verbal_memory_words
                FROM temp_user_updates t
                WHERE m.person_id = t.person_id AND m.timestamp = t.timestamp
            """)

            updated = cur.rowcount

        conn.commit()
        logger.debug(f"Batch updated {updated} measurement user records")

    except Exception as e:
        logger.error(f"Database error: {e}")
        conn.rollback()
    finally:
        conn.close()

    return updated, rejected


def log_rejected_records(rejected_records: list[dict]) -> int:
    if not rejected_records:
        return 0

    conn = get_db_connection()
    if not conn:
        return 0

    logged = 0

    try:
        cur = conn.cursor()

        values = [
            (
                reject.get('table', 'unknown'),
                json.dumps(reject.get('record', {}), default=str),
                reject.get('error', 'Unknown error')[:500],
            )
            for reject in rejected_records
        ]

        execute_values(
            cur,
            """INSERT INTO stg_rejects (source_name, raw_payload, reason) VALUES %s""",
            values
        )

        logged = len(values)
        conn.commit()
        logger.debug(f"Batch logged {logged} rejected records")

    except Exception as e:
        logger.error(f"Database error: {e}")
        conn.rollback()
    finally:
        conn.close()

    return logged


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
                (SELECT COUNT(*) FROM stg_rejects) as stg_rejects,
                (SELECT COUNT(*) FROM measurements WHERE sleep_hours IS NOT NULL) as with_user_data
        """)

        row = cur.fetchone()
        counts['persons'] = row[0]
        counts['measurements'] = row[1]
        counts['stg_rejects'] = row[2]
        counts['measurements_with_user_data'] = row[3]

    except Exception as e:
        logger.error(f"Failed to get counts: {e}")
    finally:
        conn.close()

    return counts


def clear_person_cache() -> None:
    global _person_cache
    _person_cache = {}
