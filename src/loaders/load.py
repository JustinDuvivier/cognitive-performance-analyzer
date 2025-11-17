import psycopg2
import logging
import json
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import DB_CONFIG

logger = logging.getLogger(__name__)


def get_db_connection():
    """Create database connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        return None


def get_pressure_24h_ago(current_timestamp):
    """Get pressure value from 24 hours ago for calculating pressure_change_24h"""
    from datetime import timedelta
    
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        cur = conn.cursor()
        # Look for pressure within 1 hour window of 24 hours ago (±30 minutes)
        target_time = current_timestamp - timedelta(hours=24)
        time_window_start = target_time - timedelta(minutes=30)
        time_window_end = target_time + timedelta(minutes=30)
        
        query = """
            SELECT pressure_hpa 
            FROM external_factors 
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


def upsert_external_factors(records):
    """Insert or update external factors data"""
    if not records:
        return 0, []

    conn = get_db_connection()
    if not conn:
        return 0, records

    inserted = 0
    rejected = []

    try:
        cur = conn.cursor()

        for record in records:
            try:
                # UPSERT query - insert or update on conflict
                query = """
                        INSERT INTO external_factors (timestamp, pressure_hpa, pressure_change_24h, \
                                                      temperature, humidity, hour_of_day, day_of_week, \
                                                      weekend, pm25, aqi) \
                        VALUES (%(timestamp)s, %(pressure_hpa)s, %(pressure_change_24h)s, \
                                %(temperature)s, %(humidity)s, %(hour_of_day)s, %(day_of_week)s, \
                                %(weekend)s, %(pm25)s, %(aqi)s) ON CONFLICT (timestamp) 
                    DO \
                        UPDATE SET
                            pressure_hpa = EXCLUDED.pressure_hpa, \
                            pressure_change_24h = EXCLUDED.pressure_change_24h, \
                            temperature = EXCLUDED.temperature, \
                            humidity = EXCLUDED.humidity, \
                            pm25 = EXCLUDED.pm25, \
                            aqi = EXCLUDED.aqi \
                        """

                cur.execute(query, record)
                inserted += 1
                logger.debug(f"Inserted/updated external factors for {record['timestamp']}")

            except Exception as e:
                logger.error(f"Failed to insert external record: {e}")
                rejected.append({
                    'record': record,
                    'error': str(e),
                    'table': 'external_factors'
                })

        conn.commit()
        logger.debug(f"Successfully inserted/updated {inserted} external factors records")

    except Exception as e:
        logger.error(f"Database error: {e}")
        conn.rollback()
    finally:
        conn.close()

    return inserted, rejected


def insert_user_tracking(records):
    """Insert user tracking data (no update, only new records)"""
    if not records:
        return 0, []

    conn = get_db_connection()
    if not conn:
        return 0, records

    inserted = 0
    rejected = []

    try:
        cur = conn.cursor()

        for record in records:
            try:
                # Insert only, skip if exists
                query = """
                        INSERT INTO user_tracking (timestamp, sleep_hours, breakfast_skipped, lunch_skipped, \
                                                   phone_usage, caffeine_count, steps, water_glasses, exercise, \
                                                   brain_fog_score, reaction_time_ms, verbal_memory_words) \
                        VALUES (%(timestamp)s, %(sleep_hours)s, %(breakfast_skipped)s, %(lunch_skipped)s, \
                                %(phone_usage)s, %(caffeine_count)s, %(steps)s, %(water_glasses)s, %(exercise)s, \
                                %(brain_fog_score)s, %(reaction_time_ms)s, \
                                %(verbal_memory_words)s) ON CONFLICT (timestamp) DO NOTHING \
                        """

                cur.execute(query, record)

                # Check if row was actually inserted
                if cur.rowcount > 0:
                    inserted += 1
                    logger.debug(f"Inserted user tracking for {record['timestamp']}")
                else:
                    logger.debug(f"Skipped duplicate user tracking for {record['timestamp']}")

            except Exception as e:
                logger.error(f"Failed to insert user record: {e}")
                rejected.append({
                    'record': record,
                    'error': str(e),
                    'table': 'user_tracking'
                })

        conn.commit()
        logger.debug(f"Successfully inserted {inserted} user tracking records")

    except Exception as e:
        logger.error(f"Database error: {e}")
        conn.rollback()
    finally:
        conn.close()

    return inserted, rejected


def log_rejected_records(rejected_records):
    """Log rejected records to stg_rejects table"""
    if not rejected_records:
        return 0

    conn = get_db_connection()
    if not conn:
        return 0

    logged = 0

    try:
        cur = conn.cursor()

        for reject in rejected_records:
            try:
                query = """
                        INSERT INTO stg_rejects (source_name, raw_payload, reason) \
                        VALUES (%(source_name)s, %(raw_payload)s, %(reason)s) \
                        """

                params = {
                    'source_name': reject.get('table', 'unknown'),
                    'raw_payload': json.dumps(reject.get('record', {})),
                    'reason': reject.get('error', 'Unknown error')[:500]  # Limit reason length
                }

                cur.execute(query, params)
                logged += 1

            except Exception as e:
                logger.error(f"Failed to log rejected record: {e}")

        conn.commit()
        logger.debug(f"Logged {logged} rejected records")

    except Exception as e:
        logger.error(f"Database error: {e}")
        conn.rollback()
    finally:
        conn.close()

    return logged


def check_table_counts():
    """Get current record counts for all tables"""
    conn = get_db_connection()
    if not conn:
        return {}

    counts = {}

    try:
        cur = conn.cursor()

        tables = ['external_factors', 'user_tracking', 'stg_rejects']

        for table in tables:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            counts[table] = count

    except Exception as e:
        logger.error(f"Failed to get counts: {e}")
    finally:
        conn.close()

    return counts