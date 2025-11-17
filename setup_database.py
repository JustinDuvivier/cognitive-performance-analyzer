import psycopg2
from psycopg2 import sql
import os
from dotenv import load_dotenv

load_dotenv()


def create_database():
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        database='postgres',
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )
    conn.autocommit = True
    cur = conn.cursor()

    try:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = 'brain_fog_db'")
        exists = cur.fetchone()
        if not exists:
            cur.execute('CREATE DATABASE brain_fog_db')
            print("✓ Created brain_fog_db database")
        else:
            print("✓ Database brain_fog_db already exists")
    except Exception as e:
        print(f"Error creating database: {e}")
    finally:
        cur.close()
        conn.close()


def create_tables():
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        database='brain_fog_db',
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )
    cur = conn.cursor()

    try:
        cur.execute("""
                    CREATE TABLE IF NOT EXISTS external_factors
                    (
                        timestamp
                        TIMESTAMP
                        PRIMARY
                        KEY,
                        pressure_hpa
                        FLOAT,
                        pressure_change_24h
                        FLOAT,
                        temperature
                        FLOAT,
                        humidity
                        FLOAT,
                        hour_of_day
                        INT,
                        day_of_week
                        INT,
                        weekend
                        BOOLEAN,
                        pm25
                        FLOAT,
                        aqi
                        INT
                    )
                    """)
        print("✓ Created external_factors table")

        cur.execute("""
                    CREATE TABLE IF NOT EXISTS user_tracking
                    (
                        timestamp
                        TIMESTAMP
                        PRIMARY
                        KEY,
                        sleep_hours
                        FLOAT,
                        breakfast_skipped
                        BOOLEAN,
                        lunch_skipped
                        BOOLEAN,
                        phone_usage
                        INT,
                        caffeine_count
                        INT,
                        steps
                        INT,
                        water_glasses
                        INT,
                        exercise
                        BOOLEAN,
                        brain_fog_score
                        INT
                        CHECK
                    (
                        brain_fog_score
                        BETWEEN
                        1
                        AND
                        10
                    ),
                        reaction_time_ms FLOAT,
                        verbal_memory_words INT
                        )
                    """)
        print("✓ Created user_tracking table")

        cur.execute("""
                    CREATE TABLE IF NOT EXISTS stg_rejects
                    (
                        rejected_at
                        TIMESTAMP
                        DEFAULT
                        NOW
                    (
                    ),
                        source_name TEXT,
                        raw_payload JSONB,
                        reason TEXT
                        )
                    """)
        print("✓ Created stg_rejects table")

        conn.commit()
        print("\n✓ All tables created successfully!")

    except Exception as e:
        print(f"Error creating tables: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    create_database()
    create_tables()
    print("\nDatabase setup complete!")