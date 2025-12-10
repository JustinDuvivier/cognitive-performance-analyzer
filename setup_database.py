import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

DB_NAME = os.getenv('DB_NAME', 'cognitive_performance_db')


def get_db_connection(database=None):
    if database is None:
        database = DB_NAME
    return psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        database=database,
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )


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
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DB_NAME,))
        exists = cur.fetchone()
        if not exists:
            cur.execute(f'CREATE DATABASE {DB_NAME}')
            print(f"✓ Created {DB_NAME} database")
        else:
            print(f"✓ Database {DB_NAME} already exists")
    except Exception as e:
        print(f"Error creating database: {e}")
    finally:
        cur.close()
        conn.close()


def create_tables():
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_persons (
                person_id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL UNIQUE,
                location_name VARCHAR(100),
                latitude FLOAT,
                longitude FLOAT,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        print("✓ Created dim_persons table")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS fact_cognitive_performance (
                fact_id SERIAL PRIMARY KEY,
                person_id INT NOT NULL REFERENCES dim_persons(person_id),
                timestamp TIMESTAMP NOT NULL,
                pressure_hpa FLOAT,
                pressure_change_24h FLOAT,
                temperature FLOAT,
                humidity FLOAT,
                hour_of_day INT,
                day_of_week INT,
                weekend BOOLEAN,
                pm25 FLOAT,
                aqi INT,
                co FLOAT,
                no FLOAT,
                no2 FLOAT,
                o3 FLOAT,
                so2 FLOAT,
                pm10 FLOAT,
                nh3 FLOAT,
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
                verbal_memory_words INT,
                UNIQUE(person_id, timestamp)
            )
        """)
        print("✓ Created fact_cognitive_performance table (unified behavioral / cognitive / environmental facts)")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS rejected_records (
                id SERIAL PRIMARY KEY,
                rejected_at TIMESTAMP DEFAULT NOW(),
                source_name TEXT,
                raw_payload JSONB,
                reason TEXT
            )
        """)
        print("✓ Created rejected_records table")

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
