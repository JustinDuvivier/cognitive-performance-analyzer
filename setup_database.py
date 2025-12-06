import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

DB_NAME = os.getenv('DB_NAME', 'brain_fog_db')


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
            CREATE TABLE IF NOT EXISTS persons (
                person_id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL UNIQUE,
                location_name VARCHAR(100),
                latitude FLOAT,
                longitude FLOAT,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        print("✓ Created persons table")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS measurements (
                id SERIAL PRIMARY KEY,
                person_id INT NOT NULL REFERENCES persons(person_id),
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
        print("✓ Created measurements table")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS stg_rejects (
                id SERIAL PRIMARY KEY,
                rejected_at TIMESTAMP DEFAULT NOW(),
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


def seed_persons():
    conn = get_db_connection()
    cur = conn.cursor()

    persons = [
        ('Justin', 'Teaneck, NJ', 40.8879, -74.0159),
        ('Emily', 'Suffern, NY', 41.1148, -74.1490),
        ('Melanie', 'Suffern, NY', 41.1148, -74.1490),
        ('Deshaun', 'Orange, NJ', 40.7707, -74.2323),
    ]

    try:
        for name, location, lat, lon in persons:
            cur.execute("""
                INSERT INTO persons (name, location_name, latitude, longitude)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (name) DO UPDATE SET
                    location_name = EXCLUDED.location_name,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude
            """, (name, location, lat, lon))
            print(f"✓ Added/updated person: {name} ({location})")

        conn.commit()
        print("\n✓ All persons seeded successfully!")

    except Exception as e:
        print(f"Error seeding persons: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    create_database()
    create_tables()
    seed_persons()
    print("\nDatabase setup complete!")
