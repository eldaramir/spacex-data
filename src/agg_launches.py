import psycopg2
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", 5432)
DB_NAME = os.getenv("DB_NAME", "spacex")
DB_USER = os.getenv("DB_USER", "spacex")
DB_PASS = os.getenv("DB_PASS", "spacex")

def calculate_aggregates(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE success IS TRUE) AS successful,
                AVG(payload_mass_kg) AS avg_mass,
                AVG(EXTRACT(EPOCH FROM date_utc) - date_unix) / 3600 AS avg_delay
            FROM launches_raw
        """)
        result = cur.fetchone()
        return {
            "total": result[0],
            "successful": result[1],
            "avg_mass": result[2] or 0,
            "avg_delay": result[3] or 0
        }

def upsert_aggregation(conn, agg):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO launches_agg (
                id, total_launches, successful_launches, avg_payload_mass, avg_delay_hours, last_updated
            )
            VALUES (
                1, %s, %s, %s, %s, %s
            )
            ON CONFLICT (id) DO UPDATE SET
                total_launches = EXCLUDED.total_launches,
                successful_launches = EXCLUDED.successful_launches,
                avg_payload_mass = EXCLUDED.avg_payload_mass,
                avg_delay_hours = EXCLUDED.avg_delay_hours,
                last_updated = EXCLUDED.last_updated;
        """, (
            agg["total"],
            agg["successful"],
            agg["avg_mass"],
            agg["avg_delay"],
            datetime.utcnow()
        ))
    conn.commit()

def main():
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

    print("Calculating launch aggregates...")
    agg = calculate_aggregates(conn)
    print(f"Inserting: {agg}")
    upsert_aggregation(conn, agg)

    conn.close()

if __name__ == "__main__":
    main()
