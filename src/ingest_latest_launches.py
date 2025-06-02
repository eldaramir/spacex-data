import requests
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

def fetch_payload_mass(payload_ids):
    total_mass = 0.0
    for pid in payload_ids:
        r = requests.get(f"https://api.spacexdata.com/v4/payloads/{pid}")
        r.raise_for_status()
        payload = r.json()
        mass = payload.get("mass_kg") or 0
        total_mass += mass
    return total_mass

def launch_exists(conn, launch_id):
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM launches_raw WHERE id = %s", (launch_id,))
        return cur.fetchone() is not None

def insert_launch(conn, launch):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO launches_raw (id, name, date_utc, success, payload_mass_kg, launchpad, rocket, date_unix, inserted_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            launch["id"],
            launch["name"],
            launch["date_utc"],
            launch["success"],
            launch["payload_mass_kg"],
            launch["launchpad"],
            launch["rocket"],
            launch["date_unix"],
            datetime.utcnow()
        ))
    conn.commit()

def ingest_latest_launch(conn):
    print("Fetching latest launch...")
    launch = requests.get("https://api.spacexdata.com/v4/launches/latest").json()
    payload_mass = fetch_payload_mass(launch.get("payloads", []))
    launch["payload_mass_kg"] = payload_mass

    if launch_exists(conn, launch["id"]):
        print(f"Launch {launch['id']} already ingested.")
    else:
        insert_launch(conn, launch)
        print(f"Ingested latest launch: {launch['name']} ({launch['id']})")

def ingest_all_launches(conn):
    print("Fetching all launches...")
    launches = requests.get("https://api.spacexdata.com/v4/launches").json()
    ingested = 0
    for launch in launches:
        if not launch_exists(conn, launch["id"]):
            payload_mass = fetch_payload_mass(launch.get("payloads", []))
            launch["payload_mass_kg"] = payload_mass
            insert_launch(conn, launch)
            ingested += 1
    print(f"Ingested {ingested} new launches.")

def main():
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

    # Run once to backfill historical data
    ingest_all_launches(conn)

    # Use this for scheduled incremental ingestion
    #ingest_latest_launch(conn)

    conn.close()

if __name__ == "__main__":
    main()
