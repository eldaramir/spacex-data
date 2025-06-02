import trino
from trino.dbapi import connect

queries = [
    {
        "title": "Q1: Launch Performance Over Time",
        "sql": """
            SELECT
              year(date_utc) AS launch_year,
              COUNT(*) AS total_launches,
              COUNT(CASE WHEN success THEN 1 ELSE NULL END) AS successful_launches,
              ROUND(
                100.0 * COUNT(CASE WHEN success THEN 1 ELSE NULL END) / COUNT(*), 2
              ) AS success_rate_pct
            FROM postgresql.public.launches_raw
            GROUP BY year(date_utc)
            ORDER BY launch_year
        """
    },
    {
        "title": "Q2: Top Payload Masses",
        "sql": """
            SELECT
              id,
              name,
              payload_mass_kg
            FROM postgresql.public.launches_raw
            ORDER BY payload_mass_kg DESC NULLS LAST
            LIMIT 5
        """
    },
    {
        "title": "Q3: Launch Delay Breakdown",
        "sql": """
             SELECT
              year(date_utc) AS launch_year,
              ROUND(AVG(to_unixtime(date_utc) - date_unix), 2) AS avg_delay_hours,
              ROUND(MAX(to_unixtime(date_utc) - date_unix), 2) AS max_delay_hours
            FROM postgresql.public.launches_raw
            GROUP BY year(date_utc)
            ORDER BY launch_year
        """
    },
    {
        "title": "Q4: Launch Site Utilization",
        "sql": """
            SELECT
              launchpad,
              COUNT(*) AS launch_count,
              ROUND(AVG(payload_mass_kg), 2) AS avg_payload_mass
            FROM postgresql.public.launches_raw
            GROUP BY launchpad
            ORDER BY launch_count DESC
        """
    }
]

def run_queries():
    conn = connect(
        host='localhost',
        port=8080,
        user='spacex',
        catalog='postgresql',
        schema='public',
    )
    cursor = conn.cursor()

    for q in queries:
        print(f"\n{q['title']}")
        print("-" * len(q['title']))
        cursor.execute(q["sql"])
        for row in cursor.fetchall():
            print(row)

if __name__ == "__main__":
    run_queries()