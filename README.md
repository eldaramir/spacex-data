SpaceX Data Pipeline Project

Overview

This project demonstrates a modular data pipeline that ingests the latest launch data from the SpaceX API, stores it in a PostgreSQL database, and exposes it for querying through Trino. It includes raw and aggregated tables, along with analytical SQL queries to derive business insights.

Tech Stack

Python
PostgreSQL
Trino
Docker Compose
DBeaver used for testing and checking the data is correct.

Setup Instructions
1. Clone the Repository

git clone https://github.com/eldaramir/spacex-data.git
cd spacex-data

2. Start Services with Docker Compose
cd docker 
docker-compose -f docker/docker-compose.yml up -d

3. Create Tables 
cd sql 
I used DBeaver from the task description I created the tables in pg sql syntax
or run the SQL in sql/create_tables.sql to create the launches_raw and launches_agg tables.

4. Set up Python Virtual Environment
python -m venv .venv
source .venv/bin/activate   # On Windows: .venv\Scripts\activate
pip install -r requirements.txt

Running the Pipeline
cd src
1. python src/ingest_latest_launches.py

2. python src/agg_launches.py

3. python src/queries_answers.py