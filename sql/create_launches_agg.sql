CREATE TABLE spacex.public.launches_agg (
    id SERIAL PRIMARY KEY,
    total_launches INTEGER,
    successful_launches INTEGER,
    avg_payload_mass DOUBLE PRECISION,
    avg_delay_hours DOUBLE PRECISION,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);