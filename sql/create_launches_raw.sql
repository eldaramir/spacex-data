CREATE TABLE spacex.public.launches_raw (
    id VARCHAR,
    name VARCHAR,
    date_utc TIMESTAMP,
    success BOOLEAN,
    payload_mass_kg DOUBLE PRECISION,
    launchpad VARCHAR,
    rocket VARCHAR,
    date_unix BIGINT,
    inserted_at TIMESTAMP
);
