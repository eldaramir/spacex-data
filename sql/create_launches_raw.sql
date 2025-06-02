CREATE TABLE postgresql.public.launches_raw (
    id VARCHAR,
    name VARCHAR,
    date_utc TIMESTAMP,
    success BOOLEAN,
    payload_mass_kg DOUBLE,
    launchpad VARCHAR,
    rocket VARCHAR,
    date_unix BIGINT,
    inserted_at TIMESTAMP
);
