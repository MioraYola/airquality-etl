CREATE TABLE IF NOT EXISTS dim_city (
    city_id SERIAL PRIMARY KEY,
    ville VARCHAR(100) NOT NULL,
    pays VARCHAR(10) NOT NULL,
    latitude NUMERIC(10, 6),
    longitude NUMERIC(10, 6),
    UNIQUE (ville, pays)
);

CREATE TABLE IF NOT EXISTS dim_datetime (
    datetime_id SERIAL PRIMARY KEY,
    timestamp_utc TIMESTAMPTZ NOT NULL,
    date_only DATE,
    hour INT,
    day INT,
    month INT,
    year INT,
    weekday VARCHAR(20),
    UNIQUE (timestamp_utc)
);

CREATE TABLE IF NOT EXISTS fact_air_quality (
    air_quality_id SERIAL PRIMARY KEY,

    city_id INT NOT NULL REFERENCES dim_city(city_id),
    datetime_id INT NOT NULL REFERENCES dim_datetime(datetime_id),

    aqi INT,
    co NUMERIC(12, 4),
    no NUMERIC(12, 4),
    no2 NUMERIC(12, 4),
    o3 NUMERIC(12, 4),
    so2 NUMERIC(12, 4),
    pm2_5 NUMERIC(12, 4),
    pm10 NUMERIC(12, 4),
    nh3 NUMERIC(12, 4),

    UNIQUE (city_id, datetime_id)
);