CREATE TABLE IF NOT EXISTS weather_hourly (
    id BIGSERIAL PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    latitude NUMERIC(8, 4) NOT NULL,
    longitude NUMERIC(8, 4) NOT NULL,
    forecast_time TIMESTAMP NOT NULL,
    temperature_2m NUMERIC(6, 2),
    relative_humidity_2m NUMERIC(6, 2),
    apparent_temperature NUMERIC(6, 2),
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT weather_hourly_unique_row
        UNIQUE (city, latitude, longitude, forecast_time)
);

CREATE INDEX IF NOT EXISTS idx_weather_hourly_forecast_time
    ON weather_hourly (forecast_time);

CREATE TABLE IF NOT EXISTS ingestion_log (
    id BIGSERIAL PRIMARY KEY,
    pipeline_name VARCHAR(150) NOT NULL,
    run_id VARCHAR(250) NOT NULL,
    city VARCHAR(100) NOT NULL,
    records_loaded INTEGER NOT NULL DEFAULT 0,
    status VARCHAR(50) NOT NULL,
    message TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
