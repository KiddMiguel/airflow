import os
from pathlib import Path


PIPELINE_NAME = "open_meteo_pipeline"
OPEN_METEO_BASE_URL = os.getenv("OPEN_METEO_BASE_URL", "https://api.open-meteo.com/v1/forecast")
RAW_DATA_DIR = Path(os.getenv("RAW_DATA_DIR", "/opt/airflow/data/raw"))
PREPARED_DATA_DIR = Path(os.getenv("PREPARED_DATA_DIR", "/opt/airflow/data/prepared"))

DEFAULT_CITY = os.getenv("PIPELINE_CITY", "Paris")
DEFAULT_LATITUDE = float(os.getenv("PIPELINE_LATITUDE", "48.8566"))
DEFAULT_LONGITUDE = float(os.getenv("PIPELINE_LONGITUDE", "2.3522"))
DEFAULT_HOURLY_VARIABLES = os.getenv(
    "PIPELINE_HOURLY_VARIABLES",
    "temperature_2m,relative_humidity_2m,apparent_temperature",
)
DEFAULT_CITIES_CONFIG = os.getenv("PIPELINE_CITIES", "")
QUALITY_FORCE_FAILURE_CITY = os.getenv("QUALITY_FORCE_FAILURE_CITY", "")
TASK_RETRIES = int(os.getenv("TASK_RETRIES", "2"))
TASK_RETRY_DELAY_SECONDS = int(os.getenv("TASK_RETRY_DELAY_SECONDS", "30"))
TASK_TIMEOUT_SECONDS = int(os.getenv("TASK_TIMEOUT_SECONDS", "120"))

TARGET_POSTGRES_HOST = os.getenv("TARGET_POSTGRES_HOST", "postgres")
TARGET_POSTGRES_PORT = os.getenv("TARGET_POSTGRES_PORT", "5432")
TARGET_POSTGRES_DB = os.getenv("TARGET_POSTGRES_DB", "meteo")
TARGET_POSTGRES_USER = os.getenv("TARGET_POSTGRES_USER", "meteo")
TARGET_POSTGRES_PASSWORD = os.getenv("TARGET_POSTGRES_PASSWORD", "meteo")


def get_default_cities() -> list[dict]:
    if not DEFAULT_CITIES_CONFIG:
        return [
            {
                "city": DEFAULT_CITY,
                "latitude": DEFAULT_LATITUDE,
                "longitude": DEFAULT_LONGITUDE,
            }
        ]

    cities = []
    for city_definition in DEFAULT_CITIES_CONFIG.split(";"):
        city_definition = city_definition.strip()
        if not city_definition:
            continue

        city, latitude, longitude = [part.strip() for part in city_definition.split("|")]
        cities.append(
            {
                "city": city,
                "latitude": float(latitude),
                "longitude": float(longitude),
            }
        )

    return cities
