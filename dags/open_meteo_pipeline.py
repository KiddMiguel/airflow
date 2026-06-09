import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

import psycopg2
import requests
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context


PIPELINE_NAME = "open_meteo_pipeline"
DEFAULT_CITY = os.getenv("PIPELINE_CITY", "Paris")
DEFAULT_LATITUDE = float(os.getenv("PIPELINE_LATITUDE", "48.8566"))
DEFAULT_LONGITUDE = float(os.getenv("PIPELINE_LONGITUDE", "2.3522"))
DEFAULT_HOURLY_VARIABLES = os.getenv(
    "PIPELINE_HOURLY_VARIABLES",
    "temperature_2m,relative_humidity_2m,apparent_temperature",
)
OPEN_METEO_BASE_URL = os.getenv(
    "OPEN_METEO_BASE_URL",
    "https://api.open-meteo.com/v1/forecast",
)
RAW_DATA_DIR = Path(os.getenv("RAW_DATA_DIR", "/opt/airflow/data/raw"))
PREPARED_DATA_DIR = Path(os.getenv("PREPARED_DATA_DIR", "/opt/airflow/data/prepared"))


def _get_runtime_params() -> dict[str, Any]:
    context = get_current_context()
    params = context["params"]
    return {
        "city": params["city"],
        "latitude": float(params["latitude"]),
        "longitude": float(params["longitude"]),
        "hourly_variables": params["hourly_variables"],
        "run_id": context["run_id"],
        "ts_nodash": context["ts_nodash"],
    }


def _build_postgres_connection():
    return psycopg2.connect(
        host=os.getenv("TARGET_POSTGRES_HOST", "postgres"),
        port=os.getenv("TARGET_POSTGRES_PORT", "5432"),
        dbname=os.getenv("TARGET_POSTGRES_DB", "meteo"),
        user=os.getenv("TARGET_POSTGRES_USER", "meteo"),
        password=os.getenv("TARGET_POSTGRES_PASSWORD", "meteo"),
    )


with DAG(
    dag_id=PIPELINE_NAME,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    params={
        "city": DEFAULT_CITY,
        "latitude": DEFAULT_LATITUDE,
        "longitude": DEFAULT_LONGITUDE,
        "hourly_variables": DEFAULT_HOURLY_VARIABLES,
    },
    tags=["open-meteo", "postgresql", "tp"],
) as dag:
    @task(task_id="extract_open_meteo")
    def extract_open_meteo() -> str:
        runtime_params = _get_runtime_params()
        RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

        request_params = {
            "latitude": runtime_params["latitude"],
            "longitude": runtime_params["longitude"],
            "hourly": runtime_params["hourly_variables"],
            "timezone": "Europe/Paris",
            "forecast_days": 1,
        }

        response = requests.get(
            OPEN_METEO_BASE_URL,
            params=request_params,
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        payload["metadata"] = {
            "city": runtime_params["city"],
            "requested_hourly_variables": runtime_params["hourly_variables"],
            "run_id": runtime_params["run_id"],
        }

        raw_file_path = RAW_DATA_DIR / f"open_meteo_raw_{runtime_params['ts_nodash']}.json"
        raw_file_path.write_text(
            json.dumps(payload, indent=2, ensure_ascii=True),
            encoding="utf-8",
        )
        return str(raw_file_path)

    @task(task_id="transform_weather_data")
    def transform_weather_data(raw_file_path: str) -> str:
        runtime_params = _get_runtime_params()
        PREPARED_DATA_DIR.mkdir(parents=True, exist_ok=True)

        payload = json.loads(Path(raw_file_path).read_text(encoding="utf-8"))
        hourly = payload.get("hourly", {})

        times = hourly.get("time", [])
        temperatures = hourly.get("temperature_2m", [])
        humidities = hourly.get("relative_humidity_2m", [])
        apparent_temperatures = hourly.get("apparent_temperature", [])

        prepared_rows = []
        for idx, forecast_time in enumerate(times):
            prepared_rows.append(
                {
                    "city": runtime_params["city"],
                    "latitude": runtime_params["latitude"],
                    "longitude": runtime_params["longitude"],
                    "forecast_time": forecast_time,
                    "temperature_2m": temperatures[idx] if idx < len(temperatures) else None,
                    "relative_humidity_2m": humidities[idx] if idx < len(humidities) else None,
                    "apparent_temperature": (
                        apparent_temperatures[idx]
                        if idx < len(apparent_temperatures)
                        else None
                    ),
                }
            )

        prepared_payload = {
            "pipeline_name": PIPELINE_NAME,
            "run_id": runtime_params["run_id"],
            "row_count": len(prepared_rows),
            "rows": prepared_rows,
        }

        prepared_file_path = (
            PREPARED_DATA_DIR / f"open_meteo_prepared_{runtime_params['ts_nodash']}.json"
        )
        prepared_file_path.write_text(
            json.dumps(prepared_payload, indent=2, ensure_ascii=True),
            encoding="utf-8",
        )
        return str(prepared_file_path)

    @task(task_id="load_weather_to_postgres")
    def load_weather_to_postgres(prepared_file_path: str) -> dict[str, Any]:
        prepared_payload = json.loads(Path(prepared_file_path).read_text(encoding="utf-8"))
        rows = prepared_payload["rows"]

        with _build_postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO weather_hourly (
                        city,
                        latitude,
                        longitude,
                        forecast_time,
                        temperature_2m,
                        relative_humidity_2m,
                        apparent_temperature
                    )
                    VALUES (%(city)s, %(latitude)s, %(longitude)s, %(forecast_time)s,
                            %(temperature_2m)s, %(relative_humidity_2m)s, %(apparent_temperature)s)
                    ON CONFLICT (city, latitude, longitude, forecast_time)
                    DO UPDATE SET
                        temperature_2m = EXCLUDED.temperature_2m,
                        relative_humidity_2m = EXCLUDED.relative_humidity_2m,
                        apparent_temperature = EXCLUDED.apparent_temperature,
                        ingested_at = CURRENT_TIMESTAMP
                    """,
                    rows,
                )
            connection.commit()

        return {
            "pipeline_name": prepared_payload["pipeline_name"],
            "run_id": prepared_payload["run_id"],
            "city": rows[0]["city"] if rows else DEFAULT_CITY,
            "records_loaded": len(rows),
            "status": "success",
            "message": f"{len(rows)} rows loaded into weather_hourly",
        }

    @task(task_id="log_ingestion")
    def log_ingestion(load_result: dict[str, Any]) -> None:
        with _build_postgres_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO ingestion_log (
                        pipeline_name,
                        run_id,
                        city,
                        records_loaded,
                        status,
                        message
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        load_result["pipeline_name"],
                        load_result["run_id"],
                        load_result["city"],
                        load_result["records_loaded"],
                        load_result["status"],
                        load_result["message"],
                    ),
                )
            connection.commit()

    raw_data = extract_open_meteo()
    prepared_data = transform_weather_data(raw_data)
    loaded_result = load_weather_to_postgres(prepared_data)
    log_ingestion(loaded_result)
