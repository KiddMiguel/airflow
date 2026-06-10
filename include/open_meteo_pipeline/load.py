import json
import logging
from pathlib import Path
from typing import Any

from open_meteo_pipeline.config import PIPELINE_NAME
from open_meteo_pipeline.storage import build_postgres_connection


LOGGER = logging.getLogger(__name__)


# Etape de chargement des donnees dans PostgreSQL
def load_weather_rows(valid_manifests: list[dict], run_id: str) -> dict[str, Any]:
    if not valid_manifests:
        raise ValueError("Aucun manifeste valide a charger dans PostgreSQL")

    total_rows = 0
    loaded_cities = []
    with build_postgres_connection() as connection:
        with connection.cursor() as cursor:
            for manifest in valid_manifests:
                LOGGER.info("Chargement PostgreSQL pour %s", manifest["city"])
                prepared_payload = json.loads(
                    Path(manifest["prepared_file_path"]).read_text(encoding="utf-8")
                )
                rows = prepared_payload["rows"]
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
                total_rows += len(rows)
                loaded_cities.append(manifest["city"])
        connection.commit()
    LOGGER.info("%s lignes chargees pour %s", total_rows, ", ".join(loaded_cities))

    return {
        "pipeline_name": PIPELINE_NAME,
        "run_id": run_id,
        "city": ", ".join(loaded_cities),
        "records_loaded": total_rows,
        "status": "success",
        "message": f"{total_rows} rows loaded into weather_hourly",
        "loaded_manifests": valid_manifests,
    }
