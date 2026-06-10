import json
import logging
from pathlib import Path

from open_meteo_pipeline.config import PIPELINE_NAME, PREPARED_DATA_DIR


LOGGER = logging.getLogger(__name__)


# Etape de transformation des donnees
def transform_weather_payload(raw_file_manifests: list[dict], runtime_params: dict) -> list[dict]:
    PREPARED_DATA_DIR.mkdir(parents=True, exist_ok=True)

    prepared_manifests = []
    for raw_manifest in raw_file_manifests:
        LOGGER.info("Transformation des donnees pour %s", raw_manifest["city"])
        payload = json.loads(Path(raw_manifest["raw_file_path"]).read_text(encoding="utf-8"))
        hourly = payload.get("hourly", {})

        times = hourly.get("time", [])
        temperatures = hourly.get("temperature_2m", [])
        humidities = hourly.get("relative_humidity_2m", [])
        apparent_temperatures = hourly.get("apparent_temperature", [])

        prepared_rows = []
        for idx, forecast_time in enumerate(times):
            prepared_rows.append(
                {
                    "city": raw_manifest["city"],
                    "latitude": raw_manifest["latitude"],
                    "longitude": raw_manifest["longitude"],
                    "forecast_time": forecast_time,
                    "temperature_2m": temperatures[idx] if idx < len(temperatures) else None,
                    "relative_humidity_2m": humidities[idx] if idx < len(humidities) else None,
                    "apparent_temperature": (
                        apparent_temperatures[idx] if idx < len(apparent_temperatures) else None
                    ),
                }
            )

        prepared_payload = {
            "pipeline_name": PIPELINE_NAME,
            "run_id": runtime_params["run_id"],
            "city": raw_manifest["city"],
            "row_count": len(prepared_rows),
            "rows": prepared_rows,
        }

        safe_city = raw_manifest["city"].lower().replace(" ", "_")
        prepared_file_path = (
            PREPARED_DATA_DIR / f"open_meteo_prepared_{safe_city}_{runtime_params['ts_nodash']}.json"
        )
        prepared_file_path.write_text(
            json.dumps(prepared_payload, indent=2, ensure_ascii=True),
            encoding="utf-8",
        )
        prepared_manifests.append(
            {
                "city": raw_manifest["city"],
                "latitude": raw_manifest["latitude"],
                "longitude": raw_manifest["longitude"],
                "prepared_file_path": str(prepared_file_path),
            }
        )
        LOGGER.info(
            "%s lignes preparees pour %s",
            len(prepared_rows),
            raw_manifest["city"],
        )

    return prepared_manifests
