import json
import logging

import requests

from open_meteo_pipeline.config import OPEN_METEO_BASE_URL, RAW_DATA_DIR


LOGGER = logging.getLogger(__name__)


# Etape d'extraction des donnees Open-Meteo
def extract_open_meteo_data(runtime_params: dict) -> list[dict]:
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

    extracted_files = []
    for city_config in runtime_params["cities"]:
        request_params = {
            "latitude": city_config["latitude"],
            "longitude": city_config["longitude"],
            "hourly": runtime_params["hourly_variables"],
            "timezone": "Europe/Paris",
            "forecast_days": 1,
        }
    
        LOGGER.info("Extraction Open-Meteo pour %s", city_config["city"])
        try:
            response = requests.get(OPEN_METEO_BASE_URL, params=request_params, timeout=30)
            response.raise_for_status()
        except requests.RequestException as exc:
            LOGGER.exception("Echec de l'appel Open-Meteo pour %s", city_config["city"])
            raise
        payload = response.json()
        payload["metadata"] = {
            "city": city_config["city"],
            "latitude": city_config["latitude"],
            "longitude": city_config["longitude"],
            "requested_hourly_variables": runtime_params["hourly_variables"],
            "run_id": runtime_params["run_id"],
        }

        safe_city = city_config["city"].lower().replace(" ", "_")
        raw_file_path = RAW_DATA_DIR / f"open_meteo_raw_{safe_city}_{runtime_params['ts_nodash']}.json"
        raw_file_path.write_text(
            json.dumps(payload, indent=2, ensure_ascii=True),
            encoding="utf-8",
        )
        extracted_files.append(
            {
                "city": city_config["city"],
                "latitude": city_config["latitude"],
                "longitude": city_config["longitude"],
                "raw_file_path": str(raw_file_path),
            }
        )

    return extracted_files
