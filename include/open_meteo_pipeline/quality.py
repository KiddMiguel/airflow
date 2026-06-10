import json
import logging
from pathlib import Path

from open_meteo_pipeline.config import QUALITY_FORCE_FAILURE_CITY


LOGGER = logging.getLogger(__name__)


# Etape de controle qualite
def evaluate_quality(prepared_manifests: list[dict], run_id: str) -> dict:
    anomalies = []
    valid_manifests = []

    for prepared_manifest in prepared_manifests:
        prepared_payload = json.loads(
            Path(prepared_manifest["prepared_file_path"]).read_text(encoding="utf-8")
        )
        rows = prepared_payload.get("rows", [])
        city = prepared_manifest["city"]
        LOGGER.info("Controle qualite pour %s", city)

        if not rows:
            anomalies.append(
                {
                    "run_id": run_id,
                    "city": city,
                    "reason": "Aucune ligne preparee pour cette ville",
                }
            )
            LOGGER.warning("Controle qualite en echec pour %s : aucune ligne preparee", city)
            continue

        if QUALITY_FORCE_FAILURE_CITY and city.lower() == QUALITY_FORCE_FAILURE_CITY.lower():
            anomalies.append(
                {
                    "run_id": run_id,
                    "city": city,
                    "reason": "Anomalie qualite simulee via QUALITY_FORCE_FAILURE_CITY",
                }
            )
            LOGGER.warning("Controle qualite simule en echec pour %s", city)
            continue

        has_missing_values = any(
            row["temperature_2m"] is None
            or row["relative_humidity_2m"] is None
            or row["apparent_temperature"] is None
            for row in rows
        )
        if has_missing_values:
            anomalies.append(
                {
                    "run_id": run_id,
                    "city": city,
                    "reason": "Valeurs obligatoires manquantes dans les donnees preparees",
                }
            )
            LOGGER.warning("Controle qualite en echec pour %s : valeurs manquantes", city)
            continue

        valid_manifests.append(prepared_manifest)
        LOGGER.info("Controle qualite valide pour %s", city)

    return {
        "run_id": run_id,
        "is_valid": len(anomalies) == 0,
        "valid_manifests": valid_manifests,
        "anomalies": anomalies,
    }
