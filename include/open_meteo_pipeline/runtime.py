import ast
from typing import Any

from airflow.operators.python import get_current_context
from open_meteo_pipeline.config import get_default_cities


def _parse_city_string(city_definition: str) -> dict[str, Any]:
    city, latitude, longitude = [part.strip() for part in city_definition.split("|")]
    return {
        "city": city,
        "latitude": float(latitude),
        "longitude": float(longitude),
    }


def _normalize_cities(raw_cities: Any) -> list[dict[str, Any]]:
    if isinstance(raw_cities, str):
        raw_cities = raw_cities.strip()
        if not raw_cities:
            return get_default_cities()

        if "|" in raw_cities and ";" in raw_cities:
            return [
                _parse_city_string(city_definition)
                for city_definition in raw_cities.split(";")
                if city_definition.strip()
            ]

        try:
            raw_cities = ast.literal_eval(raw_cities)
        except (SyntaxError, ValueError) as exc:
            raise ValueError(f"Format invalide pour cities: {raw_cities}") from exc

    if isinstance(raw_cities, dict):
        raw_cities = [raw_cities]

    if not isinstance(raw_cities, list):
        raise TypeError(f"Le parametre cities doit etre une liste ou un dict, recu: {type(raw_cities)}")

    normalized_cities = []
    for city in raw_cities:
        if isinstance(city, str):
            if "|" in city:
                normalized_cities.append(_parse_city_string(city))
                continue

            try:
                city = ast.literal_eval(city)
            except (SyntaxError, ValueError) as exc:
                raise ValueError(f"Ville invalide dans cities: {city}") from exc

        if not isinstance(city, dict):
            raise TypeError(f"Chaque ville doit etre un dict, recu: {type(city)}")

        normalized_cities.append(
            {
                "city": city["city"],
                "latitude": float(city["latitude"]),
                "longitude": float(city["longitude"]),
            }
        )

    return normalized_cities


# Etape de lecture des parametres d'execution du pipeline
def get_runtime_params() -> dict[str, Any]:
    context = get_current_context()
    params = context["params"]
    cities = _normalize_cities(params.get("cities") or get_default_cities())

    return {
        "cities": cities,
        "hourly_variables": params["hourly_variables"],
        "run_id": context["run_id"],
        "ts_nodash": context["ts_nodash"],
    }
