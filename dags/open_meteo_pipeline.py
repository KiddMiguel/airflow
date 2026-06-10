import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from open_meteo_pipeline.config import (
    DEFAULT_HOURLY_VARIABLES,
    PIPELINE_NAME,
    TASK_RETRIES,
    TASK_RETRY_DELAY_SECONDS,
    TASK_TIMEOUT_SECONDS,
    get_default_cities,
)
from open_meteo_pipeline.extract import extract_open_meteo_data
from open_meteo_pipeline.ingestion import write_ingestion_log, write_quality_anomalies
from open_meteo_pipeline.load import load_weather_rows
from open_meteo_pipeline.quality import evaluate_quality
from open_meteo_pipeline.runtime import get_runtime_params
from open_meteo_pipeline.transform import transform_weather_payload


LOGGER = logging.getLogger(__name__)
DEFAULT_TASK_OPTIONS = {
    "retries": TASK_RETRIES,
    "retry_delay": timedelta(seconds=TASK_RETRY_DELAY_SECONDS),
    "execution_timeout": timedelta(seconds=TASK_TIMEOUT_SECONDS),
}

## Definition du DAG Airflow pour le pipeline de traitement des donnees Open-Meteo.
with DAG(
    dag_id=PIPELINE_NAME,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    params={
        "cities": get_default_cities(),
        "hourly_variables": DEFAULT_HOURLY_VARIABLES,
    },
    tags=["open-meteo", "postgresql", "tp5"],
) as dag:
    ## Etape de recuperation des donnees Open-Meteo
    @task(task_id="extract_open_meteo", **DEFAULT_TASK_OPTIONS)
    def extract_open_meteo() -> list[dict]:
        LOGGER.info("Debut de l'extraction Open-Meteo")
        return extract_open_meteo_data(get_runtime_params())

    ## Etape de transformation des donnees
    @task(task_id="transform_weather_data", **DEFAULT_TASK_OPTIONS)
    def transform_weather_data(raw_file_manifests: list[dict]) -> list[dict]:
        LOGGER.info("Transformation des donnees pour %s villes", len(raw_file_manifests))
        return transform_weather_payload(raw_file_manifests, get_runtime_params())

    ## Etape de controle qualite
    @task(task_id="quality_check", **DEFAULT_TASK_OPTIONS)
    def quality_check(prepared_manifests: list[dict]) -> dict:
        LOGGER.info(
            "Controle qualite sur %s jeux de donnees prepares",
            len(prepared_manifests),
        )
        runtime_params = get_runtime_params()
        return evaluate_quality(prepared_manifests, runtime_params["run_id"])

    ## Etape de branchement conditionnel
    @task.branch(task_id="branch_on_quality", **DEFAULT_TASK_OPTIONS)
    def branch_on_quality(quality_result: dict) -> str:
        if quality_result["is_valid"]:
            LOGGER.info("Qualite validee, branchement vers le chargement PostgreSQL")
            return "load_weather_to_postgres"
        LOGGER.warning("Qualite invalide, branchement vers la journalisation des anomalies")
        return "log_quality_anomalies"

    ## Etape de chargement des donnees dans PostgreSQL
    @task(task_id="load_weather_to_postgres", **DEFAULT_TASK_OPTIONS)
    def load_weather_to_postgres(quality_result: dict) -> dict:
        return load_weather_rows(quality_result["valid_manifests"], quality_result["run_id"])

    ## Etape d'ecriture du suivi d'ingestion
    @task(task_id="log_ingestion", **DEFAULT_TASK_OPTIONS)
    def log_ingestion(load_result: dict) -> None:
        LOGGER.info(
            "Journalisation du chargement pour %s villes",
            len(load_result["loaded_manifests"]),
        )
        write_ingestion_log(load_result)

    ## Etape de trace des anomalies qualite
    @task(task_id="log_quality_anomalies", **DEFAULT_TASK_OPTIONS)
    def log_quality_anomalies(quality_result: dict) -> None:
        write_quality_anomalies(quality_result)

    ## Etape d'orchestration du pipeline
    extracted_data = extract_open_meteo()
    prepared_data = transform_weather_data(extracted_data)
    quality_result = quality_check(prepared_data)
    branch_result = branch_on_quality(quality_result)
    loaded_result = load_weather_to_postgres(quality_result)
    anomaly_result = log_quality_anomalies(quality_result)

    branch_result >> [loaded_result, anomaly_result]
    log_ingestion(loaded_result)
