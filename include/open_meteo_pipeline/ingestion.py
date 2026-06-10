import logging
from typing import Any

from open_meteo_pipeline.storage import build_postgres_connection


LOGGER = logging.getLogger(__name__)


# Etape d'ecriture du suivi d'ingestion
def write_ingestion_log(load_result: dict[str, Any]) -> None:
    LOGGER.info(
        "Ecriture du suivi d'ingestion pour %s avec %s lignes",
        load_result["city"],
        load_result["records_loaded"],
    )
    with build_postgres_connection() as connection:
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


def write_quality_anomalies(quality_result: dict[str, Any]) -> None:
    LOGGER.warning(
        "Ecriture de %s anomalies qualite pour le run %s",
        len(quality_result["anomalies"]),
        quality_result["run_id"],
    )
    with build_postgres_connection() as connection:
        with connection.cursor() as cursor:
            for anomaly in quality_result["anomalies"]:
                cursor.execute(
                    """
                    INSERT INTO quality_anomalies (
                        pipeline_name,
                        run_id,
                        city,
                        reason
                    )
                    VALUES (%s, %s, %s, %s)
                    """,
                    (
                        "open_meteo_pipeline",
                        anomaly["run_id"],
                        anomaly["city"],
                        anomaly["reason"],
                    ),
                )
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
                        "open_meteo_pipeline",
                        anomaly["run_id"],
                        anomaly["city"],
                        0,
                        "quality_failed",
                        anomaly["reason"],
                    ),
                )
        connection.commit()
