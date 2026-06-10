import psycopg2

from open_meteo_pipeline.config import (
    TARGET_POSTGRES_DB,
    TARGET_POSTGRES_HOST,
    TARGET_POSTGRES_PASSWORD,
    TARGET_POSTGRES_PORT,
    TARGET_POSTGRES_USER,
)

## Cette fonction est un utilitaire pour etablir une connexion a la base de donnees PostgreSQL 
## cible en utilisant les parametres de configuration definis dans config.py.
def build_postgres_connection():
    return psycopg2.connect(
        host=TARGET_POSTGRES_HOST,
        port=TARGET_POSTGRES_PORT,
        dbname=TARGET_POSTGRES_DB,
        user=TARGET_POSTGRES_USER,
        password=TARGET_POSTGRES_PASSWORD,
    )
