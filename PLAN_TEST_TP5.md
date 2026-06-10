# Plan de test - TP 5 Industrialisation du pipeline Airflow Open-Meteo

## Objectif

Ce document sert de feuille de route pour tester le pipeline industrialise et produire les preuves demandees dans le TP 5.

Les 3 cas a demontrer sont :

1. cas nominal ;
2. cas d'anomalie qualite ;
3. cas de relance sans doublon.

## Prerequis

- Docker Desktop demarre
- conteneurs `airflow` et `postgres` disponibles
- Airflow accessible sur `http://localhost:8080`
- DAG `open_meteo_pipeline` visible

## Variables utiles

Verifier dans `.env` :

- `PIPELINE_CITIES`
- `PIPELINE_HOURLY_VARIABLES`
- `QUALITY_FORCE_FAILURE_CITY`
- `TASK_RETRIES`
- `TASK_RETRY_DELAY_SECONDS`
- `TASK_TIMEOUT_SECONDS`

Valeur recommandee pour le test nominal :

```env
QUALITY_FORCE_FAILURE_CITY=
```

Valeur recommandee pour le test anomalie :

```env
QUALITY_FORCE_FAILURE_CITY=Paris
```

## Preparation de la base PostgreSQL

Si la base a deja ete creee avant l'ajout de `quality_anomalies`, executer le script SQL manuellement :

```powershell
docker compose exec postgres psql -U meteo -d meteo -f /docker-entrypoint-initdb.d/create_tables.sql
```

Verifier les tables :

```powershell
docker compose exec postgres psql -U meteo -d meteo -c "\dt"
```

Visuel attendu :

- presence de `weather_hourly`
- presence de `ingestion_log`
- presence de `quality_anomalies`

## Redemarrage de l'environnement si besoin

Si le code ou le `.env` a change :

```powershell
docker compose down
docker compose up -d
```

Verifier les conteneurs :

```powershell
docker compose ps
```

Visuel attendu :

- service `airflow` en cours d'execution
- service `postgres` en cours d'execution

## Cas 1 - Execution nominale

### Configuration

Dans `.env` :

```env
QUALITY_FORCE_FAILURE_CITY=
```

Si tu modifies `.env`, relancer :

```powershell
docker compose down
docker compose up -d
```

### Lancement

Depuis Airflow :

1. ouvrir le DAG `open_meteo_pipeline`
2. lancer un run manuel

### Visuels attendus dans Airflow

- `extract_open_meteo` en vert
- `transform_weather_data` en vert
- `quality_check` en vert
- `branch_on_quality` en vert
- `load_weather_to_postgres` en vert
- `log_ingestion` en vert
- `log_quality_anomalies` en `skipped`

### Verifications PostgreSQL

Verifier le contenu de `weather_hourly` :

```powershell
docker compose exec postgres psql -U meteo -d meteo -c "SELECT COUNT(*) AS total_rows FROM weather_hourly;"
```

Verifier un apercu des donnees :

```powershell
docker compose exec postgres psql -U meteo -d meteo -c "SELECT city, forecast_time, temperature_2m, relative_humidity_2m, apparent_temperature FROM weather_hourly ORDER BY forecast_time LIMIT 10;"
```

Verifier le suivi d'ingestion :

```powershell
docker compose exec postgres psql -U meteo -d meteo -c "SELECT run_id, city, records_loaded, status, message, created_at FROM ingestion_log ORDER BY created_at DESC LIMIT 10;"
```

## Cas 2 - Anomalie qualite

### Configuration

Dans `.env`, forcer une anomalie pour une ville :

```env
QUALITY_FORCE_FAILURE_CITY=Paris
```

Puis relancer les services :

```powershell
docker compose down
docker compose up -d
```

### Lancement

Depuis Airflow :

1. lancer un nouveau run manuel du DAG

### Visuels attendus dans Airflow

- `extract_open_meteo` en vert
- `transform_weather_data` en vert
- `quality_check` en vert
- `branch_on_quality` en vert
- `log_quality_anomalies` en vert
- `load_weather_to_postgres` en `skipped`
- `log_ingestion` en `skipped` ou non execute sur le chemin succes

### Verifications PostgreSQL

Verifier les anomalies :

```powershell
docker compose exec postgres psql -U meteo -d meteo -c "SELECT run_id, city, reason, created_at FROM quality_anomalies ORDER BY created_at DESC LIMIT 10;"
```

Verifier la trace dans `ingestion_log` :

```powershell
docker compose exec postgres psql -U meteo -d meteo -c "SELECT run_id, city, records_loaded, status, message, created_at FROM ingestion_log WHERE status = 'quality_failed' ORDER BY created_at DESC LIMIT 10;"
```

Verifier que le volume de donnees chargees n'a pas augmente pour ce run :

```powershell
docker compose exec postgres psql -U meteo -d meteo -c "SELECT COUNT(*) AS total_rows FROM weather_hourly;"
```

## Cas 3 - Relance sans doublon

### Configuration

Revenir en mode nominal :

```env
QUALITY_FORCE_FAILURE_CITY=
```

Puis relancer les services :

```powershell
docker compose down
docker compose up -d
```

### Lancement

1. lancer un run manuel nominal
2. noter le nombre de lignes dans `weather_hourly`
3. relancer le DAG une deuxieme fois avec les memes villes

### Verifications PostgreSQL

Compter les lignes apres le premier run :

```powershell
docker compose exec postgres psql -U meteo -d meteo -c "SELECT COUNT(*) AS total_rows FROM weather_hourly;"
```

Relancer le DAG, puis recompter :

```powershell
docker compose exec postgres psql -U meteo -d meteo -c "SELECT COUNT(*) AS total_rows FROM weather_hourly;"
```

Verifier l'unicite logique :

```powershell
docker compose exec postgres psql -U meteo -d meteo -c "SELECT city, latitude, longitude, forecast_time, COUNT(*) FROM weather_hourly GROUP BY city, latitude, longitude, forecast_time HAVING COUNT(*) > 1;"
```

Visuel attendu :

- le nombre total de lignes reste stable si les memes donnees sont rechargees
- la requete `HAVING COUNT(*) > 1` ne retourne aucune ligne

