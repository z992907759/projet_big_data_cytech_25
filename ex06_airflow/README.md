L'objectif de cette partie additionnel est de pouvoir proposer un système entièrement automatisé avec Airflow.
1. Pour cela, nous devez ajouter un nouveau service dans le docker-compose nommé Airflow
2. Ensuite vous devez concevoir votre DAG afin d'automatiser sous la forme de pipelines l'ensemble des exercices demandés

## Structure EX6
- Dossier Airflow de l'exercice: `ex06_airflow/airflow/`
- DAG principal: `ex06_airflow/airflow/dags/ex6_pipeline.py`
- Compose: `docker-compose.airflow.yml`

## Lancement
Depuis la racine du projet:

```bash
export PROJECT_DIR="$(pwd)"
export AIRFLOW_DOCKER_NETWORK="bigdata_projet_spark-network"
docker compose -f docker-compose.airflow.yml up -d
```

UI Airflow:
- URL: `http://localhost:8080`
- User: `admin`
- Password: `admin`

## Execution du pipeline EX6
```bash
docker exec airflow-scheduler airflow dags trigger ex6_pipeline
```

Suivi:
```bash
docker exec airflow-scheduler airflow dags list-runs -d ex6_pipeline -o table
docker exec airflow-scheduler airflow tasks states-for-dag-run ex6_pipeline <run_id>
```

## Ordre du DAG
`EX1 -> EX2(Q1) -> EX3(Q1) -> EX2(Q2) -> EX3(Q2) -> EX4 -> EX5`

