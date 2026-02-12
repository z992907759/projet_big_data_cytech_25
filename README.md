# NYC Taxi Data Pipeline & ML Prediction

Projet Big Data de bout en bout sur les courses de taxi NYC:
- EX1: collecte des parquet bruts
- EX2: nettoyage + ingestion DWH
- EX3: schema SQL (creation + insertion reference)
- EX4: dashboard
- EX5: entrainement + prediction ML
- EX6: orchestration complete via Airflow

## Architecture
- Data Lake: MinIO (`nyc-raw`, `nyc-cleaned`)
- ETL: Spark + Scala
- DWH: PostgreSQL (schema `dwh`)
- Viz: Streamlit
- ML: scikit-learn
- Orchestration: Airflow 2.9

## Prerequis
- Docker / Docker Compose
- Java 11
- sbt
- uv (pour EX5 en local)

## 1) Lancer l'infra de base (MinIO + Postgres metier)
```bash
docker compose up -d --build
docker ps
```

Acces MinIO:
- API: `http://localhost:9000`
- Console: `http://localhost:9001`
- Credentials par defaut: `minio` / `minio123`

## 2) Lancer EX1-EX5 manuellement (optionnel)

EX1:
```bash
cd ex01_data_retrieval
sbt run
```

EX2:
```bash
cd ../ex02_data_ingestion
sbt run
```

EX3:
```bash
cd ..
docker exec -i postgres-db psql -U myuser -d taxidb < ex03_sql_table_creation/creation.sql
docker exec -i postgres-db psql -U myuser -d taxidb < ex03_sql_table_creation/insertion.sql
```

EX4:
```bash
cd ex04_dashboard
python src/dashboard.py
```

EX5 (local):
```bash
cd ../ex05_ml_prediction_service
uv venv .venv --python 3.11
source .venv/bin/activate
uv pip install -r requirements.txt
export MINIO_ENDPOINT="http://localhost:9000"
export MINIO_ACCESS_KEY="minio"
export MINIO_SECRET_KEY="minio123"
python -m src.train
python -m src.predict "{}"
```

## 3) EX6 - Pipeline automatise avec Airflow (recommande)

Le fichier `docker-compose.airflow.yml` orchestre MinIO + Airflow.
Les buckets MinIO sont crees automatiquement (plus besoin de creation manuelle).

Depuis la racine du repo:
```bash
export PROJECT_DIR="$(pwd)"
export AIRFLOW_DOCKER_NETWORK="bigdata_projet_spark-network"
docker compose -f docker-compose.airflow.yml up -d
```

UI Airflow:
- `http://localhost:8080`
- login: `admin`
- password: `admin`

Declenchement du DAG EX6:
```bash
docker exec airflow-scheduler airflow dags trigger ex6_pipeline
```

Suivi d'un run:
```bash
docker exec airflow-scheduler airflow dags list-runs -d ex6_pipeline -o table
docker exec airflow-scheduler airflow tasks states-for-dag-run ex6_pipeline <run_id>
```

## Notes d'execution EX6
- Le DAG `ex6_pipeline` execute EX1 -> EX2(Q1) -> EX3(Q1) -> EX2(Q2) -> EX3(Q2) -> EX4 -> EX5.
- La configuration par defaut est stabilisee pour un run reproductible en environnement local (mois `01`).
- Les commandes sont overrideables via variables d'environnement (`EX1_CMD`, `EX2_Q1_CMD`, `EX2_Q2_CMD`, etc.).

## Verification rapide DWH
```bash
docker exec -it postgres-db psql -U myuser -d taxidb -c "SELECT COUNT(*) FROM dwh.fact_trip;"
```

## Nettoyage
```bash
docker compose down -v
docker compose -f docker-compose.airflow.yml down -v
```
