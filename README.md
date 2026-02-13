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
#### Il faudra pour le lancement manuel créer 2 buckets à nommer "nyc-raw" et "nyc-cleaned".

## 2) Lancer EX1-EX5 manuellement (via la branche manual-version, ```git clone -b manual-version https://github.com/z992907759/BigData_projet.git```)

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
Créé avec le docker-compose.

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
streamlit run streamlit_app/app.py
```
Test de qualité code :
```bash
flake8 --max-line-length=100 src/

export MINIO_BUCKET_CLEAN="nyc-cleaned"
export YEAR="2024"
export MONTH="01"
python -m pytest tests/test_pipeline.py
```

EX4:
```bash
cd ../ex04_dashboard
# dashboard Streamlit (EX4)
streamlit run src/dashboard.py
```

FIN (depuis le dossier exercice 5) :
```bash
deactivate
rm -rf .venv/
cd ..
docker-compose down -v
```

## 3) EX6 - Pipeline automatise avec Airflow (recommande)

Le fichier `docker-compose.airflow.yml` orchestre MinIO + Airflow.
Les buckets MinIO sont crees automatiquement (plus besoin de création manuelle).
Les fichiers Airflow de l'exercice sont dans `ex06_airflow/airflow/` (DAGs, logs, plugins).

Pre-requis important:
```bash
# demarrer d'abord la stack de base (cree le reseau spark-network + postgres-db)
docker compose up -d
```

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

Declenchement recommande (UI):
- Ouvrir `http://localhost:8080`
- Activer le DAG `ex6_pipeline`
- Cliquer `Trigger DAG`
- Suivre le statut des taches dans `Grid` / `Graph`

Declenchement du DAG EX6 (CLI, optionnel):
```bash
docker compose -f docker-compose.airflow.yml exec airflow-scheduler airflow dags trigger ex6_pipeline
```

Suivi d'un run (CLI, optionnel):
```bash
docker compose -f docker-compose.airflow.yml exec airflow-scheduler airflow dags list-runs -d ex6_pipeline -o table
docker compose -f docker-compose.airflow.yml exec airflow-scheduler airflow tasks states-for-dag-run ex6_pipeline <run_id>
```

## Notes d'execution EX6
- Le DAG `ex6_pipeline` execute EX1 -> EX2(Q1) -> EX3(Q1) -> EX2(Q2) -> EX3(Q2) -> EX4 -> EX5.
- La configuration par defaut est stabilisee pour un run reproductible en environnement local (mois `01`).
- Les commandes sont overrideables via variables d'environnement (`EX1_CMD`, `EX2_Q1_CMD`, `EX2_Q2_CMD`, etc.).
- EX4 dans ce repo est base sur `ex04_dashboard/src/dashboard.py`; la generation PDF est disponible via `ex04_dashboard/src/create_pdf.py`.

## Verification rapide DWH
```bash
docker exec -it postgres-db psql -U myuser -d taxidb -c "SELECT COUNT(*) FROM dwh.fact_trip;"
```

## Nettoyage
```bash
docker compose down -v
docker compose -f docker-compose.airflow.yml down -v
```
