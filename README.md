# NYC Taxi Data Pipeline & ML Prediction

Ce projet implante une architecture Big Data complète pour le traitement, le stockage et l'analyse prédictive des données de taxis new-yorkais. Il couvre l'ensemble du cycle de vie de la donnée : de la collecte brute au dashboard de visualisation, en passant par un Data Warehouse et un modèle de Machine Learning performant.

## 🚀 Architecture du Projet

* **Data Lake (MinIO)** : Stockage des données brutes (Bronze) et nettoyées (Silver).
* **ETL (Apache Spark / Scala)** : Nettoyage, filtrage et ingestion massive.
* **Data Warehouse (PostgreSQL)** : Modélisation en étoile (Star Schema) pour l'analyse.
* **Machine Learning (Scikit-Learn)** : Prédiction du montant total des courses.
* **Visualisation (Streamlit)** : Démonstration de l'utilisation du modèle

---

## Prérequis
* Java 11 Eclipse Temurin
* sbt via sdkman par exemple
* uv téléchargeable avec la commande ```curl -LsSf https://astral.sh/uv/install.sh | sh```
* Docker

---

## 🛠️ Guide d'installation et de lancement

### 1. Infrastructure (Docker)

Lancez les services MinIO et PostgreSQL :

```bash
docker-compose up -d --build
# Vérifiez que les containers sont "Up"
docker ps
```
Si la commande ```docker-compose``` ne fonctionne pas utiliser ```docker compose``` sans le -.

Accédez à l'interface MinIO [http://localhost:9001](http://localhost:9001/login) avec, pour notre exemple, comme identifiant "minio" pour mot de passe "minio123" et créez manuellement les buckets :
- "nyc-raw"
- "nyc-cleaned"

### 2. Collecte et Ingestion (Scala/Spark)
#### Étape 1 : Récupération des données brutes

```bash
cd ex01_data_retrieval
sbt run
```

#### Étape 2 : Nettoyage et Ingestion (DWH)

```bash
cd ../ex02_data_ingestion
sbt run
```

Vérification du volume :
```bash
docker exec -it postgres-db psql -U myuser -d taxidb -c "SELECT count(*) FROM dwh.fact_trip;"
```

### 3. Machine Learning & Qualité (Python)
#### Préparez l'environnement et entraînez le modèle :

```bash
cd ../ex05_ml_prediction_service
uv venv .venv --python 3.11
source .venv/bin/activate
uv pip install -r requirements.txt
```

#### Configuration des accès MinIO
```bash
export MINIO_ENDPOINT="http://localhost:9000"
export MINIO_ACCESS_KEY="minio"
export MINIO_SECRET_KEY="minio123"
```

#### Entraînement
```bash
python -m src.train

```
#### Vérifications Qualité :
Linting (PEP 8)
```bash
flake8 --max-line-length=100 src/
```

#### Tests Unitaires
```bash
export MINIO_BUCKET_CLEAN="nyc-cleaned"
export YEAR="2024"
export MONTH="01"
python -m pytest tests/test_pipeline.py
```

### 4. Démonstration

Lancez l'application Streamlit :

```bash
streamlit run streamlit_app/app.py
```

---

## 📈 Résultats et Performances

* Volume de données : Ingestion réussie de plusieurs millions de lignes dans le Data Warehouse PostgreSQL.

* Précision du Modèle : RMSE de 4.25 obtenu sur la prédiction du total_amount (Cible : < 10).

* Modèle utilisé : HistGradientBoostingRegressor (robuste aux grands volumes tabulaires).

* Qualité : Documentation au format NumpyDoc et conformité PEP 8.

---

## 🧹 Nettoyage
Pour arrêter les services et supprimer les données persistantes (volumes) :

```bash
deactivate
rm -rf .venv/
cd ..
docker-compose down -v
```
