from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

PROJECT_DIR = os.environ.get("PROJECT_DIR", "/opt/project")
TARGET_PATH = "/workspace"

DOCKER_NETWORK = os.environ.get("AIRFLOW_DOCKER_NETWORK") or "bigdata_projet_spark-network"

PGHOST = os.environ.get("PGHOST", "postgres-db")
PGPORT = os.environ.get("PGPORT", "5432")
PGDATABASE = os.environ.get("PGDATABASE", "taxidb")
PGUSER = os.environ.get("PGUSER", "myuser")
PGPASSWORD = os.environ.get("PGPASSWORD", "mypassword")

SBT_JAVA11_IMAGE = os.environ.get("SBT_JAVA11_IMAGE", "mdsol/sbt-java11-jdk:2026")

EX1_CMD = os.environ.get(
    "EX1_CMD",
    f"bash -lc 'cd {TARGET_PATH}/ex01_data_retrieval && sbt -no-colors run'",
)

EX2_Q1_CMD = os.environ.get(
    "EX2_Q1_CMD",
    f"bash -lc 'cd {TARGET_PATH}/ex02_data_ingestion && SBT_OPTS=\"-Xms1g -Xmx4g -XX:+UseG1GC\" EX2_MODE=q1 MONTHS=01 EX2_MAX_ROWS=300000 sbt -no-colors run'",
)

EX3_Q1_CMD = os.environ.get(
    "EX3_Q1_CMD",
    "bash -lc "
    f"'export PGPASSWORD=\"{PGPASSWORD}\"; "
    f"psql -h \"{PGHOST}\" -p \"{PGPORT}\" -U \"{PGUSER}\" -d \"{PGDATABASE}\" "
    f"-f {TARGET_PATH}/ex03_sql_table_creation/creation.sql'",
)

EX2_Q2_CMD = os.environ.get(
    "EX2_Q2_CMD",
    f"bash -lc 'cd {TARGET_PATH}/ex02_data_ingestion && SBT_OPTS=\"-Xms1g -Xmx4g -XX:+UseG1GC\" EX2_MODE=q2 MONTHS=01 EX2_MAX_ROWS=300000 sbt -no-colors run'",
)

EX3_Q2_CMD = os.environ.get(
    "EX3_Q2_CMD",
    "bash -lc "
    f"'export PGPASSWORD=\"{PGPASSWORD}\"; "
    f"psql -h \"{PGHOST}\" -p \"{PGPORT}\" -U \"{PGUSER}\" -d \"{PGDATABASE}\" "
    f"-f {TARGET_PATH}/ex03_sql_table_creation/insertion.sql'",
)

EX4_CMD = os.environ.get(
    "EX4_CMD",
    "bash -lc "
    f"'cd {TARGET_PATH}/ex04_dashboard "
    "&& python -V "
    "&& pip install --no-cache-dir pandas streamlit sqlalchemy psycopg2-binary plotly matplotlib "
    f"&& export PG_HOST=\"{PGHOST}\" PG_PORT=\"{PGPORT}\" PG_DB=\"{PGDATABASE}\" PG_USER=\"{PGUSER}\" PG_PASS=\"{PGPASSWORD}\" "
    "&& python src/dashboard.py'",
)

EX5_CMD = os.environ.get(
    "EX5_CMD",
    "bash -lc "
    f"'cd {TARGET_PATH}/ex05_ml_prediction_service "
    "&& python -m pip install --no-cache-dir uv "
    "&& uv pip install --system --no-cache -r requirements.txt "
    "&& export MINIO_ENDPOINT=\"http://minio:9000\" MINIO_ACCESS_KEY=\"minio\" MINIO_SECRET_KEY=\"minio123\" MINIO_BUCKET=\"nyc-cleaned\" EX5_SOURCE_PREFIX=\"year=2024/month=01/\" "
    "&& EX5_SELECTED_PREFIX=$(uv run python - <<\"PY\"\n"
    "import os, s3fs\n"
    "bucket = os.environ[\"MINIO_BUCKET\"]\n"
    "prefix = os.environ[\"EX5_SOURCE_PREFIX\"]\n"
    "fs = s3fs.S3FileSystem(\n"
    "    key=os.environ[\"MINIO_ACCESS_KEY\"],\n"
    "    secret=os.environ[\"MINIO_SECRET_KEY\"],\n"
    "    client_kwargs={\"endpoint_url\": os.environ[\"MINIO_ENDPOINT\"]},\n"
    ")\n"
    "keys = [k for k in fs.find(f\"{bucket}/{prefix}\") if k.endswith(\".parquet\")]\n"
    "if not keys:\n"
    "    raise SystemExit(f\"No parquet found under {bucket}/{prefix}\")\n"
    "largest_key = max(keys, key=lambda k: fs.info(k).get(\"size\", 0))\n"
    "print(largest_key.split(f\"{bucket}/\", 1)[1])\n"
    "PY\n"
    ") "
    "&& export MINIO_PREFIX=\"$EX5_SELECTED_PREFIX\" "
    "&& uv run python -m src.train "
    "&& uv run python -m src.predict \"{}\"'",
)


def docker_task(task_id: str, image: str, command: str) -> DockerOperator:
    return DockerOperator(
        task_id=task_id,
        image=image,
        auto_remove=True,
        docker_url="unix:///var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        mounts=[
            Mount(
                source=PROJECT_DIR,
                target=TARGET_PATH,
                type="bind",
            )
        ],
        command=command,
        mount_tmp_dir=False,
    )


with DAG(
    dag_id="ex6_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ex6", "pipeline"],
) as dag:

    ex1 = docker_task(
        task_id="ex1_data_retrieval",
        image=SBT_JAVA11_IMAGE,
        command=EX1_CMD,
    )

    ex2_q1 = docker_task(
        task_id="ex2_q1_data_ingestion",
        image=SBT_JAVA11_IMAGE,
        command=EX2_Q1_CMD,
    )

    ex3_q1 = docker_task(
        task_id="ex3_q1_create_tables",
        image="postgres:15-alpine",
        command=EX3_Q1_CMD,
    )

    ex2_q2 = docker_task(
        task_id="ex2_q2_data_ingestion",
        image=SBT_JAVA11_IMAGE,
        command=EX2_Q2_CMD,
    )

    ex3_q2 = docker_task(
        task_id="ex3_q2_insert_reference_data",
        image="postgres:15-alpine",
        command=EX3_Q2_CMD,
    )

    ex4 = docker_task(
        task_id="ex4_dashboard",
        image="python:3.11-slim",
        command=EX4_CMD,
    )

    ex5 = docker_task(
        task_id="ex5_demo",
        image="python:3.11-slim",
        command=EX5_CMD,
    )

    ex1 >> ex2_q1 >> ex3_q1 >> ex2_q2 >> ex3_q2 >> ex4 >> ex5
