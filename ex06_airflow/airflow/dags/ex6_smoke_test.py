from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="ex6_smoke_test",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    hello = BashOperator(
        task_id="hello",
        bash_command="""
        echo "Airflow is running!"
        echo "Current user:"
        whoami
        echo "Current directory:"
        pwd
        echo "Project folder content:"
        ls -la /opt/project || echo "No /opt/project mount found"
        echo "Docker socket check:"
        ls -la /var/run/docker.sock || echo "Docker socket not mounted"
        """,
    )