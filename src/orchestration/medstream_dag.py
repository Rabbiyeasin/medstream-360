from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "medstream",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="medstream_360_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["medstream", "etl", "healthcare"]
) as dag:

    # 1. Bronze ingestion (Kafka → Delta)
    bronze = BashOperator(
        task_id="bronze_layer",
        bash_command="python /home/user/projects/MediStream-360/src/ingestion/vitals_consumer.py"
    )

    # 2. Silver transformation (dbt models)
    silver = BashOperator(
        task_id="silver_layer",
        bash_command="cd /home/user/projects/MediStream-360/dbt && dbt run --select silver"
    )

    # 3. Gold analytics (dbt models)
    gold = BashOperator(
        task_id="gold_layer",
        bash_command="cd /home/user/projects/MediStream-360/dbt && dbt run --select gold"
    )

    bronze >> silver >> gold