from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Default arguments for the DAG tasks
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "olist_ecommerce_etl",
    default_args=default_args,
    description="Orchestrates ingestion, transformation, loading, and validation of Olist Brazilian e-commerce dataset",
    schedule_interval=None,  # Run manually, or set to a cron e.g., '0 0 * * *' (daily)
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["ecommerce", "etl"],
) as dag:

    # 1. Ingest Task - Download CSVs and save locally/upload to S3
    ingest_task = BashOperator(
        task_id="ingest_data",
        bash_command="python /opt/airflow/etl/ingest.py",
    )

    # 2. Transform Task - Clean, deduplicate and join datasets
    transform_task = BashOperator(
        task_id="transform_data",
        bash_command="python /opt/airflow/etl/transform.py",
    )

    # 3. Load Task - Write analytics table to staging schema in Postgres
    load_task = BashOperator(
        task_id="load_data",
        bash_command="python /opt/airflow/etl/load.py",
    )

    # 4. Validate Task - Validate database table using Great Expectations
    validate_task = BashOperator(
        task_id="validate_data",
        bash_command="python /opt/airflow/etl/validate.py",
    )

    # Task dependencies
    ingest_task >> transform_task >> load_task >> validate_task
