# Part of: E-commerce ETL Pipeline & RAG Support Assistant
"""
Apache Airflow DAG for the E-commerce ETL Pipeline.
Orchestrates extract, validate, transform, load, and notification tasks.
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator

# Add the project root to sys.path so that the 'etl' package can be imported by Airflow
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

# Import ETL functions
from etl.extract import extract_and_upload
from etl.validate import validate
from etl.transform import transform
from etl.load import load_to_postgres

# Default arguments for the DAG
default_args = {
    "owner": "arpit",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def run_extract():
    """Wrapper function to execute extract step."""
    print("[DAG] Running extract task...")
    # Use default dataset location
    extract_and_upload(local_path=str(project_root / "data" / "data.csv"))
    print("[DAG] Extract task completed.")

def run_validate():
    """Wrapper function to execute validate step."""
    print("[DAG] Running validate task...")
    validate(
        input_path="/tmp/raw_orders.parquet",
        output_path="/tmp/validated_orders.parquet"
    )
    print("[DAG] Validate task completed.")

def run_transform():
    """Wrapper function to execute transform step."""
    print("[DAG] Running transform task...")
    transform(
        input_path="/tmp/validated_orders.parquet",
        output_dir="/tmp"
    )
    print("[DAG] Transform task completed.")

def run_load():
    """Wrapper function to execute load task."""
    print("[DAG] Running load task...")
    load_to_postgres(input_dir="/tmp")
    print("[DAG] Load task completed.")

def notify_success():
    """Dummy operator function to print pipeline success."""
    print("[DAG] Pipeline completed successfully!")

# Define the DAG
with DAG(
    dag_id="ecom_etl_pipeline",
    default_args=default_args,
    description="E-commerce ETL Pipeline Orchestration (Extract, Validate, Transform, Load)",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["ecommerce", "etl"],
) as dag:
    
    # Task definitions
    task_extract = PythonOperator(
        task_id="extract",
        python_callable=run_extract,
    )
    
    task_validate = PythonOperator(
        task_id="validate",
        python_callable=run_validate,
    )
    
    task_transform = PythonOperator(
        task_id="transform",
        python_callable=run_transform,
    )
    
    task_load = PythonOperator(
        task_id="load",
        python_callable=run_load,
    )
    
    task_notify = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
    )
    
    # Task dependency chain
    task_extract >> task_validate >> task_transform >> task_load >> task_notify
