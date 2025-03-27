from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
from pathlib import Path
import importlib.util

# Add debug logging at the top of the file
print("Current working directory:", os.getcwd())
print("Contents of /opt/airflow:", os.listdir("/opt/airflow"))
print("Python path before:", sys.path)

# Ensure the project root (where data_ingestion and data_processing are) is in PYTHONPATH
AIRFLOW_HOME = "/opt/airflow"
if AIRFLOW_HOME not in sys.path:
    sys.path.insert(0, AIRFLOW_HOME)

print("Python path after:", sys.path)

# Import modules directly from files
def import_module_from_path(module_name, file_path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

# Import modules directly from files
ingest_module = import_module_from_path(
    "ingest",
    "/opt/airflow/data_ingestion/ingest.py"
)
process_module = import_module_from_path(
    "process",
    "/opt/airflow/data_processing/process.py"
)

def process_flight_data(**context):
    return ingest_module.process_flight_data()

def process_data(**context):
    return process_module.process_flight_data()

# DAG definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

with DAG(
    'flight_data_pipeline',
    default_args=default_args,
    description='Pipeline for flight data processing',
    schedule_interval='@daily',
    catchup=False,
    tags=['flight_data'],
) as dag:
    
    ingest_task = PythonOperator(
        task_id='ingest_data',
        python_callable=process_flight_data,
        dag=dag,
        doc_md="""
        ### Ingest Task
        Ingests flight data from SFTP server or uses existing files in raw directory.
        Validates data format and required columns.
        """,
    )

    process_task = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
        dag=dag,
        doc_md="""
        ### Process Task
        Processes ingested flight data:
        - Cleans data
        - Removes columns with high missing values
        - Calculates metrics
        - Saves processed data as CSV
        """,
    )

    ingest_task >> process_task

# Add description
dag.doc_md = """
# Flight Data Pipeline
This DAG orchestrates the flight data processing pipeline:
1. Ingests data from SFTP or local directory
2. Validates data quality
3. Processes and transforms data
4. Saves results

## Schedule
Runs daily at midnight UTC

## Dependencies
- data_ingestion.ingest
- data_processing.process
""" 