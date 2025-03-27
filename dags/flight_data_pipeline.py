from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
import pandas as pd
import json
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

# Add project root to PYTHONPATH
sys.path.append('/opt/airflow/project')

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
    "/opt/airflow/project/data_ingestion/ingest.py"
)
process_module = import_module_from_path(
    "process",
    "/opt/airflow/project/data_processing/process.py"
)

# Import processing functions - updated paths
sys.path.append('/opt/airflow')
from data_ingestion.ingest import process_flight_data as validate_data
from data_processing.process import process_flight_data as process_data

def process_flight_data(**context):
    return ingest_module.process_flight_data()

def process_data(**context):
    return process_module.process_flight_data()

def load_to_postgres(**context):
    """Load processed data into PostgreSQL database"""
    df = pd.read_csv('/opt/airflow/data/processed/processed_flights.csv')
    
    pg_hook = PostgresHook(postgres_conn_id='airflow_db')
    
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS processed_flights (
        flight_date DATE,
        airline VARCHAR(50),
        flight_number VARCHAR(20),
        origin VARCHAR(10),
        destination VARCHAR(10),
        scheduled_departure TIME,
        actual_departure TIME,
        departure_delay INT,
        scheduled_arrival TIME,
        actual_arrival TIME,
        arrival_delay INT,
        flight_status VARCHAR(20)
    );
    """
    pg_hook.run(create_table_sql)
    
    pg_hook.insert_rows(
        table='processed_flights',
        rows=df.values.tolist(),
        target_fields=df.columns.tolist(),
        replace=True
    )
    
    return "Data loaded to PostgreSQL"

def run_performance_metrics(**context):
    """Calculate and store performance metrics"""
    pg_hook = PostgresHook(postgres_conn_id='airflow_db')
    
    metrics_query = """
    WITH delay_stats AS (
        SELECT 
            airline,
            COUNT(*) as total_flights,
            AVG(departure_delay) as avg_departure_delay,
            AVG(arrival_delay) as avg_arrival_delay,
            COUNT(CASE WHEN flight_status = 'Delayed' THEN 1 END) as delayed_flights,
            COUNT(CASE WHEN flight_status = 'On Time' THEN 1 END) as ontime_flights
        FROM processed_flights
        GROUP BY airline
    )
    SELECT 
        airline,
        total_flights,
        ROUND(avg_departure_delay::numeric, 2) as avg_departure_delay,
        ROUND(avg_arrival_delay::numeric, 2) as avg_arrival_delay,
        delayed_flights,
        ontime_flights,
        ROUND((ontime_flights::float / total_flights * 100)::numeric, 2) as ontime_percentage
    FROM delay_stats
    ORDER BY total_flights DESC;
    """
    
    df = pg_hook.get_pandas_df(metrics_query)
    
    # Save as CSV
    df.to_csv('/opt/airflow/data/processed/flight_metrics.csv', index=False)
    
    # Save as JSON with metrics
    metrics_dict = {
        'generated_at': datetime.now().isoformat(),
        'total_flights': int(df['total_flights'].sum()),
        'total_delayed_flights': int(df['delayed_flights'].sum()),
        'overall_ontime_percentage': float(df['ontime_flights'].sum() / df['total_flights'].sum() * 100),
        'airlines': df.to_dict(orient='records')
    }
    
    with open('/opt/airflow/data/processed/flight_metrics.json', 'w') as f:
        json.dump(metrics_dict, f, indent=2)
    
    return "Metrics generated"

def run_route_analysis(**context):
    """Analyze route performance"""
    pg_hook = PostgresHook(postgres_conn_id='airflow_db')
    
    route_query = """
    SELECT 
        origin,
        destination,
        COUNT(*) as total_flights,
        ROUND(AVG(departure_delay)::numeric, 2) as avg_departure_delay,
        ROUND(AVG(arrival_delay)::numeric, 2) as avg_arrival_delay,
        COUNT(CASE WHEN flight_status = 'Delayed' THEN 1 END) as delayed_flights,
        ROUND((COUNT(CASE WHEN flight_status = 'On Time' THEN 1 END)::float / 
               COUNT(*) * 100)::numeric, 2) as ontime_percentage
    FROM processed_flights
    GROUP BY origin, destination
    ORDER BY total_flights DESC;
    """
    
    df = pg_hook.get_pandas_df(route_query)
    df.to_csv('/opt/airflow/data/processed/route_analysis.csv', index=False)
    
    return "Route analysis complete"

def generate_api_metrics(**context):
    """Generate metrics for API consumption"""
    with open('/opt/airflow/data/processed/flight_metrics.json', 'r') as f:
        metrics = json.load(f)
    
    # Add additional API-specific metrics
    api_metrics = {
        'metrics_version': '1.0',
        'last_updated': datetime.now().isoformat(),
        'summary': {
            'total_flights': metrics['total_flights'],
            'total_delayed': metrics['total_delayed_flights'],
            'overall_performance': {
                'ontime_percentage': metrics['overall_ontime_percentage'],
                'status': 'Good' if metrics['overall_ontime_percentage'] > 80 else 'Needs Improvement'
            }
        },
        'airline_performance': metrics['airlines'],
        'metadata': {
            'data_source': 'processed_flights',
            'generated_by': 'Airflow DAG'
        }
    }
    
    # Save enhanced metrics for API
    with open('/opt/airflow/data/processed/api_metrics.json', 'w') as f:
        json.dump(api_metrics, f, indent=2)
    
    return "API metrics generated"

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
    'catchup': False,
}

dag = DAG(
    'flight_data_pipeline',
    default_args=default_args,
    description='Pipeline for flight data processing',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['flight_data'],
)

# Validation task
validate_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    provide_context=True,
    dag=dag,
)

# Processing task
process_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    provide_context=True,
    dag=dag,
)

load_db_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    dag=dag,
)

performance_metrics_task = PythonOperator(
    task_id='calculate_performance_metrics',
    python_callable=run_performance_metrics,
    dag=dag,
)

route_analysis_task = PythonOperator(
    task_id='analyze_routes',
    python_callable=run_route_analysis,
    dag=dag,
)

api_metrics_task = PythonOperator(
    task_id='generate_api_metrics',
    python_callable=generate_api_metrics,
    dag=dag,
)

# Set task dependencies
validate_task >> process_task >> load_db_task
load_db_task >> [performance_metrics_task, route_analysis_task]
[performance_metrics_task, route_analysis_task] >> api_metrics_task

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