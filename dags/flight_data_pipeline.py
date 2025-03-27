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
from loguru import logger
import numpy as np

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

# Add the project root directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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

# Add logging wrapper
def log_wrapper(func):
    def wrapper(**context):
        logger.info(f"Starting {func.__name__}")
        try:
            result = func(**context)
            logger.info(f"Successfully completed {func.__name__}")
            return result
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {str(e)}")
            raise
    return wrapper

def process_flight_data(**context):
    return ingest_module.process_flight_data()

def process_data(**context):
    return process_module.process_flight_data()

def load_to_postgres(**context):
    try:
        # Create PostgreSQL connection hook
        pg_hook = PostgresHook(postgres_conn_id='flight_db')
        
        # Create table if it doesn't exist
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS flights (
            flight_date DATE,
            airline VARCHAR(10),
            flight_number INTEGER,
            origin VARCHAR(5),
            destination VARCHAR(5),
            scheduled_departure VARCHAR(10),
            actual_departure VARCHAR(10),
            scheduled_arrival VARCHAR(10),
            actual_arrival VARCHAR(10),
            departure_delay FLOAT,
            arrival_delay FLOAT,
            flight_status VARCHAR(20)
        );
        """
        pg_hook.run(create_table_sql)
        
        # Load processed data
        df = pd.read_csv('/opt/airflow/data/processed/final_flights.csv')
        
        # Convert numpy types to Python native types
        df = df.replace({np.nan: None})
        df['flight_number'] = df['flight_number'].astype(int)
        df['departure_delay'] = df['departure_delay'].astype(float)
        df['arrival_delay'] = df['arrival_delay'].astype(float)
        
        # Convert dates to proper format
        df['flight_date'] = pd.to_datetime(df['flight_date']).dt.strftime('%Y-%m-%d')
        
        # Convert DataFrame to list of tuples
        rows = list(df.itertuples(index=False, name=None))
        
        # Insert data
        insert_sql = """
        INSERT INTO flights (
            flight_date, airline, flight_number, origin, destination,
            scheduled_departure, actual_departure, scheduled_arrival,
            actual_arrival, departure_delay, arrival_delay, flight_status
        ) VALUES %s;
        """
        pg_hook.insert_rows('flights', rows)
        
        return "Data loaded to PostgreSQL successfully"
        
    except Exception as e:
        logger.error(f"Error loading to PostgreSQL: {str(e)}")
        raise

def run_performance_metrics(**context):
    """Calculate and store performance metrics"""
    try:
        # Use the same connection ID as load_to_postgres
        pg_hook = PostgresHook(postgres_conn_id='flight_db')
        
        metrics_query = """
        WITH delay_stats AS (
            SELECT 
                airline,
                COUNT(*) as total_flights,
                AVG(departure_delay) as avg_departure_delay,
                AVG(arrival_delay) as avg_arrival_delay,
                COUNT(CASE WHEN flight_status = 'Delayed' THEN 1 END) as delayed_flights,
                COUNT(CASE WHEN flight_status = 'On Time' THEN 1 END) as ontime_flights
            FROM flights
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
        metrics_dir = '/opt/airflow/data/processed'
        os.makedirs(metrics_dir, exist_ok=True)
        df.to_csv(f'{metrics_dir}/flight_metrics.csv', index=False)
        
        # Save as JSON with metrics
        metrics_dict = {
            'generated_at': datetime.now().isoformat(),
            'total_flights': int(df['total_flights'].sum()),
            'total_delayed_flights': int(df['delayed_flights'].sum()),
            'overall_ontime_percentage': float(df['ontime_flights'].sum() / df['total_flights'].sum() * 100),
            'airlines': df.to_dict(orient='records')
        }
        
        with open(f'{metrics_dir}/flight_metrics.json', 'w') as f:
            json.dump(metrics_dict, f, indent=2)
        
        return "Metrics generated successfully"
        
    except Exception as e:
        logger.error(f"Error generating metrics: {str(e)}")
        raise

def run_route_analysis(**context):
    """Analyze route performance"""
    try:
        # Use the same connection ID as other tasks
        pg_hook = PostgresHook(postgres_conn_id='flight_db')
        
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
        FROM flights
        GROUP BY origin, destination
        ORDER BY total_flights DESC;
        """
        
        df = pg_hook.get_pandas_df(route_query)
        
        # Save route analysis
        output_dir = '/opt/airflow/data/processed'
        os.makedirs(output_dir, exist_ok=True)
        df.to_csv(f'{output_dir}/route_analysis.csv', index=False)
        
        # Create JSON summary
        summary = {
            'generated_at': datetime.now().isoformat(),
            'total_routes': len(df),
            'top_routes': df.head(10).to_dict('records'),
            'worst_performing_routes': df.nsmallest(5, 'ontime_percentage').to_dict('records')
        }
        
        with open(f'{output_dir}/route_analysis.json', 'w') as f:
            json.dump(summary, f, indent=2)
        
        return "Route analysis complete"
        
    except Exception as e:
        logger.error(f"Error in route analysis: {str(e)}")
        raise

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
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'flight_data_pipeline',
    default_args=default_args,
    description='Pipeline for processing flight data',
    schedule_interval=timedelta(days=1),
    catchup=False
)

# Task 1: Data Ingestion
ingest_task = PythonOperator(
    task_id='ingest_flight_data',
    python_callable=log_wrapper(validate_data),
    provide_context=True,
    dag=dag,
)

# Task 2: Data Processing
process_task = PythonOperator(
    task_id='process_flight_data',
    python_callable=log_wrapper(process_data),
    provide_context=True,
    dag=dag,
)

# Task 3: Load to PostgreSQL
load_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=log_wrapper(load_to_postgres),
    provide_context=True,
    dag=dag,
)

# Task 4: Calculate Performance Metrics
performance_metrics_task = PythonOperator(
    task_id='calculate_performance_metrics',
    python_callable=log_wrapper(run_performance_metrics),
    provide_context=True,
    dag=dag,
)

# Task 5: Route Analysis
route_analysis_task = PythonOperator(
    task_id='analyze_routes',
    python_callable=log_wrapper(run_route_analysis),
    provide_context=True,
    dag=dag,
)

# Task 6: Generate API Metrics
api_metrics_task = PythonOperator(
    task_id='generate_api_metrics',
    python_callable=generate_api_metrics,
    dag=dag,
)

# Set task dependencies
ingest_task >> process_task >> load_task >> [performance_metrics_task, route_analysis_task]
[performance_metrics_task, route_analysis_task] >> api_metrics_task

# Add description
dag.doc_md = """
# Flight Data Pipeline
This DAG orchestrates the flight data processing pipeline:
1. Ingests data from SFTP or local directory
2. Validates data quality
3. Processes and transforms data
4. Loads data to PostgreSQL
5. Generates metrics and route analysis in parallel
6. Creates API metrics

## Schedule
Runs daily at midnight UTC

## Dependencies
- data_ingestion.ingest
- data_processing.process
""" 