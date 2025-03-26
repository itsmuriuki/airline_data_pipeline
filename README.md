# Airline Data Pipeline

## Overview
This project demonstrates a complete data engineering pipeline using Docker, processing Airline On-Time Performance data.

## Components
- SFTP Server: Simulates data source
- Data Ingestion: Retrieves files from SFTP
- Data Processing: Cleans and transforms flight data
- API: Provides access to processed data


## Setup and Run
1. Clone the repository
2. Place sample flight data CSV files in `data/raw/`
3. navigate to the airline-data-pipeline directory `cd airline-data-pipeline`
4. create a virtual environment `pyenv virtualenv 3.9.10 airline-data-pipeline`
5. install the requirements `pip install -r requirements.txt`
6. run the ingestion service `python data-ingestion/ingest.py`
7. run the processing service `python data-processing/process.py`
7. run the API service `python api/app.py`

## API Usage
Endpoint: `http://localhost:5000/api/...`
swagger documentation: `http://localhost:5000/docs`

##To test the API endpoints using curl:
Test the flights endpoint:

Get flights with pagination
`curl -u admin:admin_password "http://localhost:5000/api/flights?limit=10" `

# Get flights with date filter
`curl -u admin:admin_password "http://localhost:5000/api/flights?start_date=2024-01-01&end_date=2024-01-31"`

Test the metrics endpoint:
Get all metrics
`curl -u admin:admin_password "http://localhost:5000/api/metrics"`

Get metrics for a specific date range
`curl -u admin:admin_password "http://localhost:5000/api/metrics?start_date=2024-01-01&end_date=2024-01-31"`

### Authentication
- Username: `admin`
- Password: `admin_password`

## Error Handling
- Logs maintained in `data/logs/`
- Robust error handling in ingestion and processing scripts

## Data Ingestion
- Retrieves data from `data/raw/`
- Saves to `data/processed/`

## Data Processing
- Combines multiple CSV files and json files
- Cleans and transforms data
- Categorizes flight performance
- Saves to `data/processed/`

