import pandas as pd
import numpy as np
from loguru import logger
import sys
from pathlib import Path
from typing import Dict, Any
import os
from datetime import datetime

# Configure logging
logger.remove()
logger.add(sys.stderr, level="INFO")
logger.add("data/logs/processing_{time}.log", rotation="500 MB")

def clean_flight_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and transform flight data
    """
    try:
        # Create a copy to avoid modifying original data
        df_clean = df.copy()
        
        # Calculate percentage of missing values for each column
        missing_percentages = (df_clean.isnull().sum() / len(df_clean)) * 100
        logger.info("Missing value percentages:")
        for col, pct in missing_percentages.items():
            logger.info(f"{col}: {pct:.2f}%")
        
        # Remove columns with more than 70% missing values
        threshold = 70.0
        columns_to_drop = missing_percentages[missing_percentages > threshold].index
        if len(columns_to_drop) > 0:
            logger.warning(f"Dropping columns with >{threshold}% missing values: {list(columns_to_drop)}")
            df_clean = df_clean.drop(columns=columns_to_drop)
        
        # Convert date and time columns
        df_clean['FL_DATE'] = pd.to_datetime(df_clean['FL_DATE'])
        
        # Clean and standardize time fields
        def clean_time(time_val):
            if pd.isna(time_val):
                return np.nan
            time_str = str(int(time_val)).zfill(4)
            return f"{time_str[:2]}:{time_str[2:]}"
        
        df_clean['DEP_TIME'] = df_clean['DEP_TIME'].apply(clean_time)
        df_clean['ARR_TIME'] = df_clean['ARR_TIME'].apply(clean_time)
        
        # Convert airport codes to uppercase
        df_clean['ORIGIN'] = df_clean['ORIGIN'].str.upper()
        df_clean['DEST'] = df_clean['DEST'].str.upper()
        
        # Handle missing values
        df_clean['CANCELLED'] = df_clean['CANCELLED'].fillna(0)
        df_clean['DIVERTED'] = df_clean['DIVERTED'].fillna(0)
        
        # Calculate delay metrics
        delay_columns = ['DEP_DELAY', 'ARR_DELAY', 'CARRIER_DELAY', 
                        'WEATHER_DELAY', 'NAS_DELAY', 'SECURITY_DELAY', 
                        'LATE_AIRCRAFT_DELAY']
        
        for col in delay_columns:
            if col in df_clean.columns:
                df_clean[col] = df_clean[col].fillna(0)
        
        # Add derived columns
        df_clean['IS_DELAYED'] = (df_clean['DEP_DELAY'] > 15) | (df_clean['ARR_DELAY'] > 15)
        
        # Log the final columns
        logger.info(f"Final columns after cleaning: {df_clean.columns.tolist()}")
        
        return df_clean
        
    except Exception as e:
        logger.error(f"Error cleaning flight data: {e}")
        raise

def validate_processed_data(df: pd.DataFrame) -> bool:
    """
    Validate processed data quality
    """
    try:
        # Check for required columns
        required_columns = ['FL_DATE', 'OP_CARRIER', 'ORIGIN', 'DEST', 
                          'DEP_TIME', 'ARR_TIME', 'CANCELLED', 'DIVERTED']
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            logger.error(f"Missing required columns: {missing_cols}")
            return False
        
        # Check for null values in critical columns
        critical_columns = ['FL_DATE', 'OP_CARRIER', 'ORIGIN', 'DEST']
        null_counts = df[critical_columns].isnull().sum()
        if null_counts.any():
            logger.error(f"Null values found in critical columns: {null_counts[null_counts > 0]}")
            return False
        
        # Validate data types
        expected_types = {
            'FL_DATE': 'datetime64[ns]',
            'CANCELLED': 'float64',
            'DIVERTED': 'float64'
        }
        
        for col, expected_type in expected_types.items():
            if df[col].dtype != expected_type:
                logger.error(f"Invalid data type for {col}. Expected {expected_type}, got {df[col].dtype}")
                return False
        
        # Validate value ranges
        if not (0 <= df['CANCELLED'].max() <= 1):
            logger.error("Invalid values in CANCELLED column")
            return False
            
        if not (0 <= df['DIVERTED'].max() <= 1):
            logger.error("Invalid values in DIVERTED column")
            return False
        
        return True
        
    except Exception as e:
        logger.error(f"Error validating processed data: {e}")
        return False

def calculate_metrics(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Calculate key business metrics
    """
    metrics = {
        'total_flights': len(df),
        'cancellation_rate': df['CANCELLED'].mean() * 100,
        'diversion_rate': df['DIVERTED'].mean() * 100,
        'delay_rate': df['IS_DELAYED'].mean() * 100,
        'top_routes': df.groupby(['ORIGIN', 'DEST']).size().nlargest(5).to_dict(),
        'top_carriers': df['OP_CARRIER'].value_counts().nlargest(5).to_dict()
    }
    
    return metrics

def process_flight_data():
    try:
        # Use relative paths instead of absolute Airflow paths
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        processed_dir = os.path.join(base_dir, 'data', 'processed')
        
        # Define input and output paths
        input_file = os.path.join(processed_dir, 'processed_flights.csv')
        output_file = os.path.join(processed_dir, 'final_flights.csv')

        # Ensure directory exists
        os.makedirs(processed_dir, exist_ok=True)

        if not os.path.exists(input_file):
            raise FileNotFoundError(f"Processed data file not found: {input_file}")

        logger.info(f"Reading processed data from {input_file}")
        df = pd.read_csv(input_file, low_memory=False)

        # Process the data
        logger.info("Processing data...")
        
        # Add your data processing logic here
        # For example, calculate delays
        df['departure_delay'] = pd.to_numeric(df['actual_departure']) - pd.to_numeric(df['scheduled_departure'])
        df['arrival_delay'] = pd.to_numeric(df['actual_arrival']) - pd.to_numeric(df['scheduled_arrival'])

        # Save processed data
        logger.info(f"Saving final processed data to {output_file}")
        df.to_csv(output_file, index=False)
        logger.info("Data processing completed successfully")

    except Exception as e:
        logger.error(f"Error in data processing: {str(e)}")
        raise

if __name__ == "__main__":
    process_flight_data()