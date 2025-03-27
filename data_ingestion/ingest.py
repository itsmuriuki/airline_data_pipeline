import os
import paramiko
import pandas as pd
import json
from loguru import logger
import sys
import tempfile
import shutil
from pathlib import Path
import smtplib
from email.message import EmailMessage
from typing import List, Tuple, Dict, Any

# Configure Loguru with more detailed logging
logger.remove()  # Remove default handler
logger.add(sys.stderr, level="INFO")  # Add console handler
logger.add("data/logs/ingestion_{time}.log", rotation="500 MB")  # File logging with rotation

def configure_logging():
    """
    Configure logging with custom formatting and handlers
    """
    log_format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
        "<level>{message}</level>"
    )
    logger.remove()
    logger.add(sys.stderr, format=log_format, level="INFO")
    logger.add("data/logs/ingestion_{time}.log", rotation="500 MB", format=log_format)

def send_alert(subject: str, message: str) -> None:
    """
    Send email alerts for critical errors
    """
    smtp_host = os.getenv('SMTP_HOST', 'smtp.gmail.com')
    smtp_port = int(os.getenv('SMTP_PORT', '587'))
    smtp_user = os.getenv('SMTP_USER')
    smtp_password = os.getenv('SMTP_PASSWORD')
    alert_recipients = os.getenv('ALERT_RECIPIENTS', '').split(',')

    if not all([smtp_user, smtp_password, alert_recipients]):
        logger.warning("Email alerts disabled - missing SMTP configuration")
        return

    try:
        msg = EmailMessage()
        msg.set_content(message)
        msg['Subject'] = f"Data Ingestion Alert: {subject}"
        msg['From'] = smtp_user
        msg['To'] = ', '.join(alert_recipients)

        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.send_message(msg)
            logger.info(f"Alert email sent: {subject}")
    except Exception as e:
        logger.error(f"Failed to send alert email: {e}")

def validate_file_content(file_path: str) -> Tuple[bool, str]:
    """
    Validate file content based on file type
    Returns: (is_valid, error_message)
    """
    try:
        file_ext = Path(file_path).suffix.lower()
        
        if file_ext == '.csv':
            # Read CSV with low_memory=False to avoid DtypeWarning
            df = pd.read_csv(file_path, low_memory=False)
            
            # Log actual columns for debugging
            logger.info(f"Actual columns in {file_path}: {df.columns.tolist()}")
            
            # Check if this is the flight data file
            if 'FL_DATE' in df.columns:
                required_columns = {
                    'FL_DATE', 'OP_CARRIER', 'ORIGIN', 'DEST', 
                    'DEP_TIME', 'ARR_TIME'
                }
            else:
                # For other CSV files, just check if they have data
                required_columns = set()
            
            if required_columns:
                missing_cols = [col for col in required_columns if col not in df.columns]
                if missing_cols:
                    return False, f"Missing required columns in {file_path}: {missing_cols}"
            
            if df.empty:
                return False, f"File is empty: {file_path}"
            
        elif file_ext == '.json':
            with open(file_path) as f:
                data = json.load(f)
            if not isinstance(data, (list, dict)):
                return False, f"Invalid JSON structure in {file_path}"
            
        else:
            return False, f"Unsupported file format: {file_ext}"
        
        return True, ""
        
    except pd.errors.EmptyDataError:
        return False, f"Empty file: {file_path}"
    except pd.errors.ParserError:
        return False, f"Invalid CSV format in {file_path}"
    except json.JSONDecodeError:
        return False, f"Invalid JSON format in {file_path}"
    except Exception as e:
        return False, f"Validation error for {file_path}: {str(e)}"

def create_mock_sftp_server():
    """
    Creates a temporary directory with mock data for testing
    Returns tuple of (mock_dir, mock_files)
    """
    temp_dir = tempfile.mkdtemp()
    mock_files = []
    
    try:
        # Handle flight_data.csv
        source_csv = 'data/raw/flight_data.csv'
        dest_csv = Path(temp_dir) / 'flight_data.csv'
        
        if os.path.exists(source_csv):
            shutil.copy2(source_csv, dest_csv)
            logger.info(f"Copied existing flight_data.csv to mock SFTP directory")
        else:
            # Fallback to create minimal flight data with correct column names
            logger.warning(f"Could not find {source_csv}, creating minimal sample data")
            csv_content = (
                "FL_DATE,OP_CARRIER,ORIGIN,DEST,DEP_TIME,ARR_TIME\n"
                "2024-01-01,AA,JFK,LAX,0800,1100\n"
                "2024-01-01,DL,LAX,JFK,0930,1730\n"
            )
            dest_csv.write_text(csv_content)
        mock_files.append('flight_data.csv')
        
        # Create weather data JSON file
        weather_json = Path(temp_dir) / 'weather_data.json'
        json_content = {
            "stations": [
                {"id": "JFK", "temperature": 72, "conditions": "clear"},
                {"id": "LAX", "temperature": 75, "conditions": "sunny"}
            ]
        }
        weather_json.write_text(json.dumps(json_content))
        mock_files.append('weather_data.json')
        
        logger.info(f"Created mock SFTP directory at {temp_dir}")
        return temp_dir, mock_files
        
    except Exception as e:
        logger.error(f"Error setting up mock SFTP: {e}")
        raise

class MockSFTPClient:
    """Mock SFTP client for testing"""
    def __init__(self, mock_dir):
        self.mock_dir = Path(mock_dir)
    
    def listdir(self, path):
        return os.listdir(self.mock_dir)
    
    def get(self, remotepath, localpath):
        filename = Path(remotepath).name
        shutil.copy(self.mock_dir / filename, localpath)
    
    def close(self):
        pass

def connect_sftp():
    """
    Establish SFTP connection with fallback to mock server for testing
    """
    try:
        hostname = os.getenv('SFTP_HOST')
        username = os.getenv('SFTP_USERNAME')
        password = os.getenv('SFTP_PASSWORD')

        # If credentials not provided, use mock SFTP
        if not all([hostname, username, password]):
            logger.warning("SFTP credentials not found - using mock SFTP server for testing")
            mock_dir, _ = create_mock_sftp_server()
            return MockSFTPClient(mock_dir), None

        # Real SFTP connection logic
        port = int(os.getenv('SFTP_PORT', '22'))
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            hostname=hostname,
            port=port,
            username=username,
            password=password
        )
        sftp = client.open_sftp()
        logger.info(f"Successfully connected to SFTP server at {hostname}")
        return sftp, client
    except Exception as e:
        logger.error(f"SFTP connection failed: {e}")
        raise

def download_files():
    """
    Download files from SFTP with mock server support
    """
    local_path = 'data/raw'
    os.makedirs(local_path, exist_ok=True)
    os.makedirs('data/logs', exist_ok=True)
    
    configure_logging()
    
    try:
        sftp, client = connect_sftp()
        
        # Adjust path based on whether we're using mock or real SFTP
        remote_path = '/home/ftpuser/files' if client else ''
        files = sftp.listdir(remote_path)
        logger.info(f"Found {len(files)} files to process")
        
        downloaded_files = []
        for filename in files:
            try:
                remote_file = f"{remote_path}/{filename}" if remote_path else filename
                local_file = f"{local_path}/{filename}"
                
                sftp.get(remote_file, local_file)
                downloaded_files.append(filename)
                logger.info(f"Downloaded: {filename}")
            except Exception as file_error:
                logger.warning(f"Failed to download {filename}: {file_error}")
        
        logger.success(f"Successfully downloaded {len(downloaded_files)} files")
        return downloaded_files
    
    except Exception as e:
        logger.error(f"File download process failed: {e}")
        raise
    
    finally:
        if 'sftp' in locals():
            sftp.close()
        if client:  # Only close client if it's a real SFTP connection
            client.close()
        # Clean up mock directory if it exists
        if not client and 'sftp' in locals():
            shutil.rmtree(sftp.mock_dir)

def validate_downloaded_files(local_path: str = 'data/raw') -> bool:
    """
    Validate downloaded files
    """
    try:
        files = os.listdir(local_path)
        if not files:
            msg = "No files found in download directory"
            logger.warning(msg)
            send_alert("Validation Failed", msg)
            return False
        
        validation_errors = []
        for file in files:
            file_path = os.path.join(local_path, file)
            is_valid, error_msg = validate_file_content(file_path)
            
            if not is_valid:
                validation_errors.append(error_msg)
                logger.error(f"Validation failed for {file}: {error_msg}")
            else:
                logger.info(f"Validated file: {file}")
        
        if validation_errors:
            error_msg = "\n".join(validation_errors)
            send_alert("File Validation Errors", error_msg)
            return False
        
        return True
        
    except Exception as e:
        error_msg = f"File validation failed: {e}"
        logger.error(error_msg)
        send_alert("Validation Error", error_msg)
        return False

def process_flight_data():
    """
    Process flight data from raw directory
    """
    try:
        # Set up input/output paths
        raw_dir = '/opt/airflow/data/raw'
        processed_dir = '/opt/airflow/data/processed'
        
        # Ensure directories exist
        os.makedirs(raw_dir, exist_ok=True)
        os.makedirs(processed_dir, exist_ok=True)
        
        # Read raw flight data
        raw_file = os.path.join(raw_dir, 'flight_data.csv')
        logger.info(f"Reading data from {raw_file}")
        
        if not os.path.exists(raw_file):
            raise FileNotFoundError(f"Raw file not found: {raw_file}")
            
        df = pd.read_csv(raw_file)
        
        # Basic validation
        required_columns = ['flight_date', 'airline', 'flight_number', 
                          'origin', 'destination', 'scheduled_departure',
                          'actual_departure', 'scheduled_arrival', 'actual_arrival']
                          
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")
            
        # Save validated data
        output_file = os.path.join(processed_dir, 'validated_flights.csv')
        df.to_csv(output_file, index=False)
        logger.info(f"Saved validated data to {output_file}")
        
        return "Data ingestion completed successfully"
        
    except Exception as e:
        logger.error(f"Error in data ingestion: {str(e)}")
        raise

if __name__ == "__main__":
    process_flight_data()