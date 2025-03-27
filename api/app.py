import os
import pandas as pd
from flask import Flask, request, jsonify, abort
from flask_httpauth import HTTPBasicAuth
from werkzeug.security import generate_password_hash, check_password_hash
from pathlib import Path
import json
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from datetime import datetime
import base64
from flask_swagger_ui import get_swaggerui_blueprint
from prometheus_client import Counter, Histogram, generate_latest
from prometheus_client import CONTENT_TYPE_LATEST
import time

app = Flask(__name__)
auth = HTTPBasicAuth()
limiter = Limiter(
    app=app,
    key_func=get_remote_address,
    default_limits=["100 per day", "10 per minute"]
)

# Swagger configuration
SWAGGER_URL = '/docs'
API_URL = '/static/swagger.json'

swaggerui_blueprint = get_swaggerui_blueprint(
    SWAGGER_URL,
    API_URL,
    config={
        'app_name': "Flight Data API"
    }
)
app.register_blueprint(swaggerui_blueprint, url_prefix=SWAGGER_URL)

# Store users in a dictionary with hashed passwords
users = {
    "admin": generate_password_hash("admin_password")
}

# Add metric collectors
REQUEST_COUNT = Counter(
    'request_count', 'App Request Count',
    ['method', 'endpoint', 'http_status']
)
REQUEST_LATENCY = Histogram(
    'request_latency_seconds', 'Request latency',
    ['endpoint']
)

@auth.verify_password
def verify_password(username, password):
    if username in users and check_password_hash(users.get(username), password):
        return username
    return None

def encode_cursor(last_date, last_id):
    """Encode the cursor for pagination"""
    cursor_data = f"{last_date}:{last_id}"
    return base64.b64encode(cursor_data.encode()).decode()

def decode_cursor(cursor):
    """Decode the cursor for pagination"""
    try:
        cursor_data = base64.b64decode(cursor.encode()).decode()
        last_date, last_id = cursor_data.split(':')
        return last_date, int(last_id)
    except:
        return None, None

@app.route('/api/flights', methods=['GET'])
@auth.login_required
@limiter.limit("30/minute")
def get_flights():
    """
    Get flight data with filtering and pagination
    ---
    tags:
      - Flights
    security:
      - BasicAuth: []
    parameters:
      - name: start_date
        in: query
        type: string
        format: date
        description: Filter flights from this date (YYYY-MM-DD)
      - name: end_date
        in: query
        type: string
        format: date
        description: Filter flights until this date (YYYY-MM-DD)
      - name: origin
        in: query
        type: string
        description: Filter by origin airport code
      - name: destination
        in: query
        type: string
        description: Filter by destination airport code
      - name: cursor
        in: query
        type: string
        description: Pagination cursor from previous response
      - name: limit
        in: query
        type: integer
        default: 100
        description: Number of records to return (max 1000)
    responses:
      200:
        description: Successful response
        schema:
          properties:
            flights:
              type: array
              items:
                type: object
            next_cursor:
              type: string
            count:
              type: integer
            total:
              type: integer
      401:
        description: Unauthorized
      429:
        description: Rate limit exceeded
    """
    try:
        # Load processed flight data from CSV instead of Parquet
        df = pd.read_csv('data/processed/processed_flights.csv')
        # Convert FL_DATE back to datetime as it's now string from CSV
        df['FL_DATE'] = pd.to_datetime(df['FL_DATE'])
        
        # Get query parameters
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        origin = request.args.get('origin')
        destination = request.args.get('destination')
        cursor = request.args.get('cursor')
        limit = min(int(request.args.get('limit', 100)), 1000)  # Max 1000 records per request
        
        # Add row index for cursor-based pagination
        df = df.reset_index()
        
        # Apply filters
        if start_date:
            df = df[df['FL_DATE'] >= start_date]
        if end_date:
            df = df[df['FL_DATE'] <= end_date]
        if origin:
            df = df[df['ORIGIN'] == origin.upper()]
        if destination:
            df = df[df['DEST'] == destination.upper()]
            
        # Apply cursor-based pagination
        if cursor:
            last_date, last_id = decode_cursor(cursor)
            if last_date and last_id is not None:
                df = df[
                    (df['FL_DATE'] > last_date) |
                    ((df['FL_DATE'] == last_date) & (df['index'] > last_id))
                ]
        
        # Sort for consistent pagination
        df = df.sort_values(['FL_DATE', 'index'])
        
        # Get page of results
        page_df = df.head(limit)
        
        # Create next cursor if there are more results
        next_cursor = None
        if len(page_df) == limit and len(df) > limit:
            last_row = page_df.iloc[-1]
            next_cursor = encode_cursor(
                str(last_row['FL_DATE'].date()),
                last_row['index']
            )
        
        # Prepare response
        results = page_df.drop('index', axis=1).to_dict(orient='records')
        
        return jsonify({
            'flights': results,
            'next_cursor': next_cursor,
            'count': len(results),
            'total': len(df)
        })
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/metrics', methods=['GET'])
@auth.login_required
@limiter.limit("60/minute")
def get_metrics():
    """
    Get flight metrics with date filtering
    ---
    tags:
      - Metrics
    security:
      - BasicAuth: []
    parameters:
      - name: start_date
        in: query
        type: string
        format: date
        description: Calculate metrics from this date (YYYY-MM-DD)
      - name: end_date
        in: query
        type: string
        format: date
        description: Calculate metrics until this date (YYYY-MM-DD)
    responses:
      200:
        description: Successful response
        schema:
          properties:
            total_flights:
              type: integer
            cancellation_rate:
              type: number
            diversion_rate:
              type: number
            delay_rate:
              type: number
            top_routes:
              type: object
            top_carriers:
              type: object
            date_range:
              type: object
      401:
        description: Unauthorized
      429:
        description: Rate limit exceeded
    """
    try:
        # Load processed flight data from CSV instead of Parquet
        df = pd.read_csv('data/processed/processed_flights.csv')
        # Convert FL_DATE back to datetime as it's now string from CSV
        df['FL_DATE'] = pd.to_datetime(df['FL_DATE'])
        
        # Get date range parameters
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        # Apply date filters if provided
        if start_date:
            df = df[df['FL_DATE'] >= start_date]
        if end_date:
            df = df[df['FL_DATE'] <= end_date]
        
        # Calculate metrics for the filtered data
        # Convert tuple keys to strings for JSON serialization
        top_routes = df.groupby(['ORIGIN', 'DEST']).size().nlargest(5)
        top_routes_dict = {f"{origin}-{dest}": count 
                          for (origin, dest), count in top_routes.items()}
        
        metrics = {
            'total_flights': len(df),
            'cancellation_rate': float(df['CANCELLED'].mean() * 100),
            'diversion_rate': float(df['DIVERTED'].mean() * 100),
            'delay_rate': float(df['IS_DELAYED'].mean() * 100),
            'top_routes': top_routes_dict,
            'top_carriers': df['OP_CARRIER'].value_counts().nlargest(5).to_dict(),
            'date_range': {
                'start': str(df['FL_DATE'].min().date()),
                'end': str(df['FL_DATE'].max().date())
            }
        }
        
        return jsonify(metrics)
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/airports', methods=['GET'])
@auth.login_required
@limiter.limit("60/minute")
def get_airports():
    """
    Get list of all airports
    ---
    tags:
      - Airports
    security:
      - BasicAuth: []
    responses:
      200:
        description: Successful response
        schema:
          properties:
            airports:
              type: array
              items:
                type: string
            count:
              type: integer
      401:
        description: Unauthorized
      429:
        description: Rate limit exceeded
    """
    try:
        # Load processed flight data from CSV instead of Parquet
        df = pd.read_csv('data/processed/processed_flights.csv')
        
        origins = df['ORIGIN'].unique().tolist()
        destinations = df['DEST'].unique().tolist()
        airports = sorted(list(set(origins + destinations)))
        
        return jsonify({
            'airports': airports,
            'count': len(airports)
        })
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.errorhandler(429)
def ratelimit_handler(e):
    return jsonify({
        'error': 'Rate limit exceeded',
        'message': str(e.description)
    }), 429

# Add metrics endpoint
@app.route('/metrics')
def metrics():
    return generate_latest(), 200, {'Content-Type': CONTENT_TYPE_LATEST}

# Add metrics collection to existing endpoints
@app.before_request
def before_request():
    request.start_time = time.time()

@app.after_request
def after_request(response):
    if hasattr(request, 'start_time'):
        request_latency = time.time() - request.start_time
        REQUEST_LATENCY.labels(request.endpoint).observe(request_latency)
    
    REQUEST_COUNT.labels(
        method=request.method,
        endpoint=request.endpoint,
        http_status=response.status_code
    ).inc()
    return response

if __name__ == '__main__':
    app.run(debug=True)