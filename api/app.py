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
        # Load processed flight data from CSV
        df = pd.read_csv('data/processed/processed_flights.csv')
        # Convert flight_date back to datetime
        df['flight_date'] = pd.to_datetime(df['flight_date'])
        
        # Get query parameters
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        origin = request.args.get('origin')
        destination = request.args.get('destination')
        cursor = request.args.get('cursor')
        limit = min(int(request.args.get('limit', 100)), 1000)
        
        # Add row index for cursor-based pagination
        df = df.reset_index()
        
        # Apply filters
        if start_date:
            df = df[df['flight_date'] >= start_date]
        if end_date:
            df = df[df['flight_date'] <= end_date]
        if origin:
            df = df[df['origin'] == origin.upper()]
        if destination:
            df = df[df['destination'] == destination.upper()]
            
        # Apply cursor-based pagination
        if cursor:
            last_date, last_id = decode_cursor(cursor)
            if last_date and last_id is not None:
                df = df[
                    (df['flight_date'] > last_date) |
                    ((df['flight_date'] == last_date) & (df['index'] > last_id))
                ]
        
        # Sort for consistent pagination
        df = df.sort_values(['flight_date', 'index'])
        
        # Get page of results
        page_df = df.head(limit)
        
        # Create next cursor if there are more results
        next_cursor = None
        if len(page_df) == limit and len(df) > limit:
            last_row = page_df.iloc[-1]
            next_cursor = encode_cursor(
                str(last_row['flight_date'].date()),
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
        # Load processed flight data from CSV
        df = pd.read_csv('data/processed/processed_flights.csv')
        # Convert flight_date back to datetime
        df['flight_date'] = pd.to_datetime(df['flight_date'])
        
        # Get date range parameters
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        # Apply date filters if provided
        if start_date:
            df = df[df['flight_date'] >= start_date]
        if end_date:
            df = df[df['flight_date'] <= end_date]
        
        # Calculate metrics
        top_routes = df.groupby(['origin', 'destination']).size().nlargest(5)
        top_routes_dict = {f"{origin}-{dest}": count 
                          for (origin, dest), count in top_routes.items()}
        
        # Calculate delays based on actual vs scheduled times
        df['is_delayed'] = (pd.to_numeric(df['actual_departure']) > 
                           pd.to_numeric(df['scheduled_departure']))
        
        # Calculate cancellation rate (flights with no actual departure/arrival)
        df['is_cancelled'] = df['actual_departure'].isna() | df['actual_arrival'].isna()
        cancellation_rate = float(df['is_cancelled'].mean() * 100)
        
        # Calculate diversion rate based on available data
        # For now, set to 0 as we don't have diversion information
        diversion_rate = 0.0
        
        metrics = {
            'total_flights': len(df),
            'delay_rate': float(df['is_delayed'].mean() * 100),
            'cancellation_rate': cancellation_rate,
            'diversion_rate': diversion_rate,
            'top_routes': top_routes_dict,
            'top_carriers': df['airline'].value_counts().nlargest(5).to_dict(),
            'date_range': {
                'start': str(df['flight_date'].min().date()),
                'end': str(df['flight_date'].max().date())
            }
        }
        
        # Add average delays
        df['departure_delay'] = pd.to_numeric(df['actual_departure']) - pd.to_numeric(df['scheduled_departure'])
        df['arrival_delay'] = pd.to_numeric(df['actual_arrival']) - pd.to_numeric(df['scheduled_arrival'])
        
        metrics.update({
            'avg_departure_delay': float(df['departure_delay'].mean()),
            'avg_arrival_delay': float(df['arrival_delay'].mean()),
            'max_departure_delay': float(df['departure_delay'].max()),
            'max_arrival_delay': float(df['arrival_delay'].max())
        })
        
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
        # Load processed flight data from CSV
        df = pd.read_csv('data/processed/processed_flights.csv')
        
        origins = df['origin'].unique().tolist()
        destinations = df['destination'].unique().tolist()
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

if __name__ == '__main__':
    app.run(debug=True)