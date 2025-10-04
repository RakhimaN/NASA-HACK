from flask import Flask, jsonify, send_from_directory, request
from flask_cors import CORS
import os
from dotenv import load_dotenv
import requests

load_dotenv()

app = Flask(__name__, static_folder='../frontend', static_url_path='/')
CORS(app)

GOOGLE_MAPS_API_KEY = os.environ.get("GOOGLE_MAPS_API_KEY", "YOUR_GOOGLE_MAPS_API_KEY_HERE")

@app.route('/')
def serve_index():
    return send_from_directory(app.static_folder, 'index.html')

@app.route('/api/hello')
def hello_api():
    return jsonify({"message": "Hello from Flask backend!"})

@app.route('/api/google-maps-api-key')
def google_maps_api_key():
    return jsonify({"apiKey": GOOGLE_MAPS_API_KEY})

@app.route('/api/weather')
def get_weather_data():
    lat = request.args.get('lat')
    lon = request.args.get('lon')
    start_date = request.args.get('start_date') # YYYYMMDD
    end_date = request.args.get('end_date')     # YYYYMMDD

    if not all([lat, lon, start_date, end_date]):
        return jsonify({"error": "Missing latitude, longitude, start_date, or end_date"}), 400

    # NASA POWER API endpoint
    # Example: https://power.larc.nasa.gov/api/temporal/daily/point?parameters=T2M,RH2M,WS2M,PRECTOT&community=AG&longitude=-71.09&latitude=42.36&startdate=20200101&enddate=20200130&format=JSON
    
    power_api_url = f"https://power.larc.nasa.gov/api/temporal/daily/point?parameters=T2M,RH2M,WS2M,PRECTOT&community=AG&latitude={lat}&longitude={lon}&startdate={start_date}&enddate={end_date}&format=JSON"

    try:
        response = requests.get(power_api_url)
        response.raise_for_status() # Raise an exception for HTTP errors (4xx or 5xx)
        weather_data = response.json()
        return jsonify(weather_data)
    except requests.exceptions.RequestException as e:
        return jsonify({"error": f"Error fetching data from NASA POWER API: {e}"}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5000)
