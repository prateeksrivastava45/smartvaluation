"""
Main Flask Application for Weather Dashboard
"""
import os
from flask import Flask, render_template, request, jsonify
from dotenv import load_dotenv
from src.weather_api import WeatherAPI
from src.config import Config

# Load environment variables
load_dotenv()

# Initialize Flask app
app = Flask(__name__)
app.config.from_object(Config)

# Initialize Weather API
weather_api = WeatherAPI(os.getenv('OPENWEATHER_API_KEY'))


@app.route('/')
def index():
    """Render the main dashboard page"""
    return render_template('index.html')


@app.route('/api/weather', methods=['GET'])
def get_weather():
    """
    API endpoint to fetch weather data
    Query params:
    - city: City name (required)
    - units: Temperature unit - metric or imperial (default: metric)
    """
    try:
        city = request.args.get('city')
        units = request.args.get('units', 'metric')
        
        if not city:
            return jsonify({'error': 'City name is required'}), 400
        
        # Validate city name
        if len(city) > 100 or not city.isalnum():
            return jsonify({'error': 'Invalid city name'}), 400
        
        # Fetch weather data
        weather_data = weather_api.get_current_weather(city, units)
        
        if not weather_data:
            return jsonify({'error': 'City not found'}), 404
        
        return jsonify(weather_data), 200
    
    except Exception as e:
        print(f"Error fetching weather: {str(e)}")
        return jsonify({'error': 'Failed to fetch weather data'}), 500


@app.route('/api/forecast', methods=['GET'])
def get_forecast():
    """
    API endpoint to fetch 5-day forecast
    Query params:
    - city: City name (required)
    - units: Temperature unit - metric or imperial (default: metric)
    """
    try:
        city = request.args.get('city')
        units = request.args.get('units', 'metric')
        
        if not city:
            return jsonify({'error': 'City name is required'}), 400
        
        # Fetch forecast data
        forecast_data = weather_api.get_forecast(city, units)
        
        if not forecast_data:
            return jsonify({'error': 'City not found'}), 404
        
        return jsonify(forecast_data), 200
    
    except Exception as e:
        print(f"Error fetching forecast: {str(e)}")
        return jsonify({'error': 'Failed to fetch forecast data'}), 500


@app.route('/api/weather/coordinates', methods=['GET'])
def get_weather_by_coordinates():
    """
    API endpoint to fetch weather by coordinates
    Query params:
    - lat: Latitude (required)
    - lon: Longitude (required)
    - units: Temperature unit (default: metric)
    """
    try:
        lat = request.args.get('lat')
        lon = request.args.get('lon')
        units = request.args.get('units', 'metric')
        
        if not lat or not lon:
            return jsonify({'error': 'Latitude and longitude are required'}), 400
        
        # Validate coordinates
        try:
            lat = float(lat)
            lon = float(lon)
            if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
                raise ValueError("Invalid coordinates")
        except ValueError:
            return jsonify({'error': 'Invalid coordinates'}), 400
        
        # Fetch weather data
        weather_data = weather_api.get_weather_by_coordinates(lat, lon, units)
        
        if not weather_data:
            return jsonify({'error': 'Weather data not found'}), 404
        
        return jsonify(weather_data), 200
    
    except Exception as e:
        print(f"Error fetching weather by coordinates: {str(e)}")
        return jsonify({'error': 'Failed to fetch weather data'}), 500


@app.route('/api/search', methods=['GET'])
def search_city():
    """
    API endpoint to search for cities
    Query params:
    - query: Search query (required)
    """
    try:
        query = request.args.get('query')
        
        if not query or len(query) < 2:
            return jsonify({'error': 'Search query too short'}), 400
        
        # Search for cities
        results = weather_api.search_cities(query)
        
        return jsonify({'results': results}), 200
    
    except Exception as e:
        print(f"Error searching cities: {str(e)}")
        return jsonify({'error': 'Search failed'}), 500


@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({'status': 'ok', 'message': 'Weather Dashboard is running'}), 200


@app.errorhandler(404)
def page_not_found(e):
    """Handle 404 errors"""
    return jsonify({'error': 'Page not found'}), 404


@app.errorhandler(500)
def internal_error(e):
    """Handle 500 errors"""
    return jsonify({'error': 'Internal server error'}), 500


if __name__ == '__main__':
    app.run(
        debug=os.getenv('FLASK_DEBUG', False),
        host='0.0.0.0',
        port=int(os.getenv('PORT', 5000))
    )
