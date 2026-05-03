"""
Weather API Integration Module
Handles all communication with OpenWeatherMap API
"""
import requests
from typing import Dict, List, Optional
from datetime import datetime


class WeatherAPI:
    """Class to interact with OpenWeatherMap API"""
    
    BASE_URL = "https://api.openweathermap.org/data/2.5"
    
    def __init__(self, api_key: str):
        """
        Initialize Weather API client
        
        Args:
            api_key (str): OpenWeatherMap API key
        """
        if not api_key:
            raise ValueError("API key is required")
        self.api_key = api_key
        self.session = requests.Session()
    
    def get_current_weather(self, city: str, units: str = 'metric') -> Optional[Dict]:
        """
        Fetch current weather for a city
        
        Args:
            city (str): City name
            units (str): Temperature unit - 'metric' or 'imperial'
        
        Returns:
            dict: Weather data or None if error occurs
        """
        try:
            url = f"{self.BASE_URL}/weather"
            params = {
                'q': city,
                'appid': self.api_key,
                'units': units
            }
            
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            return self._format_weather_data(data, units)
        
        except requests.exceptions.RequestException as e:
            print(f"Error fetching weather: {e}")
            return None
    
    def get_forecast(self, city: str, units: str = 'metric') -> Optional[Dict]:
        """
        Fetch 5-day weather forecast
        
        Args:
            city (str): City name
            units (str): Temperature unit
        
        Returns:
            dict: Forecast data or None
        """
        try:
            url = f"{self.BASE_URL}/forecast"
            params = {
                'q': city,
                'appid': self.api_key,
                'units': units
            }
            
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            return self._format_forecast_data(data, units)
        
        except requests.exceptions.RequestException as e:
            print(f"Error fetching forecast: {e}")
            return None
    
    def get_weather_by_coordinates(self, lat: float, lon: float, 
                                   units: str = 'metric') -> Optional[Dict]:
        """
        Fetch weather by latitude and longitude
        
        Args:
            lat (float): Latitude
            lon (float): Longitude
            units (str): Temperature unit
        
        Returns:
            dict: Weather data or None
        """
        try:
            url = f"{self.BASE_URL}/weather"
            params = {
                'lat': lat,
                'lon': lon,
                'appid': self.api_key,
                'units': units
            }
            
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            return self._format_weather_data(data, units)
        
        except requests.exceptions.RequestException as e:
            print(f"Error fetching weather by coordinates: {e}")
            return None
    
    def search_cities(self, query: str) -> List[Dict]:
        """
        Search for cities by name
        
        Args:
            query (str): City name or part of it
        
        Returns:
            list: List of matching cities
        """
        try:
            url = f"{self.BASE_URL}/find"
            params = {
                'q': query,
                'appid': self.api_key,
                'type': 'like',
                'cnt': 10
            }
            
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            results = []
            
            for city in data.get('list', []):
                results.append({
                    'name': city['name'],
                    'country': city['sys']['country'],
                    'lat': city['coord']['lat'],
                    'lon': city['coord']['lon']
                })
            
            return results
        
        except requests.exceptions.RequestException as e:
            print(f"Error searching cities: {e}")
            return []
    
    @staticmethod
    def _format_weather_data(data: Dict, units: str) -> Dict:
        """
        Format raw API response into usable data
        
        Args:
            data (dict): Raw API response
            units (str): Temperature unit
        
        Returns:
            dict: Formatted weather data
        """
        try:
            temp_symbol = '°C' if units == 'metric' else '°F'
            speed_unit = 'm/s' if units == 'metric' else 'mph'
            
            return {
                'city': data['name'],
                'country': data['sys']['country'],
                'latitude': data['coord']['lat'],
                'longitude': data['coord']['lon'],
                'weather': {
                    'main': data['weather'][0]['main'],
                    'description': data['weather'][0]['description'],
                    'icon': data['weather'][0]['icon']
                },
                'temperature': {
                    'current': round(data['main']['temp'], 1),
                    'feels_like': round(data['main']['feels_like'], 1),
                    'min': round(data['main']['temp_min'], 1),
                    'max': round(data['main']['temp_max'], 1),
                    'unit': temp_symbol
                },
                'humidity': data['main']['humidity'],
                'pressure': data['main']['pressure'],
                'visibility': data.get('visibility', 'N/A'),
                'wind': {
                    'speed': round(data['wind']['speed'], 1),
                    'degree': data['wind'].get('deg', 0),
                    'unit': speed_unit
                },
                'clouds': data['clouds']['all'],
                'sunrise': datetime.fromtimestamp(data['sys']['sunrise']).strftime('%H:%M'),
                'sunset': datetime.fromtimestamp(data['sys']['sunset']).strftime('%H:%M'),
                'timestamp': datetime.now().isoformat()
            }
        
        except (KeyError, TypeError) as e:
            print(f"Error formatting weather data: {e}")
            return {}
    
    @staticmethod
    def _format_forecast_data(data: Dict, units: str) -> Dict:
        """
        Format forecast data
        
        Args:
            data (dict): Raw forecast response
            units (str): Temperature unit
        
        Returns:
            dict: Formatted forecast data
        """
        try:
            temp_symbol = '°C' if units == 'metric' else '°F'
            forecasts = []
            
            for item in data['list'][:40]:  # 5 days * 8 (3-hour intervals)
                forecasts.append({
                    'date': datetime.fromtimestamp(item['dt']).strftime('%Y-%m-%d %H:%M'),
                    'temperature': round(item['main']['temp'], 1),
                    'weather': item['weather'][0]['main'],
                    'description': item['weather'][0]['description'],
                    'humidity': item['main']['humidity'],
                    'wind_speed': round(item['wind']['speed'], 1),
                    'icon': item['weather'][0]['icon']
                })
            
            return {
                'city': data['city']['name'],
                'country': data['city']['country'],
                'forecasts': forecasts,
                'temp_unit': temp_symbol
            }
        
        except (KeyError, TypeError) as e:
            print(f"Error formatting forecast data: {e}")
            return {}
