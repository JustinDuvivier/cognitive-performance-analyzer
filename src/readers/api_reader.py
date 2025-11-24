import requests
import logging
from datetime import datetime
import sys
import os

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from config import (
    OPENWEATHER_API_KEY,
    OPENAQ_API_KEY,
    WEATHER_API_URL,
    AIR_POLLUTION_API_URL,
    LOCATION,
    REQUEST_TIMEOUT,
)
from loaders.load import get_pressure_24h_ago
from cleaners.clean import clean_timestamp

logger = logging.getLogger(__name__)


def fetch_weather_data():
    try:
        params = {
            'lat': LOCATION['lat'],
            'lon': LOCATION['lon'],
            'appid': OPENWEATHER_API_KEY,
            'units': 'imperial'
        }

        response = requests.get(WEATHER_API_URL, params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

        data = response.json()

        now = datetime.now()
        timestamp_str = now.strftime("%Y-%m-%d %H:%M")

        weather_record = {
            'timestamp': timestamp_str,
            'pressure_hpa': data['main'].get('pressure'),
            'temperature': data['main'].get('temp'),
            'humidity': data['main'].get('humidity'),
            'hour_of_day': now.hour,
            'day_of_week': now.isoweekday(),
            'weekend': now.isoweekday() >= 6,
            'pressure_change_24h': None
        }

        return weather_record

    except Exception as e:
        logger.error(f"Error fetching weather data: {e}")
        return None


def fetch_air_quality_data():
    aq_record = {'pm25': None, 'aqi': None}

    if not OPENAQ_API_KEY:
        return aq_record

    try:
        headers = {'X-API-Key': OPENAQ_API_KEY, 'Accept': 'application/json'}
        params = {
            'coordinates': f"{LOCATION['lon']},{LOCATION['lat']}",
            'radius': 25000,
            'limit': 100
        }

        response = requests.get(
            "https://api.openaq.org/v3/parameters/2/latest",
            params=params,
            headers=headers,
            timeout=REQUEST_TIMEOUT
        )

        if response.status_code == 200:
            results = response.json().get('results', [])
            if results:
                value = results[0].get('value')
                if value is not None:
                    aq_record['pm25'] = value
                    aq_record['aqi'] = calculate_aqi_from_pm25(value)

    except Exception as e:
        logger.error(f"Error fetching air quality: {e}")

    return aq_record


def fetch_additional_pollutants():
    pollutants = {
        'co': None,
        'no': None,
        'no2': None,
        'o3': None,
        'so2': None,
        'pm10': None,
        'nh3': None,
    }

    if not OPENWEATHER_API_KEY:
        return pollutants

    try:
        params = {
            'lat': LOCATION['lat'],
            'lon': LOCATION['lon'],
            'appid': OPENWEATHER_API_KEY,
        }

        response = requests.get(AIR_POLLUTION_API_URL, params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

        data = response.json()
        items = data.get('list', [])
        if not items:
            return pollutants

        components = items[0].get('components', {}) or {}
        for key in pollutants.keys():
            if key in components:
                pollutants[key] = components[key]

    except Exception as e:
        logger.error(f"Error fetching detailed pollutants from OpenWeather: {e}")

    return pollutants


def calculate_aqi_from_pm25(pm25):
    if pm25 is None:
        return None

    if pm25 <= 12.0:
        return int((50 / 12.0) * pm25)
    elif pm25 <= 35.4:
        return int(50 + (50 / 23.4) * (pm25 - 12.0))
    elif pm25 <= 55.4:
        return int(100 + (50 / 20.0) * (pm25 - 35.4))
    elif pm25 <= 150.4:
        return int(150 + (100 / 95.0) * (pm25 - 55.4))
    else:
        return 250


def calculate_pressure_change(current_pressure, current_timestamp):
    if current_pressure is None:
        return 0

    normalized_timestamp = clean_timestamp(current_timestamp)

    historical_pressure = get_pressure_24h_ago(normalized_timestamp)

    if historical_pressure is None:
        logger.debug("No historical pressure data available, pressure_change_24h set to 0")
        return 0

    pressure_change = current_pressure - historical_pressure
    logger.debug(f"Pressure change: {current_pressure} - {historical_pressure} = {pressure_change:.2f} hPa")
    return round(pressure_change, 2)


def fetch_all_external_data():
    weather_data = fetch_weather_data()
    aq_data = fetch_air_quality_data()
    pollutant_data = fetch_additional_pollutants()

    if not weather_data:
        return None

    weather_data.update(aq_data)
    weather_data.update(pollutant_data)

    current_pressure = weather_data.get('pressure_hpa')
    current_timestamp = weather_data.get('timestamp')
    if current_pressure and current_timestamp:
        weather_data['pressure_change_24h'] = calculate_pressure_change(
            current_pressure,
            current_timestamp,
        )

    return weather_data