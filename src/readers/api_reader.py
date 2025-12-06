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
    OPENAQ_API_URL,
    AIR_POLLUTION_API_URL,
    REQUEST_TIMEOUT,
    WEATHER_UNITS,
    OPENAQ_SEARCH_RADIUS,
    OPENAQ_RESULT_LIMIT,
)
from loaders.load import get_pressure_24h_ago, get_all_persons
from cleaners.clean import clean_timestamp

logger = logging.getLogger(__name__)

_location_cache = {}


def _get_location_key(lat: float, lon: float) -> str:
    return f"{round(lat, 4)},{round(lon, 4)}"


def fetch_weather_data_for_location(lat: float, lon: float) -> dict | None:
    if not OPENWEATHER_API_KEY:
        logger.warning("OPENWEATHER_API_KEY not configured")
        return None

    try:
        params = {
            'lat': lat,
            'lon': lon,
            'appid': OPENWEATHER_API_KEY,
            'units': WEATHER_UNITS
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
        logger.error(f"Error fetching weather data for ({lat}, {lon}): {e}")
        return None


def fetch_air_quality_for_location(lat: float, lon: float) -> dict:
    aq_record = {'pm25': None, 'aqi': None}

    if not OPENAQ_API_KEY:
        return aq_record

    try:
        headers = {'X-API-Key': OPENAQ_API_KEY, 'Accept': 'application/json'}
        params = {
            'coordinates': f"{lon},{lat}",
            'radius': OPENAQ_SEARCH_RADIUS,
            'limit': OPENAQ_RESULT_LIMIT
        }

        response = requests.get(
            OPENAQ_API_URL,
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
        logger.error(f"Error fetching air quality for ({lat}, {lon}): {e}")

    return aq_record


def fetch_additional_pollutants_for_location(lat: float, lon: float) -> dict:
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
            'lat': lat,
            'lon': lon,
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
        logger.error(f"Error fetching detailed pollutants for ({lat}, {lon}): {e}")

    return pollutants


def calculate_aqi_from_pm25(pm25: float | None) -> int | None:
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


def calculate_pressure_change(current_pressure: float | None, current_timestamp: str, person_id: int | None = None) -> float:
    if current_pressure is None:
        return 0

    normalized_timestamp = clean_timestamp(current_timestamp)
    historical_pressure = get_pressure_24h_ago(normalized_timestamp, person_id)

    if historical_pressure is None:
        logger.debug("No historical pressure data available, pressure_change_24h set to 0")
        return 0

    pressure_change = current_pressure - historical_pressure
    logger.debug(f"Pressure change: {current_pressure} - {historical_pressure} = {pressure_change:.2f} hPa")
    return round(pressure_change, 2)


def _fetch_location_data(lat: float, lon: float) -> dict | None:
    location_key = _get_location_key(lat, lon)

    if location_key in _location_cache:
        logger.debug(f"Using cached data for location ({lat}, {lon})")
        return _location_cache[location_key].copy()

    weather_data = fetch_weather_data_for_location(lat, lon)
    if not weather_data:
        return None

    aq_data = fetch_air_quality_for_location(lat, lon)
    pollutant_data = fetch_additional_pollutants_for_location(lat, lon)

    weather_data.update(aq_data)
    weather_data.update(pollutant_data)

    _location_cache[location_key] = weather_data
    return weather_data.copy()


def fetch_all_external_data() -> list[dict]:
    global _location_cache
    _location_cache = {}

    persons = get_all_persons()

    if not persons:
        logger.warning("No persons found in database with location data")
        return []

    locations_by_key = {}
    for person in persons:
        key = _get_location_key(person['latitude'], person['longitude'])
        if key not in locations_by_key:
            locations_by_key[key] = {
                'lat': person['latitude'],
                'lon': person['longitude'],
                'location_name': person.get('location_name', 'unknown'),
                'persons': []
            }
        locations_by_key[key]['persons'].append(person)

    logger.info(f"Fetching data for {len(locations_by_key)} unique locations ({len(persons)} persons)")

    all_records = []

    for location_key, location_info in locations_by_key.items():
        lat = location_info['lat']
        lon = location_info['lon']
        location_name = location_info['location_name']

        location_data = _fetch_location_data(lat, lon)

        if not location_data:
            for person in location_info['persons']:
                logger.warning(f"Failed to fetch external data for {person['name']}")
            continue

        logger.info(f"Fetched external data for {location_name} (shared by {len(location_info['persons'])} persons)")

        for person in location_info['persons']:
            record = location_data.copy()
            record['person_id'] = person['person_id']
            record['person'] = person['name']

            current_pressure = record.get('pressure_hpa')
            current_timestamp = record.get('timestamp')
            if current_pressure and current_timestamp:
                record['pressure_change_24h'] = calculate_pressure_change(
                    current_pressure,
                    current_timestamp,
                    person['person_id']
                )

            all_records.append(record)

    return all_records
