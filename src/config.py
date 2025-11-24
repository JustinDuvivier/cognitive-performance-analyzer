import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'brain_fog_db'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD')
}

OPENWEATHER_API_KEY = os.getenv('OPENWEATHER_API_KEY')
OPENAQ_API_KEY = os.getenv('OPENAQ_API_KEY')

WEATHER_API_URL = "https://api.openweathermap.org/data/2.5/weather"
AIR_QUALITY_API_URL = "https://api.openaq.org/v3/latest"
AIR_POLLUTION_API_URL = "http://api.openweathermap.org/data/2.5/air_pollution"

LOCATION = {
    'city': 'New York',
    'country': 'US',
    'lat': 40.7128,
    'lon': -74.0060
}

BATCH_SIZE = 100
LOG_LEVEL = 'INFO'

REQUEST_TIMEOUT = 30