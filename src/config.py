import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'brain_fog_db'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD')
}

# API configuration
OPENWEATHER_API_KEY = os.getenv('OPENWEATHER_API_KEY')
OPENAQ_API_KEY = os.getenv('OPENAQ_API_KEY')  # OpenAQ v3 doesn't require key for basic access

# API endpoints - UPDATED for 2025
WEATHER_API_URL = "https://api.openweathermap.org/data/2.5/weather"
AIR_QUALITY_API_URL = "https://api.openaq.org/v3/latest"  # Updated to v3

# Location settings (modify these for your location)
LOCATION = {
    'city': 'New York',  # Update with your city
    'country': 'US',
    'lat': 40.7128,  # Update with your latitude
    'lon': -74.0060  # Update with your longitude
}

# Data collection settings
BATCH_SIZE = 100
LOG_LEVEL = 'INFO'

# API request timeout
REQUEST_TIMEOUT = 30  # seconds