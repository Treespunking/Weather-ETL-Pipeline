from dotenv import load_dotenv
import os
import requests
import yaml
from datetime import datetime, timedelta
from pymongo import MongoClient

# Load environment variables from .env
load_dotenv()

# Load config
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Override config values with environment variables
config['api_key'] = os.getenv('WEATHER_API_KEY', config.get('api_key', ''))
config['postgres']['password'] = os.getenv('POSTGRES_PASSWORD', config['postgres'].get('password', ''))

# Validate required environment variables
if not config['api_key']:
    raise EnvironmentError("Environment variable 'WEATHER_API_KEY' is missing.")

if not config['postgres']['password']:
    raise EnvironmentError("Environment variable 'POSTGRES_PASSWORD' is missing.")


def get_lat_lon(city: str, country_code: str = "NGA") -> dict:
    """Get latitude and longitude for a given city."""
    url = f"http://api.openweathermap.org/geo/1.0/direct?q={city},{country_code}&limit=1&appid={config['api_key']}"
    response = requests.get(url).json()
    if not response:
        raise ValueError(f"Geocoding failed for {city}, {country_code}")
    return {
        "lat": response[0]["lat"],
        "lon": response[0]["lon"],
        "city": response[0]["name"],
        "country": country_code
    }


def fetch_weather_forecast(lat: float, lon: float) -> dict:
    """Fetch 5-day weather forecast (every 3 hours) from OpenWeatherMap."""
    url = f"http://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={config['api_key']}"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def load_to_mongo(data: dict):
    """Insert raw weather forecast data into MongoDB."""
    client = MongoClient(config["mongo"]["host"], config["mongo"]["port"])
    db = client[config["mongo"]["db"]]
    collection = db[config["mongo"]["collection"]]
    collection.insert_one(data)
    print(f"Inserted forecast data for {data['city']} into MongoDB")


def main():
    # Process each city
    for city_info in config["cities"]:
        city = city_info["name"]
        country_code = city_info["country_code"]

        try:
            geo_data = get_lat_lon(city, country_code)
            lat, lon = geo_data["lat"], geo_data["lon"]
            print(f"Fetched coordinates for {city}: lat={lat}, lon={lon}")

            try:
                print(f"Fetching 5-day forecast for {city}...")
                forecast_data = fetch_weather_forecast(lat, lon)

                # Add metadata to raw data
                raw_data_with_metadata = {
                    "city": city,
                    "country": country_code,
                    "lat": lat,
                    "lon": lon,
                    "fetched_at": datetime.utcnow(),
                    "raw_forecast_json": forecast_data
                }

                # Save to MongoDB
                load_to_mongo(raw_data_with_metadata)

            except Exception as e:
                print(f"Error fetching or inserting forecast data for {city}: {e}")

        except Exception as e:
            print(f"Error fetching geolocation for {city}: {e}")


if __name__ == "__main__":
    main()