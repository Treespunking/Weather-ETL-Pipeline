import os
import yaml
from datetime import datetime
from pymongo import MongoClient
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Load config
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Override config values with environment variables
config['postgres']['password'] = os.getenv('POSTGRES_PASSWORD', config['postgres'].get('password', ''))

if not config['postgres']['password']:
    raise EnvironmentError("Environment variable 'POSTGRES_PASSWORD' is missing.")


def connect_mongo():
    client = MongoClient(config["mongo"]["host"], config["mongo"]["port"])
    db = client[config["mongo"]["db"]]
    collection = db[config["mongo"]["collection"]]
    return collection


def connect_postgres():
    conn = psycopg2.connect(
        host=config["postgres"]["host"],
        port=config["postgres"]["port"],
        dbname=config["postgres"]["dbname"],
        user=config["postgres"]["user"],
        password=config["postgres"]["password"]
    )
    return conn


def create_table_if_not_exists(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS weather_forecast (
                id SERIAL PRIMARY KEY,
                city VARCHAR(100),
                country VARCHAR(10),
                lat NUMERIC,
                lon NUMERIC,
                fetched_at TIMESTAMP,
                forecast_time TIMESTAMP,
                temp NUMERIC,
                feels_like NUMERIC,
                temp_min NUMERIC,
                temp_max NUMERIC,
                pressure INT,
                humidity INT,
                wind_speed NUMERIC,
                wind_deg INT,
                clouds INT,
                weather_main VARCHAR(50),
                weather_description TEXT,
                pod VARCHAR(2)
            );
        """)
        conn.commit()


def extract_data_from_mongo(collection):
    documents = collection.find()
    transformed_records = []

    for doc in documents:
        city = doc.get("city")
        country = doc.get("country")
        lat = doc.get("lat")
        lon = doc.get("lon")
        fetched_at = doc.get("fetched_at")

        forecasts = doc.get("raw_forecast_json", {}).get("list", [])
        for forecast in forecasts:
            main = forecast.get("main", {})
            weather = forecast.get("weather", [{}])[0]
            wind = forecast.get("wind", {})
            clouds = forecast.get("clouds", {})

            record = (
                city,
                country,
                lat,
                lon,
                fetched_at,
                forecast.get("dt_txt"),
                main.get("temp"),
                main.get("feels_like"),
                main.get("temp_min"),
                main.get("temp_max"),
                main.get("pressure"),
                main.get("humidity"),
                wind.get("speed"),
                wind.get("deg"),
                clouds.get("all"),
                weather.get("main"),
                weather.get("description"),
                forecast.get("sys", {}).get("pod")
            )

            transformed_records.append(record)

    return transformed_records


def load_into_postgres(data):
    conn = connect_postgres()
    try:
        create_table_if_not_exists(conn)
        insert_query = """
            INSERT INTO weather_forecast (
                city, country, lat, lon, fetched_at, forecast_time,
                temp, feels_like, temp_min, temp_max, pressure, humidity,
                wind_speed, wind_deg, clouds, weather_main, weather_description, pod
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """
        with conn.cursor() as cur:
            execute_batch(cur, insert_query, data, page_size=1000)
        conn.commit()
        print(f"Loaded {len(data)} records into PostgreSQL.")
    finally:
        conn.close()


if __name__ == "__main__":
    mongo_collection = connect_mongo()
    records = extract_data_from_mongo(mongo_collection)
    if records:
        load_into_postgres(records)
    else:
        print("No data found in MongoDB to load.")