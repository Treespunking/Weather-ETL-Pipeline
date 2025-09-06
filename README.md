# Weather Forecast ETL Pipeline

A robust **ETL pipeline** to extract weather forecast data from **OpenWeatherMap API**, store raw JSON in **MongoDB**, and load structured data into **PostgreSQL** for analytics. Orchestrated with **Prefect** for reliable, automated workflows.

![Python](https://img.shields.io/badge/Python-3.8%2B-blue)
![OpenWeatherMap](https://img.shields.io/badge/API-OpenWeatherMap-orange)
![MongoDB](https://img.shields.io/badge/Storage-MongoDB-green)
![PostgreSQL](https://img.shields.io/badge/Database-PostgreSQL-blueviolet)

---

## Features
- Fetches 5-day weather forecasts (3-hour intervals) from **OpenWeatherMap**
- Geocodes cities to latitude/longitude dynamically
- Stores raw JSON in **MongoDB** for auditability and reprocessing
- Transforms and loads clean data into **PostgreSQL** for analysis
- Configurable via `config.yaml` and environment variables
- Orchestrated using **Prefect** for workflow automation
- Modular design with error handling and logging
- Supports multiple cities and countries

---

## Requirements
- Python 3.8+
- OpenWeatherMap API key ([get one free here](https://openweathermap.org/api))
- MongoDB instance (local or cloud)
- PostgreSQL database
- Prefect (for orchestration)
- `pip` and `dotenv` for dependency & secret management

---

## Setup & Installation

### 1. Clone the repo
```bash
git clone https://github.com/Treespunking/Weather-ETL-Pipeline.git
cd Weather-ETL-Pipeline
```

### 2. Set up virtual environment
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

> If you don’t have a `requirements.txt`, create one with:
```txt
requests
pymongo
psycopg2-binary
PyYAML
python-dotenv
prefect
```

### 4. Configure environment variables
Create a `.env` file:
```env
WEATHER_API_KEY=your_openweathermap_api_key
POSTGRES_PASSWORD=your_postgres_password
```
> Never commit this file! It should be in `.gitignore`.

### 5. Update configuration
Edit `config.yaml` to customize cities and database settings:
```yaml
api_key: ""  # Will be overridden by .env
cities:
  - name: Lagos
    country_code: NGA
  - name: Abuja
    country_code: NGA
  - name: New York
    country_code: USA
mongo:
  host: localhost
  port: 27017
  db: weather_db
  collection: forecasts
postgres:
  host: localhost
  port: 5432
  dbname: analytics_db
  user: postgres
  password: ""  # Will be overridden by .env
```

### 6. Run the pipeline
```bash
python pipeline_flow.py
```
> This orchestrates the full ETL: extract → transform → load.

---

## Data Flow Overview

```
[OpenWeatherMap API]
        ↓
[Geocode City → Lat/Lon]
        ↓
[Fetch 5-day Forecast]
        ↓
[MongoDB (Raw JSON)]
        ↓
[Transform: Flatten & Clean]
        ↓
[PostgreSQL (Structured Table)]
```

### PostgreSQL Table: `weather_forecast`
| Column | Type |
|-------|------|
| city | VARCHAR |
| country | VARCHAR |
| lat | NUMERIC |
| lon | NUMERIC |
| fetched_at | TIMESTAMP |
| forecast_time | TIMESTAMP |
| temp | NUMERIC |
| feels_like | NUMERIC |
| temp_min | NUMERIC |
| temp_max | NUMERIC |
| pressure | INT |
| humidity | INT |
| wind_speed | NUMERIC |
| wind_deg | INT |
| clouds | INT |
| weather_main | VARCHAR |
| weather_description | TEXT |
| pod | VARCHAR |

---

## Test the Components

### Run extraction only
```bash
python extract_weather_data.py
```

### Run loading only
```bash
python load_to_postgres.py
```

### Verify data in PostgreSQL
```sql
SELECT city, forecast_time, temp, weather_main FROM weather_forecast LIMIT 10;
```

---

## Project Structure

```
weather-etl-pipeline/
│
├── pipeline_flow.py         # Prefect workflow orchestrator
├── extract_weather_data.py  # Extract: API → MongoDB
├── load_to_postgres.py      # Load: MongoDB → PostgreSQL
├── config.yaml              # Configuration file
├── .env                     # Environment variables (secrets)
├── requirements.txt         # Dependencies
│
└── data/
    └── raw/                 # Optional: backup of raw JSON
```

---

## Orchestration with Prefect

This pipeline uses **Prefect** to manage task dependencies and retries:

```python
@flow(name="Weather ETL Pipeline")
def run_pipeline():
    extract_result = extract_weather_data()
    load_result = load_to_postgres()
```

To run with Prefect UI:
```bash
prefect orion start
python pipeline_flow.py
```

---
