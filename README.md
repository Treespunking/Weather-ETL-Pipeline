# Weather-ETL-Pipeline

## Project Summary
A comprehensive weather data pipeline that extracts 5-day weather forecasts from OpenWeatherMap API, stores raw data in MongoDB, transforms it, and loads it into PostgreSQL for analysis. The pipeline is orchestrated using Prefect for workflow management.

## Architecture Overview

```
OpenWeatherMap API → MongoDB (Raw Data) → PostgreSQL (Structured Data)
                    ↑                     ↑
            extract_weather_data.py   load_to_postgres.py
                    ↑___________________|
                        pipeline_flow.py (Prefect Orchestration)
```

## Script Breakdown

### 1. `extract_weather_data.py` - Data Extraction
**Purpose**: Fetches weather data from OpenWeatherMap API and stores raw data in MongoDB

**Key Functions**:
- `get_lat_lon()`: Geocodes city names to latitude/longitude coordinates
- `fetch_weather_forecast()`: Retrieves 5-day weather forecasts (3-hour intervals)
- `load_to_mongo()`: Stores raw forecast data with metadata in MongoDB

**Data Flow**:
1. Reads city configurations from `config.yaml`
2. Geocodes each city to get coordinates
3. Fetches 5-day forecast data from OpenWeatherMap API
4. Adds metadata (city, country, coordinates, fetch timestamp)
5. Stores raw JSON data in MongoDB collection

### 2. `load_to_postgres.py` - Data Transformation & Loading
**Purpose**: Extracts data from MongoDB, transforms it into structured format, and loads into PostgreSQL

**Key Functions**:
- `connect_mongo()`: Establishes MongoDB connection
- `connect_postgres()`: Establishes PostgreSQL connection
- `create_table_if_not_exists()`: Creates weather_forecast table schema
- `extract_data_from_mongo()`: Transforms nested JSON into flat records
- `load_into_postgres()`: Bulk inserts structured data using batch processing

**Data Transformation**:
- Flattens nested JSON structure from OpenWeatherMap API
- Extracts weather parameters: temperature, humidity, pressure, wind, clouds
- Normalizes forecast timestamps and location data
- Handles missing data gracefully

**PostgreSQL Schema**:
```sql
weather_forecast (
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
```

### 3. `pipeline_flow.py` - Workflow Orchestration
**Purpose**: Orchestrates the ETL pipeline using Prefect for task management and monitoring

**Key Components**:
- `@task` decorators for individual pipeline steps
- `@flow` decorator for overall pipeline orchestration
- Subprocess execution for script isolation
- Error handling and logging

**Execution Flow**:
1. Runs extraction script to fetch and store raw data
2. Runs transformation/loading script to process and store structured data
3. Provides output logging and error handling

## Technical Stack

### Dependencies
- **API**: OpenWeatherMap API for weather data
- **Database**: MongoDB (raw data storage), PostgreSQL (structured data)
- **Orchestration**: Prefect for workflow management
- **Libraries**: `requests`, `pymongo`, `psycopg2`, `yaml`, `python-dotenv`

### Configuration Management
- **Environment Variables**: API keys and database passwords stored securely
- **YAML Configuration**: Database connections, city lists, and other settings
- **Error Handling**: Comprehensive validation for missing environment variables

### Security Features
- Environment variable usage for sensitive data
- Password protection for database connections
- API key management through environment variables

## Data Processing Capabilities

### Weather Parameters Captured
- Temperature metrics (actual, feels-like, min/max)
- Atmospheric conditions (pressure, humidity)
- Wind data (speed, direction)
- Cloud coverage
- Weather descriptions and classifications
- Time-based forecasting (3-hour intervals over 5 days)

### Batch Processing
- Efficient bulk inserts using `execute_batch()`
- Configurable page size for memory optimization
- Transaction management for data integrity

## Use Cases

### Business Applications
- Weather monitoring and alerting systems
- Agricultural planning and decision support
- Tourism and event planning
- Supply chain and logistics optimization
- Energy consumption forecasting

### Data Analysis
- Historical weather trend analysis
- Forecast accuracy assessment
- Regional weather pattern comparison
- Climate monitoring and reporting

## Deployment Considerations

### Scalability
- Modular design allows independent scaling of components
- MongoDB handles high-volume raw data storage
- PostgreSQL optimized for analytical queries
- Prefect provides distributed task execution

### Monitoring
- Prefect dashboard for pipeline monitoring
- Comprehensive logging throughout the pipeline
- Error tracking and alerting capabilities
- Performance metrics collection

### Maintenance
- Configuration-driven approach for easy updates
- Environment-specific settings management
- Automated table creation and schema management
- Graceful error handling and recovery
