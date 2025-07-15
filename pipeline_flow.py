from prefect import task, flow
import subprocess
import sys
import os

# Define absolute paths to your scripts
EXTRACT_SCRIPT_PATH = r"C:\Users\MY PC\openweather\extract_weather_data.py"
LOAD_SCRIPT_PATH = r"C:\Users\MY PC\openweather\load_to_postgres.py"


@task(name="Extract Weather Data")
def extract_weather_data():
    """Runs the extract_weather_data.py script."""
    result = subprocess.run(
        [sys.executable, EXTRACT_SCRIPT_PATH],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        raise RuntimeError(f"Script failed with error: {result.stderr}")
    return result.stdout


@task(name="Load to Postgres")
def load_to_postgres():
    """Runs the load_to_postgres.py script."""
    result = subprocess.run(
        [sys.executable, LOAD_SCRIPT_PATH],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        raise RuntimeError(f"Script failed with error: {result.stderr}")
    return result.stdout


@flow(name="Weather ETL Pipeline")
def run_pipeline():
    extract_result = extract_weather_data()
    print("Extract Output:", extract_result)

    load_result = load_to_postgres()
    print("Load Output:", load_result)


if __name__ == "__main__":
    run_pipeline()