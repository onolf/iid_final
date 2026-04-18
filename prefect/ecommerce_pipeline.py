import requests
from prefect import flow, task
from prefect.logging import get_run_logger
import subprocess, os
from dotenv import load_dotenv

load_dotenv()

AIRBYTE_URL = os.getenv("AIRBYTE_URL", "http://localhost:8000")
AIRBYTE_CONNECTION_ID = os.getenv("AIRBYTE_CONNECTION_ID")
AIRBYTE_USER = os.getenv("AIRBYTE_USER")
AIRBYTE_PASS = os.getenv("AIRBYTE_PASS")

CONNECTIONS = {
    "olist_orders":           os.getenv("OLIST_ORDERS_UUID"),
    "olist_order_items":      os.getenv("OLIST_ORDER_ITEMS_UUID"),
    "olist_customers":        os.getenv("OLIST_CUSTOMERS_UUID"),
    "olist_geolocation":      os.getenv("OLIST_GEOLOCATION_UUID"),
    "olist_products":         os.getenv("OLIST_PRODUCTS_UUID"),
    "olist_sellers":          os.getenv("OLIST_SELLERS_UUID"),
    "olist_order_payments":   os.getenv("OLIST_ORDER_PAYMENTS_UUID"),
    "olist_order_reviews":    os.getenv("OLIST_ORDER_REVIEWS_UUID"),
    "weather_stations":       os.getenv("WEATHER_STATIONS_UUID"),
    "weather_stations_codes": os.getenv("WEATHER_STATIONS_CODES_UUID"),
    "weather_wind_codes":     os.getenv("WEATHER_WIND_CODES_UUID"),
}

@task(name="airbyte_sync", retries=2, retry_delay_seconds=30)
def trigger_airbyte_sync(connection_name: str, connection_id: str):
    logger = get_run_logger()
    logger.info(f"Sincronizando: {connection_name}...")

    response = requests.post(
        f"{AIRBYTE_URL}/api/v1/connections/sync",
        json={"connectionId": connection_id},
        headers={"Content-Type": "application/json"},
        auth=(AIRBYTE_USER, AIRBYTE_PASS)
    )

    if response.status_code != 200:
        raise Exception(f"{connection_name} falló: {response.status_code} {response.text}")

    job_id = response.json()["job"]["id"]
    logger.info(f"{connection_name} iniciado — job_id: {job_id}")
    return job_id

@task(name="dbt_run", retries=2, retry_delay_seconds=10)
def run_dbt_models():
    logger = get_run_logger()
    logger.info("Ejecutando modelos dbt...")
    result = subprocess.run(
        ["dbt", "run",
         "--project-dir", os.path.expanduser("~/iid_final/dbt"),
         "--profiles-dir", os.path.expanduser("~/iid_final/dbt")],
        capture_output=True, text=True
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        raise Exception(f"dbt run falló:\n{result.stderr}")
    return "dbt run completado"

@task(name="dbt_test", retries=1, retry_delay_seconds=5)
def run_dbt_tests():
    logger = get_run_logger()
    logger.info("Ejecutando tests de calidad...")
    result = subprocess.run(
        ["dbt", "test",
         "--project-dir", os.path.expanduser("~/iid_final/dbt"),
         "--profiles-dir", os.path.expanduser("~/iid_final/dbt")],
        capture_output=True, text=True
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        raise Exception(f"dbt test falló:\n{result.stderr}")
    return "dbt test completado"

@flow(name="olist_weather_pipeline", log_prints=True)
def olist_pipeline():
    logger = get_run_logger()
    logger.info("Iniciando pipeline Olist + Weather → MotherDuck")

    try:
        # Disparar todos los syncs EN PARALELO (submit = no espera)
        sync_futures = [
            trigger_airbyte_sync.submit(name, conn_id)
            for name, conn_id in CONNECTIONS.items()
        ]

        # dbt corre solo cuando TODOS los syncs terminaron
        run_result  = run_dbt_models(wait_for=sync_futures)
        test_result = run_dbt_tests(wait_for=[run_result])

        logger.info("Pipeline completado exitosamente ✓")

    except Exception as e:
        logger.error(f"Pipeline falló: {str(e)}")
        raise

if __name__ == "__main__":
    olist_pipeline()
