import requests
from prefect import flow, task
from prefect.logging import get_run_logger
import subprocess, os

AIRBYTE_URL = "http://localhost:8000"
AIRBYTE_CONNECTION_ID = "6bdbca65-cffa-4a63-815e-41e2d360e075"

@task(name="airbyte_sync", retries=2, retry_delay_seconds=30)
def trigger_airbyte_sync():
    logger = get_run_logger()
    logger.info("Disparando sync en Airbyte...")
    
    # Trigger sync via API
    response = requests.post(
        f"{AIRBYTE_URL}/api/v1/connections/sync",
        json={"connectionId": AIRBYTE_CONNECTION_ID},
        headers={"Content-Type": "application/json"}
    )
    
    if response.status_code != 200:
        raise Exception(f"Airbyte sync falló: {response.text}")
    
    job_id = response.json()["job"]["id"]
    logger.info(f"Sync iniciado — job_id: {job_id}")
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
        sync_result  = trigger_airbyte_sync()
        run_result   = run_dbt_models(wait_for=[sync_result])
        test_result  = run_dbt_tests(wait_for=[run_result])
        logger.info("Pipeline completado exitosamente ✓")
    except Exception as e:
        logger.error(f"Pipeline falló: {str(e)}")
        raise

if __name__ == "__main__":
    olist_pipeline()
