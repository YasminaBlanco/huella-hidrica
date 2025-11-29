from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import json
from datetime import timedelta


API_CONN_ID = "agua_latam_api"  


def check_api_health(**context):
    hook = HttpHook(method="GET", http_conn_id=API_CONN_ID)
    response = hook.run("/health")

    if response.status_code != 200:
        raise ValueError(f"Healthcheck FAILED. Status: {response.status_code}")

    data = response.json()
    print("API Healthcheck data:", json.dumps(data, indent=2))

    if data.get("status") != "ok":
        raise ValueError("API healthcheck returned non-ok status.")

    return "API is healthy."


default_args = {
    "owner": "pi-shde",
    "retries": 2,
    "retry_delay": timedelta(seconds=20),
}

with DAG(
    dag_id="test_api",
    default_args=default_args,
    description="DAG que verifica el healthcheck de la API",
    schedule_interval="@hourly",
    start_date=days_ago(1),
    catchup=False,
    tags=["api", "healthcheck"],
) as dag:

    task_check_api = PythonOperator(
        task_id="check_api_health",
        python_callable=check_api_health,
    )

    task_check_api
