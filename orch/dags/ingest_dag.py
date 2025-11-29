from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.email import EmailOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import datetime, timedelta, timezone
import json
import logging
import os

API_CONN_ID = "agua_latam_api"
LOG_FILE = "/opt/airflow/logs/main_ingest.json"   

logger = logging.getLogger("main_ingest_logger")
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.FileHandler(LOG_FILE)
    formatter = logging.Formatter('%(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

def log_result(task_id, endpoint, status, message=""):
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "task_id": task_id,
        "endpoint": endpoint,
        "status": status,
        "message": message,
    }

    if os.path.exists(LOG_FILE):
        try:
            with open(LOG_FILE, "r") as f:
                data = json.load(f)
        except json.JSONDecodeError:
            data = {"logs": []}
    else:
        data = {"logs": []}

    data["logs"].append(entry)

    with open(LOG_FILE, "w") as f:
        json.dump(data, f, indent=2)

    logger.info(json.dumps(entry))

def healthcheck(task_id):
    try:
        hook = HttpHook(method="GET", http_conn_id=API_CONN_ID)
        response = hook.run("/health")

        if response.status_code != 200:
            raise ValueError(f"Healthcheck FAILED ({response.status_code})")

        data = response.json()
        if data.get("status") != "ok":
            raise ValueError("Healthcheck returned non-ok")

        log_result(task_id, "/health", "SUCCESS", json.dumps(data))
        return True

    except Exception as e:
        log_result(task_id, "/health", "FAILURE", str(e))
        raise

def call_endpoint_with_logging(task_id, endpoint, params=None):
    try:
        hook = HttpHook(method="GET", http_conn_id=API_CONN_ID)

        if params:
            response = hook.run(endpoint, data=params)
        else:
            response = hook.run(endpoint)

        if response.status_code != 200:
            raise ValueError(f"Endpoint {endpoint} failed ({response.status_code})")

        log_result(task_id, endpoint, "SUCCESS", response.text)
        return response.text

    except Exception as e:
        log_result(task_id, endpoint, "FAILURE", str(e))
        raise

def run_once_check(task_id):
    if Variable.get(task_id, default_var=None):   # flag ya existe
        return False

    Variable.set(task_id, "done")                 # setea flag
    return True

def is_yearly_run():
    today = datetime.now(timezone.utc)
    return today.month == 1 and today.day == 10   # <-- tu fecha anual

def should_run_source2():
    today = datetime.now(timezone.utc)
    current_year = today.year

    last_run_year = Variable.get("source2_last_run_year", default_var=None)

    if last_run_year is not None and int(last_run_year) == current_year:
        print(f"Source2 ya se ejecutó en {current_year}. No se vuelve a ejecutar.")
        return False

    print(f"Source2 NO se ha ejecutado en {current_year}. Se ejecutará.")
    return True

def mark_source2_year():
    today = datetime.now(timezone.utc)
    current_year = today.year
    Variable.set("source2_last_run_year", str(current_year))
    print(f"Source2 marcado como ejecutado en {current_year}")

default_args = {
    "owner": "pf-shde",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="main_ingest",
    start_date=days_ago(1),
    schedule_interval="0 6 * * *",
    catchup=False,
    default_args=default_args,
    tags=["ingestion"],
) as dag:

    run_source1_once = ShortCircuitOperator(
        task_id="run_source1_once",
        python_callable=lambda: run_once_check("ingest_source1_once_flag"),
    )

    health_source1 = PythonOperator(
        task_id="health_source1",
        python_callable=lambda: healthcheck("health_source1"),
    )

    paramsSource1 = {"countries": "ARG", "indicators": "NY.GDP", "start_year":2023, "end_year":2024, "isTest":True}

    task_source1 = PythonOperator(
        task_id="ingest_source1_once",
        python_callable=lambda: call_endpoint_with_logging("ingest_source1_once", "/ingest/world-bank", paramsSource1),
        retries=3,
        retry_delay=timedelta(minutes=5),
    )

    notify_failure_source1 = EmailOperator(
        task_id="notify_failure_source1",
        to="c4devx@gmail.com",
        subject="Fallo en ingest_source1_once",
        html_content="<p>Revisar logs en /opt/airflow/logs/main_ingest.json</p>",
        trigger_rule="one_failed",
    )

    notify_success_source1 = EmailOperator(
    task_id="notify_success_source1",
    to="c4devx@gmail.com",
    subject="Éxito en ingest_source1_once",
    html_content="<p>La tarea ingest_source1_once finalizó correctamente.</p>",
    trigger_rule="all_success",
    conn_id="mailgun_mtp",  
    )

    run_source1_once >> health_source1 >> task_source1 
    task_source1 >> [notify_failure_source1, notify_success_source1]

    yearly_gate = ShortCircuitOperator(
        task_id="yearly_gate",
        python_callable=should_run_source2, 
    )

    health_source2 = PythonOperator(
        task_id="health_source2",
        python_callable=lambda: healthcheck("health_source2"),
    )

    paramsSource2 = {"country": "ARG", "latitude": 45, "longitude": 45, "start_year":"2023_10_01", "end_year":"2024_10_01", "isTest":True}
    
    task_source2 = PythonOperator(
        task_id="ingest_source2",
        python_callable=lambda: call_endpoint_with_logging("ingest_source2_yearly", "/ingest/world-bank", paramsSource1),
    )

    mark_year = PythonOperator(
        task_id="mark_source2_year",
        python_callable=mark_source2_year,
    )

    
    notify_failure_source2 = EmailOperator(
        task_id="notify_failure_source2",
        to="c4devx@gmail.com",
        subject="Falló ingest_source2",
        html_content="<p>La tarea <b>ingest_source2</b> falló. Revisar logs en /opt/airflow/logs/main_ingest.json</p>",
        trigger_rule="one_failed",   
    )

    notify_success_source2 = EmailOperator(
        task_id="notify_success_source2",
        to="c4devx@gmail.com",
        subject="Éxito ingest_source2",
        html_content="<p>La tarea <b>ingest_source2</b> finalizó correctamente.</p>",
        trigger_rule="all_success",   
        conn_id="mailgun_mtp", 
    )

    yearly_gate >> health_source2 >> task_source2

    task_source2 >> mark_year >> notify_success_source2

    [health_source2, task_source2] >> notify_failure_source2


    health_source3 = PythonOperator(
        task_id="health_source3",
        python_callable=lambda: healthcheck("health_source3"),
    )

    paramsSource3 = {"country": "ARG", "start_date": "2023-10-01", "end_date":"2024-10-01", "isTest":True}

    task_source3 = PythonOperator(
        task_id="ingest_source3_daily",
        python_callable=lambda: call_endpoint_with_logging("ingest_source3_daily", "/ingest/open-meteo", paramsSource3),
        retries=3,
        retry_delay=timedelta(minutes=5),
    )

    notify_failure_source3 = EmailOperator(
        task_id="notify_failure_source3",
        to="c4devx@gmail.com",
        subject="Falló ingest_source3",
        html_content="<p>La tarea <b>ingest_source3</b> falló. Revisar logs en /opt/airflow/logs/main_ingest.json</p>",
        trigger_rule="one_failed",   
    )

    notify_success_source3 = EmailOperator(
        task_id="notify_success_source3",
        to="c4devx@gmail.com",
        subject="Éxito ingest_source3",
        html_content="<p>La tarea <b>ingest_source3</b> finalizó correctamente.</p>",
        trigger_rule="all_success",   
        conn_id="mailgun_mtp", 
    )

    health_source3 >> task_source3 >> [notify_success_source3, notify_failure_source3]

