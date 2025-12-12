import json
import logging
import os
from datetime import datetime, timedelta, timezone

import boto3
from airflow.models import Variable
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

from airflow import DAG

# ----------------- Config -----------------
API_CONN_ID = "agua_latam_api"
LOG_FILE = "/opt/airflow/logs/main_ingest.json"
PYSPARK_INSTANCE_ID = "i-02024816014b00588"
PYSPARK_PRIVATE_IP = "172.31.11.188"
AWS_REGION = "us-east-2"
SSH_KEY_PATH = "/opt/airflow/.ssh/spark-ap.pem"
BASE_BUCKET = "henry-pf-g2-huella-hidrica"
EMAIL_TO = ""

# Flags para transformaciones
UNIQUE_INGEST_FLAG = "ingest_unique_done_OK"
ANNUAL_INGEST_FLAG = "ingest_annual_done_OK"
WEEKLY_INGEST_FLAG = "ingest_weekly_done_OK"

# ----------------- Logger -----------------
logger = logging.getLogger("main_ingest_logger")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.FileHandler(LOG_FILE)
    formatter = logging.Formatter("%(message)s")
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


# ----------------- Funciones Ingesta -----------------


# healthcheck de la ingest_api
def ingest_api_healthcheck(task_id):
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


# consumo de la ingest_api
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
    if Variable.get(task_id, default_var=None):  # flag ya existe
        return False
    Variable.set(task_id, "done")  # setea flag
    return True


def should_run_annual():
    today = datetime.now(timezone.utc)
    current_year = today.year
    last_run_year = Variable.get("source2_last_run_year", default_var=None)
    if last_run_year is not None and int(last_run_year) == current_year:
        return False
    return True


def mark_annual_done():
    today = datetime.now(timezone.utc)
    current_year = today.year
    Variable.set("source2_last_run_year", str(current_year))


# prender ec2
def start_ec2():
    ec2 = boto3.client("ec2", region_name=AWS_REGION)
    ec2.start_instances(InstanceIds=[PYSPARK_INSTANCE_ID])
    waiter = ec2.get_waiter("instance_running")
    waiter.wait(InstanceIds=[PYSPARK_INSTANCE_ID])
    print("EC2 PySpark lista y corriendo")


# apagar ec2
def stop_ec2():
    ec2 = boto3.client("ec2", region_name=AWS_REGION)
    ec2.stop_instances(InstanceIds=[PYSPARK_INSTANCE_ID])
    print("EC2 PySpark apagada")


def can_run_transformations():
    """Decide si se ejecutan transformaciones según reglas"""
    if not Variable.get(UNIQUE_INGEST_FLAG, default_var=None):
        return False
    if not Variable.get(WEEKLY_INGEST_FLAG, default_var=None):
        return False
    # Si coincide semana de anual, verificar que se haya completado
    today = datetime.now()
    annual_date = datetime(today.year, 1, 10)
    if today.isocalendar()[1] == annual_date.isocalendar()[1]:
        if not Variable.get(ANNUAL_INGEST_FLAG, default_var=None):
            return False
    return True


# ejecutar job de pyspark
def spark_cmd(job):
    today = datetime.now()
    year = today.year
    month = today.month

    match job:
        case "silver":
            script = "main_silver.py"
            needs_date = True
            needs_base = True

        case "silver_model":
            script = "main_silver_model.py"
            needs_date = True
            needs_base = True

        case "gold_model":
            script = "main_gold_model.py"
            needs_date = False
            needs_base = True

        case "test":
            script = "test_spark.py"
            needs_date = False
            needs_base = False

        case _:
            raise ValueError(
                "Job inválido. Opciones: silver | silver_model | gold_model | test"
            )

    base = "cd /opt/elt/app && \\"

    if needs_base:
        base += """
        export BASE_BUCKET=henry-pf-g2-huella-hidrica && \\
        """

    if needs_date:
        base += f"""
        export PROCESS_YEAR={year} && \\
        export PROCESS_MONTH={month} && \\
        """

    spark = f"/opt/bitnami/spark/bin/spark-submit --master local[*] {script}"

    return f"""
        docker exec -i spark bash -lc '
            {base}
            {spark}
        '
    """


# ----------------- DAG -----------------
default_args = {
    "owner": "pf-shde",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="main_ingest",
    start_date=days_ago(1),
    schedule_interval="0 6 * * *",
    catchup=False,
    default_args=default_args,
    tags=["ingestion", "transform"],
) as dag:

    # ingesta JMP
    run_unique_ingest = ShortCircuitOperator(
        task_id="run_unique_ingest",
        python_callable=lambda: run_once_check(UNIQUE_INGEST_FLAG),
    )

    health_unique = PythonOperator(
        task_id="health_unique",
        python_callable=lambda: ingest_api_healthcheck("health_unique"),
    )

    task_unique_ingest = PythonOperator(
        task_id="ingest_unique",
        python_callable=lambda: call_endpoint_with_logging(
            "ingest_unique",
            "/ingest/world-bank",
            {
                "countries": "ARG",
                "indicators": "NY.GDP",
                "start_year": 2023,
                "end_year": 2024,
                "isTest": True,
            },
        ),
        retries=3,
        retry_delay=timedelta(minutes=5),
    )

    notify_unique_success = EmailOperator(
        task_id="notify_unique_success",
        to=EMAIL_TO,
        subject="Ingesta única finalizada",
        html_content="La ingesta única se realizó correctamente.",
        trigger_rule="all_success",
        conn_id="mailgun_smtp",
    )

    # run_unique_ingest >> health_unique >> task_unique_ingest
    run_unique_ingest >> health_unique >> task_unique_ingest >> notify_unique_success

    # ingesta wb
    run_annual_ingest = ShortCircuitOperator(
        task_id="run_annual_ingest", python_callable=should_run_annual
    )

    health_annual = PythonOperator(
        task_id="health_annual",
        python_callable=lambda: ingest_api_healthcheck("health_annual"),
    )

    task_annual_ingest = PythonOperator(
        task_id="ingest_annual",
        python_callable=lambda: call_endpoint_with_logging(
            "ingest_unique",
            "/ingest/world-bank",
            {
                "countries": "ARG",
                "indicators": "NY.GDP",
                "start_year": 2023,
                "end_year": 2024,
                "isTest": True,
            },
        ),
        retries=3,
        retry_delay=timedelta(minutes=5),
    )

    mark_annual = PythonOperator(
        task_id="mark_annual", python_callable=mark_annual_done
    )

    notify_annual_success = EmailOperator(
        task_id="notify_annual_success",
        to=EMAIL_TO,
        subject="Ingesta anual finalizada",
        html_content="La ingesta anual se realizó correctamente.",
        trigger_rule="all_success",
        conn_id="mailgun_smtp",
    )

    # run_annual_ingest >> health_annual >> task_annual_ingest >> mark_annual
    (
        run_annual_ingest
        >> health_annual
        >> task_annual_ingest
        >> [mark_annual, notify_annual_success]
    )

    # open meteo
    check_weekly_ingest = ShortCircuitOperator(
        task_id="check_weekly_ingest",
        python_callable=lambda: Variable.get(WEEKLY_INGEST_FLAG, default_var=None)
        is None,
    )

    health_weekly = PythonOperator(
        task_id="health_weekly",
        python_callable=lambda: ingest_api_healthcheck("health_weekly"),
    )

    task_weekly_ingest = PythonOperator(
        task_id="ingest_weekly",
        python_callable=lambda: call_endpoint_with_logging(
            "ingest_weekly",
            "/ingest/open-meteo",
            {
                "country": "ARG",
                "start_date": "2023-10-01",
                "end_date": "2024-10-01",
                "isTest": True,
            },
        ),
        retries=3,
        retry_delay=timedelta(minutes=5),
    )

    mark_weekly_done = PythonOperator(
        task_id="mark_weekly_done",
        python_callable=lambda: Variable.set(WEEKLY_INGEST_FLAG, "done"),
    )

    notify_weekly_success = EmailOperator(
        task_id="notify_weekly_success",
        to=EMAIL_TO,
        subject="Ingesta semanal finalizada",
        html_content="La ingesta semanal se realizó correctamente.",
        trigger_rule="all_success",
        conn_id="mailgun_smtp",
    )

    # check_weekly_ingest >> health_weekly >> task_weekly_ingest >> mark_weekly_done
    (
        check_weekly_ingest
        >> health_weekly
        >> task_weekly_ingest
        >> [mark_weekly_done, notify_weekly_success]
    )

    # T

    transform_gate = ShortCircuitOperator(
        task_id="transform_gate",
        python_callable=can_run_transformations
    )

    start_ec2_task = PythonOperator(
        task_id="start_ec2",
        python_callable=start_ec2
    )

    ssh_hook = SSHHook(ssh_conn_id="ssh_spark_ec2", timeout=3600)

    with TaskGroup("spark_jobs_group") as spark_jobs:

        run_test_task = SSHOperator(
            task_id="run_test",
            ssh_hook=ssh_hook,
            command=spark_cmd("test"),
            get_pty=True,
            cmd_timeout=60 * 60 * 3,
        )

        run_silver_task = SSHOperator(
            task_id="run_silver",
            ssh_hook=ssh_hook,
            # ssh_conn_id="ssh_spark_ec2",
            command=spark_cmd("silver"),
            get_pty=True,
            cmd_timeout=60 * 60 * 3,
        )

        run_silver_model_task = SSHOperator(
            task_id="run_silver_model",
            ssh_hook=ssh_hook,
            # ssh_conn_id="ssh_spark_ec2",
            command=spark_cmd("silver_model"),
            get_pty=True,
            cmd_timeout=60 * 60 * 3,
        )

        run_gold_task = SSHOperator(
            task_id="run_gold",
            ssh_hook=ssh_hook,
            # ssh_conn_id="ssh_spark_ec2",
            command=spark_cmd("gold_model"),
            get_pty=True,
            cmd_timeout=60 * 60 * 3,
        )

        run_test_task >> run_silver_task >> run_silver_model_task >> run_gold_task

    stop_ec2_task = PythonOperator(
        task_id="stop_ec2",
        python_callable=stop_ec2
    )

    notify_transform_success = EmailOperator(
        task_id="notify_transform_success",
        to=EMAIL_TO,
        subject="Transformaciones finalizadas",
        html_content="Todas las transformaciones PySpark finalizaron correctamente.",
        trigger_rule="all_success",
        conn_id="mailgun_smtp",
    )

    [task_unique_ingest, task_annual_ingest, task_weekly_ingest] >> transform_gate
    transform_gate >> start_ec2_task >> run_test_task
    run_gold_task >> [stop_ec2_task, notify_transform_success]
