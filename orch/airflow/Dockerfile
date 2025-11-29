FROM apache/airflow:2.9.2-python3.10

USER root
RUN apt-get update && apt-get install -y bash && apt-get clean

USER airflow
RUN pip install --no-cache-dir requests