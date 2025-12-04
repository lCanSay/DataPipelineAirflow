FROM apache/airflow:2.7.1-python3.11

USER root
RUN apt-get update && apt-get install -y wget curl chromium \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
ENV PATH=/home/airflow/.local/bin:$PATH

COPY requirements.txt .
RUN pip install --user --no-cache-dir -r requirements.txt

RUN python -m playwright install chromium

COPY src/ /opt/airflow/src/
COPY airflow_dag.py /opt/airflow/dags/
COPY data/ /opt/airflow/data/
