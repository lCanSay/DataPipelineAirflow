from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from src.scraper import scrape_pages, save_csv
from src.cleaner import run_cleaning
from src.loader import create_db, load_csv_to_db

RAW_CSV = "data/raw.csv"
CLEANED_CSV = "data/cleaned.csv"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def scrape_task_callable():
    import asyncio
    rows = asyncio.run(scrape_pages(max_pages=6))
    save_csv(rows, path=RAW_CSV)

def clean_task_callable():
    run_cleaning(in_path=RAW_CSV, out_path=CLEANED_CSV)

def create_db_task_callable():
    create_db()

def load_task_callable():
    load_csv_to_db(csv_path=CLEANED_CSV)

with DAG(
    dag_id="gog_pipeline",
    default_args=default_args,
    description="GOG scraping → cleaning → loading pipeline",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 12, 4),
    catchup=False,
    tags=["gog"],
) as dag:

    scrape_task = PythonOperator(
        task_id="scrape_data",
        python_callable=scrape_task_callable,
    )

    clean_task = PythonOperator(
        task_id="clean_data",
        python_callable=clean_task_callable,
    )

    create_db_task = PythonOperator(
        task_id="create_db",
        python_callable=create_db_task_callable,
    )

    load_task = PythonOperator(
        task_id="load_data",
        python_callable=load_task_callable,
    )

    scrape_task >> clean_task >> create_db_task >> load_task
