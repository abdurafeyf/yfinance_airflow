from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from yfinance_etl import run_news_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 9),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'yahoo_finance_news_dag',
    default_args=default_args,
    description='ETL for updating news related to finance'
)

run_etl = PythonOperator(
    task_id='complete_yfinance_etl',
    python_callable=run_news_etl,
    dag=dag
)

run_etl