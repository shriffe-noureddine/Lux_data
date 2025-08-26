# Minimal DAG so you can confirm Airflow works
# What it does: prints a hello line once on manual trigger

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

def say_hello():
    print("Airflow is up. Hello from your DAG.")

with DAG(
    dag_id="hello_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,   # run manually for now
    catchup=False,
    tags=["test"],
) as dag:
    start = EmptyOperator(task_id="start")
    hello = PythonOperator(task_id="hello", python_callable=say_hello)
    start >> hello
