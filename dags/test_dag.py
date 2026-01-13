from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello_world():
    print("Hello from Airflow! Setup is working!")
    return "Success"


with DAG(
        dag_id='test_setup',
        start_date=datetime(2024, 1, 1),
        schedule_interval=None,
        catchup=False,
        tags=['test']
) as dag:
    test_task = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world
    )