import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

def fetch_data():
    response = requests.get('https://jsonplaceholder.typicode.com/todos/1')
    data = response.json()
    with open('/opt/airflow/fetched_data.json', 'w') as f:
        f.write(str(data))
    print("Data fetched")

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    'http_request_dag',
    default_args=default_args,
    description='A DAG to make an HTTP request',
    schedule_interval='@once',
)

fetch_task = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_data,
    dag=dag,
)
