from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

def print_hello():
    print("Hello, Airflow!")

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    'simple_dag',
    default_args=default_args,
    description='A simple DAG to print Hello Airflow',
    schedule_interval='@once',
)

hello_task = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=dag,
)
