from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

def task_1():
    print("Task 1")

def task_2():
    print("Task 2")

def task_3():
    print("Task 3")

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 3,
}

dag = DAG(
    'dependencies_dag',
    default_args=default_args,
    description='A DAG with task dependencies and retry policies',
    schedule_interval='@once',
)

task_1 = PythonOperator(
    task_id='task_1',
    python_callable=task_1,
    dag=dag,
)

task_2 = PythonOperator(
    task_id='task_2',
    python_callable=task_2,
    dag=dag,
)

task_3 = PythonOperator(
    task_id='task_3',
    python_callable=task_3,
    dag=dag,
)

task_1 >> task_2 >> task_3
