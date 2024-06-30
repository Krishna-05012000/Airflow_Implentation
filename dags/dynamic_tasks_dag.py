from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

def process_data(task_id):
    print(f"Processing data for {task_id}")

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    'dynamic_tasks_dag',
    default_args=default_args,
    description='A DAG with dynamically generated tasks',
    schedule_interval='@once',
)

tasks = []
for i in range(5):
    task = PythonOperator(
        task_id=f'process_data_{i}',
        python_callable=process_data,
        op_kwargs={'task_id': i},
        dag=dag,
    )
    tasks.append(task)

for i in range(len(tasks) - 1):
    tasks[i] >> tasks[i + 1]
