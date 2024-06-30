import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

def extract_data():
    data = {'name': ['John', 'Jane', 'Jake'], 'age': [23, 34, 45]}
    df = pd.DataFrame(data)
    df.to_csv('/opt/airflow/input_output_files/output_files/extracted_data.csv', index=False)
    print("Data extracted")

def transform_data():
    df = pd.read_csv('/opt/airflow/input_output_files/output_files/extracted_data.csv')
    df['age'] = df['age'] + 1
    df.to_csv('/opt/airflow/input_output_files/output_files/transformed_data.csv', index=False)
    print("Data transformed")

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    'data_pipeline_dag',
    default_args=default_args,
    description='A simple data pipeline DAG',
    schedule_interval='@once',
)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

extract_task >> transform_task


