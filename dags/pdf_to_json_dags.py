import json
import os
import pdfplumber
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


# Function to convert PDF to JSON
def pdf_to_json(pdf_folder_path, json_folder_path):
    print('inside function')
    for filename in os.listdir(pdf_folder_path):
        if filename.endswith('.pdf'):
            pdf_path = os.path.join(pdf_folder_path, filename)
            json_filename = f"{os.path.splitext(filename)[0]}.json"
            json_path = os.path.join(json_folder_path, json_filename)
            with pdfplumber.open(pdf_path) as pdf:
                pages = pdf.pages
                data = [page.extract_text() for page in pages]
                with open(json_path, 'w') as json_file:
                    json.dump(data, json_file)
    print(f"Completed writing to: {json_path}")

# DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}


dag = DAG(
    'pdf_to_json',
    default_args=default_args,
    description='A simple DAG to convert PDF to JSON',
    schedule_interval='@once',
)

# task to convert PDF to JSON
convert_pdf_to_json_task = PythonOperator(
    task_id='convert_pdf_to_json',
    python_callable=pdf_to_json,
    op_kwargs={
        'pdf_folder_path': '/opt/airflow/input_output_files/input_files',
        'json_folder_path': '/opt/airflow/input_output_files/output_files'
    },
    dag=dag,
)
# pdf_to_json('/opt/airflow/input_folder', '/opt/airflow/output_folder')