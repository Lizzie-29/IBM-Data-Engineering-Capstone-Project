from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import tarfile
import os

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'email': 'your_email@example.com',
    'email_on_failure': False,
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'process_web_log',
    default_args=default_args,
    description='A DAG to process web logs daily',
    schedule_interval='@daily',
)

# Function to extract IP addresses
def extract_ip_addresses():
    input_file_path = '/home/project/airflow/dags/capstone/accesslog.txt'
    output_file_path = '/home/project/airflow/dags/capstone/extracted_data.txt'
    
    with open(input_file_path, 'r') as file:
        lines = file.readlines()

    ip_addresses = [line.split()[0] for line in lines if line]

    with open(output_file_path, 'w') as output_file:
        for ip in ip_addresses:
            output_file.write(f"{ip}\n")

# Create the extract_data task
extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=extract_ip_addresses,
    dag=dag,
)

# Function to transform data by filtering out a specific IP address
def transform_ip_addresses():
    input_file_path = '/home/project/airflow/dags/capstone/extracted_data.txt'
    output_file_path = '/home/project/airflow/dags/capstone/transformed_data.txt'
    ip_to_filter = '198.46.149.143'

    with open(input_file_path, 'r') as file:
        lines = file.readlines()

    filtered_ips = [line for line in lines if line.strip() != ip_to_filter]

    with open(output_file_path, 'w') as output_file:
        output_file.writelines(filtered_ips)

# Create the transform_data task
transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=transform_ip_addresses,
    dag=dag,
)

# Function to archive the transformed data
def load_transformed_data():
    input_file_path = '/home/project/airflow/dags/capstone/transformed_data.txt'
    output_tar_path = '/home/project/airflow/dags/capstone/weblog.tar'
    
    with tarfile.open(output_tar_path, 'w') as tar:
        tar.add(input_file_path, arcname=os.path.basename(input_file_path))

# Create the load_data task
load_data = PythonOperator(
    task_id='load_data',
    python_callable=load_transformed_data,
    dag=dag,
)

# Set task dependencies to define the task pipeline
extract_data >> transform_data >> load_data


