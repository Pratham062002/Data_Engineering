from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from load_db import load_data

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 1),  
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'load_data_to_mysql',
    default_args=default_args,
    description='A DAG to load data from files to MySQL database',
    schedule_interval=timedelta(days=1),  
)

# Task to load data into the database
load_db_task = PythonOperator(
    task_id='load_data_to_mysql',
    python_callable=load_data,
    dag=dag,
)

# Define task dependencies (if you have multiple tasks; currently just one task)
load_db_task
