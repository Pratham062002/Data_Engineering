from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from train_model import train_model
from test_enhanced_model_with_city import test_model

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 28),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'fraud_detection_pipeline',
    default_args=default_args,
    description='Pipeline for training, testing, and processing fraud detection models',
    schedule_interval=timedelta(days=1),  # Runs daily
)

# Task 1: Train the fraud detection model
train_task = PythonOperator(
    task_id='train_fraud_model',
    python_callable=train_model,
    dag=dag,
)

# Task 2: Test the model and generate fraud reports
test_task = PythonOperator(
    task_id='test_fraud_model',
    python_callable=test_model,
    dag=dag,
)


# Define task dependencies
train_task >> test_task