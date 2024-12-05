from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import boto3

# Function to upload the CSV file to S3
def upload_csv_to_s3():
    local_file_path = 'fraudTest.csv'  # Path to the local CSV file
    bucket_name = 'your-s3-bucket-name'  # Replace with your S3 bucket name
    s3_file_path = 'fraud-detection/fraudTest.csv'  # Desired path in S3 (folder + file name)

    # Initialize the S3 client
    s3_client = boto3.client('s3')

    try:
        # Upload the file
        s3_client.upload_file(local_file_path, bucket_name, s3_file_path)
        print(f"File {local_file_path} successfully uploaded to s3://{bucket_name}/{s3_file_path}")
    except Exception as e:
        print(f"Error uploading file to S3: {e}")

# Import the transform_data function
from transform import transform_data

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 28),  # Set start date
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
    description='Pipeline for credit card fraud detection with CSV upload and S3 integration',
    schedule_interval=timedelta(days=1),  # Runs daily
)

# Task 1: Upload CSV to S3
upload_csv_task = PythonOperator(
    task_id='upload_csv_to_s3',
    python_callable=upload_csv_to_s3,
    dag=dag,
)

# Task 2: Data Transformation and Feature Engineering
transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

# Define the task flow
upload_csv_task >> transform_task
