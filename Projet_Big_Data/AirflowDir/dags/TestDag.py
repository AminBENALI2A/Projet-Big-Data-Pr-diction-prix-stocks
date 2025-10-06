from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# DAG Definition
with DAG(
    dag_id='Testing_First_Dag',  # Unique identifier for the DAG
    description='A simple example DAG',  # Brief description of the DAG
    default_args={
        'owner': 'airflow',  # Owner of the DAG
        'depends_on_past': False,  # DAG doesn't depend on previous runs
        'email_on_failure': False,  # Disable email notifications for failures
        'email_on_retry': False,  # Disable email notifications for retries
        'retries': 1,  # Number of retries on failure
        'retry_delay': timedelta(minutes=5),  # Delay between retries
    },
    schedule_interval=None,  # The schedule interval (None for manual trigger)
    start_date=datetime(2023, 1, 1),  # Start date of the DAG
    catchup=False,  # Skip backfilling for missed runs
    tags=['example', 'bash'],  # Tags to help organize the DAG
) as dag:
    # Task Definition
    task = BashOperator(
        task_id='say_hello',  # Unique ID for the task
        bash_command='echo "Hello, Airflow!"',  # Command to execute
    )

