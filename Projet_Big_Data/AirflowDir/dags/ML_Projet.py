from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 12),  # Ensure this is in the past
    'retries': 1,
}

# Initialize the DAG
dag = DAG(
    'Training_and_Prediction',
    default_args=default_args,
    description='DAG to run two Python scripts consecutively',
    schedule_interval=None,  # Disable automatic scheduling
    catchup=False,  # Prevent running missed schedules
)

# Task 1: Run the first Python script
first_script_task = BashOperator(
    task_id='Training_Script',
    bash_command='python3 /shared_volume/Projet_Big_Data/AirflowDir/train_model.py',  # Absolute path to the script
    dag=dag,
)

# Task 2: Run the second Python script
second_script_task = BashOperator(
    task_id='Predicting_Script',
    bash_command='python3 /shared_volume/Projet_Big_Data/AirflowDir/Predict.py',  # Absolute path to the script
    dag=dag,
)

# Set the execution order: second script runs after the first script
first_script_task >> second_script_task
