from datetime import datetime
import time
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1), 
}

with DAG(
    "phase4_q1",
    default_args=default_args,
    schedule="0 8 * * *",  # Daily at 8:00 AM using Cron expression
    catchup=False,
    tags=["exercise_q1"],
) as dag:

    # Task 1: Prints 'Data Pipeline Started'
    task_start = BashOperator(
        task_id="print_start",
        bash_command="echo 'Data Pipeline Started'",
    )

    # Task 2: Waits for 10 seconds
    def wait_ten_seconds():
        time.sleep(10)

    task_wait = PythonOperator(
        task_id="wait_10_seconds",
        python_callable=wait_ten_seconds,
    )

    # Task 3: Prints 'Data Pipeline Completed'
    task_complete = BashOperator(
        task_id="print_complete",
        bash_command="echo 'Data Pipeline Completed'",
    )

    # Definición del flujo secuencial del pipeline
    task_start >> task_wait >> task_complete