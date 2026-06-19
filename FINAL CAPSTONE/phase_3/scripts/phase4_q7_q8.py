from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator


DBT_DIR = "/opt/airflow/dbt_churn_project"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
}

# Failure notifications 
def notify_failure(context):
    task_id = context['task_instance'].task_id
    print(f"  ALERT: The task '{task_id}' failed.")

with DAG(
    "orchestrate_dbt_churn",
    default_args=default_args,
    schedule=None,  
    catchup=False,
    on_failure_callback=notify_failure  # Xaptures  fails in the DAG
) as dag:

    # Execute dbt run (Create in Snowflake: staging, dim and fct tables)
    task_dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir .",
        on_failure_callback=notify_failure  #Alert if dbt run fails
    )

    # Execute dbt test 
    task_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir .",
    )

    task_dbt_run >> task_dbt_test