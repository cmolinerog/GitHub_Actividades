from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# Conexion and databse
SNOWFLAKE_CONN = "snowflake_default"
DB_SCHEMA = "PHASE3_DB.PUBLIC"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026, 1, 1),
}

with DAG(
    dag_id="snowflake_etl_churn",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["snowflake", "churn", "etl"],
) as dag:

    # Task 1: Load CSV to stage
    def upload_to_stage():
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN)
        hook.run(
            """
            PUT file:///opt/airflow/data/raw/telecom_customers_churn.csv
            @my_stage
            AUTO_COMPRESS=TRUE
            OVERWRITE=TRUE;
            """
        )

    task_upload = PythonOperator(
        task_id="upload_csv_to_stage",
        python_callable=upload_to_stage,
    )

    # Task 2: Create raw table from stage
    create_and_load_sql = f"""
    CREATE OR REPLACE TABLE {DB_SCHEMA}.CUSTOMERS_CHURN_RAW (
        CUSTOMERID STRING,
        GENDER STRING,
        SENIORCITIZEN INTEGER,
        PARTNER STRING,
        DEPENDENTS STRING,
        TENURE INTEGER,
        PHONESERVICE STRING,
        MULTIPLELINES STRING,
        INTERNETSERVICE STRING,
        ONLINESECURITY STRING,
        ONLINEBACKUP STRING,
        DEVICEPROTECTION STRING,
        TECHSUPPORT STRING,
        STREAMINGTV STRING,
        STREAMINGMOVIES STRING,
        CONTRACT STRING,
        PAPERLESSBILLING STRING,
        PAYMENTMETHOD STRING,
        MONTHLYCHARGES FLOAT,
        TOTALCHARGES FLOAT,
        CHURN STRING
    );

    COPY INTO {DB_SCHEMA}.CUSTOMERS_CHURN_RAW
    FROM @{DB_SCHEMA}.MY_STAGE
    FILE_FORMAT = (
        TYPE = CSV
        SKIP_HEADER = 1
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        EMPTY_FIELD_AS_NULL = TRUE
        NULL_IF = ('', ' ', 'NULL')
    );
    """

    task_load = SQLExecuteQueryOperator(
        task_id="create_and_load_raw",
        conn_id=SNOWFLAKE_CONN,
        sql=create_and_load_sql,
    )

    # Task 3: Transformation (Filter customers by Churn = 'Yes')
    transform_sql = f"""
    CREATE OR REPLACE TABLE {DB_SCHEMA}.CLIENTS_CHURN AS
    SELECT 
        CUSTOMERID,
        GENDER,
        CONTRACT,
        PAYMENTMETHOD,
        MONTHLYCHARGES,
        TOTALCHARGES,
        CURRENT_TIMESTAMP() AS PROCESSED_DATE
    FROM {DB_SCHEMA}.CUSTOMERS_CHURN_RAW
    WHERE CHURN = 'Yes';
    """

    task_transform = SQLExecuteQueryOperator(
        task_id="churn_data_transformation",
        conn_id=SNOWFLAKE_CONN,
        sql=transform_sql,
    )

    # Task 4: Report (how many rows has the dataset)
    report_sql = f"""
    CREATE OR REPLACE TABLE {DB_SCHEMA}.REPORT_COUNT_CHURN AS
    SELECT 
        CURRENT_TIMESTAMP() AS TIMESTAMP,
        COUNT(*) AS TOTAL_CHURN_CLIENTS
    FROM {DB_SCHEMA}.CLIENTS_CHURN;
    """

    task_report_row_count = SQLExecuteQueryOperator(
        task_id="report_row_count",
        conn_id=SNOWFLAKE_CONN,
        sql=report_sql,
    )

    task_upload >> task_load >> task_transform >> task_report_row_count