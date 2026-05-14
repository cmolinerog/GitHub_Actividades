from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    "owner": "retail_team",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": True,
    "email": ["fotosbalticos@gmail.com"]
}

# =========================================================
# FILE PATHS
# =========================================================

RAW_FILE_CUSTOMERS = "/opt/airflow/data/raw/customers.csv"
RAW_FILE_ORDER_ITEMS = "/opt/airflow/data/raw/order_items.csv"
RAW_FILE_PRODUCTS = "/opt/airflow/data/raw/products.csv"

STAGING_FILE_CUSTOMERS = "/opt/airflow/data/staging/staging_customers.csv"
STAGING_FILE_ORDER_ITEMS = "/opt/airflow/data/staging/staging_order_items.csv"
STAGING_FILE_PRODUCTS = "/opt/airflow/data/staging/staging_products.csv"

PROCESSED_FILE_CUSTOMERS = "/opt/airflow/data/processed/cleaned_customers.csv"
PROCESSED_FILE_ORDER_ITEMS = "/opt/airflow/data/processed/cleaned_order_items.csv"
PROCESSED_FILE_PRODUCTS = "/opt/airflow/data/processed/cleaned_products.csv"


# =========================================================
# CUSTOMERS
# =========================================================

def validate_customers_data():

    df = pd.read_csv(RAW_FILE_CUSTOMERS)

    if df.isnull().values.any():
        raise ValueError("Null values detected in customers!")

    print("Customers validation successful!")


def load_customers_staging():

    df = pd.read_csv(RAW_FILE_CUSTOMERS)

    df.to_csv(STAGING_FILE_CUSTOMERS, index=False)

    print("Customers loaded into staging!")


def transform_customers_data():

    df = pd.read_csv(STAGING_FILE_CUSTOMERS)

    df["country"] = df["country"].str.upper()

    df.to_csv(PROCESSED_FILE_CUSTOMERS, index=False)

    print("Customers transformation completed!")


# =========================================================
# ORDER ITEMS
# =========================================================

def validate_order_items_data():

    df = pd.read_csv(RAW_FILE_ORDER_ITEMS)

    if df.isnull().values.any():
        raise ValueError("Null values detected in order_items")

    if (df["unit_price_usd"] < 0).any():
        raise ValueError("Negative price found")

    print("Order_items validation successful")


def load_order_items_staging():

    df = pd.read_csv(RAW_FILE_ORDER_ITEMS)

    df.to_csv(STAGING_FILE_ORDER_ITEMS, index=False)

    print("Order_items loaded into staging")


def transform_order_items_data():

    df = pd.read_csv(STAGING_FILE_ORDER_ITEMS)

    df["Total_Sales"] = df["unit_price_usd"] * df["quantity"]

    df.to_csv(PROCESSED_FILE_ORDER_ITEMS, index=False)

    print("Order_items transformation completed")


# =========================================================
# PRODUCTS
# =========================================================

def validate_products_data():

    df = pd.read_csv(RAW_FILE_PRODUCTS)

    if df.isnull().values.any():
        raise ValueError("Null values detected in products")

    if (df["price_usd"] < 0).any():
        raise ValueError("Negative price found")

    print("Products validation successful!")


def load_products_staging():

    df = pd.read_csv(RAW_FILE_PRODUCTS)

    df.to_csv(STAGING_FILE_PRODUCTS, index=False)

    print("Products loaded into staging")


def transform_products_data():

    df = pd.read_csv(STAGING_FILE_PRODUCTS)

    df["margin_pct"] = df["margin_usd"] / df["price_usd"]

    df.to_csv(PROCESSED_FILE_PRODUCTS, index=False)

    print("Products transformation completed")


# =========================================================
# DAG
# =========================================================

with DAG(
    dag_id="mini_project_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    default_args=default_args,
) as dag:

    # =====================================================
    # CUSTOMERS TASKS
    # =====================================================

    wait_for_file_customers = FileSensor(
        task_id="wait_for_customers_file",
        filepath=RAW_FILE_CUSTOMERS,
        fs_conn_id="fs_default",
        poke_interval=30,
        timeout=300,
        mode="poke"
    )

    validate_task_customers = PythonOperator(
        task_id="validate_customers_data",
        python_callable=validate_customers_data
    )

    load_staging_customers = PythonOperator(
        task_id="load_customers_staging",
        python_callable=load_customers_staging
    )

    transform_task_customers = PythonOperator(
        task_id="transform_customers_data",
        python_callable=transform_customers_data
    )

    # =====================================================
    # ORDER ITEMS TASKS
    # =====================================================

    wait_for_file_order_items = FileSensor(
        task_id="wait_for_order_items_file",
        filepath=RAW_FILE_ORDER_ITEMS,
        fs_conn_id="fs_default",
        poke_interval=30,
        timeout=300,
        mode="poke"
    )

    validate_task_order_items = PythonOperator(
        task_id="validate_order_items_data",
        python_callable=validate_order_items_data
    )

    load_staging_order_items = PythonOperator(
        task_id="load_order_items_staging",
        python_callable=load_order_items_staging
    )

    transform_task_order_items = PythonOperator(
        task_id="transform_order_items_data",
        python_callable=transform_order_items_data
    )

    # =====================================================
    # PRODUCTS TASKS
    # =====================================================

    wait_for_file_products = FileSensor(
        task_id="wait_for_products_file",
        filepath=RAW_FILE_PRODUCTS,
        fs_conn_id="fs_default",
        poke_interval=30,
        timeout=300,
        mode="poke"
    )

    validate_task_products = PythonOperator(
        task_id="validate_products_data",
        python_callable=validate_products_data
    )

    load_staging_products = PythonOperator(
        task_id="load_products_staging",
        python_callable=load_products_staging
    )

    transform_task_products = PythonOperator(
        task_id="transform_products_data",
        python_callable=transform_products_data
    )

    # =====================================================
    # DEPENDENCIES
    # =====================================================

    wait_for_file_customers >> validate_task_customers >> load_staging_customers >> transform_task_customers

    wait_for_file_order_items >> validate_task_order_items >> load_staging_order_items >> transform_task_order_items

    wait_for_file_products >> validate_task_products >> load_staging_products >> transform_task_products