from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from kafka import KafkaConsumer
import json
import pandas as pd

default_args = {
    'owner': 'data_eng',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def consumer_kafka():
    consumer = KafkaConsumer(
    'orders_ecommerce',
    bootstrap_servers='host.docker.internal:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    consumer_timeout_ms=5000
)    
    #Put all the messages in the file orders_arrived.csv
    orders = []
    for message in consumer:
        orders.append(message.value)

    df = pd.DataFrame(orders)
    filename = "/opt/airflow/data/raw/ecommerce_orders_arrived.csv"
    df.to_csv(filename, index=False)


#Airflow dag runs hourly

with DAG(
    dag_id='ecommerce_pipeline_hourly',
    start_date= datetime(2026, 1, 1),
    schedule='@hourly',
    default_args=default_args,
    catchup=False,
) as dag:
    consume_task = PythonOperator(
        task_id='consumer_kafka',
        python_callable=consumer_kafka
    )

    consume_task