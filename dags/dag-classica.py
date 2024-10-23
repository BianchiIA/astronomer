from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

import requests
import logging

def get_bitcoin_price():
    
    url = "https://pro-api.coingecko.com/api/v3/simple/price"
    headers = {"accept": "application/json"}
    response = requests.get(url, headers=headers).json()

    return response


def process_bitcoin(**kwargs):
    ti = kwargs['ti']
    bitcoin_data = ti.xcom_pull(task_ids='extract_bitcoin_price')
    logging.info(bitcoin_data)
    processed_data = {'usd':bitcoin_data['usd'], 'chance':bitcoin_data['usd_24h_change']}
    ti.xcom_push(key='processed_data', value=processed_data)

def stroge_bitcoin(**kwargs):
    ti = kwargs['ti']
    processed_data = ti.xcom_pull(key='processed_data', task_ids='process_bitcoin_price')
    logging.info(processed_data)

## Define the basic parameters of the DAG, like schedule and start_date

with DAG('bitcoin_price_etl', 
         start_date=datetime(2024, 10, 23)
         , schedule_interval='@daily'
         ) as dag:
    
    extract_bitcoin_price = PythonOperator(
        task_id='extract_bitcoin_price',
        python_callable=get_bitcoin_price
    )

    process_bitcoin_price = PythonOperator(
        task_id='process_bitcoin_price',
        python_callable=process_bitcoin
    )

    store_bitcoin_from_api = PythonOperator(
        task_id='store_bitcoin_price',
        python_callable=stroge_bitcoin
    )

    extract_bitcoin_price >> process_bitcoin_price >> store_bitcoin_from_api

