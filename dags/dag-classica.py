from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

import requests
import logging

def get_bitcoin_price():
    
    url = 'https://api-publica.speedio.com.br/buscarcnpj?cnpj=00000000000191'
    headers = {"accept": "application/json"}
    response = requests.get(url, headers=headers).json()
    #print(response)

    return response


def process_bitcoin(**kwargs):
    ti = kwargs['ti']
    cnpj_data = ti.xcom_pull(task_ids='extract_cnpj')
    logging.info(cnpj_data)
   # print(cnpj_data)
    processed_data = {'razao':cnpj_data['NOME FANTASIA'], 'STATUS':cnpj_data['STATUS']}
    ti.xcom_push(key='processed_data', value=processed_data)

def stroge_bitcoin(**kwargs):
    ti = kwargs['ti']
    processed_data = ti.xcom_pull(key='processed_data', task_ids='processed_data')
    logging.info(processed_data)

## Define the basic parameters of the DAG, like schedule and start_date

with DAG('bitcoin_price_etl', 
         start_date=datetime(2024, 10, 23)
         , schedule_interval='@daily'
         ) as dag:
    
    extract_bitcoin_price = PythonOperator(
        task_id='extract_cnpj',
        python_callable=get_bitcoin_price
    )

    process_bitcoin_price = PythonOperator(
        task_id='processed_data',
        python_callable=process_bitcoin
    )

    store_bitcoin_from_api = PythonOperator(
        task_id='store_bitcoin_price',
        python_callable=stroge_bitcoin
    )

    extract_bitcoin_price >> process_bitcoin_price >> store_bitcoin_from_api

