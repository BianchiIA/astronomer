from airflow.decorators import dag, task
from datetime import datetime
import logging

@dag(
    start_date=datetime(2024, 11, 25),
    schedule_interval='@once',
    dag_id='das-taskflow',
    catchup=False
)
def cnpj_etl():
    @task
    def extract_cnpj():
        import requests

        url = 'https://api-publica.speedio.com.br/buscarcnpj?cnpj=00000000000191'
        headers = {"accept": "application/json"}
        response = requests.get(url, headers=headers).json()

        return response
    
    @task
    def process_cnpj(cnpj_data):
        processed_data = {'razao':cnpj_data['NOME FANTASIA'], 'STATUS':cnpj_data['STATUS']}
        return processed_data
    
    @task
    def store_cnpj(processed_data):
        logging.info(processed_data)

    store_cnpj(process_cnpj(extract_cnpj()))

cnpj_etl()