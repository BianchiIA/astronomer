from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from typing import Dict
import logging
from airflow.models.param import Param, ParamsDict

params = {
    'teste': "Funcionou",
    'teste2': 6,
    'teste3': Param(default="Funcionou", type="string")}

@dag(
    start_date=datetime(2024, 11, 25),
    schedule_interval='@once',
    dag_id='das-taskflow-2',
    catchup=False,
    params=params
)
def cnpj_etl():
    
    with TaskGroup("process_group") as process_group:
        @task
        def extract_cnpj():
            import requests

            url = 'https://api-publica.speedio.com.br/buscarcnpj?cnpj=00000000000191'
            headers = {"accept": "application/json"}
            response = requests.get(url, headers=headers).json()

            return response
        
        @task
        def process_cnpj(cnpj_data: Dict[str, str]) -> Dict[str, str]:
            processed_data = {'razao':cnpj_data['NOME FANTASIA'], 'STATUS':cnpj_data['STATUS']}
            return processed_data


    




        process = process_cnpj(extract_cnpj())

    @task
    def params_test(**kwargs):
        #var = params['teste3']
        params: ParamsDict = kwargs["params"]
        return params['teste3']
    

    teste = params_test()

    with TaskGroup("store_group") as store_group:
        @task(retries=3, retry_delay=timedelta(minutes=5))
        def store_cnpj(processed_data: Dict[str, str]) -> None:
            logging.info(processed_data)

        store_cnpj(process) >> teste

cnpj_etl()