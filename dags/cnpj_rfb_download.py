import requests
import datetime
import logging

from google.cloud import storage
import urllib.parse
import os

from airflow.decorators import task, dag
from airflow.operators.empty import EmptyOperator
from airflow.models.param import Param, ParamsDict
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook 

hook = GoogleBaseHook(gcp_conn_id="gcs_default")
credentials = hook.get_credentials()


default_args = { 
    "owner": "Vinicius B. Soares",
    "start_date": datetime.datetime(2025, 2, 24),
    "retries": 3,
    "retry_delay": datetime.timedelta(minutes=5)}


@dag(
    dag_id='cnpj_etl',
    schedule="@once",
    doc_md=__doc__,
    catchup=False,
    default_args=default_args,
    max_active_tasks=5
    #params=params_dict
)
def cnpj_download():
    
    download_files = {
        'empresas': [f'Empresas{i}.zip' for i in range(10)],
        'estabelecimentos': [f'Estabelecimentos{i}.zip' for i in range(10)],
        'socios': [f'Socios{i}.zip' for i in range(10)],
        'dimensoes': ['Cnaes.zip', 'Municipios.zip', 'Naturezas.zip', 'Paises.zip', 'Qualificacoes.zip','Simples.zip'],
        'regimes': ['https://arquivos.receitafederal.gov.br/dados/cnpj/regime_tributario/Lucro%20Real.zip',
                   'https://arquivos.receitafederal.gov.br/dados/cnpj/regime_tributario/Lucro%20Presumido.zip',
                   'https://arquivos.receitafederal.gov.br/dados/cnpj/regime_tributario/Imunes%20e%20Isentas.zip',
                   'https://arquivos.receitafederal.gov.br/dados/cnpj/regime_tributario/Lucro%20Arbitrado.zip'
                   ]
    }
    
    PA = datetime.datetime.now().strftime("%Y-%m")
    
    
    @task(task_id='downoad_cnpjs_gcs')
    def download_and_upload_to_gcs(**kwargs):
        
        url = f"https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{kwargs['PA']}/{kwargs['files']}"
        local_filename = kwargs['files']
        bucket_name = kwargs['bucket_name']
        destination_blob_name = f"cnpj/{kwargs['PA']}/{kwargs['type']}/{kwargs['files']}"
        logging.info(f'PA is {kwargs['PA']}')
        logging.info(f'Init request in {url}')
        #try:
        response = requests.get(url, stream=True, timeout=300, verify=False)
        response.raise_for_status()
        
        with open(local_filename, "wb") as file:
            for chunk in response.iter_content(chunk_size=1024*1024):
                file.write(chunk)
        
        client = storage.Client(credentials=credentials)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(local_filename)
            
        return "Arquivo enviado com sucesso para o GCS."
        #except requests.exceptions.RequestException as e:
        #    return f"Erro ao baixar o arquivo: {e}"
        #except Exception as e:
        #    return f"Erro ao enviar para o GCS: {e}"
        
    @task(task_id='download_regimes_fiscal')
    def download_regimes_fiscal(**kwargs):
        nome_arquivo = kwargs['files'].split('/')[-1]
        url = kwargs['files']
        nome_decodificado = urllib.parse.unquote(nome_arquivo)
        # Remove a extensão do arquivo
        nome_sem_extensao, _ = os.path.splitext(nome_decodificado)
        # Remove caracteres especiais
        nome_limpo = ''.join(c for c in nome_sem_extensao if c.isalnum())
        nome_limpo = nome_limpo + '.zip'
        local_filename = nome_limpo 
        bucket_name = kwargs['bucket_name']
        destination_blob_name = f"cnpj/{kwargs['PA']}/regimes/{nome_limpo}"
        logging.info(f'Init request in {url}')
        try:
            response = requests.get(url, stream=True, timeout=300, verify=False)
            response.raise_for_status()
            
            with open(local_filename, "wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
            
            client = storage.Client(credentials=credentials)
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_filename(local_filename)
            
            return "Arquivo enviado com sucesso para o GCS."
        except requests.exceptions.RequestException as e:
            f"Erro ao baixar o arquivo: {e}"
            e
        except Exception as e:
            f"Erro ao enviar para o GCS: {e}"
            e
    
    
    start = EmptyOperator(task_id = 'start')
    download_empresas = download_and_upload_to_gcs.partial(PA=PA, type = 'empresas', bucket_name = 'dataita').expand(files = download_files['empresas'])
    download_socios = download_and_upload_to_gcs.partial( type = 'socios', PA='2025-02', bucket_name = 'dataita').expand(files = download_files['socios'])
    download_estabelecimentos = download_and_upload_to_gcs.partial( type = 'estabelecimentos', PA='2025-02', bucket_name = 'dataita').expand(files = download_files['estabelecimentos'])
    download_dimensoes = download_and_upload_to_gcs.partial( type = 'dimensoes', PA='2025-02', bucket_name = 'dataita').expand(files = download_files['dimensoes'])
    download_regime = download_regimes_fiscal.partial( type = 'regimes', PA='2025-02', bucket_name = 'dataita').expand(files = download_files['regimes'])

    end = EmptyOperator(task_id = 'end')
    
    start >>  download_empresas >> download_socios >> download_estabelecimentos >> download_dimensoes >> download_regime >> end
        
cnpj_download()