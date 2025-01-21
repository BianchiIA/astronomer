"""
Dag de Pipeline de ETL dos arquivos decred: zip >> BigQuery
"""

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import get_current_context
from airflow.models.param import Param, ParamsDict
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.cloud_run import CloudRunUpdateJobOperator

import duckdb
import pandas as pd
import numpy as np

from datetime import datetime
import os 
from zipfile import ZipFile
from io import BytesIO
import logging

# Defina os parâmetros do DAG (valores padrão podem ser sobrescritos ao iniciar o DAG)
conn_id = "gcs_default"
params_dict = {
    'BUCKET_NAME': Param(default='dataita', type="string"),
    'prefix': Param(default='decred/pastaRaw/pastaZip', type="string"),
    'ZIP_FOLDER': Param(default='decred/pastaRaw/pastaZip', type="string"),
    'UNZIP_FOLDER': Param(default='decred/pastaRaw/pastaUnzip', type="string"),
    'DEST_FOLDER_PROCESSED': Param(default='decred/pastaProcessed', type="string"),
    'TABLE_NAME': Param(default='decred', type="string"),
    'SERVICE': Param(default='duckdb', type="string")
}


params = ParamsDict(params_dict)
#bucket_name = params_default["BUCKET_NAME"]


@dag(
    dag_id='decred_etl_duckdb',
    start_date=datetime(2024, 10, 3),
    schedule="@once",
    doc_md=__doc__,
    catchup=False,
    params=params
)
def decred_etl_duckdb():
    
    start = EmptyOperator(task_id='start')
    

    @task(task_id="get_files", multiple_outputs=True)
    def get_files(**kwargs):
        
        params : ParamsDict = kwargs["params"]  

        hook = GCSHook(gcp_conn_id=conn_id)
        ## Get all zips in folder e subfolders 
        files = hook.list(params["BUCKET_NAME"], prefix=params["prefix"], delimiter=".zip" )
        list_name_files = [file.split('/')[-1] for file in files]
        logging.info(list_name_files)
        list_period = [f'decred-{file.split("_")[4]}-{file.split("_")[5]}' for file in list_name_files]
        
        metadata_decred = {'file_name': list_name_files, 'period': list_period, 'path_files': files}

        return metadata_decred

    @task
    def extract_path_files(metadata_decred):
        return metadata_decred['path_files']
    
    @task
    def extract_period(metadata_decred):
        return metadata_decred['period']    

    @task(map_index_template='{{ period }}')
    def zip_to_gcs(**kwargs):
        
        context = get_current_context()
        context['decred'] = kwargs['period']

        params : ParamsDict = kwargs["params"]
        bucket_name = params["BUCKET_NAME"]


        hook = GCSHook(gcp_conn_id=conn_id)
        files = [kwargs['period']]
          
        path_descompacted_files = f'tmp/{kwargs['period'].split("/")[-1].replace(".zip","")}/'
        list_dest_paths = list()
        for i in range(len(files)):
            zip_data = BytesIO(hook.download(bucket_name, files[i]))
            with ZipFile(zip_data, 'r') as zip_ref:
                for file in zip_ref.namelist():
                    if file.endswith('.zip'):
                        continue  # Skip directories in the zip file
                    file_data = zip_ref.read(file)
                    print(path_descompacted_files + file)
                    print(logging.info(path_descompacted_files + file))
                    hook.upload(bucket_name,object_name= path_descompacted_files + file, data=file_data)


        return path_descompacted_files


    @task
    def get_xcom(values):
        return list(values)

    @task(task_id="teste")
    def get_metadata():

        hook = GCSHook(gcp_conn_id=conn_id)
        files = hook.list(bucket_name, prefix=params_default["prefix"], delimiter=".zip")
        print(files)
        #file_name = files.split('/')[-1]
        #file = file_name.split('_')
        #file[-1] = file[-1].strip('.zip')

        #id_file = file[4], file[5], file[6]

        df = pd.DataFrame(data=np.array(file).reshape(-1, len(file)),
            columns = ['tipo1', 'tipo2', 'tipo3','cnpj_minicipio', 'df_ref_init', 'df_ref_end', 'id_extact' ])
        ## Carregar paraquet e biquery

        return file_name

        


    @task(multiple_outputs=True)
    def process_csv_to_parquet(path_descompacted_files, **kwargs):

        params : ParamsDict = kwargs["params"]

        id_file = params['id_file']

        duckdb.sql("""
        INSTALL httpfs; -- Instalar extensão necessária
        LOAD httpfs;    -- Carregar a extensão
        """)
        duckdb.sql(f"""
        SET s3_region = 'auto'; -- DuckDB usa 'auto' para regiões GCS
        SET s3_access_key_id = '{Variable.get("KEY_ID_GCS")}';
        SET s3_secret_access_key = '{Variable.get("SECRET_GCS")}';
        """)


        tipos = {'DS_ORIGEM_INFORMACAO': 'VARCHAR',
            'SQ_REG_0000': 'VARCHAR',
            'NU_CNPJ_ADMINISTRADORA_ARQUIVO': 'VARCHAR',
            'NO_ADMINISTRADORA_ARQUIVO': 'VARCHAR',
            'NU_CNPJ_LIQUIDANTE_OPERACAO': 'VARCHAR',
            'NO_LIQUIDANTE_OPERACAO': 'VARCHAR',
            'TP_NIVEL_OPERACAO': 'VARCHAR',
            'DS_TIPO_NIVEL': 'VARCHAR',
            'NU_CNPJ_CLIENTE_ORIG': 'VARCHAR',
            'NU_CNPJ_CLIENTE': 'VARCHAR',
            'NU_CPF_CLIENTE': 'VARCHAR',
            'NO_FANTASIA_CLIENTE': 'VARCHAR',
            'NO_RESP_CADASTRO': 'VARCHAR',
            'CO_MCAPT': 'VARCHAR',
            'NU_MCAPT_LOGICO': 'VARCHAR',
            'IN_MCAPT_TERMINAL_PROPRIO': 'VARCHAR',
            'DS_MCAPT_MARCA': 'VARCHAR',
            'NO_MCAPT_TIPO_TECNOLOGIA': 'VARCHAR',
            'DS_OPERACAO_MES': 'VARCHAR',
            'DS_OPERACAO_ANO': 'VARCHAR',
            'DH_OPERACAO': 'VARCHAR',
            'DS_OPERACAO_BANDEIRA_CARTAO': 'VARCHAR',
            'DS_OPERACAO_NSU': 'VARCHAR',
            'DS_OPERACAO_TRANSACAO': 'VARCHAR',
            'CO_OPERACAO_AUT': 'VARCHAR',
            'DS_OPERACAO_TIPO_NATUREZA': 'VARCHAR',
            'CO_NAT_OPERACAO': 'VARCHAR',
            'VL_OPERACAO': 'FLOAT',
            'VL_CANCELADO': 'VARCHAR',
            'ID_SEFAZ': 'VARCHAR'
        }

    
        # Criação de um banco de dados DuckDB em memória
        logging.info(f"lendo arquivos de: gs://{bucket_name}/{path_descompacted_files}*")
        df_db = duckdb.read_csv(f"gs://{bucket_name}/{path_descompacted_files}*.csv",dtype=tipos, decimal=',')

        
        prefix_dest  = f'{params["dest_data"]}-{datetime.now().strftime("%Y-%m-%d-%H-%M-%S")}/'
        dest_path_root = os.path.join(f'gs://{bucket_name}/', prefix_dest)

        # Criação de dados em parquet
        df_db.write_parquet(os.path.join(dest_path_root, f"content/daspag-{periodo_arr['MIN_PER'][0]}-{periodo_arr['MAX_PER'][0]}.parquet"))
        #df_meta.write_parquet(os.path.join(dest_path_root, f"meta/daspagmeta-{periodo_arr['MIN_PER'][0]}-{periodo_arr['MAX_PER'][0]}.parquet"))

        dict_paths = {
            "content": os.path.join(prefix_dest, "content"),
            "meta": os.path.join(prefix_dest, "meta")
        }
        
        logging.info(f"Parquet criado em {dest_path_root}")
        #duckdb.close()
        logging.info(f"path_criados em {dict_paths}")

        return dict_paths
   
    
    transform_and_load = DockerOperator(
        task_id='transform_and_load',
        image='southamerica-east1-docker.pkg.dev/infra-itaborai/dataita/decred:latest',
        docker_url='tcp://docker-socket-proxy:2375',
        environment={
            "BUCKET_NAME": "{{ params.BUCKET_NAME }}",
            "PATH_FOLDER": "{{ params.UNZIP_FOLDER }}",
            "TABLE": "{{ params.TABLE_NAME }}",
            "SERVICE": "{{ params.SERVICE }}"
        },
        force_pull=False,
        mount_tmp_dir=False,
        mem_limit="52g"
    )

    clear_gcs_2 = DockerOperator(
        task_id='clear_gcs_end',
        image='southamerica-east1-docker.pkg.dev/infra-itaborai/dataita/gcs-operations:latest',
        docker_url='tcp://docker-socket-proxy:2375',
        environment={
            "BUCKET_NAME": "{{ params.BUCKET_NAME }}",
            "SOURCE_FOLDER": "{{ params.UNZIP_FOLDER }}",
            "SERVICE": "drop"
        },
        force_pull=False,
        mount_tmp_dir=False
    )
    #start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    list_files = get_files()
    path_files = extract_path_files(list_files)
    extract_zip = zip_to_gcs.expand(period=path_files)
    get_path_extract = get_xcom(extract_zip)

    start >> list_files >> path_files >> extract_zip >> get_path_extract >> end

decred_etl_duckdb()