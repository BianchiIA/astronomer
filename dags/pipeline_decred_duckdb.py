"""
Dag de Pipeline de ETL dos arquivos decred: zip >> BigQuery
"""

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.empty import EmptyOperator
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
    'SOURCE_FOLDER': Param(default='decred/pastaRaw/pastaZip', type="string"),
    'ZIP_FOLDER': Param(default='decred/pastaRaw/pastaZip', type="string"),
    'UNZIP_FOLDER': Param(default='decred/pastaRaw/pastaUnzip', type="string"),
    'DEST_FOLDER_PROCESSED': Param(default='decred/pastaProcessed', type="string"),
    'TABLE_NAME': Param(default='decred', type="string"),
    'SERVICE': Param(default='duckdb', type="string")
}


params_default = ParamsDict(params_dict)
bucket_name = params_default["BUCKET_NAME"]


@dag(
    dag_id='decred_etl_duckdb',
    start_date=datetime(2024, 10, 3),
    schedule="@once",
    doc_md=__doc__,
    catchup=False
)
def decred_etl_duckdb():

    start = EmptyOperator(task_id='start')

    @task
    def zip_to_gcs(**kwargs):

        params : ParamsDict = kwargs["params"]

        hook = GCSHook(gcp_conn_id=conn_id)
        files = hook.list(bucket_name, prefix=params["prefix"], delimiter=".zip")
        destination_file = f'tmp/{datetime.now().strftime("%Y-%m-%d-%H-%M-%S")}/'
        for i in range(len(files)):
            zip_data = BytesIO(hook.download(bucket_name, files[i]))
            with ZipFile(zip_data, 'r') as zip_ref:
                for file in zip_ref.namelist():
                    if file.endswith('.zip'):
                        continue  # Skip directories in the zip file
                    file_data = zip_ref.read(file)
                    print(destination_file + file)
                    print(logging.info(destination_file + file))
                    hook.upload(bucket_name,object_name= destination_file + file, data=file_data)


        return destination_file


    @task(task_id="teste")
    def get_metadata(path_file):

        file = path_file.split('/')[-1]
        file = file.split('_')
        file[-1] = file[-1].strip('.zip')

        id_file = file[4], file[5], file[6]

        df = pd.DataFrame(data=np.array(file).reshape(-1, len(file)),
            columns = ['tipo1', 'tipo2', 'tipo3','cnpj_minicipio', 'df_ref_init', 'df_ref_end', 'id_extact' ])

        return id_file

        


    @task(multiple_outputs=True)
    def process_csv_to_parquet(destination_file, **kwargs):

        params : ParamsDict = kwargs["params"]

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
        logging.info(f"lendo arquivos de: gs://{bucket_name}/{destination_file}*")
        df_db = duckdb.read_csv(f"gs://{bucket_name}/{destination_file}*.csv",dtype=tipos, decimal=',')


        # Extracao periodo de pagamentos
        periodo_arr = duckdb.sql("""
            SELECT MAX(CAST(data_arrecadacao AS INT)) AS MAX_PER, MIN(CAST(data_arrecadacao AS INT)) AS MIN_PER FROM df_meta
        """ ).fetchnumpy()
        
        prefix_dest  = f'{params["dest_data"]}-{datetime.now().strftime("%Y-%m-%d-%H-%M-%S")}/'
        dest_path_root = os.path.join(f'gs://{bucket_name}/', prefix_dest)

        # Criação de dados em parquet
        df_content.write_parquet(os.path.join(dest_path_root, f"content/daspag-{periodo_arr['MIN_PER'][0]}-{periodo_arr['MAX_PER'][0]}.parquet"))
        df_meta.write_parquet(os.path.join(dest_path_root, f"meta/daspagmeta-{periodo_arr['MIN_PER'][0]}-{periodo_arr['MAX_PER'][0]}.parquet"))

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

    end = EmptyOperator(task_id='end')

    a = get_metadata(path_file='testes/pastaRaw/pastaZip/EXTTR_DIMP_SMF_28741080000155_01012023_31012023_314030.zip')

    start >> a >> end

decred_etl_duckdb()