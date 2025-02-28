from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.models.variable import Variable
from airflow.models.param import Param, ParamsDict

from airflow.providers.google.cloud.hooks.gcs import GCSHook

from datetime import datetime, timedelta
import logging
import duckdb
import os
import io

default_args = { 
    "owner": "Vinicius B. Soares",
    "start_date": datetime(2025, 2, 27),
    "retries": 3,
    "retry_delay": timedelta(minutes=5)}

conn_id = "gcs_default"

hook = GCSHook(gcp_conn_id=conn_id)
MIN_SIZE_BYTES = 3 * 1024 * 1024 * 1024

@dag(
    dag_id="cnpj_rfb_load",
    default_args=default_args,
    schedule_interval="@once",
    catchup=False,
    tags=["cnpj", "extract", "transform", "load"],
    doc_md=__doc__,
    max_active_tasks=1,
    params={
        "bucket_name": Param(
            "dataita",
            type="string",
            title="Bucket Name"
    ),
        "prefix": Param(
            "tmp/estabelecimentos/",
            type="string",
            title="Prefix"
        )
    }
)
def cnpj_rfb_etl():
    #dataset = "cnpj_rfb"
    #table = "estabelecimentos"
    #buket_name = "dataita"
    
    @task
    def split_large_csv(**kwargs):
        gcs_hook = GCSHook(gcp_conn_id='gcs_default')
        params : ParamsDict = kwargs["params"]
        # Abre o arquivo diretamente do GCS como stream
        file_obj = gcs_hook.download_as_byte_array(bucket_name=params['bucket_name'], object_name=kwargs['files'])
        
        # Lê o cabeçalho
        header = file_obj.readline()
        
        part1 = io.BytesIO()
        part2 = io.BytesIO()
        
        part1.write(header)
        part2.write(header)
        
        line_count = 0
        for line in file_obj:
            if line_count % 2 == 0:
                part1.write(line)
            else:
                part2.write(line)
            line_count += 1
        OUTPUT_PATH_1 = kwargs['files']
        OUTPUT_PATH_2 = kwargs['files'] + '_2'
        # Faz o upload dos arquivos gerados para GCS
        for buffer, output_path in [(part1, OUTPUT_PATH_1), (part2, OUTPUT_PATH_2)]:
            buffer.seek(0)
            gcs_hook.upload(bucket_name=params['bucket_name'], object_name=output_path, data=buffer.getvalue(), mime_type='text/csv')
    
    
    @task
    def estabelecimentos_to_parquet(**kwargs):
        
        logging.info("Iniciando leitura de estabelecimentos")
        params : ParamsDict = kwargs["params"]
        bucket_name = params["bucket_name"]
        file_name = params["prefix"].split("/")[-1]
        
        
        duckdb.sql("""
        INSTALL httpfs; -- Instalar extensão necessária
        LOAD httpfs;    -- Carregar a extensão
        """)
        duckdb.sql(f"""
        SET s3_region = 'auto'; -- DuckDB usa 'auto' para regiões GCS
        SET s3_access_key_id = '{Variable.get("KEY_ID_GCS")}';
        SET s3_secret_access_key = '{Variable.get("SECRET_GCS")}';
        """)
        
        colunas_estabelecimento = {
            "CNPJ_BASICO": "STRING",
            "CNPJ_ORDEM": "STRING",
            "CNPJ_DV": "STRING",
            "MATRIZ_FILIAL": "STRING",
            "NOME_FANTASIA": "STRING",
            "SITUACAO_CADASTRAL": "STRING",
            "DATA_SITUACAO_CADASTRAL": "STRING",
            "MOTIVO_SITUACAO": "STRING",
            "NOME_CIDADE_EXT": "STRING",
            "PAIS": "STRING",
            "Data_Inicio_Atividade": "STRING",
            "CNAE_PRINCIPAL": "STRING",
            "CNAE_SECUNDARIA": "STRING",
            "TIPO_LOGRADOURO": "STRING",
            "LOGRADOURO": "STRING",
            "NUM": "STRING",
            "COMPLEMENTO": "STRING",
            "BAIRRO": "STRING",
            "CEP": "STRING",
            "UF": "STRING",
            "MUNICIPIO": "STRING",
            "DDD1": "STRING",
            "TEL1": "STRING",
            "DDD2": "STRING",
            "TEL2": "STRING",
            "DDD_FAX": "STRING",
            "TEL_FAX": "STRING",
            "E_MAIL": "STRING",
            "SITUACAO_ESPECIAL": "STRING",
            "DATA_SIT_ESPECIAL": "STRING"
        }

        dest_path = os.path.join(f"gs://{bucket_name}/tmp/cnpj/estabelecimentos/{datetime.today().strftime('%Y-%m-%d')}/{file_name}.parquet", file_name)
        
        df_db = duckdb.read_csv(f"gs://{bucket_name}/{kwargs['input_file']}", all_varchar=True, dtype=colunas_estabelecimento, delimiter=";", encoding="UTF-8")
        df_db = df_db.write_parquet(dest_path)
    
        
        logging.info("Leitura de estabelecimentos concluida")
        return dest_path
    @task
    def get_file_list(**kwargs):
        params : ParamsDict = kwargs["params"]
        print(params)
        return hook.list(bucket_name=params["bucket_name"], prefix=params["prefix"])
    
    list_files_estab = get_file_list()
    
    divide_arquvivos = split_large_csv.expand(files=list_files_estab)
   #create_parquet_estab = estabelecimentos_to_parquet.expand(input_file=list_files_estab)
    
    #list_files_estab = get_file_list('gs://dataita/tmp/estabelecimentos/', conn_id='gcs_default')
    
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    start >> list_files_estab >> divide_arquvivos >> end
    
    
cnpj_rfb_etl()