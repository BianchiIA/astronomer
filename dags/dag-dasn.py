from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
 


from datetime import datetime, timedelta
import logging
import io
from zipfile import ZipFile
import os

import duckdb


conn_id = "gcs_default"
bucket_name = Variable.get("bucket_name")
prefix = Variable.get("prefix")
dest_data = Variable.get("dest_data")


DATASET_NAME = 'teste2'
TABLE_NAME = 'daspag'

default_args = {
    'params': {"prefix":'testes/pastaRaw/daspag',
               "dest_data": 'testes/pastaParquet/daspag'
               }
}



@dag(
    dag_id="dag-dasn",
    start_date=datetime(2024, 11, 25),
    schedule_interval='@once',
    catchup=False,
    #default_args=default_args
)
def init():

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    @task
    def zip_to_gcs():
        #prefix = "{{ params.prefix }}"
        hook = GCSHook(gcp_conn_id=conn_id)
        files = hook.list(bucket_name, prefix=prefix, delimiter=".zip")
        destination_file = f'tmp/{datetime.now().strftime("%Y-%m-%d-%H-%M-%S")}/'
        for i in range(len(files)):
            zip_data = io.BytesIO(hook.download(bucket_name, files[i]))
            with ZipFile(zip_data, 'r') as zip_ref:
                for file in zip_ref.namelist():
                    if file.endswith('.zip'):
                        continue  # Skip directories in the zip file
                    file_data = zip_ref.read(file)
                    print(destination_file + file)
                    print(logging.info(destination_file + file))
                    hook.upload(bucket_name,object_name= destination_file + file, data=file_data)


        return destination_file
    

    @task(multiple_outputs=True)
    def process_csv_to_parquet(destination_file):
        bucket_name = Variable.get("bucket_name")
        #dest_data = "{{ params.dest_data }}"
        duckdb.sql(f"""CREATE SECRET (
        TYPE GCS,
        KEY_ID {Variable.get("KEY_ID_GCS")},
        SECRET {Variable.get("SECRET_GCS")}
        );""")


        nomes_colunas = [
                "identificacao_registro",
                "numero_das",
                "data_arrecadacao",
                "codigo_banco",
                "codigo_agencia",
                "numero_remessa_bancaria",
                "numero_daf607",
                "valor_total_das",
                "sequencial_registro"
                ]
        #

        tipos = {'identificacao_registro': 'VARCHAR',
            'numero_das': 'VARCHAR',
            'data_arrecadacao': 'VARCHAR',
            'codigo_banco': 'VARCHAR',
            'codigo_agencia': 'VARCHAR',
            'numero_remessa_bancaria': 'VARCHAR',
            'numero_daf607': 'VARCHAR',
            'valor_total_das': 'VARCHAR',
            'sequencial_registro': 'VARCHAR'}
    
        # Criação de um banco de dados DuckDB em memória
        
        df_db = duckdb.read_csv(f"gs://{bucket_name}/{destination_file}*" , sep='|', header=False, dtype=tipos, names=nomes_colunas)

        # Filtros de conteúdo e metadados
        df_content = duckdb.sql('''
            SELECT "identificacao_registro",
                "numero_das",
                "data_arrecadacao",
                "codigo_banco",
                "codigo_agencia",
                "numero_remessa_bancaria",
                "numero_daf607",
                "valor_total_das",
                "sequencial_registro" 
            FROM df_db WHERE identificacao_registro = 1
            '''
            )
                
        df_meta = duckdb.sql('''
            SELECT "identificacao_registro",
                "data_arrecadacao",
                "numero_remessa_bancaria",
                "numero_daf607",
                "valor_total_das",
                "sequencial_registro"
            FROM df_db WHERE identificacao_registro = 0
            '''
            )

        # Extracao periodo de pagamentos
        periodo_arr = duckdb.sql("""
            SELECT MAX(CAST(data_arrecadacao AS INT)) AS MAX_PER, MIN(CAST(data_arrecadacao AS INT)) AS MIN_PER FROM df_meta
        """ ).fetchnumpy()
        
        prefix_dest  = f"{dest_data}-{datetime.now().strftime("%Y-%m-%d-%H-%M-%S")}/"
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

    load_content_parquet = GCSToBigQueryOperator(
        task_id="gcs_to_bigquery_dasn_content",
        gcp_conn_id=conn_id,  
        bucket=bucket_name,
        source_format='PARQUET',
        source_objects=[os.path.join("{{ ti.xcom_pull(task_ids='process_csv_to_parquet', key='content') }}", "*.parquet")],
        destination_project_dataset_table=f"{DATASET_NAME}.daspag",
        write_disposition="WRITE_APPEND",
    )

    load_meta_parquet = GCSToBigQueryOperator(
        task_id="gcs_to_bigquery_dasn_meta",
        gcp_conn_id=conn_id, 
        bucket=bucket_name,
        source_format='PARQUET',
        source_objects=[os.path.join("{{ ti.xcom_pull(task_ids='process_csv_to_parquet', key='meta') }}", "*.parquet")],
        destination_project_dataset_table=f"{DATASET_NAME}.daspag_meta",
        write_disposition="WRITE_APPEND",
    )

    delete_gcs_tmp = GCSDeleteObjectsOperator(
        task_id="delete_gcs_tmp",
        bucket_name=bucket_name,
        prefix="{{ ti.xcom_pull(task_ids='zip_to_gcs', key='return_value') }}",
        gcp_conn_id=conn_id
    )
    
    unzip_gcs = zip_to_gcs()
    save_parquet_file = process_csv_to_parquet(unzip_gcs)

    start >>  unzip_gcs >> save_parquet_file >> [load_content_parquet, load_meta_parquet] >> delete_gcs_tmp >> end

dag = init()    

