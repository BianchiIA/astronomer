"""
Dag de Pipeline de ETL dos arquivos decred: zip >> BigQuery
"""

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from airflow.models.param import Param, ParamsDict
from airflow.models import Variable
from airflow.models.baseoperator import chain
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

from google.cloud import bigquery
from google.cloud import storage


from datetime import datetime
from zipfile import ZipFile
from io import BytesIO
import logging

from google.cloud import bigquery
from plugins.operators.pgdas import PgdasETLOperator


# Defina os parâmetros do DAG (valores padrão podem ser sobrescritos ao iniciar o DAG)
conn_id = "gcs_default"
params_dict = {
    'BUCKET_NAME': Param(default='dataita', type="string"),
    'prefix': Param(default='pgdas/pastaRaw/pastaZip/teste', type="string"),
    'dest_data': Param(default='teste/pgdas/pastaProcessed', type="string"),
    'dataset': Param(default='teste', type="string"),
    'SERVICE': Param(default='duckdb', type="string")
}


params = ParamsDict(params_dict)
hook = GoogleBaseHook(gcp_conn_id="gcs_default")
credentials = hook.get_credentials()

@dag(
    dag_id='pgdas_pipeline',
    start_date=datetime(2025, 2, 1),
    schedule="@once",
    doc_md=__doc__,
    catchup=False,
    params=params
)
def pgdas_etl_test():
    
    start = EmptyOperator(task_id='start')
    

    @task(task_id="get_files", multiple_outputs=True)
    def get_files(**kwargs):
        
        """
        Get all zips in folder e subfolders in google cloud storage.

        Returns a dictionary with the following keys:
        file_name: list of file names
        period: list of periods
        path_files: list of paths of the files
        """
        hook = GCSHook(gcp_conn_id=conn_id)
        ## Get all zips in folder e subfolders 
        files = hook.list(params["BUCKET_NAME"], prefix=params["prefix"], delimiter=".zip" )
        list_name_files = [file.split('/')[-1] for file in files]
        logging.info(list_name_files)
        list_period = [f'pgdas-{file.split("-")[4]}-{file.split("-")[5].strip(".zip")}' for file in list_name_files]
        
        metadata = {'file_name': list_name_files, 'period': list_period, 'path_files': files}

        return metadata

    @task
    def extract_path_files(metadata):
        return metadata['path_files']
    
    @task
    def extract_period(metadata):
        return metadata['period']    

    @task(map_index_template='{{ path_files }}')
    def zip_to_gcs(**kwargs):
        
        #context = get_current_context()
        #context['pgdas'] = kwargs['period']
        #params : ParamsDict = kwargs["params"]

        bucket_name = params["BUCKET_NAME"]
        hook = GCSHook(gcp_conn_id=conn_id)
        files = [kwargs['path_files']]
          
        path_descompacted_files = f'tmp/{kwargs['path_files'].split("/")[-1].replace(".zip","")}/'
        #list_dest_paths = list()
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


    @task(task_id="get_xcom")    
    def get_xcom(values):
        return list(values)

    
    pgdas_parquet = PgdasETLOperator(
        
    )
    
    
    @task(map_index_template='{{ source_pgdas }}')
    def process_pgdas_to_parquet( **kwargs):

        params : ParamsDict = kwargs["params"]
        
        #context = get_current_context()
        #context['source_csv'] = kwargs['source_csv']


        bucket_name = params["BUCKET_NAME"]
        path_descompacted_files = kwargs['source_pgdas']
        
   


        
        return dest_path_file
    

    @task(task_id = 'get_xcom2')
    def get_xcom2(values):
        values = [value.replace(f'gs://{params["BUCKET_NAME"]}/', '') for value in values]
        logging.info(f'type values: {type(values)}')
        return values





    @task(task_id='load_parquet_to_bigquery', map_index_template='{{ file_path }}')
    def load_parquet_to_bigquery(file_path):

        bq_client = bigquery.Client(credentials=credentials)
        table_ref = f'infra-itaborai.{params["dataset"]}.decred'
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
        load_job = bq_client.load_table_from_uri(
            f"gs://{params['BUCKET_NAME']}/{file_path}", table_ref, job_config=job_config
        )
        load_job.result()


    @task(map_index_template='{{ folder_path }}')
    def delete_folder(folder_path):
        
        client = storage.Client(credentials=credentials)
        bucket = client.bucket(params['BUCKET_NAME'])
        blobs = bucket.list_blobs(prefix=folder_path)
        for blob in blobs:
            blob.delete()

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    list_files = get_files()
    path_files = extract_path_files(list_files)
    extract_zip = zip_to_gcs.expand(period=path_files)
    get_path_csv_xcom = get_xcom(extract_zip)
    tranform_in_parquet = process_csv_to_parquet.expand(source_csv=get_path_csv_xcom)
    
    get_path_parquet_xcom = get_xcom2(tranform_in_parquet)
    load_tasks = load_parquet_to_bigquery.expand(file_path=get_path_parquet_xcom)

    delete_tmp = delete_folder.expand(folder_path=get_path_csv_xcom)
   

    
    chain(start, list_files, path_files, extract_zip, get_path_csv_xcom, tranform_in_parquet, get_path_parquet_xcom, load_tasks, delete_tmp, end)

    
    

    #start >> list_files >> path_files >> extract_zip >> get_path_csv_xcom >> tranform_in_parquet >> get_path_parquet_xcom >> mylist1[-1] >>  mylist2[-1] >> end
    """
    #tart = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    list_files = get_files()
    path_files = extract_path_files(list_files)
    extract_zip = zip_to_gcs.expand(path_files=path_files)
    get_path_csv_xcom = get_xcom(extract_zip)


    chain(start, list_files, path_files, extract_zip, get_path_csv_xcom)

pgdas_etl_test()