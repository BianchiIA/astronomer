"""
Dag de Pipeline de ETL dos arquivos PGDAS: zip >> BigQuery
"""

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.models.param import Param, ParamsDict
from airflow.models.baseoperator import chain
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook 
from google.cloud import storage


from datetime import datetime
from zipfile import ZipFile
from io import BytesIO
import logging

from plugins.operators.pgdas import PgdasETLOperator
from plugins.operators.gcs_operators import GCSListObjectsOperator

# Defina os parâmetros do DAG (valores padrão podem ser sobrescritos ao iniciar o DAG)
conn_id = "gcs_default"
params_dict = {
    'BUCKET_NAME': Param(default='dataita', type="string"),
    'prefix': Param(default='teste/pgdas/', type="string"),
    'dataset': Param(default='teste', type="string")
}


params = ParamsDict(params_dict)
hook = GoogleBaseHook(gcp_conn_id="gcs_default")
credentials = hook.get_credentials()

@dag(
    dag_id='pgdas_pipeline',
    start_date=datetime(2025, 2, 15),
    schedule="@once",
    doc_md=__doc__,
    catchup=False,
    params=params
)
def pgdas_etl_test():
    

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

    @task(map_index_template='{{ folder_path }}')
    def delete_folder(folder_path):
        
        client = storage.Client(credentials=credentials)
        bucket = client.bucket(params['BUCKET_NAME'])
        blobs = bucket.list_blobs(prefix=folder_path)
        for blob in blobs:
            blob.delete()



    start = EmptyOperator(task_id='start')

    list_files_zip = GCSListObjectsOperator(
        task_id="list_files",
        bucket_name=params['BUCKET_NAME'],
        prefix=params['prefix'],  # Define a pasta onde estão os arquivos
        delimiter=".zip",
        gcp_conn_id=conn_id
    )

    list_files_txt = zip_to_gcs.expand(path_files=list_files_zip)


    pgdas = PgdasETLOperator.partial(
            task_id='pgdas_teste',
            prefix=params['prefix'],
            bucket_name=params['BUCKET_NAME'],
            cloud=True,
            project_id='infra-itaborai',
            destination_table='teste.pgdas',
            credentials=credentials,           
            dataset=params['dataset'],
    ).expand(file=list_files_txt)
    
    delete_tmp = delete_folder.expand(folder_path=list_files_txt)
    
    end = EmptyOperator(task_id='end')
    
    chain(start, list_files_zip)
    


pgdas_etl_test()