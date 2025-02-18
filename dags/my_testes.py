from plugins.operators.pgdas import PgdasETLOperator
from plugins.operators.gcs_operators import GCSListObjectsOperators
from airflow.decorators import dag, task
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

from datetime import datetime
from airflow.models.param import Param, ParamsDict


params_dict = {
    'BUCKET_NAME': Param(default='dataita', type="string"),
    'prefix': Param(default='teste/pgdas/', type="string"),
    'dataset': Param(default='teste', type="string")
}


params = ParamsDict(params_dict)
hook = GoogleBaseHook(gcp_conn_id="gcs_default")
credentials = hook.get_credentials()
conn_id = "gcs_default"

@dag(
    dag_id='mytests',
    start_date=datetime(2025, 2, 18),
    schedule="@once",
    doc_md=__doc__,
    catchup=False
    #params=params
)
def test():

    teste = GCSListObjectsOperators(
        task_id="list_files",
        bucket_name=params['BUCKET_NAME'],
        prefix= params['prefix'],  # Define a pasta onde estão os arquivos
        delimiter=".zip",
        gcp_conn_id=conn_id
    )
    

    
    
    
    
test()