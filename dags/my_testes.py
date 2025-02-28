from plugins.operators.pgdas import PgdasETLOperator
from plugins.operators.gcs_operators import GCSListObjectsOperators
from airflow.decorators import dag, task
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

from datetime import datetime
from airflow.models.param import Param, ParamsDict
from plugins.operators.unzip import UnzipGCS
from datetime import timedelta

params_dict = {
    'bucket_name': Param(default='dataita', type="string"),
    'prefix': Param(default='teste/pgdas/', type="string"),
    'dataset': Param(default='teste', type="string")
}


params = ParamsDict(params_dict)
hook = GoogleBaseHook(gcp_conn_id="gcs_default")
credentials = hook.get_credentials()
conn_id = "gcs_default"
bucket_name = params["bucket_name"]

@dag(
    dag_id='mytests',
    start_date=datetime(2025, 2, 18),
    schedule="@once",
    doc_md=__doc__,
    catchup=False,
    params=params_dict
)
def test():

    teste = UnzipGCS(
        execution_timeout=timedelta(minutes=10),
        task_id='test_unzip_123',
        bucket_name=bucket_name,
        conn_id=conn_id,
        prefix='cnpj/2025-02/estabelecimentos',
        
    )

    teste
    
    
test()