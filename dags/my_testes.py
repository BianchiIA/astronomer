from plugins.operators.pgdas import PgdasETLOperator
from airflow.decorators import dag, task
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

from datetime import datetime

hook = GoogleBaseHook(gcp_conn_id="gcs_default")
credentials = hook.get_credentials()

@dag(
    dag_id='mytests',
    start_date=datetime(2025, 2, 1),
    schedule="@once",
    doc_md=__doc__,
    catchup=False
    #params=params
)
def test():
    pgdas = PgdasETLOperator(
        task_id='pgdas_teste',
        path_file="tmp/90-0000-PUB-PGDASD2018-20211205-01/",
        file='90-0000-PUB-PGDASD2018-20211205-01.txt',
        bucket='dataita',
        cloud=True,
        project_id='infra-itaborai',
        destination_table='teste.pgdas',
        credentials=credentials

    )
    
    
    
test()