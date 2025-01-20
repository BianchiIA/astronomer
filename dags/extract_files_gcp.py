""""
Dag de extração de arquivos .zip do cloud storage

"""


from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime


default_args = {
    'params': {
        'bucket_name': 'dataita',
        'path_folder': 'dasn/pastaRaw/pastaZip/',
        'dest_folder': 'dasn/pastaRaw/pastaUnzip/'
    }
}



with DAG(
    dag_id='unzip_gcp'
    ,start_date=datetime(2024, 11, 13)
    ,schedule="@once"
    ,doc_md = __doc__
    ,default_args=default_args
    ,catchup=False
):
    start =  EmptyOperator(task_id = 'start')


    extract_files = DockerOperator(task_id='extract_files_gcs',
                                    image='southamerica-east1-docker.pkg.dev/infra-itaborai/dataita/villam/unzip:1.0' , 
                                    docker_url='tcp://docker-socket-proxy:2375',
                                    environment={
                                            "BUCKET_NAME":"{{ params.bucket_name }}",
                                             "PATH_FOLDER":"{{ params.path_folder }}",
                                             "DEST_FOLDER":"{{ params.dest_folder }}",
                                             "SERVICE":"unzip"
                                             },
                                    force_pull=False)


    end = EmptyOperator(task_id = 'end')

(start >> extract_files >> end)

