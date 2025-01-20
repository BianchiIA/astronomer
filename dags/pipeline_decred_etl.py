""""

Dag de Pipeline de ETL dos arquivos decred: zip >> BigQuery

"""


from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from airflow.operators.python import PythonOperator

## function remove all files in google cloud storage

with DAG(
    dag_id='decred_etl_new'
    ,start_date=datetime(2024, 9, 27)
    ,schedule="@once"
    ,doc_md = __doc__
    ,catchup=False
):
    start =  EmptyOperator(task_id = 'start')

    clear_gcs = DockerOperator(task_id='clear_gcs',
                               image='southamerica-east1-docker.pkg.dev/infra-itaborai/dataita/gcs-operations:latest',
                               docker_url='tcp://docker-socket-proxy:2375',
                               environment={
                                    "BUCKET_NAME":"dataita",
                                    "SOURCE_FOLDER":"testes/pastaRaw/pastaUnzip",
                                    "SERVICE":"drop"

                                    },
                               force_pull=False,
                               mount_tmp_dir=False
                               )

    extract_files = DockerOperator(task_id='extract_files_gcs',
                                    image='southamerica-east1-docker.pkg.dev/infra-itaborai/dataita/villam/unzip:1.0' , 
                                    docker_url='tcp://docker-socket-proxy:2375',
                                    environment={
                                            "BUCKET_NAME":"dataita",
                                             "PATH_FOLDER":"testes/pastaRaw/pastaZip",
                                             "DEST_FOLDER":"testes/pastaRaw/pastaUnzip/",
                                             "SERVICE":"unzip"
                                             },
                                    force_pull=False,
                                    mount_tmp_dir=False
                                    )
    
    transform_and_load = DockerOperator(task_id='transform_and_load',
                                        image='southamerica-east1-docker.pkg.dev/infra-itaborai/dataita/decred:latest',
                                        docker_url='tcp://docker-socket-proxy:2375',
                                        environment={
                                            "BUCKET_NAME":"dataita",
                                            "PATH_FOLDER":"testes/pastaRaw/pastaUnzip",
                                            "TABLE":"teste"
                                            },
                                        force_pull=False,
                                        mount_tmp_dir=False
                                
                                        )
    
    move_files = DockerOperator(task_id='move_files',
                                        image='southamerica-east1-docker.pkg.dev/infra-itaborai/dataita/villam/move-files:1.0',
                                        docker_url='tcp://docker-socket-proxy:2375',
                                        environment={
                                            "BUCKET_NAME":"dataita",
                                            "PATH_FOLDER":"testes/pastaRaw/pastaUnzip/",
                                            "DEST_FOLDER":"testes/arquivosLidos/"
                                            },
                                        force_pull=False,
                                        mount_tmp_dir=False
                                        )
    
    


    end = EmptyOperator(task_id = 'end')

(start >> clear_gcs >> extract_files >> transform_and_load >> move_files >> end)

