""""

Dag de Pipeline de ETL dos arquivos PGDAs: zip >> BigQuery

"""


from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

## function remove all files in google cloud storage

default_args = {
    'params': {

    "PROJECT_ID": "infra-itaborai",
    "BUCKET_NAME": "dataita",
    "ZIP_FOLDER": "pgdas/pastaRaw/pastaZip",
    "UNZIP_FOLDER": "pgdas/pastaRaw/pastaUnzip2/",

    # Parâmetros da função load_pgdas
    "DATASET": "pgdas.",
    "MODE_SAVE_OUTPUT": "gbq",
    "IF_EXIST_TABLE_BQ": "append",
    "CLOUD_SCR_READ": True,
    "ENCODING_FILE": "utf8",
    "MODE_OPEN_BLOB": "r"
}
}


with DAG(
    dag_id='pgdas_elt',
    start_date=datetime(2024, 10, 3),
    schedule="@once",
    doc_md=__doc__,
    catchup=False,
    default_args=default_args, tags=['pgdas', 'simples nacional']
):
    start = EmptyOperator(task_id='start')

    clear_gcs = DockerOperator(
        task_id='clear_gcs',
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

    extract_files_1 = DockerOperator(
        task_id='extract_files_gcs_1',
        image='southamerica-east1-docker.pkg.dev/infra-itaborai/dataita/gcs-operations:latest',
        docker_url='tcp://docker-socket-proxy:2375',
        environment={
            "BUCKET_NAME": "{{ params.BUCKET_NAME }}",
            "SOURCE_FOLDER": "pgdas/pastaRaw/pastaZip/2021/07",
            "DEST_FOLDER": "{{ params.UNZIP_FOLDER }}",
            "SERVICE": "unzip"
        },
        force_pull=False,
        mount_tmp_dir=False
    )

    extract_files_2 = DockerOperator(
        task_id='extract_files_gcs_2',
        image='southamerica-east1-docker.pkg.dev/infra-itaborai/dataita/gcs-operations:latest',
        docker_url='tcp://docker-socket-proxy:2375',
        environment={
            "BUCKET_NAME": "{{ params.BUCKET_NAME }}",
            "SOURCE_FOLDER": "pgdas/pastaRaw/pastaZip/2021/08",
            "DEST_FOLDER": "{{ params.UNZIP_FOLDER }}",
            "SERVICE": "unzip"
        },
        force_pull=False,
        mount_tmp_dir=False
    )

    extract_files_3 = DockerOperator(
        task_id='extract_files_gcs_3',
        image='southamerica-east1-docker.pkg.dev/infra-itaborai/dataita/gcs-operations:latest',
        docker_url='tcp://docker-socket-proxy:2375',
        environment={
            "BUCKET_NAME": "{{ params.BUCKET_NAME }}",
            "SOURCE_FOLDER": "pgdas/pastaRaw/pastaZip/2021/09",
            "DEST_FOLDER": "{{ params.UNZIP_FOLDER }}",
            "SERVICE": "unzip"
        },
        force_pull=False,
        mount_tmp_dir=False
    )

    extract_files_4 = DockerOperator(
        task_id='extract_files_gcs_4',
        image='southamerica-east1-docker.pkg.dev/infra-itaborai/dataita/gcs-operations:latest',
        docker_url='tcp://docker-socket-proxy:2375',
        environment={
            "BUCKET_NAME": "{{ params.BUCKET_NAME }}",
            "SOURCE_FOLDER": "pgdas/pastaRaw/pastaZip/2021/10",
            "DEST_FOLDER": "{{ params.UNZIP_FOLDER }}",
            "SERVICE": "unzip"
        },
        force_pull=False,
        mount_tmp_dir=False
    )

    extract_files_5 = DockerOperator(
        task_id='extract_files_gcs_5',
        image='southamerica-east1-docker.pkg.dev/infra-itaborai/dataita/gcs-operations:latest',
        docker_url='tcp://docker-socket-proxy:2375',
        environment={
            "BUCKET_NAME": "{{ params.BUCKET_NAME }}",
            "SOURCE_FOLDER": "pgdas/pastaRaw/pastaZip/2021/11",
            "DEST_FOLDER": "{{ params.UNZIP_FOLDER }}",
            "SERVICE": "unzip"
        },
        force_pull=False,
        mount_tmp_dir=False
    )

    extract_files_6 = DockerOperator(
        task_id='extract_files_gcs_6',
        image='southamerica-east1-docker.pkg.dev/infra-itaborai/dataita/gcs-operations:latest',
        docker_url='tcp://docker-socket-proxy:2375',
        environment={
            "BUCKET_NAME": "{{ params.BUCKET_NAME }}",
            "SOURCE_FOLDER": "pgdas/pastaRaw/pastaZip/2021/12",
            "DEST_FOLDER": "{{ params.UNZIP_FOLDER }}",
            "SERVICE": "unzip"
        },
        force_pull=False,
        mount_tmp_dir=False
    )

    """  transform_and_load_pgdas = DockerOperator(
        task_id='transform_and_load',
        image='southamerica-east1-docker.pkg.dev/infra-itaborai/dataita/villam/pgdas:1.0',
        docker_url='tcp://docker-socket-proxy:2375',
        timeout=3600,
        environment={

            "PROJECT_ID": "{{ params.PROJECT_ID }}",
            "DATASET": "{{ params.DATASET }}",
            # Parâmetros da função load_pgdas
            "MODE_SAVE_OUTPUT": "{{ params.MODE_SAVE_OUTPUT }}",
            "IF_EXIST_TABLE_BQ": "{{ params.IF_EXIST_TABLE_BQ }}",
            "BUCKET_NAME": "{{ params.BUCKET_NAME }}",
            "CLOUD_SCR_READ": "{{ params.CLOUD_SCR_READ }}",
            "ENCODING_FILE": "{{ params.ENCODING_FILE }}",
            "MODE_OPEN_BLOB": "{{ params.MODE_OPEN_BLOB }}",

            "BUCKET_NAME": "{{ params.BUCKET_NAME }}",
            "PATH_FILE_PGDAS": "{{ params.UNZIP_FOLDER }}"
        },
        force_pull=False,
        mount_tmp_dir=False,
        mem_limit="52g"
    )""" 
    
    
    run_pgdas_container = BashOperator(
        task_id='run_pgdas_container',
        bash_command="""
        docker run --name pgdas \
        -e PATH_FILE_PGDAS="pgdas/pastaRaw/pastaUnzip/" \
        -d southamerica-east1-docker.pkg.dev/infra-itaborai/dataita/villam/pgdas:1.0
        """,
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

(start >> clear_gcs >> extract_files_1 >> extract_files_2 >> extract_files_3 >> extract_files_4 >> extract_files_5 >> extract_files_6 >> run_pgdas_container >> clear_gcs_2 >> end)


