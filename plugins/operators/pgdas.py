from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from plugins.hooks.pgdas_hook import ETLPgdasHook
import pandas as pd
from google.cloud import storage


class PgdasETLOperator(BaseOperator):
    @apply_defaults
    def __init__(self, path_file, file, bucket=None, cloud=False, encoding='utf8', mode='r',
                 destination_table=None, project_id=None, credentials=None, if_exists='append', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.path_file = path_file
        self.file = file
        self.bucket = bucket
        self.cloud = cloud
        self.encoding = encoding
        self.mode = mode
        self.destination_table = destination_table
        self.project_id = project_id
        self.if_exists = if_exists
        self.credentials = credentials

    def execute(self, context):
        """
        Método execute é obrigatório e será chamado quando a tarefa for executada.
        """
        # Inicializa o Hook
        dados = ETLPgdasHook()

        # Lê o arquivo
        dados.read_pgdas(
            path_file=self.path_file,
            file=self.file,
            bucket=self.bucket,
            credentials=self.credentials,  # Passe as credenciais se necessário
            cloud=self.cloud,
            encoding=self.encoding,
            mode=self.mode
        )
       

        # Cria o DataFrame e salva no formato Parquet
        df_aaaaa = dados.create_dataframe_aaaaa()
        
        client = storage.Client(credentials=self.credentials)
        bucket = client.get_bucket('my-bucket-name')
    
        bucket.blob('upload_test/test.csv').upload_from_string(df_aaaaa.to_parquet(), 'text/parquet')
        
        

        # Log de sucesso
        self.log.info(f"Arquivo {self.file} processado e salvo em gs://{self.bucket}/teste/pgdas/file-name.parquet.")
