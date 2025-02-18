from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from plugins.hooks.pgdas_hook import ETLPgdasHook
import pandas as pd
from google.cloud import storage
import io
import logging
from pandas_gbq import to_gbq
import os

class PgdasETLOperator(BaseOperator):
    @apply_defaults
    def __init__(self, file, destination_bucket=None, bucket_name=None, cloud=True, encoding='utf8', mode='r',
                 destination_table=None, project_id=None, credentials=None, if_exists='append', dataset=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        #self.path_file = prefix
        self.destination_bucket = destination_bucket
        self.file = file #prefix.split('/')[-1]
        self.bucket_name = bucket_name
        self.cloud = cloud
        self.encoding = encoding
        self.mode = mode
        self.destination_table = destination_table
        self.project_id = project_id
        self.if_exists = if_exists
        self.credentials = credentials
        self.dataset = dataset
    def execute(self, context):
        """
        Método execute é obrigatório e será chamado quando a tarefa for executada.
        """
        # Inicializa o Hook
        dados = ETLPgdasHook()
        path = os.path.dirname(self.file)
        file = self.file.split('/')[-1]
        # Lê o arquivo
        dados.read_pgdas(
            path_file=path,
            file=file,
            bucket=self.bucket_name,
            credentials=self.credentials,  # Passe as credenciais se necessário
            cloud=self.cloud,
            encoding=self.encoding,
            mode=self.mode
        )
        
      
        
        to_gbq(dados.create_dataframe_aaaaa(), destination_table=self.dataset + '.arquivos_importados_aaaaa',
                   if_exists=self.if_exists, credentials=self.credentials)
        
        to_gbq(dados.create_dataframe_00000(), destination_table=self.dataset + '.contribuinte_apuracao_00000',
                project_id=self.project_id, if_exists=self.if_exists, credentials=self.credentials)
        
        to_gbq(dados.create_dataframe_00001(), destination_table=self.dataset + '.processo_nao_optante_00001',
                project_id=self.project_id, if_exists=self.if_exists, credentials=self.credentials)
        
        to_gbq(dados.create_dataframe_01000(), destination_table=self.dataset + '.valores_das_01000', project_id=self.project_id,
                if_exists=self.if_exists, credentials=self.credentials)
        
        to_gbq(dados.create_dataframe_01100(), destination_table=self.dataset + '.perfil_das_01100', project_id=self.project_id,
                if_exists=self.if_exists, credentials=self.credentials)
        
        to_gbq(dados.create_dataframe_01500(), destination_table=self.dataset + '.receita_bruta_per_ant_opcao_01500',
                project_id=self.project_id, if_exists=self.if_exists, credentials=self.credentials)
        
        to_gbq(dados.create_dataframe_01501(), destination_table=self.dataset + '.receita_bruta_per_ant_opcao_int_01501',
                project_id=self.project_id, if_exists=self.if_exists, credentials=self.credentials)
        
        to_gbq(dados.create_dataframe_01502(), destination_table=self.dataset + '.receita_bruta_per_ant_opcao_ext_01502',
                project_id=self.project_id, if_exists=self.if_exists, credentials=self.credentials)
        
        to_gbq(dados.create_dataframe_02000(), destination_table=self.dataset + '.receita_bruta_per_ant_vlrorig_tribfixos_02000', project_id=self.project_id,
                if_exists=self.if_exists, credentials=self.credentials)
        
        to_gbq(dados.create_dataframe_03000(), destination_table=self.dataset + '.estabelecimentos_filial_03000',
                project_id=self.project_id, if_exists=self.if_exists, credentials=self.credentials)
        
        to_gbq(dados.create_dataframe_03100(), destination_table=self.dataset + '.atividade_estabelecimento_03100',
                project_id=self.project_id, if_exists=self.if_exists, credentials=self.credentials)
        
        to_gbq(dados.create_dataframe_03110(), destination_table=self.dataset + '.valor_receita_atividade_fa_03110',
                project_id=self.project_id, if_exists=self.if_exists, credentials=self.credentials)
        
        to_gbq(dados.create_dataframe_03120(), destination_table=self.dataset + '.valor_receita_atividade_fb_03120',
                project_id=self.project_id, if_exists=self.if_exists, credentials=self.credentials)
        
        to_gbq(dados.create_dataframe_03130(), destination_table=self.dataset + '.valor_receita_atividade_fc_03130',
                project_id=self.project_id, if_exists=self.if_exists, credentials=self.credentials)
        
        to_gbq(dados.create_dataframe_03111(), destination_table=self.dataset + '.valor_receita_isencao_03111',
                project_id=self.project_id, if_exists=self.if_exists, credentials=self.credentials)
        
        to_gbq(dados.create_dataframe_03112(), destination_table=self.dataset + '.valor_receita_reducao_03112',
                project_id=self.project_id, if_exists=self.if_exists, credentials=self.credentials)
        
        to_gbq(dados.create_dataframe_03500(), destination_table=self.dataset + '.folha_salarios_03500',
                project_id=self.project_id, if_exists=self.if_exists, credentials=self.credentials)
        
        to_gbq(dados.create_dataframe_contribuintes(), destination_table=self.dataset + '.contribuintes_pgdas',
                project_id=self.project_id, if_exists=self.if_exists, credentials=self.credentials)
    
        
        
        
        """
        client = storage.Client(credentials=self.credentials)
        bucket = client.get_bucket(self.bucket_name)
        
        # Cria o DataFrame e salva no formato Parquet
        destination = self.destination_bucket + '/df_aaaaa.parquet'
        logging.info(f"Destination bucket:o {destination}")
        blob = bucket.blob(destination)
        parquet_buffer = io.BytesIO()
        dados.create_dataframe_aaaaa().to_parquet(parquet_buffer, engine='pyarrow')
        blob.upload_from_file(parquet_buffer, content_type='application/parquet', rewind=True)
        logging.info(f"Salvo no bucket {self.bucket_name} no caminho {destination}")
        
        # Cria o DataFrame e salva no formato Parquet
        destination = self.destination_bucket + '/df_00000.parquet'
        logging.info(f"Destination bucket:o {destination}")
        blob = bucket.blob(destination)
        parquet_buffer = io.BytesIO()
        dados.create_dataframe_00000().to_parquet(parquet_buffer, engine='pyarrow')
        blob.upload_from_file(parquet_buffer, content_type='application/parquet', rewind=True)
        logging.info(f"Salvo no bucket {self.bucket_name} no caminho {destination}")
        
        dados.create_dataframe_00000()
        df_00001 = dados.create_dataframe_00001()
        df_01000 = dados.create_dataframe_01000()
        df_01100 = dados.create_dataframe_01100()
        df_01500 = dados.create_dataframe_01500()
        df_01501 = dados.create_dataframe_01501()
        df_01502 = dados.create_dataframe_01502()
        df_02000 = dados.create_dataframe_02000()
        df_03000 = dados.create_dataframe_03000()
        df_03100 = dados.create_dataframe_03100()
        df_03110 = dados.create_dataframe_03110()
        df_03120 = dados.create_dataframe_03120()
        df_03130 = dados.create_dataframe_03130()
        df_03111 = dados.create_dataframe_03111()
        df_03112 = dados.create_dataframe_03112()
        df_03500 = dados.create_dataframe_03500()
        df_contri = dados.create_dataframe_contribuintes()
        
        
        blob = bucket.blob()
        parquet_buffer = io.BytesIO()
        df_aaaaa.to_parquet(parquet_buffer, engine='pyarrow')
        blob.upload_from_file(parquet_buffer, content_type='application/parquet', rewind=True)
        """
        

        # Log de sucesso
        #self.log.info(f"Arquivo {self.file} processado e salvo em gs://{self.bucket}/teste/pgdas/file-name.parquet.")
