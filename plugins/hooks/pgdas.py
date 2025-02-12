import os
import pandas as pd
from pandas_gbq import to_gbq
from google.cloud import storage
from airflow.hooks.base import BaseHook

class PgdasHook(BaseHook):
    def __init__(self, credentials=None):
        super().__init__()
        self.credentials = credentials

    def read_file(self, path_file, file=None, bucket=None, cloud=False, encoding='utf8', mode='r'):
        """
        Lê um arquivo do sistema de arquivos local ou do Google Cloud Storage.
        """
        if cloud:
            client = storage.Client(credentials=self.credentials)
            bucket = client.bucket(bucket_name=bucket)
            blob = bucket.blob(os.path.join(path_file, file))
            with blob.open(mode=mode, encoding=encoding) as f:
                rows = f.readlines()
        else:
            with open(os.path.join(path_file, file), mode, encoding=encoding) as f:
                rows = f.readlines()
        return rows

    def clean_data(self, df, real_columns, date_columns):
        """
        Realiza a limpeza básica dos dados.
        """
        df_cleaned = df.copy()
        if real_columns:
            for col in real_columns:
                df_cleaned[col] = pd.to_numeric(df_cleaned[col].str.replace(',', '.'), errors='coerce', downcast='float')
        if date_columns:
            for col in date_columns:
                df_cleaned[col] = pd.to_datetime(df_cleaned[col].apply(lambda x: x[:8]), format='%Y%m%d', errors='coerce')
        return df_cleaned

    def load_to_bigquery(self, df, destination_table, project_id, if_exists='append'):
        """
        Carrega um DataFrame no BigQuery.
        """
        to_gbq(df, destination_table=destination_table, project_id=project_id, if_exists=if_exists, credentials=self.credentials)