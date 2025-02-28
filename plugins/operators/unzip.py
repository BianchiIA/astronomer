from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.google.cloud.hooks.gcs import GCSHook    
from airflow.models.param import Param, ParamsDict

import logging
from io import BytesIO
from zipfile import ZipFile




class UnzipGCS(BaseOperator):
    @apply_defaults
    def __init__(self, prefix, bucket_name='dataita', conn_id='gcs_default', **kwargs):
        super().__init__(**kwargs)
        self.kwargs = kwargs  
        self.bucket_name = bucket_name
        self.conn_id = conn_id 
        self.prefix  = prefix
    
    
    def execute(self, context):

        """
        Descompacta o arquivo zip do GCS para um diretório temporário no GCS.

        Args:
            **kwargs: 
                path_files (str): Caminho do arquivo zip no GCS

        Returns:
            str: Caminho do diretório temporário no GCS com o nome do arquivo zip descompactado
        """
        #params : ParamsDict = self.kwargs["params"]
        
        bucket_name = self.bucket_name
        hook = GCSHook(gcp_conn_id=self.conn_id)
        files = hook.list(self.bucket_name, prefix=self.prefix, delimiter=".zip" )
        
        logging.info(f'connect in bucket: {bucket_name}')
        
        path_descompacted_files = f'tmp/{self.prefix.split("/")[-1].replace(".zip","")}/'
        for i in range(len(files)):
            zip_data = BytesIO(hook.download(self.bucket_name, files[i]))
            with ZipFile(zip_data, 'r') as zip_ref:
                for file in zip_ref.namelist():
                    if file.endswith('.zip'):
                        continue  # Skip directories in the zip file
                    file_data = zip_ref.read(file)
                    print(path_descompacted_files + file)
                    print(logging.info(path_descompacted_files + file))
                    hook.upload(bucket_name,object_name= path_descompacted_files + file, data=file_data)


        return path_descompacted_files + file