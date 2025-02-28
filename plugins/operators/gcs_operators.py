
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.google.cloud.hooks.gcs import GCSHook    

import logging


class GCSListObjectsOperators(BaseOperator): 
    template_fields = ("bucket_name", "prefix") 
    @apply_defaults 
    def __init__(self, prefix, bucket_name=None, gcp_conn_id='gcs_default', delimiter=None, **kwargs): 
        super().__init__(**kwargs)
        self.prefix = prefix
        self.bucket_name = bucket_name
        self.delimiter = delimiter
        self.gcp_conn_id = gcp_conn_id
        self.kwargs = kwargs

    def execute(self, context): 
        logging.info(f'Args from GCSListObjectsOperators: prefix:{self.prefix}, bucket_name {self.bucket_name}, gcp_conn_id: {self.gcp_conn_id}, delimiter: {self.delimiter}, kwargs: {self.kwargs}') #self.kwargs)  
        
        hook = GCSHook(gcp_conn_id=self.gcp_conn_id)
        ## Get all zips in folder e subfolders 
        files = hook.list(self.bucket_name, prefix= self.prefix, delimiter=self.delimiter  )
        list_name_files = [file.split('/')[-1] for file in files]
        logging.info(list_name_files)
               
        #metadata_decred = {'file_name': list_name_files, 'path_files': files}
  
        return files