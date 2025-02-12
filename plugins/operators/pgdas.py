from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from plugins.hooks.pgdas_hook import PgdasHook

class PgdasETLOperator(BaseOperator):
    @apply_defaults
    def __init__(self, path_file, file, bucket=None, cloud=False, encoding='utf8', mode='r',
                 destination_table=None, project_id=None, if_exists='append', *args, **kwargs):
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

    def execute(self, context):
        # Inicializa o Hook
        hook = PgdasHook(credentials=None)  # Passe as credenciais se necessário

        # 1. Extração: Ler o arquivo
        rows = hook.read_file(
            path_file=self.path_file,
            file=self.file,
            bucket=self.bucket,
            cloud=self.cloud,
            encoding=self.encoding,
            mode=self.mode
        )

        # 2. Transformação: Processar os dados
        # Exemplo: Criar um DataFrame a partir das linhas lidas
        data = [row.strip().split('|') for row in rows]
        df = pd.DataFrame(data, columns=["col1", "col2", "col3"])  # Substitua pelas colunas reais

        # Limpar os dados
        df_cleaned = hook.clean_data(
            df=df,
            real_columns=["col2"],  # Substitua pelas colunas reais
            date_columns=["col3"]    # Substitua pelas colunas de data
        )

        # 3. Carregamento: Carregar os dados no BigQuery
        if self.destination_table and self.project_id:
            hook.load_to_bigquery(
                df=df_cleaned,
                destination_table=self.destination_table,
                project_id=self.project_id,
                if_exists=self.if_exists
            )

        # Log de sucesso
        self.log.info(f"Dados processados e carregados com sucesso para {self.destination_table}.")