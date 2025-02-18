from airflow.hooks.base import BaseHook

# Imports e Settings

import os
import pandas as pd
from pandas_gbq import to_gbq
from google.cloud import storage

#from dotenv import load_dotenv
#load_dotenv()



# Nome das metadados
dicionario_00000 = {
    "colunas": [
        "REG",
        "Pgdasd_ID_Declaracao",
        "Pgdasd_Num_Recibo",
        "Pgdasd_Num_Autenticacao",
        "Pgdasd_Dt_Transmissao",
        "Pgdasd_Versao",
        "Cnpjmatriz",
        "Nome",
        "Cod_TOM",
        "Optante",
        "Abertura",
        "PA",
        "Rpa",
        "R",
        "Operacao",
        "Regime",
        "RpaC",
        "Rpa_Int",
        "Rpa_Ext",
        "Rpac_Int",
        "Rpac_Ext",
        "RetidoMalha",
        "IP",
        "SerieCertificadoDigital",
        "NomeArquivo"
    ],
    "colunas_reais": [
        "Rpa",
        "RpaC",
        "Rpa_Int",
        "Rpa_Ext",
        "Rpac_Int",
        "Rpac_Ext",
    ],
    'colunas_datas': [
        'Pgdasd_Dt_Transmissao',
        'Abertura',

    ]
}
dicionario_00001 = {

    'colunas': [
        "REG",
        "admtrib",
        "uf",
        "munic",
        "codmunic",
        "numproc"
    ],
    'colunas_reais': None,
    'colunas_datas': None
}

dicionario_01000 = {
    'colunas': [
        "REG",
        "Nrpagto",
        "Princ",
        "Multa",
        "Juros",
        "Tdevido",
        "Dtvenc",
        "Dtvalcalc",
        "Dt_Emissao_Das"
    ],

    'colunas_reais': [
        "Princ",
        "Multa",
        "Juros",
        "Tdevido"
    ],
    'colunas_datas': [
        "Dtvenc",
        "Dtvalcalc",
        "Dt_Emissao_Das"
    ]
}

dicionario_01100 = {
    'colunas': [
        "REG",
        "codrecp",
        "valorprinc",
        "codrecm",
        "valorm",
        "codrecj",
        "valorj",
        "uf",
        "codmunic"
    ],

    'colunas_reais': [
        "valorprinc",
        "valorm",
        "valorj"
    ],
    'colunas_datas': None
}

dicionario_01500 = {
    'colunas': [
        "REG",
        "rbsn_PA",
        "rbsn_valor"
    ],
    'colunas_reais': [
        "rbsn_valor"
    ],
    'colunas_datas': None
}

dicionario_01501 = {
    'colunas': [
        "REG",
        "Rbsn_Int_PA",
        "Rbsn_Int_valor"
    ],
    'colunas_reais': [
        "Rbsn_Int_valor"
    ],
    'colunas_datas': None
}
dicionario_01502 = {
    'colunas': [
        "REG",
        "Rbsn_Ext_PA",
        "Rbsn_Ext_valor"
    ],
    'colunas_reais': [
        "Rbsn_Ext_valor"
    ],
    'colunas_datas': None
}

dicionario_02000 = {
    'colunas': [
        "REG",
        "rbt12",
        "Rbtaa",
        "Rba",
        "rbt12o",
        "Rbtaao",
        "ICMS",
        "ISS",
        "Rbtaa_Int",
        "Rbtaa_Into",
        "Rbtaa_Ext",
        "Rbtaa_Exto",
        "Rbt12_Int",
        "Rbt12_Into",
        "Rba_Int",
        "Rba_Ext",
        "Rbt12_Ext",
        "Rbt12_Exto"
    ],
    'colunas_reais': [
        "rbt12",
        "Rbtaa",
        "Rba",
        "rbt12o",
        "Rbtaao",
        "ICMS",
        "ISS",
        "Rbtaa_Int",
        "Rbtaa_Into",
        "Rbtaa_Ext",
        "Rbtaa_Exto",
        "Rbt12_Int",
        "Rbt12_Into",
        "Rba_Int",
        "Rba_Ext",
        "Rbt12_Ext",
        "Rbt12_Exto"
    ],
    'colunas_datas': None
}
dicionario_03000 = {
    'colunas': [
        "REG",
        "CNPJ",
        "Uf",
        "Cod_TOM",
        "Vltotal",
        "Sublimite",
        "Prex_int_sublimite",
        "Prex_int_limite",
        "Prex_ext_sublimite",
        "Prex_ext_limite",
        "ImpedidoIcmsIss"
    ],
    'colunas_reais': [
        "Vltotal",
        "Sublimite",
        "Prex_int_sublimite",
        "Prex_int_limite",
        "Prex_ext_sublimite",
        "Prex_ext_limite"
    ],
    'colunas_datas': None
}

dicionario_03100 = {
    'colunas': [
        "REG",
        "Tipo",
        "Vltotal"
    ],
    'colunas_reais': [
        "Vltotal"
    ],
    'colunas_datas': None
}

dicionario_03110 = {
    'colunas': [
        "REG",
        "UF",
        "Cod_TOM",
        "Valor",
        "COFINS",
        "CSLL",
        "ICMS",
        "INSS",
        "IPI",
        "IRPJ",
        "ISS",
        "PIS",
        "Aliqapur",
        "Vlimposto",
        "Aliquota_apurada_de_COFINS",
        "Valor_apurado_de_COFINS",
        "Aliquota_apurada_de_CSLL",
        "Valor_apurado_de_CSLL",
        "Aliquota_apurada_de_ICMS",
        "Valor_apurado_de_ICMS",
        "Aliquota_apurada_de_INSS",
        "Valor_apurado_de_INSS",
        "Aliquota_apurada_de_IPI",
        "Valor_apurado_de_IPI",
        "Aliquota_apurada_de_IRPJ",
        "Valor_apurado_de_IRPJ",
        "Aliquota_apurada_de_ISS",
        "Valor_apurado_de_ISS",
        "Aliquota_apurada_de_PIS",
        "Valor_apurado_de_PIS"
    ],

    'colunas_reais': [
        "Valor",
        "Aliqapur",
        "Vlimposto",
        "Aliquota_apurada_de_COFINS",
        "Valor_apurado_de_COFINS",
        "Aliquota_apurada_de_CSLL",
        "Valor_apurado_de_CSLL",
        "Aliquota_apurada_de_ICMS",
        "Valor_apurado_de_ICMS",
        "Aliquota_apurada_de_INSS",
        "Valor_apurado_de_INSS",
        "Aliquota_apurada_de_IPI",
        "Valor_apurado_de_IPI",
        "Aliquota_apurada_de_IRPJ",
        "Valor_apurado_de_IRPJ",
        "Aliquota_apurada_de_ISS",
        "Valor_apurado_de_ISS",
        "Aliquota_apurada_de_PIS",
        "Valor_apurado_de_PIS"
    ],
    'colunas_datas': None

}

dicionario_03120 = {
    'colunas': [
        "REG",
        "Aliqapur",
        "Aliquota_apurada_de_COFINS",
        "Valor_apurado_de_COFINS",
        "Aliquota_apurada_de_CSLL",
        "Valor_apurado_de_CSLL",
        "Aliquota_apurada_de_ICMS",
        "Valor_apurado_de_ICMS",
        "Aliquota_apurada_de_INSS",
        "Valor_apurado_de_INSS",
        "Aliquota_apurada_de_IPI",
        "Valor_apurado_de_IPI",
        "Aliquota_apurada_de_IRPJ",
        "Valor_apurado_de_IRPJ",
        "Aliquota_apurada_de_ISS",
        "Valor_apurado_de_ISS",
        "Aliquota_apurada_de_PIS",
        "Valor_apurado_de_PIS",
        "Vlimposto"
    ],
    'colunas_reais': [
        "Aliqapur",
        "Aliquota_apurada_de_COFINS",
        "Valor_apurado_de_COFINS",
        "Aliquota_apurada_de_CSLL",
        "Valor_apurado_de_CSLL",
        "Aliquota_apurada_de_ICMS",
        "Valor_apurado_de_ICMS",
        "Aliquota_apurada_de_INSS",
        "Valor_apurado_de_INSS",
        "Aliquota_apurada_de_IPI",
        "Valor_apurado_de_IPI",
        "Aliquota_apurada_de_IRPJ",
        "Valor_apurado_de_IRPJ",
        "Aliquota_apurada_de_ISS",
        "Valor_apurado_de_ISS",
        "Aliquota_apurada_de_PIS",
        "Valor_apurado_de_PIS",
        "Vlimposto"
    ],
    'colunas_datas': None
}

dicionario_03130 = {
    'colunas': [
        "REG",
        "Aliqapur",
        "Aliquota_apurada_de_COFINS",
        "Valor_apurado_de_COFINS",
        "Aliquota_apurada_de_CSLL",
        "Valor_apurado_de_CSLL",
        "Aliquota_apurada_de_ICMS",
        "Valor_apurado_de_ICMS",
        "Aliquota_apurada_de_INSS",
        "Valor_apurado_de_INSS",
        "Aliquota_apurada_de_IPI",
        "Valor_apurado_de_IPI",
        "Aliquota_apurada_de_IRPJ",
        "Valor_apurado_de_IRPJ",
        "Aliquota_apurada_de_ISS",
        "Valor_apurado_de_ISS",
        "Aliquota_apurada_de_PIS",
        "Valor_apurado_de_PIS",
        "Vlimposto"
    ],
    'colunas_reais': [
        "Aliqapur",
        "Aliquota_apurada_de_COFINS",
        "Valor_apurado_de_COFINS",
        "Aliquota_apurada_de_CSLL",
        "Valor_apurado_de_CSLL",
        "Aliquota_apurada_de_ICMS",
        "Valor_apurado_de_ICMS",
        "Aliquota_apurada_de_INSS",
        "Valor_apurado_de_INSS",
        "Aliquota_apurada_de_IPI",
        "Valor_apurado_de_IPI",
        "Aliquota_apurada_de_IRPJ",
        "Valor_apurado_de_IRPJ",
        "Aliquota_apurada_de_ISS",
        "Valor_apurado_de_ISS",
        "Aliquota_apurada_de_PIS",
        "Valor_apurado_de_PIS",
        "Vlimposto"
    ],
    'colunas_datas': None
}

dicionario_03111 = {
    'colunas': [
        "REG",
        "Valor"
    ],
    'colunas_reais': [
        "Valor"
    ],
    'colunas_datas': None
}

dicionario_03112 = {
    'colunas': [
        "REG",
        "Valor",
        "Red"
    ],
    'colunas_reais': [
        "Valor",
        "Red"
    ],
    'colunas_datas': None
}
dicionario_03500 = {
    'colunas': [
        "REG",
        "fssn_PA",
        "fssn_valor"
    ],
    'colunas_reais': [
        "fssn_valor"
    ],
    'colunas_datas': None
}
dicionario_aaaaa = {
    'colunas': [
        'REG_ABERTURA',
        'COD_VER',
        'DT_INI',
        'DT_FIN',
        'NomeArquivo'
    ],
    'colunas_reais': None,
    'colunas_datas': [
        #'DT_INI',
        #'DT_FIN'
    ]
}

class ETLPgdasHook(BaseHook):
    def __init__(self):
        super().__init__()
        self.atributos_arquivo_aaaaa = []
        self.contribuite_apuracao_00000 = []
        self.info_sobre_processo_nao_optante_00001 = []
        self.info_perfil_das_01100 = []
        self.info_valores_calculados_das_01000 = []
        self.receita_bruta_anterior_opcao_01500 = []
        self.receita_bruta_anterior_opcao_mercado_interno_01501 = []
        self.receita_bruta_anterior_opcao_mercado_externo_01502 = []
        self.receita_bruta_anterior_opcao_valor_original_tributos_fixos_02000 = []
        self.info_estabelecimentos_03000 = []
        self.estabelecimentos_atividade_03100 = []
        self.rec_estab_atividade_faixa_a_03110 = []
        self.rec_estab_atividade_faixa_b_03120 = []
        self.rec_estab_atividade_faixa_c_03130 = []
        self.info_valor_rec_com_isencao_faixa_a_03111 = []
        self.info_valor_receita_redeucao_percentual_faixa_a_03112 = []
        self.folha_salarial_03500 = []
        self.erros = []

    def _read(self, path_file,file=None, bucket=None, credentials=None, cloud=False, encoding='utf8', mode='r'):
        path_file = os.path.join(path_file, file)
        print(path_file)
        if cloud:
            print('aqui')
            client = storage.Client(credentials=credentials)
            bucket = client.bucket(bucket_name=bucket)
            blob = bucket.blob(path_file)
            #data = StringIO(blob.download_as_string())
            
            with blob.open(mode=mode, encoding=encoding) as file:
                rows = file.readlines()
                file.close()
            #print(rows[:10])
            return rows
            
        else:
            print('aqui2')
            arq = open(path_file, mode, encoding='utf-8')
            rows = arq.readlines()
            return rows
        
        
        
    
    def read_pgdas(self, path_file, file=None, bucket=None, credentials=None, cloud=False, encoding='utf8', mode='r'):

        #arq = open(path_file, 'r', encoding='utf-8')
        rows = self._read(path_file=path_file,file=file, bucket=bucket, credentials=credentials, cloud=cloud, encoding=encoding, mode=mode)

        for i in range(len(rows)):
            linha = rows[i].strip('\n')  # .split('|')
            n_linha = len(linha.split('|'))

            if linha[0:5] == 'AAAAA':
                self.atributos_arquivo_aaaaa.append(linha + '|' + file)


            elif linha[0:5] == '00000':
                if n_linha == 21:
                    self.contribuite_apuracao_00000.append(linha + '||||' + file)
                    # id_pgdas = linha.split('|')[1]
                else:
                    self.contribuite_apuracao_00000.append(linha + '|' + file)
                id_pgdas = linha.split('|')[1]
                # print(id_pgdas)

            elif linha[0:5] == '00001':
                self.info_sobre_processo_nao_optante_00001.append(linha + '|' + id_pgdas)


            elif linha[0:5] == '01000':
                self.info_valores_calculados_das_01000.append(linha + '|' + id_pgdas)
                guia_das = linha.split('|')[1]
                # print(linha + '|' + id_pgdas)

            elif linha[0:5] == '01100':
                self.info_perfil_das_01100.append(linha + '|' + id_pgdas + '|' + guia_das)

            elif linha[0:5] == '01500':
                self.receita_bruta_anterior_opcao_01500.append(linha + '|' + id_pgdas)

            elif linha[0:5] == '01501':
                self.receita_bruta_anterior_opcao_mercado_interno_01501.append(linha + '|' + id_pgdas)

            elif linha[0:5] == '01502':
                self.receita_bruta_anterior_opcao_mercado_externo_01502.append(linha + '|' + id_pgdas)

            elif linha[0:5] == '02000':
                self.receita_bruta_anterior_opcao_valor_original_tributos_fixos_02000.append(linha + '|' + id_pgdas)


            ## Estabelecimentos
            elif linha[0:5] == '03000':

                if n_linha == 10:
                    self.info_estabelecimentos_03000.append(linha + '||' + id_pgdas)
                    # id_pgdas = linha.split('|')[1]
                else:
                    self.info_estabelecimentos_03000.append(linha + '|' + id_pgdas)

                estabelecimento = linha.split('|')[1] + '|' + id_pgdas

            elif linha[0:5] == '03100':
                self.estabelecimentos_atividade_03100.append(estabelecimento + '|' + linha)
                tipo_atividade = linha.split('|')[1]




            elif linha[0:5] == '03110':
                self.rec_estab_atividade_faixa_a_03110.append(estabelecimento + '|' + tipo_atividade + '|' + linha)

            elif linha[0:5] == '03120':
                self.rec_estab_atividade_faixa_b_03120.append(estabelecimento + '|' + tipo_atividade + '|' + linha)

            elif linha[0:5] == '03130':
                self.rec_estab_atividade_faixa_c_03130.append(estabelecimento + '|' + tipo_atividade + '|' + linha)


            elif linha[0:5] == '03111':
                self.info_valor_rec_com_isencao_faixa_a_03111.append(
                    estabelecimento + '|' + tipo_atividade + '|' + linha)
                # print(estabelecimento + '|' + linha)

            elif linha[0:5] == '03112':
                self.info_valor_receita_redeucao_percentual_faixa_a_03112.append(
                    estabelecimento + '|' + tipo_atividade + '|' + linha)
                # print(estabelecimento + '|' + linha)

            ## Fim Estabelecimentos

            elif linha[0:5] == '03500':
                self.folha_salarial_03500.append(linha + '|' + id_pgdas)

    def clean(self, df, real_columns, date_columns):
        df_c = df.copy()
        if real_columns is not None:
            for col in real_columns:
                df_c[col] = pd.to_numeric(df_c[col].str.replace(',', '.'), errors='coerce', downcast='float')

        if date_columns is not None:
            for col in date_columns:
                #df_c[col] = df_c[col].astype(str).str.ljust(14, '0') 
                df_c[col] = pd.to_datetime(df_c[col], format='mixed', errors='coerce')

        if 'id_pgdas' in df_c:
            df_c['RAIZ_CNPJ'] = df_c['id_pgdas'].apply(lambda x: x[:8])
            df_c['PA'] = df_c['id_pgdas'].apply(lambda x: x[8:14])
            df_c['SEQ_PGDAS_ID'] = df_c['id_pgdas'].apply(lambda x: x[14:])

        if 'PA' in df_c:
            df_c['PA'] = pd.to_numeric(df_c['PA'], downcast='integer')

        return df_c

    def create_dataframe_aaaaa(self, meta=dicionario_aaaaa):
        list_data = self.atributos_arquivo_aaaaa
        col_names = meta['colunas']
        real_columns = meta['colunas_reais']
        date_columns = meta['colunas_datas']
        data = []

        for i in list_data:
            data.append(i.split('|'))

        df = pd.DataFrame(data, columns=col_names)

        return self.clean(df, real_columns=real_columns, date_columns=date_columns)

    def create_dataframe_00000(self, meta=dicionario_00000):
        list_data = self.contribuite_apuracao_00000
        col_names = meta['colunas']
        real_columns = meta['colunas_reais']
        date_columns = meta['colunas_datas']
        data = []

        for i in list_data:
            data.append(i.split('|'))

        df = pd.DataFrame(data, columns=col_names)

        return self.clean(df, real_columns=real_columns, date_columns=date_columns)

    def create_dataframe_00001(self, meta=dicionario_00001):
        list_data = self.info_sobre_processo_nao_optante_00001
        col_names = meta['colunas'] + ['id_pgdas']
        real_columns = meta['colunas_reais']
        date_columns = meta['colunas_datas']
        data = []

        for i in list_data:
            data.append(i.split('|'))

        df = pd.DataFrame(data, columns=col_names)

        return self.clean(df, real_columns=real_columns, date_columns=date_columns)

    def create_dataframe_01000(self, meta=dicionario_01000):
        list_data = self.info_valores_calculados_das_01000
        col_names = meta['colunas'] + ['id_pgdas']
        real_columns = meta['colunas_reais']
        date_columns = meta['colunas_datas']
        data = []

        for i in list_data:
            data.append(i.split('|'))

        df = pd.DataFrame(data, columns=col_names)

        return self.clean(df, real_columns=real_columns, date_columns=date_columns)

    def create_dataframe_01100(self, meta=dicionario_01100):
        list_data = self.info_perfil_das_01100
        col_names = meta['colunas'] + ['id_pgdas', 'num_guia']
        real_columns = meta['colunas_reais']
        date_columns = meta['colunas_datas']
        data = []

        for i in list_data:
            data.append(i.split('|'))

        df = pd.DataFrame(data, columns=col_names)

        return self.clean(df, real_columns=real_columns, date_columns=date_columns)

    def create_dataframe_01500(self, meta=dicionario_01500):
        list_data = self.receita_bruta_anterior_opcao_01500
        col_names = meta['colunas'] + ['id_pgdas']
        real_columns = meta['colunas_reais']
        date_columns = meta['colunas_datas']
        data = []

        for i in list_data:
            data.append(i.split('|'))

        df = pd.DataFrame(data, columns=col_names)

        return self.clean(df, real_columns=real_columns, date_columns=date_columns)

    def create_dataframe_01501(self, meta=dicionario_01501):
        list_data = self.receita_bruta_anterior_opcao_mercado_interno_01501
        col_names = meta['colunas'] + ['id_pgdas']
        real_columns = meta['colunas_reais']
        date_columns = meta['colunas_datas']
        data = []

        for i in list_data:
            data.append(i.split('|'))

        df = pd.DataFrame(data, columns=col_names)

        return self.clean(df, real_columns=real_columns, date_columns=date_columns)

    def create_dataframe_01502(self, meta=dicionario_01502):
        list_data = self.receita_bruta_anterior_opcao_mercado_externo_01502
        col_names = meta['colunas'] + ['id_pgdas']
        real_columns = meta['colunas_reais']
        date_columns = meta['colunas_datas']
        data = []

        for i in list_data:
            data.append(i.split('|'))

        df = pd.DataFrame(data, columns=col_names)

        return self.clean(df, real_columns=real_columns, date_columns=date_columns)

    def create_dataframe_02000(self, meta=dicionario_02000):
        list_data = self.receita_bruta_anterior_opcao_valor_original_tributos_fixos_02000
        col_names = meta['colunas'] + ['id_pgdas']
        real_columns = meta['colunas_reais']
        date_columns = meta['colunas_datas']
        data = []

        for i in list_data:
            data.append(i.split('|'))

        df = pd.DataFrame(data, columns=col_names)

        return self.clean(df, real_columns=real_columns, date_columns=date_columns)

    def create_dataframe_03000(self, meta=dicionario_03000):
        list_data = self.info_estabelecimentos_03000
        col_names = meta['colunas'] + ['id_pgdas']
        real_columns = meta['colunas_reais']
        date_columns = meta['colunas_datas']
        data = []
        for i in list_data:
            data.append(i.split('|'))
        df = pd.DataFrame(data, columns=col_names)

        return self.clean(df, real_columns=real_columns, date_columns=date_columns)

    def create_dataframe_03100(self, meta=dicionario_03100):
        list_data = self.estabelecimentos_atividade_03100
        col_names = ['CNPJ', 'id_pgdas'] + meta['colunas']
        real_columns = meta['colunas_reais']
        date_columns = meta['colunas_datas']
        data = []
        for i in list_data:
            data.append(i.split('|'))
        df = pd.DataFrame(data, columns=col_names)

        return self.clean(df, real_columns=real_columns, date_columns=date_columns)

    def create_dataframe_03110(self, meta=dicionario_03110):
        list_data = self.rec_estab_atividade_faixa_a_03110
        col_names = ['CNPJ', 'id_pgdas', 'tipo_ativ'] + meta['colunas']
        real_columns = meta['colunas_reais']
        date_columns = meta['colunas_datas']
        data = []
        for i in list_data:
            data.append(i.split('|'))
        df = pd.DataFrame(data, columns=col_names)

        return self.clean(df, real_columns=real_columns, date_columns=date_columns)

    def create_dataframe_03120(self, meta=dicionario_03120):
        list_data = self.rec_estab_atividade_faixa_b_03120
        col_names = ['CNPJ', 'id_pgdas', 'tipo_ativ'] + meta['colunas']
        real_columns = meta['colunas_reais']
        date_columns = meta['colunas_datas']
        data = []
        for i in list_data:
            data.append(i.split('|'))
        df = pd.DataFrame(data, columns=col_names)

        return self.clean(df, real_columns=real_columns, date_columns=date_columns)

    def create_dataframe_03130(self, meta=dicionario_03130):
        list_data = self.rec_estab_atividade_faixa_c_03130
        col_names = ['CNPJ', 'id_pgdas', 'tipo_ativ'] + meta['colunas']
        real_columns = meta['colunas_reais']
        date_columns = meta['colunas_datas']
        data = []
        for i in list_data:
            data.append(i.split('|'))
        df = pd.DataFrame(data, columns=col_names)

        return self.clean(df, real_columns=real_columns, date_columns=date_columns)

    def create_dataframe_03111(self, meta=dicionario_03111):
        list_data = self.info_valor_rec_com_isencao_faixa_a_03111
        col_names = ['CNPJ', 'id_pgdas', 'tipo_ativ'] + meta['colunas']
        real_columns = meta['colunas_reais']
        date_columns = meta['colunas_datas']
        data = []
        for i in list_data:
            data.append(i.split('|')[:5])
        df = pd.DataFrame(data, columns=col_names)

        return self.clean(df, real_columns=real_columns, date_columns=date_columns)

    def create_dataframe_03112(self, meta=dicionario_03112):
        list_data = self.info_valor_receita_redeucao_percentual_faixa_a_03112
        col_names = ['CNPJ', 'id_pgdas', 'tipo_ativ'] + meta['colunas']
        real_columns = meta['colunas_reais']
        date_columns = meta['colunas_datas']
        data = []
        for i in list_data:
            data.append(i.split('|')[:6])
        df = pd.DataFrame(data, columns=col_names)

        return self.clean(df, real_columns=real_columns, date_columns=date_columns)

    def create_dataframe_03500(self, meta=dicionario_03500):
        list_data = self.folha_salarial_03500
        col_names = meta['colunas'] + ['id_pgdas']
        real_columns = meta['colunas_reais']
        date_columns = meta['colunas_datas']
        data = []
        for i in list_data:
            data.append(i.split('|')[:5])
        df = pd.DataFrame(data, columns=col_names)

        return self.clean(df, real_columns=real_columns, date_columns=date_columns)

    def create_dataframe_contribuintes(self):
        df = self.create_dataframe_00000()
        df = df[["Cnpjmatriz", "Nome", "Cod_TOM", "Optante", "Abertura"]]
        df["RAIZ_CNPJ"] = df["Cnpjmatriz"].apply(lambda x: x[:8])
        df.drop_duplicates(subset='RAIZ_CNPJ', keep='last', inplace=True)

        return df
    
    

