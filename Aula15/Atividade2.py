import polars as pl
import os
import gc
from datetime import datetime

FONTE_DADOS = r'./Dados/'

try:
    hora_import = datetime.now()
    print('Obtendo dados...')

    lst_arquivos = []

    lst_dir_arquivos = os.listdir(FONTE_DADOS)

    for arquivo in lst_dir_arquivos:
        if arquivo.endswith('.csv'):
            lst_arquivos.append(arquivo)

    for arquivo in lst_arquivos:
        print(f'Carregando arquivo {arquivo}...')

        df = pl.read_csv(FONTE_DADOS + arquivo, separator=';', encoding='iso-8859-1')

        if 'df_bolsa_familia' in locals():
            df_bolsa_familia = pl.concat([df_bolsa_familia, df])
        else:
            df_bolsa_familia = df

        df_bolsa_familia = df_bolsa_familia.with_columns(pl.col('VALOR PARCELA').str.replace(',', '.').cast(pl.Float64))

        print(df_bolsa_familia)

        df_bolsa_familia.write_parquet(FONTE_DADOS + 'bolsa_familia_abr_mai.parquet')

        del df_bolsa_familia

        gc.collect()

    hora_impressao = datetime.now()

    print(f'Tempo de processamento: {hora_impressao - hora_import}')

except ImportError as e:
    print('Erro ao obter dados: ', e)        
