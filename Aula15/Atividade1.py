import polars as pl 
import os
import numpy as np

from datetime import datetime
ENDERECO_DADOS = r'./dados/'


try:
    print('Obtendo dados...')

    hora_import = datetime.now()

    df_fevereiro = pl.read_csv(ENDERECO_DADOS + '202402_NovoBolsaFamilia.csv', separator=';', encoding='iso-8859-1')

    print(df_fevereiro.head())

    hora_impressao = datetime.now()

    print(f'Tempo de Execução: {hora_impressao - hora_import}')

except ImportError as e:
    print('Erro ao obter dados: ', e)