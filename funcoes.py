## ----- IMPORTANDO BIBLIOTECAS -----

import pandas as pd
import os
from datetime import datetime

## ----- FUNÇÃO PARA LER ARQUIVOS -----

def ler_arquivo(caminho, colunas, sheet=None):

    '''
    Função para ler/carregar um arquivo em CSV, XLS ou XLSX.

    Parâmetros:
    caminho: O caminho do diretório contendo os arquivos
                        de vendas da Embarca (CSV ou Excel).
    colunas: definição das colunas que serão consideradas.
    sheet: definição de qual aba importar. Caso seja None, processará todas as abas presentes no dataframe.
    
    Retorna:
    pd.DataFrame: Um DataFrame consolidado com os dados processados.
    '''

    nome_arquivo = os.path.basename(caminho)
    print(f'SISTEMA: Processando o arquivo "{nome_arquivo}".')

    if nome_arquivo.endswith('.csv'):
        df = pd.read_csv(caminho, usecols=colunas, encoding='latin-1', sep=';')

    elif nome_arquivo.endswith(('.xlsx', '.xls')):
        if sheet is not None:
            df = pd.read_excel(caminho, usecols=colunas, sheet_name=sheet)
        else:
            df = pd.read_excel(caminho, usecols=colunas)

    else:
        return None

    df['Origem'] = nome_arquivo
    return df

## ----- FUNÇÕES DE AGRUPAMENTO -----

def agrupamento_merge(df1, df2, left_on, right_on, how, drop=False):
    
    print('SISTEMA: Agrupando planilhas...')
    df_agrupado = pd.DataFrame()

    try:
        df_agrupado = pd.merge(
            df1,
            df2,
            left_on=left_on,
            right_on=right_on,
            how=how
        )
        if drop:
            df_agrupado.drop(columns=drop, inplace=True)

    except Exception as e:
        print(f'AVISO: Erro ao agrupar planilhas... ({e})')
    
    return df_agrupado

def agrupamento_concat(df):
    
    print('SISTEMA: Agrupando planilhas...')
    df_agrupado = pd.DataFrame()

    try:
        df_agrupados = pd.concat(df, ignore_index=True)
    except Exception as e:
        print(f'AVISO: Erro ao agrupar planilhas... ({e})')

    return df_agrupados

## ----- FUNÇÃO PARA SALVAR PLANILHA -----

def salvar(df, diretorio):

    print(f'SISTEMA: Salvando planilha...')
    try:
        df.to_csv(diretorio, sep=';', decimal=',', index=False)
    except Exception as e:
        print(f'AVISO: Erro ao salvar a planilha no diretório! ({e})')

