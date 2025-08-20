## ----- IMPORTANDO BIBLIOTECAS -----

import pandas as pd
import os
from datetime import datetime
from funcoes import ler_arquivo

## ----- LOCALIZANDO INCONSISTÊNCIAS NO RELATÓRIO DE VENDAS DA EMBARCA -----

def apontamento_inconsistencias(df_embarca_vendas):

    df_diferencas = df_embarca_vendas[
        (df_embarca_vendas['Operadora'].isna()) |
        (df_embarca_vendas['ID do Bilhete'].isna()) |
        (df_embarca_vendas['Metodo de pagamento'].isna()) |
        (df_embarca_vendas['parcelas'].isna()) |
        (df_embarca_vendas['Data da Compra'].isna())
    ].copy()

    return df_diferencas

## ----- PROCESSAMENTO DAS VENDAS -----

def processamento_embarca_vendas(caminho_embarca_vendas):

    '''
    Processa arquivos de vendas da EMBARCA de um diretório, concatenando-os
    em um único DataFrame, tratando os tipos de dados e adicionando uma
    coluna de status.

    Parâmetros:
    caminho_embarca_vendas (str): O caminho do diretório contendo os arquivos
                                  de vendas da Embarca (CSV ou Excel).
    
    Retorna:
    pd.DataFrame: Um DataFrame consolidado com os dados processados.
    '''

    ## definindo colunas
    colunas_embarca = ['Operadora', 'ID do Bilhete', 'Metodo de pagamento', 'parcelas', 'Data da Compra']

    ## criando lista vazia
    lista_embarca_vendas = []

    ## carregando todos os diretórios 
    caminhos_arquivo = [
        os.path.join(caminho_embarca_vendas, f) for f in os.listdir(caminho_embarca_vendas) if f.endswith(('.csv', '.xlsx', '.xls'))
    ]

    ## looping dos arquivos
    for caminho in caminhos_arquivo:
        try:
            df_temp = ler_arquivo(caminho, colunas_embarca, 'Base_Aprovados')
            if df_temp is not None:
                lista_embarca_vendas.append(df_temp)
        except Exception as e:
            print(f'SISTEMA: Erro ao processar o arquivo {os.path.basename(caminho)}. ({e})')
    
    ## concatenando/agrupando todos os dataframes processados
    try:
        if lista_embarca_vendas:
            df_embarca_vendas = pd.concat(lista_embarca_vendas, ignore_index=True)
            df_embarca_vendas['Status'] = 'V'
        else:
            print('Aviso: Nenhum arquivo foi encontrado na pasta de vendas da Embarca.')
    except Exception as e:
        print(f'Aviso: erro ao consolidar os arquivos de vendas da Embarca ({e})')

    ## definindo datas
    df_embarca_vendas['Data da Compra'] = pd.to_datetime(df_embarca_vendas['Data da Compra'], errors='coerce').dt.floor('D')

    ## tratando colunas
    df_embarca_vendas['ID do Bilhete'] = df_embarca_vendas['ID do Bilhete'].astype(str).str.split('.').str[0].replace('nan', None)

    ## identificando informações faltantes
    diferencas = apontamento_inconsistencias(df_embarca_vendas)

    del df_embarca_vendas['Origem']

    return df_embarca_vendas, diferencas