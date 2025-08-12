## ----- IMPORTANDO BIBLIOTECAS -----
import pandas as pd
import os
from datetime import datetime
import numpy as np
from funcoes import ler_arquivo

## ----- PROCESSAMENTO DAS VENDAS -----
def processamento_totalbus(caminho_totalbus):

    '''
    Processa arquivos de vendas e cancelados do TOTALBUS de um diretório, concatenando-os
    em um único DataFrame e tratando os tipos de dados.

    Parâmetros:
    caminho_totalbus: O caminho do diretório contendo os arquivos
                                  de vendas da Embarca (CSV ou Excel).
    
    Retorna:
    pd.DataFrame: Um DataFrame consolidado com os dados processados.
    '''

    ## definindo colunas
    colunas = ['EMPRESA', 'NUMERO BILHETE', 'DATA HORA VENDA', 'STATUS BILHETE', 'TARIFA', 'PEDAGIO', 'TAXA_EMB',
                        'TOTAL DO BILHETE', 'FORMA PAGAMENTO 1', 'AGENCIA ORIGINAL', 'ID TRANSACAO ORIGINAL', 'NOME PASSAGEIRO',
                        'VALOR MULTA', 'DATA HORA VIAGEM', 'DATA HORA VENDA PARA CANC.']

    ## criando uma lista vazia para armazenamento
    lista_totalbus = []

    caminhos_arquivos = [
        os.path.join(caminho_totalbus, f) for f in os.listdir(caminho_totalbus) if f.endswith(('.csv', '.xls', '.xlsx'))
    ]

    ## realizando o looping dentro do diretório
    for caminho in caminhos_arquivos:
        try:
            df_temp = ler_arquivo(caminho, colunas)
            if df_temp is not None:
                lista_totalbus.append(df_temp)
        except Exception as e:
            print(f'SISTEMA: Erro ao processar o arquivo {os.path.basename(caminho)}. ({e})')

    ## agrupando todos os arquivos em um único dataframe
    try:
        if lista_totalbus:

            ## concatenando/agrupando todos os arquivos presente na lista
            df_totalbus = pd.concat(lista_totalbus, ignore_index=True)

        else:
            print('AVISO: Nenhum arquivo foi encontrato na pasta de vendas do Totalbus.')

    except Exception as e:
        print(f'AVISO: Erro ao agrupar os arquivos de vendas do Totalbus. ({e})')
    
    ## definição do nome de cada empresa baseado em seu código
    definicao_empresas = {
        1: 'Viação Garcia',
        3: 'Princesa do Ivaí',
        6: 'Brasil Sul',
        17: 'Santo Anjo'
    }

    df_totalbus['NOME_EMPRESA'] = df_totalbus['EMPRESA'].map(definicao_empresas)

    ## definindo tipos de datas
    colunas_tipo_data = ['DATA HORA VENDA', 'DATA HORA VENDA PARA CANC.']

    for c in colunas_tipo_data:
        df_totalbus[c] = pd.to_datetime(df_totalbus[c], errors='coerce', dayfirst=True).dt.floor('D')

    ## definindo tipos de valores
    colunas_tipo_valor = ['TARIFA', 'PEDAGIO', 'TAXA_EMB', 'VALOR MULTA']

    for c in colunas_tipo_valor:
        df_totalbus[c] = df_totalbus[c].astype(str).str.replace(',', '.', regex=False).astype(float)

    ## definindo se o cancelamento foi efetuado no mês da venda
    df_totalbus['Cancelamento_Mesmo_Mes'] = (
        df_totalbus['DATA HORA VENDA'].dt.to_period('M') ==
        df_totalbus['DATA HORA VENDA PARA CANC.'].dt.to_period('M')
    ).astype(int)

    ## definindo valores negativos nos cancelamentos
    condicional_negativar = df_totalbus['STATUS BILHETE'] == 'C'

    colunas_negativar = ['TARIFA', 'PEDAGIO', 'TAXA_EMB', 'TOTAL DO BILHETE']

    for c in colunas_negativar:
        df_totalbus.loc[condicional_negativar, c] = -df_totalbus.loc[condicional_negativar, c]

    return df_totalbus