## ----- IMPORTANDO BIBLIOTECAS -----

from datetime import datetime, timedelta
import pandas as pd
import numpy as np

## ----- PROJEÇÕES -----

def processando_projecao(df_totalbus):

    print(f'SISTEMA: Iniciando processo de projeção das parcelas...')
    df_projetado = df_totalbus.loc[df_totalbus.index.repeat(df_totalbus['parcelas'])].reset_index(drop=True)
    df_projetado['PARCELA_ATUAL'] = df_projetado.groupby(['EMPRESA', 'DATA HORA VENDA', 'ID TRANSACAO ORIGINAL', 'STATUS BILHETE']).cumcount() + 1

    ## tratando colunas
    df_projetado['Metodo de pagamento'] = df_projetado['Metodo de pagamento'].fillna(df_projetado['FORMA PAGAMENTO 1'])
    df_projetado['Data de Lancamento'] = df_projetado['DATA HORA VENDA']
    df_projetado['Observacao'] = pd.NaT
    df_projetado['Seguro_Parcela'] = 0

    ## definindo como maiusculo

    df_projetado['FORMA PAGAMENTO 1'] = df_projetado['FORMA PAGAMENTO 1'].astype(str).str.upper()
    df_projetado['Metodo de pagamento'] = df_projetado['Metodo de pagamento'].astype(str).str.upper()
    df_projetado['STATUS BILHETE'] = df_projetado['STATUS BILHETE'].astype(str).str.upper()

    ## definindo condições, resultados e projeções das parcelas
    df_projetado['DATA HORA VENDA'] = pd.to_datetime(df_projetado['DATA HORA VENDA'], errors='coerce')
    df_projetado['DATA HORA VENDA PARA CANC.'] = pd.to_datetime(df_projetado['DATA HORA VENDA PARA CANC.'], errors='coerce')
    df_projetado['Data da Venda'] = pd.to_datetime(df_projetado['Data da Venda'], errors='coerce')

    condicoes_projecao_data = [

        ## se for pix cancelado no mês diferente ao da venda
        (df_projetado['STATUS BILHETE'] == 'C') &
        (df_projetado['Cancelamento_Mesmo_Mes'] == 0) &
        (df_projetado['Metodo de pagamento'] == 'PIX'),

        ## se for cartao cancelado no mês diferente ao da venda
        (df_projetado['STATUS BILHETE'] == 'C') &
        (df_projetado['Cancelamento_Mesmo_Mes'] == 0) &
        (
            (df_projetado['Metodo de pagamento'] == 'CRÉDITO') |
            (df_projetado['Me' \
            'todo de pagamento'] == 'CREDIT_CARD') |
            (df_projetado['Metodo de pagamento'] == 'VOUCHER')
        ),

        ## se for pix cancelado no mesmo mês da venda
        (df_projetado['STATUS BILHETE'] == 'C') &
        (df_projetado['Cancelamento_Mesmo_Mes'] == 1) &
        (df_projetado['Metodo de pagamento'] == 'PIX'),

        ## se for cartão cancelado no mesmo mês da venda
        (df_projetado['STATUS BILHETE'] == 'C') &
        (df_projetado['Cancelamento_Mesmo_Mes'] == 1) &
        (
            (df_projetado['Metodo de pagamento'] == 'CRÉDITO') |
            (df_projetado['Metodo de pagamento'] == 'CREDIT_CARD') |
            (df_projetado['Metodo de pagamento'] == 'VOUCHER')
        ),

        ## se for venda pix
        (df_projetado['STATUS BILHETE'] == 'V') &
        (df_projetado['Metodo de pagamento'] == 'PIX'),

        ## se for venda cartão
        (df_projetado['STATUS BILHETE'] == 'V') &
        (
            (df_projetado['Metodo de pagamento'] == 'CREDIT_CARD') |
            (df_projetado['Metodo de pagamento'] == 'CRÉDITO') |
            (df_projetado['Metodo de pagamento'] == 'VOUCHER')
        )
    ]

    resultado_projecao_data = [
        df_projetado['DATA HORA VENDA'] + timedelta(days=1),
        df_projetado['DATA HORA VENDA'] + (timedelta(days=30) * df_projetado['PARCELA_ATUAL']) + timedelta(days=1),
        df_projetado['Data da Venda'] + timedelta(days=1),
        df_projetado['Data da Venda'] + (timedelta(days=30) * df_projetado['PARCELA_ATUAL']) + timedelta(days=1),
        df_projetado['Data da Venda'] + timedelta(days=1),
        df_projetado['Data da Venda'] + (timedelta(days=30) * df_projetado['PARCELA_ATUAL']) + timedelta(days=1)
    ]

    df_projetado['DATA_PROJECAO'] = np.select(condicoes_projecao_data, resultado_projecao_data, pd.NaT)
    df_projetado['DATA_PROJECAO'] = pd.to_datetime(df_projetado['DATA_PROJECAO'], errors='coerce')

    ## ajustando data da projecao para dia de semana
    condicoes_semana = [
        df_projetado['DATA_PROJECAO'].dt.dayofweek == 5,
        df_projetado['DATA_PROJECAO'].dt.dayofweek == 6
    ]

    resultado_semana = [
        df_projetado['DATA_PROJECAO'] + timedelta(days=2),
        df_projetado['DATA_PROJECAO'] + timedelta(days=1)
    ]

    df_projetado['DATA_PROJECAO'] = np.select(condicoes_semana, resultado_semana, df_projetado['DATA_PROJECAO'])
    df_projetado['DATA_PROJECAO'] = pd.to_datetime(df_projetado['DATA_PROJECAO'], errors='coerce')


    ## definindo comissao
    condicoes_comissao = [
        df_projetado['AGENCIA ORIGINAL'] == '999-50',
        df_projetado['AGENCIA ORIGINAL'] == '999-51',
        df_projetado['AGENCIA ORIGINAL'] == '999-52'
    ]

    resultado_comissao_canal = [
        'Web',
        'App',
        'Whatsapp'
    ]

    resultado_comissao_percentual = [
        0.03,
        0.03,
        0.05
    ]

    df_projetado['CANAL_VENDA'] = np.select(condicoes_comissao, resultado_comissao_canal, pd.NaT)
    df_projetado['PERCENTUAL_COMISSAO'] = np.select(condicoes_comissao, resultado_comissao_percentual, 0)


    ## definindo valores
    df_projetado['TAXAS'] = df_projetado['PEDAGIO'] + df_projetado['TAXA_EMB']
    df_projetado['TAXA_CONV'] = df_projetado['TOTAL DO BILHETE'] * df_projetado['% Tx Conv']
    df_projetado['TOTAL_VENDA'] = df_projetado['TOTAL DO BILHETE'] + df_projetado['TAXA_CONV']
    df_projetado['COMISSAO'] = df_projetado['TOTAL_VENDA'] * df_projetado['PERCENTUAL_COMISSAO']
    df_projetado['TOTAL_REPASSE'] = df_projetado['TOTAL_VENDA'] - df_projetado['COMISSAO']
    df_projetado['TARIFA_PARCELA'] = df_projetado['TARIFA'] / df_projetado['parcelas']
    df_projetado['PEDAGIO_PARCELA'] = df_projetado['PEDAGIO'] / df_projetado['parcelas']
    df_projetado['TAXA_EMB_PARCELA'] = df_projetado['TAXA_EMB'] / df_projetado['parcelas']
    df_projetado['TAXA_CONV_PARCELA'] = df_projetado['TAXA_CONV'] / df_projetado['parcelas']
    df_projetado['TAXAS_PARCELA'] = df_projetado['TAXAS'] / df_projetado['parcelas']
    df_projetado['COMISSAO_PARCELA'] = df_projetado['COMISSAO'] / df_projetado['parcelas']
    df_projetado['TOTAL_BILHETE_PARCELA'] = df_projetado['TOTAL DO BILHETE'] / df_projetado['parcelas']
    df_projetado['TOTAL_VENDA_PARCELA'] = df_projetado['TOTAL_VENDA'] / df_projetado['parcelas']
    df_projetado['TOTAL_REPASSE_PARCELA'] = df_projetado['TOTAL_REPASSE'] / df_projetado['parcelas']

    ## considerando valor da multa nos repasses dos cancelados
    condicao_multa = [
        (df_projetado['STATUS BILHETE'] == 'C') &
        (df_projetado['PARCELA_ATUAL'] == 1)
    ]

    resultado_multa = [
        df_projetado['TOTAL_REPASSE_PARCELA'] + df_projetado['VALOR MULTA']
    ]

    df_projetado['TOTAL_REPASSE_PARCELA'] = np.select(condicao_multa, resultado_multa, df_projetado['TOTAL_REPASSE_PARCELA'])

    ## definindo tipos das colunas
    tipo_colunas = {
        'Origem': str,
        'EMPRESA': int,
        'NUMERO BILHETE': int,
        'STATUS BILHETE': str,
        'TARIFA': float,
        'PEDAGIO': float,
        'TAXA_EMB': float,
        'TAXAS': float,
        'TOTAL DO BILHETE': float,
        'FORMA PAGAMENTO 1': str,
        'AGENCIA ORIGINAL': str,
        'ID TRANSACAO ORIGINAL': str,
        'NOME PASSAGEIRO': str,
        'NOME_EMPRESA': str,
        '% Tx Conv': float,
        'parcelas': int,
        'PARCELA_ATUAL': int,
        'TAXA_CONV': float,
        'TOTAL_VENDA': float,
        'COMISSAO': float,
        'TOTAL_REPASSE': float,
        'TARIFA_PARCELA': float,
        'PEDAGIO_PARCELA': float,
        'TAXA_EMB_PARCELA': float,
        'TAXAS_PARCELA': float,
        'TAXA_CONV_PARCELA': float,
        'COMISSAO_PARCELA': float,
        'TOTAL_BILHETE_PARCELA': float,
        'TOTAL_VENDA_PARCELA': float,
        'TOTAL_REPASSE_PARCELA': float
    }

    df_projetado = df_projetado.astype(tipo_colunas)

    ## excluindo colunas
    df_projetado.drop(columns=['Cancelamento_Mesmo_Mes'], inplace=True)

    return df_projetado
