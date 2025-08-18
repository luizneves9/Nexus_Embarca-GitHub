## ----- IMPORTANDO BIBLIOTECAS -----

import pandas as pd
from datetime import datetime
from totalbus import processamento_totalbus
from embarca_vendas import processamento_embarca_vendas
from embarca_repasse import processamento_repasses
from funcoes import agrupamento_merge, agrupamento_concat
from projecao import processando_projecao
import os

## ----- DEFININDO DIRETÓRIOS -----

caminho_base = os.path.dirname(os.path.abspath(__file__))

caminho_banco_itau_vgl = os.path.join('Banco/Banco Itaú/Viação Garcia')
caminho_banco_itau_ep = os.path.join('Banco/Banco Itaú/Princesa do Ivaí')
caminho_banco_itau_bs = os.path.join(caminho_base, 'Banco/Banco Itaú/Brasil Sul')
caminho_banco_itau_sa = os.path.join(caminho_base, 'Banco/Banco Itaú/Santo Anjo')
caminho_embarca_repasse = os.path.join(caminho_base, 'Embarca_Repasse')
caminho_embarca_vendas = os.path.join(caminho_base, 'Embarca_Vendas')
caminho_totalbus = os.path.join(caminho_base, 'Totalbus')
caminho_tx_conveniencia = os.path.join(caminho_base, 'Tabela Tx Conv.xlsx')
caminho_relatorio_final_compra = os.path.join(caminho_base, 'Relatorio Final/Data de Lancamento')
caminho_relatorio_final_projecao = os.path.join('Relatorio Final/Data de Projecao')
caminho_relatorio_final_recebimento = os.path.join('Relatorio Final/Data de Recebimento')
caminho_relatorio_final_resumo = os.path.join(caminho_base, 'Relatorio Final/Resumo de Valores')

## ----- CARREGANDO TX DE CONVENIÊNCIA -----

try:
    df_taxa_conveniencia = pd.read_excel(caminho_tx_conveniencia)
except Exception as e:
    print(f'AVISO: Erro ao carregar a planilha de taxa de conveniência... ({e})')

## ----- PROCESSAMENTO DAS VENDAS E CANCELAMENTO -----

df_totalbus = processamento_totalbus(caminho_totalbus)
df_embarca_vendas = processamento_embarca_vendas(caminho_embarca_vendas)

## ----- AGRUPANDO TABELAS DO TOTALBUS COM A TAXA DE CONVENIÊNCIA -----

df_totalbus = agrupamento_merge(
    df_totalbus,
    df_taxa_conveniencia,
    'DATA HORA VENDA',
    'Data',
    'left',
    'Data'
)

df_totalbus['% Tx Conv'] = df_totalbus['% Tx Conv'] / 100
df_totalbus['VALOR MULTA'] = df_totalbus['VALOR MULTA'].fillna(0)

## ----- TRATANDO DADOS -----

## tratando 'DATA HORA VENDA PARA CANC.' das vendas

temp_series = df_totalbus.apply(
    lambda row: row['DATA HORA VENDA'] if row['STATUS BILHETE'] == 'V' else pd.NaT,
    axis=1
)

df_totalbus['DATA HORA VENDA PARA CANC.'] = df_totalbus['DATA HORA VENDA PARA CANC.'].fillna(temp_series)

## ----- AGRUPANDO TABELAS DO TOTALBUS COM EMBARCA VENDAS -----

df_totalbus['DATA HORA VENDA PARA CANC.'] = pd.to_datetime(df_totalbus['DATA HORA VENDA PARA CANC.'], errors='coerce')
df_totalbus['ID TRANSACAO ORIGINAL'] = df_totalbus['ID TRANSACAO ORIGINAL'].astype(str)
df_embarca_vendas['Data da Compra'] = pd.to_datetime(df_embarca_vendas['Data da Compra'], errors='coerce')
df_embarca_vendas['ID do Bilhete'] = df_embarca_vendas['ID do Bilhete'].astype(str)

df_totalbus['DATA HORA VENDA PARA CANC.'] = pd.to_datetime(df_totalbus['DATA HORA VENDA PARA CANC.'])
df_embarca_vendas['Data da Compra'] = pd.to_datetime(df_embarca_vendas['Data da Compra'])

df_totalbus = df_totalbus.sort_values(by='DATA HORA VENDA PARA CANC.')
df_embarca_vendas = df_embarca_vendas.sort_values(by='Data da Compra')

tolerancia = pd.Timedelta('1 day')

df_embarca_vendas.rename(columns={'Data da Compra': 'Data da Venda'}, inplace=True)

df_totalbus = pd.merge_asof(
    df_totalbus,
    df_embarca_vendas,
    left_on='DATA HORA VENDA PARA CANC.',
    right_on='Data da Venda',
    left_by=['NOME_EMPRESA', 'ID TRANSACAO ORIGINAL'],
    right_by=['Operadora', 'ID do Bilhete'],
    direction='nearest',
    tolerance=tolerancia
)

df_totalbus.drop(columns=['Operadora', 'ID do Bilhete', 'Status'], inplace=True)
df_totalbus['Data da Venda'] = df_totalbus['Data da Venda'].fillna(df_totalbus['DATA HORA VENDA PARA CANC.'])

## ----- TRATANDO PARCELAS NÃO LOCALIZADAS -----

df_totalbus['parcelas'] = df_totalbus['parcelas'].fillna(1)

## ----- PROJETANDO AS PARCELAS -----

df_projecao = processando_projecao(df_totalbus)

## ----- CARREGANDO REPASSES DA EMBARCA -----

df_embarca = processamento_repasses(caminho_embarca_repasse, df_embarca_vendas, df_totalbus)

## ----- RENOMEANDO E AGRUPANDO TOTALBUS E EMBARCA -----

df_projecao_renomear = {
    'NUMERO BILHETE': 'Bilhete',
    'STATUS BILHETE': 'Status',
    'ID TRANSACAO ORIGINAL': 'ID Transacao',
    'NOME PASSAGEIRO': 'Nome do Passageiro',
    'VALOR MULTA': 'Multa',
    'DATA HORA VENDA PARA CANC.': 'Data da Compra',
    'NOME_EMPRESA': 'Nome da Empresa',
    '% Tx Conv': 'Taxa de Conv. (%)',
    'Metodo de pagamento': 'Metodo de Pagamento',
    'parcelas': 'Parcelas',
    'PARCELA_ATUAL': 'Parcela Atual',
    'Data de Lancamento': 'Data de Lancamento',
    'Observacao': 'Observacao',
    'DATA_PROJECAO': 'Data Projecao',
    'CANAL_VENDA': 'Canal',
    'PERCENTUAL_COMISSAO': 'Percentual de Comissao',
    'TARIFA_PARCELA': 'Tarifa_Parcela',
    'TAXA_CONV_PARCELA': 'Taxa de Conv._Parcela',
    'TAXAS_PARCELA': 'Taxas_Parcela',
    'COMISSAO_PARCELA': 'Comissao_Parcela',
    'TOTAL_BILHETE_PARCELA': 'Total do Bilhete_Parcela',
    'TOTAL_VENDA_PARCELA': 'Total da Venda_Parcela',
    'Seguro_Parcela': 'Seguro_Parcela',
     'TOTAL_REPASSE_PARCELA': 'Total do Repasse_Parcela'
}

df_embarca_renomear = {
    'Operadora': 'Nome da Empresa',
    'ID do Bilhete': 'ID Transacao',
    'Nº do Sistema': 'Bilhete',
    'Forma de pagamento': 'Metodo de Pagamento',
    'Canal': 'Canal',
    'Nome do passageiro': 'Nome do Passageiro',
    'Status': 'Status',
    'Data da Compra': 'Data da Compra',
    'Tarifa': 'Tarifa_Parcela',
    'Taxas': 'Taxas_Parcela',
    'Valor Total': 'Total do Bilhete_Parcela',
    'Parcelas': 'Parcelas',
    'Taxa de conveniência': 'Taxa de Conv._Parcela',
    'Comissão': 'Comissao_Parcela',
    'Multa': 'Multa',
    'Repasse Seguro Parcela': 'Seguro_Parcela',
    'Obs': 'Observacao',
    'Parcela_Atual': 'Parcela Atual',
    'Data de Lancamento': 'Data de Lancamento',
    'Taxa de Conv. (%)': 'Taxa de Conv. (%)',
    'Percentual de Comissao': 'Percentual de Comissao',
    'Total da Venda': 'Total da Venda_Parcela',
    'Repasse_liquido_inv': 'Total do Repasse_Parcela',
    'Repasse_liquido_com_seguro_inv': 'Total do Repasse com Seguro_Parcela',
    'Data_Projecao': 'Data Projecao'
}

df_projecao.rename(columns=df_projecao_renomear, inplace=True)
df_embarca.rename(columns=df_embarca_renomear, inplace=True)

df_projecao['Base'] = 'Totalbus'
df_projecao['Metodo de Pagamento_V'] = df_projecao['Metodo de Pagamento']
df_projecao['Sequencial'] = df_projecao['Parcela Atual']
df_projecao['Parcela Referente'] = df_projecao['Parcela Atual']
df_projecao['Venda Localizada'] = ''
df_projecao['Data de Recebimento'] = ''
df_projecao['Data BPE'] = df_projecao['Data da Compra']
df_projecao['Total do Repasse com Seguro_Parcela'] = df_projecao['Total do Repasse_Parcela']
df_projecao['Data BPE'] = df_projecao['Data BPE'].fillna(df_projecao['Data da Compra'])
df_projecao['Tipo'] = 'Automatico'

df_embarca['Base'] = 'Embarca'
df_embarca['Data da Venda'] = df_embarca['Data da Compra']
df_embarca['Data BPE'] = df_embarca['Data BPE'].fillna(df_embarca['Data da Compra'])

df_agrupado = pd.concat(
    [
        df_projecao[['Origem', 'Base', 'Tipo', 'Nome da Empresa', 'Bilhete', 'ID Transacao', 'Status', 'Data da Compra', 'Data da Venda', 'Data BPE', 'Data de Lancamento', 'Nome do Passageiro',
                     'Metodo de Pagamento_V', 'Parcelas', 'Parcela Atual', 'Parcela Referente', 'Sequencial', 'Data Projecao', 'Taxa de Conv. (%)', 'Canal', 'Percentual de Comissao',
                     'Tarifa_Parcela', 'Taxas_Parcela', 'Total do Bilhete_Parcela', 'Taxa de Conv._Parcela', 'Total da Venda_Parcela',
                     'Comissao_Parcela', 'Multa', 'Total do Repasse_Parcela', 'Seguro_Parcela', 'Total do Repasse com Seguro_Parcela', 'Observacao', 'Venda Localizada', 'Data de Recebimento']],
        df_embarca[['Origem', 'Base', 'Tipo', 'Nome da Empresa', 'Bilhete', 'ID Transacao', 'Status', 'Data da Compra', 'Data da Venda', 'Data BPE', 'Data de Lancamento', 'Nome do Passageiro',
                     'Metodo de Pagamento_V', 'Parcelas', 'Parcela Atual', 'Parcela Referente', 'Sequencial', 'Data Projecao', 'Taxa de Conv. (%)', 'Canal', 'Percentual de Comissao',
                     'Tarifa_Parcela', 'Taxas_Parcela', 'Total do Bilhete_Parcela', 'Taxa de Conv._Parcela', 'Total da Venda_Parcela',
                     'Comissao_Parcela', 'Multa', 'Total do Repasse_Parcela', 'Seguro_Parcela', 'Total do Repasse com Seguro_Parcela', 'Observacao', 'Venda Localizada', 'Data de Recebimento']]
    ], ignore_index=True
)

saldo_por_id_data = df_agrupado.groupby(['Nome da Empresa', 'ID Transacao', 'Data Projecao'])['Total do Repasse_Parcela'].sum().reset_index()
saldo_por_id_data.rename(columns={'Total do Repasse_Parcela': 'Saldo'}, inplace=True)
saldo_total = df_agrupado.groupby(['Nome da Empresa', 'ID Transacao'])['Total do Repasse_Parcela'].sum().reset_index()
saldo_total.rename(columns={'Total do Repasse_Parcela': 'Saldo_Total'}, inplace=True)
df_agrupado = pd.merge(df_agrupado, saldo_por_id_data, how='left', on=['Nome da Empresa', 'ID Transacao', 'Data Projecao'])
df_agrupado = pd.merge(df_agrupado, saldo_total, how='left', on=['Nome da Empresa', 'ID Transacao'])

## ----- SALVAMENTO DO DF_AGRUPADO DE FORMA FRACIONADA -----

## definindo o nome geral dos arquivos
nome_base_arquivo_venda = 'conciliacao_geral-v'
nome_base_arquivo_proj = 'conciliacao_geral-p'
nome_base_arquivo_recebimento = 'conciliacao_geral-r'
nome_base_arquivo_resumo = 'resumo_projecao'

## agrupando os DataFrames pela DATA PROJECAO e DATA DA COMPRA
grupo_por_mes_projecao = df_agrupado.groupby(df_agrupado['Data Projecao'].dt.to_period('M'))
grupo_por_mes_venda = df_agrupado.groupby(df_agrupado['Data de Lancamento'].dt.to_period('M'))
grupo_por_mes_recebimento = df_agrupado.groupby(pd.to_datetime(df_agrupado['Data de Recebimento']).dt.to_period('M'))

## salvando pela data projecao
for periodo, df_grupo in grupo_por_mes_projecao:
    ano_mes_str = periodo.strftime('%Y_%m')

    nome_arquivo_completo = f'{nome_base_arquivo_proj}_{ano_mes_str}.csv'

    caminho_arquivo_completo = os.path.join(caminho_relatorio_final_projecao, nome_arquivo_completo)

    try:
        df_grupo.to_csv(caminho_arquivo_completo, sep=';', decimal=',', index=False)
        print(f'SISTEMA: Salvo {len(df_grupo)} registros para {nome_arquivo_completo}')
    except Exception as e:
        print(f'AVISO: Erro ao salvar o arquivo {nome_arquivo_completo}: {e}')

## salvando pela data do lançamento
for periodo, df_grupo in grupo_por_mes_venda:
    ano_mes_str = periodo.strftime('%Y_%m')

    nome_arquivo_completo = f'{nome_base_arquivo_venda}_{ano_mes_str}.csv'

    caminho_arquivo_completo = os.path.join(caminho_relatorio_final_compra, nome_arquivo_completo)
    caminho_resumo_completo = os.path.join(caminho_relatorio_final_resumo, nome_base_arquivo_resumo)

    ## salvando relatórios
    try:
        df_grupo.to_csv(caminho_arquivo_completo, sep=';', decimal=',', index=False)
        print(f'SISTEMA: Salvo {len(df_grupo)} registros para {nome_arquivo_completo}')
    except Exception as e:
        print(f'AVISO: Erro ao salvar o arquivo {nome_arquivo_completo}: {e}')

print(f'SISTEMA: Encerrando sistema Nexus!')

