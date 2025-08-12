## ----- IMPORTANDO BIBLIOTECAS -----

import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
from funcoes import agrupamento_merge

## ----- PROCESSAMENTO DOS REPASSES DA EMBARCA -----

# -----

# criando uma função para ler os arquivos de repasses de forma mais prática

def ler_arquivo_repasse(caminho_repasse, colunas_repasse):
    '''
    Função para LER/CARREGAR os arquivos de repasses, tratando suas colunas e agrupando
    para um único data frame.

    Parametros:
        caminho_repasse: diretório do arquivo.
        colunas_repasse: definição das colunas que serão consideradas nos dataframes.

    Retorna:
        pd.DataFrame: Um DataFrame consolidado com os dados processados.
    '''

    ## definindo o nome do arquivo
    nome_arquivo = os.path.basename(caminho_repasse)
    print(f'SISTEMA: Processando o arquivo "{nome_arquivo}"')

    ## carregando o arquivo
    try:
        df_geral = pd.read_excel(caminho_repasse, sheet_name=['Base_Aprov', 'Base_Canc'], usecols=colunas_repasse)
        df_aprov, df_canc = df_geral['Base_Aprov'], df_geral['Base_Canc']
    except Exception as e:
        print(f'SISTEMA: Erro ao processar o arquivo "{nome_arquivo}". ({e})')

    ## definindo as datas de cada dataframe
    for df in [df_aprov, df_canc]:
        if 'Data da Compra' in df.columns:
            df['Data da Compra'] = pd.to_datetime(df['Data da Compra'], errors='coerce').dt.tz_localize(None).dt.normalize()
        if 'Data do Cancelamento' in df.columns:
            df['Data do Cancelamento'] = pd.to_datetime(df['Data do Cancelamento'], errors='coerce').dt.tz_localize(None).dt.normalize()           

    ## ajustando o valor negativo dos cancelados
    colunas_valor_cancelado = ['Tarifa', 'Taxas', 'Valor Total', 'Taxa de conveniência','Repasse', 'Seguro', 'Repasse Seguro', 'Repasse Seguro Parcela']
    for col in colunas_valor_cancelado:
        if col in df_canc.columns:
            df_canc[col] = -df_canc[col]

    ## definindo a coluna 'origem' em cada arquivo
    df_aprov['Origem'] = nome_arquivo
    df_canc['Origem'] = nome_arquivo

    return [df_aprov, df_canc]

# -----

def processamento_repasses(diretorio_embarca_repasse, df_embarca_vendas, df_totalbus):

    ## carregando planilhas
    try:
        lista_temp = []
        colunas_embarca = ['Operadora', 'order_id', 'ID do Bilhete', 'Nº do Sistema', 'Forma de pagamento', 'id_adiquirente', 'Canal', 'Nome do passageiro', 'Status', 'Data da Compra', 'Data do Cancelamento', 'Tarifa', 'Taxas', 'Valor Total', 'Parcelas', 'Taxa de conveniência', 'Valor do Cupom (R$)', 'Promoção', 'Descontos vindos da API', 'Comissão', 'Repasse', 'Multa', 'Marketing Digital', 'parcelas pagas', 'URL do BPe', 'URL do Bilhete', 'Seguro', 'Repasse Seguro', 'Repasse Seguro Parcela', 'Obs']
        colunas_valor_cancelado = ['Tarifa', 'Taxas', 'Valor Total', 'Taxa de conveniência','Repasse', 'Seguro', 'Repasse Seguro', 'Repasse Seguro Parcela']

        for nome_arquivos in os.listdir(diretorio_embarca_repasse):

            if nome_arquivos.endswith('.xlsx') or nome_arquivos.endswith('.xls'):
                caminho_completo = os.path.join(diretorio_embarca_repasse, nome_arquivos)
                print(f'SISTEMA: Processando o arquivo "{nome_arquivos}".')

                try:
                    arquivo_temp = pd.read_excel(caminho_completo, sheet_name=['Base_Aprov', 'Base_Canc'], usecols=colunas_embarca)
                    arquivo_temp_v, arquivo_temp_c = arquivo_temp['Base_Aprov'], arquivo_temp['Base_Canc']
                    
                    for df in [arquivo_temp_v, arquivo_temp_c]:

                        if 'Data da Compra' in df.columns:
                            df['Data da Compra'] = pd.to_datetime(df['Data da Compra'], errors='coerce')
                            
                            if df['Data da Compra'].dt.tz is not None:
                                df['Data da Compra'] = df['Data da Compra'].dt.tz_localize(None)

                            df['Data da Compra'] = df['Data da Compra'].dt.normalize()

                        if 'Data do Cancelamento' in df.columns:
                            df['Data do Cancelamento'] = pd.to_datetime(df['Data do Cancelamento'], errors='coerce')

                            if df['Data do Cancelamento'].dt.tz is not None:
                                df['Data do Cancelamento'] = df['Data do Cancelamento'].dt.tz_localize(None)

                            df['Data do Cancelamento'] = df['Data do Cancelamento'].dt.normalize()

                    arquivo_temp_v['Origem'] = nome_arquivos

                    lista_temp.append(arquivo_temp_v)

                    for colunas in colunas_valor_cancelado:
                        arquivo_temp_c[colunas] = -arquivo_temp_c[colunas] 

                    arquivo_temp_c['Origem'] = nome_arquivos
                    lista_temp.append(arquivo_temp_c)      

                except Exception as file_e:
                    print(f'AVISO: Erro ao processar o arquivo "{nome_arquivos}". Ignorando este arquivo. ({file_e})')

    except Exception as e:
        print(f'AVISO: Erro ao processar os arquivos de repasse da Embarca. ({e})')

    ## agrupando as planilhas
    try:
        if lista_temp:
            embarca = pd.concat(lista_temp, ignore_index=True)
        else:
            print('AVISO: Nenhuma planilha da Embarca foi processada com sucesso. Retornando DataFrame vazio.')
            return pd.DataFrame()

    except Exception as e: 
        print(f'AVISO: Erro ao agrupar os arquivos de repasse da Embarca. ({e})')
        return pd.DataFrame()

    if embarca.empty:
            print('AVISO: DataFrame embarca está vazio após a concatenação. Nenhuma operação subsequente será realizada.')
            return pd.DataFrame()


    ## renomeando colunas
    embarca.rename(columns={
        'Operadora': 'Nome da Empresa',
        'ID do Bilhete': 'ID Transacao'
    }, inplace=True)

    ## incluindo data de recebimento
    embarca['Data de Recebimento'] = embarca['Origem'].str[:4] + '-' + embarca['Origem'].str[4:6] + '-01'

    ## incluindo coluna Parcela_Atual e tratando dados
    embarca['Parcela_Atual'] = embarca['parcelas pagas'].astype(str).str.split('/').str[0]
    embarca['Nº do Sistema'] = embarca['Nº do Sistema'].astype(str).str.split('.').str[0]
    embarca['ID Transacao'] = embarca['ID Transacao'].astype(str).str.split('.').str[0]

    ## ajustando forma de pagamento conforme a venda

    df_embarca_vendas2 = df_embarca_vendas.copy()
    df_embarca_vendas2.drop(columns=['Status'], inplace=True)

    df_embarca_vendas2.rename(columns={
        'Operadora': 'Nome da Empresa',
        'ID do Bilhete': 'ID Transacao',
        'Metodo de pagamento': 'Metodo de Pagamento_V',
        'parcelas': 'Parcelas da Venda'
    }, inplace=True)

    embarca['Nome da Empresa'] = embarca['Nome da Empresa'].astype(str)

    df_embarca_vendas2['Data da Venda'] = pd.to_datetime(df_embarca_vendas2['Data da Venda'], errors='coerce')
    if df_embarca_vendas2['Data da Venda'].dt.tz is not None:
        df_embarca_vendas2['Data da Venda'] = df_embarca_vendas2['Data da Venda'].dt.tz_localize(None)
    df_embarca_vendas2['Data da Venda'] = df_embarca_vendas2['Data da Venda'].dt.normalize()

    df_embarca_vendas2['Nome da Empresa'] = df_embarca_vendas2['Nome da Empresa'].astype(str)

    embarca = agrupamento_merge(
        embarca,
        df_embarca_vendas2,
        ['Nome da Empresa', 'ID Transacao', 'Data da Compra'],
        ['Nome da Empresa', 'ID Transacao', 'Data da Venda'],
        'left'
    )
    embarca['Metodo de Pagamento_V'] = embarca['Metodo de Pagamento_V'].fillna(embarca['Forma de pagamento'])
    embarca['Venda Localizada'] = np.where(embarca['Parcelas da Venda'].isna(), 'NAO', 'SIM')
    embarca['Parcelas da Venda'] = embarca['Parcelas da Venda'].fillna(1)

    ## ajustando sequencial das transações (será utilizado posteriormente para cálcular a data de projeção correta)
    
    condicao_parcela_referente = [
        embarca['order_id'] == 'AJUSTE'
    ]

    resultado_parcela_referente = [
        (embarca['Parcela_Atual'])
    ]

    resultado_tipo = [
        'Manual'
    ]
    
    embarca['Tipo'] = np.select(condicao_parcela_referente, resultado_tipo, 'Automatico')
    embarca['Sequencial'] = embarca.groupby(['Nome da Empresa', 'ID Transacao', 'Data da Compra', 'Status', 'Tipo']).cumcount() + 1
    embarca['Parcela Referente'] = np.select(condicao_parcela_referente, resultado_parcela_referente, np.minimum(embarca['Sequencial'], embarca['Parcelas da Venda'])).astype(int)

    ## tratando colunas
    embarca['Nome do passageiro'] = embarca['Nome do passageiro'].str.replace('\n', '', regex=False).str.strip()
    embarca['Marketing Digital'] = embarca['Marketing Digital'].fillna(0)
    embarca['Forma de pagamento'] = embarca['Forma de pagamento'].astype(str).str.upper()
    embarca['Metodo de Pagamento_V'] = embarca['Metodo de Pagamento_V'].astype(str).str.upper()
    embarca['Status'] = embarca['Status'].astype(str).str.upper()
    embarca['Parcela_Atual'] = embarca['Parcela_Atual'].fillna(-1).astype(int)

    embarca['Taxa de Conv. (%)'] = embarca['Taxa de conveniência'] / embarca['Valor Total']
    embarca['Percentual de Comissao'] = embarca['Comissão'] / (embarca['Valor Total'] + embarca['Taxa de conveniência'])
    embarca['Total da Venda'] = embarca['Valor Total'] + embarca['Taxa de conveniência']

    ## incluindo a coluna Repasse_liquido

    condicional_repasse = [
        (embarca['Status'] == 'CANCELADO'),
        (embarca['Status'] == 'CANCELADO Q'),
        (embarca['Status'] == 'APROVADO')
    ]

    resultado_repasse = [
        embarca['Repasse'] + embarca['Multa'],
        embarca['Repasse'] + embarca['Multa'] + embarca['Comissão'],
        embarca['Repasse']
    ]

    resultado_repasse_com_seguro = [
        embarca['Repasse'] + embarca['Multa'] + embarca['Repasse Seguro Parcela'],
        embarca['Repasse'] + embarca['Multa'] + embarca['Comissão'],
        embarca['Repasse'] + embarca['Repasse Seguro Parcela']
    ]

    embarca['Repasse_liquido'] = np.select(condicional_repasse, resultado_repasse, 0)
    embarca['Repasse_liquido_inv'] = -embarca['Repasse_liquido']

    embarca['Repasse_liquido_com_seguro'] = np.select(condicional_repasse, resultado_repasse_com_seguro, 0)
    embarca['Repasse_liquido_com_seguro_inv'] = -embarca['Repasse_liquido_com_seguro']

    ## validação de cancelamento no mesmo mês da venda

    embarca['MesAno_Venda'] = embarca['Data da Compra'].dt.strftime('%Y-%m')
    embarca['MesAno_Cancelado'] = embarca['Data do Cancelamento'].dt.strftime('%Y-%m')

    condicional_mes_cancelamento = [embarca['MesAno_Venda'] == embarca['MesAno_Cancelado']]
    resultado_mes_cancelamento = [1]
    embarca['Cancelamento_Mesmo_Mes'] = np.select(condicional_mes_cancelamento, resultado_mes_cancelamento, 0)

    ## agrupando com planilha do totalbus para trazer a data BPE
    
    df_totalbus_conciliador = df_totalbus.copy()

    embarca['Data da Compra'] = pd.to_datetime(embarca['Data da Compra'], errors='coerce')
    embarca['ID Transacao'] = embarca['ID Transacao'].astype(str)
    df_totalbus_conciliador['DATA HORA VENDA PARA CANC.'] = pd.to_datetime(df_totalbus_conciliador['DATA HORA VENDA PARA CANC.'], errors='coerce')
    df_totalbus_conciliador['ID TRANSACAO ORIGINAL'] = df_totalbus_conciliador['ID TRANSACAO ORIGINAL'].astype(str)

    df_totalbus_conciliador = df_totalbus_conciliador[df_totalbus_conciliador['STATUS BILHETE'] == 'V']
    df_totalbus_conciliador = df_totalbus_conciliador[['NOME_EMPRESA', 'DATA HORA VENDA PARA CANC.', 'ID TRANSACAO ORIGINAL']]

    df_totalbus_conciliador = df_totalbus_conciliador.sort_values(by='DATA HORA VENDA PARA CANC.')
    embarca = embarca.sort_values(by='Data da Compra')

    embarca = pd.merge_asof(
        embarca,
        df_totalbus_conciliador,
        left_on= 'Data da Compra',
        right_on= 'DATA HORA VENDA PARA CANC.',
        left_by= ['Nome da Empresa', 'ID Transacao'],
        right_by= ['NOME_EMPRESA', 'ID TRANSACAO ORIGINAL'],
        direction= 'nearest',
        tolerance= pd.Timedelta('1 day')
    )

    embarca.rename(columns={'DATA HORA VENDA PARA CANC.': 'Data BPE'}, inplace=True)
    embarca['Data BPE'] = embarca['Data BPE'].fillna(embarca['Data da Compra'])

    ## tratando parcela_atual e projetando suas datas de pagamento

    condicional_projecao = [
    
        ## ----- REGISTROS ANTES DE 01/01/2025 -----
    
        ## vendas com forma de pagamento pix
        (embarca['Status'] == 'APROVADO') &
        (embarca['Metodo de Pagamento_V'] == 'PIX') &
        (embarca['Data BPE'] < '2025-01-01'),

        ## vendas com forma de pagamento credit_card
        (embarca['Status'] == 'APROVADO') &
        (embarca['Metodo de Pagamento_V'] == 'CREDIT_CARD') &
        (embarca['Data BPE'] < '2025-01-01'),

        ## cancelamentos com forma de pagamento pix realizados no mesmo mês da venda
        (
            (embarca['Status'] == 'CANCELADO') |
            (embarca['Status'] == 'CANCELADO Q')
        ) &
        (embarca['Metodo de Pagamento_V'] == 'PIX') &
        (embarca['Cancelamento_Mesmo_Mes'] == 1) &
        (embarca['Data do Cancelamento'] < '2025-01-01'),

        ## cancelamentos com forma de pagamento credit_card realizados no mesmo mês da venda
        (
            (embarca['Status'] == 'CANCELADO') |
            (embarca['Status'] == 'CANCELADO Q')
        ) &
        (embarca['Metodo de Pagamento_V'] == 'CREDIT_CARD') &
        (embarca['Cancelamento_Mesmo_Mes'] == 1) &
        (embarca['Data do Cancelamento'] < '2025-01-01'),

        ## cancelamentos com forma de pagamento pix realizados no mês diferente ao da venda
        (
            (embarca['Status'] == 'CANCELADO') |
            (embarca['Status'] == 'CANCELADO Q')
        ) &
        (embarca['Metodo de Pagamento_V'] == 'PIX') &
        (embarca['Cancelamento_Mesmo_Mes'] == 0) &
        (embarca['Data do Cancelamento'] < '2025-01-01'),

        ## cancelamentos com forma de pagamento credit_card realizados no mês diferente ao da venda
        (
            (embarca['Status'] == 'CANCELADO') |
            (embarca['Status'] == 'CANCELADO Q')
        ) &
        (embarca['Metodo de Pagamento_V'] == 'CREDIT_CARD') &
        (embarca['Cancelamento_Mesmo_Mes'] == 0) & 
        (embarca['Data do Cancelamento'] < '2025-01-01'),
        
        ## ----- REGISTROS DEPOIS DE 01/01/2025 -----
        
        ## vendas com forma de pagamento pix
        (embarca['Status'] == 'APROVADO') &
        (embarca['Metodo de Pagamento_V'] == 'PIX') &
        (embarca['Data BPE'] >= '2025-01-01'),

        ## vendas com forma de pagamento credit_card
        (embarca['Status'] == 'APROVADO') &
        (embarca['Metodo de Pagamento_V'] == 'CREDIT_CARD') &
        (embarca['Data BPE'] >= '2025-01-01'),

        ## cancelamentos com forma de pagamento pix realizados no mesmo mês da venda
        (
            (embarca['Status'] == 'CANCELADO') |
            (embarca['Status'] == 'CANCELADO Q')
        ) &
        (embarca['Metodo de Pagamento_V'] == 'PIX') &
        (embarca['Cancelamento_Mesmo_Mes'] == 1) &
        (embarca['Data do Cancelamento'] >= '2025-01-01'),

        ## cancelamentos com forma de pagamento credit_card realizados no mesmo mês da venda
        (
            (embarca['Status'] == 'CANCELADO') |
            (embarca['Status'] == 'CANCELADO Q')
        ) &
        (embarca['Metodo de Pagamento_V'] == 'CREDIT_CARD') &
        (embarca['Cancelamento_Mesmo_Mes'] == 1) &
        (embarca['Data do Cancelamento'] >= '2025-01-01'),

        ## cancelamentos com forma de pagamento pix realizados no mês diferente ao da venda
        (
            (embarca['Status'] == 'CANCELADO') |
            (embarca['Status'] == 'CANCELADO Q')
        ) &
        (embarca['Metodo de Pagamento_V'] == 'PIX') &
        (embarca['Cancelamento_Mesmo_Mes'] == 0) &
        (embarca['Data do Cancelamento'] >= '2025-01-01'),

        ## cancelamentos com forma de pagamento credit_card realizados no mês diferente ao da venda
        (
            (embarca['Status'] == 'CANCELADO') |
            (embarca['Status'] == 'CANCELADO Q')
        ) &
        (embarca['Metodo de Pagamento_V'] == 'CREDIT_CARD') &
        (embarca['Cancelamento_Mesmo_Mes'] == 0) & 
        (embarca['Data do Cancelamento'] >= '2025-01-01')
        
    ]

    resultado_projecao = [
    
        ## ----- REGISTROS ANTES DE 01/01/2025 -----
        
        embarca['Data BPE'] + timedelta(days=1),
        embarca['Data BPE'] + (timedelta(days=30) * embarca['Parcela_Atual']) + timedelta(days=1),
        embarca['Data BPE'] + timedelta(days=1),
        embarca['Data BPE'] + (timedelta(days=30) * embarca['Parcela_Atual']) + timedelta(days=1),
        embarca['Data do Cancelamento'] + timedelta(days=1),
        embarca['Data do Cancelamento'] + (timedelta(days=30) * embarca['Parcela_Atual']) + timedelta(days=1),
        
        ## ----- REGISTROS DEPOIS DE 01/01/2025 -----
        
        embarca['Data BPE'] + timedelta(days=1),
        embarca['Data BPE'] + (timedelta(days=30) * embarca['Parcela Referente']) + timedelta(days=1),
        embarca['Data BPE'] + timedelta(days=1),
        embarca['Data BPE'] + (timedelta(days=30) * embarca['Parcela Referente']) + timedelta(days=1),
        embarca['Data do Cancelamento'] + timedelta(days=1),
        embarca['Data do Cancelamento'] + (timedelta(days=30) * embarca['Parcela Referente']) + timedelta(days=1)
        
    ]

    embarca['Data_Projecao'] = np.select(condicional_projecao, resultado_projecao, pd.NaT)
    embarca['Data_Projecao'] = pd.to_datetime(embarca['Data_Projecao']).dt.normalize()

    ## ajustando data útil

    condicional_dt_util = [
        embarca['Data_Projecao'].dt.dayofweek == 5,
        embarca['Data_Projecao'].dt.dayofweek == 6
    ]

    resultado_dt_util = [
        embarca['Data_Projecao'] + timedelta(days=2),
        embarca['Data_Projecao'] + timedelta(days=1)
    ]

    embarca['Data_Projecao'] = np.select(condicional_dt_util, resultado_dt_util, embarca['Data_Projecao'])

    ## definindo data de lancamento

    embarca['Data de Lancamento'] = np.where(
        embarca['Status'] == 'APROVADO',
        embarca['Data BPE'],
        embarca['Data do Cancelamento']
    )

    ## dropando colunas desnecessárias

    embarca.drop(columns=['MesAno_Venda', 'MesAno_Cancelado', 'Cancelamento_Mesmo_Mes', 'NOME_EMPRESA', 'ID TRANSACAO ORIGINAL'], inplace=True)

    ## definindo tipos das colunas

    tipos_colunas = {
        'Origem': str,
        'Nome da Empresa': str,
        'order_id': str,
        'ID Transacao': str,
        'Nº do Sistema': str,
        'Forma de pagamento': str,
        'id_adiquirente': str,
        'Canal': str,
        'Nome do passageiro': str,
        'Status': str,
        'Tarifa': float,
        'Taxas': float,
        'Valor Total': float,
        'Taxa de conveniência': float,
        'Valor do Cupom (R$)': float,
        'Promoção': float,
        'Descontos vindos da API': float,
        'Comissão': float,
        'Repasse': float,
        'Multa': float,
        'Marketing Digital': float,
        'parcelas pagas': str,
        'URL do BPe': str,
        'URL do Bilhete': str,
        'Seguro': float,
        'Repasse Seguro': float,
        'Repasse Seguro Parcela': float,
        'Obs': str,
        'Parcela_Atual': int,
        'Repasse_liquido': float
    }

    embarca = embarca.astype(tipos_colunas)

    embarca['Data da Compra'] = pd.to_datetime(embarca['Data da Compra'], errors='coerce').dt.normalize()
    embarca['Data do Cancelamento'] = pd.to_datetime(embarca['Data do Cancelamento'], errors='coerce').dt.normalize()
    embarca['Data de Lancamento'] = pd.to_datetime(embarca['Data de Lancamento'], errors='coerce').dt.normalize()
    embarca['Data_Projecao'] = pd.to_datetime(embarca['Data_Projecao'], errors='coerce').dt.normalize()
    embarca['Data BPE'] = pd.to_datetime(embarca['Data BPE'], errors='coerce').dt.normalize()
    embarca['Data de Recebimento'] = pd.to_datetime(embarca['Data de Recebimento'], errors='coerce').dt.normalize()

    return embarca