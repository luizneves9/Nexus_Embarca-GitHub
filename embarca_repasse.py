## ----- IMPORTANDO BIBLIOTECAS -----

import pandas as pd
import numpy as np
import os
from datetime import timedelta

## ----- FUNÇÃO DE PROCESSAMENTO DOS REPASSES DA EMBARCA -----

def ler_arquivo_repasse(caminho_repasse, colunas_repasse):
    '''
    Função para LER/CARREGAR um arquivos de repasses, tratando suas colunas e retornando dois data frames.

    Parametros:
        caminho_repasse: diretório do arquivo.
        colunas_repasse: definição das colunas que serão consideradas nos dataframes.

    Retorna:
        pd.DataFrame: Dois data frames (df_aprov e df_canc) com os dados processados.
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

    if 'Data da Compra' in df_aprov.columns:
        df_aprov['Data da Compra'] = pd.to_datetime(df_aprov['Data da Compra'], errors='coerce').dt.tz_localize(None).dt.normalize()
    if 'Data do Cancelamento' in df_aprov.columns:
        df_aprov['Data do Cancelamento'] = pd.to_datetime(df_aprov['Data do Cancelamento'], errors='coerce').dt.tz_localize(None).dt.normalize()           
    
    if 'Data da Compra' in df_canc.columns:
        df_canc['Data da Compra'] = pd.to_datetime(df_canc['Data da Compra'], errors='coerce').dt.tz_localize(None).dt.normalize()
    if 'Data do Cancelamento' in df_canc.columns:
        df_canc['Data do Cancelamento'] = pd.to_datetime(df_canc['Data do Cancelamento'], errors='coerce').dt.tz_localize(None).dt.normalize() 

    ## ajustando o valor negativo dos cancelados
    colunas_valor_cancelado = ['Tarifa', 'Taxas', 'Valor Total', 'Taxa de conveniência','Repasse', 'Seguro', 'Repasse Seguro', 'Repasse Seguro Parcela']
    for col in colunas_valor_cancelado:
        if col in df_canc.columns:
            df_canc[col] = -df_canc[col]

    ## definindo a coluna 'origem' em cada arquivo
    df_aprov['Origem'] = nome_arquivo
    df_canc['Origem'] = nome_arquivo

    return [df_aprov, df_canc]

## ----- AGRUPANDO/CONSOLIDANDO RELATÓRIOS -----

def consolidar_arquivos_repasses(diretorio_repasse, colunas):

    '''
    Função para LER/CARREGAR os arquivos de repasses e agrupar todos os dados recebiso em um único data frame.

    Parametros:
        caminho_repasse: diretório da pasta.
        colunas_repasse: definição das colunas que serão consideradas nos dataframes.

    Retorna:
        pd.DataFrame: Um DataFrame consolidado com os dados processados.
    '''

    ## criando lista vazia
    lista = []

    ## definindo os arquivos dentro do diretório/repasse
    caminhos_arquivos = [
        os.path.join(diretorio_repasse, f) for f in os.listdir(diretorio_repasse) if f.endswith(('.xlsx', '.xls'))
    ]

    ## percorrendo cada arquivo e lendo
    for caminho in caminhos_arquivos:
        try:
            df_aprov, df_canc = ler_arquivo_repasse(caminho, colunas)
            if df_aprov is not None:
                lista.append(df_aprov)
            if df_canc is not None:
                lista.append(df_canc)
        except Exception as e:
            print(f'AVISO: Erro ao processar os arquivos de repasse da Embarca. ({e})')

    ## concatenando/agrupando os arquivos
    if lista:
        try:
            df = pd.concat(lista, ignore_index=True)
        except Exception as e:
            print(f'AVISO: Erro ao consolidar os arquivos de repasse da Embarca. ({e})')                

    return df

## ----- PRÉ-PROCESSAMENTO DO DATA FRAME EMBARCA -----

def pre_processamento_embarca(df):

    '''
    Função para realizar o pré processamento dos arquivos da Embarca.

    Parametros:
        df: dataframe da Embarca que precisamos tratar.

    Retorna:
        pd.DataFrame: Um DataFrame com os primeiros processamentos.
    '''

    ## renomeando colunas
    df.rename(columns={
        'Operadora': 'Nome da Empresa',
        'ID do Bilhete': 'ID Transacao'
    }, inplace=True)

    ## incluindo colunas
    df['Data de Recebimento'] = df['Origem'].str[:4] + '-' + df['Origem'].str[4:6] + '-01'
    df['Parcela_Atual'] = df['parcelas pagas'].astype(str).str.split('/').str[0]
    df['Nº do Sistema'] = df['Nº do Sistema'].astype(str).str.split('.').str[0]
    df['ID Transacao'] = df['ID Transacao'].astype(str).str.split('.').str[0]

    df['MesAno_Venda'] = df['Data da Compra'].dt.strftime('%Y-%m')
    df['MesAno_Cancelado'] = df['Data do Cancelamento'].dt.strftime('%Y-%m')

    condicional_mes_cancelamento = [df['MesAno_Venda'] == df['MesAno_Cancelado']]
    resultado_mes_cancelamento = [1]
    df['Cancelamento_Mesmo_Mes'] = np.select(condicional_mes_cancelamento, resultado_mes_cancelamento, 0)

    df = df.sort_values('Data de Recebimento')

    return df

## ----- AGRUPANDO A PLANILHA DO CLIENTE COM O TOTALBUS PARA TRAZER A DATA BPE

def mesclagem_totalbus(df_embarca, df_totalbus):

    '''
    Função para mesclar os arquivos processados da EMBARCA com o do TOTALBUS/VENDAS, cujo a finalidade é trazer a DATA BPE.

    Parametros:
        df_embarca: Data frame da EMBARCA.
        df_totalbus: Data frame do TOTALBUS.

    Retorna:
        pd.DataFrame: Data frame da Embarca com a coluna DATA BPE preenchida, ou com a emissão do bilhete, ou com a própria data da compra.
    '''

    ## pré processamento do data frame totalbus
    df_totalbus_conciliador = df_totalbus.copy()

    df_totalbus_conciliador.rename(columns={'DATA HORA VENDA PARA CANC.': 'Data BPE'}, inplace=True)

    df_totalbus_conciliador['Data BPE'] = pd.to_datetime(df_totalbus_conciliador['Data BPE'], errors='coerce')
    df_totalbus_conciliador['ID TRANSACAO ORIGINAL'] = df_totalbus_conciliador['ID TRANSACAO ORIGINAL'].astype(str)

    ## filtrando os dados necessários e ordenando
    df_totalbus_conciliador = df_totalbus_conciliador[df_totalbus_conciliador['STATUS BILHETE'] == 'V']
    df_totalbus_conciliador = df_totalbus_conciliador[['NOME_EMPRESA', 'Data BPE', 'ID TRANSACAO ORIGINAL']]
    df_totalbus_conciliador = df_totalbus_conciliador.sort_values(by='Data BPE')

    ## pré processamento do data frame principal
    df_embarca['Data da Compra'] = pd.to_datetime(df_embarca['Data da Compra'], errors='coerce')
    df_embarca['ID Transacao'] = df_embarca['ID Transacao'].astype(str)
    df_embarca = df_embarca.sort_values(by='Data da Compra')

    ## realizando o agrupamento pelo merge_asoft (permite tolerância entre datas)
    df_agrupado = pd.merge_asof(
        df_embarca,
        df_totalbus_conciliador,
        left_on= 'Data da Compra',
        right_on= 'Data BPE',
        left_by= ['Nome da Empresa', 'ID Transacao'],
        right_by= ['NOME_EMPRESA', 'ID TRANSACAO ORIGINAL'],
        direction= 'nearest',
        tolerance= pd.Timedelta('1 day')
    )

    ## tratamento final coluna Data BPE (dados vazios) 
    df_agrupado['Data BPE'] = df_agrupado['Data BPE'].fillna(df_agrupado['Data da Compra'])
    df_agrupado = df_agrupado.sort_values('Data de Recebimento')

    return df_agrupado


## ----- FUNÇÃO DE PROJEÇÃO DE DATA DE PAGAMENTO -----

def projecao_data_pagamento(df):

    '''
    Função para PROJETAR a data de pagamento dos dados da EMBARCA. Com essa data, identificaremos onde (data) esse valor realmente deverá ser considerado.
    
    Parametros:
        df: Data frame da EMBARCA.

    Retorna:
        pd.DataFrame: Data frame da EMBARCA com as projeções de pagamento e parcela referente.
    '''

    ## ----- AJUSTANDO SEQUENCIAL DAS TRANSAÇÕES (SERÁ UTILIZADO PARA CÁLCULAR A DATA DE PROJEÇÃO CORRETA) ----- 

    ## definindo a coluna tipo: automatico/manual
    df['Tipo'] = 'Automatico'

    filtro_tipo = df['order_id'] == 'AJUSTE'
    df.loc[filtro_tipo, 'Tipo'] = 'Manual'

    ## definindo um sequencial
    df['Sequencial'] = df.groupby(['Nome da Empresa', 'ID Transacao', 'Data de Lancamento', 'Status', 'Tipo']).cumcount() + 1

    ## definindo uma coluna de parcela referente, considerando parcela atual ou sequencial
    df['Parcela Referente'] = np.minimum(df['Sequencial'], df['Parcelas da Venda']).astype(int)
    df.loc[filtro_tipo, 'Parcela Referente'] = df['Parcela_Atual']

    ## ----- PROJETANDO A DATA DE PAGAMENTO -----

    data_base = pd.to_datetime('2024-10-01')

    ## inclusão da condição, critério e resultado, através do np.select
    condicional_projecao_pos_data_base = [
        (df['Status'] == 'APROVADO') & (df['Metodo de Pagamento_V'] == 'PIX') & (df['Data de Lancamento'] >= data_base),
        (df['Status'] == 'APROVADO') & (df['Metodo de Pagamento_V'] == 'CREDIT_CARD') & (df['Data de Lancamento'] >= data_base),
        (df['Status'].isin(['CANCELADO', 'CANCELADO Q'])) & (df['Metodo de Pagamento_V'] == 'PIX') & (df['Cancelamento_Mesmo_Mes'] == 1) & (df['Data de Lancamento'] >= data_base),
        (df['Status'].isin(['CANCELADO', 'CANCELADO Q'])) & (df['Metodo de Pagamento_V'] == 'PIX') & (df['Cancelamento_Mesmo_Mes'] == 0) & (df['Data de Lancamento'] >= data_base),
        (df['Status'].isin(['CANCELADO', 'CANCELADO Q'])) & (df['Metodo de Pagamento_V'] == 'CREDIT_CARD') & (df['Cancelamento_Mesmo_Mes'] == 1) & (df['Data de Lancamento'] >= data_base),
        (df['Status'].isin(['CANCELADO', 'CANCELADO Q'])) & (df['Metodo de Pagamento_V'] == 'CREDIT_CARD') & (df['Cancelamento_Mesmo_Mes'] == 0) & (df['Data de Lancamento'] >= data_base)
    ]
    
    ## resultados para projeção de datas
    resultado_projecao_pos_data_base = [
        df['Data BPE'] + timedelta(days=1),
        df['Data BPE'] + (timedelta(days=30) * df['Parcela Referente']) + timedelta(days=1),
        df['Data BPE'] + timedelta(days=1),
        df['Data do Cancelamento'] + timedelta(days=1),
        df['Data BPE'] + (timedelta(days=30) * df['Parcela Referente']) + timedelta(days=1),
        df['Data do Cancelamento'] + (timedelta(days=30) * df['Parcela Referente']) + timedelta(days=1)
    ]

    condicional_projecao_pre_data_base = [
        (df['Status'] == 'APROVADO') & (df['Metodo de Pagamento_V'] == 'PIX') & (df['Data de Lancamento'] < data_base),
        (df['Status'] == 'APROVADO') & (df['Metodo de Pagamento_V'] == 'CREDIT_CARD') & (df['Data de Lancamento'] < data_base),
        (df['Status'].isin(['CANCELADO', 'CANCELADO Q'])) & (df['Metodo de Pagamento_V'] == 'PIX') & (df['Cancelamento_Mesmo_Mes'] == 1) & (df['Data de Lancamento'] < data_base),
        (df['Status'].isin(['CANCELADO', 'CANCELADO Q'])) & (df['Metodo de Pagamento_V'] == 'PIX') & (df['Cancelamento_Mesmo_Mes'] == 0) & (df['Data de Lancamento'] < data_base),
        (df['Status'].isin(['CANCELADO', 'CANCELADO Q'])) & (df['Metodo de Pagamento_V'] == 'CREDIT_CARD') & (df['Cancelamento_Mesmo_Mes'] == 1) & (df['Data de Lancamento'] < data_base),
        (df['Status'].isin(['CANCELADO', 'CANCELADO Q'])) & (df['Metodo de Pagamento_V'] == 'CREDIT_CARD') & (df['Cancelamento_Mesmo_Mes'] == 0) & (df['Data de Lancamento'] < data_base)
    ]

    ## resultados para projeção de datas ANTES da data base
    resultado_projecao_pre_data_base = [
        df['Data BPE'] + timedelta(days=1),
        df['Data BPE'] + (timedelta(days=30) * df['Parcela_Atual']) + timedelta(days=1),
        df['Data BPE'] + timedelta(days=1),
        df['Data do Cancelamento'] + timedelta(days=1),
        df['Data BPE'] + (timedelta(days=30) * df['Parcela_Atual']) + timedelta(days=1),
        df['Data do Cancelamento'] + (timedelta(days=30) * df['Parcela_Atual']) + timedelta(days=1)
    ]

    df['Data de Lancamento'] = pd.to_datetime(df['Data de Lancamento'], errors='coerce')

    ## juntando as condições e resultados
    condicoes_completas = condicional_projecao_pos_data_base + condicional_projecao_pre_data_base
    resultados_completos = resultado_projecao_pos_data_base + resultado_projecao_pre_data_base
    
    ## definindo data_projecao
    df['Data_Projecao'] = np.select(condicoes_completas, resultados_completos, pd.NaT)

    df['Data_Projecao'] = pd.to_datetime(df['Data_Projecao']).dt.normalize()

    ## ajustando data útil
    data_sabado = df['Data_Projecao'] == 5
    data_domingo = df['Data_Projecao'] == 6

    df.loc[data_sabado, 'Data_Projecao'] += timedelta(days=2)
    df.loc[data_domingo, 'Data_Projecao'] += timedelta(days=1)

    return df

## ----- INCLUINDO A COLUNA REPASSE_LIQUIDO -----

def calculo_repasse(df):

    '''
    Função para CALCULAR o valor de repasse do df da Embarca.

    Parametros:
        df: Data frame da Embarca.
        
    Retorna:
        pd.DataFrame: Um DataFrame com os valores de repasses..
    '''

    ## ----- CRIANDO COLUNA DE REPASSE -----

    ## inclusão da condição
    condicional_repasse = [
        (df['Status'] == 'CANCELADO'),
        (df['Status'] == 'CANCELADO Q'),
        (df['Status'] == 'APROVADO')
    ]

    ## inclusão do resultado
    resultado_repasse = [
        df['Repasse'] + df['Multa'],
        df['Repasse'] + df['Multa'] + df['Comissão'],
        df['Repasse']
    ]

    ## definindo as colunas de repasse
    df['Repasse_liquido'] = np.select(condicional_repasse, resultado_repasse, 0)
    df['Repasse_liquido_inv'] = -df['Repasse_liquido']

    ## ----- CRIANDO COLUNA DE REPASSE COM SEGURO -----

    ## replicando a coluna de repasse
    df['Repasse_liquido_com_seguro'] = df['Repasse_liquido']

    ## filtrando apenas o que for diferente do CANCELADO Q
    filtro_seguro = df['Status'] != 'CANCELADO Q'

    ## somando o seguro nos dados filtrados
    df.loc[filtro_seguro, 'Repasse_liquido_com_seguro'] = df.loc[filtro_seguro, 'Repasse_liquido_com_seguro'] + df['Repasse Seguro Parcela']

    df['Repasse_liquido_com_seguro_inv'] = -df['Repasse_liquido_com_seguro']

    return df

def ajuste_tipo(df):
    
    '''
    Função para DEFINIR O TIPO de cada coluna do arquivo.
    
    Parametros:
        df: Data frame da Embarca.
        
    Retorna:
        pd.DataFrame: Um DataFrame consolidado com os tipos das colunas processadas.
    '''

    ## definindo as colunas que são do tipo str
    colunas_str = [
        'Origem', 'Nome da Empresa', 'order_id', 'ID Transacao', 'Nº do Sistema', 'Forma de pagamento',
                   'id_adiquirente', 'Canal', 'Nome do passageiro', 'Status', 'parcelas pagas', 'URL do BPe',
                   'URL do Bilhete', 'Obs'
                   ]
    
    ## definindo as colunas que são do tipo float

    colunas_float = [
        'Tarifa', 'Taxas', 'Valor Total', 'Taxa de conveniência', 'Valor do Cupom (R$)', 'Promoção',
                     'Descontos vindos da API', 'Comissão', 'Repasse', 'Multa', 'Marketing Digital', 'Seguro',
                     'Repasse Seguro', 'Repasse Seguro Parcela', 'Repasse_liquido'
                     ]

    ## definindo as colunas que são do tipo int
    colunas_int = [
        'Parcela_Atual'
        ]

    ## definindo o tipo das colunas
    tipos_colunas = {}
    tipos_colunas.update({col: str for col in colunas_str})
    tipos_colunas.update({col: float for col in colunas_float})
    tipos_colunas.update({col: int for col in colunas_int})

    return df.astype(tipos_colunas)

## ----- PROCESSANDO OS REPASSES DA EMBARCA -----

def processamento_repasses(diretorio_embarca_repasse, df_embarca_vendas, df_totalbus):

    '''
    Função para PROCESSAR os arquivos da EMBARCA (Centralizador de todo o processo).

    Parametros:
        diretorio_embarca_repasse: diretório da pasta onde estão salvos os arquivos da EMBARCA.
        df_embarca_vendas: Data frame das vendas da EMBARCA.
        df_totalbus: Data frame das vendas do TOTALBUS.

    Retorna:
        pd.DataFrame: Um DataFrame consolidado com os dados processados da EMBARCA.
    '''

    ## ----- IMPORTAÇÃO DE PLANILHAS -----

    ## definindo as colunas do data frame
    colunas_embarca = ['Operadora', 'order_id', 'ID do Bilhete', 'Nº do Sistema', 'Forma de pagamento',
                        'id_adiquirente', 'Canal', 'Nome do passageiro', 'Status', 'Data da Compra',
                        'Data do Cancelamento', 'Tarifa', 'Taxas', 'Valor Total', 'Parcelas',
                        'Taxa de conveniência', 'Valor do Cupom (R$)', 'Promoção', 'Descontos vindos da API',
                        'Comissão', 'Repasse', 'Multa', 'Marketing Digital', 'parcelas pagas', 'URL do BPe',
                        'URL do Bilhete', 'Seguro', 'Repasse Seguro', 'Repasse Seguro Parcela', 'Obs']
    
    ## carregando todos os arquivos em um data frame
    embarca = consolidar_arquivos_repasses(diretorio_embarca_repasse, colunas_embarca)

    ## ----- TRATANDO AS PRIMEIRAS COLUNAS DO RELATÓRIO -----

    embarca = pre_processamento_embarca(embarca)

    ## ----- AGRUPANDO A PLANILHA DO CLIENTE COM O TOTALBUS PARA TRAZER A DATA BPE

    embarca = mesclagem_totalbus(embarca, df_totalbus)

    ## ----- AJUSTANDO A FORMA DE PAGAMENTO CONFORME A VENDA

    ## copiando a planilha de vendas da embarca
    df_embarca_vendas2 = df_embarca_vendas.copy()
    df_embarca_vendas2.drop(columns=['Status'], inplace=True)

    ## renomeando colunas
    df_embarca_vendas2.rename(columns={
        'Operadora': 'Nome da Empresa',
        'ID do Bilhete': 'ID Transacao',
        'Metodo de pagamento': 'Metodo de Pagamento_V',
        'parcelas': 'Parcelas da Venda'
    }, inplace=True)

    ## definindo tipo
    embarca['Nome da Empresa'] = embarca['Nome da Empresa'].astype(str)

    ## tratando colunas de data
    df_embarca_vendas2['Data da Venda'] = pd.to_datetime(df_embarca_vendas2['Data da Venda'], errors='coerce')
    if df_embarca_vendas2['Data da Venda'].dt.tz is not None:
        df_embarca_vendas2['Data da Venda'] = df_embarca_vendas2['Data da Venda'].dt.tz_localize(None)
    df_embarca_vendas2['Data da Venda'] = df_embarca_vendas2['Data da Venda'].dt.normalize()

    df_embarca_vendas2['Nome da Empresa'] = df_embarca_vendas2['Nome da Empresa'].astype(str)

    ## ordenando colunas pela data
    embarca = embarca.sort_values('Data da Compra')
    df_embarca_vendas2 = df_embarca_vendas2.sort_values('Data da Venda')

    ## agrupando as duas planilhas (embarca repasses e embarca vendas) pelo merge_asoft
    embarca = pd.merge_asof(
        embarca,
        df_embarca_vendas2,
        left_on= 'Data da Compra',
        right_on= 'Data da Venda',
        left_by= ['Nome da Empresa', 'ID Transacao'],
        right_by= ['Nome da Empresa', 'ID Transacao'],
        direction= 'nearest',
        tolerance= pd.Timedelta('1 day')
    )

    ## tratando dados
    embarca['Metodo de Pagamento_V'] = embarca['Metodo de Pagamento_V'].fillna(embarca['Forma de pagamento'])
    embarca['Venda Localizada'] = np.where(embarca['Parcelas da Venda'].isna(), 'NAO', 'SIM')
    embarca['Parcelas da Venda'] = embarca['Parcelas da Venda'].fillna(1)
    embarca = embarca.sort_values('Data de Recebimento')

    ## ----- DEFININDO A DATA DE LANÇAMENTO -----
    embarca['Status'] = embarca['Status'].astype(str).str.upper()

    ## inclusão da condição, critério e resultado, através do np.select
    condicional_dt_lancamento = [
        (embarca['Status'] == 'APROVADO'),
        (embarca['Status'] == 'CANCELADO') | (embarca['Status'] == 'CANCELADO Q')
    ]

    ## tratando colunas
    embarca['Nome do passageiro'] = embarca['Nome do passageiro'].str.replace('\n', '', regex=False).str.strip()
    embarca['Marketing Digital'] = embarca['Marketing Digital'].fillna(0)
    embarca['Forma de pagamento'] = embarca['Forma de pagamento'].astype(str).str.upper()
    embarca['Metodo de Pagamento_V'] = embarca['Metodo de Pagamento_V'].astype(str).str.upper()
    embarca['Parcela_Atual'] = embarca['Parcela_Atual'].fillna(-1).astype(int)

    embarca['Taxa de Conv. (%)'] = embarca['Taxa de conveniência'] / embarca['Valor Total']
    embarca['Percentual de Comissao'] = embarca['Comissão'] / (embarca['Valor Total'] + embarca['Taxa de conveniência'])
    embarca['Total da Venda'] = embarca['Valor Total'] + embarca['Taxa de conveniência']

    ## ----- INCLUINDO A COLUNA REPASSE_LIQUIDO -----

    embarca = calculo_repasse(embarca)

    ## ----- INCLUINDO A DATA DE LANCAMENTO -----

    resultado_dt_lancamento = [
        embarca['Data BPE'],
        embarca['Data do Cancelamento']
    ]

    embarca['Data de Lancamento'] = np.select(condicional_dt_lancamento, resultado_dt_lancamento, pd.NaT)
    embarca['Data de Lancamento'] = pd.to_datetime(embarca['Data de Lancamento'], errors='coerce')

    ## ----- PROJETANDO AS DATAS DE PAGAMENTO -----

    embarca = projecao_data_pagamento(embarca)

    ## ----- TRATATIVAS FINAIS DO RELATÓRIO -----

    ## dropando colunas desnecessárias
    embarca.drop(columns=['MesAno_Venda', 'MesAno_Cancelado', 'Cancelamento_Mesmo_Mes', 'NOME_EMPRESA', 'ID TRANSACAO ORIGINAL'], inplace=True)

    ## definindo o tipo das colunas
    embarca = ajuste_tipo(embarca)

    ## definindo o tipo das colunas de data
    colunas_data = ['Data da Compra', 'Data do Cancelamento', 'Data de Lancamento', 'Data_Projecao', 'Data BPE', 'Data de Recebimento']

    for col in colunas_data:
        embarca[col] = pd.to_datetime(embarca[col], errors='coerce').dt.normalize()

    return embarca